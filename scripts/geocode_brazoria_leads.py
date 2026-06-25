#!/usr/bin/env python3
"""Backfill lat/lng for Brazoria permit-lead rows that have an address but no
coords, using the free US Census geocoder, into the shared `geocoded_addresses`
cache.

Phase 3 of the Brazoria TX permit-lead feed. The `brazoria_permit_leads`
materialized view LEFT JOINs `geocoded_addresses` (keyed on a normalized
address) to fill coords for rows whose source landed no lat/lng — chiefly
`mgo_angleton` (MGO Connect exposes no geometry). 911 rows already carry coords
and are skipped.

Reuses
------
- The Census geocoder call shape from `app/services/rural_score.py`
  (`_call_census_geocoder`) — same benchmark, same one-line address.
- The `geocoded_addresses` cache table (address_norm PK) that rural_score and
  the broadband resolver already populate. We only ADD rows; never destructive.

Why a batch script (not in-request / not in the MV refresh)
-----------------------------------------------------------
Geocoding is rate-limited network I/O (Census ~ a few req/s politely). Doing it
inside the MV refresh would make the nightly refresh hang on the network. So we
geocode out-of-band into the cache; the MV just reads the cache. Run this on a
cron after the scrapers, before the MV refresh.

Safety
------
- Source-filtered via the indexed `ix_hot_leads_source` — NEVER a full scan of
  the 12.9M-row hot_leads table.
- `--limit` caps how many addresses are geocoded per run (default 500).
- Already-cached addresses are skipped (cache lookup is a PK hit).

Usage
-----
    python3 scripts/geocode_brazoria_leads.py                 # up to 500 rows
    python3 scripts/geocode_brazoria_leads.py --limit 100
    python3 scripts/geocode_brazoria_leads.py --dry-run
    python3 scripts/geocode_brazoria_leads.py --sleep 0.3     # politeness delay
"""

from __future__ import annotations

import argparse
import re
import sys
import time

try:
    import httpx
except ImportError:
    print("ERROR: pip install httpx", file=sys.stderr)
    raise

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary", file=sys.stderr)
    raise

DB_HOST_DEFAULT = "100.122.216.15"
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"

# Brazoria permit sources that may lack coords. Kept in sync with
# app/services/permit_lead_classify.BRAZORIA_SOURCES (non-trigger sources only —
# 911 rows already carry geometry). Add a source here when it joins the feed.
GEOCODE_SOURCES = ["mgo_angleton"]

# Default county for one-line geocode when the row's county is NULL.
DEFAULT_COUNTY_STATE = ("Brazoria", "TX")


def log(msg: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def normalize_address(address: str) -> str:
    """Mirror app/api/v1/hail_leads._ADDRESS_NORM_SQL:
    UPPER(REGEXP_REPLACE(addr, '[^A-Za-z0-9 ]', ' ', 'g')) then collapse spaces.
    The MV joins geocoded_addresses on this exact normalization."""
    s = re.sub(r"[^A-Za-z0-9 ]", " ", address or "")
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s


def fetch_ungeocoded(conn, limit: int) -> list[dict]:
    """Brazoria permit rows with an address, no coords, and no cache hit yet.

    Source-filtered (indexed). Statement-timeout guards against the write-storm.
    """
    sources_sql = ", ".join("%s" for _ in GEOCODE_SOURCES)
    sql = f"""
        SET LOCAL statement_timeout = '60s';
        SELECT DISTINCT ON (norm)
               hl.address,
               hl.city,
               hl.zip,
               UPPER(REGEXP_REPLACE(hl.address, '[^A-Za-z0-9 ]', ' ', 'g')) AS norm
          FROM hot_leads hl
          LEFT JOIN geocoded_addresses g
                 ON g.address_norm =
                    UPPER(REGEXP_REPLACE(hl.address, '[^A-Za-z0-9 ]', ' ', 'g'))
         WHERE hl.source IN ({sources_sql})
           AND hl.address IS NOT NULL
           AND (hl.lat IS NULL OR hl.lng IS NULL)
           AND g.address_norm IS NULL
         ORDER BY norm
         LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute("BEGIN")
        cur.execute(sql, (*GEOCODE_SOURCES, limit))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        conn.commit()
    return [dict(zip(cols, r)) for r in rows]


def census_geocode(
    client: httpx.Client, address: str, city: str | None, zip_code: str | None
) -> tuple[float, float, str] | None:
    parts = [address.strip()]
    if city:
        parts.append(city.strip())
    parts.append(DEFAULT_COUNTY_STATE[1])  # state TX
    if zip_code:
        parts.append(str(zip_code)[:5].strip())
    one_line = ", ".join(p for p in parts if p)
    try:
        resp = client.get(
            CENSUS_URL,
            params={
                "address": one_line,
                "benchmark": "Public_AR_Current",
                "format": "json",
            },
            timeout=15.0,
        )
        resp.raise_for_status()
        body = resp.json()
    except Exception as e:  # noqa: BLE001
        log(f"  geocode error for {one_line!r}: {e}")
        return None
    matches = (body.get("result") or {}).get("addressMatches") or []
    if not matches:
        return None
    coords = matches[0].get("coordinates") or {}
    lat, lon = coords.get("y"), coords.get("x")
    if lat is None or lon is None:
        return None
    return float(lat), float(lon), matches[0].get("matchedAddress") or "match"


def upsert_cache(conn, rows: list[tuple]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO geocoded_addresses
                (address_norm, lat, lon, match_type, match_address, source)
            VALUES %s
            ON CONFLICT (address_norm) DO UPDATE SET
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                match_type = EXCLUDED.match_type,
                match_address = EXCLUDED.match_address,
                source = EXCLUDED.source,
                geocoded_at = NOW()
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Geocode Brazoria permit leads")
    parser.add_argument("--db-host", default=DB_HOST_DEFAULT)
    parser.add_argument("--limit", type=int, default=500,
                        help="Max addresses to geocode this run (default 500).")
    parser.add_argument("--sleep", type=float, default=0.25,
                        help="Seconds between Census calls (politeness).")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=args.db_host, port=DB_PORT, dbname=DB_NAME, user=DB_USER
    )
    log(f"DB connected: {args.db_host}/{DB_NAME}")

    try:
        targets = fetch_ungeocoded(conn, args.limit)
    except Exception as e:  # noqa: BLE001
        log(f"FATAL: could not fetch ungeocoded rows: {e}")
        conn.close()
        return 1

    log(f"{len(targets)} address(es) need geocoding (cap {args.limit}).")
    if args.dry_run:
        for t in targets[:20]:
            log(f"  would geocode: {t['address']} / {t.get('city')} {t.get('zip') or ''}")
        conn.close()
        return 0

    hits: list[tuple] = []
    misses = 0
    with httpx.Client() as client:
        for i, t in enumerate(targets, 1):
            res = census_geocode(client, t["address"], t.get("city"), t.get("zip"))
            norm = t["norm"]
            if res:
                lat, lon, matched = res
                hits.append((norm, lat, lon, "census", matched, "census_brazoria"))
            else:
                # Record the miss so we don't re-attempt every run (lat/lon NULL).
                hits.append((norm, None, None, "no_match", None, "census_brazoria"))
                misses += 1
            if i % 25 == 0:
                log(f"  {i}/{len(targets)} processed ({misses} misses so far)")
            time.sleep(args.sleep)

    written = upsert_cache(conn, hits)
    matched = written - misses
    log(f"DONE: {written} cached ({matched} matched, {misses} no-match).")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
