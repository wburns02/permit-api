#!/usr/bin/env python3
"""Verify / top-up Ascension Parish, LA (FIPS 22005) storm_events.

Ascension is a WIND/tropical product, same peril as East Baton Rouge. The
production NOAA loader (app/services/noaa_loader.py) already includes LOUISIANA
in STATE_FILTER, and the EBR backfill (scripts/load_ebr_storm_events.py) already
seeded ALL Louisiana storm_events -- so Ascension (cz_name='ASCENSION',
cz_type='C', cz_fips=5) is ALREADY present and stays fresh nightly with NO code
change. This script is the idempotent verify/top-up mirror of the EBR storm
loader: it re-runs the same LA-only NCEI backfill (safe, ON CONFLICT) and then
PRINTS the Ascension coverage so a deploy can confirm the storm substrate.

Peril for Ascension is WIND/tropical (Thunderstorm Wind dominates; High/Strong
Wind, Tropical Storm, Hurricane) with HAIL secondary. The peril FILTER lives in
the unserviced_hail_leads MV, not here -- this loads ALL LA event types.

Usage:
    python3 scripts/load_ascension_storm_events.py [--years N] [--verify-only]

DSN: --dsn / $PERMITS_DSN / ~/.config/permitlookup/permits_dsn.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

import httpx
import psycopg2
from psycopg2.extras import execute_batch

INDEX_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
STATE = "LOUISIANA"
_INDEX_RX = re.compile(
    r'href="(StormEvents_details-ftp_v1\.0_d(\d{4})_c(\d{8})\.csv\.gz)"'
)

UPSERT = """
INSERT INTO storm_events (
    event_id, episode_id, state, state_fips, year, event_type,
    cz_type, cz_fips, cz_name, wfo,
    begin_datetime, end_datetime, cz_timezone,
    injuries_direct, injuries_indirect, deaths_direct, deaths_indirect,
    damage_property, damage_crops, source,
    magnitude, magnitude_type, flood_cause, tor_f_scale,
    begin_location, end_location, begin_lat, begin_lon, end_lat, end_lon,
    episode_narrative, event_narrative, scraped_at
) VALUES (
    %(event_id)s, %(episode_id)s, %(state)s, %(state_fips)s, %(year)s, %(event_type)s,
    %(cz_type)s, %(cz_fips)s, %(cz_name)s, %(wfo)s,
    %(begin_datetime)s, %(end_datetime)s, %(cz_timezone)s,
    %(injuries_direct)s, %(injuries_indirect)s, %(deaths_direct)s, %(deaths_indirect)s,
    %(damage_property)s, %(damage_crops)s, %(source)s,
    %(magnitude)s, %(magnitude_type)s, %(flood_cause)s, %(tor_f_scale)s,
    %(begin_location)s, %(end_location)s, %(begin_lat)s, %(begin_lon)s, %(end_lat)s, %(end_lon)s,
    %(episode_narrative)s, %(event_narrative)s, %(scraped_at)s
)
ON CONFLICT (event_id) DO UPDATE SET
    damage_property = COALESCE(EXCLUDED.damage_property, storm_events.damage_property),
    magnitude       = COALESCE(EXCLUDED.magnitude, storm_events.magnitude),
    event_narrative = COALESCE(EXCLUDED.event_narrative, storm_events.event_narrative),
    scraped_at      = EXCLUDED.scraped_at
"""

ASCENSION_COVERAGE = """
SELECT event_type, count(*) AS n,
       min(begin_datetime)::date AS d0, max(begin_datetime)::date AS d1
  FROM storm_events
 WHERE state='LOUISIANA' AND cz_name='ASCENSION' AND cz_type='C'
 GROUP BY event_type ORDER BY event_type
"""


def resolve_dsn(cli):
    if cli:
        return cli
    if os.environ.get("PERMITS_DSN"):
        return os.environ["PERMITS_DSN"]
    p = Path.home() / ".config" / "permitlookup" / "permits_dsn"
    if p.exists():
        return p.read_text().strip()
    sys.exit("No DSN.")


def _i(s):
    if s in (None, ""):
        return None
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def _f(s):
    if s in (None, ""):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _ts(s):
    if not s:
        return None
    s = s.strip()
    for fmt in ("%d-%b-%y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def list_year_urls(client):
    r = client.get(INDEX_URL, timeout=30)
    r.raise_for_status()
    best = {}
    for m in _INDEX_RX.finditer(r.text):
        fname, yr, cdate = m.group(1), int(m.group(2)), m.group(3)
        if yr not in best or cdate > best[yr][1]:
            best[yr] = (fname, cdate)
    return {yr: INDEX_URL + t[0] for yr, t in best.items()}


def fetch_la(client, url):
    r = client.get(url, timeout=180)
    r.raise_for_status()
    raw = gzip.decompress(r.content).decode("utf-8", errors="replace")
    today = date.today()
    out = []
    for row in csv.DictReader(io.StringIO(raw)):
        if row.get("STATE") != STATE:
            continue
        eid = _i(row.get("EVENT_ID"))
        if eid is None:
            continue
        out.append({
            "event_id": eid,
            "episode_id": _i(row.get("EPISODE_ID")),
            "state": row.get("STATE"),
            "state_fips": _i(row.get("STATE_FIPS")),
            "year": _i(row.get("YEAR")),
            "event_type": row.get("EVENT_TYPE"),
            "cz_type": row.get("CZ_TYPE"),
            "cz_fips": _i(row.get("CZ_FIPS")),
            "cz_name": row.get("CZ_NAME"),
            "wfo": row.get("WFO"),
            "begin_datetime": _ts(row.get("BEGIN_DATE_TIME")),
            "end_datetime": _ts(row.get("END_DATE_TIME")),
            "cz_timezone": row.get("CZ_TIMEZONE"),
            "injuries_direct": _i(row.get("INJURIES_DIRECT")),
            "injuries_indirect": _i(row.get("INJURIES_INDIRECT")),
            "deaths_direct": _i(row.get("DEATHS_DIRECT")),
            "deaths_indirect": _i(row.get("DEATHS_INDIRECT")),
            "damage_property": row.get("DAMAGE_PROPERTY"),
            "damage_crops": row.get("DAMAGE_CROPS"),
            "source": row.get("SOURCE"),
            "magnitude": _f(row.get("MAGNITUDE")),
            "magnitude_type": row.get("MAGNITUDE_TYPE"),
            "flood_cause": row.get("FLOOD_CAUSE"),
            "tor_f_scale": row.get("TOR_F_SCALE"),
            "begin_location": row.get("BEGIN_LOCATION"),
            "end_location": row.get("END_LOCATION"),
            "begin_lat": _f(row.get("BEGIN_LAT")),
            "begin_lon": _f(row.get("BEGIN_LON")),
            "end_lat": _f(row.get("END_LAT")),
            "end_lon": _f(row.get("END_LON")),
            "episode_narrative": (row.get("EPISODE_NARRATIVE") or "")[:2000] or None,
            "event_narrative": (row.get("EVENT_NARRATIVE") or "")[:2000] or None,
            "scraped_at": today,
        })
    return out


def print_coverage(conn):
    with conn.cursor() as c:
        c.execute("SET statement_timeout = '30s'")
        c.execute(ASCENSION_COVERAGE)
        rows = c.fetchall()
    print("Ascension storm_events coverage (cz_name='ASCENSION', cz_type='C'):",
          flush=True)
    if not rows:
        print("  NONE -- Ascension not present (unexpected; LA backfill missing?)",
              flush=True)
    for et, n, d0, d1 in rows:
        print(f"  {et:<22} n={n:<4} {d0} .. {d1}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn")
    ap.add_argument("--years", type=int, default=3)
    ap.add_argument("--verify-only", action="store_true",
                    help="Skip the NCEI top-up; just print Ascension coverage.")
    args = ap.parse_args()

    dsn = resolve_dsn(args.dsn)
    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    if args.verify_only:
        print_coverage(conn)
        conn.close()
        return 0

    cur_year = datetime.utcnow().year
    years = list(range(cur_year - args.years + 1, cur_year + 1))
    total = 0
    started = time.time()

    with httpx.Client(follow_redirects=True) as client:
        urls = list_year_urls(client)
        for yr in years:
            url = urls.get(yr)
            if not url:
                print(f"year {yr}: not in NCEI index, skip", flush=True)
                continue
            recs = fetch_la(client, url)
            print(f"year {yr}: {len(recs)} LA events fetched", flush=True)
            with conn.cursor() as c:
                c.execute("SET lock_timeout = '15s'")
                c.execute("SET statement_timeout = '0'")  # batches can exceed 20s
                for i in range(0, len(recs), 500):
                    execute_batch(c, UPSERT, recs[i:i + 500], page_size=500)
                    conn.commit()
            total += len(recs)

    print(f"LA storm_events upserted={total} in {time.time()-started:.0f}s",
          flush=True)
    print_coverage(conn)
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
