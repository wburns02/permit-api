#!/usr/bin/env python3
"""Smith CAD (Tyler / Lindale, TX) per-property VALUE scraper -> tx_cad_parcels.

Fills the Smith County value gap. The bulk county GIS layer
(load_smithcad_parcels.py) carries owner/situs/year_built/sqft but NO value
columns, so tx_cad_parcels (cad_source='SMITHCAD') has market_value /
assessed_value / land_value / improvement_value all NULL.

The values DO live, free and open (no login / no captcha), on the GSACorp
eSearch property detail page, server-rendered HTML:

    GET https://smithcad-search.gsacorp.io/parcel/{parcel_id}

The value table renders as <tr><th>LABEL</th><td>$N</td></tr> rows inside the
"Preliminary Values" block. We parse:

  * market_value      = "Total Property Value"      (CAD market value)
  * assessed_value    = "Net Assessed Value"        (appraised / taxable cap)
  * land_value        = "Total Land Value"
  * improvement_value = "Total Building Value"

ID mapping (verified 2026-06-29 via Playwright + curl on live Lindale parcels):
  tx_cad_parcels.parcel_id (the GIS ACCOUNT/PIN, an 18-digit number, e.g.
  100000007100001010) IS the eSearch parcel route id. The "Property ID" shown on
  the page (1.00000.0071.00.001010) is the SAME number, only dot-formatted, and
  the detail URL uses the un-dotted 18-digit form == our parcel_id. So
  ACCOUNT == PropID; no extra join. (smith_parcel_geometries.parcel_id is the
  same value, available as a fallback.)

There is NO per-parcel JSON API: the detail page is server-rendered, so we parse
the HTML directly (the BCAD pattern). A CSV export endpoint
(/export?format=csv&catalog=r&search_str=...) exists but only carries Net
Assessed Value (not market) and only the limited search-result column set, so
the detail page is the authoritative value source.

DB write: idempotent UPDATE of the existing SMITHCAD row, keyed by
(cad_source, parcel_id). lock_timeout set per-statement; NEVER calls
pg_terminate / pg_cancel; no full-table scans (updates a single PK row each).

Resumable: a checkpoint table smithcad_value_progress records every parcel_id
processed (with outcome) so a restart skips done work. Raw HTML for any property
is optionally staged via --raw-dir (default off; MUST be off /home/will).

Usage (on R730-2, which can reach gsacorp + the T430 DB):
  python3 scrape_smithcad_values.py --priority-only   # Lindale lead parcels first
  python3 scrape_smithcad_values.py                   # whole county, resumable
  python3 scrape_smithcad_values.py --seed-csv path/to/lindale_best_skiptraced.csv
"""
from __future__ import annotations

import argparse
import csv
import html as _html
import json
import os
import re
import signal
import sys
import time
from datetime import datetime, timezone

import requests

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:  # pragma: no cover
    print("ERROR: pip install psycopg2-binary", file=sys.stderr)
    raise

BASE = "https://smithcad-search.gsacorp.io"
PARCEL_URL = BASE + "/parcel/{pid}"
CAD = "SMITHCAD"
COUNTY_FIPS = "48423"
DEFAULT_TAX_YEAR = 2026

DB_HOST_DEFAULT = os.environ.get("PGHOST", "100.122.216.15")
DB_NAME_DEFAULT = os.environ.get("PGDATABASE", "permits")
DB_USER_DEFAULT = os.environ.get("PGUSER", "will")
DB_PORT_DEFAULT = int(os.environ.get("PGPORT", "5432"))

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Safari/537.36 (ecbtx smith value enrich)")

# ---- parsing -------------------------------------------------------------

# Value rows render as: <tr><th>Total Property Value</th><td>$238,563</td></tr>
# (whitespace-tolerant). We capture the label -> dollar string for the four
# value rows we care about. The page repeats the block per appraisal year; the
# FIRST occurrence is the current (top) tax year, which is what we want.
VALUE_ROW = re.compile(
    r'<tr>\s*<th>\s*([^<]+?)\s*</th>\s*<td>\s*(\$[\d,]+|\$0)\s*</td>\s*</tr>',
    re.I | re.S,
)

LABEL_MAP = {
    "total property value": "market_value",
    "total building value": "improvement_value",
    "total land value": "land_value",
    "net assessed value": "assessed_value",
}


def _dollars(s):
    if s is None:
        return None
    s = str(s).replace("$", "").replace(",", "").strip()
    if not s:
        return None
    try:
        v = int(round(float(s)))
        return v
    except ValueError:
        return None


def parse_values(html: str) -> dict:
    """Return {market_value, assessed_value, land_value, improvement_value}.

    Uses the FIRST occurrence of each label (current tax year). Returns a dict
    with None for any value not present. A property with no improvements still
    has land/market/assessed rows, so absence of all four => parse_empty.
    """
    out = {
        "market_value": None,
        "assessed_value": None,
        "land_value": None,
        "improvement_value": None,
    }
    if not html:
        return out
    for m in VALUE_ROW.finditer(html):
        label = _html.unescape(m.group(1)).strip().lower()
        col = LABEL_MAP.get(label)
        if col and out[col] is None:  # keep first (current-year) value
            out[col] = _dollars(m.group(2))
        if all(out[c] is not None for c in out):
            break
    return out


# ---- db ------------------------------------------------------------------

PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS public.smithcad_value_progress (
    parcel_id        text PRIMARY KEY,
    outcome          text NOT NULL,   -- enriched | parse_empty | http_NNN | http_err
    market_value     bigint,
    assessed_value   bigint,
    land_value       bigint,
    improvement_value bigint,
    scraped_at       timestamptz NOT NULL DEFAULT now()
);
"""

# COALESCE so a re-run never wipes an already-set value with a transient NULL.
UPDATE_SQL = """
UPDATE public.tx_cad_parcels
   SET market_value      = COALESCE(%(market_value)s, market_value),
       assessed_value    = COALESCE(%(assessed_value)s, assessed_value),
       land_value        = COALESCE(%(land_value)s, land_value),
       improvement_value = COALESCE(%(improvement_value)s, improvement_value),
       raw = COALESCE(raw, '{}'::jsonb)
             || jsonb_build_object('smithcad_values', %(detail)s::jsonb)
 WHERE cad_source = %(cad)s
   AND parcel_id  = %(parcel_id)s
"""

PROGRESS_UPSERT = """
INSERT INTO public.smithcad_value_progress
    (parcel_id, outcome, market_value, assessed_value,
     land_value, improvement_value)
VALUES %s
ON CONFLICT (parcel_id) DO UPDATE
   SET outcome=EXCLUDED.outcome,
       market_value=EXCLUDED.market_value,
       assessed_value=EXCLUDED.assessed_value,
       land_value=EXCLUDED.land_value,
       improvement_value=EXCLUDED.improvement_value,
       scraped_at=now()
"""


def get_conn(args):
    if args.dsn:
        return psycopg2.connect(args.dsn, connect_timeout=20)
    return psycopg2.connect(
        host=args.host, port=args.port, dbname=args.db, user=args.user,
        connect_timeout=20)


# Lindale-city lead parcels FIRST so Zeth's leads get value immediately, then
# the rest of the county. "Lead" = a SMITHCAD parcel in Lindale that still has
# NULL market_value. The 50-row skiptraced CSV is keyed by address, so we ALSO
# resolve those addresses to parcel_ids when --seed-csv is given.
PRIORITY_SQL = """
SELECT p.parcel_id
  FROM public.tx_cad_parcels p
 WHERE p.cad_source = %(cad)s
   AND p.market_value IS NULL
   AND p.situs_city ILIKE 'LINDALE%%'
   AND NOT EXISTS (
        SELECT 1 FROM public.smithcad_value_progress g
         WHERE g.parcel_id = p.parcel_id)
 ORDER BY p.parcel_id
"""

# Seed-CSV address -> parcel_id resolver. Normalizes the situs the same way the
# MV does (strip unit/suite/#, punctuation, collapse spaces, upper).
SEED_SQL = """
SELECT p.parcel_id
  FROM public.tx_cad_parcels p
 WHERE p.cad_source = %(cad)s
   AND p.market_value IS NULL
   AND TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
         UPPER(p.situs_address),
         '(^|\\s)(SUITE|STE|UNIT|APT|#)\\s+\\S+', ' ', 'g'),
         '[.,#]', '', 'g'), '\\s+', ' ')) = ANY(%(addrs)s)
"""

ALL_SQL = """
SELECT p.parcel_id
  FROM public.tx_cad_parcels p
 WHERE p.cad_source = %(cad)s
   AND p.market_value IS NULL
   AND NOT EXISTS (
        SELECT 1 FROM public.smithcad_value_progress g
         WHERE g.parcel_id = p.parcel_id)
 ORDER BY p.parcel_id
"""


def _norm_addr(s: str) -> str:
    s = (s or "").upper()
    s = re.sub(r'(^|\s)(SUITE|STE|UNIT|APT|#)\s+\S+', ' ', s)
    s = re.sub(r'[.,#]', '', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


def load_targets(conn, args):
    cur = conn.cursor()
    cur.execute(PROGRESS_DDL)
    conn.commit()
    params = {"cad": CAD}
    targets: list[str] = []
    seen: set[str] = set()

    def _add(pids):
        for (pid,) in pids:
            if pid not in seen:
                seen.add(pid)
                targets.append(pid)

    if not args.full_only:
        # 1) explicit skiptraced lead CSV (address-keyed) -> parcel_ids
        if args.seed_csv and os.path.exists(args.seed_csv):
            addrs = set()
            with open(args.seed_csv, newline="") as fh:
                for row in csv.DictReader(fh):
                    a = row.get("address") or row.get("Address") or ""
                    if a:
                        addrs.add(_norm_addr(a))
            if addrs:
                cur.execute(SEED_SQL, {"cad": CAD, "addrs": list(addrs)})
                pre = len(targets)
                _add(cur.fetchall())
                print(f"[seed] {len(targets) - pre} parcels matched from "
                      f"{len(addrs)} CSV addresses ({args.seed_csv})", flush=True)
        # 2) broader Lindale-city lead set
        cur.execute(PRIORITY_SQL, params)
        pre = len(targets)
        _add(cur.fetchall())
        print(f"[priority] +{len(targets) - pre} Lindale parcels pending "
              f"(total priority={len(targets)})", flush=True)

    if not args.priority_only:
        cur.execute(ALL_SQL, params)
        pre = len(targets)
        _add(cur.fetchall())
        print(f"[all] +{len(targets) - pre} remaining county parcels", flush=True)

    cur.close()
    return targets


# ---- scrape loop ---------------------------------------------------------

_STOP = False


def _sig(_s, _f):
    global _STOP
    _STOP = True
    print("[signal] stopping after current parcel...", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=os.environ.get("SMITHCAD_DSN",
                    os.environ.get("PERMITS_DSN", "")))
    ap.add_argument("--host", default=DB_HOST_DEFAULT)
    ap.add_argument("--db", default=DB_NAME_DEFAULT)
    ap.add_argument("--user", default=DB_USER_DEFAULT)
    ap.add_argument("--port", type=int, default=DB_PORT_DEFAULT)
    ap.add_argument("--rate", type=float, default=2.5,
                    help="max requests/sec (default 2.5, gentle on host)")
    ap.add_argument("--seed-csv", default="",
                    help="address-keyed lead CSV; matched parcels scraped first")
    ap.add_argument("--priority-only", action="store_true",
                    help="only scrape Lindale/seed lead parcels, then exit")
    ap.add_argument("--full-only", action="store_true",
                    help="skip the priority set, scrape the rest of the county")
    ap.add_argument("--limit", type=int, default=0, help="cap parcels (0=all)")
    ap.add_argument("--lock-timeout", default="5s")
    ap.add_argument("--raw-dir", default="",
                    help="optional dir to stage raw HTML (MUST NOT be /home/will)")
    args = ap.parse_args()

    if args.raw_dir and os.path.abspath(args.raw_dir).startswith("/home/will"):
        print("REFUSING: --raw-dir on home drive violates Storage Policy",
              file=sys.stderr)
        sys.exit(3)
    if args.raw_dir:
        os.makedirs(args.raw_dir, exist_ok=True)

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    conn = get_conn(args)
    conn.autocommit = False
    targets = load_targets(conn, args)
    if args.limit:
        targets = targets[: args.limit]
    print(f"[smith] {len(targets):,} parcels to scrape "
          f"(rate<= {args.rate}/s)", flush=True)

    sess = requests.Session()
    sess.headers.update({"User-Agent": UA, "Referer": BASE + "/"})

    min_interval = 1.0 / args.rate if args.rate > 0 else 0
    cur = conn.cursor()
    t0 = time.time()
    enriched = empties = errs = 0
    prog_batch: list[tuple] = []
    last_req = 0.0
    i = 0

    for i, pid in enumerate(targets, 1):
        if _STOP:
            break
        dt = time.time() - last_req
        if dt < min_interval:
            time.sleep(min_interval - dt)
        last_req = time.time()

        outcome = "http_err"
        vals = {"market_value": None, "assessed_value": None,
                "land_value": None, "improvement_value": None}
        try:
            r = sess.get(PARCEL_URL.format(pid=pid), timeout=30)
            if r.status_code == 200:
                html = r.text
                if args.raw_dir:
                    with open(os.path.join(args.raw_dir, f"{pid}.html"), "w") as fh:
                        fh.write(html)
                vals = parse_values(html)
                if any(v is not None for v in vals.values()):
                    outcome = "enriched"
                else:
                    outcome = "parse_empty"
            else:
                outcome = f"http_{r.status_code}"
        except Exception as e:  # noqa: BLE001
            outcome = "http_err"
            errs += 1
            if errs % 20 == 1:
                print(f"[smith] req err pid={pid}: {e}", flush=True)
            time.sleep(min(10, 1 + errs * 0.2))

        if outcome == "enriched":
            detail = json.dumps({
                **vals,
                "tax_year": DEFAULT_TAX_YEAR,
                "source": "smithcad-search.gsacorp.io/parcel",
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
            try:
                cur.execute(f"SET LOCAL lock_timeout = '{args.lock_timeout}'")
                cur.execute(UPDATE_SQL, {
                    **vals, "detail": detail,
                    "cad": CAD, "parcel_id": pid})
                enriched += 1
            except psycopg2.errors.LockNotAvailable:
                conn.rollback()
                outcome = "lock_skip"
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                outcome = "db_err"
                print(f"[smith] db err pid={pid}: {e}", flush=True)
        elif outcome == "parse_empty":
            empties += 1

        prog_batch.append((pid, outcome, vals["market_value"],
                           vals["assessed_value"], vals["land_value"],
                           vals["improvement_value"]))

        if len(prog_batch) >= 25:
            execute_values(cur, PROGRESS_UPSERT, prog_batch)
            conn.commit()
            prog_batch.clear()

        if i % 200 == 0 or i <= 5:
            el = time.time() - t0
            rate = i / el if el else 0
            eta = (len(targets) - i) / rate / 60 if rate else 0
            print(f"[smith] {i:,}/{len(targets):,} enriched={enriched:,} "
                  f"empty={empties:,} err={errs} {rate:.1f}/s "
                  f"eta={eta:.0f}m", flush=True)

    if prog_batch:
        execute_values(cur, PROGRESS_UPSERT, prog_batch)
        conn.commit()
    cur.close()
    conn.close()
    el = (time.time() - t0) / 60
    print(f"[smith] DONE processed={i:,} enriched={enriched:,} "
          f"empty={empties:,} err={errs} in {el:.1f}m", flush=True)


if __name__ == "__main__":
    main()
