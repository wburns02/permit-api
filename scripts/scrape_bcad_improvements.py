#!/usr/bin/env python3
"""BCAD (Brazoria CAD) per-property improvement scraper -> tx_cad_parcels.

Fills the Brazoria year_built / building_sqft gap. The bulk county GIS layer
(load_brazoriacad.py) carries owner/situs/value but NO improvement detail; that
lives only in the BCAD esearch viewer behind an AJAX call:

    GET https://esearch.brazoriacad.org/Property/GetImprovements
        ?propertyId={prop_id}&year={year}&hideValue=False&valueMethod=C

This returns partial HTML (one "Property Improvement - Building" panel per
structure). We parse:
  * year_built       = MIN "Year Built" across all building segments
  * building_sqft    = SUM of per-panel "Living Area: N sqft" (finished area);
                       falls back to SUM of MA/MAIN AREA segment SQFT
  * improvement_class = the Class CD of the primary (largest-sqft) MA segment
  * improvement detail is also stashed into raw->'bcad_improvements'

ID mapping: tx_cad_parcels.parcel_id (cad_source='BRAZORIACAD') IS the esearch
propertyId (both = prop_id). No extra join needed.

The reCAPTCHA only guards the SEARCH form, not this direct AJAX route (verified).

DB write: idempotent UPDATE of the existing BRAZORIACAD row, keyed by
(cad_source, parcel_id, tax_year). lock_timeout set per-statement; NEVER calls
pg_terminate / pg_cancel; no full-table scans (updates a single PK row each).

Resumable: a checkpoint table bcad_improvement_progress records every prop_id
processed (with outcome) so a restart skips done work. Raw HTML for any property
is optionally staged to --raw-dir (default off; if set, must be off /home/will).

Usage (on R730-2, which can reach esearch + the T430 DB):
  python3 scrape_bcad_improvements.py --priority-only   # lead parcels first
  python3 scrape_bcad_improvements.py                   # whole county, resumable
"""
from __future__ import annotations

import argparse
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

ESEARCH = "https://esearch.brazoriacad.org"
IMPROV_URL = ESEARCH + "/Property/GetImprovements"
CAD = "BRAZORIACAD"
DEFAULT_YEAR = 2026

DB_HOST_DEFAULT = os.environ.get("PGHOST", "100.122.216.15")
DB_NAME_DEFAULT = os.environ.get("PGDATABASE", "permits")
DB_USER_DEFAULT = os.environ.get("PGUSER", "will")
DB_PORT_DEFAULT = int(os.environ.get("PGPORT", "5432"))

UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0 Safari/537.36 (ecbtx brazoria improvement enrich)")

# ---- parsing -------------------------------------------------------------

# Each structure renders as a panel that starts with the heading text and
# contains a "Living Area: N sqft" line plus a table of segment rows. A segment
# row's trailing two table-number cells are Year Built and SQFT.
PANEL_SPLIT = re.compile(r'Property Improvement', re.I)
LIVING_AREA = re.compile(r'Living Area:\s*</strong>\s*([\d,]+(?:\.\d+)?)\s*sqft', re.I)
# Row: <td>TYPE</td><td>DESC</td>[<td>CLASS</td>]<td class="table-number">YEAR</td><td class="table-number">SQFT</td>
SEG_ROW = re.compile(
    r'<td>\s*([A-Z0-9]{1,6})\s*</td>\s*'          # type code (MA, FG, OFP...)
    r'<td>\s*(.*?)\s*</td>'                        # description
    r'(?:\s*<td>\s*(.*?)\s*</td>)?'               # optional class CD
    r'\s*<td class="table-number">\s*(\d{3,4})\s*</td>'   # year built
    r'\s*<td class="table-number">\s*([\d,]+(?:\.\d+)?)\s*</td>',  # sqft
    re.S | re.I,
)


def _num(s):
    if s is None:
        return None
    s = str(s).replace(",", "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_improvements(html: str) -> dict:
    """Return {year_built, building_sqft, improvement_class, segments, panels}.

    year_built = min plausible year across all segments.
    building_sqft = sum of panel Living Areas if any > 0, else sum of MA segs.
    improvement_class = class code of the largest MA segment.
    """
    out = {
        "year_built": None,
        "building_sqft": None,
        "improvement_class": None,
        "segments": [],
        "panels": 0,
    }
    if not html or "Property Improvement" not in html:
        return out

    out["panels"] = max(0, len(PANEL_SPLIT.findall(html)) - 0)

    living_areas = [_num(m.group(1)) for m in LIVING_AREA.finditer(html)]
    living_total = sum(v for v in living_areas if v)

    years = []
    ma_sqft_total = 0.0
    best_ma = None  # (sqft, class)
    for m in SEG_ROW.finditer(html):
        typ = (m.group(1) or "").strip().upper()
        desc = _html.unescape(re.sub(r"\s+", " ", (m.group(2) or "")).strip())
        cls = _html.unescape((m.group(3) or "").strip()) or None
        yr = int(m.group(4))
        sqft = _num(m.group(5))
        if 1700 < yr < 2100:
            years.append(yr)
        seg = {"type": typ, "desc": desc, "class": cls, "year": yr, "sqft": sqft}
        out["segments"].append(seg)
        if typ == "MA" or "MAIN AREA" in desc.upper():
            if sqft:
                ma_sqft_total += sqft
            if sqft and (best_ma is None or sqft > best_ma[0]):
                best_ma = (sqft, cls)

    if years:
        out["year_built"] = min(years)
    if living_total > 0:
        out["building_sqft"] = round(living_total, 1)
    elif ma_sqft_total > 0:
        out["building_sqft"] = round(ma_sqft_total, 1)
    if best_ma:
        out["improvement_class"] = best_ma[1]
    return out


# ---- db ------------------------------------------------------------------

PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS public.bcad_improvement_progress (
    parcel_id   text PRIMARY KEY,
    tax_year    integer NOT NULL,
    outcome     text NOT NULL,          -- enriched | no_improvement | http_err | parse_empty
    year_built  integer,
    building_sqft numeric,
    scraped_at  timestamptz NOT NULL DEFAULT now()
);
"""

UPDATE_SQL = """
UPDATE public.tx_cad_parcels
   SET year_built        = COALESCE(%(year_built)s, year_built),
       building_sqft     = COALESCE(%(building_sqft)s, building_sqft),
       improvement_class = COALESCE(%(improvement_class)s, improvement_class),
       raw = COALESCE(raw, '{}'::jsonb)
             || jsonb_build_object('bcad_improvements', %(detail)s::jsonb)
 WHERE cad_source = %(cad)s
   AND parcel_id  = %(parcel_id)s
   AND tax_year   = %(tax_year)s
"""

PROGRESS_UPSERT = """
INSERT INTO public.bcad_improvement_progress
    (parcel_id, tax_year, outcome, year_built, building_sqft)
VALUES %s
ON CONFLICT (parcel_id) DO UPDATE
   SET tax_year=EXCLUDED.tax_year, outcome=EXCLUDED.outcome,
       year_built=EXCLUDED.year_built, building_sqft=EXCLUDED.building_sqft,
       scraped_at=now()
"""


def get_conn(args):
    if args.dsn:
        return psycopg2.connect(args.dsn, connect_timeout=20)
    return psycopg2.connect(
        host=args.host, port=args.port, dbname=args.db, user=args.user,
        connect_timeout=20)


PRIORITY_SQL = """
-- Lead-relevant parcels FIRST: any BRAZORIACAD parcel whose normalized situs
-- matches a brazoria_permit_lead (cad_matched), Pearland + Angleton leads. We
-- read the parcel_ids straight off the MV's cad_parcel_id (already resolved).
SELECT DISTINCT p.parcel_id
  FROM public.tx_cad_parcels p
  JOIN public.brazoria_permit_leads l
    ON l.cad_parcel_id = p.parcel_id
 WHERE p.cad_source = %(cad)s
   AND p.tax_year   = %(tax_year)s
   AND p.year_built IS NULL
"""

ALL_SQL = """
SELECT p.parcel_id
  FROM public.tx_cad_parcels p
 WHERE p.cad_source = %(cad)s
   AND p.tax_year   = %(tax_year)s
   AND p.year_built IS NULL
   AND NOT EXISTS (
        SELECT 1 FROM public.bcad_improvement_progress g
         WHERE g.parcel_id = p.parcel_id)
 ORDER BY p.parcel_id
"""


def load_targets(conn, args):
    cur = conn.cursor()
    cur.execute(PROGRESS_DDL)
    conn.commit()
    params = {"cad": CAD, "tax_year": args.tax_year}
    targets: list[str] = []
    seen = set()
    if not args.full_only:
        cur.execute(PRIORITY_SQL, params)
        for (pid,) in cur.fetchall():
            if pid not in seen:
                seen.add(pid)
                targets.append(pid)
        # drop any priority parcels already done
        if targets:
            cur.execute(
                "SELECT parcel_id FROM public.bcad_improvement_progress "
                "WHERE parcel_id = ANY(%s)", (targets,))
            done = {r[0] for r in cur.fetchall()}
            targets = [p for p in targets if p not in done]
        print(f"[priority] {len(targets)} lead parcels pending", flush=True)
    if not args.priority_only:
        cur.execute(ALL_SQL, params)
        for (pid,) in cur.fetchall():
            if pid not in seen:
                seen.add(pid)
                targets.append(pid)
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
    ap.add_argument("--dsn", default=os.environ.get("BRAZORIACAD_DSN",
                    os.environ.get("PERMITS_DSN", "")))
    ap.add_argument("--host", default=DB_HOST_DEFAULT)
    ap.add_argument("--db", default=DB_NAME_DEFAULT)
    ap.add_argument("--user", default=DB_USER_DEFAULT)
    ap.add_argument("--port", type=int, default=DB_PORT_DEFAULT)
    ap.add_argument("--tax-year", type=int, default=DEFAULT_YEAR)
    ap.add_argument("--rate", type=float, default=2.5,
                    help="max requests/sec (default 2.5)")
    ap.add_argument("--priority-only", action="store_true",
                    help="only scrape lead-matched parcels, then exit")
    ap.add_argument("--full-only", action="store_true",
                    help="skip the priority set, scrape the rest")
    ap.add_argument("--limit", type=int, default=0, help="cap parcels (0=all)")
    ap.add_argument("--lock-timeout", default="5s")
    ap.add_argument("--raw-dir", default="",
                    help="optional dir to stage raw HTML (MUST NOT be /home/will)")
    args = ap.parse_args()

    if args.raw_dir and args.raw_dir.startswith("/home/will"):
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
    print(f"[bcad] {len(targets):,} parcels to scrape "
          f"(rate<= {args.rate}/s)", flush=True)

    sess = requests.Session()
    sess.headers.update({"User-Agent": UA,
                         "X-Requested-With": "XMLHttpRequest",
                         "Referer": ESEARCH + "/"})

    min_interval = 1.0 / args.rate if args.rate > 0 else 0
    cur = conn.cursor()
    t0 = time.time()
    enriched = no_improv = errs = 0
    prog_batch: list[tuple] = []
    last_req = 0.0

    for i, pid in enumerate(targets, 1):
        if _STOP:
            break
        # throttle
        dt = time.time() - last_req
        if dt < min_interval:
            time.sleep(min_interval - dt)
        last_req = time.time()

        outcome = "http_err"
        parsed = {"year_built": None, "building_sqft": None,
                  "improvement_class": None, "segments": [], "panels": 0}
        try:
            r = sess.get(IMPROV_URL, params={
                "propertyId": pid, "year": args.tax_year,
                "hideValue": "False", "valueMethod": "C"}, timeout=30)
            if r.status_code == 200:
                html = r.text
                if args.raw_dir:
                    with open(os.path.join(args.raw_dir, f"{pid}.html"), "w") as fh:
                        fh.write(html)
                parsed = parse_improvements(html)
                if parsed["year_built"] or parsed["building_sqft"]:
                    outcome = "enriched"
                elif "Property Improvement" in html:
                    outcome = "parse_empty"
                else:
                    outcome = "no_improvement"
            else:
                outcome = f"http_{r.status_code}"
        except Exception as e:  # noqa: BLE001
            outcome = "http_err"
            errs += 1
            if errs % 20 == 1:
                print(f"[bcad] req err pid={pid}: {e}", flush=True)
            time.sleep(min(10, 1 + errs * 0.2))

        if outcome == "enriched":
            detail = json.dumps({
                "year_built": parsed["year_built"],
                "building_sqft": parsed["building_sqft"],
                "improvement_class": parsed["improvement_class"],
                "panels": parsed["panels"],
                "segments": parsed["segments"],
                "scraped_at": datetime.now(timezone.utc).isoformat(),
            })
            try:
                cur.execute(f"SET LOCAL lock_timeout = '{args.lock_timeout}'")
                cur.execute(UPDATE_SQL, {
                    "year_built": parsed["year_built"],
                    "building_sqft": parsed["building_sqft"],
                    "improvement_class": parsed["improvement_class"],
                    "detail": detail, "cad": CAD,
                    "parcel_id": pid, "tax_year": args.tax_year})
                enriched += 1
            except psycopg2.errors.LockNotAvailable:
                conn.rollback()
                outcome = "lock_skip"
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                outcome = "db_err"
                print(f"[bcad] db err pid={pid}: {e}", flush=True)
        elif outcome in ("no_improvement", "parse_empty"):
            no_improv += 1

        prog_batch.append((pid, args.tax_year, outcome,
                           parsed["year_built"], parsed["building_sqft"]))

        # commit every 25 parcels (one txn covers the UPDATEs since last commit)
        if len(prog_batch) >= 25:
            execute_values(cur, PROGRESS_UPSERT, prog_batch)
            conn.commit()
            prog_batch.clear()

        if i % 200 == 0 or i <= 5:
            el = time.time() - t0
            rate = i / el if el else 0
            eta = (len(targets) - i) / rate / 60 if rate else 0
            print(f"[bcad] {i:,}/{len(targets):,} enriched={enriched:,} "
                  f"no_imp={no_improv:,} err={errs} {rate:.1f}/s "
                  f"eta={eta:.0f}m", flush=True)

    if prog_batch:
        execute_values(cur, PROGRESS_UPSERT, prog_batch)
        conn.commit()
    cur.close()
    conn.close()
    el = (time.time() - t0) / 60
    print(f"[bcad] DONE processed={i if targets else 0:,} enriched={enriched:,} "
          f"no_imp={no_improv:,} err={errs} in {el:.1f}m", flush=True)


if __name__ == "__main__":
    main()
