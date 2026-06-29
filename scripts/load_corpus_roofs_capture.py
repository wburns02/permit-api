#!/usr/bin/env python3
"""Load the browser-captured Corpus Christi re-roof permit set into hot_leads.

The City of Corpus Christi Infor "Rhythm"/CIVICS portal authorizes its public
search API only inside an established browser session (the JSESSIONID is bound to
the originating client — a standalone replay scraper gets 401 even with the exact
cookie jar). So scripts/scrape_corpus_permits.py (the replay scraper) cannot run
headless from the server; the roof subset was harvested via the MCP browser
context (list+detail XHR, see scratchpad corpus_recon.md) and staged to
  /mnt/win11/Fedora/free_data/corpus_permits/corpus_roofs_18mo.json

This loader ingests that staged JSON into hot_leads (source='infor_corpuschristi')
so scripts/promote_nueces_reroof.py promotes it into nueces_permits exactly like
the Port Aransas OpenGov rows. Re-run after a fresh harvest to refresh.

HONEST NOTE: Corpus does NOT issue a distinct "Re-Roof" permit type. These are
Residential Permit Application rows (workType Remodel/Addition/Accessory/New
Building) whose work-type/applicant/comments free-text mentions roof/shingle
(solar excluded). It is the best available roof signal for the market — sparse
(~142 over 18 months) and conservative.

Gentle on the DB: lock_timeout + statement_timeout on the write, ON CONFLICT
DO NOTHING, single bounded INSERT (no scans).

Usage:
  python3 load_corpus_roofs_capture.py
  python3 load_corpus_roofs_capture.py --file /path/to/corpus_roofs.json --dry-run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("pip install psycopg2-binary", file=sys.stderr)
    raise

DEFAULT_FILE = "/mnt/win11/Fedora/free_data/corpus_permits/corpus_roofs_18mo.json"
SOURCE = "infor_corpuschristi"
JURISDICTION = "Corpus Christi, TX"

INSERT_SQL = """
INSERT INTO hot_leads (
    id, permit_number, permit_type, work_class, description,
    address, city, state, zip, county, issue_date, status,
    jurisdiction, source
) VALUES %s
ON CONFLICT (permit_number, source) DO NOTHING
"""


def load_rows(path: str):
    raw = Path(path).read_text()
    obj = json.loads(raw)
    if isinstance(obj, str):  # double-encoded
        obj = json.loads(obj)
    return obj["rows"] if isinstance(obj, dict) and "rows" in obj else obj


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=DEFAULT_FILE)
    ap.add_argument("--host", default=os.environ.get("PGHOST", "100.122.216.15"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "5432")))
    ap.add_argument("--db", default=os.environ.get("PGDATABASE", "permits"))
    ap.add_argument("--user", default=os.environ.get("PGUSER", "will"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rows = load_rows(args.file)
    print(f"[load] {len(rows)} Corpus roof rows from {args.file}", flush=True)

    values = []
    for r in rows:
        pn = (r.get("pn") or "").strip()
        addr = (r.get("addr") or "").strip()
        if not pn or not addr:
            continue
        values.append((
            str(uuid.uuid4()),
            pn[:200],
            "Residential Permit Application",   # permit_type (Corpus RES)
            (r.get("work") or "")[:200] or None,   # work_class = workTypeDescription
            "roof-signal residential permit (Corpus has no distinct re-roof type)",
            addr[:300],
            "CORPUS CHRISTI",
            "TX",
            (r.get("zip") or None),
            "Nueces",
            (r.get("issued") or None),   # issue_date (may be NULL)
            (r.get("status") or "")[:100] or None,
            JURISDICTION,
            SOURCE,
        ))

    print(f"[load] {len(values)} rows ready (have pn+address)", flush=True)
    if args.dry_run:
        print("[load] dry-run; no writes", flush=True)
        return

    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.db,
                            user=args.user, connect_timeout=20)
    try:
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("SET statement_timeout='60s'")
        cur.execute("SET lock_timeout='10s'")
        execute_values(cur, INSERT_SQL, values, page_size=500)
        n = cur.rowcount
        conn.commit()
        print(f"[load] inserted {n} (ON CONFLICT skips dupes) into hot_leads "
              f"(source={SOURCE})", flush=True)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
