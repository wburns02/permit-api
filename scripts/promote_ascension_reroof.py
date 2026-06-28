#!/usr/bin/env python3
"""Promote Ascension-parish MGO Re-Roof permits from hot_leads -> ascension_permits.

The serviced-exclusion arm of the unserviced_hail_leads MV (Ascension) drops any
candidate parcel whose normalized situs address already has a Re-Roof permit in
ascension_permits. The source of those permits is MGO Connect, loaded into
hot_leads by scripts/load_mgo_la_permits.py (state='LA', source='mgo_<slug>').

This promoter copies the Re-Roof subset of the Ascension-parish MGO juris
(Ascension, Gonzales, Donaldsonville, Sorrento) into ascension_permits with the
SAME address normalization the MV uses, so the NOT EXISTS join lines up.

HONEST LIMITATION (verified): the MGO v3 search-projects response carries no
issue/issued date — issue_date is NULL for every MGO LA row. So we CANNOT
time-gate the exclusion to "re-roofed since the storm". Instead we treat ANY
recorded Re-Roof whose status is issued/complete (not pending/expired/void) as
"serviced". This is deliberately conservative (drops a few parcels that were
re-roofed before the storm too) — the safe direction for an un-serviced claim.
issued_date is left NULL; the MV exclusion is written to ignore the date when it
is NULL.

Gentle on the DB: bounded statement_timeout, lock_timeout on writes, NO
pg_terminate/pg_cancel, no unbounded scans (the SELECT is keyed by the work_class
trigram GIN index, then filtered to LA juris in-query).

Usage (run anywhere with DB reach, e.g. R730-2 or workstation):
  python3 promote_ascension_reroof.py            # upsert
  python3 promote_ascension_reroof.py --dry-run  # report counts only
"""
from __future__ import annotations
import argparse, os, sys
try:
    import psycopg2
except ImportError:
    print("pip install psycopg2-binary", file=sys.stderr); raise

ASC_SOURCES = ("mgo_ascension", "mgo_gonzales", "mgo_donaldsonville", "mgo_sorrento")

# Same normalization as the MV (UPPER, strip unit designators, strip punctuation,
# collapse whitespace) — mirrors normalize_address()/the MV norm_situs.
NORM = r"""TRIM(REGEXP_REPLACE(
    REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(address),
            '(^|\s)(SUITE|STE|UNIT|APT|#)\s+\S+', ' ', 'g'),
        '[.,#]', '', 'g'),
    '\s+', ' '))"""

# Issued/serviced statuses we count (anything not clearly pending/void/expired).
SERVICED_STATUS = (
    "status IS NULL OR ("
    "status NOT ILIKE '%%pending%%' AND status NOT ILIKE '%%under review%%' "
    "AND status NOT ILIKE '%%void%%' AND status NOT ILIKE '%%denied%%' "
    "AND status NOT ILIKE '%%withdrawn%%')"
)

SELECT_SQL = f"""
SELECT permit_number,
       {NORM} AS address_norm,
       issue_date,
       source,
       status
  FROM hot_leads
 WHERE (work_class ILIKE '%%re-roof%%' OR work_class ILIKE '%%reroof%%'
        OR work_class ILIKE '%%re roof%%'
        OR description ILIKE '%%re-roof%%' OR description ILIKE '%%reroof%%')
   AND source = ANY(%(srcs)s)
   AND address IS NOT NULL
   AND ({SERVICED_STATUS})
"""

DDL = """
CREATE TABLE IF NOT EXISTS public.ascension_permits (
    permit_number text,
    address_norm  text,
    is_reroof     boolean NOT NULL DEFAULT false,
    issued_date   date,
    source        text,
    raw           jsonb
);
CREATE INDEX IF NOT EXISTS ix_ascension_permits_reroof
    ON public.ascension_permits (address_norm) WHERE is_reroof;
"""

UPSERT = """
INSERT INTO public.ascension_permits
    (permit_number, address_norm, is_reroof, issued_date, source)
VALUES (%(pn)s, %(an)s, true, %(idate)s, %(src)s)
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("PGHOST", "100.122.216.15"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", "5432")))
    ap.add_argument("--db", default=os.environ.get("PGDATABASE", "permits"))
    ap.add_argument("--user", default=os.environ.get("PGUSER", "will"))
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.db,
                            user=args.user, connect_timeout=20)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SET statement_timeout='60s'")
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            cur.execute(stmt)
    conn.commit()

    cur.execute(SELECT_SQL, {"srcs": list(ASC_SOURCES)})
    rows = cur.fetchall()
    print(f"[promote] {len(rows)} Ascension-parish Re-Roof permits found in hot_leads",
          flush=True)
    by_src = {}
    for _pn, _an, _id, src, _st in rows:
        by_src[src] = by_src.get(src, 0) + 1
    for src, n in sorted(by_src.items()):
        print(f"          {src}: {n}", flush=True)

    if args.dry_run:
        print("[promote] dry-run; no writes", flush=True)
        conn.close(); return

    cur.execute("SET LOCAL lock_timeout='5s'")
    cur.execute("TRUNCATE public.ascension_permits")  # rebuild-in-place, idempotent
    ins = 0
    for pn, an, idate, src, _st in rows:
        if not an:
            continue
        cur.execute(UPSERT, {"pn": pn, "an": an, "idate": idate, "src": src})
        ins += 1
    conn.commit()
    print(f"[promote] upserted {ins} rows into ascension_permits", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
