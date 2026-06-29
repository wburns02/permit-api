#!/usr/bin/env python3
"""Promote Nueces-County (Corpus Christi + Port Aransas) Re-Roof permits from
hot_leads -> nueces_permits.

The Nueces arm of the unserviced_hail_leads MV drops any storm-hit parcel whose
normalized situs address already has a Re-Roof permit in nueces_permits (the
serviced-exclusion that upgrades Corpus from "storm-zone" to VERIFIED
un-serviced, mirroring the EBR arm).

Sources of those permits, both already in hot_leads:
  - source='infor_corpuschristi'  (City of Corpus Christi Infor/CIVICS portal,
                                    scripts/scrape_corpus_permits.py — carries a
                                    real issued_date for ~80% of rows)
  - source='opengov_portaransastx' (Port Aransas OpenGov, scripts/scrape_opengov.py
                                    — the 78373 beach homes; issue_date present)

Re-roof detection: roof signal in permit_type / work_class / description. Corpus
re-roofs file as "Residential Permit Application" with the roof keyword in
work_class (workTypeDescription) or description (comments); OpenGov tags the
permit_type. We keep ANY roof-keyword permit.

Issue date: Corpus carries a real issued/applied date; Port Aransas (OpenGov
dateCreated) carries issue_date. We store it. The MV exclusion time-gates to
"re-roofed at/after the matched storm" when issued_date IS present, and treats a
NULL date as "any recorded re-roof = serviced" (conservative, the safe direction
for an un-serviced claim — identical to the Ascension arm).

Address normalization mirrors the MV norm_situs / normalize_address() EXACTLY so
the NOT EXISTS join lines up (UPPER, strip unit designators, strip [.,#],
collapse whitespace).

Gentle on the DB: bounded statement_timeout, lock_timeout on writes, NO
pg_terminate/pg_cancel. The SELECT is keyed by the source btree index
(ix_hot_leads_source) then filtered, never a full scan.

Usage (run anywhere with DB reach):
  python3 promote_nueces_reroof.py            # rebuild nueces_permits in place
  python3 promote_nueces_reroof.py --dry-run  # report counts only
"""
from __future__ import annotations

import argparse
import os
import sys

try:
    import psycopg2
except ImportError:
    print("pip install psycopg2-binary", file=sys.stderr)
    raise

NUECES_SOURCES = ("infor_corpuschristi", "opengov_portaransastx")

# Same normalization as the MV norm_situs (see app/main.py Nueces arm).
NORM = r"""TRIM(REGEXP_REPLACE(
    REGEXP_REPLACE(
        REGEXP_REPLACE(UPPER(address),
            '(^|\s)(SUITE|STE|UNIT|APT|#)\s+\S+', ' ', 'g'),
        '[.,#]', '', 'g'),
    '\s+', ' '))"""

# Roof keyword over the three free-text columns.
ROOF_PRED = (
    "(permit_type ILIKE '%%roof%%' OR work_class ILIKE '%%roof%%' "
    "OR description ILIKE '%%roof%%' OR permit_type ILIKE '%%reroof%%' "
    "OR work_class ILIKE '%%reroof%%' OR description ILIKE '%%shingle%%' "
    "OR work_class ILIKE '%%shingle%%')"
)

# Drop clearly-not-serviced statuses (pending / void / denied / withdrawn /
# deleted). Anything else (issued, finalized, open, NULL) counts as serviced.
SERVICED_STATUS = (
    "(status IS NULL OR ("
    "status NOT ILIKE '%%pending%%' AND status NOT ILIKE '%%under review%%' "
    "AND status NOT ILIKE '%%void%%' AND status NOT ILIKE '%%denied%%' "
    "AND status NOT ILIKE '%%withdrawn%%' AND status NOT ILIKE '%%deleted%%' "
    "AND status NOT ILIKE '%%customer input%%'))"
)

SELECT_SQL = f"""
SELECT permit_number,
       {NORM} AS address_norm,
       issue_date,
       source,
       status
  FROM hot_leads
 WHERE source = ANY(%(srcs)s)
   AND address IS NOT NULL
   AND {ROOF_PRED}
   AND {SERVICED_STATUS}
"""

DDL = """
CREATE TABLE IF NOT EXISTS public.nueces_permits (
    permit_number text,
    address_norm  text,
    is_reroof     boolean NOT NULL DEFAULT false,
    issued_date   date,
    source        text,
    raw           jsonb
);
CREATE INDEX IF NOT EXISTS ix_nueces_permits_reroof
    ON public.nueces_permits (address_norm) WHERE is_reroof;
"""

UPSERT = """
INSERT INTO public.nueces_permits
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
    cur.execute("SET statement_timeout='120s'")
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            cur.execute(stmt)
    conn.commit()

    cur.execute(SELECT_SQL, {"srcs": list(NUECES_SOURCES)})
    rows = cur.fetchall()
    print(f"[promote] {len(rows)} Nueces Re-Roof permits found in hot_leads",
          flush=True)
    by_src = {}
    dated = 0
    for _pn, _an, idate, src, _st in rows:
        by_src[src] = by_src.get(src, 0) + 1
        if idate is not None:
            dated += 1
    for src, n in sorted(by_src.items()):
        print(f"          {src}: {n}", flush=True)
    print(f"          (with issue date: {dated} / {len(rows)})", flush=True)

    if args.dry_run:
        print("[promote] dry-run; no writes", flush=True)
        conn.close()
        return

    cur.execute("SET LOCAL lock_timeout='5s'")
    cur.execute("TRUNCATE public.nueces_permits")  # rebuild-in-place, idempotent
    ins = 0
    for pn, an, idate, src, _st in rows:
        if not an:
            continue
        cur.execute(UPSERT, {"pn": pn, "an": an, "idate": idate, "src": src})
        ins += 1
    conn.commit()
    print(f"[promote] upserted {ins} rows into nueces_permits", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
