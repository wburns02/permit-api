"""
Load NYC DOB jobs + NYC PLUTO from R730 staging CSVs into the permits Postgres.

Source files (on R730 at 100.85.99.69):
  /mnt/data/staging/nyc_dob_jobs_fixed.csv
  /mnt/data/staging/nyc_pluto_fixed.csv

Both CSVs are pre-cleaned. Headers:
  DOB:   address,city,state,zip,county,land_use
  PLUTO: address,city,state,zip,county,owner_name,land_use,year_built,lot_size_sqft,lat,lng

Outputs:
  - DOB rows → permits (partition permits_ny) with source='nyc_dob_jobs', state_code='NY'
  - PLUTO rows → new nyc_pluto table

Idempotent: on re-run, deletes existing rows with source='nyc_dob_jobs' first; nyc_pluto
table is TRUNCATEd before reload.

Usage:
  python scripts/load_nyc_dob_from_staging.py \\
      --db postgresql://will@100.122.216.15:5432/permits \\
      --dob /mnt/data/staging/nyc_dob_jobs_fixed.csv \\
      --pluto /mnt/data/staging/nyc_pluto_fixed.csv

If --db is omitted, reads DATABASE_URL from env (strips +asyncpg). Run from a host that
can both read the CSVs and reach the DB (e.g. directly on R730 100.85.99.69).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import psycopg2


DOB_SOURCE = "nyc_dob_jobs"
DOB_SOURCE_FILE = "nyc_dob_jobs_fixed.csv"
PLUTO_SOURCE_FILE = "nyc_pluto_fixed.csv"


PLUTO_DDL = """
CREATE TABLE IF NOT EXISTS nyc_pluto (
    id           BIGSERIAL PRIMARY KEY,
    address      TEXT,
    city         TEXT,
    state        CHAR(2),
    zip_code     TEXT,
    county       TEXT,
    owner_name   TEXT,
    land_use     TEXT,
    year_built   INTEGER,
    lot_size_sqft NUMERIC,
    lat          DOUBLE PRECISION,
    lng          DOUBLE PRECISION,
    loaded_at    TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_nyc_pluto_zip ON nyc_pluto (zip_code);
CREATE INDEX IF NOT EXISTS ix_nyc_pluto_geo ON nyc_pluto USING gist (point(lng, lat));
"""


def get_db_url(arg_url: str | None) -> str:
    url = arg_url or os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("ERROR: pass --db or set DATABASE_URL")
    return url.replace("postgresql+asyncpg://", "postgresql://")


def load_dob(conn, dob_path: Path, cleanup: bool = True) -> int:
    """Load DOB jobs CSV into permits_ny."""
    print(f"[DOB] streaming {dob_path} ...", flush=True)
    t0 = time.time()
    with conn.cursor() as cur:
        # idempotent: drop prior load. Skip with --no-cleanup on first load
        # (avoids a full seq scan on the 10GB+ permits_ny partition).
        if cleanup:
            print(f"[DOB] DELETE FROM permits WHERE source={DOB_SOURCE} ...", flush=True)
            cur.execute(
                "DELETE FROM permits WHERE state_code = 'NY' AND source = %s",
                (DOB_SOURCE,),
            )
            print(f"[DOB] deleted {cur.rowcount:,} prior rows", flush=True)
        else:
            print(f"[DOB] --no-cleanup: skipping prior-row delete", flush=True)

        # staging temp table that matches CSV
        cur.execute("""
            CREATE TEMP TABLE nyc_dob_raw (
                address  TEXT,
                city     TEXT,
                state    TEXT,
                zip      TEXT,
                county   TEXT,
                land_use TEXT
            ) ON COMMIT DROP;
        """)

        with open(dob_path, "rb") as f:
            cur.copy_expert(
                "COPY nyc_dob_raw FROM STDIN WITH (FORMAT csv, HEADER true)",
                f,
            )
        cur.execute("SELECT COUNT(*) FROM nyc_dob_raw")
        raw_rows = cur.fetchone()[0]
        print(f"[DOB] staged {raw_rows:,} rows in {time.time()-t0:.1f}s")

        # insert into partition table (parent so partition routing works)
        t1 = time.time()
        cur.execute("""
            INSERT INTO permits (
                address, city, state_code, zip_code, county, category,
                source, source_file, loaded_at
            )
            SELECT
                NULLIF(address, ''),
                NULLIF(city, ''),
                'NY'::char(2),
                NULLIF(zip, ''),
                NULLIF(county, ''),
                NULLIF(land_use, ''),
                %s,
                %s,
                NOW()
            FROM nyc_dob_raw;
        """, (DOB_SOURCE, DOB_SOURCE_FILE))
        inserted = cur.rowcount
        print(f"[DOB] inserted {inserted:,} rows into permits in {time.time()-t1:.1f}s")
    conn.commit()
    return inserted


def load_pluto(conn, pluto_path: Path) -> int:
    """Load PLUTO CSV into nyc_pluto."""
    print(f"[PLUTO] streaming {pluto_path} ...")
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(PLUTO_DDL)
        cur.execute("TRUNCATE nyc_pluto RESTART IDENTITY;")
        # staging temp table
        cur.execute("""
            CREATE TEMP TABLE pluto_raw (
                address       TEXT,
                city          TEXT,
                state         TEXT,
                zip           TEXT,
                county        TEXT,
                owner_name    TEXT,
                land_use      TEXT,
                year_built    TEXT,
                lot_size_sqft TEXT,
                lat           TEXT,
                lng           TEXT
            ) ON COMMIT DROP;
        """)
        with open(pluto_path, "rb") as f:
            cur.copy_expert(
                "COPY pluto_raw FROM STDIN WITH (FORMAT csv, HEADER true)",
                f,
            )
        cur.execute("SELECT COUNT(*) FROM pluto_raw")
        raw_rows = cur.fetchone()[0]
        print(f"[PLUTO] staged {raw_rows:,} rows in {time.time()-t0:.1f}s")

        t1 = time.time()
        cur.execute("""
            INSERT INTO nyc_pluto (
                address, city, state, zip_code, county, owner_name, land_use,
                year_built, lot_size_sqft, lat, lng
            )
            SELECT
                NULLIF(address, ''),
                NULLIF(city, ''),
                'NY'::char(2),
                NULLIF(zip, ''),
                NULLIF(county, ''),
                NULLIF(owner_name, ''),
                NULLIF(land_use, ''),
                NULLIF(year_built, '')::INTEGER,
                NULLIF(lot_size_sqft, '')::NUMERIC,
                NULLIF(lat, '')::DOUBLE PRECISION,
                NULLIF(lng, '')::DOUBLE PRECISION
            FROM pluto_raw;
        """)
        inserted = cur.rowcount
        print(f"[PLUTO] inserted {inserted:,} rows into nyc_pluto in {time.time()-t1:.1f}s")
    conn.commit()
    return inserted


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", help="Postgres DSN (overrides DATABASE_URL)")
    ap.add_argument("--dob", default="/mnt/data/staging/nyc_dob_jobs_fixed.csv")
    ap.add_argument("--pluto", default="/mnt/data/staging/nyc_pluto_fixed.csv")
    ap.add_argument("--skip-dob", action="store_true")
    ap.add_argument("--skip-pluto", action="store_true")
    ap.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Skip the DELETE-prior-rows step (use on first load only)",
    )
    args = ap.parse_args()

    dsn = get_db_url(args.db)
    print(f"connecting to {dsn.split('@')[-1]} ...")
    conn = psycopg2.connect(dsn)

    dob_n = pluto_n = 0
    if not args.skip_dob:
        dob_n = load_dob(conn, Path(args.dob), cleanup=not args.no_cleanup)
    if not args.skip_pluto:
        pluto_n = load_pluto(conn, Path(args.pluto))

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM permits_ny")
        ny_total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM nyc_pluto")
        pluto_total = cur.fetchone()[0]

    print(f"\nSUMMARY:")
    print(f"  DOB rows inserted:       {dob_n:,}")
    print(f"  PLUTO rows inserted:     {pluto_n:,}")
    print(f"  permits_ny total now:    {ny_total:,}")
    print(f"  nyc_pluto total now:     {pluto_total:,}")
    conn.close()


if __name__ == "__main__":
    main()
