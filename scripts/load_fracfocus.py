#!/usr/bin/env python3
"""Load the FracFocus full-registry CSV export into fracfocus.registry
(ingredient-level) and fracfocus.disclosures (disclosure-level).

Source: https://www.fracfocusdata.org/digitaldownload/FracFocusCSV.zip
(linked from https://fracfocus.org/data-download). Zip contains:
  DisclosureList_1.csv      one row per disclosure (~250K)
  FracFocusRegistry_*.csv   one row per disclosure-ingredient (~9M, 15 shards)
  WaterSource_1.csv         water-source detail (not loaded)
  readme csv.txt            data dictionary

Full reload each run (TRUNCATE + COPY); the export is a complete snapshot.

API normalization: FracFocus APINumber is 14-digit (sometimes dashed
xx-xxx-xxxxx-xx-xx). We strip non-digits, derive api10 = first 10 digits,
api14 = digits right-padded with '0' to 14, and keep api_raw verbatim.

Known dirt: header row repeated per shard; bogus job dates (e.g. 1955 frack
jobs) kept but dates outside 1990..today+1y are nulled; empty-string
numerics; NUL bytes stripped; embedded tabs/newlines escaped for COPY.

Usage: python3 load_fracfocus.py [--dir /mnt/win11/Fedora/raw-public-data/fracfocus/csv]
"""
import argparse
import csv
import glob
import io
import os
import re
import sys
import time
from datetime import date, datetime

import psycopg2

DSN = "host=100.122.216.15 port=5432 dbname=permits user=will"
DEFAULT_DIR = "/mnt/win11/Fedora/raw-public-data/fracfocus/csv"

csv.field_size_limit(10_000_000)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def parse_dt(v):
    """'5/1/2014 12:00:00 AM' -> ISO date or None. Guard absurd years."""
    v = (v or "").strip()
    if not v:
        return None
    try:
        dt = datetime.strptime(v.split(" ")[0], "%m/%d/%Y").date()
    except ValueError:
        return None
    if dt.year < 1990 or dt > date(date.today().year + 1, 12, 31):
        return None
    return dt.isoformat()


def num(v):
    v = (v or "").strip()
    if not v:
        return None
    try:
        float(v)
    except ValueError:
        return None
    return v


def boo(v):
    v = (v or "").strip().lower()
    return "t" if v == "true" else ("f" if v == "false" else None)


def latlon(v, lo, hi):
    n = num(v)
    if n is None:
        return None
    f = float(n)
    return n if lo <= f <= hi else None


def api_parts(raw):
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) < 10:
        return None, None
    return digits.ljust(14, "0")[:14], digits[:10]


def esc(v):
    if v is None or v == "":
        return "\\N"
    return (
        v.replace("\x00", "")
        .replace("\\", "\\\\")
        .replace("\t", " ")
        .replace("\n", " ")
        .replace("\r", " ")
    )


DDL = """
CREATE SCHEMA IF NOT EXISTS fracfocus;

DROP TABLE IF EXISTS fracfocus.registry;
CREATE TABLE fracfocus.registry (
    disclosure_id text,
    job_start_date date,
    job_end_date date,
    api_raw text,
    api14 text,
    api10 text,
    state_name text,
    county_name text,
    operator_name text,
    well_name text,
    latitude double precision,
    longitude double precision,
    projection text,
    tvd numeric,
    total_base_water_volume numeric,
    total_base_nonwater_volume numeric,
    ff_version text,
    federal_well boolean,
    indian_well boolean,
    purpose_id text,
    trade_name text,
    supplier text,
    purpose text,
    ingredients_id text,
    cas_number text,
    ingredient_name text,
    ingredient_common_name text,
    percent_high_additive numeric,
    percent_hf_job numeric,
    ingredient_comment text,
    ingredient_msds text,
    mass_ingredient numeric,
    claimant_company text
);

DROP TABLE IF EXISTS fracfocus.disclosures;
CREATE TABLE fracfocus.disclosures (
    disclosure_id text PRIMARY KEY,
    job_start_date date,
    job_end_date date,
    api_raw text,
    api14 text,
    api10 text,
    state_name text,
    county_name text,
    operator_name text,
    well_name text,
    latitude double precision,
    longitude double precision,
    projection text,
    tvd numeric,
    total_base_water_volume numeric,
    total_base_nonwater_volume numeric,
    ff_version text,
    federal_well boolean,
    indian_well boolean,
    geom geometry(Point, 4326)
);
"""


def shared_cols(row):
    """First 17 columns common to both files -> cleaned COPY values."""
    api14, api10 = api_parts(row[3])
    return [
        row[0].strip() or None,           # DisclosureId
        parse_dt(row[1]),                 # JobStartDate
        parse_dt(row[2]),                 # JobEndDate
        row[3].strip() or None,           # api_raw
        api14,
        api10,
        row[4].strip() or None,           # StateName
        row[5].strip() or None,           # CountyName
        row[6].strip() or None,           # OperatorName
        row[7].strip() or None,           # WellName
        latlon(row[8], -90, 90),          # Latitude
        latlon(row[9], -180, 180),        # Longitude
        row[10].strip() or None,          # Projection
        num(row[11]),                     # TVD
        num(row[12]),                     # TotalBaseWaterVolume
        num(row[13]),                     # TotalBaseNonWaterVolume
        row[14].strip() or None,          # FFVersion
        boo(row[15]),                     # FederalWell
        boo(row[16]),                     # IndianWell
    ]


def load_file(cur, path, table, ncols, extra):
    buf, n, skipped = [], 0, 0
    t0 = time.time()

    def flush():
        if buf:
            cur.copy_expert(f"COPY {table} FROM STDIN", io.StringIO("".join(buf)))
            buf.clear()

    with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
        r = csv.reader(f)
        next(r)  # header
        for row in r:
            if len(row) != ncols or not row[0].strip():
                skipped += 1
                continue
            vals = shared_cols(row) + extra(row)
            buf.append("\t".join(esc(v) for v in vals) + "\n")
            n += 1
            if n % 100_000 == 0:
                flush()
    flush()
    log(f"  {os.path.basename(path)}: {n:,} rows ({skipped} skipped, "
        f"{n / max(time.time() - t0, 0.01):,.0f}/s)")
    return n, skipped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=DEFAULT_DIR)
    args = ap.parse_args()

    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute(DDL)
    conn.commit()

    # --- disclosures (DisclosureList_*.csv, 17 cols) ---
    log("loading fracfocus.disclosures")
    total = 0
    for path in sorted(glob.glob(os.path.join(args.dir, "DisclosureList_*.csv"))):
        n, _ = load_file(
            cur, path,
            "fracfocus.disclosures (disclosure_id, job_start_date, job_end_date,"
            " api_raw, api14, api10, state_name, county_name, operator_name,"
            " well_name, latitude, longitude, projection, tvd,"
            " total_base_water_volume, total_base_nonwater_volume, ff_version,"
            " federal_well, indian_well)",
            17, lambda row: [])
        total += n
    cur.execute("""
        UPDATE fracfocus.disclosures
        SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
        WHERE longitude IS NOT NULL AND latitude IS NOT NULL
          AND longitude BETWEEN -180 AND -30 AND latitude BETWEEN 15 AND 75
    """)
    log(f"disclosures: {total:,} loaded, geom set on {cur.rowcount:,}")
    conn.commit()

    # --- registry (FracFocusRegistry_*.csv, 31 cols) ---
    log("loading fracfocus.registry")
    total_r = 0

    def extra(row):
        return [
            row[17].strip() or None,  # PurposeId
            row[18].strip() or None,  # TradeName
            row[19].strip() or None,  # Supplier
            row[20].strip() or None,  # Purpose
            row[21].strip() or None,  # IngredientsId
            row[22].strip() or None,  # CASNumber
            row[23].strip() or None,  # IngredientName
            row[24].strip() or None,  # IngredientCommonName
            num(row[25]),             # PercentHighAdditive
            num(row[26]),             # PercentHFJob
            row[27].strip() or None,  # IngredientComment
            row[28].strip() or None,  # IngredientMSDS
            num(row[29]),             # MassIngredient
            row[30].strip() or None,  # ClaimantCompany
        ]

    files = sorted(glob.glob(os.path.join(args.dir, "FracFocusRegistry_*.csv")),
                   key=lambda p: int(re.search(r"_(\d+)\.csv$", p).group(1)))
    for path in files:
        n, _ = load_file(cur, path, "fracfocus.registry", 31, extra)
        total_r += n
        conn.commit()
    log(f"registry: {total_r:,} loaded")

    log("building indexes")
    cur.execute("CREATE INDEX ON fracfocus.disclosures (api10)")
    cur.execute("CREATE INDEX ON fracfocus.disclosures (state_name)")
    cur.execute("CREATE INDEX ON fracfocus.disclosures USING gist (geom)")
    cur.execute("CREATE INDEX ON fracfocus.registry (disclosure_id)")
    cur.execute("CREATE INDEX ON fracfocus.registry (api10)")
    cur.execute("ANALYZE fracfocus.disclosures; ANALYZE fracfocus.registry")
    conn.commit()
    conn.close()
    log("done")


if __name__ == "__main__":
    sys.exit(main())
