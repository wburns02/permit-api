#!/usr/bin/env python3
"""Load Mississippi MARIS / MDEQ parcel shapefiles into ms_parcels.

Source: /mnt/data/staging/ms_maris_parcels/{east,west}/MDEQ_PARCEL_POLY_*.shp
Shapefile contains ~1.99M polygon parcels statewide (Aug 2023 snapshot).

Strategy:
  * Read DBF + SHP via pyshp (no GDAL dependency).
  * Polygons are in State Plane (East 2301 / West 2302). We do NOT reproject
    the polygon here; the file already exposes WGS84 centroid LATDEC/LONGDEC
    which we store as a POINT geography. Polygon reprojection can be added
    later via PostGIS ST_Transform if needed.
  * COPY into a TEMP staging table per region, then INSERT ... ON CONFLICT
    DO NOTHING into ms_parcels keyed on (stcntyfips, parno).
  * Idempotent: if ms_parcels already has > 1M rows from ms_maris, skip.

Run on R730 (where the staging data lives) or anywhere with DB + file access.

    python3 load_ms_maris_parcels.py [--force] [--region east|west|both]
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import date
from pathlib import Path

import psycopg2
import shapefile  # pyshp

STAGING_ROOT = Path("/mnt/data/staging/ms_maris_parcels")
REGIONS = {
    "east": STAGING_ROOT / "east" / "MDEQ_PARCEL_POLY_EAST",
    "west": STAGING_ROOT / "west" / "MDEQ_PARCEL_POLY_WEST",
}

# Field map: shapefile DBF -> ms_parcels column.
FIELD_MAP = [
    ("CAMA",        "cama_vendor"),
    ("PARNO",       "parcel_id"),
    ("ALTPARNO",    "alt_parcel_id"),
    ("PPIN",        "ppin"),
    ("OWNNAME",     "owner_name"),
    ("MAILADD1",    "mailing_address"),
    ("MCITY1",      "mailing_city"),
    ("MSTATE1",     "mailing_state"),
    ("MZIP1",       "mailing_zip"),
    ("SITEADD",     "address"),
    ("SCITY",       "city"),
    ("SSTATE",      "state_code"),
    ("SZIP",        "zip_code"),
    ("SUBNAME",     "subdivision"),
    ("SUBDIVNO",    "subdivision_no"),
    ("TAXACRES",    "tax_acres"),
    ("GISACRES",    "gis_acres"),
    ("DEEDREF",     "deed_ref"),
    ("DEEDDATE",    "deed_date_raw"),
    ("PLATREF",     "plat_ref"),
    ("PLATDATE",    "plat_date"),
    ("TAXMAP",      "tax_map"),
    ("SECTION",     "section"),
    ("TWSP",        "township"),
    ("RANGE",       "range_"),
    ("TAXSTATUS",   "tax_status"),
    ("CNTYNAME",    "county"),
    ("CNTYFIPS",    "cnty_fips"),
    ("STCNTYFIPS",  "stcnty_fips"),
    ("LANDVAL",     "land_value"),
    ("IMPVAL1",     "improvement_value"),
    ("IMPVAL2",     "improvement_value_2"),
    ("TOTVAL",      "total_value"),
    ("TOTAL_AC",    "total_acres"),
    ("LATDEC",      "lat"),
    ("LONGDEC",     "lng"),
    ("ZONING",      "zoning"),
    ("LEGLDESC",    "legal_description"),
    ("TAXYEAR",     "tax_year"),
]

DDL = """
CREATE TABLE IF NOT EXISTS ms_parcels (
    id                  BIGSERIAL PRIMARY KEY,
    parcel_id           TEXT,
    alt_parcel_id       TEXT,
    ppin                TEXT,
    cama_vendor         TEXT,
    owner_name          TEXT,
    address             TEXT,
    city                TEXT,
    state_code          CHAR(2) DEFAULT 'MS',
    zip_code            TEXT,
    mailing_address     TEXT,
    mailing_city        TEXT,
    mailing_state       TEXT,
    mailing_zip         TEXT,
    subdivision         TEXT,
    subdivision_no      TEXT,
    legal_description   TEXT,
    county              TEXT,
    cnty_fips           TEXT,
    stcnty_fips         TEXT,
    tax_acres           NUMERIC,
    gis_acres           NUMERIC,
    total_acres         NUMERIC,
    deed_ref            TEXT,
    deed_date_raw       TEXT,
    plat_ref            TEXT,
    plat_date           DATE,
    tax_map             TEXT,
    section             TEXT,
    township            TEXT,
    range_              TEXT,
    tax_status          TEXT,
    land_value          NUMERIC,
    improvement_value   NUMERIC,
    improvement_value_2 NUMERIC,
    total_value         NUMERIC,
    tax_year            TEXT,
    zoning              TEXT,
    lat                 DOUBLE PRECISION,
    lng                 DOUBLE PRECISION,
    geom_point          GEOGRAPHY(Point, 4326),
    source              TEXT DEFAULT 'ms_maris',
    region              TEXT,
    loaded_at           TIMESTAMPTZ DEFAULT NOW(),
    raw_data            JSONB,
    CONSTRAINT ms_parcels_natural_key UNIQUE (stcnty_fips, parcel_id, alt_parcel_id)
);

CREATE INDEX IF NOT EXISTS ms_parcels_county_idx     ON ms_parcels (county);
CREATE INDEX IF NOT EXISTS ms_parcels_parcel_id_idx  ON ms_parcels (parcel_id);
CREATE INDEX IF NOT EXISTS ms_parcels_owner_trgm_idx ON ms_parcels USING gin (owner_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS ms_parcels_geom_gix       ON ms_parcels USING gist (geom_point);
CREATE INDEX IF NOT EXISTS ms_parcels_zip_idx        ON ms_parcels (zip_code);

CREATE EXTENSION IF NOT EXISTS pg_trgm;
"""

# Order matches the COPY column list.
COPY_COLUMNS = [c for _, c in FIELD_MAP] + ["region", "raw_data"]


def parse_plat(v):
    """PLATDATE comes through pyshp as datetime.date or None."""
    if v is None:
        return r"\N"
    if isinstance(v, date):
        return v.isoformat()
    s = str(v).strip()
    return s if s else r"\N"


def fmt(v):
    import math
    if v is None:
        return r"\N"
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return r"\N"
        # pyshp coerces empty Numeric to 0.0; keep as-is.
        return repr(v)
    if isinstance(v, int):
        return str(v)
    if isinstance(v, date):
        return v.isoformat()
    s = str(v).strip()
    if not s:
        return r"\N"
    # Escape backslash, tab, newline for COPY text format.
    return s.replace("\\", "\\\\").replace("\t", " ").replace("\n", " ").replace("\r", " ")


def db_url() -> str:
    if "DATABASE_URL" in os.environ:
        return os.environ["DATABASE_URL"]
    # Read /etc/permit-api.env on R730.
    p = Path("/etc/permit-api.env")
    if p.exists():
        for line in p.read_text().splitlines():
            if line.startswith("DATABASE_URL="):
                u = line.split("=", 1)[1].strip()
                # psycopg2 wants postgresql://, not postgresql+asyncpg://
                return u.replace("postgresql+asyncpg://", "postgresql://")
    raise RuntimeError("DATABASE_URL not found")


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
        cur.execute(DDL)
    conn.commit()


def region_loaded(conn, region: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM ms_parcels WHERE source='ms_maris' AND region=%s;",
            (region,),
        )
        n = cur.fetchone()[0]
    # East has ~1.12M, West has ~875k; either region has >500k means it's already loaded.
    return n > 500_000


def already_loaded(conn) -> bool:
    # Whole-load guard: both regions present.
    return region_loaded(conn, "east") and region_loaded(conn, "west")


def load_region(conn, region: str, base: Path, batch: int = 50_000) -> int:
    """Stream-read shapefile records and COPY into ms_parcels via a temp staging table."""
    print(f"[{region}] opening {base}", flush=True)
    reader = shapefile.Reader(str(base))
    total = len(reader)
    print(f"[{region}] {total:,} records", flush=True)

    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS ms_raw_stage;")
    cur.execute(f"""
        CREATE UNLOGGED TABLE ms_raw_stage (
            {', '.join(f'{col} TEXT' for col in COPY_COLUMNS)}
        );
    """)

    # Write to a UNIX pipe via COPY FROM STDIN.
    import io
    buf = io.StringIO()
    written = 0
    t0 = time.time()
    rec_iter = reader.iterRecords()
    for rec in rec_iter:
        d = rec.as_dict()
        cols = []
        for src, _ in FIELD_MAP:
            v = d.get(src)
            if src == "PLATDATE":
                cols.append(parse_plat(v))
            else:
                cols.append(fmt(v))
        cols.append(region)
        # Sanitize NaN/Inf: PG JSONB rejects them. Replace with None.
        import math
        safe_d = {}
        for k, v in d.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                safe_d[k] = None
            elif isinstance(v, date):
                safe_d[k] = v.isoformat()
            else:
                safe_d[k] = v
        cols.append(fmt(json.dumps(safe_d, default=str, allow_nan=False)))
        buf.write("\t".join(cols))
        buf.write("\n")
        written += 1
        if written % batch == 0:
            buf.seek(0)
            cur.copy_expert(
                f"COPY ms_raw_stage ({', '.join(COPY_COLUMNS)}) FROM STDIN WITH (FORMAT text)",
                buf,
            )
            conn.commit()
            rate = written / max(1, time.time() - t0)
            print(f"[{region}] staged {written:,}/{total:,} ({rate:,.0f}/s)", flush=True)
            buf = io.StringIO()
    if buf.tell() > 0:
        buf.seek(0)
        cur.copy_expert(
            f"COPY ms_raw_stage ({', '.join(COPY_COLUMNS)}) FROM STDIN WITH (FORMAT text)",
            buf,
        )
        conn.commit()
    reader.close()
    print(f"[{region}] all {written:,} rows staged in {time.time()-t0:.0f}s", flush=True)

    # Insert into ms_parcels with type coercion and geometry construction.
    print(f"[{region}] inserting into ms_parcels...", flush=True)
    t0 = time.time()
    cur.execute("""
        INSERT INTO ms_parcels (
            parcel_id, alt_parcel_id, ppin, cama_vendor, owner_name,
            address, city, state_code, zip_code,
            mailing_address, mailing_city, mailing_state, mailing_zip,
            subdivision, subdivision_no, legal_description,
            county, cnty_fips, stcnty_fips,
            tax_acres, gis_acres, total_acres,
            deed_ref, deed_date_raw, plat_ref, plat_date,
            tax_map, section, township, range_,
            tax_status,
            land_value, improvement_value, improvement_value_2, total_value,
            tax_year, zoning,
            lat, lng, geom_point,
            region, raw_data, source
        )
        SELECT
            NULLIF(parcel_id,''),
            NULLIF(alt_parcel_id,''),
            NULLIF(ppin,''),
            NULLIF(cama_vendor,''),
            NULLIF(owner_name,''),
            NULLIF(address,''),
            NULLIF(city,''),
            COALESCE(NULLIF(state_code,''), 'MS'),
            NULLIF(zip_code,''),
            NULLIF(mailing_address,''),
            NULLIF(mailing_city,''),
            NULLIF(mailing_state,''),
            NULLIF(mailing_zip,''),
            NULLIF(subdivision,''),
            NULLIF(subdivision_no,''),
            NULLIF(legal_description,''),
            NULLIF(county,''),
            NULLIF(cnty_fips,''),
            NULLIF(stcnty_fips,''),
            NULLIF(tax_acres,'')::NUMERIC,
            NULLIF(gis_acres,'')::NUMERIC,
            NULLIF(total_acres,'')::NUMERIC,
            NULLIF(deed_ref,''),
            NULLIF(deed_date_raw,''),
            NULLIF(plat_ref,''),
            CASE WHEN plat_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN plat_date::date ELSE NULL END,
            NULLIF(tax_map,''),
            NULLIF(section,''),
            NULLIF(township,''),
            NULLIF(range_,''),
            NULLIF(tax_status,''),
            NULLIF(land_value,'')::NUMERIC,
            NULLIF(improvement_value,'')::NUMERIC,
            NULLIF(improvement_value_2,'')::NUMERIC,
            NULLIF(total_value,'')::NUMERIC,
            NULLIF(tax_year,''),
            NULLIF(zoning,''),
            NULLIF(lat,'')::DOUBLE PRECISION,
            NULLIF(lng,'')::DOUBLE PRECISION,
            CASE
                WHEN NULLIF(lat,'') IS NOT NULL AND NULLIF(lng,'') IS NOT NULL
                  AND lat::double precision BETWEEN 29 AND 36
                  AND lng::double precision BETWEEN -92 AND -87
                THEN ST_SetSRID(ST_MakePoint(lng::double precision, lat::double precision), 4326)::geography
                ELSE NULL
            END,
            region,
            NULLIF(raw_data,'')::jsonb,
            'ms_maris'
        FROM ms_raw_stage
        ON CONFLICT ON CONSTRAINT ms_parcels_natural_key DO NOTHING;
    """)
    inserted = cur.rowcount
    conn.commit()
    print(f"[{region}] inserted {inserted:,} rows in {time.time()-t0:.0f}s", flush=True)
    cur.execute("DROP TABLE IF EXISTS ms_raw_stage;")
    conn.commit()
    return inserted


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", choices=["east", "west", "both"], default="both")
    ap.add_argument("--force", action="store_true",
                    help="Load even if ms_parcels already has data.")
    args = ap.parse_args()

    if not STAGING_ROOT.exists():
        print(f"[fatal] staging dir missing: {STAGING_ROOT}", file=sys.stderr)
        sys.exit(2)

    conn = psycopg2.connect(db_url())
    conn.autocommit = False
    print("Connected.", flush=True)
    ensure_schema(conn)
    print("Schema ensured.", flush=True)

    if already_loaded(conn) and not args.force:
        print("ms_parcels already populated (>1M rows). Use --force to reload.", flush=True)
        return

    regions = [args.region] if args.region != "both" else ["east", "west"]
    grand_total = 0
    for r in regions:
        base = REGIONS[r]
        if not Path(str(base) + ".shp").exists():
            print(f"[skip] {r} shp missing", flush=True)
            continue
        if region_loaded(conn, r) and not args.force:
            print(f"[skip] {r} already loaded ({r} present in ms_parcels)", flush=True)
            continue
        grand_total += load_region(conn, r, base)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM ms_parcels;")
        n = cur.fetchone()[0]
        cur.execute("ANALYZE ms_parcels;")
    conn.commit()
    print(f"DONE. Total rows in ms_parcels: {n:,}", flush=True)


if __name__ == "__main__":
    main()
