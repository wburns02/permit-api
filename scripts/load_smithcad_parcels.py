#!/usr/bin/env python3
"""Load Smith County (Lindale / Tyler, TX) CAD parcels into the permits DB.

Source: FREE Smith County GIS Consortium "Tax Parcels" ArcGIS REST layer
    https://www.smithcountymapsite.org/publicgis/rest/services/Gallery/TaxParcelQuery/MapServer/1
    ~141,852 polygon parcels, pagination=True, CRS 3857 (we request outSR=4326).

Mirrors the Bexar/Travis/Harris CAD load pattern:
  * Geometry  -> smith_parcel_geometries  (geom 4326 + centroid + bbox)
  * Attributes-> tx_cad_parcels  (cad_source='SMITHCAD', county_fips='48423')

Join key: the ArcGIS ACCOUNT field (tax account number) is stored identically
as text in BOTH smith_parcel_geometries.parcel_id and tx_cad_parcels.parcel_id,
so the MV's geom.parcel_id = tx_cad_parcels.parcel_id join is ~100%.

Smith's source carries NO value column, so tx_cad_parcels.assessed_value /
market_value are loaded NULL. year_built (YRBLT) IS present (~90k of 142k).

Gentle-on-the-box rules honored:
  * lock_timeout on every write txn (default 15s).
  * NEVER full-scan: writes are keyed UPSERTs on the PK.
  * NO pg_terminate / pg_cancel anywhere.
  * Raw ArcGIS pages staged to /mnt/win11/Fedora/free_data/tx_cad/smith, NOT /home.

Usage:
    python3 scripts/load_smithcad_parcels.py            # full load
    python3 scripts/load_smithcad_parcels.py --limit 5000   # smoke test
    python3 scripts/load_smithcad_parcels.py --dry-run      # fetch only, no DB
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from shapely.geometry import shape
from shapely import wkt as shapely_wkt

LAYER = (
    "https://www.smithcountymapsite.org/publicgis/rest/services/"
    "Gallery/TaxParcelQuery/MapServer/1"
)
PAGE = 2000  # service maxRecordCount
CAD_SOURCE = "SMITHCAD"
COUNTY_FIPS = "48423"
STAGE_DIR = Path("/mnt/win11/Fedora/free_data/tx_cad/smith")

DSN_FILE = os.path.expanduser("~/.config/permitlookup/permits_dsn")

OUT_FIELDS = ",".join([
    "ACCOUNT", "ParcelID", "PIN", "ADDRESS", "CITY_COUNTY", "POSTAL_CITY",
    "ZIPCODE", "OWN1", "OWN2", "YRBLT", "SFLA", "TAXYR", "SUBDNUM",
    "ISD", "Calc_Acre", "STATUS", "Type",
])


def get_dsn() -> str:
    raw = Path(DSN_FILE).read_text().strip()
    # psycopg2 wants the standard postgresql:// scheme
    return raw.replace("postgresql+asyncpg://", "postgresql://")


def fetch_count(session: requests.Session) -> int:
    r = session.get(
        f"{LAYER}/query",
        params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
        timeout=60,
    )
    r.raise_for_status()
    return int(r.json()["count"])


def fetch_page(session: requests.Session, offset: int) -> list[dict]:
    r = session.get(
        f"{LAYER}/query",
        params={
            "where": "1=1",
            "outFields": OUT_FIELDS,
            "returnGeometry": "true",
            "outSR": "4326",
            "geometryPrecision": "6",
            "resultOffset": offset,
            "resultRecordCount": PAGE,
            "orderByFields": "OBJECTID",
            "f": "geojson",
        },
        timeout=120,
    )
    r.raise_for_status()
    return r.json().get("features", [])


def ensure_tables(cur) -> None:
    cur.execute("SET LOCAL lock_timeout = '15s'")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS smith_parcel_geometries (
            parcel_id     text PRIMARY KEY,
            account_no    text,
            geom          geometry(Geometry, 4326),
            area_sqft     numeric,
            area_acres    numeric,
            centroid_lat  numeric,
            centroid_lon  numeric,
            bbox_min_lat  numeric,
            bbox_max_lat  numeric,
            bbox_min_lon  numeric,
            bbox_max_lon  numeric,
            source_county text,
            loaded_at     timestamptz DEFAULT now()
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS smith_parcel_geometries_geom_gix "
        "ON smith_parcel_geometries USING gist (geom)"
    )


GEOM_UPSERT = """
INSERT INTO smith_parcel_geometries
    (parcel_id, account_no, geom, area_acres,
     centroid_lat, centroid_lon,
     bbox_min_lat, bbox_max_lat, bbox_min_lon, bbox_max_lon,
     source_county, loaded_at)
VALUES (%(pid)s, %(acct)s,
        ST_SetSRID(ST_GeomFromText(%(wkt)s), 4326),
        %(acres)s, %(clat)s, %(clon)s,
        %(miny)s, %(maxy)s, %(minx)s, %(maxx)s,
        'Smith', now())
ON CONFLICT (parcel_id) DO UPDATE SET
    account_no   = EXCLUDED.account_no,
    geom         = EXCLUDED.geom,
    area_acres   = EXCLUDED.area_acres,
    centroid_lat = EXCLUDED.centroid_lat,
    centroid_lon = EXCLUDED.centroid_lon,
    bbox_min_lat = EXCLUDED.bbox_min_lat,
    bbox_max_lat = EXCLUDED.bbox_max_lat,
    bbox_min_lon = EXCLUDED.bbox_min_lon,
    bbox_max_lon = EXCLUDED.bbox_max_lon,
    loaded_at    = now()
"""

ATTR_UPSERT = """
INSERT INTO tx_cad_parcels
    (parcel_id, cad_source, tax_year, situs_address, situs_city, situs_state,
     situs_zip, owner_name, subdivision, lot_acres, building_sqft, year_built,
     school_district, county_fips, loaded_at)
VALUES (%(pid)s, 'SMITHCAD', %(taxyr)s, %(addr)s, %(city)s, 'TX',
        %(zip)s, %(owner)s, %(subd)s, %(acres)s, %(sfla)s, %(yrblt)s,
        %(isd)s, '48423', now())
ON CONFLICT (parcel_id, cad_source) DO UPDATE SET
    tax_year        = EXCLUDED.tax_year,
    situs_address   = EXCLUDED.situs_address,
    situs_city      = EXCLUDED.situs_city,
    situs_zip       = EXCLUDED.situs_zip,
    owner_name      = EXCLUDED.owner_name,
    subdivision     = EXCLUDED.subdivision,
    lot_acres       = EXCLUDED.lot_acres,
    building_sqft   = EXCLUDED.building_sqft,
    year_built      = EXCLUDED.year_built,
    school_district = EXCLUDED.school_district,
    loaded_at       = now()
"""


def _norm_str(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _norm_int(v):
    try:
        i = int(v)
        return i if i != 0 else None
    except (TypeError, ValueError):
        return None


def build_rows(features: list[dict]):
    geom_rows, attr_rows = [], []
    for feat in features:
        props = feat.get("properties") or {}
        acct = _norm_str(props.get("ACCOUNT"))
        if not acct:
            continue  # ACCOUNT is the join key; skip rows without it
        geo = feat.get("geometry")
        wkt = clat = clon = miny = maxy = minx = maxx = None
        if geo:
            try:
                g = shape(geo)
                if not g.is_empty:
                    if not g.is_valid:
                        g = g.buffer(0)
                    wkt = shapely_wkt.dumps(g, rounding_precision=6)
                    c = g.centroid
                    clat, clon = c.y, c.x
                    minx, miny, maxx, maxy = g.bounds
            except Exception:
                wkt = None
        if wkt:
            geom_rows.append({
                "pid": acct, "acct": acct, "wkt": wkt,
                "acres": props.get("Calc_Acre"),
                "clat": clat, "clon": clon,
                "miny": miny, "maxy": maxy, "minx": minx, "maxx": maxx,
            })
        attr_rows.append({
            "pid": acct,
            # tax_year is NOT NULL in tx_cad_parcels; the Smith feed leaves it
            # null on non-residential/median strips. Default to the current
            # TAXYR seen on residential rows (2026) when absent.
            "taxyr": _norm_int(props.get("TAXYR")) or 2026,
            "addr": _norm_str(props.get("ADDRESS")),
            "city": _norm_str(props.get("CITY_COUNTY")),
            "zip": _norm_str(props.get("ZIPCODE")),
            "owner": _norm_str(props.get("OWN1")),
            "subd": _norm_str(props.get("SUBDNUM")),
            "acres": props.get("Calc_Acre"),
            "sfla": _norm_int(props.get("SFLA")),
            "yrblt": _norm_int(props.get("YRBLT")),
            "isd": _norm_str(props.get("ISD")),
        })
    return geom_rows, attr_rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="max parcels (smoke test)")
    ap.add_argument("--dry-run", action="store_true", help="fetch only; no DB writes")
    ap.add_argument("--no-stage", action="store_true", help="skip raw page staging")
    args = ap.parse_args()

    STAGE_DIR.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    session.headers["User-Agent"] = "permit-api-smithcad-loader/1.0"

    total = fetch_count(session)
    target = min(total, args.limit) if args.limit else total
    print(f"Smith CAD Tax Parcels: {total} total; loading up to {target}")

    con = None
    if not args.dry_run:
        con = psycopg2.connect(get_dsn(), connect_timeout=15)
        con.autocommit = False
        with con.cursor() as cur:
            cur.execute("BEGIN")
            ensure_tables(cur)
            con.commit()

    loaded_geom = loaded_attr = fetched = 0
    offset = 0
    while offset < target:
        feats = fetch_page(session, offset)
        if not feats:
            break
        fetched += len(feats)
        if not args.no_stage:
            (STAGE_DIR / f"page_{offset:07d}.geojson").write_text(
                json.dumps({"features": feats})
            )
        geom_rows, attr_rows = build_rows(feats)
        if not args.dry_run:
            with con.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = '15s'")
                psycopg2.extras.execute_batch(cur, GEOM_UPSERT, geom_rows, page_size=500)
                psycopg2.extras.execute_batch(cur, ATTR_UPSERT, attr_rows, page_size=500)
            con.commit()
            loaded_geom += len(geom_rows)
            loaded_attr += len(attr_rows)
        offset += PAGE
        print(f"  offset={offset:>7}  fetched={fetched:>7}  "
              f"geom={loaded_geom:>7}  attr={loaded_attr:>7}", flush=True)
        time.sleep(0.2)  # be polite to the public GIS host

    if con:
        con.close()
    print(f"DONE. fetched={fetched} geom={loaded_geom} attr={loaded_attr} "
          f"staged_to={STAGE_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
