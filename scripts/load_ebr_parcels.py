#!/usr/bin/env python3
"""Load East Baton Rouge Parish, LA (FIPS 22033) tax parcels.

FIRST Louisiana build for the un-serviced storm-lead product. EBR is a
WIND/tropical product (little hail; roof-damage peril is wind + tropical
systems). This loader provides the parcel substrate (geometry + attributes)
that the `unserviced_hail_leads` EBR arm joins against.

Source (free ArcGIS, geometry + attributes inline):
    https://maps.brla.gov/gis/rest/services/Cadastral/Tax_Parcel/MapServer/0
    ~205,820 polygon parcels, paginated 2000/page, requested in EPSG:4326
    (the layer is natively LA State Plane 3452; outSR=4326 reprojects on the
    server). Fields used: ASSESSMENT_NUM (join key), OWNER, PHYSICAL_ADDRESS
    (situs), SUBDIVISION, SUM_FAIR_MARKET_VALUE.

    GAP: the layer carries NO year_built — we leave year_built NULL in
    tx_cad_parcels and never fabricate it.

Targets (mirrors the TX Bexar/Comal pattern so the existing MV machinery and
the /v1/hail-leads/unserviced endpoint work unchanged):
    ebr_parcel_geometries  — geom (4326 MultiPolygon) + centroid_lat/lon,
                             parcel_id = ASSESSMENT_NUM. GIST index on geom.
    tx_cad_parcels         — attributes; cad_source='EBRPA', county_fips='22033',
                             situs_address/city/state/zip, owner_name, market_value,
                             subdivision; year_built NULL.

Box-gentle: per-page upserts in their own txn with lock_timeout; never a full
scan; ON CONFLICT keeps it idempotent. Raw pages staged to
/mnt/win11/Fedora/free_data/ebr/ (NOT /home/will).

Usage:
    python3 scripts/load_ebr_parcels.py [--limit-pages N] [--force]

DSN resolution order: --dsn, $PERMITS_DSN, ~/.config/permitlookup/permits_dsn.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import date
from pathlib import Path

import httpx
import psycopg2
from psycopg2.extras import execute_batch

LAYER_URL = (
    "https://maps.brla.gov/gis/rest/services/Cadastral/Tax_Parcel/"
    "MapServer/0/query"
)
PAGE_SIZE = 2000
CAD_SOURCE = "EBRPA"
COUNTY_FIPS = "22033"
SITUS_STATE = "LA"
STAGING_DIR = Path("/mnt/win11/Fedora/free_data/ebr")

OUT_FIELDS = ",".join([
    "ASSESSMENT_NUM",
    "OWNER",
    "OWNER_ADDRESS",
    "OWNER_CITY_STATE_ZIP",
    "PHYSICAL_ADDRESS",
    "SUBDIVISION",
    "LEGAL_DESCRIPTION",
    "SUM_LAND_VALUE",
    "SUM_IMPROVEMENT_VALUE",
    "SUM_FAIR_MARKET_VALUE",
    "SUM_ASSESSED_VALUE",
])

GEOM_DDL = """
CREATE TABLE IF NOT EXISTS ebr_parcel_geometries (
    parcel_id     TEXT PRIMARY KEY,
    geom          geometry(MultiPolygon, 4326),
    area_sqft     NUMERIC,
    area_acres    NUMERIC,
    centroid_lat  NUMERIC,
    centroid_lon  NUMERIC,
    bbox_min_lat  NUMERIC,
    bbox_max_lat  NUMERIC,
    bbox_min_lon  NUMERIC,
    bbox_max_lon  NUMERIC,
    source_county TEXT,
    loaded_at     TIMESTAMPTZ DEFAULT NOW()
);
"""

GEOM_GIX = (
    "CREATE INDEX IF NOT EXISTS ebr_parcel_geometries_geom_gix "
    "ON ebr_parcel_geometries USING GIST (geom)"
)

GEOM_UPSERT = """
INSERT INTO ebr_parcel_geometries
    (parcel_id, geom, area_sqft, area_acres, centroid_lat, centroid_lon,
     bbox_min_lat, bbox_max_lat, bbox_min_lon, bbox_max_lon,
     source_county, loaded_at)
SELECT
    %(parcel_id)s,
    g.geom,
    ST_Area(g.geom::geography) * 10.7639,
    ST_Area(g.geom::geography) / 4046.8564224,
    ST_Y(ST_Centroid(g.geom)),
    ST_X(ST_Centroid(g.geom)),
    ST_YMin(g.geom), ST_YMax(g.geom),
    ST_XMin(g.geom), ST_XMax(g.geom),
    %(source_county)s,
    NOW()
FROM (
    SELECT ST_Multi(
             ST_CollectionExtract(
               ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(%(geojson)s), 4326)),
               3)
           ) AS geom
) g
WHERE g.geom IS NOT NULL AND NOT ST_IsEmpty(g.geom)
ON CONFLICT (parcel_id) DO UPDATE SET
    geom          = EXCLUDED.geom,
    area_sqft     = EXCLUDED.area_sqft,
    area_acres    = EXCLUDED.area_acres,
    centroid_lat  = EXCLUDED.centroid_lat,
    centroid_lon  = EXCLUDED.centroid_lon,
    bbox_min_lat  = EXCLUDED.bbox_min_lat,
    bbox_max_lat  = EXCLUDED.bbox_max_lat,
    bbox_min_lon  = EXCLUDED.bbox_min_lon,
    bbox_max_lon  = EXCLUDED.bbox_max_lon,
    source_county = EXCLUDED.source_county,
    loaded_at     = NOW();
"""

ATTR_UPSERT = """
INSERT INTO tx_cad_parcels
    (parcel_id, cad_source, tax_year, county_fips, situs_address, situs_city,
     situs_state, situs_zip, owner_name, owner_address_full, subdivision,
     legal_description, land_value, improvement_value, market_value,
     assessed_value, year_built, raw, loaded_at)
VALUES
    (%(parcel_id)s, %(cad_source)s, %(tax_year)s, %(county_fips)s,
     %(situs_address)s, %(situs_city)s, %(situs_state)s, %(situs_zip)s,
     %(owner_name)s, %(owner_address_full)s, %(subdivision)s,
     %(legal_description)s, %(land_value)s, %(improvement_value)s,
     %(market_value)s, %(assessed_value)s, NULL, %(raw)s, NOW())
ON CONFLICT (parcel_id, cad_source) DO UPDATE SET
    situs_address      = EXCLUDED.situs_address,
    situs_city         = EXCLUDED.situs_city,
    situs_state        = EXCLUDED.situs_state,
    situs_zip          = EXCLUDED.situs_zip,
    owner_name         = EXCLUDED.owner_name,
    owner_address_full = EXCLUDED.owner_address_full,
    subdivision        = EXCLUDED.subdivision,
    legal_description  = EXCLUDED.legal_description,
    land_value         = EXCLUDED.land_value,
    improvement_value  = EXCLUDED.improvement_value,
    market_value       = EXCLUDED.market_value,
    assessed_value     = EXCLUDED.assessed_value,
    county_fips        = EXCLUDED.county_fips,
    raw                = EXCLUDED.raw,
    loaded_at          = NOW();
"""


def resolve_dsn(cli_dsn: str | None) -> str:
    if cli_dsn:
        return cli_dsn
    env = os.environ.get("PERMITS_DSN")
    if env:
        return env
    path = Path.home() / ".config" / "permitlookup" / "permits_dsn"
    if path.exists():
        return path.read_text().strip()
    sys.exit("No DSN: pass --dsn, set $PERMITS_DSN, or create ~/.config/permitlookup/permits_dsn")


def _num(v):
    if v is None or v == "":
        return None
    try:
        f = float(v)
        return None if f != f else f  # drop NaN
    except (ValueError, TypeError):
        return None


def _clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


# Placeholder assessment numbers (unassigned/dummy geometries): all-zero digit
# groups like 000-0000-0 / 000-000-0. These carry no owner/address — skip them.
_ZERO_ASSESS_RX = re.compile(r"^0+(-0+)*$")


def _is_junk_parcel(pid: str | None, address: str | None) -> bool:
    if not pid:
        return True
    if _ZERO_ASSESS_RX.match(pid.replace(" ", "")):
        return True
    # No situs address = no mailable lead and no MV address-join value.
    if not address:
        return True
    return False


def _split_owner_csz(csz: str | None):
    """Best-effort split of 'CITY ST ZIP' — used only for owner_address_full."""
    return _clean(csz)


def ensure_tables(cur) -> None:
    cur.execute(GEOM_DDL)
    cur.execute(GEOM_GIX)
    # tx_cad_parcels already exists in prod; guard the unique key used by the
    # ON CONFLICT target so a fresh DB still works.
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_tx_cad_parcels_parcel_cad "
        "ON tx_cad_parcels (parcel_id, cad_source)"
    )


def fetch_page(client: httpx.Client, offset: int) -> dict:
    params = {
        # Server-side skip of the unassigned placeholder parcels (all carry
        # ASSESSMENT_NUM='000-0000-0' and no address); keeps the page useful.
        "where": "PHYSICAL_ADDRESS IS NOT NULL",
        "outFields": OUT_FIELDS,
        "returnGeometry": "true",
        "outSR": "4326",
        "resultRecordCount": str(PAGE_SIZE),
        "resultOffset": str(offset),
        "f": "geojson",
    }
    r = client.get(LAYER_URL, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def get_total(client: httpx.Client) -> int:
    r = client.get(
        LAYER_URL,
        params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
        timeout=60,
    )
    r.raise_for_status()
    return int(r.json().get("count", 0))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn")
    ap.add_argument("--limit-pages", type=int, default=None,
                    help="Cap pages (debug). Default: all.")
    ap.add_argument("--force", action="store_true",
                    help="Reserved; loader is idempotent via ON CONFLICT.")
    ap.add_argument("--no-stage", action="store_true",
                    help="Skip writing raw page JSON to staging dir.")
    args = ap.parse_args()

    dsn = resolve_dsn(args.dsn)
    stage = not args.no_stage
    if stage:
        STAGING_DIR.mkdir(parents=True, exist_ok=True)

    started = time.time()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    geom_ok = attr_ok = geom_skip = 0

    with httpx.Client(follow_redirects=True) as client:
        total = get_total(client)
        print(f"EBR Tax_Parcel layer reports {total} parcels", flush=True)

        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '15s'")
            cur.execute("SET statement_timeout = '60s'")
            ensure_tables(cur)
        conn.commit()

        offset = 0
        page_no = 0
        while True:
            if args.limit_pages is not None and page_no >= args.limit_pages:
                break
            try:
                fc = fetch_page(client, offset)
            except Exception as exc:  # noqa: BLE001
                print(f"page offset={offset} fetch failed: {exc}", flush=True)
                time.sleep(3)
                continue

            feats = fc.get("features") or []
            if not feats:
                break

            if stage:
                (STAGING_DIR / f"parcels_{offset:07d}.geojson").write_text(
                    json.dumps(fc)
                )

            geom_rows = []
            attr_rows = []
            for ft in feats:
                props = ft.get("properties") or {}
                geom = ft.get("geometry")
                pid = _clean(props.get("ASSESSMENT_NUM"))
                situs = _clean(props.get("PHYSICAL_ADDRESS"))
                if _is_junk_parcel(pid, situs):
                    continue
                if geom:
                    geom_rows.append({
                        "parcel_id": pid,
                        "geojson": json.dumps(geom),
                        "source_county": "East Baton Rouge",
                    })
                attr_rows.append({
                    "parcel_id": pid,
                    "cad_source": CAD_SOURCE,
                    "tax_year": date.today().year,
                    "county_fips": COUNTY_FIPS,
                    "situs_address": situs,
                    "situs_city": "Baton Rouge",
                    "situs_state": SITUS_STATE,
                    "situs_zip": None,
                    "owner_name": _clean(props.get("OWNER")),
                    "owner_address_full": _split_owner_csz(
                        " ".join(filter(None, [
                            _clean(props.get("OWNER_ADDRESS")),
                            _clean(props.get("OWNER_CITY_STATE_ZIP")),
                        ])) or None
                    ),
                    "subdivision": _clean(props.get("SUBDIVISION")),
                    "legal_description": _clean(props.get("LEGAL_DESCRIPTION")),
                    "land_value": _num(props.get("SUM_LAND_VALUE")),
                    "improvement_value": _num(props.get("SUM_IMPROVEMENT_VALUE")),
                    "market_value": _num(props.get("SUM_FAIR_MARKET_VALUE")),
                    "assessed_value": _num(props.get("SUM_ASSESSED_VALUE")),
                    "raw": json.dumps(props),
                })

            with conn.cursor() as cur:
                cur.execute("SET lock_timeout = '15s'")
                cur.execute("SET statement_timeout = '60s'")
                before = cur.rowcount
                if geom_rows:
                    for gr in geom_rows:
                        try:
                            cur.execute(GEOM_UPSERT, gr)
                            geom_ok += cur.rowcount
                        except Exception as exc:  # noqa: BLE001
                            geom_skip += 1
                            conn.rollback()
                            cur.execute("SET lock_timeout = '15s'")
                            cur.execute("SET statement_timeout = '60s'")
                            if geom_skip <= 10:
                                print(f"  geom skip {gr['parcel_id']}: {exc}",
                                      flush=True)
                if attr_rows:
                    execute_batch(cur, ATTR_UPSERT, attr_rows, page_size=500)
                    attr_ok += len(attr_rows)
            conn.commit()

            page_no += 1
            offset += len(feats)
            if page_no % 10 == 0 or len(feats) < PAGE_SIZE:
                print(f"  page {page_no}: offset={offset} "
                      f"geom_ok={geom_ok} attr_ok={attr_ok} "
                      f"geom_skip={geom_skip}", flush=True)
            if len(feats) < PAGE_SIZE:
                break

    # ANALYZE the geom table so the GIST plan is good for the MV refresh.
    with conn.cursor() as cur:
        cur.execute("SET lock_timeout = '15s'")
        try:
            cur.execute("ANALYZE ebr_parcel_geometries")
        except Exception as exc:  # noqa: BLE001
            print(f"ANALYZE skipped: {exc}", flush=True)
    conn.commit()
    conn.close()

    dur = time.time() - started
    print(f"DONE: geom_ok={geom_ok} attr_ok={attr_ok} geom_skip={geom_skip} "
          f"in {dur:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
