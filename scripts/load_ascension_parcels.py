#!/usr/bin/env python3
"""Load Ascension Parish, LA (FIPS 22005) tax parcels.

SECOND Louisiana build for the un-serviced storm-lead product (mirrors the
East Baton Rouge build). Gonzales sits in the Baton Rouge metro, the parish
immediately south/east of EBR, so the peril is the SAME: WIND/tropical
(Thunderstorm Wind dominates; hail secondary; little standalone hail). This
loader provides the parcel substrate (geometry + attributes) that the
`unserviced_hail_leads` Ascension arm joins against.

Source (free hosted ArcGIS Feature Service, geometry + attributes inline):
    https://services6.arcgis.com/1fGAZVgZnPx4zcNH/arcgis/rest/services/
        Ascension_Parish_Tax_Parcels/FeatureServer/0
    ~48,115 polygon parcels, paginated 2000/page, requested in EPSG:4326
    (the layer is natively Web Mercator 3857; outSR=4326 reprojects on the
    server). Owned by "GIS Ascension Parish" (the assessor's live parcel feed).

    Fields used (shapefile 10-char truncated names):
      PARCEL_NO / ParcelNumb  -> parcel_id (numeric assessor id, the join key)
      LOCATION_S              -> situs house number
      STREET_DIR              -> situs street pre-direction
      LOCATION_1              -> situs street name
      LOCATION_A / LOCATION_2 -> situs unit (mostly blank)
      LOCATION_C              -> situs CITY (real per-parcel: Gonzales,
                                 Prairieville, Donaldsonville, etc.) -- the
                                 field that lets the MV isolate Gonzales-city.
      OWNERNAME_              -> owner_name
      OWNERMAILI              -> owner mailing address (owner_address_full)
      SUBD_NAME_              -> subdivision
      PROPERTYDE              -> legal description
      ASSMT_LAND/IMPR/TOTA    -> ASSESSED values. LA assesses residential at
                                 ~10% of fair-market value, so ASSMT_TOTA is the
                                 ASSESSED total, NOT market. We store it in
                                 assessed_value and LEAVE market_value NULL -- we
                                 do NOT fabricate a market estimate.

    GAPS (reported honestly, never fabricated):
      * NO year_built field          -> year_built stays NULL (same as EBR).
      * Value is ASSESSED only       -> market_value NULL; assessed_value set.

Targets (mirror the EBR pattern so the MV machinery + endpoint work unchanged):
    ascension_parcel_geometries  -- geom (4326 MultiPolygon) + centroid_lat/lon,
                                   parcel_id = PARCEL_NO. GIST index on geom.
    tx_cad_parcels               -- attributes; cad_source='ASCPA',
                                   county_fips='22005', situs_address/city/state,
                                   owner_name, assessed_value; market_value NULL,
                                   year_built NULL.

Box-gentle: per-page upserts in their own txn with lock_timeout; never a full
scan; ON CONFLICT keeps it idempotent. Raw pages staged to
/mnt/win11/Fedora/free_data/ascension/ (NOT /home/will).

Usage:
    python3 scripts/load_ascension_parcels.py [--limit-pages N] [--no-stage]

DSN resolution order: --dsn, $PERMITS_DSN, ~/.config/permitlookup/permits_dsn.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date
from pathlib import Path

import httpx
import psycopg2
from psycopg2.extras import execute_batch

LAYER_URL = (
    "https://services6.arcgis.com/1fGAZVgZnPx4zcNH/arcgis/rest/services/"
    "Ascension_Parish_Tax_Parcels/FeatureServer/0/query"
)
PAGE_SIZE = 2000
CAD_SOURCE = "ASCPA"
COUNTY_FIPS = "22005"
SITUS_STATE = "LA"
STAGING_DIR = Path("/mnt/win11/Fedora/free_data/ascension")

OUT_FIELDS = ",".join([
    "PARCEL_NO",
    "ParcelNumb",
    "LOCATION_S",
    "STREET_DIR",
    "LOCATION_1",
    "LOCATION_A",
    "LOCATION_2",
    "LOCATION_C",
    "OWNERNAME_",
    "OWNERMAILI",
    "OWNER_CITY",
    "SUBD_NAME_",
    "PROPERTYDE",
    "ASSMT_LAND",
    "ASSMT_IMPR",
    "ASSMT_TOTA",
    "TAXABLE_VA",
    "SALEPRICE_",
])

GEOM_DDL = """
CREATE TABLE IF NOT EXISTS ascension_parcel_geometries (
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
    "CREATE INDEX IF NOT EXISTS ascension_parcel_geometries_geom_gix "
    "ON ascension_parcel_geometries USING GIST (geom)"
)

GEOM_UPSERT = """
INSERT INTO ascension_parcel_geometries
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

# year_built and market_value stay NULL (no year-built field; LA value is
# ASSESSED, not market -- assessed_value carries the ~10%-of-market figure).
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
     NULL, %(assessed_value)s, NULL, %(raw)s, NOW())
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
    assessed_value     = EXCLUDED.assessed_value,
    county_fips        = EXCLUDED.county_fips,
    raw                = EXCLUDED.raw,
    loaded_at          = NOW();
"""


def resolve_dsn(cli_dsn):
    if cli_dsn:
        return cli_dsn
    env = os.environ.get("PERMITS_DSN")
    if env:
        return env
    path = Path.home() / ".config" / "permitlookup" / "permits_dsn"
    if path.exists():
        return path.read_text().strip()
    sys.exit("No DSN: pass --dsn, set $PERMITS_DSN, or create "
             "~/.config/permitlookup/permits_dsn")


def _num(v):
    if v is None or v == "":
        return None
    try:
        f = float(v)
        return None if f != f else f  # drop NaN
    except (ValueError, TypeError):
        return None


def _int(v):
    f = _num(v)
    return None if f is None else int(round(f))


def _clean(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _parcel_id(props):
    """Assessor parcel number; comes through as a float like 20022915.0."""
    raw = props.get("PARCEL_NO")
    if raw in (None, ""):
        raw = props.get("ParcelNumb")
    if raw in (None, ""):
        return None
    try:
        return str(int(round(float(raw))))
    except (ValueError, TypeError):
        return _clean(raw)


def _situs(props):
    """Build the physical/situs street address from the LOCATION_* parts."""
    parts = [
        _clean(props.get("LOCATION_S")),   # house number
        _clean(props.get("STREET_DIR")),   # pre-direction
        _clean(props.get("LOCATION_1")),   # street name
        _clean(props.get("LOCATION_A")),   # unit-ish (mostly blank/' ')
        _clean(props.get("LOCATION_2")),
    ]
    addr = " ".join(p for p in parts if p)
    return addr or None


def _is_junk_parcel(pid, situs, owner):
    if not pid:
        return True
    # No situs address = no mailable lead and no MV address value.
    if not situs:
        return True
    # Drop the obvious non-mailable common-area / placeholder rows.
    if not owner:
        return True
    return False


def ensure_tables(cur):
    cur.execute(GEOM_DDL)
    cur.execute(GEOM_GIX)
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_tx_cad_parcels_parcel_cad "
        "ON tx_cad_parcels (parcel_id, cad_source)"
    )


def fetch_page(client, offset):
    params = {
        # Server-side skip of rows with no situs street name (vacant common
        # areas / dummy rows carry a blank LOCATION_1); keeps each page useful.
        "where": "LOCATION_1 IS NOT NULL AND LOCATION_1 <> ' '",
        "outFields": OUT_FIELDS,
        "returnGeometry": "true",
        "outSR": "4326",
        "resultRecordCount": str(PAGE_SIZE),
        "resultOffset": str(offset),
        "orderByFields": "PARCEL_NO",
        "f": "geojson",
    }
    r = client.get(LAYER_URL, params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def get_total(client):
    r = client.get(
        LAYER_URL,
        params={"where": "1=1", "returnCountOnly": "true", "f": "json"},
        timeout=60,
    )
    r.raise_for_status()
    return int(r.json().get("count", 0))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn")
    ap.add_argument("--limit-pages", type=int, default=None,
                    help="Cap pages (debug). Default: all.")
    ap.add_argument("--no-stage", action="store_true",
                    help="Skip writing raw page JSON to staging dir.")
    args = ap.parse_args()

    dsn = resolve_dsn(args.dsn)
    stage = not args.no_stage
    if stage:
        try:
            STAGING_DIR.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            print(f"staging dir unavailable ({exc}); continuing --no-stage",
                  flush=True)
            stage = False

    started = time.time()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    geom_ok = attr_ok = geom_skip = 0

    with httpx.Client(follow_redirects=True) as client:
        total = get_total(client)
        print(f"Ascension Tax_Parcel layer reports {total} parcels", flush=True)

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
                pid = _parcel_id(props)
                situs = _situs(props)
                owner = _clean(props.get("OWNERNAME_"))
                if _is_junk_parcel(pid, situs, owner):
                    continue
                if geom:
                    geom_rows.append({
                        "parcel_id": pid,
                        "geojson": json.dumps(geom),
                        "source_county": "Ascension",
                    })
                mailing = _clean(props.get("OWNERMAILI"))
                attr_rows.append({
                    "parcel_id": pid,
                    "cad_source": CAD_SOURCE,
                    "tax_year": date.today().year,
                    "county_fips": COUNTY_FIPS,
                    "situs_address": situs,
                    "situs_city": _clean(props.get("LOCATION_C")),
                    "situs_state": SITUS_STATE,
                    "situs_zip": None,
                    "owner_name": owner,
                    "owner_address_full": mailing,
                    "subdivision": _clean(props.get("SUBD_NAME_")),
                    "legal_description": _clean(props.get("PROPERTYDE")),
                    "land_value": _int(props.get("ASSMT_LAND")),
                    "improvement_value": _int(props.get("ASSMT_IMPR")),
                    "assessed_value": _int(props.get("ASSMT_TOTA")),
                    "raw": json.dumps(props),
                })

            with conn.cursor() as cur:
                cur.execute("SET lock_timeout = '15s'")
                cur.execute("SET statement_timeout = '60s'")
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
            if page_no % 5 == 0 or len(feats) < PAGE_SIZE:
                print(f"  page {page_no}: offset={offset} "
                      f"geom_ok={geom_ok} attr_ok={attr_ok} "
                      f"geom_skip={geom_skip}", flush=True)
            if len(feats) < PAGE_SIZE:
                break

    with conn.cursor() as cur:
        cur.execute("SET lock_timeout = '15s'")
        try:
            cur.execute("ANALYZE ascension_parcel_geometries")
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
