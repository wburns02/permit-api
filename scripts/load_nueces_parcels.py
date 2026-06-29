#!/usr/bin/env python3
"""Load Nueces County, TX (FIPS 48355 — Corpus Christi) tax parcels.

FIRST South-Texas / Gulf-Coast build for the un-serviced storm-lead product.
Nueces is a WIND product (the roof-damage peril is straight-line thunderstorm
wind + tropical systems, NOT hail). Recon: in the last 18 months the Corpus
bbox shows only ~2 hail reports vs 34 Thunderstorm-Wind events (max 100 kt /
115 mph on 2025-05-08). So the `unserviced_hail_leads` Nueces arm keys on WIND,
mirroring the East Baton Rouge / Ascension arms — NOT hail like the TX arms.

This loader provides the parcel substrate (geometry + attributes) that the MV
Nueces arm joins against.

Source (FREE, authoritative — TxGIO / StratMap statewide land-parcel program):
    The City of Corpus Christi and Nueces CAD ArcGIS REST endpoints are either
    walled (BIS Consulting, 500/token) or mislabeled (the Corpus OpenData
    "NCad_Parcels" layer is a single county-boundary polygon). The clean free
    path is the TxGIO StratMap per-county download, which carries the full CAD
    attribute schema already translated to a common form:

        api.tnris.org collection 0fa04328-...  = StratMap 2025 Land Parcels
        Nueces resource = stratmap25-landparcels_48355_lp.zip  (~85 MB)
        S3 mirror (CloudFront gates the data.geographic.texas.gov URL with 403;
        the underlying S3 origin serves it directly):
            https://s3.amazonaws.com/data.tnris.org/<collection>/resources/<file>

    The zip contains a File Geodatabase (EPSG:4326, MultiPolygon, 157,198
    parcels, TAX_YEAR 2025). Fields used: Prop_ID (join key), OWNER_NAME,
    SITUS_ADDR / SITUS_CITY / SITUS_ZIP, LAND_VALUE / IMP_VALUE / MKT_VALUE,
    YEAR_BUILT, LEGAL_DESC.

    Fill rates (verified): SITUS_ADDR 100%, OWNER 91%, MKT_VALUE 91%,
    SITUS_CITY 90%, YEAR_BUILT 69%. Unlike EBR (no year_built at all) Nueces
    DOES carry year_built — so the Nueces MV arm projects a real roof-age
    signal. MKT_VALUE is a real market value (not structurally 0 like the EBR
    feed), so Nueces uses market_value as its value basis.

    YEAR_BUILT can be a comma-joined list on multi-improvement parcels
    (e.g. '1956,1990,1987'); we take the NEWEST (max) year — the most recent
    roof is the relevant roof-age signal.

Targets (mirror the EBR/Bexar pattern so the MV machinery + the
/v1/hail-leads/unserviced endpoint work unchanged):
    nueces_parcel_geometries  — geom (4326 MultiPolygon) + centroid_lat/lon,
                                parcel_id = Prop_ID. GIST index on geom.
    tx_cad_parcels            — attributes; cad_source='NUECESCAD',
                                county_fips='48355', situs_*, owner_name,
                                land/improvement/market value, year_built.

Box-gentle: batched upserts in bounded txns with lock_timeout=15s /
statement_timeout=60s; never a full scan; ON CONFLICT keeps it idempotent.
NEVER pg_terminate / pg_cancel. Raw GDB staged under
/mnt/win11/Fedora/free_data/nueces/ (NOT /home/will).

Usage:
    # Needs GDAL via pyogrio — run inside a venv that has it, e.g. the staging
    # gdalenv created during recon:
    #   /mnt/win11/Fedora/free_data/nueces/gdalenv/bin/python \
    #       scripts/load_nueces_parcels.py [--limit N] [--gdb PATH] [--download]
    python3 scripts/load_nueces_parcels.py [--limit N] [--gdb PATH] [--download]

DSN resolution order: --dsn, $PERMITS_DSN, ~/.config/permitlookup/permits_dsn.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import zipfile
from datetime import date
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch

CAD_SOURCE = "NUECESCAD"
COUNTY_FIPS = "48355"
SITUS_STATE = "TX"
SOURCE_COUNTY = "Nueces"
BATCH = 1000

STAGING_DIR = Path("/mnt/win11/Fedora/free_data/nueces")
# TxGIO StratMap 2025 Land Parcels collection + Nueces resource (S3 origin).
TNRIS_COLLECTION = "0fa04328-872e-481c-b453-126a74777593"
NUECES_ZIP = "stratmap25-landparcels_48355_lp.zip"
S3_URL = (
    f"https://s3.amazonaws.com/data.tnris.org/{TNRIS_COLLECTION}/resources/"
    f"{NUECES_ZIP}"
)

GEOM_DDL = """
CREATE TABLE IF NOT EXISTS nueces_parcel_geometries (
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
    "CREATE INDEX IF NOT EXISTS nueces_parcel_geometries_geom_gix "
    "ON nueces_parcel_geometries USING GIST (geom)"
)

GEOM_UPSERT = """
INSERT INTO nueces_parcel_geometries
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
               ST_MakeValid(ST_SetSRID(ST_GeomFromWKB(%(wkb)s), 4326)),
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
     %(market_value)s, %(assessed_value)s, %(year_built)s, %(raw)s, NOW())
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
    year_built         = EXCLUDED.year_built,
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


def _clean(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in ("none", "nan"):
        return None
    return s


def _year_built(v):
    """YEAR_BUILT may be a comma list ('1956,1990,1987') on multi-improvement
    parcels. Take the NEWEST plausible 4-digit year (the most recent roof)."""
    s = _clean(v)
    if not s:
        return None
    years = [int(y) for y in re.findall(r"\b(1[6-9]\d{2}|20\d{2})\b", s)]
    years = [y for y in years if 1600 <= y <= date.today().year + 1]
    return max(years) if years else None


def _is_junk_parcel(pid: str | None, address: str | None) -> bool:
    if not pid:
        return True
    if re.match(r"^0+$", pid.replace(" ", "")):
        return True
    if not address:  # no situs = no mailable lead / no MV address-join value
        return True
    return False


def ensure_tables(cur) -> None:
    cur.execute(GEOM_DDL)
    cur.execute(GEOM_GIX)
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_tx_cad_parcels_parcel_cad "
        "ON tx_cad_parcels (parcel_id, cad_source)"
    )


def download_source() -> Path:
    """Fetch the StratMap Nueces zip to staging (idempotent) and return the
    extracted .gdb path."""
    import httpx

    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = STAGING_DIR / NUECES_ZIP
    if not zip_path.exists() or zip_path.stat().st_size < 1_000_000:
        print(f"downloading {S3_URL} -> {zip_path}", flush=True)
        with httpx.Client(follow_redirects=True, timeout=300) as c:
            r = c.get(S3_URL, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            zip_path.write_bytes(r.content)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(STAGING_DIR)
    return _find_gdb(STAGING_DIR)


def _find_gdb(root: Path) -> Path:
    gdbs = list(root.rglob("*.gdb"))
    if not gdbs:
        sys.exit(f"No .gdb found under {root}; pass --gdb or --download")
    return gdbs[0]


def iter_features(gdb: Path, limit: int | None):
    """Yield (props_dict, wkb_bytes) from the GDB layer, batched, never loading
    the whole layer if --limit is given. Uses pyogrio (bundled GDAL)."""
    try:
        import pyogrio
        import shapely
    except ImportError as e:
        sys.exit(
            f"GDAL/shapely required ({e}). Run inside a venv with pyogrio + "
            f"shapely, e.g. /mnt/win11/Fedora/free_data/nueces/gdalenv "
            f"(pip install pyogrio shapely)."
        )

    layers = pyogrio.list_layers(str(gdb))
    layer = layers[0][0]
    info = pyogrio.read_info(str(gdb), layer=layer)
    total = info["features"]
    print(f"GDB layer '{layer}': {total} features, crs={info['crs']}",
          flush=True)
    # Page through with pyogrio raw read using skip_features/max_features so we
    # never materialize all 157K geoms at once.
    read = 0
    offset = 0
    page = BATCH if limit is None else min(BATCH, limit)
    while True:
        if limit is not None and read >= limit:
            return
        n = page if limit is None else min(page, limit - read)
        meta, _fids, geoms, field_data = pyogrio.raw.read(
            str(gdb), layer=layer, skip_features=offset, max_features=n,
            read_geometry=True,
        )
        fields = list(meta["fields"])
        cnt = len(geoms) if geoms is not None else 0
        if cnt == 0:
            return
        # geoms come back as WKB bytes (pyogrio raw returns WKB array)
        for i in range(cnt):
            props = {fields[j]: field_data[j][i] for j in range(len(fields))}
            g = geoms[i]
            wkb = None
            if g is not None:
                if isinstance(g, (bytes, bytearray, memoryview)):
                    wkb = bytes(g)
                else:
                    wkb = shapely.to_wkb(g)
            yield props, wkb
        read += cnt
        offset += cnt
        if cnt < n:
            return


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn")
    ap.add_argument("--gdb", help="Path to the StratMap Nueces .gdb (skip "
                    "download).")
    ap.add_argument("--download", action="store_true",
                    help="Fetch + extract the StratMap zip to staging first.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap features (debug). Default: all.")
    ap.add_argument("--no-raw", action="store_true",
                    help="Do not store the full attribute dict in raw jsonb.")
    args = ap.parse_args()

    dsn = resolve_dsn(args.dsn)

    if args.gdb:
        gdb = Path(args.gdb)
    elif args.download:
        gdb = download_source()
    else:
        gdb = _find_gdb(STAGING_DIR)
    print(f"Using GDB: {gdb}", flush=True)

    started = time.time()
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    geom_ok = attr_ok = geom_skip = junk = 0

    with conn.cursor() as cur:
        cur.execute("SET lock_timeout = '15s'")
        cur.execute("SET statement_timeout = '60s'")
        ensure_tables(cur)
    conn.commit()

    geom_rows: list[dict] = []
    attr_rows: list[dict] = []

    def flush():
        nonlocal geom_ok, attr_ok, geom_skip
        if not geom_rows and not attr_rows:
            return
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '15s'")
            cur.execute("SET statement_timeout = '60s'")
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
        geom_rows.clear()
        attr_rows.clear()

    for props, wkb in iter_features(gdb, args.limit):
        pid = _clean(props.get("Prop_ID"))
        situs = _clean(props.get("SITUS_ADDR"))
        if _is_junk_parcel(pid, situs):
            junk += 1
            continue
        if wkb:
            geom_rows.append({
                "parcel_id": pid,
                "wkb": psycopg2.Binary(wkb),
                "source_county": SOURCE_COUNTY,
            })
        attr_rows.append({
            "parcel_id": pid,
            "cad_source": CAD_SOURCE,
            "tax_year": int(_clean(props.get("TAX_YEAR")) or date.today().year),
            "county_fips": COUNTY_FIPS,
            "situs_address": situs,
            "situs_city": _clean(props.get("SITUS_CITY")),
            "situs_state": _clean(props.get("SITUS_STAT")) or SITUS_STATE,
            "situs_zip": _clean(props.get("SITUS_ZIP")),
            "owner_name": _clean(props.get("OWNER_NAME")),
            "owner_address_full": _clean(props.get("MAIL_ADDR")),
            "subdivision": None,
            "legal_description": _clean(props.get("LEGAL_DESC")),
            "land_value": _num(props.get("LAND_VALUE")),
            "improvement_value": _num(props.get("IMP_VALUE")),
            "market_value": _num(props.get("MKT_VALUE")),
            "assessed_value": None,
            "year_built": _year_built(props.get("YEAR_BUILT")),
            "raw": None if args.no_raw else json.dumps(
                {k: (None if v is None else str(v)) for k, v in props.items()}
            ),
        })
        if len(attr_rows) >= BATCH:
            flush()
            if attr_ok % (BATCH * 10) == 0:
                print(f"  ... attr_ok={attr_ok} geom_ok={geom_ok} "
                      f"junk={junk} geom_skip={geom_skip}", flush=True)

    flush()
    conn.close()
    dt = time.time() - started
    print(f"DONE Nueces: geom_ok={geom_ok} attr_ok={attr_ok} "
          f"geom_skip={geom_skip} junk_skipped={junk} in {dt:.0f}s", flush=True)


if __name__ == "__main__":
    main()
