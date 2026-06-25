#!/usr/bin/env python3
"""BRAZORIACAD (Brazoria County AD) -> tx_cad_parcels loader.

Source: Brazoria County GIS REST layer (FREE, no token) — the same ArcGIS
server the 911 address scraper already uses:
  https://maps.brazoriacountytx.gov/arcgis/rest/services/general/Parcels/MapServer/1
("Parcel Information" layer — carries owner, situs, appraised value, subdivision).

This is Phase 4 (contact enrichment) of the Brazoria TX permit-lead feed. The
attributes loaded here are joined to `brazoria_permit_leads` by normalized
situs address to attribute owner_name + a canonical mailable situs to the
~1,905 leads that arrived without one.

Field mapping (tx_cad_parcels <- BRAZORIACAD Parcel Information):
  parcel_id          = prop_id (cast to text, no decimal)
  cad_source         = 'BRAZORIACAD'
  tax_year           = 2026
  county_fips        = '48039'
  situs_address      = SITUS, collapsed whitespace (fallback: situs_num + situs_street)
  situs_city         = situs_city (trimmed)
  situs_state        = 'TX'
  situs_zip          = situs_zip (first 5 if numeric)
  owner_name         = py_owner_name (trimmed)
  legal_description  = legal_desc (+ legal_desc2 if present)
  subdivision        = abs_subdv_desc
  market_value       = appraised_val
  assessed_value     = appraised_val
  lot_acres          = legal_acreage (fallback Land_Acreage)
  year_built         = NULL  (not exposed on this layer; bonus field, optional)
  geo_id             = stored into raw->>'geo_id'

This loader mirrors free_data_ingest/tx_cad/load_bcad.py: streams the layer in
keyset pages (OBJECTID range scan), inserts with ON CONFLICT DO NOTHING on the
(cad_source, parcel_id, tax_year) unique key. Geometry is intentionally NOT
loaded — the owner/address join needs only attributes.

DB: defaults to T430 (host 100.122.216.15 dbname=permits) or a full DSN via
--dsn / $BRAZORIACAD_DSN / $PERMITS_DSN. No large file is staged — the loader
streams straight from ArcGIS into Postgres (Storage Policy: nothing lands on
/home/will).
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys
import time

import httpx
import psycopg2
from psycopg2.extras import execute_values

SERVICE_URL = (
    "https://maps.brazoriacountytx.gov/arcgis/rest/services/"
    "general/Parcels/MapServer/1"
)
QUERY_URL = f"{SERVICE_URL}/query"
CAD = "BRAZORIACAD"
TAX_YEAR = 2026
COUNTY_FIPS = "48039"
PAGE = 1000
OUT_FIELDS = (
    "OBJECTID,prop_id,geo_id,py_owner_name,SITUS,situs_num,situs_street,"
    "situs_street_prefx,situs_street_suffix,situs_unit,situs_city,situs_zip,"
    "appraised_val,abs_subdv_desc,legal_desc,legal_desc2,legal_acreage,"
    "Land_Acreage"
)


def s(x):
    if x is None:
        return None
    if not isinstance(x, str):
        x = str(x)
    x = re.sub(r"\s+", " ", x).strip()
    if not x or x.upper() == "NULL":
        return None
    return x


def num(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    x = x.strip()
    if not x or x.upper() == "NULL":
        return None
    try:
        return float(x)
    except ValueError:
        return None


def i64(x):
    n = num(x)
    return int(n) if n is not None else None


def propid_to_text(v):
    """prop_id -> text, no decimal."""
    if v is None:
        return None
    if isinstance(v, float):
        v = int(v)
    elif isinstance(v, str):
        v = v.strip()
        if not v or v.upper() == "NULL":
            return None
        try:
            v = int(float(v))
        except ValueError:
            pass
    out = str(v).strip()
    return out or None


def zip5(x):
    z = s(x)
    if not z:
        return None
    z = z[:5]
    return z if z.isdigit() else None


def build_situs(a):
    """Prefer the assembled SITUS; fall back to the structured parts."""
    situs = s(a.get("SITUS"))
    if situs:
        return situs
    parts = [
        s(a.get("situs_num")),
        s(a.get("situs_street_prefx")),
        s(a.get("situs_street")),
        s(a.get("situs_street_suffix")),
        s(a.get("situs_unit")),
    ]
    joined = " ".join(p for p in parts if p)
    return joined or None


def build_legal(a):
    parts = [s(a.get("legal_desc")), s(a.get("legal_desc2"))]
    joined = " ".join(p for p in parts if p)
    return joined or None


def fetch_count(client, situs_filter=None):
    where = f"({situs_filter})" if situs_filter else "1=1"
    r = client.get(QUERY_URL, params={
        "where": where, "returnCountOnly": "true", "f": "json"})
    return r.json().get("count", 0)


def fetch_keyset(client, last_oid, upper_oid, situs_filter=None):
    """Keyset page: OBJECTID > last_oid (and <= upper_oid if bounded).

    situs_filter (optional) is an extra ArcGIS WHERE clause, e.g. a
    `situs_city IN (...)` restriction so a load can target only the
    jurisdictions that have leads (useful when the box is under IO load and a
    full-county pull would take hours)."""
    where = f"OBJECTID>{last_oid}"
    if upper_oid:
        where += f" AND OBJECTID<={upper_oid}"
    if situs_filter:
        where += f" AND ({situs_filter})"
    params = {
        "where": where,
        "outFields": OUT_FIELDS,
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": PAGE,
        "orderByFields": "OBJECTID ASC",
    }
    r = client.get(QUERY_URL, params=params, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    j = r.json()
    if j.get("error"):
        raise RuntimeError(f"AGS error: {j['error']}")
    return j


INSERT_SQL = """
    INSERT INTO public.tx_cad_parcels (
        parcel_id, cad_source, tax_year,
        situs_address, situs_city, situs_state, situs_zip,
        owner_name, legal_description, subdivision,
        lot_acres, year_built,
        market_value, assessed_value,
        county_fips, raw
    ) VALUES %s
    ON CONFLICT (cad_source, parcel_id, tax_year) DO NOTHING
"""


def to_row(a):
    pid = propid_to_text(a.get("prop_id"))
    if not pid:
        return None
    appr = i64(a.get("appraised_val"))
    acres = num(a.get("legal_acreage"))
    if acres is None:
        acres = num(a.get("Land_Acreage"))
    raw = json.dumps({"geo_id": s(a.get("geo_id"))})
    return (
        pid, CAD, TAX_YEAR,
        build_situs(a), s(a.get("situs_city")), "TX", zip5(a.get("situs_zip")),
        s(a.get("py_owner_name")), build_legal(a), s(a.get("abs_subdv_desc")),
        acres, None,
        appr, appr,
        COUNTY_FIPS, raw,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("PGHOST", ""))
    ap.add_argument("--db", default="permits")
    ap.add_argument("--user", default=os.environ.get("PGUSER", ""))
    ap.add_argument("--dsn", default=os.environ.get("BRAZORIACAD_DSN",
                    os.environ.get("PERMITS_DSN", "")))
    ap.add_argument("--page-limit", type=int, default=0)
    ap.add_argument("--oid-start", type=int, default=0,
                    help="keyset: start AFTER this OBJECTID (exclusive lower)")
    ap.add_argument("--oid-end", type=int, default=0,
                    help="keyset: stop AT this OBJECTID (inclusive upper); 0=no bound")
    ap.add_argument("--no-delete", action="store_true",
                    help="skip the startup DELETE (sharded runs)")
    ap.add_argument("--situs-cities", default="",
                    help="comma-separated situs_city list to restrict the pull "
                         "(e.g. 'ANGLETON,ALVIN'). Empty = whole county. Targets "
                         "only lead jurisdictions when the box is IO-bound.")
    ap.add_argument("--commit-every", type=int, default=1,
                    help="commit every N pages instead of every page — fewer "
                         "fsyncs under IO contention (default 1).")
    args = ap.parse_args()

    situs_filter = None
    if args.situs_cities.strip():
        cities = [c.strip().upper().replace("'", "''")
                  for c in args.situs_cities.split(",") if c.strip()]
        in_list = ",".join(f"'{c}'" for c in cities)
        situs_filter = f"UPPER(situs_city) IN ({in_list})"

    t0 = time.time()
    if args.dsn:
        conn = psycopg2.connect(args.dsn, connect_timeout=20)
    else:
        conn = psycopg2.connect(
            dbname=args.db, host=args.host or None,
            user=args.user or None, connect_timeout=20)
    conn.autocommit = False
    cur = conn.cursor()
    if not args.no_delete:
        cur.execute(
            "DELETE FROM public.tx_cad_parcels WHERE cad_source=%s AND tax_year=%s",
            (CAD, TAX_YEAR),
        )
        print(f"  deleted prior {CAD} {TAX_YEAR} rows: {cur.rowcount:,}", flush=True)
        conn.commit()

    with httpx.Client(timeout=120, http2=False, headers={
            "User-Agent": "Mozilla/5.0 (ecbtx free-data ingest; brazoria parcels)"}) as client:
        total = fetch_count(client, situs_filter)
        scope = f" (situs filter: {args.situs_cities})" if situs_filter else ""
        print(f"[{CAD}] total parcels{scope}: {total:,}", flush=True)

        last_oid = args.oid_start
        page_no = inserted = seen = errs = pending = 0
        while True:
            if args.page_limit and page_no >= args.page_limit:
                print(f"[{CAD}] page-limit {args.page_limit} reached", flush=True)
                break
            try:
                j = fetch_keyset(client, last_oid, args.oid_end, situs_filter)
                errs = 0
            except Exception as e:  # noqa: BLE001
                errs += 1
                print(f"[{CAD}] fetch err @ oid>{last_oid}: {e}", flush=True)
                if errs >= 6:
                    print(f"[{CAD}] 6 consecutive errors, aborting", flush=True)
                    sys.exit(2)
                time.sleep(3 * errs)
                continue

            feats = j.get("features", [])
            if not feats:
                print(f"[{CAD}] no more features", flush=True)
                break

            batch = []
            max_oid = last_oid
            for f in feats:
                seen += 1
                attrs = f.get("attributes") or {}
                oid = attrs.get("OBJECTID")
                if isinstance(oid, (int, float)) and oid > max_oid:
                    max_oid = int(oid)
                row = to_row(attrs)
                if row:
                    batch.append(row)
            if batch:
                execute_values(cur, INSERT_SQL, batch, page_size=PAGE)
                inserted += len(batch)
            pending += 1
            page_no += 1
            last_oid = max_oid
            # Commit every N pages — fewer fsyncs when the box is IO-bound.
            if pending >= args.commit_every:
                conn.commit()
                pending = 0

            if page_no % 25 == 1 or page_no <= 3:
                el = time.time() - t0
                rate = inserted / el if el else 0
                eta = (total - inserted) / rate / 60 if rate else 0
                print(f"[{CAD}] page {page_no} oid={last_oid} "
                      f"inserted_total={inserted:,} rate={rate:.0f}/s "
                      f"eta={eta:.1f}m", flush=True)

            # Terminal condition. With NO situs filter, OBJECTID pages are dense,
            # so a sub-PAGE page with no exceededTransferLimit means done. With a
            # filter, pages are SPARSE (matches scattered across OBJECTID space) —
            # a short page does NOT mean done; only an EMPTY fetch (caught above)
            # or hitting the layer's max OBJECTID does. Page until empty.
            if not situs_filter and len(feats) < PAGE and not j.get("exceededTransferLimit"):
                print(f"[{CAD}] last partial page", flush=True)
                break
            time.sleep(0.1)
        if pending:
            conn.commit()

    cur.close()
    conn.close()
    print(f"[{CAD}] DONE seen={seen:,} inserted={inserted:,} in "
          f"{(time.time()-t0)/60:.1f}m", flush=True)


if __name__ == "__main__":
    main()
