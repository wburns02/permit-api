#!/usr/bin/env python3
"""
TCAD (Travis County / Austin Appraisal District) -> tx_cad_parcels loader.

Source: Travis County GIS REST layer (FREE, no token):
  https://taxmaps.traviscountytx.gov/arcgis/rest/services/Parcels/MapServer/0
  ~373,683 parcels, maxRecordCount 2000.

Mirrors load_bcad.py exactly. Geometry is already held in
travis_parcel_geometries (parcel_id = PROP_ID::text), so this loader only
fetches ATTRIBUTES (returnGeometry=false) and upserts them into
tx_cad_parcels so the unserviced_hail_leads MV can join geom.parcel_id =
tx_cad_parcels.parcel_id AND cad_source='TCAD'.

Field mapping (tx_cad_parcels <- TCAD):
  parcel_id      = PROP_ID (cast to text, plain integer string, NO zero-pad —
                   IDENTICAL to travis_parcel_geometries.parcel_id)
  cad_source     = 'TCAD'
  tax_year       = 2026
  county_fips    = '48453'
  situs_address  = situs_address (whitespace-collapsed; the layer embeds the
                   full mailable street + ' TX ' + zip with doubled spaces)
  situs_city     = NULL (layer has no clean city field)
  situs_zip      = situs_zip (first 5 if numeric)
  owner_name     = py_owner_name
  year_built     = F1year_imprv (validate plausible 1700-2030 else NULL)
  market_value   = market_value
  assessed_value = assessed_val (often NULL on this layer)

DB: pass --dsn "postgresql://will@100.122.216.15:5432/permits" (T430) or rely
on PGHOST/peer auth (dbname=permits) when run ON the T430. Mirrors load_bcad.py.
"""
from __future__ import annotations
import argparse
import os
import sys
import time

import httpx
import psycopg2
from psycopg2.extras import execute_values

SERVICE_URL = "https://taxmaps.traviscountytx.gov/arcgis/rest/services/Parcels/MapServer/0"
QUERY_URL = f"{SERVICE_URL}/query"
CAD = "TCAD"
TAX_YEAR = 2026
COUNTY_FIPS = "48453"
PAGE = 2000  # layer maxRecordCount
OUT_FIELDS = (
    "PROP_ID,geo_id,situs_address,situs_num,situs_street,situs_zip,"
    "py_owner_name,F1year_imprv,market_value,assessed_val,legal_desc"
)


def s(x):
    if x is None:
        return None
    if not isinstance(x, str):
        x = str(x)
    x = x.strip()
    if not x or x.upper() == "NULL":
        return None
    return x


def collapse(x):
    """Trim + collapse internal whitespace runs to single spaces."""
    v = s(x)
    if not v:
        return None
    return " ".join(v.split())


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


def year_ok(x):
    n = num(x)
    if n is None:
        return None
    n = int(n)
    if n < 1700 or n > 2030:
        return None
    return n


def propid_to_text(v):
    """PROP_ID -> text, no decimal. IDENTICAL to travis_parcel_geometries."""
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


def fetch_count(client):
    r = client.get(QUERY_URL, params={
        "where": "1=1", "returnCountOnly": "true", "f": "json"})
    return r.json().get("count", 0)


def fetch_page(client, offset):
    params = {
        "where": "1=1",
        "outFields": OUT_FIELDS,
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": PAGE,
        "resultOffset": offset,
        "orderByFields": "OBJECTID ASC",
    }
    r = client.get(QUERY_URL, params=params, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    j = r.json()
    if j.get("error"):
        raise RuntimeError(f"AGS error: {j['error']}")
    return j


def fetch_keyset(client, last_oid, upper_oid):
    """Keyset page: OBJECTID > last_oid (and <= upper_oid if bounded)."""
    where = f"OBJECTID>{last_oid}"
    if upper_oid:
        where += f" AND OBJECTID<={upper_oid}"
    params = {
        "where": where,
        "outFields": "OBJECTID," + OUT_FIELDS,
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
        owner_name,
        year_built, legal_description,
        market_value, assessed_value,
        county_fips
    ) VALUES %s
    ON CONFLICT (cad_source, parcel_id, tax_year) DO NOTHING
"""


def to_row(a):
    pid = propid_to_text(a.get("PROP_ID"))
    if not pid:
        return None
    return (
        pid, CAD, TAX_YEAR,
        collapse(a.get("situs_address")), None, "TX", zip5(a.get("situs_zip")),
        s(a.get("py_owner_name")),
        year_ok(a.get("F1year_imprv")), s(a.get("legal_desc")),
        i64(a.get("market_value")), i64(a.get("assessed_val")),
        COUNTY_FIPS,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default=os.environ.get("PGHOST", ""))
    ap.add_argument("--db", default="permits")
    ap.add_argument("--dsn", default=os.environ.get("TCAD_DSN", ""))
    ap.add_argument("--page-limit", type=int, default=0)
    ap.add_argument("--resume-from", type=int, default=0)
    ap.add_argument("--offset-end", type=int, default=0)
    ap.add_argument("--no-delete", action="store_true")
    ap.add_argument("--keyset", action="store_true")
    ap.add_argument("--oid-start", type=int, default=0)
    ap.add_argument("--oid-end", type=int, default=0)
    args = ap.parse_args()

    t0 = time.time()
    if args.dsn:
        conn = psycopg2.connect(args.dsn, connect_timeout=20)
    else:
        conn = psycopg2.connect(dbname=args.db, host=args.host or None,
                                connect_timeout=20)
    conn.autocommit = False
    cur = conn.cursor()
    # Be gentle on a recovering box: short lock_timeout on writes.
    cur.execute("SET lock_timeout = '15s'")
    conn.commit()
    if not args.no_delete:
        cur.execute(
            "DELETE FROM public.tx_cad_parcels WHERE cad_source=%s AND tax_year=%s",
            (CAD, TAX_YEAR),
        )
        print(f"  deleted prior {CAD} {TAX_YEAR} rows: {cur.rowcount:,}", flush=True)
        conn.commit()

    with httpx.Client(timeout=120, http2=False,
                      headers={"User-Agent": "Mozilla/5.0 (ecbtx free-data ingest; travis parcels)"}) as client:
        total = fetch_count(client)
        print(f"[{CAD}] total parcels: {total:,}", flush=True)

        offset = args.resume_from
        last_oid = args.oid_start
        page_no = inserted = seen = 0
        errs = 0
        while True:
            if args.page_limit and page_no >= args.page_limit:
                print(f"[{CAD}] page-limit {args.page_limit} reached", flush=True)
                break
            if not args.keyset and args.offset_end and offset >= args.offset_end:
                print(f"[{CAD}] offset-end {args.offset_end} reached", flush=True)
                break
            try:
                if args.keyset:
                    j = fetch_keyset(client, last_oid, args.oid_end)
                else:
                    j = fetch_page(client, offset)
                errs = 0
            except Exception as e:
                errs += 1
                loc = f"oid>{last_oid}" if args.keyset else f"offset={offset}"
                print(f"[{CAD}] fetch err @ {loc}: {e}", flush=True)
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
                if args.keyset:
                    oid = attrs.get("OBJECTID")
                    if isinstance(oid, (int, float)) and oid > max_oid:
                        max_oid = int(oid)
                row = to_row(attrs)
                if row:
                    batch.append(row)
            if batch:
                execute_values(cur, INSERT_SQL, batch, page_size=PAGE)
                inserted += len(batch)
            conn.commit()
            page_no += 1
            offset += len(feats)
            if args.keyset:
                last_oid = max_oid

            if page_no % 25 == 1 or page_no <= 3:
                el = time.time() - t0
                rate = inserted / el if el else 0
                pos = f"oid={last_oid}" if args.keyset else f"offset={offset}"
                eta = (total - inserted) / rate / 60 if rate else 0
                print(f"[{CAD}] page {page_no} {pos} "
                      f"inserted_total={inserted:,} rate={rate:.0f}/s "
                      f"eta={eta:.1f}m", flush=True)

            if len(feats) < PAGE and not j.get("exceededTransferLimit"):
                print(f"[{CAD}] last partial page", flush=True)
                break
            time.sleep(0.1)

    cur.close()
    conn.close()
    print(f"[{CAD}] DONE seen={seen:,} inserted={inserted:,} in "
          f"{(time.time()-t0)/60:.1f}m", flush=True)


if __name__ == "__main__":
    main()
