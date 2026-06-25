#!/usr/bin/env python3
"""
County 911 Address-Point Adapter — NENA NG911 leading-indicator trigger.

Pulls newly-created 911 address points from a county's NENA NG911 ArcGIS
Address_Points layer and lands them in hot_leads as NEW-ADDRESS triggers
(NOT building permits). A freshly created 911 address point is the earliest
public signal that a new structure is going up — it precedes the building
permit, the OSSF/septic permit, and the certificate of occupancy. For a
roofing / new-build lead platform this is the leading indicator.

Why this is generic
--------------------
The NENA NG911 GIS Data Model standardizes the address-point schema across
every county that publishes one:
    CR_DATETIME / AT_DATETIME / SP_DATETIME  (create/update/retire stamps)
    Full_Addr                                (the full street address)
    Owner                                    (parcel owner, when populated)
    PID                                      (parcel id)
    Site_NGUID                               (globally unique NENA id)
    Inc_Muni / Post_Code / County / State    (jurisdiction)
Adding another county is therefore a single COUNTIES registry entry
(county name + ArcGIS layer URL + optional field overrides), NOT new code.
This is the statewide-generalizing centerpiece of the TX lead platform.

Incremental
-----------
We track the last-seen CR_DATETIME high-water mark per county in the
hot_leads_sources ledger (latest_issue_date). Each run queries
    where = CR_DATETIME > <last_run_epoch_ms>
ordered CR_DATETIME DESC and paginates at maxRecordCount (~2000). Geometry
is requested in outSR=4326 because the source layers publish a projected SR
(e.g. EPSG:2278 TX South Central feet) and the layer's own Lat/Long
attribute columns are zero-filled.

hot_leads landing
-----------------
911 address points have NO permit number, so we leave permit_number NULL and
rely on the partial unique index
    idx_hot_leads_addr_source_null_permit ON (address, source) WHERE permit_number IS NULL
for dedup/upsert. Because permit_number is NULL these rows are intentionally
NOT promoted by bridge_hot_leads_to_permits.py into the building-permit table
— they are a separate trigger layer. permit_type / work_class are stamped
"NEW ADDRESS (911)" / "NEW-ADDRESS TRIGGER" so downstream consumers can tell
them apart from real permits.

Usage:
    python3 scrape_county_911_addresses.py --county brazoria
    python3 scrape_county_911_addresses.py --county brazoria --since-days 120
    python3 scrape_county_911_addresses.py --county brazoria --full        # ignore high-water mark
    python3 scrape_county_911_addresses.py --county brazoria --dry-run
    python3 scrape_county_911_addresses.py --list

Database target (defaults):
    host=100.122.216.15 dbname=permits user=will
    hot_leads upsert ON CONFLICT (address, source) WHERE permit_number IS NULL
    hot_leads_sources ledger row written per run (drives /v1/freshness)

Cron (PREPARED — DO NOT enable without sign-off):
    # County 911 new-address triggers — daily 05:05 CT, 2-day look-back
    5 5 * * * cd /home/will/permit-api-live && python3 scripts/scrape_county_911_addresses.py --county brazoria --since-days 2 >> /tmp/county_911_brazoria.log 2>&1
"""

import argparse
import re
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Optional

try:
    import httpx
except ImportError:
    print("ERROR: pip install httpx", file=sys.stderr)
    sys.exit(1)

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

# config
DB_HOST_DEFAULT = "100.122.216.15"
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

ARCGIS_UA = "Mozilla/5.0 (permit-scraper county-911)"
PAGE_SIZE = 2000  # matches layer maxRecordCount

# NENA NG911 standard field names. Per-county overrides go in the registry
# under "field_map" if a county renames a column.
NENA_DEFAULTS = {
    "address": "Full_Addr",
    "owner": "Owner",
    "parcel": "PID",
    "created": "CR_DATETIME",
    "updated": "AT_DATETIME",
    "nguid": "Site_NGUID",
    "muni": "Inc_Muni",
    "zip": "Post_Code",
    "county": "County",
    "state": "State",
}

# County registry. Add a county = add one entry here. No code changes.
COUNTIES = {
    "brazoria": {
        "url": "https://maps.brazoriacountytx.gov/arcgis/rest/services/general/Address_Points/MapServer/0",
        "county": "Brazoria",
        "state": "TX",
        "source": "brazoria_co_911_addresses",
        "field_map": {},  # pure NENA standard, no overrides
    },
}


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn(db_host):
    return psycopg2.connect(host=db_host, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def fields_for(cfg):
    fm = dict(NENA_DEFAULTS)
    fm.update(cfg.get("field_map") or {})
    return fm


def epoch_ms_to_date(v) -> Optional[date]:
    if v in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(float(v) / 1000.0, tz=timezone.utc).date()
    except Exception:
        return None


def extract_zip(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    m = re.search(r"\b(\d{5})(?:-\d{4})?\b", str(s))
    return m.group(1) if m else None


def arcgis_count(session, url, where) -> Optional[int]:
    try:
        r = session.get(
            f"{url}/query",
            params={"where": where, "returnCountOnly": "true", "f": "json"},
            timeout=30,
        )
        j = r.json()
        if "error" in j:
            return None
        return j.get("count")
    except Exception:
        return None


def build_where(created_field, since_dt, session, url):
    """Pick a date-filter WHERE clause this layer accepts.

    Brazoria's ArcGIS MapServer rejects the epoch-ms numeric compare that some
    FeatureServers accept (esriFieldTypeDate), but accepts ESRI date literals.
    We try TIMESTAMP first, then DATE, validating each with returnCountOnly so
    a new county that only supports one syntax still works. Returns the WHERE
    string (or the IS NOT NULL fallback if none validate)."""
    if since_dt is None:
        return f"{created_field} IS NOT NULL"
    ts = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    d = since_dt.strftime("%Y-%m-%d")
    for cand in (
        f"{created_field} > TIMESTAMP '{ts}'",
        f"{created_field} > DATE '{d}'",
        f"{created_field} > {int(since_dt.timestamp() * 1000)}",  # epoch-ms fallback
    ):
        if arcgis_count(session, url, cand) is not None:
            return cand
    log(f"    no date WHERE syntax validated for {created_field}; falling back to IS NOT NULL")
    return f"{created_field} IS NOT NULL"


def arcgis_fetch_addresses(url, created_field, since_dt, full=False, max_records=200000):
    """Page newly-created 911 address points created after since_dt (a tz-aware
    datetime) or all of them when full=True.

    Returns a list of ArcGIS features (attributes + geometry in WGS84).
    """
    session = httpx.Client(
        timeout=45, headers={"User-Agent": ARCGIS_UA}, follow_redirects=True
    )

    if full or since_dt is None:
        where = f"{created_field} IS NOT NULL"
    else:
        where = build_where(created_field, since_dt, session, url)

    total = arcgis_count(session, url, where)
    if total is not None:
        log(f"    server reports {total} matching address points")

    all_feats = []
    offset = 0
    while True:
        params = {
            "where": where,
            "outFields": "*",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "orderByFields": f"{created_field} DESC",
            "returnGeometry": "true",
            "outSR": 4326,  # force WGS84 lat/lng out of the projected source SR
        }
        try:
            r = session.get(f"{url}/query", params=params, timeout=60)
            data = r.json()
        except Exception as e:
            log(f"    ArcGIS error at offset {offset}: {e}")
            break
        if "error" in data:
            log(f"    ArcGIS returned error: {data.get('error')}")
            break
        feats = data.get("features", [])
        if not feats:
            break
        all_feats.extend(feats)
        log(f"    fetched {len(all_feats)} so far...")
        if len(feats) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        if offset >= max_records:
            log(f"    hit safety cap {max_records}")
            break
        time.sleep(0.5)
    session.close()
    return all_feats


def normalize_911(feature, cfg, fm):
    attrs = feature.get("attributes") or {}
    geom = feature.get("geometry") or {}

    address = attrs.get(fm["address"])
    address = str(address).strip() if address else None
    if not address:
        return None  # an address point with no address is useless as a lead

    owner = attrs.get(fm["owner"])
    parcel = attrs.get(fm["parcel"])
    nguid = attrs.get(fm["nguid"])
    created = epoch_ms_to_date(attrs.get(fm["created"]))
    updated = epoch_ms_to_date(attrs.get(fm["updated"]))

    muni = attrs.get(fm["muni"]) or ""
    city = str(muni).strip().upper() if muni else None
    if city == "":
        city = None

    zip_code = extract_zip(attrs.get(fm["zip"])) or extract_zip(address)
    county = (attrs.get(fm["county"]) or cfg["county"])
    county = county.strip() if isinstance(county, str) else cfg["county"]
    st = attrs.get(fm["state"])
    state = (st.strip()[:2].upper() if isinstance(st, str) and st.strip() else cfg["state"])

    lng = geom.get("x")
    lat = geom.get("y")
    if (lat in (0, 0.0)) and (lng in (0, 0.0)):
        lat = lng = None

    desc_bits = []
    if parcel:
        desc_bits.append(f"Parcel: {parcel}")
    if nguid:
        desc_bits.append(f"NENA: {nguid}")
    if updated:
        desc_bits.append(f"Updated: {updated}")
    description = " | ".join(desc_bits) if desc_bits else None

    return {
        # permit_number intentionally NULL — this is a 911 trigger, not a permit.
        # Dedup happens on (address, source) via the partial unique index.
        "permit_number": None,
        "permit_type": "NEW ADDRESS (911)",
        "work_class": "NEW-ADDRESS TRIGGER",
        "description": description,
        "address": address[:200],
        "city": (city[:100]) if city else None,
        "state": state,
        "zip": zip_code,
        "county": county,
        "lat": lat,
        "lng": lng,
        "issue_date": created,
        "applied_date": created,
        "status": "NEW ADDRESS",
        "owner_name": (str(owner)[:200]) if owner else None,
        "jurisdiction": f"{cfg['county']} County, {cfg['state']}",
        "source": cfg["source"],
    }


HOT_LEADS_COLS = [
    "id", "permit_number", "permit_type", "work_class", "description",
    "address", "city", "state", "zip", "county", "lat", "lng",
    "issue_date", "applied_date", "status", "owner_name",
    "jurisdiction", "source",
]


def upsert_hot_leads(conn, rows_in):
    """Upsert 911 address points. permit_number is NULL for all of these, so
    we conflict on the partial unique index (address, source)."""
    if not rows_in:
        return 0
    deduped = {}
    for p in rows_in:
        if not p.get("address"):
            continue
        deduped[(p["address"], p["source"])] = p
    rows = [
        (
            str(uuid.uuid4()),
            p["permit_number"], p["permit_type"], p["work_class"], p["description"],
            p["address"], p["city"], p["state"], p["zip"], p["county"],
            p["lat"], p["lng"], p["issue_date"], p["applied_date"],
            p["status"], p["owner_name"], p["jurisdiction"], p["source"],
        )
        for p in deduped.values()
    ]
    sql = f"""
        INSERT INTO hot_leads ({', '.join(HOT_LEADS_COLS)})
        VALUES %s
        ON CONFLICT (address, source) WHERE permit_number IS NULL
        DO UPDATE SET
            city = COALESCE(EXCLUDED.city, hot_leads.city),
            zip = COALESCE(EXCLUDED.zip, hot_leads.zip),
            county = COALESCE(EXCLUDED.county, hot_leads.county),
            lat = COALESCE(EXCLUDED.lat, hot_leads.lat),
            lng = COALESCE(EXCLUDED.lng, hot_leads.lng),
            issue_date = COALESCE(EXCLUDED.issue_date, hot_leads.issue_date),
            applied_date = COALESCE(EXCLUDED.applied_date, hot_leads.applied_date),
            status = COALESCE(EXCLUDED.status, hot_leads.status),
            owner_name = COALESCE(EXCLUDED.owner_name, hot_leads.owner_name),
            description = COALESCE(EXCLUDED.description, hot_leads.description),
            scraped_at = CURRENT_DATE
    """
    cur = conn.cursor()
    try:
        execute_values(cur, sql, rows, page_size=1000)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        log(f"  hot_leads upsert error: {e}")
        return 0
    finally:
        cur.close()


def ensure_sources_table(conn):
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hot_leads_sources (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source_name TEXT NOT NULL,
                state TEXT,
                file_name TEXT,
                records_loaded INTEGER DEFAULT 0,
                records_skipped INTEGER DEFAULT 0,
                latest_issue_date DATE,
                loaded_at TIMESTAMPTZ DEFAULT NOW(),
                error_message TEXT
            )
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"  (sources table ensure: {e})")
    finally:
        cur.close()


def get_high_water_mark(conn, source_name) -> Optional[date]:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT MAX(latest_issue_date) FROM hot_leads_sources WHERE source_name = %s",
            (source_name,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None
    finally:
        cur.close()


def record_source(conn, source_name, state, loaded, latest_date, error=None):
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO hot_leads_sources
               (source_name, state, file_name, records_loaded, records_skipped, latest_issue_date, error_message)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (source_name, state, "arcgis_911_address_points", loaded, 0, latest_date, error),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"  (record_source: {e})")
    finally:
        cur.close()


def run_county(county_key, cfg, conn, since_days, full, dry_run):
    fm = fields_for(cfg)
    created_field = fm["created"]
    log(f"[{county_key}] source={cfg['source']} url={cfg['url']}")

    since_dt = None
    if not full:
        hwm = None
        if conn is not None:
            hwm = get_high_water_mark(conn, cfg["source"])
        if hwm is not None:
            since_dt = datetime(hwm.year, hwm.month, hwm.day, tzinfo=timezone.utc) - timedelta(days=1)
            log(f"  incremental from ledger high-water mark {hwm} (re-pull since {since_dt.date()})")
        else:
            since_dt = datetime.now(timezone.utc) - timedelta(days=since_days)
            log(f"  no ledger history — first run, look-back {since_days} days (since {since_dt.date()})")

    feats = arcgis_fetch_addresses(cfg["url"], created_field, since_dt, full=full)
    normalized = [normalize_911(f, cfg, fm) for f in feats]
    normalized = [p for p in normalized if p]
    log(f"  normalized {len(normalized)} address points with an address")

    latest_date = None
    for p in normalized:
        if p["issue_date"] and (latest_date is None or p["issue_date"] > latest_date):
            latest_date = p["issue_date"]

    loaded = 0
    if dry_run:
        log("  DRY RUN — not writing. Sample:")
        for p in normalized[:8]:
            log(f"    {p['issue_date']} | {p['address']} | owner={p['owner_name']} | "
                f"{p['city']} {p['zip']} | {p['lat']},{p['lng']}")
    elif normalized:
        loaded = upsert_hot_leads(conn, normalized)
        record_source(conn, cfg["source"], cfg["state"], loaded, latest_date)
        log(f"  upserted {loaded} into hot_leads; ledger latest_issue_date={latest_date}")
    else:
        log("  nothing to load")
        if conn is not None:
            record_source(conn, cfg["source"], cfg["state"], 0, latest_date)

    return len(normalized), loaded


def main():
    parser = argparse.ArgumentParser(description="County 911 address-point adapter")
    parser.add_argument("--county", choices=list(COUNTIES.keys()), help="County to pull")
    parser.add_argument("--all", action="store_true", help="Pull every registered county")
    parser.add_argument("--since-days", type=int, default=90,
                        help="First-run look-back window in days (default 90)")
    parser.add_argument("--full", action="store_true",
                        help="Ignore the high-water mark and pull everything")
    parser.add_argument("--db-host", default=DB_HOST_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--list", action="store_true", help="List registered counties")
    args = parser.parse_args()

    if args.list:
        print("Registered counties:")
        for k, v in COUNTIES.items():
            print(f"  {k:14s} {v['county']} County, {v['state']:2s}  source={v['source']}")
            print(f"                 {v['url']}")
        return

    if not args.county and not args.all:
        parser.error("must provide --county or --all (or --list)")

    targets = [args.county] if args.county else list(COUNTIES.keys())

    log("=" * 64)
    log(f"COUNTY 911 ADDRESS-POINT ADAPTER — {len(targets)} county/counties")
    log(f"Targets: {', '.join(targets)} | full={args.full} since_days={args.since_days}")
    log("=" * 64)

    conn = None
    if not args.dry_run:
        conn = get_conn(args.db_host)
        ensure_sources_table(conn)
        log(f"DB connected: {args.db_host}/{DB_NAME}")

    summary = []
    for ck in targets:
        try:
            fetched, loaded = run_county(
                ck, COUNTIES[ck], conn, args.since_days, args.full, args.dry_run
            )
            summary.append((ck, fetched, loaded))
        except Exception as e:
            log(f"  ERROR {ck}: {e}")
            summary.append((ck, 0, 0))

    log("=" * 64)
    for ck, f, l in summary:
        log(f"  {ck:14s} normalized={f:>7} loaded={l:>7}")
    log("=" * 64)

    if conn:
        conn.close()
    log("Done.")


if __name__ == "__main__":
    main()
