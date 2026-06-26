#!/usr/bin/env python3
"""
Generic ArcGIS permit FeatureServer/MapServer adapter — TX city building permits.

Many TX cities publish their building-permit case data on a public ArcGIS REST
endpoint (Esri MapServer/FeatureServer) with no auth and no captcha. The schema
varies city-to-city only in field NAMES, so this adapter is a registry: adding a
new city is ONE entry in CITIES (service base URL + per-layer paths + a field
map), NOT new code. Each city can expose several layers (residential +
commercial); each layer is pulled and landed in hot_leads under the same source.

First registered city: Pearland (Brazoria County). eTRAKiT was ABANDONED for
Pearland — `etrakit.pearlandtx.gov` is network-blackholed from us (HTTP 000,
see docs/tx-permit-leads-plan.md). The city GIS FeatureServer is the live source:
    https://gis.pearlandtx.gov/hosting/rest/services
        Residential_Permits/MapServer/0   (new custom/tract homes, remodels,
                                            additions, accessory, demos)
        Commercial_Permits/MapServer/0    (commercial new/alteration/demo, etc.)

Pearland schema (both layers):
    OjectID, DATE_ISSUED, BUS_CASE_DESC, CASE_STATUS, LOCATION, CX, CY,
    CASE_NAME, CASE_NUMBER, Applicant, PropertyOwner, SHAPE
Note: the OID field is literally named "OjectID" (the city's typo). DATE_ISSUED
is a STRING 'YYYY-MM-DD HH:MM' (NOT an esri date) — lexically sortable, so the
incremental WHERE `DATE_ISSUED >= 'YYYY-MM-DD'` works and we parse it to a real
timestamp on ingest. CX/CY are Web Mercator (EPSG:3857) projected coords, NOT
lat/lng — so we request geometry with outSR=4326 and read lat=geometry.y,
lng=geometry.x (Pearland ~29.55N, -95.29W).

Field map -> hot_leads:
    address       = LOCATION
    permit_type   = BUS_CASE_DESC   (also work_class — single category field)
    issue_date    = DATE_ISSUED     (parsed string)
    status        = CASE_STATUS
    owner_name    = PropertyOwner
    applicant     = Applicant       (-> description, since hot_leads has no
                                      dedicated applicant column)
    permit_number = CASE_NUMBER
    lat/lng       = geometry (outSR=4326)

hot_leads landing:
    Pearland rows carry a permit_number (CASE_NUMBER), so dedup/upsert uses the
    UNIQUE index ix_hot_leads_permit (permit_number, source). The bridge
    (bridge_hot_leads_to_permits.py) promotes them into `permits`, and the
    brazoria_permit_leads MV picks them up because 'pearland_permits' is now a
    registered Brazoria source in app/services/permit_lead_classify.BRAZORIA_SOURCES.

Incremental:
    High-water mark per source is the MAX(latest_issue_date) in the
    hot_leads_sources ledger. Each run queries
        where = DATE_ISSUED >= '<hwm-with-lookback>'
    (string compare; DATE_ISSUED is lexically sortable) and paginates past the
    layer's maxRecordCount (2000) via resultOffset.

Usage:
    python3 scrape_arcgis_permits.py --city pearland
    python3 scrape_arcgis_permits.py --city pearland --since-days 90
    python3 scrape_arcgis_permits.py --city pearland --full        # ignore HWM
    python3 scrape_arcgis_permits.py --city pearland --dry-run
    python3 scrape_arcgis_permits.py --list

Cron (PREPARED — DO NOT enable without sign-off):
    # Pearland city-GIS building permits — daily 05:25 CT, 7-day look-back
    25 5 * * * cd /home/will/permit-api-live && python3 scripts/scrape_arcgis_permits.py --city pearland --since-days 7 >> /tmp/arcgis_permits_pearland.log 2>&1
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

ARCGIS_UA = "Mozilla/5.0 (permit-scraper arcgis-permits)"
PAGE_SIZE = 2000  # matches layer maxRecordCount

# Default Esri-permit field map. Cities that rename columns override per-city
# (or per-layer) in the CITIES registry under "field_map".
ESRI_PERMIT_DEFAULTS = {
    "address": "LOCATION",
    "category": "BUS_CASE_DESC",   # both permit_type and work_class
    "issued": "DATE_ISSUED",       # STRING 'YYYY-MM-DD HH:MM'
    "status": "CASE_STATUS",
    "owner": "PropertyOwner",
    "applicant": "Applicant",
    "permit_number": "CASE_NUMBER",
    "case_name": "CASE_NAME",
}

# City registry. Add a city = add one entry here. No code changes.
#   url:      service base (informational)
#   layers:   list of full layer query bases (each pulled, same source)
#   county / state / source / field_map / city
CITIES = {
    "pearland": {
        "url": "https://gis.pearlandtx.gov/hosting/rest/services",
        "layers": [
            "https://gis.pearlandtx.gov/hosting/rest/services/Residential_Permits/MapServer/0",
            "https://gis.pearlandtx.gov/hosting/rest/services/Commercial_Permits/MapServer/0",
        ],
        "city": "Pearland",
        "county": "Brazoria",
        "state": "TX",
        "source": "pearland_permits",
        "field_map": {},  # pure Esri-permit standard (Pearland's native schema)
    },
}


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn(db_host):
    # Hard caps so a heavily-loaded box is never put under an unbounded scan.
    return psycopg2.connect(
        host=db_host, port=DB_PORT, dbname=DB_NAME, user=DB_USER,
        connect_timeout=10,
        options="-c statement_timeout=60000 -c lock_timeout=10000",
    )


def fields_for(cfg):
    fm = dict(ESRI_PERMIT_DEFAULTS)
    fm.update(cfg.get("field_map") or {})
    return fm


def parse_issue_dt(v) -> Optional[datetime]:
    """Parse the DATE_ISSUED string 'YYYY-MM-DD HH:MM' (hour may be 1 digit) to a
    naive datetime. Falls back to date-only. Returns None if unparseable."""
    if v in (None, ""):
        return None
    s = str(v).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    # last resort: take the leading date token
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d")
        except ValueError:
            return None
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


def arcgis_fetch_permits(url, issued_field, since_dt, full=False, max_records=200000):
    """Page permit features issued on/after since_dt (a date) or all when full.

    DATE_ISSUED is a STRING and lexically sortable, so the WHERE is a plain
    string compare `DATE_ISSUED >= 'YYYY-MM-DD'` and we order by it ASC/DESC as
    a string. Returns a list of ArcGIS features (attributes + WGS84 geometry).
    """
    session = httpx.Client(
        timeout=45, headers={"User-Agent": ARCGIS_UA}, follow_redirects=True
    )

    if full or since_dt is None:
        where = f"{issued_field} IS NOT NULL"
    else:
        d = since_dt.strftime("%Y-%m-%d")
        where = f"{issued_field} >= '{d}'"
        # validate; if the layer rejects the compare, fall back to all rows
        if arcgis_count(session, url, where) is None:
            log(f"    date WHERE not accepted; falling back to IS NOT NULL")
            where = f"{issued_field} IS NOT NULL"

    total = arcgis_count(session, url, where)
    if total is not None:
        log(f"    server reports {total} matching permits")

    all_feats = []
    offset = 0
    while True:
        params = {
            "where": where,
            "outFields": "*",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "orderByFields": f"{issued_field} DESC",
            "returnGeometry": "true",
            "outSR": 4326,  # force WGS84 lat/lng (source SR is Web Mercator 3857)
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


def normalize_permit(feature, cfg, fm):
    attrs = feature.get("attributes") or {}
    geom = feature.get("geometry") or {}

    address = attrs.get(fm["address"])
    address = str(address).strip() if address else None
    if not address:
        return None  # a permit with no address is useless as a lead

    category = attrs.get(fm["category"])
    category = str(category).strip() if category else None

    permit_number = attrs.get(fm["permit_number"])
    permit_number = str(permit_number).strip() if permit_number else None

    owner = attrs.get(fm["owner"])
    applicant = attrs.get(fm["applicant"])
    status = attrs.get(fm["status"])
    case_name = attrs.get(fm.get("case_name") or "")

    issued_dt = parse_issue_dt(attrs.get(fm["issued"]))
    issue_date = issued_dt.date() if issued_dt else None

    # geometry is WGS84 (outSR=4326): x=lng, y=lat. Pearland ~29.5N, -95.3W.
    lng = geom.get("x")
    lat = geom.get("y")
    if (lat in (0, 0.0)) and (lng in (0, 0.0)):
        lat = lng = None

    # description carries the applicant + case name so nothing is lost (hot_leads
    # has no dedicated applicant column).
    desc_bits = []
    if applicant:
        desc_bits.append(f"Applicant: {str(applicant).strip()}")
    if case_name:
        desc_bits.append(f"Case: {str(case_name).strip()}")
    description = " | ".join(desc_bits) if desc_bits else None

    zip_code = extract_zip(address)

    return {
        "permit_number": permit_number,
        "permit_type": (category[:100]) if category else None,
        "work_class": (category[:100]) if category else None,
        "description": (description[:500]) if description else None,
        "address": address[:200],
        "city": cfg["city"][:100],
        "state": cfg["state"],
        "zip": zip_code,
        "county": cfg["county"],
        "lat": lat,
        "lng": lng,
        "issue_date": issue_date,
        "applied_date": issue_date,
        "status": (str(status).strip()[:50]) if status else None,
        "owner_name": (str(owner).strip()[:200]) if owner else None,
        "jurisdiction": f"{cfg['city']}, {cfg['county']} County, {cfg['state']}",
        "source": cfg["source"],
    }


HOT_LEADS_COLS = [
    "id", "permit_number", "permit_type", "work_class", "description",
    "address", "city", "state", "zip", "county", "lat", "lng",
    "issue_date", "applied_date", "status", "owner_name",
    "jurisdiction", "source",
]


def upsert_hot_leads(conn, rows_in):
    """Upsert permits keyed on (permit_number, source) — these rows carry a
    CASE_NUMBER, so we conflict on the UNIQUE index ix_hot_leads_permit."""
    if not rows_in:
        return 0
    deduped = {}
    for p in rows_in:
        pn = p.get("permit_number")
        if not pn or not p.get("address"):
            continue
        deduped[(pn, p["source"])] = p
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
        ON CONFLICT (permit_number, source)
        DO UPDATE SET
            permit_type = COALESCE(EXCLUDED.permit_type, hot_leads.permit_type),
            work_class = COALESCE(EXCLUDED.work_class, hot_leads.work_class),
            description = COALESCE(EXCLUDED.description, hot_leads.description),
            address = COALESCE(EXCLUDED.address, hot_leads.address),
            city = COALESCE(EXCLUDED.city, hot_leads.city),
            zip = COALESCE(EXCLUDED.zip, hot_leads.zip),
            county = COALESCE(EXCLUDED.county, hot_leads.county),
            lat = COALESCE(EXCLUDED.lat, hot_leads.lat),
            lng = COALESCE(EXCLUDED.lng, hot_leads.lng),
            issue_date = COALESCE(EXCLUDED.issue_date, hot_leads.issue_date),
            applied_date = COALESCE(EXCLUDED.applied_date, hot_leads.applied_date),
            status = COALESCE(EXCLUDED.status, hot_leads.status),
            owner_name = COALESCE(EXCLUDED.owner_name, hot_leads.owner_name),
            scraped_at = CURRENT_DATE
    """
    # The box runs under heavy enrichment load that holds locks on hot_leads, so
    # we write in SMALL batches with a per-batch lock-timeout retry instead of one
    # big statement. We NEVER escalate to pg_terminate or drop the timeout to 0 —
    # we just back off and retry, so a busy box only slows us, never breaks it.
    import os as _os
    BATCH = 50
    MAX_ATTEMPTS = int(_os.environ.get("ARCGIS_LOCK_RETRIES", "5"))
    LOCK_TIMEOUT = _os.environ.get("ARCGIS_LOCK_TIMEOUT", "20s")
    loaded = 0
    cur = conn.cursor()
    try:
        cur.execute(f"SET lock_timeout = '{LOCK_TIMEOUT}'")
    except Exception:
        pass
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        attempts = 0
        while True:
            attempts += 1
            try:
                execute_values(cur, sql, chunk, page_size=BATCH)
                conn.commit()
                loaded += len(chunk)
                break
            except psycopg2.errors.LockNotAvailable:
                conn.rollback()
                if attempts >= MAX_ATTEMPTS:
                    log(f"  batch {i//BATCH}: lock contention after {attempts} tries — skipping")
                    break
                wait = min(3 * attempts, 30)
                log(f"  batch {i//BATCH}: lock busy, backing off {wait}s (try {attempts})")
                time.sleep(wait)
            except Exception as e:
                conn.rollback()
                log(f"  hot_leads upsert error (batch {i//BATCH}): {e}")
                break
    cur.close()
    return loaded


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
            (source_name, state, "arcgis_permit_featureserver", loaded, 0, latest_date, error),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"  (record_source: {e})")
    finally:
        cur.close()


def run_city(city_key, cfg, conn, since_days, full, dry_run):
    fm = fields_for(cfg)
    issued_field = fm["issued"]
    log(f"[{city_key}] source={cfg['source']} layers={len(cfg['layers'])}")

    since_dt = None
    if not full:
        hwm = None
        if conn is not None:
            hwm = get_high_water_mark(conn, cfg["source"])
        if hwm is not None:
            since_dt = hwm - timedelta(days=1)
            log(f"  incremental from ledger high-water mark {hwm} (re-pull since {since_dt})")
        else:
            since_dt = (datetime.now(timezone.utc) - timedelta(days=since_days)).date()
            log(f"  no ledger history — first run, look-back {since_days} days (since {since_dt})")

    all_norm = []
    for layer in cfg["layers"]:
        log(f"  layer {layer.rsplit('/services/', 1)[-1]}")
        feats = arcgis_fetch_permits(layer, issued_field, since_dt, full=full)
        norm = [normalize_permit(f, cfg, fm) for f in feats]
        norm = [p for p in norm if p and p.get("permit_number")]
        log(f"    normalized {len(norm)} permits with address+permit_number")
        all_norm.extend(norm)

    # de-dup across layers on (permit_number, source) — keep last
    seen = {}
    for p in all_norm:
        seen[(p["permit_number"], p["source"])] = p
    normalized = list(seen.values())
    log(f"  {len(normalized)} unique permits across layers")

    latest_date = None
    for p in normalized:
        if p["issue_date"] and (latest_date is None or p["issue_date"] > latest_date):
            latest_date = p["issue_date"]

    loaded = 0
    if dry_run:
        log("  DRY RUN — not writing. Sample:")
        for p in normalized[:10]:
            log(f"    {p['issue_date']} | {p['permit_number']} | {p['permit_type']} | "
                f"{p['address']} | owner={p['owner_name']} | {p['lat']},{p['lng']}")
    elif normalized:
        loaded = upsert_hot_leads(conn, normalized)
        # Only advance the ledger high-water mark if we actually wrote rows; a
        # fully lock-blocked run must NOT skip these permits next time.
        if loaded > 0:
            record_source(conn, cfg["source"], cfg["state"], loaded, latest_date)
            log(f"  upserted {loaded} into hot_leads; ledger latest_issue_date={latest_date}")
        else:
            log("  upserted 0 (write blocked) — ledger NOT advanced; will retry next run")
    else:
        log("  nothing to load")
        if conn is not None:
            record_source(conn, cfg["source"], cfg["state"], 0, latest_date)

    return len(normalized), loaded


def main():
    parser = argparse.ArgumentParser(description="Generic ArcGIS permit FeatureServer adapter")
    parser.add_argument("--city", choices=list(CITIES.keys()), help="City to pull")
    parser.add_argument("--all", action="store_true", help="Pull every registered city")
    parser.add_argument("--since-days", type=int, default=90,
                        help="First-run look-back window in days (default 90)")
    parser.add_argument("--full", action="store_true",
                        help="Ignore the high-water mark and pull everything")
    parser.add_argument("--db-host", default=DB_HOST_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--list", action="store_true", help="List registered cities")
    args = parser.parse_args()

    if args.list:
        print("Registered cities:")
        for k, v in CITIES.items():
            print(f"  {k:14s} {v['city']}, {v['county']} County, {v['state']}  source={v['source']}")
            for ly in v["layers"]:
                print(f"                 {ly}")
        return

    if not args.city and not args.all:
        parser.error("must provide --city or --all (or --list)")

    targets = [args.city] if args.city else list(CITIES.keys())

    log("=" * 64)
    log(f"ARCGIS PERMIT ADAPTER — {len(targets)} city/cities")
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
            fetched, loaded = run_city(
                ck, CITIES[ck], conn, args.since_days, args.full, args.dry_run
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
