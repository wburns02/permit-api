#!/usr/bin/env python3
"""
City/County Plat-Record Adapter — ArcGIS leading-indicator trigger.

Pulls approved/conditional plat records (subdivision final/preliminary plats,
master plans) from a city's public ArcGIS FeatureServer and lands them in
hot_leads as NEW-SUBDIVISION / PLAT triggers (NOT building permits). A recorded
plat is leading-indicator #2 in the TX new-build thesis (after the 911 address
point, before the building permit): when a developer plats "Meridiana Section 9
Final Plat", lots and houses follow within months. For a roofing / new-build
lead platform this is an early signal of where construction is about to start.

Why this exists (the Pearland backdoor that ISN'T)
--------------------------------------------------
Every walled Brazoria city (Lake Jackson=Click2Gov, Freeport=CitizenServe,
Clute=paper, Alvin/Manvel=CentralSquare/MyGov, unincorporated=county
CentralSquare) was probed for a Pearland-style OPEN building-permit
FeatureServer. NONE exists — building permits sit behind token-gated
CentralSquare ArcGIS folders, captcha, or paper. See docs/tx-permit-leads-plan.md.

Manvel, however, publishes an OPEN, no-auth ArcGIS Online FeatureServer of its
Planning, Development & Zoning Commission plat records:
    https://services7.arcgis.com/AKMQLbXfx33spbMD/arcgis/rest/services/PDZMeetingRecord/FeatureServer/30
This is NOT a building permit (no street address, no owner, no permit number),
but it IS a fresh, geocoded, open new-subdivision signal. We harvest it as a
leading-indicator trigger so the lead substrate knows new construction is
coming to Manvel even though the building permits themselves are walled.

Manvel PDZ plat schema (layer 30):
    OBJECTID, Name, Status, Date_ (esri date, epoch ms), PlatType
    (Residential/Commercial/null), MeetingType, PDFLink (CivicClerk agenda
    packet), GlobalID, CreationDate/EditDate (epoch ms), Shape (Polygon)
There is NO street address — the project Name is the lead locator. We compute a
centroid from the polygon (outSR=4326) so each plat geocodes on the map.

hot_leads landing
-----------------
Plat records have NO permit number, so we INSERT with permit_number NULL. The DB
side has a BEFORE-INSERT trigger `trg_fill_permit_number` (fill_permit_number())
that, when permit_number is NULL/'', sets it to a DETERMINISTIC synthetic key
    permit_number := 'NOPN-' || md5(address || source)
so every "no permit number" row still gets a stable unique id. Consequences:
  * Dedup/upsert: our INSERT declares ON CONFLICT (address, source) WHERE
    permit_number IS NULL, but because the trigger fills permit_number the rows
    actually dedup on the canonical unique index ix_hot_leads_permit
    (permit_number, source) via the deterministic NOPN- hash. Re-running the
    loader upserts in place, it does not duplicate. (We keep the partial-index
    ON CONFLICT clause for forward-compat with environments that lack the
    trigger — there it dedups on (address, source).)
  * Bridging: because the NOPN- permit_number is non-null,
    bridge_hot_leads_to_permits.py DOES promote these rows into the partitioned
    `permits` table (source 'bridge_manvel_plats'), so they surface in
    /v1/permits. They carry permit_type/work_class "NEW SUBDIVISION (PLAT)" /
    "PLAT TRIGGER" so a consumer can tell a plat trigger from a real building
    permit. This is intended: Paul wants the new-build signal on the map.
We pack the project Name into the `address` slot (the dedup/lead key — a plat
has no street address), carry Status, the plat Date_ as issue_date, PlatType +
PDFLink into description, and the polygon centroid into lat/lng.

Incremental
-----------
High-water mark per source is MAX(latest_issue_date) in the hot_leads_sources
ledger. Date_ is an esri date (epoch ms); we filter
    where = Date_ > TIMESTAMP '<hwm-1day>'   (with epoch-ms fallback)
order Date_ DESC, paginate at maxRecordCount.

Usage:
    python3 scrape_arcgis_plats.py --city manvel
    python3 scrape_arcgis_plats.py --city manvel --since-days 365
    python3 scrape_arcgis_plats.py --city manvel --full
    python3 scrape_arcgis_plats.py --city manvel --dry-run
    python3 scrape_arcgis_plats.py --list

Cron (PREPARED — DO NOT enable without sign-off):
    # Manvel PDZ plat triggers — weekly Mon 05:45 CT, 14-day look-back
    45 5 * * 1 cd /home/will/permit-api-live && python3 scripts/scrape_arcgis_plats.py --city manvel --since-days 14 >> /tmp/arcgis_plats_manvel.log 2>&1
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

ARCGIS_UA = "Mozilla/5.0 (permit-scraper arcgis-plats)"
PAGE_SIZE = 2000  # matches layer maxRecordCount

# Default plat field map. Cities that rename columns override per-city in the
# CITIES registry under "field_map".
PLAT_DEFAULTS = {
    "name": "Name",        # project / plat name (the lead locator -> address slot)
    "status": "Status",    # Approved / Conditional Approval / ...
    "plat_type": "PlatType",  # Residential / Commercial / null
    "date": "Date_",       # esri date, epoch ms
    "pdf": "PDFLink",       # agenda-packet link
}

# City registry. Add a city = add one entry here. No code changes.
#   url:      full layer query base (FeatureServer/<id> or MapServer/<id>)
#   city / county / state / source / field_map
CITIES = {
    "manvel": {
        "url": "https://services7.arcgis.com/AKMQLbXfx33spbMD/arcgis/rest/services/PDZMeetingRecord/FeatureServer/30",
        "city": "Manvel",
        "county": "Brazoria",
        "state": "TX",
        "source": "manvel_plats",
        "field_map": {},  # native Manvel PDZ schema
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
    fm = dict(PLAT_DEFAULTS)
    fm.update(cfg.get("field_map") or {})
    return fm


def parse_plat_date(v) -> Optional[date]:
    """Parse the plat Date_ value. Manvel's PDZ layer types Date_ as
    esriFieldTypeDateOnly and returns a 'YYYY-MM-DD' STRING (not epoch ms), but
    other cities may publish a classic esriFieldTypeDate (epoch ms). Handle
    both: try the date-only string first, then the epoch-ms number."""
    if v in (None, ""):
        return None
    s = str(v).strip()
    # date-only string 'YYYY-MM-DD' (optionally with a time tail)
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None
    # epoch-ms (classic esri date)
    try:
        return datetime.fromtimestamp(float(v) / 1000.0, tz=timezone.utc).date()
    except Exception:
        return None


def polygon_centroid(geom) -> tuple:
    """Average-of-vertices centroid of an esri polygon (rings) in WGS84.

    Good enough to drop a pin in the right subdivision; we are not doing
    area-weighted centroids. Returns (lat, lng) or (None, None)."""
    if not geom:
        return None, None
    # point geometry passthrough
    if "x" in geom and "y" in geom:
        return geom.get("y"), geom.get("x")
    rings = geom.get("rings")
    if not rings:
        return None, None
    xs, ys = [], []
    for ring in rings:
        for pt in ring:
            if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                xs.append(pt[0])
                ys.append(pt[1])
    if not xs:
        return None, None
    lng = sum(xs) / len(xs)
    lat = sum(ys) / len(ys)
    return lat, lng


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


def build_where(date_field, since_dt, session, url):
    """Pick a date-filter WHERE clause this layer accepts. Date_ is an esri date
    (epoch ms); ArcGIS Online FeatureServers accept date literals — we try
    TIMESTAMP, then DATE, then an epoch-ms numeric compare, validating each with
    returnCountOnly. Falls back to IS NOT NULL if none validate."""
    if since_dt is None:
        return f"{date_field} IS NOT NULL"
    ts = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    d = since_dt.strftime("%Y-%m-%d")
    for cand in (
        f"{date_field} > TIMESTAMP '{ts}'",
        f"{date_field} > DATE '{d}'",
        f"{date_field} > {int(since_dt.timestamp() * 1000)}",
    ):
        if arcgis_count(session, url, cand) is not None:
            return cand
    log(f"    no date WHERE syntax validated for {date_field}; falling back to IS NOT NULL")
    return f"{date_field} IS NOT NULL"


def arcgis_fetch_plats(url, date_field, since_dt, full=False, max_records=100000):
    """Page plat records dated after since_dt (a tz-aware datetime) or all when
    full=True. Returns a list of ArcGIS features (attributes + WGS84 geometry)."""
    session = httpx.Client(
        timeout=45, headers={"User-Agent": ARCGIS_UA}, follow_redirects=True
    )

    if full or since_dt is None:
        where = f"{date_field} IS NOT NULL"
    else:
        where = build_where(date_field, since_dt, session, url)

    total = arcgis_count(session, url, where)
    if total is not None:
        log(f"    server reports {total} matching plat records")

    all_feats = []
    offset = 0
    while True:
        params = {
            "where": where,
            "outFields": "*",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": PAGE_SIZE,
            "orderByFields": f"{date_field} DESC",
            "returnGeometry": "true",
            "outSR": 4326,  # force WGS84 lat/lng for centroid
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


def normalize_plat(feature, cfg, fm):
    attrs = feature.get("attributes") or {}
    geom = feature.get("geometry") or {}

    name = attrs.get(fm["name"])
    name = str(name).strip() if name else None
    if not name:
        return None  # a plat with no project name is useless as a lead

    status = attrs.get(fm["status"])
    plat_type = attrs.get(fm["plat_type"])
    pdf = attrs.get(fm["pdf"])
    dt = parse_plat_date(attrs.get(fm["date"]))

    lat, lng = polygon_centroid(geom)
    if (lat in (0, 0.0)) and (lng in (0, 0.0)):
        lat = lng = None

    desc_bits = []
    if plat_type:
        desc_bits.append(f"PlatType: {str(plat_type).strip()}")
    if pdf:
        desc_bits.append(f"Packet: {str(pdf).strip()}")
    description = " | ".join(desc_bits) if desc_bits else None

    # The project name is the lead locator and the dedup key (no street address
    # exists on a plat). permit_number stays NULL -> NOT bridged into permits.
    return {
        "permit_number": None,
        "permit_type": "NEW SUBDIVISION (PLAT)",
        "work_class": "PLAT TRIGGER",
        "description": (description[:500]) if description else None,
        "address": name[:200],
        "city": cfg["city"][:100],
        "state": cfg["state"],
        "zip": None,
        "county": cfg["county"],
        "lat": lat,
        "lng": lng,
        "issue_date": dt,
        "applied_date": dt,
        "status": (str(status).strip()[:50]) if status else None,
        "owner_name": None,  # plats carry no owner
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
    """Upsert plat records. permit_number is NULL for all of these, so we
    conflict on the partial unique index (address, source) WHERE permit_number
    IS NULL — here `address` holds the project/plat name."""
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
            permit_type = COALESCE(EXCLUDED.permit_type, hot_leads.permit_type),
            work_class = COALESCE(EXCLUDED.work_class, hot_leads.work_class),
            description = COALESCE(EXCLUDED.description, hot_leads.description),
            city = COALESCE(EXCLUDED.city, hot_leads.city),
            county = COALESCE(EXCLUDED.county, hot_leads.county),
            lat = COALESCE(EXCLUDED.lat, hot_leads.lat),
            lng = COALESCE(EXCLUDED.lng, hot_leads.lng),
            issue_date = COALESCE(EXCLUDED.issue_date, hot_leads.issue_date),
            applied_date = COALESCE(EXCLUDED.applied_date, hot_leads.applied_date),
            status = COALESCE(EXCLUDED.status, hot_leads.status),
            scraped_at = CURRENT_DATE
    """
    # Small batches with lock-timeout retry/back-off; the box runs heavy
    # enrichment that holds locks on hot_leads. We NEVER pg_terminate/pg_cancel
    # — a busy box only slows us, never breaks it.
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
            (source_name, state, "arcgis_plat_featureserver", loaded, 0, latest_date, error),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"  (record_source: {e})")
    finally:
        cur.close()


def run_city(city_key, cfg, conn, since_days, full, dry_run):
    fm = fields_for(cfg)
    date_field = fm["date"]
    log(f"[{city_key}] source={cfg['source']} url={cfg['url']}")

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

    feats = arcgis_fetch_plats(cfg["url"], date_field, since_dt, full=full)
    normalized = [normalize_plat(f, cfg, fm) for f in feats]
    normalized = [p for p in normalized if p]
    log(f"  normalized {len(normalized)} plat records with a project name")

    latest_date = None
    for p in normalized:
        if p["issue_date"] and (latest_date is None or p["issue_date"] > latest_date):
            latest_date = p["issue_date"]

    loaded = 0
    if dry_run:
        log("  DRY RUN — not writing. Sample:")
        for p in normalized[:10]:
            log(f"    {p['issue_date']} | {p['status']} | {p['address']} | "
                f"{p['description']} | {p['lat']},{p['lng']}")
    elif normalized:
        loaded = upsert_hot_leads(conn, normalized)
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
    parser = argparse.ArgumentParser(description="ArcGIS city/county plat-record adapter")
    parser.add_argument("--city", choices=list(CITIES.keys()), help="City to pull")
    parser.add_argument("--all", action="store_true", help="Pull every registered city")
    parser.add_argument("--since-days", type=int, default=365,
                        help="First-run look-back window in days (default 365)")
    parser.add_argument("--full", action="store_true",
                        help="Ignore the high-water mark and pull everything")
    parser.add_argument("--db-host", default=DB_HOST_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--list", action="store_true", help="List registered cities")
    args = parser.parse_args()

    if args.list:
        print("Registered plat cities:")
        for k, v in CITIES.items():
            print(f"  {k:14s} {v['city']}, {v['county']} County, {v['state']}  source={v['source']}")
            print(f"                 {v['url']}")
        return

    if not args.city and not args.all:
        parser.error("must provide --city or --all (or --list)")

    targets = [args.city] if args.city else list(CITIES.keys())

    log("=" * 64)
    log(f"ARCGIS PLAT ADAPTER — {len(targets)} city/cities")
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
