#!/usr/bin/env python3
"""
EPA Toxic Release Inventory (TRI) Loader — facility + release data via Envirofacts REST API.

Working endpoints (verified 2026-03-25):
  Facilities:  https://data.epa.gov/efservice/tri_facility/state_abbr/{STATE}/rows/0:50000/JSON
  Releases:    https://data.epa.gov/efservice/tri_reporting_form/state_abbr/{STATE}/rows/{start}:{end}/JSON
  Release qty: https://data.epa.gov/efservice/tri_release_qty/state_abbr/{STATE}/rows/{start}:{end}/JSON
  Chemicals:   https://data.epa.gov/efservice/tri_chem_info/rows/0:10000/JSON

Strategy:
  1. Load all TRI facilities (65K+) — has name, address, lat/lng, parent company
  2. Load TRI reporting form data — has chemical, year, facility linkage
  3. Join facility + chemical data for rich toxic_releases records

Loads into: toxic_releases table (already exists)

Usage:
    nohup python3 -u load_epa_tri.py --db-host 100.122.216.15 > /tmp/epa_tri_load.log 2>&1 &

Cron (monthly 1st Sunday 2 AM):
    0 2 1-7 * 0 python3 -u /home/will/permit-api/scripts/load_epa_tri.py --db-host 100.122.216.15 >> /tmp/epa_tri_monthly.log 2>&1
"""

import argparse, json, os, sys, time, uuid
from datetime import date, datetime

import httpx

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary"); sys.exit(1)

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"; DB_NAME = "permits"; DB_USER = "will"
BATCH_SIZE = 5000

ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def sf(v):
    """Safe float."""
    if v in (None, "", "NA", "N/A", ".", "-", 0): return None
    try: return float(str(v).replace(",", "").replace("$", "").strip())
    except Exception: return None


def s(v, m=500):
    """Safe string with max length."""
    if not v: return None
    return str(v).strip()[:m] or None


def get_count(conn, source=None):
    cur = conn.cursor()
    if source:
        cur.execute("SELECT count(*) FROM toxic_releases WHERE source = %s", (source,))
    else:
        cur.execute("SELECT count(*) FROM toxic_releases")
    c = cur.fetchone()[0]; cur.close(); return c


def ensure_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS toxic_releases (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            facility_id TEXT, facility_name TEXT NOT NULL, address TEXT, city TEXT,
            state VARCHAR(2) NOT NULL, zip TEXT, county TEXT, lat FLOAT, lng FLOAT,
            chemical TEXT, release_type TEXT, total_release_lbs FLOAT,
            year INTEGER, industry TEXT, naics_code TEXT, parent_company TEXT,
            source TEXT NOT NULL DEFAULT 'epa_tri')
    """)
    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_tri_state ON toxic_releases (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_tri_chemical ON toxic_releases (chemical)",
        "CREATE INDEX IF NOT EXISTS ix_tri_year ON toxic_releases (year)",
        "CREATE INDEX IF NOT EXISTS ix_tri_geo ON toxic_releases (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_tri_source ON toxic_releases (source)",
        "CREATE INDEX IF NOT EXISTS ix_tri_facility ON toxic_releases (facility_id)",
    ]:
        try: cur.execute(idx)
        except Exception: conn.rollback()
    conn.commit(); cur.close()


TRI_SQL = """INSERT INTO toxic_releases
    (id, facility_id, facility_name, address, city, state, zip, county,
     lat, lng, chemical, release_type, total_release_lbs, year,
     industry, naics_code, parent_company, source)
    VALUES %s ON CONFLICT DO NOTHING"""


def load_chemical_lookup():
    """Load TRI chemical ID -> name lookup table."""
    log("  Loading chemical lookup table...")
    chem_map = {}
    try:
        offset = 0
        while True:
            resp = httpx.get(
                f"https://data.epa.gov/efservice/tri_chem_info/rows/{offset}:{offset + 10000}/JSON",
                timeout=120, follow_redirects=True
            )
            if resp.status_code != 200:
                break
            records = resp.json()
            if not records:
                break
            for r in records:
                chem_id = r.get("tri_chem_id")
                chem_name = r.get("chem_name")
                if chem_id and chem_name:
                    chem_map[chem_id] = chem_name
            offset += 10000
            if len(records) < 10000:
                break
            time.sleep(0.3)
        log(f"  Loaded {len(chem_map):,} chemicals")
    except Exception as e:
        log(f"  Chemical lookup error: {e}")
    return chem_map


def load_tri_facilities(conn):
    """Load TRI facility data for all 50 states."""
    log("=== EPA TRI Facilities (all 50 states) ===")

    source = "epa_tri"
    existing = get_count(conn, source)
    if existing > 50000:
        log(f"  SKIP facilities -- already {existing:,} records (source={source})")
        return 0

    # Load chemical lookup for enriching later
    chem_map = load_chemical_lookup()

    cur = conn.cursor()
    total = 0

    for st in ALL_STATES:
        log(f"  Facilities: {st}...")
        offset = 0
        state_total = 0

        while True:
            url = f"https://data.epa.gov/efservice/tri_facility/state_abbr/{st}/rows/{offset}:{offset + 50000}/JSON"
            try:
                resp = httpx.get(url, timeout=120, follow_redirects=True)
                if resp.status_code != 200:
                    log(f"    {st} HTTP {resp.status_code} at offset {offset}")
                    break
                records = resp.json()
                if not records:
                    break
            except Exception as e:
                log(f"    {st} error at offset {offset}: {e}")
                break

            batch = []
            for r in records:
                fname = s(r.get("facility_name"), 200)
                if not fname:
                    continue
                batch.append((
                    str(uuid.uuid4()),
                    s(r.get("tri_facility_id"), 50),
                    fname,
                    s(r.get("street_address"), 500),
                    s(r.get("city_name"), 100),
                    st,
                    s(r.get("zip_code"), 10),
                    s(r.get("county_name"), 100),
                    sf(r.get("pref_latitude") or r.get("fac_latitude")),
                    sf(r.get("pref_longitude") or r.get("fac_longitude")),
                    None,  # chemical (facility-level, no chemical)
                    None,  # release_type
                    None,  # total_release_lbs
                    None,  # year
                    None,  # industry
                    None,  # naics_code
                    s(r.get("parent_co_name") or r.get("standardized_parent_company"), 200),
                    source,
                ))

            if batch:
                try:
                    execute_values(cur, TRI_SQL, batch)
                    conn.commit()
                    state_total += len(batch)
                    total += len(batch)
                except Exception as e:
                    log(f"    {st} insert error: {e}")
                    conn.rollback()

            offset += 50000
            if len(records) < 50000:
                break
            time.sleep(0.3)

        if state_total > 0:
            log(f"    {st}: {state_total:,} facilities")
        time.sleep(0.3)

    cur.close()
    log(f"  Total facilities: {total:,}")
    return total


def load_tri_releases(conn):
    """Load TRI release data (reporting form + release quantities) for all states."""
    log("=== EPA TRI Release Data (all 50 states) ===")

    source = "epa_tri_releases"
    existing = get_count(conn, source)
    if existing > 100000:
        log(f"  SKIP releases -- already {existing:,} records (source={source})")
        return 0

    # Load chemical lookup
    chem_map = load_chemical_lookup()

    # First, build a facility lookup from the reporting form data
    # The reporting form has: doc_ctrl_num, tri_facility_id, tri_chem_id, reporting_year
    cur = conn.cursor()
    total = 0

    for st in ALL_STATES:
        log(f"  Releases: {st}...")
        offset = 0
        state_total = 0

        # Collect reporting form records for this state (has facility + chemical + year linkage)
        while True:
            url = f"https://data.epa.gov/efservice/tri_reporting_form/state_abbr/{st}/rows/{offset}:{offset + 10000}/JSON"
            try:
                resp = httpx.get(url, timeout=120, follow_redirects=True)
                if resp.status_code != 200:
                    break
                records = resp.json()
                if not records:
                    break
            except Exception as e:
                log(f"    {st} error at offset {offset}: {e}")
                break

            batch = []
            for r in records:
                fac_id = s(r.get("tri_facility_id"), 50)
                chem_id = s(r.get("tri_chem_id"), 50)
                year_str = r.get("reporting_year")
                year = None
                if year_str:
                    try: year = int(year_str)
                    except Exception: pass

                # Resolve chemical name from lookup
                chemical = chem_map.get(chem_id, chem_id) if chem_id else None

                # Get one-time release quantity if available
                release_lbs = sf(r.get("one_time_release_qty"))

                # Get facility address from the form if available
                facility_name = s(r.get("facility_name"), 200)
                if not facility_name and not fac_id:
                    continue

                batch.append((
                    str(uuid.uuid4()),
                    fac_id,
                    facility_name or f"Facility {fac_id}",
                    s(r.get("street_address"), 500),
                    s(r.get("city_name"), 100),
                    st,
                    s(r.get("zip_code"), 10),
                    s(r.get("county_name"), 100),
                    None,  # lat (not in reporting form)
                    None,  # lng
                    chemical,
                    None,  # release_type (would need release_qty join)
                    release_lbs,
                    year,
                    None,  # industry
                    None,  # naics_code
                    None,  # parent_company
                    source,
                ))

            if batch:
                try:
                    execute_values(cur, TRI_SQL, batch)
                    conn.commit()
                    state_total += len(batch)
                    total += len(batch)
                except Exception as e:
                    log(f"    {st} insert error: {e}")
                    conn.rollback()

            offset += 10000
            if len(records) < 10000:
                break
            time.sleep(0.5)

        if state_total > 0:
            log(f"    {st}: {state_total:,} release records")
        time.sleep(0.3)

    cur.close()
    log(f"  Total release records: {total:,}")
    return total


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="EPA TRI Loader")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    parser.add_argument("--facilities-only", action="store_true", help="Only load facility data")
    parser.add_argument("--releases-only", action="store_true", help="Only load release data")
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to {DB_HOST}")
    ensure_table(conn)

    grand = 0
    results = []

    if not args.releases_only:
        try:
            c = load_tri_facilities(conn)
            grand += c
            results.append(("TRI Facilities", c, "OK"))
        except Exception as e:
            log(f"FAIL facilities: {e}")
            results.append(("TRI Facilities", 0, f"FAIL: {e}"))
            conn.rollback()

    if not args.facilities_only:
        try:
            c = load_tri_releases(conn)
            grand += c
            results.append(("TRI Releases", c, "OK"))
        except Exception as e:
            log(f"FAIL releases: {e}")
            results.append(("TRI Releases", 0, f"FAIL: {e}"))
            conn.rollback()

    conn.close()
    log(f"\n{'=' * 60}\nCOMPLETE -- {grand:,} total\n{'=' * 60}")
    for n, c, st in results:
        log(f"  {n}: {c:,} ({st})")


if __name__ == "__main__":
    main()
