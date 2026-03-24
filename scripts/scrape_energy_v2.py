#!/usr/bin/env python3
"""
Energy & Environmental Scraper V2 — verified working endpoints only.

Sources (all confirmed working):
1. TX Railroad Commission — 5 datasets, 26M+ records (THE prize)
2. EPA TRI Facilities — 65K facilities via Envirofacts
3. TX TCEQ Petroleum Storage Tanks — 68K
4. CT Underground Storage Tanks — 50K
5. NY Oil & Gas Wells — 47K
6. NY Oil Gas Annual Production — 330K
7. PA DEP Well Inspections — 14K

Usage:
    nohup python3 -u scrape_energy_v2.py --db-host 100.122.216.15 > /tmp/energy_v2.log 2>&1 &

Cron (weekly Sunday 3 AM):
    0 3 * * 0 python3 -u /home/will/scrape_energy_v2.py --db-host 100.122.216.15 >> /tmp/energy_weekly.log 2>&1
"""

import argparse, csv, io, json, os, sys, time, uuid
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

def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)

def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)

def sf(v):
    if v in (None, "", "NA", "N/A", ".", "-"): return None
    try: return float(str(v).replace(",","").replace("$","").strip())
    except: return None

def sd(v):
    if not v: return None
    try: return datetime.fromisoformat(v.replace("T00:00:00.000","").replace("Z","")).date()
    except: pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try: return datetime.strptime(str(v).strip()[:10], fmt).date()
        except: continue
    return None

def s(v, m=500):
    if not v: return None
    return str(v).strip()[:m] or None

def ensure_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS oil_gas_wells (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            well_id TEXT, well_name TEXT, operator TEXT, well_type TEXT,
            well_status TEXT, state VARCHAR(2) NOT NULL, county TEXT,
            lat FLOAT, lng FLOAT, permit_date DATE, spud_date DATE,
            completion_date DATE, total_depth FLOAT, formation TEXT,
            api_number TEXT, field_name TEXT, district TEXT, source TEXT NOT NULL)
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS underground_storage_tanks (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            facility_id TEXT, facility_name TEXT, address TEXT, city TEXT,
            state VARCHAR(2) NOT NULL, zip TEXT, county TEXT, lat FLOAT, lng FLOAT,
            tank_count INTEGER, substance TEXT, tank_status TEXT,
            capacity_gallons FLOAT, install_date DATE, closure_date DATE,
            owner_name TEXT, source TEXT NOT NULL DEFAULT 'epa_ust')
    """)
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
        "CREATE INDEX IF NOT EXISTS ix_wells_state ON oil_gas_wells (state, county)",
        "CREATE INDEX IF NOT EXISTS ix_wells_api ON oil_gas_wells (api_number)",
        "CREATE INDEX IF NOT EXISTS ix_wells_geo ON oil_gas_wells (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_wells_operator ON oil_gas_wells (operator)",
        "CREATE INDEX IF NOT EXISTS ix_ust_state ON underground_storage_tanks (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_ust_address ON underground_storage_tanks (address)",
        "CREATE INDEX IF NOT EXISTS ix_tri_state ON toxic_releases (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_tri_geo ON toxic_releases (lat, lng)",
    ]:
        try: cur.execute(idx)
        except: conn.rollback()
    conn.commit(); cur.close()

def get_count(conn, table, source=None):
    cur = conn.cursor()
    if source: cur.execute(f"SELECT count(*) FROM {table} WHERE source = %s", (source,))
    else: cur.execute(f"SELECT count(*) FROM {table}")
    c = cur.fetchone()[0]; cur.close(); return c

def scrape_socrata(conn, url, process_row, table, insert_sql, source, label):
    existing = get_count(conn, table, source)
    if existing > 1000:
        log(f"  SKIP {label} — already {existing:,} records"); return 0
    try:
        r = httpx.get(f"{url}?$select=count(*)", timeout=30)
        avail = int(r.json()[0]["count"]); log(f"  {label}: {avail:,} available")
    except Exception as e:
        log(f"  {label} count failed: {e}"); avail = None
    cur = conn.cursor(); total = 0; offset = 0
    while True:
        try:
            resp = httpx.get(f"{url}?$limit=50000&$offset={offset}&$order=:id", timeout=120)
            resp.raise_for_status(); records = resp.json()
        except Exception as e:
            log(f"  Error at {offset}: {e}"); break
        if not records: break
        batch = [process_row(r) for r in records]
        batch = [b for b in batch if b]
        if batch:
            try:
                execute_values(cur, insert_sql, batch); conn.commit()
                total += len(batch)
                pct = f" ({total*100//avail}%)" if avail else ""
                log(f"    {total:,}{pct}")
            except Exception as e:
                log(f"    Insert error: {e}"); conn.rollback()
        offset += 50000
        if len(records) < 50000: break
        time.sleep(0.3)
    cur.close(); return total

WELL_SQL = """INSERT INTO oil_gas_wells (id, well_id, well_name, operator, well_type,
    well_status, state, county, lat, lng, permit_date, spud_date, completion_date,
    total_depth, formation, api_number, field_name, district, source)
    VALUES %s ON CONFLICT DO NOTHING"""

# =============================================================================
# TX RRC — 5 datasets, 26M+ records
# =============================================================================

def scrape_tx_rrc_well_locations(conn):
    log("=== TX RRC Well Bottom Locations (1.4M) ===")
    def proc(r):
        return (str(uuid.uuid4()), s(r.get("api"),100), None, None, None, None,
                "TX", None, sf(r.get("gis_lat83")), sf(r.get("gis_long83")),
                None, None, None, None, None, s(r.get("api"),50), None, None, "tx_rrc_locations")
    return scrape_socrata(conn, "https://data.texas.gov/resource/uumf-5r4y.json",
        proc, "oil_gas_wells", WELL_SQL, "tx_rrc_locations", "TX RRC Well Locations")

def scrape_tx_rrc_casing(conn):
    log("=== TX RRC Wellbore Casing (2.2M) ===")
    def proc(r):
        return (str(uuid.uuid4()), s(r.get("api_no"),100), None, None, None, None,
                "TX", None, None, None, None, None, sd(r.get("completion_date")),
                sf(r.get("total_depth_g1w2")), None, s(r.get("api_no"),50),
                None, None, "tx_rrc_casing")
    return scrape_socrata(conn, "https://data.texas.gov/resource/u9m4-xnh6.json",
        proc, "oil_gas_wells", WELL_SQL, "tx_rrc_casing", "TX RRC Wellbore Casing")

def scrape_tx_rrc_plugging(conn):
    log("=== TX RRC Wellbore Plugging (1.65M) ===")
    def proc(r):
        return (str(uuid.uuid4()), s(r.get("api_no"),100), None, None, "Plugged", None,
                "TX", None, None, None, None, None, sd(r.get("w3_date")),
                sf(r.get("total_depth_w3")), None, s(r.get("api_no"),50),
                None, None, "tx_rrc_plugging")
    return scrape_socrata(conn, "https://data.texas.gov/resource/wfzn-9fje.json",
        proc, "oil_gas_wells", WELL_SQL, "tx_rrc_plugging", "TX RRC Wellbore Plugging")

def scrape_tx_rrc_uic_locations(conn):
    log("=== TX RRC UIC Well Locations (126K) ===")
    def proc(r):
        return (str(uuid.uuid4()), s(r.get("api_no"),100), s(r.get("lease_name"),200),
                s(r.get("operator_number"),200), s(r.get("uic_type_injection"),50), None,
                "TX", None, sf(r.get("latitude_nad83")), sf(r.get("longitude_nad83")),
                None, None, None, None, None, s(r.get("api_no") or r.get("uic_number"),50),
                None, None, "tx_rrc_uic")
    return scrape_socrata(conn, "https://data.texas.gov/resource/givw-z9t4.json",
        proc, "oil_gas_wells", WELL_SQL, "tx_rrc_uic", "TX RRC UIC Well Locations")

def scrape_tx_rrc_injection(conn):
    log("=== TX RRC UIC Injection Monitoring (20.8M) ===")
    # This is the biggest — 20M+ records of injection well monitoring
    # Store in a separate table to avoid bloating oil_gas_wells
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS injection_well_monitoring (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            api_number TEXT, reporting_period TEXT, injection_psi FLOAT,
            injection_bbls FLOAT, disposal_bbls FLOAT, source TEXT NOT NULL)
    """)
    try:
        conn.cursor().execute("CREATE INDEX IF NOT EXISTS ix_inj_api ON injection_well_monitoring (api_number)")
    except: conn.rollback()
    conn.commit()

    existing = get_count(conn, "injection_well_monitoring", "tx_rrc_h10")
    if existing > 1000:
        log(f"  SKIP — already {existing:,}"); return 0

    cur = conn.cursor(); total = 0; offset = 0
    url = "https://data.texas.gov/resource/qq2j-f2zm.json"
    try:
        r = httpx.get(f"{url}?$select=count(*)", timeout=30)
        avail = int(r.json()[0]["count"]); log(f"  Available: {avail:,}")
    except: avail = None

    while True:
        try:
            resp = httpx.get(f"{url}?$limit=50000&$offset={offset}&$order=:id", timeout=120)
            resp.raise_for_status(); records = resp.json()
        except Exception as e:
            log(f"  Error: {e}"); break
        if not records: break
        batch = [(str(uuid.uuid4()), s(r.get("api_no"),50),
                  s(r.get("reporting_period"),20),
                  sf(r.get("avg_injection_psi")), sf(r.get("injection_volume_bbls")),
                  sf(r.get("disposal_volume_bbls")), "tx_rrc_h10") for r in records]
        if batch:
            try:
                execute_values(cur, """INSERT INTO injection_well_monitoring
                    (id, api_number, reporting_period, injection_psi, injection_bbls,
                    disposal_bbls, source) VALUES %s ON CONFLICT DO NOTHING""", batch)
                conn.commit(); total += len(batch)
                pct = f" ({total*100//avail}%)" if avail else ""
                log(f"    {total:,}{pct}")
            except Exception as e:
                log(f"    Error: {e}"); conn.rollback()
        offset += 50000
        if len(records) < 50000: break
        time.sleep(0.3)
    cur.close(); return total

# =============================================================================
# EPA TRI Facilities — 65K via Envirofacts
# =============================================================================

def scrape_epa_tri(conn):
    log("=== EPA TRI Facilities (65K) ===")
    existing = get_count(conn, "toxic_releases")
    if existing > 10000:
        log(f"  SKIP — already {existing:,}"); return 0
    cur = conn.cursor(); total = 0
    states = ["TX","CA","FL","NY","PA","OH","IL","GA","NC","MI","NJ","VA","WA",
              "AZ","MA","TN","IN","MD","MO","WI","CO","MN","SC","AL","LA","KY",
              "OR","OK","CT","UT","AR","MS","KS","IA","NV","NM","NE","WV","ID","HI"]
    for st in states:
        log(f"  TRI {st}...")
        try:
            resp = httpx.get(f"https://data.epa.gov/efservice/tri_facility/state_abbr/{st}/rows/0:50000/JSON",
                             timeout=120, follow_redirects=True)
            if resp.status_code != 200: continue
            records = resp.json()
            batch = [(str(uuid.uuid4()), s(r.get("tri_facility_id"),50),
                      s(r.get("facility_name"),200), s(r.get("street_address"),500),
                      s(r.get("city_name"),100), st, s(r.get("zip_code"),10),
                      s(r.get("county_name"),100), sf(r.get("pref_latitude")),
                      sf(r.get("pref_longitude")), None, None, None, None,
                      None, None, s(r.get("parent_co_name"),200), "epa_tri")
                     for r in records if r.get("facility_name")]
            if batch:
                execute_values(cur, """INSERT INTO toxic_releases (id, facility_id, facility_name,
                    address, city, state, zip, county, lat, lng, chemical, release_type,
                    total_release_lbs, year, industry, naics_code, parent_company, source)
                    VALUES %s ON CONFLICT DO NOTHING""", batch)
                conn.commit(); total += len(batch)
                log(f"    {st}: {len(batch):,}")
        except Exception as e:
            log(f"    {st} error: {e}"); conn.rollback()
        time.sleep(0.5)
    cur.close(); return total

# =============================================================================
# TX TCEQ Petroleum Storage Tanks — 68K
# =============================================================================

def scrape_tx_ust(conn):
    log("=== TX TCEQ Petroleum Storage Tanks (68K) ===")
    def proc(r):
        return (str(uuid.uuid4()), s(r.get("pst_facility_number"),100),
                s(r.get("owner_name"),200), s(r.get("facility_address"),500),
                s(r.get("facility_city"),100), "TX", None,
                s(r.get("facility_county"),100), None, None, None,
                s(r.get("product_type"),100), s(r.get("ust_status"),50),
                sf(r.get("capacity_gallons")), None, None,
                s(r.get("owner_name"),200), "tx_tceq_pst")
    return scrape_socrata(conn, "https://data.texas.gov/resource/jx8f-z4hu.json",
        proc, "underground_storage_tanks",
        """INSERT INTO underground_storage_tanks (id, facility_id, facility_name, address,
            city, state, zip, county, lat, lng, tank_count, substance, tank_status,
            capacity_gallons, install_date, closure_date, owner_name, source)
            VALUES %s ON CONFLICT DO NOTHING""",
        "tx_tceq_pst", "TX TCEQ Petroleum Storage Tanks")

# =============================================================================
# CT UST — 50K
# =============================================================================

def scrape_ct_ust(conn):
    log("=== CT Underground Storage Tanks (50K) ===")
    def proc(r):
        return (str(uuid.uuid4()), s(r.get("agencyfacilityid"),100),
                s(r.get("facilitynm"),200), s(r.get("facilityaddr"),500),
                s(r.get("facilitycity"),100), "CT", None, None, None, None, None,
                s(r.get("substancecd"),100), s(r.get("tankstatuscd"),50),
                sf(r.get("capacitygalsnum")), None, None, None, "ct_ust")
    return scrape_socrata(conn, "https://data.ct.gov/resource/utni-rddb.json",
        proc, "underground_storage_tanks",
        """INSERT INTO underground_storage_tanks (id, facility_id, facility_name, address,
            city, state, zip, county, lat, lng, tank_count, substance, tank_status,
            capacity_gallons, install_date, closure_date, owner_name, source)
            VALUES %s ON CONFLICT DO NOTHING""",
        "ct_ust", "CT Underground Storage Tanks")

# =============================================================================
# NY Oil & Gas Wells — 47K + 330K production
# =============================================================================

def scrape_ny_wells(conn):
    log("=== NY Oil & Gas Wells (47K) ===")
    def proc(r):
        return (str(uuid.uuid4()), s(r.get("api_well_number"),100),
                s(r.get("well_name"),200), s(r.get("company_name"),200),
                s(r.get("well_type"),50), s(r.get("well_status"),50),
                "NY", s(r.get("county"),100), sf(r.get("surface_latitude")),
                sf(r.get("surface_longitude")), None, sd(r.get("date_spudded")),
                None, sf(r.get("measured_depth_ft")), None,
                s(r.get("api_well_number"),50), None, None, "ny_dec_wells")
    return scrape_socrata(conn, "https://data.ny.gov/resource/szye-wmt3.json",
        proc, "oil_gas_wells", WELL_SQL, "ny_dec_wells", "NY Oil & Gas Wells")

def scrape_ny_production(conn):
    log("=== NY Oil & Gas Production (330K) ===")
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS oil_gas_production (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            api_number TEXT, county TEXT, operator TEXT, well_type TEXT,
            field_name TEXT, well_status TEXT, formation TEXT,
            oil_production FLOAT, gas_production FLOAT, year INTEGER,
            state VARCHAR(2) NOT NULL DEFAULT 'NY', source TEXT NOT NULL)
    """)
    try:
        conn.cursor().execute("CREATE INDEX IF NOT EXISTS ix_prod_api ON oil_gas_production (api_number)")
        conn.cursor().execute("CREATE INDEX IF NOT EXISTS ix_prod_state ON oil_gas_production (state, year)")
    except: conn.rollback()
    conn.commit()

    def proc(r):
        return (str(uuid.uuid4()), s(r.get("api_wellno"),50), s(r.get("cnty"),100),
                s(r.get("coname"),200), s(r.get("well_typ"),50), None,
                s(r.get("wl_status"),50), s(r.get("prod_form"),100),
                sf(r.get("production")), sf(r.get("gas")),
                int(float(r.get("year",0))) if r.get("year") else None,
                "NY", "ny_dec_production")
    return scrape_socrata(conn, "https://data.ny.gov/resource/mxea-iw3u.json",
        proc, "oil_gas_production",
        """INSERT INTO oil_gas_production (id, api_number, county, operator, well_type,
            field_name, well_status, formation, oil_production, gas_production, year,
            state, source) VALUES %s ON CONFLICT DO NOTHING""",
        "ny_dec_production", "NY Oil & Gas Production")

# =============================================================================
# PA Well Inspections — 14K
# =============================================================================

def scrape_pa_wells(conn):
    log("=== PA DEP Well Inspections (14K) ===")
    def proc(r):
        return (str(uuid.uuid4()), s(r.get("permit"),100), None,
                s(r.get("client"),200), None, s(r.get("well_status"),50),
                "PA", s(r.get("county"),100), None, None, None,
                sd(r.get("spud_date")), None, None, None,
                s(r.get("ogo_num"),50), None, s(r.get("municipality"),100), "pa_dep_inspections")
    return scrape_socrata(conn, "https://data.pa.gov/resource/f8fx-8zip.json",
        proc, "oil_gas_wells", WELL_SQL, "pa_dep_inspections", "PA DEP Well Inspections")

# =============================================================================
# MASTER RUNNER
# =============================================================================

ALL = [
    ("TX RRC Well Locations (1.4M)", scrape_tx_rrc_well_locations),
    ("TX RRC Wellbore Casing (2.2M)", scrape_tx_rrc_casing),
    ("TX RRC Wellbore Plugging (1.65M)", scrape_tx_rrc_plugging),
    ("TX RRC UIC Well Locations (126K)", scrape_tx_rrc_uic_locations),
    ("TX RRC Injection Monitoring (20.8M)", scrape_tx_rrc_injection),
    ("EPA TRI Facilities (65K)", scrape_epa_tri),
    ("TX TCEQ Petroleum Storage Tanks (68K)", scrape_tx_ust),
    ("CT Underground Storage Tanks (50K)", scrape_ct_ust),
    ("NY Oil & Gas Wells (47K)", scrape_ny_wells),
    ("NY Oil & Gas Production (330K)", scrape_ny_production),
    ("PA DEP Well Inspections (14K)", scrape_pa_wells),
]

def main():
    global DB_HOST
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", default=DB_HOST)
    args = parser.parse_args(); DB_HOST = args.db_host
    conn = get_conn(); log(f"Connected to {DB_HOST}"); ensure_tables(conn)
    grand = 0; results = []
    for name, func in ALL:
        log(f"\n{'='*60}\n*** {name} ***\n{'='*60}")
        try:
            c = func(conn); grand += c; results.append((name, c, "OK"))
            log(f"*** DONE: {name} — {c:,} ***")
        except Exception as e:
            log(f"*** FAIL: {name} — {e} ***"); results.append((name, 0, f"FAIL: {e}")); conn.rollback()
    conn.close()
    log(f"\n{'='*60}\nCOMPLETE — {grand:,} total\n{'='*60}")
    for n, c, s in results: log(f"  {n}: {c:,} ({s})")

if __name__ == "__main__":
    main()
