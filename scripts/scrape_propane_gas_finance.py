#!/usr/bin/env python3
"""
Propane/Natural Gas & Finance Scraper — 12 verified API endpoints.

Sources:
  PROPANE/GAS FACILITIES (5 sources → propane_gas_facilities):
    1. FMCSA Hazmat Carriers — 8.5K propane carriers (Socrata)
    2. NREL LPG Fueling Stations — 3K (REST)
    3. VT Gas Installer Certifications — 3K (Socrata)
    4. CT Home Heating Fuel Dealers — 1.4K (Socrata)
    5. Utah LP Gas Companies — 441 (Socrata)

  GAS CONSUMPTION (3 sources → gas_consumption):
    6. NYC Boiler Inspections — 838K (Socrata)
    7. NYC Cooking Gas Consumption — 704K (Socrata)
    8. NYC Heating Gas Consumption — 249K (Socrata)

  ENERGY PRICING (2 sources → energy_data):
    9.  EIA Propane Prices — 72K (REST, paginated)
    10. EIA Natural Gas Prices — 157K (REST, paginated)

  FINANCE (2 sources → consumer_complaints, bank_branches):
    11. CFPB Consumer Complaints — 4M+ (Socrata)
    12. NCUA Credit Unions — ~5K (REST)

Usage:
    nohup python3 -u scrape_propane_gas_finance.py --db-host 100.122.216.15 > /tmp/propane_gas_finance.log 2>&1 &

Cron (weekly Sunday 4 AM):
    0 4 * * 0 python3 -u /home/will/permit-api/scripts/scrape_propane_gas_finance.py --db-host 100.122.216.15 >> /tmp/propane_gas_finance_weekly.log 2>&1
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

def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)

def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)

def sf(v):
    """Safe float."""
    if v in (None, "", "NA", "N/A", ".", "-"): return None
    try: return float(str(v).replace(",", "").replace("$", "").strip())
    except Exception: return None

def sd(v):
    """Safe date."""
    if not v: return None
    try: return datetime.fromisoformat(v.replace("T00:00:00.000", "").replace("Z", "")).date()
    except Exception: pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try: return datetime.strptime(str(v).strip()[:10], fmt).date()
        except Exception: continue
    return None

def s(v, m=500):
    """Safe string with max length."""
    if not v: return None
    return str(v).strip()[:m] or None

def sb(v):
    """Safe boolean."""
    if v is None: return None
    if isinstance(v, bool): return v
    return str(v).strip().lower() in ("true", "yes", "1", "y")

# =============================================================================
# TABLE SETUP
# =============================================================================

def ensure_tables(conn):
    cur = conn.cursor()

    # --- propane_gas_facilities ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS propane_gas_facilities (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            facility_name TEXT, facility_type TEXT, address TEXT, city TEXT,
            state VARCHAR(2) NOT NULL, zip TEXT, phone TEXT, email TEXT,
            lat FLOAT, lng FLOAT, license_number TEXT, license_type TEXT,
            status TEXT, hazmat_certified BOOLEAN, dot_number TEXT,
            source TEXT NOT NULL)
    """)

    # --- gas_consumption ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gas_consumption (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            building_name TEXT, borough TEXT, city TEXT, state VARCHAR(2) NOT NULL,
            consumption_type TEXT, revenue_month TEXT, therms FLOAT, cost FLOAT,
            source TEXT NOT NULL)
    """)

    # --- consumer_complaints ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS consumer_complaints (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            complaint_id TEXT, date_received DATE, product TEXT, sub_product TEXT,
            issue TEXT, company TEXT, company_response TEXT, state VARCHAR(2),
            zip TEXT, consumer_disputed BOOLEAN, timely_response BOOLEAN,
            source TEXT NOT NULL DEFAULT 'cfpb')
    """)

    # --- energy_data (may already exist from scrape_energy_environmental.py) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS energy_data (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            series_id TEXT, series_name TEXT, state VARCHAR(2),
            year INTEGER, month INTEGER, value FLOAT, unit TEXT,
            source TEXT NOT NULL DEFAULT 'eia')
    """)

    # --- bank_branches (may already exist from scrape_industries_v2.py) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bank_branches (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            institution_name TEXT NOT NULL, address TEXT, city TEXT,
            state VARCHAR(2) NOT NULL, zip TEXT, lat FLOAT, lng FLOAT,
            source TEXT NOT NULL DEFAULT 'fdic')
    """)

    # Indexes
    for idx in [
        # propane_gas_facilities
        "CREATE INDEX IF NOT EXISTS ix_pgf_state ON propane_gas_facilities (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_pgf_name ON propane_gas_facilities (facility_name)",
        "CREATE INDEX IF NOT EXISTS ix_pgf_geo ON propane_gas_facilities (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_pgf_source ON propane_gas_facilities (source)",
        "CREATE INDEX IF NOT EXISTS ix_pgf_dot ON propane_gas_facilities (dot_number)",
        # gas_consumption
        "CREATE INDEX IF NOT EXISTS ix_gc_state ON gas_consumption (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_gc_type ON gas_consumption (consumption_type)",
        "CREATE INDEX IF NOT EXISTS ix_gc_source ON gas_consumption (source)",
        # consumer_complaints
        "CREATE INDEX IF NOT EXISTS ix_cc_state ON consumer_complaints (state)",
        "CREATE INDEX IF NOT EXISTS ix_cc_company ON consumer_complaints (company)",
        "CREATE INDEX IF NOT EXISTS ix_cc_product ON consumer_complaints (product)",
        "CREATE INDEX IF NOT EXISTS ix_cc_date ON consumer_complaints (date_received)",
        "CREATE INDEX IF NOT EXISTS ix_cc_source ON consumer_complaints (source)",
        # energy_data (may already exist)
        "CREATE INDEX IF NOT EXISTS ix_energy_state ON energy_data (state, year)",
        "CREATE INDEX IF NOT EXISTS ix_energy_series ON energy_data (series_id)",
        # bank_branches (may already exist)
        "CREATE INDEX IF NOT EXISTS ix_bank_state ON bank_branches (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_bank_source ON bank_branches (source)",
        "CREATE INDEX IF NOT EXISTS ix_bank_name ON bank_branches (institution_name)",
    ]:
        try: cur.execute(idx)
        except Exception: conn.rollback()
    conn.commit(); cur.close()


def get_count(conn, table, source=None):
    cur = conn.cursor()
    if source:
        cur.execute(f"SELECT count(*) FROM {table} WHERE source = %s", (source,))
    else:
        cur.execute(f"SELECT count(*) FROM {table}")
    c = cur.fetchone()[0]; cur.close(); return c


# =============================================================================
# GENERIC SOCRATA HELPER
# =============================================================================

def scrape_socrata(conn, url, process_row, table, insert_sql, source, label, page_size=50000):
    existing = get_count(conn, table, source)
    if existing > 1000:
        log(f"  SKIP {label} -- already {existing:,} records"); return 0
    try:
        r = httpx.get(f"{url}?$select=count(*)", timeout=30)
        avail = int(r.json()[0]["count"]); log(f"  {label}: {avail:,} available")
    except Exception as e:
        log(f"  {label} count failed: {e}"); avail = None
    cur = conn.cursor(); total = 0; offset = 0
    while True:
        try:
            resp = httpx.get(f"{url}?$limit={page_size}&$offset={offset}&$order=:id", timeout=120)
            resp.raise_for_status(); records = resp.json()
        except Exception as e:
            log(f"  Error at offset {offset}: {e}"); break
        if not records: break
        batch = [process_row(r) for r in records]
        batch = [b for b in batch if b]
        if batch:
            try:
                execute_values(cur, insert_sql, batch); conn.commit()
                total += len(batch)
                pct = f" ({total * 100 // avail}%)" if avail else ""
                log(f"    {total:,}{pct}")
            except Exception as e:
                log(f"    Insert error: {e}"); conn.rollback()
        offset += page_size
        if len(records) < page_size: break
        time.sleep(0.3)
    cur.close(); return total


# =============================================================================
# SQL TEMPLATES
# =============================================================================

FACILITY_SQL = """INSERT INTO propane_gas_facilities
    (id, facility_name, facility_type, address, city, state, zip, phone, email,
     lat, lng, license_number, license_type, status, hazmat_certified, dot_number, source)
    VALUES %s ON CONFLICT DO NOTHING"""

GAS_SQL = """INSERT INTO gas_consumption
    (id, building_name, borough, city, state, consumption_type, revenue_month,
     therms, cost, source)
    VALUES %s ON CONFLICT DO NOTHING"""

COMPLAINT_SQL = """INSERT INTO consumer_complaints
    (id, complaint_id, date_received, product, sub_product, issue, company,
     company_response, state, zip, consumer_disputed, timely_response, source)
    VALUES %s ON CONFLICT DO NOTHING"""

ENERGY_SQL = """INSERT INTO energy_data
    (id, series_id, series_name, state, year, month, value, unit, source)
    VALUES %s ON CONFLICT DO NOTHING"""

BANK_SQL = """INSERT INTO bank_branches
    (id, institution_name, address, city, state, zip, lat, lng, source)
    VALUES %s ON CONFLICT DO NOTHING"""


# =============================================================================
# 1. FMCSA HAZMAT CARRIERS (8.5K) — Socrata
# =============================================================================

def scrape_fmcsa_hazmat(conn):
    log("=== FMCSA Hazmat Carriers (8.5K) ===")
    def proc(r):
        name = s(r.get("legal_name"), 300) or s(r.get("dba_name"), 300)
        if not name: return None
        state = s(r.get("phy_state"), 2)
        if not state: return None
        return (str(uuid.uuid4()), name, "Hazmat Carrier",
                s(r.get("phy_street"), 500), s(r.get("phy_city"), 100),
                state, s(r.get("phy_zip"), 10),
                s(r.get("telephone"), 20), s(r.get("email_address"), 200),
                None, None, None, None, None,
                sb(r.get("hm_flag")), s(r.get("dot_number"), 20),
                "fmcsa_hazmat")
    return scrape_socrata(conn,
        "https://datahub.transportation.gov/resource/kjg3-diqy.json",
        proc, "propane_gas_facilities", FACILITY_SQL, "fmcsa_hazmat",
        "FMCSA Hazmat Carriers")


# =============================================================================
# 2. NREL LPG FUELING STATIONS (3K) — REST API
# =============================================================================

def scrape_nrel_lpg(conn):
    log("=== NREL LPG Fueling Stations (3K) ===")
    existing = get_count(conn, "propane_gas_facilities", "nrel_lpg_stations")
    if existing > 500:
        log(f"  SKIP -- already {existing:,} records"); return 0

    cur = conn.cursor(); total = 0
    try:
        resp = httpx.get(
            "https://developer.nrel.gov/api/alt-fuel-stations/v1.json",
            params={"api_key": "DEMO_KEY", "fuel_type": "LPG", "limit": 10000},
            timeout=120
        )
        resp.raise_for_status()
        data = resp.json()
        stations = data.get("alt_fuel_stations", [])
        total_avail = data.get("total_results", len(stations))
        log(f"  Available: {total_avail:,}")

        batch = []
        for r in stations:
            state = s(r.get("state"), 2)
            if not state: continue
            batch.append((
                str(uuid.uuid4()),
                s(r.get("station_name"), 300), "LPG Fueling Station",
                s(r.get("street_address"), 500), s(r.get("city"), 100),
                state, s(r.get("zip"), 10),
                s(r.get("station_phone"), 30), None,
                sf(r.get("latitude")), sf(r.get("longitude")),
                None, None,
                s(r.get("status_code"), 20), None, None,
                "nrel_lpg_stations"
            ))
        if batch:
            # Insert in chunks of BATCH_SIZE
            for i in range(0, len(batch), BATCH_SIZE):
                chunk = batch[i:i + BATCH_SIZE]
                execute_values(cur, FACILITY_SQL, chunk); conn.commit()
                total += len(chunk)
                log(f"    {total:,}")
    except Exception as e:
        log(f"  Error: {e}"); conn.rollback()
    cur.close(); return total


# =============================================================================
# 3. VT GAS INSTALLER CERTIFICATIONS (3K) — Socrata
# =============================================================================

def scrape_vt_gas_installers(conn):
    log("=== VT Gas Installer Certifications (3K) ===")
    def proc(r):
        name = s(r.get("name"), 300)
        if not name: return None
        state = s(r.get("state"), 2) or "VT"
        return (str(uuid.uuid4()), name, "Gas Installer",
                s(r.get("address"), 500), s(r.get("city"), 100),
                state, s(r.get("zip"), 10),
                None, None, None, None,
                s(r.get("license_number"), 50), s(r.get("type_desc"), 100),
                None, None, None,
                "vt_gas_installers")
    return scrape_socrata(conn,
        "https://data.vermont.gov/resource/6n8x-pr2k.json",
        proc, "propane_gas_facilities", FACILITY_SQL, "vt_gas_installers",
        "VT Gas Installer Certifications")


# =============================================================================
# 4. CT HOME HEATING FUEL DEALERS (1.4K) — Socrata
# =============================================================================

def scrape_ct_heating_fuel(conn):
    log("=== CT Home Heating Fuel Dealers (1.4K) ===")
    def proc(r):
        name = s(r.get("business_name"), 300)
        if not name: return None
        state = s(r.get("state"), 2) or "CT"
        return (str(uuid.uuid4()), name, "Heating Fuel Dealer",
                s(r.get("address"), 500), s(r.get("city"), 100),
                state, s(r.get("zip"), 10),
                None, None, None, None,
                None, s(r.get("license_type"), 100),
                s(r.get("active"), 20), None, None,
                "ct_heating_fuel")
    return scrape_socrata(conn,
        "https://data.ct.gov/resource/sjh8-d7wb.json",
        proc, "propane_gas_facilities", FACILITY_SQL, "ct_heating_fuel",
        "CT Home Heating Fuel Dealers")


# =============================================================================
# 5. UTAH LP GAS COMPANIES (441) — Socrata
# =============================================================================

def scrape_ut_lpg(conn):
    log("=== Utah LP Gas Companies (441) ===")
    def proc(r):
        name = s(r.get("company_name"), 300)
        if not name: return None
        return (str(uuid.uuid4()), name, "LP Gas Company",
                s(r.get("mailing_address"), 500), s(r.get("city"), 100),
                "UT", None,
                s(r.get("phone"), 30), None,
                sf(r.get("latitude")), sf(r.get("longitude")),
                s(r.get("license_number"), 50), s(r.get("license_class"), 100),
                None, None, None,
                "ut_lpg")
    return scrape_socrata(conn,
        "https://opendata.utah.gov/resource/77qj-mfn8.json",
        proc, "propane_gas_facilities", FACILITY_SQL, "ut_lpg",
        "Utah LP Gas Companies")


# =============================================================================
# 6. NYC BOILER INSPECTIONS (838K) — Socrata
# =============================================================================

def scrape_nyc_boilers(conn):
    log("=== NYC Boiler Inspections (838K) ===")
    def proc(r):
        return (str(uuid.uuid4()),
                s(r.get("building_id") or r.get("boiler_id"), 200),
                s(r.get("borough"), 50), "New York", "NY",
                "Boiler Inspection",
                s(r.get("inspection_date") or r.get("last_inspection_date"), 30),
                None, None,
                "nyc_boilers")
    return scrape_socrata(conn,
        "https://data.cityofnewyork.us/resource/52dp-yji6.json",
        proc, "gas_consumption", GAS_SQL, "nyc_boilers",
        "NYC Boiler Inspections")


# =============================================================================
# 7. NYC COOKING GAS CONSUMPTION (704K) — Socrata
# =============================================================================

def scrape_nyc_cooking_gas(conn):
    log("=== NYC Cooking Gas Consumption (704K) ===")
    def proc(r):
        return (str(uuid.uuid4()),
                s(r.get("development_name") or r.get("building_name"), 200),
                s(r.get("borough"), 50), "New York", "NY",
                "Cooking Gas",
                s(r.get("revenue_month"), 30),
                sf(r.get("consumption_therms") or r.get("therms")),
                sf(r.get("cost") or r.get("amount")),
                "nyc_cooking_gas")
    return scrape_socrata(conn,
        "https://data.cityofnewyork.us/resource/avhb-5jhc.json",
        proc, "gas_consumption", GAS_SQL, "nyc_cooking_gas",
        "NYC Cooking Gas Consumption")


# =============================================================================
# 8. NYC HEATING GAS CONSUMPTION (249K) — Socrata
# =============================================================================

def scrape_nyc_heating_gas(conn):
    log("=== NYC Heating Gas Consumption (249K) ===")
    def proc(r):
        return (str(uuid.uuid4()),
                s(r.get("development_name") or r.get("building_name"), 200),
                s(r.get("borough"), 50), "New York", "NY",
                "Heating Gas",
                s(r.get("revenue_month"), 30),
                sf(r.get("consumption_therms") or r.get("therms")),
                sf(r.get("cost") or r.get("amount")),
                "nyc_heating_gas")
    return scrape_socrata(conn,
        "https://data.cityofnewyork.us/resource/it56-eyq4.json",
        proc, "gas_consumption", GAS_SQL, "nyc_heating_gas",
        "NYC Heating Gas Consumption")


# =============================================================================
# 9. EIA PROPANE PRICES (72K) — REST API, paginated
# =============================================================================

def scrape_eia_propane(conn):
    log("=== EIA Propane Prices (72K) ===")
    existing = get_count(conn, "energy_data", "eia_propane_prices")
    if existing > 1000:
        log(f"  SKIP -- already {existing:,} records"); return 0

    cur = conn.cursor(); total = 0; offset = 0
    base_url = "https://api.eia.gov/v2/petroleum/pri/wfr/data/"
    while True:
        try:
            resp = httpx.get(base_url, params={
                "api_key": "DEMO_KEY",
                "frequency": "weekly",
                "data[]": "value",
                "facets[product][]": "EPLLPA",
                "length": 5000,
                "offset": offset
            }, timeout=120)
            resp.raise_for_status()
            payload = resp.json()
            response_data = payload.get("response", {})
            records = response_data.get("data", [])
            avail = response_data.get("total", None)
            if offset == 0 and avail:
                log(f"  Available: {avail:,}")
        except Exception as e:
            log(f"  Error at offset {offset}: {e}"); break
        if not records: break

        batch = []
        for r in records:
            period = s(r.get("period"), 20)
            series_id = s(r.get("series") or r.get("seriesId") or r.get("duoarea"), 100)
            area_name = s(r.get("area-name") or r.get("areaName") or r.get("series-description"), 300)
            # Extract state from duoarea if available (e.g., "SFL" -> "FL")
            state_raw = s(r.get("duoarea"), 10)
            state = state_raw[-2:] if state_raw and len(state_raw) >= 2 else None
            # Parse period for year/month
            year = None; month = None
            if period:
                parts = period.split("-")
                try: year = int(parts[0])
                except Exception: pass
                if len(parts) > 1:
                    try: month = int(parts[1])
                    except Exception: pass
            batch.append((
                str(uuid.uuid4()), series_id, area_name,
                state, year, month,
                sf(r.get("value")),
                s(r.get("units") or r.get("unit"), 50),
                "eia_propane_prices"
            ))
        if batch:
            try:
                execute_values(cur, ENERGY_SQL, batch); conn.commit()
                total += len(batch)
                pct = f" ({total * 100 // avail}%)" if avail else ""
                log(f"    {total:,}{pct}")
            except Exception as e:
                log(f"    Insert error: {e}"); conn.rollback()
        offset += 5000
        if len(records) < 5000: break
        time.sleep(0.5)
    cur.close(); return total


# =============================================================================
# 10. EIA NATURAL GAS PRICES (157K) — REST API, paginated
# =============================================================================

def scrape_eia_natgas(conn):
    log("=== EIA Natural Gas Prices (157K) ===")
    existing = get_count(conn, "energy_data", "eia_natgas_prices")
    if existing > 1000:
        log(f"  SKIP -- already {existing:,} records"); return 0

    cur = conn.cursor(); total = 0; offset = 0
    base_url = "https://api.eia.gov/v2/natural-gas/pri/sum/data/"
    while True:
        try:
            resp = httpx.get(base_url, params={
                "api_key": "DEMO_KEY",
                "frequency": "monthly",
                "data[]": "value",
                "length": 5000,
                "offset": offset
            }, timeout=120)
            resp.raise_for_status()
            payload = resp.json()
            response_data = payload.get("response", {})
            records = response_data.get("data", [])
            avail = response_data.get("total", None)
            if offset == 0 and avail:
                log(f"  Available: {avail:,}")
        except Exception as e:
            log(f"  Error at offset {offset}: {e}"); break
        if not records: break

        batch = []
        for r in records:
            period = s(r.get("period"), 20)
            series_id = s(r.get("series") or r.get("seriesId") or r.get("duoarea"), 100)
            area_name = s(r.get("area-name") or r.get("areaName") or r.get("series-description"), 300)
            state_raw = s(r.get("duoarea"), 10)
            state = state_raw[-2:] if state_raw and len(state_raw) >= 2 else None
            year = None; month = None
            if period:
                parts = period.split("-")
                try: year = int(parts[0])
                except Exception: pass
                if len(parts) > 1:
                    try: month = int(parts[1])
                    except Exception: pass
            batch.append((
                str(uuid.uuid4()), series_id, area_name,
                state, year, month,
                sf(r.get("value")),
                s(r.get("units") or r.get("unit"), 50),
                "eia_natgas_prices"
            ))
        if batch:
            try:
                execute_values(cur, ENERGY_SQL, batch); conn.commit()
                total += len(batch)
                pct = f" ({total * 100 // avail}%)" if avail else ""
                log(f"    {total:,}{pct}")
            except Exception as e:
                log(f"    Insert error: {e}"); conn.rollback()
        offset += 5000
        if len(records) < 5000: break
        time.sleep(0.5)
    cur.close(); return total


# =============================================================================
# 11. CFPB CONSUMER COMPLAINTS (4M+) — Socrata
# =============================================================================

def scrape_cfpb_complaints(conn):
    log("=== CFPB Consumer Complaints (4M+) ===")
    existing = get_count(conn, "consumer_complaints", "cfpb")
    if existing > 1000:
        log(f"  SKIP -- already {existing:,} records"); return 0

    # Get total count
    try:
        r = httpx.get("https://data.consumerfinance.gov/resource/s6ew-h6mp.json?$select=count(*)", timeout=30)
        avail = int(r.json()[0]["count"]); log(f"  Available: {avail:,}")
    except Exception as e:
        log(f"  Count failed: {e}"); avail = None

    cur = conn.cursor(); total = 0; offset = 0
    url = "https://data.consumerfinance.gov/resource/s6ew-h6mp.json"
    while True:
        try:
            resp = httpx.get(f"{url}?$limit=50000&$offset={offset}&$order=:id", timeout=120)
            resp.raise_for_status(); records = resp.json()
        except Exception as e:
            log(f"  Error at offset {offset}: {e}"); break
        if not records: break

        batch = []
        for r in records:
            batch.append((
                str(uuid.uuid4()),
                s(r.get("complaint_id"), 50),
                sd(r.get("date_received")),
                s(r.get("product"), 200),
                s(r.get("sub_product"), 200),
                s(r.get("issue"), 300),
                s(r.get("company"), 300),
                s(r.get("company_response"), 200),
                s(r.get("state"), 2),
                s(r.get("zip_code"), 10),
                sb(r.get("consumer_disputed")),
                sb(r.get("timely")),
                "cfpb"
            ))
        if batch:
            try:
                execute_values(cur, COMPLAINT_SQL, batch); conn.commit()
                total += len(batch)
                pct = f" ({total * 100 // avail}%)" if avail else ""
                log(f"    {total:,}{pct}")
            except Exception as e:
                log(f"    Insert error: {e}"); conn.rollback()
        offset += 50000
        if len(records) < 50000: break
        time.sleep(0.3)
    cur.close(); return total


# =============================================================================
# 12. NCUA CREDIT UNIONS (~5K) — REST API
# =============================================================================

def scrape_ncua_credit_unions(conn):
    log("=== NCUA Credit Unions ===")
    existing = get_count(conn, "bank_branches", "ncua_credit_unions")
    if existing > 1000:
        log(f"  SKIP -- already {existing:,} records"); return 0

    cur = conn.cursor(); total = 0; offset = 0
    base_url = "https://api.ncua.gov/api/v1/credit-unions"

    while True:
        try:
            resp = httpx.get(base_url, params={
                "limit": 1000,
                "offset": offset
            }, timeout=120, headers={"Accept": "application/json"})
            resp.raise_for_status()
            payload = resp.json()

            # NCUA API may return list directly or wrapped in an object
            if isinstance(payload, list):
                records = payload
            elif isinstance(payload, dict):
                records = payload.get("data", payload.get("results",
                          payload.get("creditUnions", payload.get("items", []))))
                if not isinstance(records, list):
                    records = [payload] if payload else []
            else:
                records = []

            if offset == 0:
                log(f"  First batch: {len(records)} records")
        except Exception as e:
            log(f"  Error at offset {offset}: {e}"); break
        if not records: break

        batch = []
        for r in records:
            # Handle various possible field name formats (camelCase, snake_case, etc.)
            name = (s(r.get("cu_name"), 300) or s(r.get("cuName"), 300) or
                    s(r.get("name"), 300) or s(r.get("charterName"), 300))
            if not name: continue
            state = (s(r.get("state"), 2) or s(r.get("physicalAddressStateCode"), 2) or
                     s(r.get("stateCode"), 2))
            if not state: continue
            address = (s(r.get("street"), 500) or s(r.get("physicalAddressLine1"), 500) or
                       s(r.get("address"), 500))
            city = (s(r.get("city"), 100) or s(r.get("physicalAddressCity"), 100))
            zipcode = (s(r.get("zip"), 10) or s(r.get("physicalAddressPostalCode"), 10) or
                       s(r.get("zipCode"), 10))
            batch.append((
                str(uuid.uuid4()), name, address, city,
                state, zipcode, None, None,
                "ncua_credit_unions"
            ))
        if batch:
            try:
                execute_values(cur, BANK_SQL, batch); conn.commit()
                total += len(batch)
                log(f"    {total:,}")
            except Exception as e:
                log(f"    Insert error: {e}"); conn.rollback()
        offset += 1000
        if len(records) < 1000: break
        time.sleep(0.3)
    cur.close(); return total


# =============================================================================
# MASTER RUNNER
# =============================================================================

ALL = [
    # Propane/Gas Facilities
    ("FMCSA Hazmat Carriers (8.5K)", scrape_fmcsa_hazmat),
    ("NREL LPG Stations (3K)", scrape_nrel_lpg),
    ("VT Gas Installers (3K)", scrape_vt_gas_installers),
    ("CT Heating Fuel Dealers (1.4K)", scrape_ct_heating_fuel),
    ("Utah LP Gas (441)", scrape_ut_lpg),
    # Gas Consumption
    ("NYC Boiler Inspections (838K)", scrape_nyc_boilers),
    ("NYC Cooking Gas (704K)", scrape_nyc_cooking_gas),
    ("NYC Heating Gas (249K)", scrape_nyc_heating_gas),
    # Energy Pricing
    ("EIA Propane Prices (72K)", scrape_eia_propane),
    ("EIA Natural Gas Prices (157K)", scrape_eia_natgas),
    # Finance
    ("CFPB Consumer Complaints (4M+)", scrape_cfpb_complaints),
    ("NCUA Credit Unions", scrape_ncua_credit_unions),
]


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="Propane/Gas & Finance Scraper")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to {DB_HOST}")
    ensure_tables(conn)

    grand = 0; results = []
    for name, func in ALL:
        log(f"\n{'=' * 60}\n*** {name} ***\n{'=' * 60}")
        try:
            c = func(conn); grand += c; results.append((name, c, "OK"))
            log(f"*** DONE: {name} -- {c:,} ***")
        except Exception as e:
            log(f"*** FAIL: {name} -- {e} ***")
            results.append((name, 0, f"FAIL: {e}"))
            conn.rollback()

    conn.close()
    log(f"\n{'=' * 60}\nCOMPLETE -- {grand:,} total\n{'=' * 60}")
    for n, c, st in results:
        log(f"  {n}: {c:,} ({st})")


if __name__ == "__main__":
    main()
