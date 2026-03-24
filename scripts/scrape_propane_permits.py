#!/usr/bin/env python3
"""
Propane/LP Gas Permit Scraper — 18 verified endpoints across US states & Canada.

Tables:
  - storage_tanks (new) — NY, CO, TX, Seattle, DE, Chicago, CT, San Diego
  - propane_consumption (new) — Gainesville FL LP gas usage
  - propane_gas_facilities (reuse) — Calgary, Cambridge, Seattle, Montgomery, NY MH Parks
  - oil_gas_wells (reuse) — MO Oil & Gas
  - professional_licenses (reuse) — VT DFS, Allegheny PA Plumbers

Sources (all confirmed working):
  Storage Tanks:
    1.  NY Bulk Storage (285K)
    2.  CO Regulated Tanks (52K)
    3.  TX Leaking PST Sites (30K)
    4.  Seattle Residential UST (50K)
    5.  DE Aboveground Tanks (6K)
    6.  Chicago Environmental Tanks (46K)
    7.  CT UST Active Facilities (3.4K)
    8.  San Diego HazMat Propane (7.4K)

  LP Gas Consumption:
    9.  Gainesville FL LP Gas Consumption (26K)

  Gas Permits:
    10. Calgary Gas Permits (194K)
    11. Cambridge MA Gas Permits (8.4K)
    12. Seattle Trade Permits - Gas (277K)
    13. Montgomery County Fire Permits (18K)

  NY Manufactured Home Parks w/ Propane:
    14. NY MH Parks 2020+ propane=true
    15. NY MH Parks Historical propane=true

  MO Oil & Gas:
    16. MO Oil & Gas Permits (10.5K)

  Professional Licenses:
    17. VT DFS Master License List (11.3K)
    18. Allegheny County PA Plumbers (1.3K)

Usage:
    nohup python3 -u scrape_propane_permits.py --db-host 100.122.216.15 > /tmp/propane_permits.log 2>&1 &

Cron (weekly Sunday 5 AM):
    0 5 * * 0 python3 -u /home/will/permit-api/scripts/scrape_propane_permits.py --db-host 100.122.216.15 >> /tmp/propane_permits_weekly.log 2>&1
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


def si(v):
    """Safe integer."""
    if v in (None, "", "NA", "N/A", ".", "-"): return None
    try: return int(float(str(v).strip()))
    except Exception: return None


# =============================================================================
# TABLE SETUP
# =============================================================================

def ensure_tables(conn):
    cur = conn.cursor()

    # --- storage_tanks (NEW) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS storage_tanks (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            facility_id TEXT, facility_name TEXT, address TEXT, city TEXT,
            state VARCHAR(2) NOT NULL, zip TEXT, county TEXT, lat FLOAT, lng FLOAT,
            tank_number TEXT, tank_status TEXT, tank_type TEXT, product TEXT,
            capacity_gallons FLOAT, tank_material TEXT,
            install_date DATE, closure_date DATE, owner_name TEXT,
            source TEXT NOT NULL)
    """)

    # --- propane_consumption (NEW) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS propane_consumption (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            address TEXT, city TEXT, state VARCHAR(2) NOT NULL, zip TEXT,
            lat FLOAT, lng FLOAT, month INTEGER, year INTEGER,
            gallons FLOAT, cost FLOAT, source TEXT NOT NULL)
    """)

    # --- propane_gas_facilities (reuse, ensure exists) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS propane_gas_facilities (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            facility_name TEXT, facility_type TEXT, address TEXT, city TEXT,
            state VARCHAR(2) NOT NULL, zip TEXT, phone TEXT, email TEXT,
            lat FLOAT, lng FLOAT, license_number TEXT, license_type TEXT,
            status TEXT, hazmat_certified BOOLEAN, dot_number TEXT,
            source TEXT NOT NULL)
    """)

    # --- oil_gas_wells (reuse, ensure exists) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS oil_gas_wells (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            well_id TEXT, well_name TEXT, operator TEXT, well_type TEXT,
            well_status TEXT, state VARCHAR(2) NOT NULL, county TEXT,
            lat FLOAT, lng FLOAT, permit_date DATE, spud_date DATE,
            completion_date DATE, total_depth FLOAT, formation TEXT,
            api_number TEXT, field_name TEXT, district TEXT, source TEXT NOT NULL)
    """)

    # --- professional_licenses (reuse, ensure exists) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS professional_licenses (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            license_number TEXT, name TEXT NOT NULL, business_name TEXT,
            profession TEXT, license_type TEXT, address TEXT, city TEXT,
            state VARCHAR(2) NOT NULL, zip TEXT, phone TEXT, email TEXT,
            status TEXT, issue_date DATE, expiration_date DATE,
            source TEXT NOT NULL)
    """)

    # Indexes
    for idx in [
        # storage_tanks
        "CREATE INDEX IF NOT EXISTS ix_st_state ON storage_tanks (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_st_address ON storage_tanks (address)",
        "CREATE INDEX IF NOT EXISTS ix_st_geo ON storage_tanks (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_st_source ON storage_tanks (source)",
        "CREATE INDEX IF NOT EXISTS ix_st_product ON storage_tanks (product)",
        "CREATE INDEX IF NOT EXISTS ix_st_owner ON storage_tanks (owner_name)",
        "CREATE INDEX IF NOT EXISTS ix_st_status ON storage_tanks (tank_status)",
        # propane_consumption
        "CREATE INDEX IF NOT EXISTS ix_pc_state ON propane_consumption (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_pc_address ON propane_consumption (address)",
        "CREATE INDEX IF NOT EXISTS ix_pc_geo ON propane_consumption (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_pc_year ON propane_consumption (year, month)",
        "CREATE INDEX IF NOT EXISTS ix_pc_source ON propane_consumption (source)",
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

    # Handle URLs with existing query params (e.g., $where filters)
    sep = "&" if "?" in url else "?"
    try:
        count_sep = "&" if "?" in url else "?"
        r = httpx.get(f"{url}{count_sep}$select=count(*)", timeout=30)
        avail = int(r.json()[0]["count"]); log(f"  {label}: {avail:,} available")
    except Exception as e:
        log(f"  {label} count failed: {e}"); avail = None

    cur = conn.cursor(); total = 0; offset = 0
    while True:
        try:
            page_url = f"{url}{sep}$limit={page_size}&$offset={offset}&$order=:id"
            resp = httpx.get(page_url, timeout=120)
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

TANK_SQL = """INSERT INTO storage_tanks
    (id, facility_id, facility_name, address, city, state, zip, county,
     lat, lng, tank_number, tank_status, tank_type, product,
     capacity_gallons, tank_material, install_date, closure_date, owner_name, source)
    VALUES %s ON CONFLICT DO NOTHING"""

CONSUMPTION_SQL = """INSERT INTO propane_consumption
    (id, address, city, state, zip, lat, lng, month, year, gallons, cost, source)
    VALUES %s ON CONFLICT DO NOTHING"""

FACILITY_SQL = """INSERT INTO propane_gas_facilities
    (id, facility_name, facility_type, address, city, state, zip, phone, email,
     lat, lng, license_number, license_type, status, hazmat_certified, dot_number, source)
    VALUES %s ON CONFLICT DO NOTHING"""

WELL_SQL = """INSERT INTO oil_gas_wells (id, well_id, well_name, operator, well_type,
    well_status, state, county, lat, lng, permit_date, spud_date, completion_date,
    total_depth, formation, api_number, field_name, district, source)
    VALUES %s ON CONFLICT DO NOTHING"""

LICENSE_SQL = """INSERT INTO professional_licenses
    (id, license_number, name, business_name, profession, license_type,
     address, city, state, zip, phone, email, status, issue_date, expiration_date, source)
    VALUES %s ON CONFLICT DO NOTHING"""


# =============================================================================
# 1. NY BULK STORAGE (285K) — storage_tanks
# =============================================================================

def scrape_ny_bulk_storage(conn):
    log("=== NY Bulk Storage (285K) ===")
    def proc(r):
        # Extract lat/lng from georeference point
        lat = lng = None
        geo = r.get("georeference")
        if isinstance(geo, dict):
            lat = sf(geo.get("latitude"))
            lng = sf(geo.get("longitude"))
        if lat is None: lat = sf(r.get("latitude"))
        if lng is None: lng = sf(r.get("longitude"))
        return (
            str(uuid.uuid4()),
            s(r.get("program_number"), 100),       # facility_id
            s(r.get("program_facility_name"), 300), # facility_name
            s(r.get("address"), 500),               # address
            s(r.get("locality"), 200),              # city
            "NY",                                   # state
            s(r.get("zip"), 10),                    # zip
            s(r.get("county"), 100),                # county
            lat, lng,
            s(r.get("tank_number"), 50),            # tank_number
            s(r.get("tank_status"), 50),            # tank_status
            s(r.get("tank_type"), 100),             # tank_type
            s(r.get("product"), 100),               # product
            sf(r.get("capacity_in_gallons")),       # capacity_gallons
            s(r.get("material_name"), 100),         # tank_material
            sd(r.get("install_date")),              # install_date
            sd(r.get("expiration_date")),           # closure_date
            s(r.get("owner_name"), 300),            # owner_name
            "ny_bulk_storage"
        )
    return scrape_socrata(conn,
        "https://data.ny.gov/resource/pteg-c78n.json",
        proc, "storage_tanks", TANK_SQL, "ny_bulk_storage",
        "NY Bulk Storage")


# =============================================================================
# 2. CO REGULATED TANKS (52K) — storage_tanks
# =============================================================================

def scrape_co_storage_tanks(conn):
    log("=== CO Regulated Tanks (52K) ===")
    def proc(r):
        return (
            str(uuid.uuid4()),
            s(r.get("facility_id") or r.get("facilityid"), 100),
            s(r.get("facility_name") or r.get("facilityname"), 300),
            s(r.get("address"), 500),
            s(r.get("city"), 200),
            "CO",
            s(r.get("zip") or r.get("zipcode"), 10),
            s(r.get("county"), 100),
            sf(r.get("latitude")), sf(r.get("longitude")),
            s(r.get("tank_name") or r.get("tankname"), 50),
            s(r.get("tank_status") or r.get("tankstatus"), 50),
            s(r.get("tank_type") or r.get("tanktype"), 100),
            s(r.get("product"), 100),
            sf(r.get("capacity")),
            s(r.get("tank_material") or r.get("tankmaterial"), 100),
            sd(r.get("installation_date") or r.get("installationdate")),
            sd(r.get("closure_date") or r.get("closuredate")),
            s(r.get("owner_name") or r.get("ownername"), 300),
            "co_storage_tanks"
        )
    return scrape_socrata(conn,
        "https://data.colorado.gov/resource/qszy-xfii.json",
        proc, "storage_tanks", TANK_SQL, "co_storage_tanks",
        "CO Regulated Tanks")


# =============================================================================
# 3. TX LEAKING PST SITES (30K) — storage_tanks
# =============================================================================

def scrape_tx_leaking_pst(conn):
    log("=== TX Leaking PST Sites (30K) ===")
    def proc(r):
        lat = sf(r.get("latitude"))
        lng = sf(r.get("longitude"))
        # Try geocoded_column if lat/lng not direct
        geo = r.get("geocoded_column") or r.get("location")
        if isinstance(geo, dict) and lat is None:
            lat = sf(geo.get("latitude"))
            lng = sf(geo.get("longitude"))
        return (
            str(uuid.uuid4()),
            s(r.get("lpst_id") or r.get("facility_id"), 100),
            s(r.get("facility_name") or r.get("site_name"), 300),
            s(r.get("address") or r.get("street_address"), 500),
            s(r.get("city"), 200),
            "TX",
            s(r.get("zip") or r.get("zip_code"), 10),
            s(r.get("county"), 100),
            lat, lng,
            None,  # tank_number
            s(r.get("status") or r.get("site_status"), 50),
            None,  # tank_type
            s(r.get("substance") or r.get("product"), 100),
            None,  # capacity_gallons
            None,  # tank_material
            None,  # install_date
            sd(r.get("closure_date")),
            s(r.get("responsible_party") or r.get("owner_name"), 300),
            "tx_leaking_pst"
        )
    return scrape_socrata(conn,
        "https://data.texas.gov/resource/hedz-nn4q.json",
        proc, "storage_tanks", TANK_SQL, "tx_leaking_pst",
        "TX Leaking PST Sites")


# =============================================================================
# 4. SEATTLE RESIDENTIAL UST (50K) — storage_tanks
# =============================================================================

def scrape_seattle_ust(conn):
    log("=== Seattle Residential UST (50K) ===")
    def proc(r):
        return (
            str(uuid.uuid4()),
            s(r.get("record_id") or r.get("permit_no"), 100),
            s(r.get("company"), 300),
            s(r.get("address"), 500),
            "Seattle",
            "WA",
            s(r.get("zip_code") or r.get("zip"), 10),
            None,  # county
            sf(r.get("latitude")), sf(r.get("longitude")),
            None,  # tank_number
            s(r.get("status") or r.get("tank_status"), 50),
            s(r.get("tank_type"), 100),
            s(r.get("tank_content") or r.get("product"), 100),
            sf(r.get("tank_size") or r.get("capacity")),
            None,  # tank_material
            sd(r.get("date_issued") or r.get("install_date")),
            sd(r.get("date_decommissioned") or r.get("closure_date")),
            s(r.get("company") or r.get("owner_name"), 300),
            "seattle_ust"
        )
    return scrape_socrata(conn,
        "https://cos-data.seattle.gov/resource/xvj2-ai6y.json",
        proc, "storage_tanks", TANK_SQL, "seattle_ust",
        "Seattle Residential UST")


# =============================================================================
# 5. DE ABOVEGROUND TANKS (6K) — storage_tanks
# =============================================================================

def scrape_de_ast(conn):
    log("=== DE Aboveground Tanks (6K) ===")
    def proc(r):
        lat = sf(r.get("latitude"))
        lng = sf(r.get("longitude"))
        geo = r.get("geocoded_column") or r.get("location")
        if isinstance(geo, dict) and lat is None:
            lat = sf(geo.get("latitude"))
            lng = sf(geo.get("longitude"))
        return (
            str(uuid.uuid4()),
            s(r.get("facility_id") or r.get("facilityid"), 100),
            s(r.get("facility_name") or r.get("facilityname"), 300),
            s(r.get("address") or r.get("street_address"), 500),
            s(r.get("city"), 200),
            "DE",
            s(r.get("zip") or r.get("zipcode"), 10),
            s(r.get("county"), 100),
            lat, lng,
            s(r.get("tank_number") or r.get("tank_id"), 50),
            s(r.get("tank_status") or r.get("status"), 50),
            s(r.get("tank_type"), 100),
            s(r.get("product") or r.get("substance"), 100),
            sf(r.get("capacity") or r.get("capacity_gallons")),
            s(r.get("tank_material") or r.get("material"), 100),
            sd(r.get("install_date") or r.get("installation_date")),
            sd(r.get("closure_date") or r.get("removal_date")),
            s(r.get("owner_name") or r.get("owner"), 300),
            "de_ast"
        )
    return scrape_socrata(conn,
        "https://data.delaware.gov/resource/cgmv-7ssg.json",
        proc, "storage_tanks", TANK_SQL, "de_ast",
        "DE Aboveground Tanks")


# =============================================================================
# 6. CHICAGO ENVIRONMENTAL TANKS (46K) — storage_tanks
# =============================================================================

def scrape_chicago_tanks(conn):
    log("=== Chicago Environmental Tanks (46K) ===")
    def proc(r):
        return (
            str(uuid.uuid4()),
            s(r.get("site_id") or r.get("facility_id"), 100),
            s(r.get("facility_name") or r.get("site_name"), 300),
            s(r.get("address"), 500),
            "Chicago",
            "IL",
            s(r.get("zip") or r.get("zip_code"), 10),
            None,  # county
            sf(r.get("latitude")), sf(r.get("longitude")),
            s(r.get("tank_id") or r.get("tank_number"), 50),
            s(r.get("tank_status") or r.get("status"), 50),
            s(r.get("tank_type"), 100),
            s(r.get("tank_product") or r.get("product"), 100),
            sf(r.get("tank_capacity") or r.get("capacity")),
            s(r.get("tank_material") or r.get("material"), 100),
            sd(r.get("installation_date") or r.get("install_date")),
            sd(r.get("removal_date") or r.get("closure_date")),
            s(r.get("owner_name") or r.get("owner"), 300),
            "chicago_tanks"
        )
    return scrape_socrata(conn,
        "https://data.cityofchicago.org/resource/ug5u-hxnx.json",
        proc, "storage_tanks", TANK_SQL, "chicago_tanks",
        "Chicago Environmental Tanks")


# =============================================================================
# 7. CT UST ACTIVE FACILITIES (3.4K) — storage_tanks
# =============================================================================

def scrape_ct_ust_active(conn):
    log("=== CT UST Active Facilities (3.4K) ===")
    def proc(r):
        lat = sf(r.get("latitude"))
        lng = sf(r.get("longitude"))
        geo = r.get("geocoded_column") or r.get("location")
        if isinstance(geo, dict) and lat is None:
            lat = sf(geo.get("latitude"))
            lng = sf(geo.get("longitude"))
        return (
            str(uuid.uuid4()),
            s(r.get("facility_id") or r.get("facilityid"), 100),
            s(r.get("facility_name") or r.get("facilityname"), 300),
            s(r.get("address") or r.get("street_address"), 500),
            s(r.get("city") or r.get("town"), 200),
            "CT",
            s(r.get("zip") or r.get("zipcode"), 10),
            s(r.get("county"), 100),
            lat, lng,
            s(r.get("tank_number") or r.get("tank_id"), 50),
            s(r.get("tank_status") or r.get("status"), 50),
            s(r.get("tank_type"), 100),
            s(r.get("product") or r.get("substance"), 100),
            sf(r.get("capacity") or r.get("capacity_gallons")),
            s(r.get("tank_material") or r.get("material"), 100),
            sd(r.get("install_date") or r.get("installation_date")),
            sd(r.get("closure_date")),
            s(r.get("owner_name") or r.get("owner"), 300),
            "ct_ust_active"
        )
    return scrape_socrata(conn,
        "https://data.ct.gov/resource/ddp2-c9uu.json",
        proc, "storage_tanks", TANK_SQL, "ct_ust_active",
        "CT UST Active Facilities")


# =============================================================================
# 8. SAN DIEGO HAZMAT PROPANE (7.4K) — storage_tanks
# =============================================================================

def scrape_sd_hazmat_propane(conn):
    log("=== San Diego HazMat Propane (7.4K) ===")
    def proc(r):
        lat = sf(r.get("latitude"))
        lng = sf(r.get("longitude"))
        geo = r.get("geocoded_column") or r.get("location")
        if isinstance(geo, dict) and lat is None:
            lat = sf(geo.get("latitude"))
            lng = sf(geo.get("longitude"))
        return (
            str(uuid.uuid4()),
            s(r.get("facility_id") or r.get("handler_id"), 100),
            s(r.get("facility_name") or r.get("handler_name") or r.get("business_name"), 300),
            s(r.get("address") or r.get("street_address"), 500),
            s(r.get("city"), 200),
            "CA",
            s(r.get("zip") or r.get("zip_code"), 10),
            "San Diego",  # county
            lat, lng,
            None,  # tank_number
            s(r.get("status"), 50),
            None,  # tank_type
            "Propane",  # product (pre-filtered)
            sf(r.get("max_daily_amount") or r.get("quantity") or r.get("max_amount")),
            None,  # tank_material
            None,  # install_date
            None,  # closure_date
            s(r.get("owner_name") or r.get("business_name"), 300),
            "sd_hazmat_propane"
        )
    return scrape_socrata(conn,
        "https://internal-sandiegocounty.data.socrata.com/resource/gvdy-5bty.json?$where=chemical_name='Propane'",
        proc, "storage_tanks", TANK_SQL, "sd_hazmat_propane",
        "San Diego HazMat Propane")


# =============================================================================
# 9. GAINESVILLE FL LP GAS CONSUMPTION (26K) — propane_consumption
# =============================================================================

def scrape_gainesville_lpg(conn):
    log("=== Gainesville FL LP Gas Consumption (26K) ===")
    def proc(r):
        return (
            str(uuid.uuid4()),
            s(r.get("service_address") or r.get("address"), 500),
            s(r.get("service_city") or r.get("city"), 200),
            s(r.get("service_state"), 2) or "FL",
            s(r.get("service_zip") or r.get("zip"), 10),
            sf(r.get("latitude")),
            sf(r.get("longitude")),
            si(r.get("month")),
            si(r.get("year")),
            sf(r.get("lp_consumption") or r.get("consumption")),
            sf(r.get("cost") or r.get("amount")),
            "gainesville_lpg"
        )
    return scrape_socrata(conn,
        "https://data.cityofgainesville.org/resource/s63h-6anu.json",
        proc, "propane_consumption", CONSUMPTION_SQL, "gainesville_lpg",
        "Gainesville FL LP Gas")


# =============================================================================
# 10. CALGARY GAS PERMITS (194K) — propane_gas_facilities
# =============================================================================

def scrape_calgary_gas_permits(conn):
    log("=== Calgary Gas Permits (194K) ===")
    def proc(r):
        name = s(r.get("contractor") or r.get("applicant") or r.get("permitnum"), 300)
        if not name: return None
        return (
            str(uuid.uuid4()),
            name,                                                   # facility_name
            s(r.get("workclassgroup") or r.get("permittype"), 100), # facility_type
            s(r.get("originaladdress") or r.get("address"), 500),   # address
            "Calgary",                                              # city
            "AB",                                                   # state (province)
            s(r.get("postalcode"), 10),                             # zip
            None,                                                   # phone
            None,                                                   # email
            sf(r.get("latitude")),                                  # lat
            sf(r.get("longitude")),                                 # lng
            s(r.get("permitnum"), 50),                              # license_number
            s(r.get("workclassgroup"), 100),                        # license_type
            s(r.get("statuscurrent") or r.get("status"), 50),      # status
            None,                                                   # hazmat_certified
            None,                                                   # dot_number
            "calgary_gas_permits"
        )
    return scrape_socrata(conn,
        "https://data.calgary.ca/resource/tg24-jt7r.json",
        proc, "propane_gas_facilities", FACILITY_SQL, "calgary_gas_permits",
        "Calgary Gas Permits")


# =============================================================================
# 11. CAMBRIDGE MA GAS PERMITS (8.4K) — propane_gas_facilities
# =============================================================================

def scrape_cambridge_gas_permits(conn):
    log("=== Cambridge MA Gas Permits (8.4K) ===")
    def proc(r):
        name = s(r.get("plumber_licensee_name") or r.get("applicant_name"), 300)
        if not name: name = s(r.get("address"), 300)
        if not name: return None
        return (
            str(uuid.uuid4()),
            name,
            "Gas Permit",
            s(r.get("address"), 500),
            "Cambridge",
            "MA",
            s(r.get("zip") or r.get("zip_code"), 10),
            None,  # phone
            None,  # email
            sf(r.get("latitude")),
            sf(r.get("longitude")),
            s(r.get("permit_number") or r.get("permitnumber"), 50),
            "Gas Piping",
            s(r.get("status"), 50),
            None,  # hazmat_certified
            None,  # dot_number
            "cambridge_gas_permits"
        )
    return scrape_socrata(conn,
        "https://data.cambridgema.gov/resource/5cra-jws5.json",
        proc, "propane_gas_facilities", FACILITY_SQL, "cambridge_gas_permits",
        "Cambridge MA Gas Permits")


# =============================================================================
# 12. SEATTLE TRADE PERMITS — GAS (277K) — propane_gas_facilities
# =============================================================================

def scrape_seattle_gas_permits(conn):
    log("=== Seattle Trade Permits - Gas (277K) ===")
    # Filter for gas work via $where clause
    url = "https://cos-data.seattle.gov/resource/c87v-5hwh.json?$where=permittype='Gas Piping' OR upper(description) LIKE '%25GAS%25'"
    def proc(r):
        name = s(r.get("contractorname") or r.get("applicantname") or r.get("description"), 300)
        if not name: name = s(r.get("originaladdress") or r.get("address"), 300)
        if not name: return None
        return (
            str(uuid.uuid4()),
            name,
            s(r.get("permittype") or r.get("permitclass"), 100),
            s(r.get("originaladdress") or r.get("address"), 500),
            "Seattle",
            "WA",
            s(r.get("zip"), 10),
            None,  # phone
            None,  # email
            sf(r.get("latitude")),
            sf(r.get("longitude")),
            s(r.get("permitnum") or r.get("permit_number"), 50),
            s(r.get("permittype"), 100),
            s(r.get("statuscurrent") or r.get("status"), 50),
            None,  # hazmat_certified
            None,  # dot_number
            "seattle_gas_permits"
        )
    return scrape_socrata(conn, url,
        proc, "propane_gas_facilities", FACILITY_SQL, "seattle_gas_permits",
        "Seattle Trade Permits - Gas")


# =============================================================================
# 13. MONTGOMERY COUNTY FIRE PERMITS (18K) — propane_gas_facilities
# =============================================================================

def scrape_montgomery_fire_permits(conn):
    log("=== Montgomery County Fire Permits (18K) ===")
    def proc(r):
        name = s(r.get("applicant") or r.get("owner") or r.get("business_name"), 300)
        if not name: name = s(r.get("address"), 300)
        if not name: return None
        return (
            str(uuid.uuid4()),
            name,
            s(r.get("permit_type") or r.get("type") or "Fire Permit", 100),
            s(r.get("address") or r.get("street_address"), 500),
            s(r.get("city"), 200),
            "MD",
            s(r.get("zip") or r.get("zip_code"), 10),
            None,  # phone
            None,  # email
            sf(r.get("latitude")),
            sf(r.get("longitude")),
            s(r.get("permit_number") or r.get("permitnumber"), 50),
            s(r.get("permit_type") or r.get("type"), 100),
            s(r.get("status"), 50),
            None,  # hazmat_certified
            None,  # dot_number
            "montgomery_fire_permits"
        )
    return scrape_socrata(conn,
        "https://data.montgomerycountymd.gov/resource/a7xm-vjfv.json",
        proc, "propane_gas_facilities", FACILITY_SQL, "montgomery_fire_permits",
        "Montgomery County Fire Permits")


# =============================================================================
# 14. NY MH PARKS 2020+ PROPANE=TRUE — propane_gas_facilities
# =============================================================================

def scrape_ny_mh_parks_propane(conn):
    log("=== NY MH Parks 2020+ (propane=true) ===")
    def proc(r):
        name = s(r.get("park_name") or r.get("facility_name"), 300)
        if not name: return None
        lat = sf(r.get("latitude"))
        lng = sf(r.get("longitude"))
        geo = r.get("georeference") or r.get("location")
        if isinstance(geo, dict) and lat is None:
            lat = sf(geo.get("latitude"))
            lng = sf(geo.get("longitude"))
        return (
            str(uuid.uuid4()),
            name,
            "Manufactured Home Park (Propane)",
            s(r.get("address") or r.get("street_address"), 500),
            s(r.get("city") or r.get("municipality"), 200),
            "NY",
            s(r.get("zip") or r.get("zip_code"), 10),
            None,  # phone
            None,  # email
            lat, lng,
            s(r.get("park_id") or r.get("facility_id"), 50),
            "MH Park - Propane",
            s(r.get("status"), 50),
            None,  # hazmat_certified
            None,  # dot_number
            "ny_mh_parks_propane"
        )
    return scrape_socrata(conn,
        "https://data.ny.gov/resource/nq2i-9jge.json?$where=propane_gas='true'",
        proc, "propane_gas_facilities", FACILITY_SQL, "ny_mh_parks_propane",
        "NY MH Parks 2020+ Propane")


# =============================================================================
# 15. NY MH PARKS HISTORICAL PROPANE — propane_gas_facilities
# =============================================================================

def scrape_ny_mh_parks_propane_hist(conn):
    log("=== NY MH Parks Historical (propane=true) ===")
    def proc(r):
        name = s(r.get("park_name") or r.get("facility_name"), 300)
        if not name: return None
        lat = sf(r.get("latitude"))
        lng = sf(r.get("longitude"))
        geo = r.get("georeference") or r.get("location")
        if isinstance(geo, dict) and lat is None:
            lat = sf(geo.get("latitude"))
            lng = sf(geo.get("longitude"))
        return (
            str(uuid.uuid4()),
            name,
            "Manufactured Home Park (Propane, Historical)",
            s(r.get("address") or r.get("street_address"), 500),
            s(r.get("city") or r.get("municipality"), 200),
            "NY",
            s(r.get("zip") or r.get("zip_code"), 10),
            None,  # phone
            None,  # email
            lat, lng,
            s(r.get("park_id") or r.get("facility_id"), 50),
            "MH Park - Propane Historical",
            s(r.get("status"), 50),
            None,  # hazmat_certified
            None,  # dot_number
            "ny_mh_parks_propane_hist"
        )
    return scrape_socrata(conn,
        "https://data.ny.gov/resource/sxi2-m23m.json?$where=propane_gas='true'",
        proc, "propane_gas_facilities", FACILITY_SQL, "ny_mh_parks_propane_hist",
        "NY MH Parks Historical Propane")


# =============================================================================
# 16. MO OIL & GAS PERMITS (10.5K) — oil_gas_wells
# =============================================================================

def scrape_mo_oil_gas(conn):
    log("=== MO Oil & Gas Permits (10.5K) ===")
    def proc(r):
        return (
            str(uuid.uuid4()),
            s(r.get("permit_number") or r.get("permit_no") or r.get("well_id"), 100),
            s(r.get("well_name") or r.get("lease_name"), 200),
            s(r.get("operator") or r.get("operator_name"), 200),
            s(r.get("well_type") or r.get("type"), 50),
            s(r.get("well_status") or r.get("status"), 50),
            "MO",
            s(r.get("county"), 100),
            sf(r.get("latitude")),
            sf(r.get("longitude")),
            sd(r.get("permit_date") or r.get("date_issued")),
            sd(r.get("spud_date")),
            sd(r.get("completion_date")),
            sf(r.get("total_depth") or r.get("depth")),
            s(r.get("formation") or r.get("producing_formation"), 100),
            s(r.get("api_number") or r.get("api"), 50),
            s(r.get("field_name") or r.get("field"), 100),
            s(r.get("district"), 100),
            "mo_oil_gas"
        )
    return scrape_socrata(conn,
        "https://data.mo.gov/resource/y64b-aec2.json",
        proc, "oil_gas_wells", WELL_SQL, "mo_oil_gas",
        "MO Oil & Gas Permits")


# =============================================================================
# 17. VT DFS MASTER LICENSE LIST (11.3K) — professional_licenses
# =============================================================================

def scrape_vt_dfs_licenses(conn):
    log("=== VT DFS Master License List (11.3K) ===")
    def proc(r):
        name = s(r.get("name") or r.get("licensee_name") or r.get("full_name"), 300)
        if not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("license_number") or r.get("license_no") or r.get("licensenumber"), 50),
            name,
            s(r.get("business_name") or r.get("company_name") or r.get("dba"), 300),
            s(r.get("profession") or r.get("license_type") or r.get("type"), 100),
            s(r.get("license_type") or r.get("type_desc") or r.get("class"), 100),
            s(r.get("address") or r.get("mailing_address"), 500),
            s(r.get("city"), 200),
            s(r.get("state"), 2) or "VT",
            s(r.get("zip") or r.get("zipcode"), 10),
            s(r.get("phone"), 30),
            s(r.get("email"), 200),
            s(r.get("status") or r.get("license_status"), 50),
            sd(r.get("issue_date") or r.get("original_issue_date")),
            sd(r.get("expiration_date") or r.get("exp_date")),
            "vt_dfs_all"
        )
    return scrape_socrata(conn,
        "https://data.vermont.gov/resource/cy8e-89cz.json",
        proc, "professional_licenses", LICENSE_SQL, "vt_dfs_all",
        "VT DFS Master License List")


# =============================================================================
# 18. ALLEGHENY COUNTY PA PLUMBERS (1.3K) — professional_licenses (CSV)
# =============================================================================

def scrape_allegheny_plumbers(conn):
    log("=== Allegheny County PA Plumbers (1.3K) ===")
    existing = get_count(conn, "professional_licenses", "allegheny_plumbers")
    if existing > 100:
        log(f"  SKIP -- already {existing:,} records"); return 0

    cur = conn.cursor(); total = 0
    try:
        url = "https://data.wprdc.org/datastore/dump/e8e2e1a7-8a06-4d69-8a0d-12f9e4e99b86"
        log(f"  Downloading CSV from WPRDC...")
        resp = httpx.get(url, timeout=120, follow_redirects=True)
        resp.raise_for_status()
        text = resp.text
        reader = csv.DictReader(io.StringIO(text))
        batch = []
        for r in reader:
            # Try multiple common field name patterns
            name = (s(r.get("NAME"), 300) or s(r.get("name"), 300) or
                    s(r.get("PLUMBER_NAME"), 300) or s(r.get("plumber_name"), 300) or
                    s(r.get("Licensee Name"), 300) or s(r.get("licensee_name"), 300))
            if not name:
                # Try combining first + last name fields
                first = s(r.get("FIRST_NAME") or r.get("first_name") or r.get("FirstName"), 150)
                last = s(r.get("LAST_NAME") or r.get("last_name") or r.get("LastName"), 150)
                if first and last: name = f"{first} {last}"
                elif last: name = last
            if not name: continue
            batch.append((
                str(uuid.uuid4()),
                s(r.get("LICENSE_NUMBER") or r.get("license_number") or
                  r.get("License Number") or r.get("LICENSE_NO") or r.get("LIC_NO"), 50),
                name,
                s(r.get("BUSINESS_NAME") or r.get("business_name") or
                  r.get("Company") or r.get("COMPANY"), 300),
                "Plumber",
                s(r.get("LICENSE_TYPE") or r.get("license_type") or
                  r.get("Type") or r.get("TYPE") or r.get("CLASS"), 100),
                s(r.get("ADDRESS") or r.get("address") or r.get("STREET"), 500),
                s(r.get("CITY") or r.get("city") or r.get("City"), 200),
                s(r.get("STATE") or r.get("state") or r.get("State"), 2) or "PA",
                s(r.get("ZIP") or r.get("zip") or r.get("Zip") or r.get("ZIPCODE"), 10),
                s(r.get("PHONE") or r.get("phone") or r.get("Phone"), 30),
                s(r.get("EMAIL") or r.get("email") or r.get("Email"), 200),
                s(r.get("STATUS") or r.get("status") or r.get("Status"), 50),
                sd(r.get("ISSUE_DATE") or r.get("issue_date") or r.get("Issue Date")),
                sd(r.get("EXPIRATION_DATE") or r.get("expiration_date") or r.get("Expiration Date")),
                "allegheny_plumbers"
            ))
            if len(batch) >= BATCH_SIZE:
                execute_values(cur, LICENSE_SQL, batch); conn.commit()
                total += len(batch); batch = []
                log(f"    {total:,}")

        if batch:
            execute_values(cur, LICENSE_SQL, batch); conn.commit()
            total += len(batch)
            log(f"    {total:,}")

        log(f"  Total loaded: {total:,}")
    except Exception as e:
        log(f"  Error: {e}"); conn.rollback()
    cur.close(); return total


# =============================================================================
# MASTER RUNNER — ALL 18 SCRAPERS
# =============================================================================

ALL_SCRAPERS = [
    # Storage Tanks (8 sources)
    ("NY Bulk Storage (285K)", scrape_ny_bulk_storage),
    ("CO Regulated Tanks (52K)", scrape_co_storage_tanks),
    ("TX Leaking PST Sites (30K)", scrape_tx_leaking_pst),
    ("Seattle Residential UST (50K)", scrape_seattle_ust),
    ("DE Aboveground Tanks (6K)", scrape_de_ast),
    ("Chicago Environmental Tanks (46K)", scrape_chicago_tanks),
    ("CT UST Active Facilities (3.4K)", scrape_ct_ust_active),
    ("San Diego HazMat Propane (7.4K)", scrape_sd_hazmat_propane),
    # LP Gas Consumption (1 source)
    ("Gainesville FL LP Gas (26K)", scrape_gainesville_lpg),
    # Gas Permits (4 sources)
    ("Calgary Gas Permits (194K)", scrape_calgary_gas_permits),
    ("Cambridge MA Gas Permits (8.4K)", scrape_cambridge_gas_permits),
    ("Seattle Trade Permits - Gas (277K)", scrape_seattle_gas_permits),
    ("Montgomery County Fire Permits (18K)", scrape_montgomery_fire_permits),
    # NY MH Parks w/ Propane (2 sources)
    ("NY MH Parks 2020+ Propane", scrape_ny_mh_parks_propane),
    ("NY MH Parks Historical Propane", scrape_ny_mh_parks_propane_hist),
    # MO Oil & Gas (1 source)
    ("MO Oil & Gas Permits (10.5K)", scrape_mo_oil_gas),
    # Professional Licenses (2 sources)
    ("VT DFS Master License List (11.3K)", scrape_vt_dfs_licenses),
    ("Allegheny County PA Plumbers (1.3K)", scrape_allegheny_plumbers),
]


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="Propane/LP Gas Permit Scraper — 18 endpoints")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to {DB_HOST}")
    ensure_tables(conn)

    grand = 0; results = []
    for name, func in ALL_SCRAPERS:
        log(f"\n{'=' * 60}\n*** {name} ***\n{'=' * 60}")
        try:
            c = func(conn); grand += c; results.append((name, c, "OK"))
            log(f"*** DONE: {name} -- {c:,} ***")
        except Exception as e:
            log(f"*** FAIL: {name} -- {e} ***")
            results.append((name, 0, f"FAIL: {e}"))
            conn.rollback()

    conn.close()
    log(f"\n{'=' * 60}\nCOMPLETE -- {grand:,} total records across 18 sources\n{'=' * 60}")
    for n, c, st in results:
        log(f"  {n}: {c:,} ({st})")


if __name__ == "__main__":
    main()
