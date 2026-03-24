#!/usr/bin/env python3
"""
Multi-Industry Data Scraper — Healthcare, Education, Liquor, Food Safety.

Sources (all verified working):
  Healthcare:
    1. CMS Hospital General Information — ~7K hospitals (direct CSV)
    2. CMS Nursing Home Provider Info — ~15K facilities (direct CSV)
    3. CMS Home Health Agencies — ~12K via CKAN datastore API
    4. NY Health Facilities — ~6K via Socrata
  Education:
    5. Chicago Public Schools — ~660 via Socrata
    6. NY School Report Cards — varies via Socrata discovery
  Liquor Licenses:
    7. CT Liquor Licenses — via Socrata discovery
    8. NY Liquor Licenses — via Socrata discovery
  Food Safety:
    9. Chicago Food Inspections — ~250K via Socrata
   10. NYC Restaurant Inspections — ~400K via Socrata

Usage:
    nohup python3 -u scrape_industries.py --db-host 100.122.216.15 > /tmp/industries.log 2>&1 &

Cron (weekly Sunday 4 AM):
    0 4 * * 0 python3 -u /home/will/permit-api/scripts/scrape_industries.py --db-host 100.122.216.15 >> /tmp/industries_weekly.log 2>&1
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
    if v in (None, "", "NA", "N/A", ".", "-", "Not Available"): return None
    try: return float(str(v).replace(",", "").replace("$", "").strip())
    except: return None

def si(v):
    """Safe int."""
    if v in (None, "", "NA", "N/A", ".", "-", "Not Available"): return None
    try: return int(float(str(v).replace(",", "").strip()))
    except: return None

def sd(v):
    """Safe date."""
    if not v or v in ("NA", "N/A", "Not Available"): return None
    v = str(v).strip()
    # Handle Socrata floating-point timestamps
    try: return datetime.fromisoformat(v.replace("T00:00:00.000", "").replace("Z", "")).date()
    except: pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d", "%m-%d-%Y", "%d-%b-%Y", "%Y/%m/%d"):
        try: return datetime.strptime(v[:10], fmt).date()
        except: continue
    return None

def sb(v):
    """Safe bool."""
    if v is None: return None
    if isinstance(v, bool): return v
    s_val = str(v).strip().lower()
    if s_val in ("yes", "y", "true", "1"): return True
    if s_val in ("no", "n", "false", "0"): return False
    return None

def s(v, m=500):
    """Safe string with max length."""
    if not v or v in ("NA", "N/A", "Not Available"): return None
    return str(v).strip()[:m] or None


# =============================================================================
# TABLE CREATION & INDEXES
# =============================================================================

def ensure_tables(conn):
    cur = conn.cursor()

    # Healthcare facilities
    cur.execute("""
        CREATE TABLE IF NOT EXISTS healthcare_facilities (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            facility_id TEXT, facility_name TEXT NOT NULL, facility_type TEXT,
            address TEXT, city TEXT, state VARCHAR(2) NOT NULL, zip TEXT, county TEXT,
            phone TEXT, lat FLOAT, lng FLOAT,
            overall_rating INTEGER, ownership TEXT, emergency_services BOOLEAN,
            beds INTEGER, cms_certification TEXT,
            source TEXT NOT NULL)
    """)

    # Schools
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schools (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            school_id TEXT, school_name TEXT NOT NULL, school_type TEXT,
            district_name TEXT, address TEXT, city TEXT, state VARCHAR(2) NOT NULL,
            zip TEXT, county TEXT, lat FLOAT, lng FLOAT,
            grade_range TEXT, enrollment INTEGER, student_teacher_ratio FLOAT,
            title_i BOOLEAN, magnet BOOLEAN, charter BOOLEAN,
            source TEXT NOT NULL)
    """)

    # Liquor licenses
    cur.execute("""
        CREATE TABLE IF NOT EXISTS liquor_licenses (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            license_number TEXT, business_name TEXT NOT NULL, license_type TEXT,
            address TEXT, city TEXT, state VARCHAR(2) NOT NULL, zip TEXT,
            status TEXT, issue_date DATE, expiration_date DATE,
            source TEXT NOT NULL)
    """)

    # Food inspections
    cur.execute("""
        CREATE TABLE IF NOT EXISTS food_inspections (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            inspection_id TEXT, business_name TEXT NOT NULL, business_type TEXT,
            address TEXT, city TEXT, state VARCHAR(2) NOT NULL, zip TEXT,
            lat FLOAT, lng FLOAT, inspection_date DATE, inspection_type TEXT,
            result TEXT, risk_level TEXT, violations INTEGER,
            source TEXT NOT NULL)
    """)

    # Indexes
    indexes = [
        # Healthcare
        "CREATE INDEX IF NOT EXISTS ix_hc_state_city ON healthcare_facilities (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_hc_address ON healthcare_facilities (address)",
        "CREATE INDEX IF NOT EXISTS ix_hc_geo ON healthcare_facilities (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_hc_type ON healthcare_facilities (facility_type)",
        "CREATE INDEX IF NOT EXISTS ix_hc_source ON healthcare_facilities (source)",
        # Schools
        "CREATE INDEX IF NOT EXISTS ix_sch_state_city ON schools (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_sch_address ON schools (address)",
        "CREATE INDEX IF NOT EXISTS ix_sch_geo ON schools (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_sch_district ON schools (district_name)",
        "CREATE INDEX IF NOT EXISTS ix_sch_source ON schools (source)",
        # Liquor
        "CREATE INDEX IF NOT EXISTS ix_liq_state_city ON liquor_licenses (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_liq_address ON liquor_licenses (address)",
        "CREATE INDEX IF NOT EXISTS ix_liq_business ON liquor_licenses (business_name)",
        "CREATE INDEX IF NOT EXISTS ix_liq_source ON liquor_licenses (source)",
        # Food
        "CREATE INDEX IF NOT EXISTS ix_food_state_city ON food_inspections (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_food_address ON food_inspections (address)",
        "CREATE INDEX IF NOT EXISTS ix_food_geo ON food_inspections (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_food_source ON food_inspections (source)",
    ]
    for idx in indexes:
        try: cur.execute(idx)
        except: conn.rollback()
    conn.commit(); cur.close()


def get_count(conn, table, source=None):
    cur = conn.cursor()
    if source: cur.execute(f"SELECT count(*) FROM {table} WHERE source = %s", (source,))
    else: cur.execute(f"SELECT count(*) FROM {table}")
    c = cur.fetchone()[0]; cur.close(); return c


# =============================================================================
# GENERIC SOCRATA HELPER
# =============================================================================

def scrape_socrata(conn, url, process_row, table, insert_sql, source, label, page_size=50000):
    """Generic Socrata scraper — handles pagination, dedup, progress."""
    existing = get_count(conn, table, source)
    if existing > 1000:
        log(f"  SKIP {label} -- already {existing:,} records"); return 0

    avail = None
    try:
        r = httpx.get(f"{url}?$select=count(*)", timeout=30)
        avail = int(r.json()[0]["count"]); log(f"  {label}: {avail:,} available")
    except Exception as e:
        log(f"  {label} count failed ({e}), proceeding anyway")

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
                pct = f" ({total*100//avail}%)" if avail else ""
                log(f"    {total:,}{pct}")
            except Exception as e:
                log(f"    Insert error: {e}"); conn.rollback()
        offset += page_size
        if len(records) < page_size: break
        time.sleep(0.3)
    cur.close(); return total


# =============================================================================
# GENERIC CMS CSV DOWNLOAD HELPER
# =============================================================================

def download_cms_csv(url, label):
    """Download a CMS CSV file and return a csv.DictReader over it."""
    log(f"  Downloading {label}...")
    resp = httpx.get(url, timeout=300, follow_redirects=True)
    resp.raise_for_status()
    text = resp.text
    # Some CMS CSVs have BOM
    if text.startswith("\ufeff"):
        text = text[1:]
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    log(f"  Downloaded {len(rows):,} rows from {label}")
    return rows


# =============================================================================
# HEALTHCARE — CMS Hospital General Information (~7K)
# =============================================================================

HC_SQL = """INSERT INTO healthcare_facilities (id, facility_id, facility_name, facility_type,
    address, city, state, zip, county, phone, lat, lng,
    overall_rating, ownership, emergency_services, beds, cms_certification, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_cms_hospitals(conn):
    log("=== CMS Hospital General Information (~7K) ===")
    existing = get_count(conn, "healthcare_facilities", "cms_hospitals")
    if existing > 1000:
        log(f"  SKIP -- already {existing:,}"); return 0

    url = "https://data.cms.gov/provider-data/sites/default/files/resources/092256becd267d9dd933f8c535b88c30_1766441111/Hospital_General_Information.csv"
    try:
        rows = download_cms_csv(url, "CMS Hospitals")
    except Exception as e:
        log(f"  Download failed: {e}"); return 0

    cur = conn.cursor(); total = 0; batch = []
    for r in rows:
        state = s(r.get("State"), 2)
        name = s(r.get("Facility Name"), 200)
        if not state or not name: continue
        row = (
            str(uuid.uuid4()),
            s(r.get("Facility ID"), 50),
            name,
            s(r.get("Hospital Type"), 100),
            s(r.get("Address"), 500),
            s(r.get("City"), 100),
            state,
            s(r.get("ZIP Code"), 10),
            s(r.get("County Name"), 100),
            s(r.get("Phone Number"), 20),
            None,  # lat — not in CSV
            None,  # lng — not in CSV
            si(r.get("Hospital overall rating")),
            s(r.get("Hospital Ownership"), 100),
            sb(r.get("Emergency Services")),
            None,  # beds — not in this CSV
            s(r.get("Facility ID"), 50),
            "cms_hospitals",
        )
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            try:
                execute_values(cur, HC_SQL, batch); conn.commit()
                total += len(batch); log(f"    {total:,}")
            except Exception as e:
                log(f"    Insert error: {e}"); conn.rollback()
            batch = []
    if batch:
        try:
            execute_values(cur, HC_SQL, batch); conn.commit()
            total += len(batch); log(f"    {total:,}")
        except Exception as e:
            log(f"    Insert error: {e}"); conn.rollback()
    cur.close(); return total


# =============================================================================
# HEALTHCARE — CMS Nursing Homes (~15K)
# =============================================================================

def scrape_cms_nursing_homes(conn):
    log("=== CMS Nursing Home Provider Info (~15K) ===")
    existing = get_count(conn, "healthcare_facilities", "cms_nursing_homes")
    if existing > 1000:
        log(f"  SKIP -- already {existing:,}"); return 0

    url = "https://data.cms.gov/provider-data/sites/default/files/resources/ee7ae90308084b47fec3dd2b0c99e903_1766441164/NH_ProviderInfo.csv"
    try:
        rows = download_cms_csv(url, "CMS Nursing Homes")
    except Exception as e:
        log(f"  Download failed: {e}"); return 0

    cur = conn.cursor(); total = 0; batch = []
    for r in rows:
        # Column names vary — try common CMS nursing home column names
        state = s(r.get("Provider State") or r.get("STATE"), 2)
        name = s(r.get("Provider Name") or r.get("PROVNAME"), 200)
        if not state or not name: continue
        row = (
            str(uuid.uuid4()),
            s(r.get("Federal Provider Number") or r.get("PROVNUM"), 50),
            name,
            "Nursing Home",
            s(r.get("Provider Address") or r.get("ADDRESS"), 500),
            s(r.get("Provider City") or r.get("CITY"), 100),
            state,
            s(r.get("Provider Zip Code") or r.get("ZIP"), 10),
            s(r.get("Provider County Name") or r.get("COUNTY_NAME"), 100),
            s(r.get("Provider Phone Number") or r.get("PHONE"), 20),
            sf(r.get("Provider Latitude") or r.get("LATITUDE")),
            sf(r.get("Provider Longitude") or r.get("LONGITUDE")),
            si(r.get("Overall Rating") or r.get("OVERALL_RATING")),
            s(r.get("Ownership Type") or r.get("OWNERSHIP"), 100),
            None,  # emergency_services
            si(r.get("Number of Certified Beds") or r.get("BEDCERT")),
            s(r.get("Federal Provider Number") or r.get("PROVNUM"), 50),
            "cms_nursing_homes",
        )
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            try:
                execute_values(cur, HC_SQL, batch); conn.commit()
                total += len(batch); log(f"    {total:,}")
            except Exception as e:
                log(f"    Insert error: {e}"); conn.rollback()
            batch = []
    if batch:
        try:
            execute_values(cur, HC_SQL, batch); conn.commit()
            total += len(batch); log(f"    {total:,}")
        except Exception as e:
            log(f"    Insert error: {e}"); conn.rollback()
    cur.close(); return total


# =============================================================================
# HEALTHCARE — CMS Home Health Agencies (~12K)
# =============================================================================

def scrape_cms_home_health(conn):
    log("=== CMS Home Health Agencies (~12K) ===")
    existing = get_count(conn, "healthcare_facilities", "cms_home_health")
    if existing > 1000:
        log(f"  SKIP -- already {existing:,}"); return 0

    cur = conn.cursor(); total = 0; offset = 0
    base = "https://data.cms.gov/provider-data/api/1/datastore/query/6jpm-sxkc"

    while True:
        try:
            resp = httpx.get(f"{base}?limit=5000&offset={offset}", timeout=120)
            resp.raise_for_status()
            data = resp.json()
            records = data.get("results", [])
        except Exception as e:
            log(f"  Error at offset {offset}: {e}"); break
        if not records: break

        batch = []
        for r in records:
            state = s(r.get("state") or r.get("State"), 2)
            name = s(r.get("provider_name") or r.get("Provider Name"), 200)
            if not state or not name: continue
            batch.append((
                str(uuid.uuid4()),
                s(r.get("cms_certification_number_ccn") or r.get("CMS Certification Number (CCN)"), 50),
                name,
                "Home Health Agency",
                s(r.get("address") or r.get("Address"), 500),
                s(r.get("city") or r.get("City"), 100),
                state,
                s(r.get("zip") or r.get("Zip"), 10),
                s(r.get("county") or r.get("County"), 100),
                s(r.get("phone") or r.get("Phone"), 20),
                None, None,  # lat, lng
                si(r.get("quality_of_patient_care_star_rating") or r.get("Quality of Patient Care Star Rating")),
                s(r.get("type_of_ownership") or r.get("Type of Ownership"), 100),
                None, None,  # emergency, beds
                s(r.get("cms_certification_number_ccn") or r.get("CMS Certification Number (CCN)"), 50),
                "cms_home_health",
            ))
        if batch:
            try:
                execute_values(cur, HC_SQL, batch); conn.commit()
                total += len(batch); log(f"    {total:,}")
            except Exception as e:
                log(f"    Insert error: {e}"); conn.rollback()
        offset += 5000
        if len(records) < 5000: break
        time.sleep(0.3)
    cur.close(); return total


# =============================================================================
# HEALTHCARE — NY Health Facilities (~6K Socrata)
# =============================================================================

def scrape_ny_health_facilities(conn):
    log("=== NY Health Facilities (~6K) ===")
    def proc(r):
        name = s(r.get("facility_name"), 200)
        if not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("facility_id") or r.get("pfi"), 50),
            name,
            s(r.get("description") or r.get("facility_type"), 100),
            s(r.get("facility_address_1"), 500),
            s(r.get("facility_city"), 100),
            "NY",
            s(r.get("facility_zip_code"), 10),
            s(r.get("facility_county"), 100),
            s(r.get("facility_phone"), 20),
            sf(r.get("facility_latitude")),
            sf(r.get("facility_longitude")),
            None, None, None, None, None,
            "ny_health_facilities",
        )
    return scrape_socrata(conn, "https://health.data.ny.gov/resource/vn5v-hh5r.json",
        proc, "healthcare_facilities", HC_SQL, "ny_health_facilities", "NY Health Facilities")


# =============================================================================
# EDUCATION — Chicago Public Schools (~660 Socrata)
# =============================================================================

SCHOOL_SQL = """INSERT INTO schools (id, school_id, school_name, school_type,
    district_name, address, city, state, zip, county, lat, lng,
    grade_range, enrollment, student_teacher_ratio, title_i, magnet, charter, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_chicago_schools(conn):
    log("=== Chicago Public Schools (~660) ===")
    def proc(r):
        name = s(r.get("short_name") or r.get("school_nm") or r.get("name_of_school"), 200)
        if not name: return None
        lat = sf(r.get("school_latitude") or r.get("the_geom", {}).get("coordinates", [None, None])[1]
                  if isinstance(r.get("the_geom"), dict) else r.get("school_latitude"))
        lng = sf(r.get("school_longitude") or r.get("the_geom", {}).get("coordinates", [None, None])[0]
                  if isinstance(r.get("the_geom"), dict) else r.get("school_longitude"))
        return (
            str(uuid.uuid4()),
            s(r.get("school_id"), 50),
            name,
            s(r.get("school_type") or r.get("governance"), 100),
            "Chicago Public Schools",
            s(r.get("street_address") or r.get("address"), 500),
            s(r.get("city"), 100) or "Chicago",
            "IL",
            s(r.get("zip"), 10),
            "Cook",
            lat, lng,
            s(r.get("grades_offered_all") or r.get("grade_cat"), 50),
            si(r.get("student_count_total") or r.get("classroom_count")),
            sf(r.get("student_teacher_ratio")),
            None, None,  # title_i, magnet
            sb(r.get("is_charter") if r.get("is_charter") else r.get("charter")),
            "chicago_schools",
        )
    return scrape_socrata(conn, "https://data.cityofchicago.org/resource/9xs2-f89t.json",
        proc, "schools", SCHOOL_SQL, "chicago_schools", "Chicago Public Schools")


# =============================================================================
# EDUCATION — NY School Enrollment (Socrata Discovery)
# =============================================================================

def scrape_ny_schools(conn):
    log("=== NY School Enrollment ===")
    # Use NY open data — enrollment by school
    def proc(r):
        name = s(r.get("entity_name") or r.get("school_name") or r.get("name"), 200)
        if not name: return None
        state = "NY"
        return (
            str(uuid.uuid4()),
            s(r.get("beds_code") or r.get("entity_cd") or r.get("school_id"), 50),
            name,
            None,
            s(r.get("district_name"), 200),
            None,  # address not in enrollment data
            None,
            state,
            None, None,
            None, None,  # lat, lng
            None,
            si(r.get("total_enrollment") or r.get("k12") or r.get("total")),
            None,
            None, None, None,  # title_i, magnet, charter
            "ny_schools",
        )
    # NY school enrollment by grade
    return scrape_socrata(conn, "https://data.ny.gov/resource/cwbq-vfhe.json",
        proc, "schools", SCHOOL_SQL, "ny_schools", "NY School Enrollment")


# =============================================================================
# LIQUOR — CT Liquor Licenses (Socrata)
# =============================================================================

LIQ_SQL = """INSERT INTO liquor_licenses (id, license_number, business_name, license_type,
    address, city, state, zip, status, issue_date, expiration_date, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_ct_liquor(conn):
    log("=== CT Liquor Licenses ===")
    # CT open data publishes liquor licenses
    def proc(r):
        name = s(r.get("dba") or r.get("business_name") or r.get("permittee_name") or r.get("name"), 200)
        if not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("permit_number") or r.get("license_number") or r.get("license_no"), 50),
            name,
            s(r.get("permit_type_description") or r.get("license_type") or r.get("type"), 100),
            s(r.get("address") or r.get("premise_address") or r.get("street"), 500),
            s(r.get("city") or r.get("town"), 100),
            "CT",
            s(r.get("zip") or r.get("zip_code"), 10),
            s(r.get("status"), 50),
            sd(r.get("issue_date") or r.get("effective_date")),
            sd(r.get("expiration_date") or r.get("expiry_date")),
            "ct_liquor",
        )
    return scrape_socrata(conn, "https://data.ct.gov/resource/4k3d-hm3u.json",
        proc, "liquor_licenses", LIQ_SQL, "ct_liquor", "CT Liquor Licenses")


def scrape_ny_liquor(conn):
    log("=== NY Liquor Licenses ===")
    def proc(r):
        name = s(r.get("premises_name") or r.get("dba") or r.get("doing_business_as")
                 or r.get("d_b_a") or r.get("business_name"), 200)
        if not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("serial_number") or r.get("license_serial_number") or r.get("license_number"), 50),
            name,
            s(r.get("license_type_name") or r.get("license_type") or r.get("type_code"), 100),
            s(r.get("actual_address_of_premises_address1") or r.get("address") or r.get("premises_address"), 500),
            s(r.get("premises_city") or r.get("city"), 100),
            "NY",
            s(r.get("zip_code") or r.get("zip"), 10),
            s(r.get("license_status") or r.get("status"), 50),
            sd(r.get("license_effective_date") or r.get("effective_date")),
            sd(r.get("license_expiration_date") or r.get("expiration_date")),
            "ny_liquor",
        )
    return scrape_socrata(conn, "https://data.ny.gov/resource/hrvs-fxs2.json",
        proc, "liquor_licenses", LIQ_SQL, "ny_liquor", "NY Liquor Licenses")


# =============================================================================
# FOOD SAFETY — Chicago Food Inspections (~250K Socrata)
# =============================================================================

FOOD_SQL = """INSERT INTO food_inspections (id, inspection_id, business_name, business_type,
    address, city, state, zip, lat, lng, inspection_date, inspection_type,
    result, risk_level, violations, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_chicago_food(conn):
    log("=== Chicago Food Inspections (~250K) ===")
    def proc(r):
        name = s(r.get("dba_name") or r.get("aka_name"), 200)
        if not name: return None
        # Count violations by splitting on "|"
        violations_text = r.get("violations") or ""
        viol_count = len([v for v in violations_text.split("|") if v.strip()]) if violations_text else None
        return (
            str(uuid.uuid4()),
            s(r.get("inspection_id"), 50),
            name,
            s(r.get("facility_type"), 100),
            s(r.get("address"), 500),
            s(r.get("city"), 100) or "Chicago",
            "IL",
            s(r.get("zip"), 10),
            sf(r.get("latitude")),
            sf(r.get("longitude")),
            sd(r.get("inspection_date")),
            s(r.get("inspection_type"), 100),
            s(r.get("results"), 100),
            s(r.get("risk"), 100),
            viol_count,
            "chicago_food",
        )
    return scrape_socrata(conn, "https://data.cityofchicago.org/resource/4ijn-s7e5.json",
        proc, "food_inspections", FOOD_SQL, "chicago_food", "Chicago Food Inspections")


# =============================================================================
# FOOD SAFETY — NYC Restaurant Inspections (~400K Socrata)
# =============================================================================

def scrape_nyc_food(conn):
    log("=== NYC Restaurant Inspections (~400K) ===")
    def proc(r):
        name = s(r.get("dba"), 200)
        if not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("camis"), 50),
            name,
            s(r.get("cuisine_description"), 100),
            s(r.get("building", "") + " " + (r.get("street") or ""), 500),
            s(r.get("boro"), 100),
            "NY",
            s(r.get("zipcode"), 10),
            sf(r.get("latitude")),
            sf(r.get("longitude")),
            sd(r.get("inspection_date")),
            s(r.get("inspection_type"), 100),
            s(r.get("action"), 200),
            s(r.get("critical_flag"), 50),
            si(r.get("score")),
            "nyc_food",
        )
    return scrape_socrata(conn, "https://data.cityofnewyork.us/resource/43nn-pn8j.json",
        proc, "food_inspections", FOOD_SQL, "nyc_food", "NYC Restaurant Inspections")


# =============================================================================
# FOOD SAFETY — Dallas Restaurant Inspections (Socrata)
# =============================================================================

def scrape_dallas_food(conn):
    log("=== Dallas Restaurant Inspections ===")
    def proc(r):
        name = s(r.get("restaurant_name") or r.get("establishment_name") or r.get("name"), 200)
        if not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("inspection_number") or r.get("inspection_id"), 50),
            name,
            s(r.get("type") or r.get("establishment_type"), 100),
            s(r.get("street_address") or r.get("address"), 500),
            "Dallas",
            "TX",
            s(r.get("zip") or r.get("zip_code"), 10),
            sf(r.get("latitude")),
            sf(r.get("longitude")),
            sd(r.get("inspection_date")),
            s(r.get("inspection_type"), 100),
            s(r.get("inspection_score") or r.get("score") or r.get("result"), 100),
            None,
            si(r.get("violation_count") or r.get("violations")),
            "dallas_food",
        )
    return scrape_socrata(conn, "https://www.dallasopendata.com/resource/dri5-wcct.json",
        proc, "food_inspections", FOOD_SQL, "dallas_food", "Dallas Restaurant Inspections")


# =============================================================================
# MASTER RUNNER
# =============================================================================

ALL_SCRAPERS = [
    # Healthcare
    ("CMS Hospitals (~7K)", scrape_cms_hospitals),
    ("CMS Nursing Homes (~15K)", scrape_cms_nursing_homes),
    ("CMS Home Health Agencies (~12K)", scrape_cms_home_health),
    ("NY Health Facilities (~6K)", scrape_ny_health_facilities),
    # Education
    ("Chicago Public Schools (~660)", scrape_chicago_schools),
    ("NY School Enrollment", scrape_ny_schools),
    # Liquor Licenses
    ("CT Liquor Licenses", scrape_ct_liquor),
    ("NY Liquor Licenses", scrape_ny_liquor),
    # Food Safety
    ("Chicago Food Inspections (~250K)", scrape_chicago_food),
    ("NYC Restaurant Inspections (~400K)", scrape_nyc_food),
    ("Dallas Restaurant Inspections", scrape_dallas_food),
]


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="Multi-industry data scraper for PermitLookup")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    parser.add_argument("--only", help="Run only this scraper (substring match on name)")
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to {DB_HOST}")
    ensure_tables(conn)

    scrapers = ALL_SCRAPERS
    if args.only:
        scrapers = [(n, f) for n, f in ALL_SCRAPERS if args.only.lower() in n.lower()]
        if not scrapers:
            log(f"No scrapers match '{args.only}'. Available:")
            for n, _ in ALL_SCRAPERS: log(f"  - {n}")
            sys.exit(1)

    grand = 0; results = []
    for name, func in scrapers:
        log(f"\n{'='*60}\n*** {name} ***\n{'='*60}")
        try:
            c = func(conn); grand += c; results.append((name, c, "OK"))
            log(f"*** DONE: {name} -- {c:,} ***")
        except Exception as e:
            log(f"*** FAIL: {name} -- {e} ***")
            results.append((name, 0, f"FAIL: {e}"))
            conn.rollback()

    conn.close()
    log(f"\n{'='*60}\nCOMPLETE -- {grand:,} total records\n{'='*60}")
    for n, c, st in results:
        log(f"  {n}: {c:,} ({st})")


if __name__ == "__main__":
    main()
