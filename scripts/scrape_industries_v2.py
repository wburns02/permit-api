#!/usr/bin/env python3
"""
Multi-Industry Data Scraper V2 — verified API endpoints only.

Sources (all confirmed working):
  Healthcare (CMS Datastore API):
    1. CMS Doctors/Clinicians (2.8M) — mj5m-pzi6
    2. CMS Hospitals (5.4K) — xubh-q36u
    3. CMS Nursing Homes with ratings (14.7K) — 4pq5-n9py
    4. CMS Home Health Agencies (12.3K) — 6jpm-sxkc
    5. CMS Dialysis Facilities (7.6K) — 23ew-n7w9
  Education:
    6. NCES Public Schools (102K) — Urban Institute API
    7. College Scorecard (6.3K) — data.gov API
  Banking:
    8. FDIC Bank Branches (78K) — FDIC API
  Food Safety (Socrata):
    9. CO Restaurant Inspections (655K) — data.colorado.gov
   10. PA Food Inspections — data.pa.gov
  Cannabis:
   11. NY Cannabis Retailers (560) — data.ny.gov (Socrata)
  Liquor:
   12. CO Liquor Licenses (20K) — data.colorado.gov (Socrata)

Usage:
    nohup python3 -u scrape_industries_v2.py --db-host 100.122.216.15 > /tmp/industries_v2.log 2>&1 &

Cron (weekly Sunday 5 AM):
    0 5 * * 0 python3 -u /home/will/permit-api/scripts/scrape_industries_v2.py --db-host 100.122.216.15 >> /tmp/industries_v2_weekly.log 2>&1
"""

import argparse, json, os, sys, time, uuid
from datetime import datetime
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
    try: return datetime.fromisoformat(v.replace("T00:00:00.000", "").replace("Z", "")).date()
    except: pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d", "%m-%d-%Y"):
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
    if not v or str(v).strip() in ("NA", "N/A", "Not Available", ""): return None
    return str(v).strip()[:m] or None


# =============================================================================
# TABLE CREATION & INDEXES
# =============================================================================

def ensure_tables(conn):
    cur = conn.cursor()

    # Healthcare facilities (reuse from v1 — add columns if missing)
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

    # Schools (reuse from v1)
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

    # Food inspections (reuse from v1)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS food_inspections (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            inspection_id TEXT, business_name TEXT NOT NULL, business_type TEXT,
            address TEXT, city TEXT, state VARCHAR(2) NOT NULL, zip TEXT,
            lat FLOAT, lng FLOAT, inspection_date DATE, inspection_type TEXT,
            result TEXT, risk_level TEXT, violations INTEGER,
            source TEXT NOT NULL)
    """)

    # Liquor licenses (reuse from v1)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS liquor_licenses (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            license_number TEXT, business_name TEXT NOT NULL, license_type TEXT,
            address TEXT, city TEXT, state VARCHAR(2) NOT NULL, zip TEXT,
            status TEXT, issue_date DATE, expiration_date DATE,
            source TEXT NOT NULL)
    """)

    # NEW: Doctors / Clinicians
    cur.execute("""
        CREATE TABLE IF NOT EXISTS doctors_clinicians (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            npi TEXT, first_name TEXT, last_name TEXT, specialty TEXT,
            facility_name TEXT, address TEXT, city TEXT, state VARCHAR(2) NOT NULL,
            zip TEXT, phone TEXT, source TEXT NOT NULL DEFAULT 'cms_physicians')
    """)

    # NEW: Bank Branches
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bank_branches (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            institution_name TEXT NOT NULL, address TEXT, city TEXT,
            state VARCHAR(2) NOT NULL, zip TEXT, lat FLOAT, lng FLOAT,
            source TEXT NOT NULL DEFAULT 'fdic')
    """)

    # NEW: Colleges
    cur.execute("""
        CREATE TABLE IF NOT EXISTS colleges (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL, city TEXT, state VARCHAR(2) NOT NULL,
            lat FLOAT, lng FLOAT, enrollment INTEGER,
            avg_sat FLOAT, source TEXT NOT NULL DEFAULT 'college_scorecard')
    """)

    # Indexes
    indexes = [
        # Healthcare
        "CREATE INDEX IF NOT EXISTS ix_hc_state_city ON healthcare_facilities (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_hc_geo ON healthcare_facilities (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_hc_type ON healthcare_facilities (facility_type)",
        "CREATE INDEX IF NOT EXISTS ix_hc_source ON healthcare_facilities (source)",
        # Schools
        "CREATE INDEX IF NOT EXISTS ix_sch_state_city ON schools (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_sch_geo ON schools (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_sch_source ON schools (source)",
        # Food
        "CREATE INDEX IF NOT EXISTS ix_food_state_city ON food_inspections (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_food_geo ON food_inspections (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_food_source ON food_inspections (source)",
        # Liquor
        "CREATE INDEX IF NOT EXISTS ix_liq_state_city ON liquor_licenses (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_liq_source ON liquor_licenses (source)",
        # Doctors
        "CREATE INDEX IF NOT EXISTS ix_doc_state ON doctors_clinicians (state)",
        "CREATE INDEX IF NOT EXISTS ix_doc_npi ON doctors_clinicians (npi)",
        "CREATE INDEX IF NOT EXISTS ix_doc_specialty ON doctors_clinicians (specialty)",
        "CREATE INDEX IF NOT EXISTS ix_doc_source ON doctors_clinicians (source)",
        "CREATE INDEX IF NOT EXISTS ix_doc_name ON doctors_clinicians (last_name, first_name)",
        # Banks
        "CREATE INDEX IF NOT EXISTS ix_bank_state ON bank_branches (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_bank_geo ON bank_branches (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_bank_source ON bank_branches (source)",
        "CREATE INDEX IF NOT EXISTS ix_bank_name ON bank_branches (institution_name)",
        # Colleges
        "CREATE INDEX IF NOT EXISTS ix_col_state ON colleges (state)",
        "CREATE INDEX IF NOT EXISTS ix_col_geo ON colleges (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_col_source ON colleges (source)",
    ]
    for idx in indexes:
        try: cur.execute(idx)
        except: conn.rollback()
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
    """Generic Socrata scraper with pagination, dedup, progress."""
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
# GENERIC CMS DATASTORE API HELPER
# =============================================================================

def scrape_cms(conn, dataset_id, process_row, table, insert_sql, source, label):
    """CMS Provider Data API scraper — offset-based pagination."""
    existing = get_count(conn, table, source)
    if existing > 1000:
        log(f"  SKIP {label} -- already {existing:,} records"); return 0

    cur = conn.cursor(); total = 0; offset = 0
    avail = None

    while True:
        url = (
            f"https://data.cms.gov/provider-data/api/1/datastore/query/{dataset_id}/0"
            f"?offset={offset}&count=true&results=true&schema=true&keys=true"
            f"&format=json&rowIds=false"
        )
        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            records = data.get("results", [])
            if avail is None:
                avail = data.get("count", 0)
                log(f"  {label}: {avail:,} available")
        except Exception as e:
            log(f"  Error at offset {offset}: {e}"); break
        if not records: break

        batch = [process_row(r) for r in records]
        batch = [b for b in batch if b]
        if batch:
            for i in range(0, len(batch), BATCH_SIZE):
                chunk = batch[i:i+BATCH_SIZE]
                try:
                    execute_values(cur, insert_sql, chunk); conn.commit()
                    total += len(chunk)
                    pct = f" ({total*100//avail}%)" if avail else ""
                    log(f"    {total:,}{pct}")
                except Exception as e:
                    log(f"    Insert error: {e}"); conn.rollback()
        offset += len(records)
        if len(records) == 0: break
        time.sleep(0.3)
    cur.close(); return total


# =============================================================================
# 1. CMS DOCTORS / CLINICIANS (2.8M) — mj5m-pzi6
# =============================================================================

DOC_SQL = """INSERT INTO doctors_clinicians (id, npi, first_name, last_name, specialty,
    facility_name, address, city, state, zip, phone, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_cms_doctors(conn):
    log("=== CMS Doctors / Clinicians (2.8M) ===")
    def proc(r):
        state = s(r.get("state"), 2)
        if not state: return None
        last = s(r.get("provider_last_name") or r.get("lst_nm"), 200)
        first = s(r.get("provider_first_name") or r.get("frst_nm"), 200)
        if not last and not first: return None
        return (
            str(uuid.uuid4()),
            s(r.get("npi"), 20),
            first,
            last,
            s(r.get("pri_spec") or r.get("provider_type"), 200),
            s(r.get("facility_name") or r.get("org_nm"), 300),
            s(r.get("adr_ln_1") or r.get("provider_street_1"), 500),
            s(r.get("citytown") or r.get("cty"), 100),
            state,
            s(r.get("zip_code") or r.get("zip"), 10),
            s(r.get("telephone_number") or r.get("phn_numbr"), 20),
            "cms_physicians",
        )
    return scrape_cms(conn, "mj5m-pzi6", proc, "doctors_clinicians", DOC_SQL,
                       "cms_physicians", "CMS Doctors/Clinicians")


# =============================================================================
# 2. CMS HOSPITALS (5.4K) — xubh-q36u
# =============================================================================

HC_SQL = """INSERT INTO healthcare_facilities (id, facility_id, facility_name, facility_type,
    address, city, state, zip, county, phone, lat, lng,
    overall_rating, ownership, emergency_services, beds, cms_certification, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_cms_hospitals(conn):
    log("=== CMS Hospitals (5.4K) ===")
    def proc(r):
        state = s(r.get("state"), 2)
        name = s(r.get("facility_name") or r.get("hospital_name"), 200)
        if not state or not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("facility_id") or r.get("provider_id"), 50),
            name,
            s(r.get("hospital_type"), 100),
            s(r.get("address"), 500),
            s(r.get("citytown") or r.get("city"), 100),
            state,
            s(r.get("zip_code") or r.get("zip"), 10),
            s(r.get("countyparish") or r.get("county_name"), 100),
            s(r.get("phone_number") or r.get("telephone_number"), 20),
            None, None,  # lat, lng not in CMS hospitals API
            si(r.get("hospital_overall_rating")),
            s(r.get("hospital_ownership"), 100),
            sb(r.get("emergency_services")),
            None,  # beds
            s(r.get("facility_id") or r.get("provider_id"), 50),
            "cms_hospitals_v2",
        )
    return scrape_cms(conn, "xubh-q36u", proc, "healthcare_facilities", HC_SQL,
                       "cms_hospitals_v2", "CMS Hospitals")


# =============================================================================
# 3. CMS NURSING HOMES WITH RATINGS (14.7K) — 4pq5-n9py
# =============================================================================

def scrape_cms_nursing_homes(conn):
    log("=== CMS Nursing Homes with Ratings (14.7K) ===")
    def proc(r):
        state = s(r.get("state") or r.get("provider_state"), 2)
        name = s(r.get("provider_name") or r.get("provname"), 200)
        if not state or not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("federal_provider_number") or r.get("provnum"), 50),
            name,
            "Nursing Home",
            s(r.get("provider_address") or r.get("address"), 500),
            s(r.get("citytown") or r.get("provider_city") or r.get("city"), 100),
            state,
            s(r.get("zip_code") or r.get("provider_zip_code") or r.get("zip"), 10),
            s(r.get("county_parish") or r.get("provider_county_name") or r.get("county"), 100),
            s(r.get("phone_number") or r.get("provider_phone_number"), 20),
            sf(r.get("latitude") or r.get("provider_latitude")),
            sf(r.get("longitude") or r.get("provider_longitude")),
            si(r.get("overall_rating")),
            s(r.get("ownership_type") or r.get("ownership"), 100),
            None,  # emergency_services
            si(r.get("number_of_certified_beds") or r.get("bedcert")),
            s(r.get("federal_provider_number") or r.get("provnum"), 50),
            "cms_nursing_homes_v2",
        )
    return scrape_cms(conn, "4pq5-n9py", proc, "healthcare_facilities", HC_SQL,
                       "cms_nursing_homes_v2", "CMS Nursing Homes")


# =============================================================================
# 4. CMS HOME HEALTH AGENCIES (12.3K) — 6jpm-sxkc
# =============================================================================

def scrape_cms_home_health(conn):
    log("=== CMS Home Health Agencies (12.3K) ===")
    def proc(r):
        state = s(r.get("state"), 2)
        name = s(r.get("provider_name"), 200)
        if not state or not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("cms_certification_number_ccn"), 50),
            name,
            "Home Health Agency",
            s(r.get("address"), 500),
            s(r.get("citytown") or r.get("city"), 100),
            state,
            s(r.get("zip_code") or r.get("zip"), 10),
            s(r.get("county") or r.get("county_parish"), 100),
            s(r.get("phone") or r.get("phone_number"), 20),
            None, None,  # lat, lng
            si(r.get("quality_of_patient_care_star_rating")),
            s(r.get("type_of_ownership"), 100),
            None, None,  # emergency, beds
            s(r.get("cms_certification_number_ccn"), 50),
            "cms_home_health_v2",
        )
    return scrape_cms(conn, "6jpm-sxkc", proc, "healthcare_facilities", HC_SQL,
                       "cms_home_health_v2", "CMS Home Health Agencies")


# =============================================================================
# 5. CMS DIALYSIS FACILITIES (7.6K) — 23ew-n7w9
# =============================================================================

def scrape_cms_dialysis(conn):
    log("=== CMS Dialysis Facilities (7.6K) ===")
    def proc(r):
        state = s(r.get("state") or r.get("provider_state"), 2)
        name = s(r.get("facility_name") or r.get("provider_name"), 200)
        if not state or not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("cms_certification_number_ccn") or r.get("provider_number"), 50),
            name,
            "Dialysis Facility",
            s(r.get("address_line_1") or r.get("address") or r.get("provider_address"), 500),
            s(r.get("citytown") or r.get("city") or r.get("provider_city"), 100),
            state,
            s(r.get("zip_code") or r.get("zip"), 10),
            s(r.get("county") or r.get("county_parish"), 100),
            s(r.get("telephone_number") or r.get("phone_number"), 20),
            None, None,  # lat, lng
            si(r.get("five_star") or r.get("star_rating")),
            s(r.get("chain_ownership") or r.get("chain_organization"), 100),
            None, None,  # emergency, beds
            s(r.get("cms_certification_number_ccn") or r.get("provider_number"), 50),
            "cms_dialysis",
        )
    return scrape_cms(conn, "23ew-n7w9", proc, "healthcare_facilities", HC_SQL,
                       "cms_dialysis", "CMS Dialysis Facilities")


# =============================================================================
# 6. NCES PUBLIC SCHOOLS (102K) — Urban Institute API
# =============================================================================

SCHOOL_SQL = """INSERT INTO schools (id, school_id, school_name, school_type,
    district_name, address, city, state, zip, county, lat, lng,
    grade_range, enrollment, student_teacher_ratio, title_i, magnet, charter, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_nces_schools(conn):
    log("=== NCES Public Schools (102K) ===")
    existing = get_count(conn, "schools", "nces_public_schools")
    if existing > 1000:
        log(f"  SKIP -- already {existing:,} records"); return 0

    cur = conn.cursor(); total = 0; offset = 0

    while True:
        url = f"https://educationdata.urban.org/api/v1/schools/ccd/directory/2022/?limit=100&offset={offset}"
        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            records = data.get("results", [])
        except Exception as e:
            log(f"  Error at offset {offset}: {e}"); break
        if not records: break

        batch = []
        for r in records:
            state = s(r.get("state_location"), 2)
            name = s(r.get("school_name"), 200)
            if not state or not name: continue
            # Map school_level codes to labels
            level = r.get("school_level")
            level_map = {1: "Primary", 2: "Middle", 3: "High", 4: "Other", -1: "Not Reported"}
            level_label = level_map.get(level, str(level) if level else None)
            batch.append((
                str(uuid.uuid4()),
                s(r.get("ncessch") or r.get("school_id"), 50),
                name,
                level_label,
                s(r.get("lea_name"), 200),
                s(r.get("street_location"), 500),
                s(r.get("city_location"), 100),
                state,
                s(r.get("zip_location"), 10),
                s(r.get("county_name"), 100),
                sf(r.get("latitude")),
                sf(r.get("longitude")),
                None,  # grade_range
                si(r.get("enrollment")),
                sf(r.get("teachers_fte")),  # using teachers_fte — ratio calc possible later
                sb(r.get("title_i_eligible")),
                sb(r.get("magnet")),
                sb(r.get("charter")),
                "nces_public_schools",
            ))
        if batch:
            for i in range(0, len(batch), BATCH_SIZE):
                chunk = batch[i:i+BATCH_SIZE]
                try:
                    execute_values(cur, SCHOOL_SQL, chunk); conn.commit()
                    total += len(chunk)
                    log(f"    {total:,}")
                except Exception as e:
                    log(f"    Insert error: {e}"); conn.rollback()
        offset += len(records)
        if len(records) < 100: break
        time.sleep(0.3)
    cur.close(); return total


# =============================================================================
# 7. COLLEGE SCORECARD (6.3K) — data.gov API
# =============================================================================

COL_SQL = """INSERT INTO colleges (id, name, city, state, lat, lng, enrollment, avg_sat, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_college_scorecard(conn):
    log("=== College Scorecard (6.3K) ===")
    existing = get_count(conn, "colleges", "college_scorecard")
    if existing > 1000:
        log(f"  SKIP -- already {existing:,} records"); return 0

    cur = conn.cursor(); total = 0; page = 0

    while True:
        url = (
            f"https://api.data.gov/ed/collegescorecard/v1/schools.json"
            f"?api_key=DEMO_KEY&per_page=100&page={page}"
            f"&fields=school.name,school.city,school.state,location.lat,location.lon,"
            f"latest.student.size,latest.admissions.sat_scores.average.overall"
        )
        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            records = data.get("results", [])
        except Exception as e:
            log(f"  Error at page {page}: {e}"); break
        if not records: break

        batch = []
        for r in records:
            state = s(r.get("school.state"), 2)
            name = s(r.get("school.name"), 200)
            if not state or not name: continue
            batch.append((
                str(uuid.uuid4()),
                name,
                s(r.get("school.city"), 100),
                state,
                sf(r.get("location.lat")),
                sf(r.get("location.lon")),
                si(r.get("latest.student.size")),
                sf(r.get("latest.admissions.sat_scores.average.overall")),
                "college_scorecard",
            ))
        if batch:
            try:
                execute_values(cur, COL_SQL, batch); conn.commit()
                total += len(batch)
                log(f"    {total:,}")
            except Exception as e:
                log(f"    Insert error: {e}"); conn.rollback()
        page += 1
        if len(records) < 100: break
        time.sleep(0.5)  # respect DEMO_KEY rate limit
    cur.close(); return total


# =============================================================================
# 8. FDIC BANK BRANCHES (78K) — FDIC API
# =============================================================================

BANK_SQL = """INSERT INTO bank_branches (id, institution_name, address, city, state, zip, lat, lng, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_fdic_banks(conn):
    log("=== FDIC Bank Branches (78K) ===")
    existing = get_count(conn, "bank_branches", "fdic")
    if existing > 1000:
        log(f"  SKIP -- already {existing:,} records"); return 0

    cur = conn.cursor(); total = 0; offset = 0

    while True:
        url = (
            f"https://api.fdic.gov/banks/locations"
            f"?fields=NAME,ADDRESS,CITY,STALP,ZIP,LATITUDE,LONGITUDE"
            f"&limit=10000&offset={offset}"
        )
        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            records = data.get("data", [])
        except Exception as e:
            log(f"  Error at offset {offset}: {e}"); break
        if not records: break

        batch = []
        for rec in records:
            # FDIC returns data as list of dicts with "data" key containing field values
            # Each record: {"data": {"NAME": "...", "ADDRESS": "...", ...}}
            r = rec.get("data", rec) if isinstance(rec, dict) else {}
            state = s(r.get("STALP"), 2)
            name = s(r.get("NAME"), 200)
            if not state or not name: continue
            batch.append((
                str(uuid.uuid4()),
                name,
                s(r.get("ADDRESS"), 500),
                s(r.get("CITY"), 100),
                state,
                s(r.get("ZIP"), 10),
                sf(r.get("LATITUDE")),
                sf(r.get("LONGITUDE")),
                "fdic",
            ))
        if batch:
            for i in range(0, len(batch), BATCH_SIZE):
                chunk = batch[i:i+BATCH_SIZE]
                try:
                    execute_values(cur, BANK_SQL, chunk); conn.commit()
                    total += len(chunk)
                    log(f"    {total:,}")
                except Exception as e:
                    log(f"    Insert error: {e}"); conn.rollback()
        offset += 10000
        if len(records) < 10000: break
        time.sleep(0.3)
    cur.close(); return total


# =============================================================================
# 9. CO RESTAURANT INSPECTIONS (655K) — Socrata
# =============================================================================

FOOD_SQL = """INSERT INTO food_inspections (id, inspection_id, business_name, business_type,
    address, city, state, zip, lat, lng, inspection_date, inspection_type,
    result, risk_level, violations, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_co_food(conn):
    log("=== CO Restaurant Inspections (655K) ===")
    def proc(r):
        name = s(r.get("facility_name") or r.get("facilityname") or r.get("restaurant_name"), 200)
        if not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("activityid") or r.get("inspection_id"), 50),
            name,
            s(r.get("facility_type") or r.get("facilitytype"), 100),
            s(r.get("facility_address") or r.get("address"), 500),
            s(r.get("facility_city") or r.get("city"), 100),
            "CO",
            s(r.get("facility_zip") or r.get("zip"), 10),
            None, None,  # lat, lng
            sd(r.get("activity_date") or r.get("inspectiondate")),
            s(r.get("activity_type_txt") or r.get("inspection_type"), 100),
            s(r.get("action") or r.get("result"), 200),
            s(r.get("risk_category") or r.get("risk"), 50),
            si(r.get("violation_count")),
            "co_food_inspections",
        )
    return scrape_socrata(conn, "https://data.colorado.gov/resource/tuvj-xz3m.json",
        proc, "food_inspections", FOOD_SQL, "co_food_inspections", "CO Restaurant Inspections")


# =============================================================================
# 10. PA FOOD INSPECTIONS — Socrata
# =============================================================================

def scrape_pa_food(conn):
    log("=== PA Food Inspections ===")
    def proc(r):
        name = s(r.get("facility_name") or r.get("facilityname") or r.get("name"), 200)
        if not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("inspection_id") or r.get("id"), 50),
            name,
            s(r.get("facility_type") or r.get("category"), 100),
            s(r.get("address") or r.get("street"), 500),
            s(r.get("city") or r.get("municipality"), 100),
            "PA",
            s(r.get("zip") or r.get("zip_code"), 10),
            sf(r.get("latitude")),
            sf(r.get("longitude")),
            sd(r.get("inspection_date") or r.get("encounter_date")),
            s(r.get("inspection_type") or r.get("purpose"), 100),
            s(r.get("inspection_result") or r.get("result") or r.get("overall_rating"), 200),
            s(r.get("risk") or r.get("risk_category"), 50),
            si(r.get("num_violations") or r.get("violation_count")),
            "pa_food_inspections",
        )
    return scrape_socrata(conn, "https://data.pa.gov/resource/etb6-jzdg.json",
        proc, "food_inspections", FOOD_SQL, "pa_food_inspections", "PA Food Inspections")


# =============================================================================
# 11. NY CANNABIS RETAILERS (560) — Socrata
# =============================================================================

LIQ_SQL = """INSERT INTO liquor_licenses (id, license_number, business_name, license_type,
    address, city, state, zip, status, issue_date, expiration_date, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_ny_cannabis(conn):
    log("=== NY Cannabis Retailers (560) ===")
    def proc(r):
        name = s(r.get("trade_name") or r.get("legal_business_name") or r.get("dba")
                 or r.get("applicant") or r.get("licensee"), 200)
        if not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("license_number") or r.get("license_no") or r.get("application_id"), 50),
            name,
            s(r.get("license_type") or r.get("license_category") or "Cannabis Retail", 100),
            s(r.get("address") or r.get("premises_address") or r.get("street_address"), 500),
            s(r.get("city") or r.get("municipality"), 100),
            "NY",
            s(r.get("zip") or r.get("zip_code"), 10),
            s(r.get("license_status") or r.get("status"), 50),
            sd(r.get("effective_date") or r.get("issue_date")),
            sd(r.get("expiration_date")),
            "ny_cannabis",
        )
    return scrape_socrata(conn, "https://data.ny.gov/resource/gttd-5u6y.json",
        proc, "liquor_licenses", LIQ_SQL, "ny_cannabis", "NY Cannabis Retailers")


# =============================================================================
# 12. CO LIQUOR LICENSES (20K) — Socrata
# =============================================================================

def scrape_co_liquor(conn):
    log("=== CO Liquor Licenses (20K) ===")
    def proc(r):
        name = s(r.get("licensee_dba_name") or r.get("dba") or r.get("business_name")
                 or r.get("licensee") or r.get("trade_name"), 200)
        if not name: return None
        return (
            str(uuid.uuid4()),
            s(r.get("license_number") or r.get("license_no") or r.get("account_number"), 50),
            name,
            s(r.get("license_type") or r.get("license_class") or r.get("type"), 100),
            s(r.get("premises_address") or r.get("address") or r.get("street_address"), 500),
            s(r.get("premises_city") or r.get("city"), 100),
            "CO",
            s(r.get("premises_zip") or r.get("zip") or r.get("zip_code"), 10),
            s(r.get("license_status") or r.get("status"), 50),
            sd(r.get("issue_date") or r.get("effective_date")),
            sd(r.get("expiration_date") or r.get("exp_date")),
            "co_liquor",
        )
    return scrape_socrata(conn, "https://data.colorado.gov/resource/ier5-5ms2.json",
        proc, "liquor_licenses", LIQ_SQL, "co_liquor", "CO Liquor Licenses")


# =============================================================================
# MASTER RUNNER
# =============================================================================

ALL_SCRAPERS = [
    # Healthcare (CMS API)
    ("CMS Doctors/Clinicians (2.8M)", scrape_cms_doctors),
    ("CMS Hospitals (5.4K)", scrape_cms_hospitals),
    ("CMS Nursing Homes (14.7K)", scrape_cms_nursing_homes),
    ("CMS Home Health Agencies (12.3K)", scrape_cms_home_health),
    ("CMS Dialysis Facilities (7.6K)", scrape_cms_dialysis),
    # Education
    ("NCES Public Schools (102K)", scrape_nces_schools),
    ("College Scorecard (6.3K)", scrape_college_scorecard),
    # Banking
    ("FDIC Bank Branches (78K)", scrape_fdic_banks),
    # Food Safety (Socrata)
    ("CO Restaurant Inspections (655K)", scrape_co_food),
    ("PA Food Inspections", scrape_pa_food),
    # Cannabis
    ("NY Cannabis Retailers (560)", scrape_ny_cannabis),
    # Liquor
    ("CO Liquor Licenses (20K)", scrape_co_liquor),
]


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="Multi-industry data scraper V2 — verified endpoints")
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
