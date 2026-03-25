#!/usr/bin/env python3
"""
International Scraper — UK and Australian open data.

Sources:
  UK:
    1. HM Land Registry Price Paid (28M+ property sales, 2020-2025)
    2. UK Planning Applications (100K via planning.data.gov.uk)
    3. UK Companies House (all live companies, ~5M, 491MB ZIP)
    4. UK Flood Risk Zones (780K via planning.data.gov.uk)
    5. UK Listed Buildings (382K via planning.data.gov.uk)

  Australia:
    6. Melbourne Building Permits (182K)
    7. Casey VIC Building Permits (180K)
    8. QLD QBCC Licensed Contractors (72MB CSV)
    9. VIC Building Practitioner Register (48K CSV)
   10. NSW Contractor Licences (XLSX)
   11. ASIC Business Names (3.3M CSV)

Usage:
    python scrape_international.py --db-host 100.122.216.15
    python scrape_international.py --db-host 100.122.216.15 --source uk_land_registry
    python scrape_international.py --db-host 100.122.216.15 --source melbourne_permits
    python scrape_international.py --source all

    nohup python3 -u scrape_international.py --db-host 100.122.216.15 > /tmp/intl_scrape.log 2>&1 &

Requires: pip install httpx psycopg2-binary openpyxl
"""

import argparse, csv, io, json, os, sys, time, uuid, zipfile, tempfile
from datetime import date, datetime
from pathlib import Path

import httpx

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary"); sys.exit(1)

# ── Config ───────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"
BATCH_SIZE = 5000

# Large file staging — NEVER use /home/will for big downloads
STAGING_DIR = "/mnt/win11/Fedora/intl_staging"

# ── Helpers ──────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)

def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)

def sf(v):
    """Safe float."""
    if v in (None, "", "NA", "N/A", ".", "-"):
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").replace("£", "").strip())
    except Exception:
        return None

def sd(v):
    """Safe date."""
    if not v:
        return None
    v_str = str(v).strip()
    # ISO format cleanup
    try:
        return datetime.fromisoformat(
            v_str.replace("T00:00:00.000", "").replace("Z", "").replace("+00:00", "")
        ).date()
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d", "%d-%b-%Y", "%d %b %Y",
                "%d/%m/%y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(v_str[:min(len(v_str), 20)], fmt).date()
        except Exception:
            continue
    return None

def s(v, m=500):
    """Safe string, truncated to m chars."""
    if not v:
        return None
    return str(v).strip()[:m] or None

def get_count(conn, table, source=None):
    cur = conn.cursor()
    if source:
        cur.execute(f"SELECT count(*) FROM {table} WHERE source = %s", (source,))
    else:
        cur.execute(f"SELECT count(*) FROM {table}")
    c = cur.fetchone()[0]
    cur.close()
    return c

def ensure_staging():
    """Create staging directory for large downloads."""
    os.makedirs(STAGING_DIR, exist_ok=True)
    log(f"Staging dir: {STAGING_DIR}")

# ── Table Setup ──────────────────────────────────────────────────────────────

def ensure_tables(conn):
    cur = conn.cursor()

    # New international_permits table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS international_permits (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            permit_number TEXT,
            permit_type TEXT,
            description TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            country VARCHAR(3) NOT NULL,
            postcode TEXT,
            lat FLOAT,
            lng FLOAT,
            issue_date DATE,
            decision_date DATE,
            estimated_cost FLOAT,
            applicant TEXT,
            status TEXT,
            source TEXT NOT NULL
        )
    """)

    # UK-specific: flood risk zones
    cur.execute("""
        CREATE TABLE IF NOT EXISTS uk_flood_risk_zones (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            entity_id TEXT,
            name TEXT,
            dataset TEXT,
            flood_zone TEXT,
            geometry_type TEXT,
            lat FLOAT,
            lng FLOAT,
            reference TEXT,
            organisation TEXT,
            entry_date DATE,
            start_date DATE,
            end_date DATE,
            source TEXT NOT NULL DEFAULT 'uk_planning_flood'
        )
    """)

    # UK-specific: listed buildings
    cur.execute("""
        CREATE TABLE IF NOT EXISTS uk_listed_buildings (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            entity_id TEXT,
            name TEXT,
            reference TEXT,
            listed_building_grade TEXT,
            dataset TEXT,
            lat FLOAT,
            lng FLOAT,
            organisation TEXT,
            entry_date DATE,
            start_date DATE,
            end_date DATE,
            source TEXT NOT NULL DEFAULT 'uk_listed_buildings'
        )
    """)

    # Widen state columns on existing tables to accommodate international codes
    for tbl in ("property_sales", "business_entities", "professional_licenses"):
        try:
            cur.execute(f"ALTER TABLE {tbl} ALTER COLUMN state TYPE VARCHAR(10)")
        except Exception:
            conn.rollback()

    # Indexes for international_permits
    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_intl_permits_country_state ON international_permits (country, state)",
        "CREATE INDEX IF NOT EXISTS ix_intl_permits_address ON international_permits (address)",
        "CREATE INDEX IF NOT EXISTS ix_intl_permits_issue_date ON international_permits (issue_date)",
        "CREATE INDEX IF NOT EXISTS ix_intl_permits_source ON international_permits (source)",
        "CREATE INDEX IF NOT EXISTS ix_intl_permits_postcode ON international_permits (postcode)",
        # flood risk
        "CREATE INDEX IF NOT EXISTS ix_ukflood_zone ON uk_flood_risk_zones (flood_zone)",
        "CREATE INDEX IF NOT EXISTS ix_ukflood_geo ON uk_flood_risk_zones (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_ukflood_entity ON uk_flood_risk_zones (entity_id)",
        # listed buildings
        "CREATE INDEX IF NOT EXISTS ix_uklb_grade ON uk_listed_buildings (listed_building_grade)",
        "CREATE INDEX IF NOT EXISTS ix_uklb_geo ON uk_listed_buildings (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_uklb_entity ON uk_listed_buildings (entity_id)",
    ]:
        try:
            cur.execute(idx)
        except Exception:
            conn.rollback()

    conn.commit()
    cur.close()


# =============================================================================
# 1. UK HM Land Registry Price Paid (2020-2025, ~5M/year)
# =============================================================================

SALES_INSERT = """INSERT INTO property_sales
    (id, document_id, address, city, state, zip, borough, sale_price, sale_date,
     recorded_date, doc_type, grantor, grantee, property_type, building_class,
     residential_units, land_sqft, gross_sqft, lat, lng, source)
    VALUES %s ON CONFLICT DO NOTHING"""

# Land Registry CSV columns (no header):
# 0: transaction_id, 1: price, 2: date, 3: postcode, 4: property_type,
# 5: old_new (Y/N), 6: duration (F/L), 7: paon, 8: saon, 9: street,
# 10: locality, 11: town, 12: district, 13: county, 14: ppd_category
PROPERTY_TYPE_MAP = {
    "D": "Detached", "S": "Semi-Detached", "T": "Terraced",
    "F": "Flat/Maisonette", "O": "Other"
}

def scrape_uk_land_registry(conn):
    log("=== UK HM Land Registry Price Paid (2020-2025) ===")
    ensure_staging()

    cur = conn.cursor()
    grand_total = 0

    for year in range(2020, 2026):
        source_tag = f"uk_land_registry_{year}"
        existing = get_count(conn, "property_sales", source_tag)
        if existing > 1000:
            log(f"  SKIP {year} — already {existing:,} records")
            continue

        url = f"https://price-paid-data.publicdata.landregistry.gov.uk/pp-{year}.csv"
        csv_path = os.path.join(STAGING_DIR, f"pp-{year}.csv")

        # Download if not already present
        if not os.path.exists(csv_path):
            log(f"  Downloading {year}...")
            try:
                with httpx.stream("GET", url, timeout=300, follow_redirects=True) as resp:
                    resp.raise_for_status()
                    with open(csv_path, "wb") as f:
                        for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                            f.write(chunk)
                size_mb = os.path.getsize(csv_path) / (1024 * 1024)
                log(f"  Downloaded {year}: {size_mb:.0f} MB")
            except Exception as e:
                log(f"  FAIL downloading {year}: {e}")
                if os.path.exists(csv_path):
                    os.remove(csv_path)
                continue
        else:
            size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            log(f"  Using cached {year}: {size_mb:.0f} MB")

        # Parse CSV and insert in batches
        log(f"  Parsing {year}...")
        total = 0
        batch = []

        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 14:
                    continue

                txn_id = row[0].strip().strip("{}")
                price = sf(row[1])
                sale_date = sd(row[2])
                postcode = s(row[3], 10)
                prop_type = PROPERTY_TYPE_MAP.get(row[4].strip(), row[4].strip())
                old_new = "New Build" if row[5].strip() == "Y" else "Existing"
                duration = "Freehold" if row[6].strip() == "F" else "Leasehold"
                paon = s(row[7], 200)
                saon = s(row[8], 200)
                street = s(row[9], 200)
                locality = s(row[10], 200)
                town = s(row[11], 100)
                district = s(row[12], 100)
                county = s(row[13], 100)

                # Build address from parts
                addr_parts = [p for p in [saon, paon, street, locality] if p]
                address = ", ".join(addr_parts) if addr_parts else None

                batch.append((
                    str(uuid.uuid4()),   # id
                    s(txn_id, 100),      # document_id
                    address,             # address
                    town,                # city
                    "UK",                # state (country code)
                    postcode,            # zip (postcode)
                    district,            # borough (district)
                    price,               # sale_price
                    sale_date,           # sale_date
                    None,                # recorded_date
                    old_new,             # doc_type (old/new)
                    county,              # grantor (county — reuse field)
                    None,                # grantee
                    prop_type,           # property_type
                    duration,            # building_class (freehold/leasehold)
                    None,                # residential_units
                    None,                # land_sqft
                    None,                # gross_sqft
                    None,                # lat
                    None,                # lng
                    source_tag,          # source
                ))

                if len(batch) >= BATCH_SIZE:
                    try:
                        execute_values(cur, SALES_INSERT, batch)
                        conn.commit()
                        total += len(batch)
                        if total % 50000 == 0:
                            log(f"    {year}: {total:,}")
                    except Exception as e:
                        log(f"    Insert error: {e}")
                        conn.rollback()
                    batch = []

        # Final batch
        if batch:
            try:
                execute_values(cur, SALES_INSERT, batch)
                conn.commit()
                total += len(batch)
            except Exception as e:
                log(f"    Final insert error: {e}")
                conn.rollback()

        log(f"  {year} DONE: {total:,} records")
        grand_total += total

    cur.close()
    return grand_total


# =============================================================================
# 2. UK Planning Applications (100K via planning.data.gov.uk)
# =============================================================================

INTL_PERMIT_INSERT = """INSERT INTO international_permits
    (id, permit_number, permit_type, description, address, city, state,
     country, postcode, lat, lng, issue_date, decision_date,
     estimated_cost, applicant, status, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_uk_planning(conn):
    log("=== UK Planning Applications (100K+) ===")
    source = "uk_planning_apps"
    existing = get_count(conn, "international_permits", source)
    if existing > 5000:
        log(f"  SKIP — already {existing:,} records")
        return 0

    cur = conn.cursor()
    total = 0
    offset = 0
    limit = 100

    while True:
        url = f"https://www.planning.data.gov.uk/entity.json?dataset=planning-application&limit={limit}&offset={offset}"
        try:
            resp = httpx.get(url, timeout=60, follow_redirects=True)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log(f"  Error at offset {offset}: {e}")
            break

        entities = data.get("entities", [])
        if not entities:
            break

        batch = []
        for e in entities:
            # Extract lat/lng from point field (WKT POINT format)
            lat, lng = None, None
            point = e.get("point")
            if point and "POINT" in str(point):
                try:
                    coords = str(point).replace("POINT(", "").replace(")", "").strip()
                    parts = coords.split()
                    if len(parts) == 2:
                        lng = float(parts[0])
                        lat = float(parts[1])
                except Exception:
                    pass

            batch.append((
                str(uuid.uuid4()),
                s(e.get("reference"), 200),        # permit_number
                "Planning Application",            # permit_type
                s(e.get("description"), 2000),     # description (can be long)
                s(e.get("address"), 500),           # address
                None,                               # city
                "UK",                               # state
                "GBR",                              # country
                None,                               # postcode
                lat,                                # lat
                lng,                                # lng
                sd(e.get("entry-date")),            # issue_date
                sd(e.get("decision-date")),         # decision_date
                None,                               # estimated_cost
                None,                               # applicant
                s(e.get("status"), 100),            # status
                source,                             # source
            ))

        if batch:
            try:
                execute_values(cur, INTL_PERMIT_INSERT, batch)
                conn.commit()
                total += len(batch)
                if total % 5000 == 0:
                    log(f"    {total:,}")
            except Exception as e:
                log(f"    Insert error: {e}")
                conn.rollback()

        offset += limit
        if len(entities) < limit:
            break
        time.sleep(0.3)

    cur.close()
    log(f"  UK Planning DONE: {total:,}")
    return total


# =============================================================================
# 3. UK Companies House (all live companies, 491MB ZIP)
# =============================================================================

def scrape_uk_companies_house(conn):
    log("=== UK Companies House (5M+ companies) ===")
    source = "uk_companies_house"
    existing = get_count(conn, "business_entities", source)
    if existing > 10000:
        log(f"  SKIP — already {existing:,} records")
        return 0

    ensure_staging()
    zip_path = os.path.join(STAGING_DIR, "companies_house.zip")
    csv_path = None

    # Try to download the ZIP
    url = "https://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-2026-03-01.zip"
    if not os.path.exists(zip_path):
        log("  Downloading Companies House ZIP (~491MB)...")
        try:
            with httpx.stream("GET", url, timeout=600, follow_redirects=True) as resp:
                resp.raise_for_status()
                with open(zip_path, "wb") as f:
                    total_dl = 0
                    for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
                        total_dl += len(chunk)
                        if total_dl % (50 * 1024 * 1024) == 0:
                            log(f"    Downloaded {total_dl // (1024*1024)} MB")
            size_mb = os.path.getsize(zip_path) / (1024 * 1024)
            log(f"  Downloaded: {size_mb:.0f} MB")
        except Exception as e:
            log(f"  FAIL download: {e}")
            if os.path.exists(zip_path):
                os.remove(zip_path)
            return 0
    else:
        log(f"  Using cached ZIP: {os.path.getsize(zip_path) // (1024*1024)} MB")

    # Extract CSV from ZIP
    log("  Extracting CSV from ZIP...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            csv_names = [n for n in zf.namelist() if n.endswith('.csv')]
            if not csv_names:
                log("  ERROR: no CSV found in ZIP")
                return 0
            csv_name = csv_names[0]
            csv_path = os.path.join(STAGING_DIR, "companies_house.csv")
            if not os.path.exists(csv_path):
                zf.extract(csv_name, STAGING_DIR)
                extracted = os.path.join(STAGING_DIR, csv_name)
                if extracted != csv_path:
                    os.rename(extracted, csv_path)
            size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            log(f"  Extracted: {size_mb:.0f} MB")
    except Exception as e:
        log(f"  FAIL extract: {e}")
        return 0

    # Parse CSV
    log("  Parsing Companies House CSV...")
    cur = conn.cursor()
    total = 0
    batch = []

    ENTITY_INSERT = """INSERT INTO business_entities
        (id, entity_name, entity_type, state, filing_number, status,
         formation_date, dissolution_date, registered_agent_name,
         registered_agent_address, principal_address, mailing_address,
         officers, source, scraped_at)
        VALUES %s ON CONFLICT DO NOTHING"""

    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = s(row.get("CompanyName"), 500)
            if not name:
                continue

            # Build address
            addr_parts = [
                row.get("RegAddress.AddressLine1", "").strip(),
                row.get("RegAddress.AddressLine2", "").strip(),
            ]
            addr = ", ".join([p for p in addr_parts if p]) or None

            postcode = s(row.get("RegAddress.PostCode"), 20)
            full_addr = f"{addr}, {postcode}" if addr and postcode else (addr or postcode)

            batch.append((
                str(uuid.uuid4()),
                name,                                        # entity_name
                s(row.get("CompanyCategory"), 50),           # entity_type
                "UK",                                        # state
                s(row.get("CompanyNumber"), 100),            # filing_number
                s(row.get("CompanyStatus"), 50),             # status
                sd(row.get("IncorporationDate")),            # formation_date
                sd(row.get("DissolutionDate")),              # dissolution_date
                None,                                        # registered_agent_name
                None,                                        # registered_agent_address
                s(full_addr, 500),                           # principal_address
                None,                                        # mailing_address
                json.dumps({"sic_code": s(row.get("SICCode.SicText_1"), 200)})
                    if row.get("SICCode.SicText_1") else None,  # officers (reuse for SIC)
                source,                                      # source
                date.today(),                                # scraped_at
            ))

            if len(batch) >= BATCH_SIZE:
                try:
                    execute_values(cur, ENTITY_INSERT, batch)
                    conn.commit()
                    total += len(batch)
                    if total % 100000 == 0:
                        log(f"    {total:,}")
                except Exception as e:
                    log(f"    Insert error: {e}")
                    conn.rollback()
                batch = []

    if batch:
        try:
            execute_values(cur, ENTITY_INSERT, batch)
            conn.commit()
            total += len(batch)
        except Exception as e:
            log(f"    Final insert error: {e}")
            conn.rollback()

    cur.close()
    log(f"  Companies House DONE: {total:,}")
    return total


# =============================================================================
# 4. UK Flood Risk Zones (780K via planning.data.gov.uk)
# =============================================================================

def scrape_uk_flood_risk(conn):
    log("=== UK Flood Risk Zones (780K) ===")
    source = "uk_planning_flood"
    existing = get_count(conn, "uk_flood_risk_zones", source)
    if existing > 5000:
        log(f"  SKIP — already {existing:,} records")
        return 0

    cur = conn.cursor()
    total = 0
    offset = 0
    limit = 100

    while True:
        url = (f"https://www.planning.data.gov.uk/entity.json"
               f"?dataset=flood-risk-zone&limit={limit}&offset={offset}")
        try:
            resp = httpx.get(url, timeout=60, follow_redirects=True)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log(f"  Error at offset {offset}: {e}")
            break

        entities = data.get("entities", [])
        if not entities:
            break

        batch = []
        for e in entities:
            lat, lng = None, None
            point = e.get("point")
            if point and "POINT" in str(point):
                try:
                    coords = str(point).replace("POINT(", "").replace(")", "").strip()
                    parts = coords.split()
                    if len(parts) == 2:
                        lng = float(parts[0])
                        lat = float(parts[1])
                except Exception:
                    pass

            batch.append((
                str(uuid.uuid4()),
                s(str(e.get("entity", "")), 100),    # entity_id
                s(e.get("name"), 500),                # name
                "flood-risk-zone",                    # dataset
                s(e.get("flood-risk-type"), 100),     # flood_zone
                s(e.get("geometry-type"), 50),         # geometry_type
                lat,
                lng,
                s(e.get("reference"), 200),            # reference
                s(e.get("organisation-entity"), 200),  # organisation
                sd(e.get("entry-date")),               # entry_date
                sd(e.get("start-date")),               # start_date
                sd(e.get("end-date")),                 # end_date
                source,
            ))

        if batch:
            try:
                execute_values(cur, """INSERT INTO uk_flood_risk_zones
                    (id, entity_id, name, dataset, flood_zone, geometry_type,
                     lat, lng, reference, organisation, entry_date, start_date,
                     end_date, source) VALUES %s ON CONFLICT DO NOTHING""", batch)
                conn.commit()
                total += len(batch)
                if total % 10000 == 0:
                    log(f"    {total:,}")
            except Exception as e:
                log(f"    Insert error at offset {offset}: {e}")
                conn.rollback()

        offset += limit
        if len(entities) < limit:
            break
        time.sleep(0.3)

    cur.close()
    log(f"  UK Flood Risk DONE: {total:,}")
    return total


# =============================================================================
# 5. UK Listed Buildings (382K via planning.data.gov.uk)
# =============================================================================

def scrape_uk_listed_buildings(conn):
    log("=== UK Listed Buildings (382K) ===")
    source = "uk_listed_buildings"
    existing = get_count(conn, "uk_listed_buildings", source)
    if existing > 5000:
        log(f"  SKIP — already {existing:,} records")
        return 0

    cur = conn.cursor()
    total = 0
    offset = 0
    limit = 100

    while True:
        url = (f"https://www.planning.data.gov.uk/entity.json"
               f"?dataset=listed-building-outline&limit={limit}&offset={offset}")
        try:
            resp = httpx.get(url, timeout=60, follow_redirects=True)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log(f"  Error at offset {offset}: {e}")
            break

        entities = data.get("entities", [])
        if not entities:
            break

        batch = []
        for e in entities:
            lat, lng = None, None
            point = e.get("point")
            if point and "POINT" in str(point):
                try:
                    coords = str(point).replace("POINT(", "").replace(")", "").strip()
                    parts = coords.split()
                    if len(parts) == 2:
                        lng = float(parts[0])
                        lat = float(parts[1])
                except Exception:
                    pass

            batch.append((
                str(uuid.uuid4()),
                s(str(e.get("entity", "")), 100),      # entity_id
                s(e.get("name"), 500),                  # name
                s(e.get("reference"), 200),              # reference
                s(e.get("listed-building-grade"), 10),   # grade
                "listed-building-outline",               # dataset
                lat,
                lng,
                s(e.get("organisation-entity"), 200),    # organisation
                sd(e.get("entry-date")),                 # entry_date
                sd(e.get("start-date")),                 # start_date
                sd(e.get("end-date")),                   # end_date
                source,
            ))

        if batch:
            try:
                execute_values(cur, """INSERT INTO uk_listed_buildings
                    (id, entity_id, name, reference, listed_building_grade, dataset,
                     lat, lng, organisation, entry_date, start_date, end_date, source)
                    VALUES %s ON CONFLICT DO NOTHING""", batch)
                conn.commit()
                total += len(batch)
                if total % 10000 == 0:
                    log(f"    {total:,}")
            except Exception as e:
                log(f"    Insert error at offset {offset}: {e}")
                conn.rollback()

        offset += limit
        if len(entities) < limit:
            break
        time.sleep(0.3)

    cur.close()
    log(f"  UK Listed Buildings DONE: {total:,}")
    return total


# =============================================================================
# 6. Melbourne Building Permits (182K)
# =============================================================================

def scrape_melbourne_permits(conn):
    log("=== Melbourne VIC Building Permits (182K) ===")
    source = "melbourne_permits"
    existing = get_count(conn, "international_permits", source)
    if existing > 5000:
        log(f"  SKIP — already {existing:,} records")
        return 0

    cur = conn.cursor()
    total = 0
    offset = 0
    limit = 100

    while True:
        url = (f"https://data.melbourne.vic.gov.au/api/v2/catalog/datasets/"
               f"building-permits/records?limit={limit}&offset={offset}")
        try:
            resp = httpx.get(url, timeout=60, follow_redirects=True)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log(f"  Error at offset {offset}: {e}")
            break

        records = data.get("records", [])
        if not records:
            break

        batch = []
        for rec in records:
            r = rec.get("record", {}).get("fields", {})
            if not r:
                r = rec.get("fields", rec)

            lat, lng = None, None
            geo = r.get("geo_point_2d") or r.get("geopoint")
            if isinstance(geo, dict):
                lat = sf(geo.get("lat"))
                lng = sf(geo.get("lon") or geo.get("lng"))

            batch.append((
                str(uuid.uuid4()),
                s(r.get("permit_number") or r.get("council_ref"), 100),
                s(r.get("permit_type") or r.get("type_of_work"), 100),
                s(r.get("description"), 2000),
                s(r.get("address") or r.get("street_address"), 500),
                "Melbourne",
                "VIC",
                "AUS",
                s(r.get("postcode"), 10),
                lat,
                lng,
                sd(r.get("issue_date") or r.get("date_permit_issued")),
                None,
                sf(r.get("estimated_cost") or r.get("cost_of_building_work")),
                s(r.get("applicant"), 200),
                s(r.get("status"), 100),
                source,
            ))

        if batch:
            try:
                execute_values(cur, INTL_PERMIT_INSERT, batch)
                conn.commit()
                total += len(batch)
                if total % 5000 == 0:
                    log(f"    {total:,}")
            except Exception as e:
                log(f"    Insert error at offset {offset}: {e}")
                conn.rollback()

        offset += limit
        if len(records) < limit:
            break
        time.sleep(0.3)

    cur.close()
    log(f"  Melbourne Permits DONE: {total:,}")
    return total


# =============================================================================
# 7. Casey VIC Building Permits (180K)
# =============================================================================

def scrape_casey_permits(conn):
    log("=== Casey VIC Building Permits (180K) ===")
    source = "casey_permits"
    existing = get_count(conn, "international_permits", source)
    if existing > 5000:
        log(f"  SKIP — already {existing:,} records")
        return 0

    cur = conn.cursor()
    total = 0
    offset = 0
    limit = 100

    while True:
        url = (f"https://data.casey.vic.gov.au/api/v2/catalog/datasets/"
               f"register-of-building-permit-applications-in-the-city-of-casey/"
               f"records?limit={limit}&offset={offset}")
        try:
            resp = httpx.get(url, timeout=60, follow_redirects=True)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log(f"  Error at offset {offset}: {e}")
            break

        records = data.get("records", [])
        if not records:
            break

        batch = []
        for rec in records:
            r = rec.get("record", {}).get("fields", {})
            if not r:
                r = rec.get("fields", rec)

            lat, lng = None, None
            geo = r.get("geo_point_2d") or r.get("geopoint")
            if isinstance(geo, dict):
                lat = sf(geo.get("lat"))
                lng = sf(geo.get("lon") or geo.get("lng"))

            batch.append((
                str(uuid.uuid4()),
                s(r.get("permit_number") or r.get("council_ref"), 100),
                s(r.get("permit_type") or r.get("nature_of_work"), 100),
                s(r.get("description") or r.get("nature_of_work"), 2000),
                s(r.get("address") or r.get("property_address"), 500),
                "Casey",
                "VIC",
                "AUS",
                s(r.get("postcode"), 10),
                lat,
                lng,
                sd(r.get("issue_date") or r.get("date_permit_issued") or r.get("permit_date")),
                None,
                sf(r.get("estimated_cost") or r.get("cost_of_building_work")),
                s(r.get("applicant"), 200),
                s(r.get("status"), 100),
                source,
            ))

        if batch:
            try:
                execute_values(cur, INTL_PERMIT_INSERT, batch)
                conn.commit()
                total += len(batch)
                if total % 5000 == 0:
                    log(f"    {total:,}")
            except Exception as e:
                log(f"    Insert error at offset {offset}: {e}")
                conn.rollback()

        offset += limit
        if len(records) < limit:
            break
        time.sleep(0.3)

    cur.close()
    log(f"  Casey Permits DONE: {total:,}")
    return total


# =============================================================================
# 8. QLD QBCC Licensed Contractors (72MB CSV)
# =============================================================================

LICENSE_INSERT = """INSERT INTO professional_licenses
    (id, license_number, name, business_name, profession, license_type,
     address, city, state, zip, phone, email, status, issue_date,
     expiration_date, source)
    VALUES %s ON CONFLICT DO NOTHING"""

def scrape_qld_qbcc(conn):
    log("=== QLD QBCC Licensed Contractors (72MB CSV) ===")
    source = "qld_qbcc"
    existing = get_count(conn, "professional_licenses", source)
    if existing > 5000:
        log(f"  SKIP — already {existing:,} records")
        return 0

    ensure_staging()
    csv_path = os.path.join(STAGING_DIR, "qld_qbcc.csv")
    url = ("https://www.data.qld.gov.au/dataset/"
           "980b6499-c0b4-491b-ba9c-1c7506368a50/resource/"
           "25608781-b28c-44f8-8545-0ab18d84082f/download/"
           "builder-contractor-qbcc-licensee-register.csv")

    if not os.path.exists(csv_path):
        log("  Downloading QBCC CSV (~72MB)...")
        try:
            with httpx.stream("GET", url, timeout=300, follow_redirects=True) as resp:
                resp.raise_for_status()
                with open(csv_path, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
            log(f"  Downloaded: {os.path.getsize(csv_path) // (1024*1024)} MB")
        except Exception as e:
            log(f"  FAIL download: {e}")
            if os.path.exists(csv_path):
                os.remove(csv_path)
            return 0
    else:
        log(f"  Using cached CSV: {os.path.getsize(csv_path) // (1024*1024)} MB")

    log("  Parsing QBCC CSV...")
    cur = conn.cursor()
    total = 0
    batch = []

    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = s(row.get("licensee_name") or row.get("Licensee Name")
                     or row.get("LICENSEE_NAME"), 500)
            if not name:
                continue

            batch.append((
                str(uuid.uuid4()),
                s(row.get("licence_number") or row.get("Licence Number")
                  or row.get("LICENCE_NUMBER"), 100),
                name,
                None,  # business_name
                s(row.get("category") or row.get("Category")
                  or row.get("CATEGORY"), 200),
                s(row.get("licence_class") or row.get("Licence Class")
                  or row.get("LICENCE_CLASS"), 100),
                s(row.get("address") or row.get("Address")
                  or row.get("ADDRESS"), 500),
                None,  # city
                "QLD",
                None,  # zip
                None,  # phone
                None,  # email
                s(row.get("status") or row.get("Status")
                  or row.get("STATUS"), 50),
                None,  # issue_date
                sd(row.get("expiry") or row.get("Expiry Date")
                   or row.get("EXPIRY")),
                source,
            ))

            if len(batch) >= BATCH_SIZE:
                try:
                    execute_values(cur, LICENSE_INSERT, batch)
                    conn.commit()
                    total += len(batch)
                    if total % 50000 == 0:
                        log(f"    {total:,}")
                except Exception as e:
                    log(f"    Insert error: {e}")
                    conn.rollback()
                batch = []

    if batch:
        try:
            execute_values(cur, LICENSE_INSERT, batch)
            conn.commit()
            total += len(batch)
        except Exception as e:
            log(f"    Final insert error: {e}")
            conn.rollback()

    cur.close()
    log(f"  QBCC DONE: {total:,}")
    return total


# =============================================================================
# 9. VIC Building Practitioner Register (48K CSV)
# =============================================================================

def scrape_vic_bpr(conn):
    log("=== VIC Building Practitioner Register (48K) ===")
    source = "vic_bpr"
    existing = get_count(conn, "professional_licenses", source)
    if existing > 1000:
        log(f"  SKIP — already {existing:,} records")
        return 0

    csv_url = "https://vicopendatavba.blob.core.windows.net/vicopendata/BPR.csv"
    log("  Downloading VIC BPR CSV...")

    try:
        resp = httpx.get(csv_url, timeout=120, follow_redirects=True)
        resp.raise_for_status()
        content = resp.text
    except Exception as e:
        log(f"  FAIL download: {e}")
        return 0

    log(f"  Downloaded: {len(content) // 1024} KB")
    cur = conn.cursor()
    total = 0
    batch = []

    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        name = s(row.get("account_name") or row.get("Account Name")
                 or row.get("ACCOUNT_NAME"), 500)
        if not name:
            continue

        batch.append((
            str(uuid.uuid4()),
            s(row.get("accreditation_id") or row.get("Accreditation ID")
              or row.get("ACCREDITATION_ID"), 100),
            name,
            None,  # business_name
            s(row.get("type") or row.get("Type") or row.get("TYPE"), 200),
            s(row.get("limitation") or row.get("Limitation")
              or row.get("LIMITATION"), 200),
            None,  # address
            None,  # city
            "VIC",
            None,  # zip
            None,  # phone
            None,  # email
            s(row.get("status") or row.get("Status") or row.get("STATUS"), 50),
            sd(row.get("commenced") or row.get("Commenced")
               or row.get("COMMENCED")),
            sd(row.get("expires") or row.get("Expires")
               or row.get("EXPIRES")),
            source,
        ))

        if len(batch) >= BATCH_SIZE:
            try:
                execute_values(cur, LICENSE_INSERT, batch)
                conn.commit()
                total += len(batch)
            except Exception as e:
                log(f"    Insert error: {e}")
                conn.rollback()
            batch = []

    if batch:
        try:
            execute_values(cur, LICENSE_INSERT, batch)
            conn.commit()
            total += len(batch)
        except Exception as e:
            log(f"    Final insert error: {e}")
            conn.rollback()

    cur.close()
    log(f"  VIC BPR DONE: {total:,}")
    return total


# =============================================================================
# 10. NSW Contractor Licences (XLSX)
# =============================================================================

def scrape_nsw_contractors(conn):
    log("=== NSW Contractor Licences (XLSX) ===")
    source = "nsw_contractors"
    existing = get_count(conn, "professional_licenses", source)
    if existing > 1000:
        log(f"  SKIP — already {existing:,} records")
        return 0

    try:
        import openpyxl
    except ImportError:
        log("  SKIP — openpyxl not installed (pip install openpyxl)")
        return 0

    url = "http://onegov.nsw.gov.au/agencies/oft/Contractor%20Licence.xlsx"
    ensure_staging()
    xlsx_path = os.path.join(STAGING_DIR, "nsw_contractor_licence.xlsx")

    if not os.path.exists(xlsx_path):
        log("  Downloading NSW Contractor Licences XLSX...")
        try:
            resp = httpx.get(url, timeout=120, follow_redirects=True)
            resp.raise_for_status()
            with open(xlsx_path, "wb") as f:
                f.write(resp.content)
            log(f"  Downloaded: {os.path.getsize(xlsx_path) // 1024} KB")
        except Exception as e:
            log(f"  FAIL download: {e}")
            return 0
    else:
        log(f"  Using cached XLSX: {os.path.getsize(xlsx_path) // 1024} KB")

    log("  Parsing XLSX...")
    wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    ws = wb.active

    # Read header row to map columns
    rows = ws.iter_rows(values_only=True)
    header = next(rows, None)
    if not header:
        log("  ERROR: empty spreadsheet")
        return 0

    # Normalize headers
    header = [str(h).strip().lower() if h else f"col_{i}" for i, h in enumerate(header)]
    log(f"  Columns: {header[:10]}")

    # Map common column name variations
    def find_col(names):
        for n in names:
            for i, h in enumerate(header):
                if n in h:
                    return i
        return None

    col_lic = find_col(["licence_number", "licence number", "license_number", "license number"])
    col_name = find_col(["entity_name", "entity name", "name", "licensee", "holder"])
    col_class = find_col(["licence_class", "licence class", "license_class", "class"])
    col_status = find_col(["status"])
    col_abn = find_col(["abn"])
    col_expiry = find_col(["expiry", "expiration", "expires"])

    cur = conn.cursor()
    total = 0
    batch = []

    for row in rows:
        name = s(row[col_name], 500) if col_name is not None and col_name < len(row) else None
        if not name:
            continue

        batch.append((
            str(uuid.uuid4()),
            s(row[col_lic], 100) if col_lic is not None and col_lic < len(row) else None,
            name,
            None,  # business_name
            "Contractor",
            s(row[col_class], 100) if col_class is not None and col_class < len(row) else None,
            None,  # address
            None,  # city
            "NSW",
            None,  # zip
            None,  # phone
            None,  # email
            s(row[col_status], 50) if col_status is not None and col_status < len(row) else None,
            None,  # issue_date
            sd(row[col_expiry]) if col_expiry is not None and col_expiry < len(row) else None,
            source,
        ))

        if len(batch) >= BATCH_SIZE:
            try:
                execute_values(cur, LICENSE_INSERT, batch)
                conn.commit()
                total += len(batch)
                if total % 10000 == 0:
                    log(f"    {total:,}")
            except Exception as e:
                log(f"    Insert error: {e}")
                conn.rollback()
            batch = []

    if batch:
        try:
            execute_values(cur, LICENSE_INSERT, batch)
            conn.commit()
            total += len(batch)
        except Exception as e:
            log(f"    Final insert error: {e}")
            conn.rollback()

    wb.close()
    cur.close()
    log(f"  NSW Contractors DONE: {total:,}")
    return total


# =============================================================================
# 11. ASIC Business Names (3.3M CSV)
# =============================================================================

# Australian state code mapping
AU_STATE_MAP = {
    "NSW": "NSW", "VIC": "VIC", "QLD": "QLD", "SA": "SA",
    "WA": "WA", "TAS": "TAS", "ACT": "ACT", "NT": "NT",
    "NEW SOUTH WALES": "NSW", "VICTORIA": "VIC", "QUEENSLAND": "QLD",
    "SOUTH AUSTRALIA": "SA", "WESTERN AUSTRALIA": "WA", "TASMANIA": "TAS",
    "AUSTRALIAN CAPITAL TERRITORY": "ACT", "NORTHERN TERRITORY": "NT",
}

def scrape_asic_business_names(conn):
    log("=== ASIC Business Names (3.3M CSV) ===")
    source = "asic_business_names"
    existing = get_count(conn, "business_entities", source)
    if existing > 10000:
        log(f"  SKIP — already {existing:,} records")
        return 0

    ensure_staging()
    csv_path = os.path.join(STAGING_DIR, "asic_business_names.csv")
    url = ("https://data.gov.au/data/dataset/"
           "bc515135-4bb6-4d50-957a-3713709a76d3/resource/"
           "55ad4b1c-5eeb-44ea-8b29-d410da431be3/download/"
           "business_names_202603.csv")

    if not os.path.exists(csv_path):
        log("  Downloading ASIC Business Names CSV...")
        try:
            with httpx.stream("GET", url, timeout=300, follow_redirects=True) as resp:
                resp.raise_for_status()
                with open(csv_path, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
            size_mb = os.path.getsize(csv_path) / (1024 * 1024)
            log(f"  Downloaded: {size_mb:.0f} MB")
        except Exception as e:
            log(f"  FAIL download: {e}")
            if os.path.exists(csv_path):
                os.remove(csv_path)
            return 0
    else:
        log(f"  Using cached CSV: {os.path.getsize(csv_path) // (1024*1024)} MB")

    ENTITY_INSERT = """INSERT INTO business_entities
        (id, entity_name, entity_type, state, filing_number, status,
         formation_date, dissolution_date, registered_agent_name,
         registered_agent_address, principal_address, mailing_address,
         officers, source, scraped_at)
        VALUES %s ON CONFLICT DO NOTHING"""

    log("  Parsing ASIC CSV...")
    cur = conn.cursor()
    total = 0
    batch = []

    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = s(row.get("business_name") or row.get("BN_NAME")
                     or row.get("Business Name"), 500)
            if not name:
                continue

            raw_state = (row.get("state") or row.get("BN_STATE")
                         or row.get("State") or "").strip().upper()
            state_code = AU_STATE_MAP.get(raw_state, raw_state[:10] if raw_state else "AU")

            abn = s(row.get("abn") or row.get("ABN") or row.get("Abn"), 20)

            batch.append((
                str(uuid.uuid4()),
                name,                                  # entity_name
                "Business Name",                       # entity_type
                state_code,                            # state
                abn,                                   # filing_number (ABN)
                s(row.get("status") or row.get("BN_STATUS")
                  or row.get("Status"), 50),           # status
                sd(row.get("registration_date") or row.get("BN_REG_DT")
                   or row.get("Registration Date")),   # formation_date
                sd(row.get("cancel_date") or row.get("BN_CANCEL_DT")
                   or row.get("Cancel Date")),         # dissolution_date
                None,                                  # registered_agent_name
                None,                                  # registered_agent_address
                None,                                  # principal_address
                None,                                  # mailing_address
                None,                                  # officers
                source,                                # source
                date.today(),                          # scraped_at
            ))

            if len(batch) >= BATCH_SIZE:
                try:
                    execute_values(cur, ENTITY_INSERT, batch)
                    conn.commit()
                    total += len(batch)
                    if total % 100000 == 0:
                        log(f"    {total:,}")
                except Exception as e:
                    log(f"    Insert error: {e}")
                    conn.rollback()
                batch = []

    if batch:
        try:
            execute_values(cur, ENTITY_INSERT, batch)
            conn.commit()
            total += len(batch)
        except Exception as e:
            log(f"    Final insert error: {e}")
            conn.rollback()

    cur.close()
    log(f"  ASIC Business Names DONE: {total:,}")
    return total


# =============================================================================
# MASTER RUNNER
# =============================================================================

ALL_SOURCES = [
    ("uk_land_registry",     "UK Land Registry Price Paid (28M+, 2020-2025)", scrape_uk_land_registry),
    ("uk_planning",          "UK Planning Applications (100K)",               scrape_uk_planning),
    ("uk_companies_house",   "UK Companies House (5M+)",                      scrape_uk_companies_house),
    ("uk_flood_risk",        "UK Flood Risk Zones (780K)",                    scrape_uk_flood_risk),
    ("uk_listed_buildings",  "UK Listed Buildings (382K)",                    scrape_uk_listed_buildings),
    ("melbourne_permits",    "Melbourne VIC Building Permits (182K)",         scrape_melbourne_permits),
    ("casey_permits",        "Casey VIC Building Permits (180K)",             scrape_casey_permits),
    ("qld_qbcc",             "QLD QBCC Licensed Contractors",                scrape_qld_qbcc),
    ("vic_bpr",              "VIC Building Practitioner Register (48K)",      scrape_vic_bpr),
    ("nsw_contractors",      "NSW Contractor Licences (XLSX)",               scrape_nsw_contractors),
    ("asic_business_names",  "ASIC Business Names (3.3M)",                   scrape_asic_business_names),
]

SOURCE_KEYS = [k for k, _, _ in ALL_SOURCES]


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="International Scraper — UK & Australia")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    parser.add_argument("--source", default="all",
                        choices=["all"] + SOURCE_KEYS,
                        help="Which source to scrape (default: all)")
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to {DB_HOST}")
    ensure_tables(conn)

    grand = 0
    results = []

    for key, name, func in ALL_SOURCES:
        if args.source != "all" and args.source != key:
            continue

        log(f"\n{'='*60}\n*** {name} ***\n{'='*60}")
        try:
            c = func(conn)
            grand += c
            results.append((name, c, "OK"))
            log(f"*** DONE: {name} -- {c:,} ***")
        except Exception as e:
            log(f"*** FAIL: {name} -- {e} ***")
            results.append((name, 0, f"FAIL: {e}"))
            conn.rollback()

    conn.close()
    log(f"\n{'='*60}\nCOMPLETE -- {grand:,} total records loaded\n{'='*60}")
    for n, c, st in results:
        log(f"  {n}: {c:,} ({st})")


if __name__ == "__main__":
    main()
