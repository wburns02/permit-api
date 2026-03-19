#!/usr/bin/env python3
"""
Weekend Mega Scraper — runs ALL known high-quality data sources unattended.

Designed to run on R730 over a weekend, working through 30+ Socrata datasets
sequentially. Logs progress, skips already-loaded datasets, handles errors gracefully.

Usage:
    nohup python3 -u weekend_mega_scraper.py --db-host 100.122.216.15 > /tmp/weekend_scraper.log 2>&1 &

Requires: pip install httpx psycopg2-binary
"""

import argparse
import csv
import gzip
import io
import json
import os
import sys
import time
import uuid
from datetime import date, datetime
from pathlib import Path

import httpx

try:
    import psycopg2
    from psycopg2.extras import execute_values, Json
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

BATCH_SIZE = 5000
PAGE_SIZE = 50000
DELAY = 0.3  # Be respectful but fast


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def safe_float(v):
    if v in (None, "", "NA"):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def safe_date(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace("T00:00:00.000", "").replace("Z", "")).date()
    except Exception:
        pass
    try:
        return datetime.strptime(v[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def safe_datetime(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except Exception:
        return safe_date(v)


def ensure_table(conn, ddl):
    """Create table if not exists."""
    cur = conn.cursor()
    cur.execute(ddl)
    conn.commit()
    cur.close()


def get_count(conn, table, source=None):
    """Get current row count for a source in a table."""
    cur = conn.cursor()
    if source:
        cur.execute(f"SELECT count(*) FROM {table} WHERE source = %s", (source,))
    else:
        cur.execute(f"SELECT count(*) FROM {table}")
    count = cur.fetchone()[0]
    cur.close()
    return count


def scrape_socrata(base_url, process_row, conn, table, insert_sql, source_name, skip_if_exists=True):
    """Generic Socrata scraper. Returns total records loaded."""
    if skip_if_exists:
        existing = get_count(conn, table, source_name)
        if existing > 1000:
            log(f"  SKIP — {source_name} already has {existing:,} records in {table}")
            return 0

    # Get count
    try:
        r = httpx.get(f"{base_url}?$select=count(*)", timeout=30)
        total_available = int(r.json()[0]["count"])
        log(f"  Available: {total_available:,} records")
    except Exception as e:
        log(f"  Could not get count: {e}")
        total_available = None

    cur = conn.cursor()
    total = 0
    offset = 0

    while True:
        url = f"{base_url}?$limit={PAGE_SIZE}&$offset={offset}&$order=:id"
        log(f"  Fetching offset {offset:,}...")
        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            log(f"  Error at offset {offset}: {e}")
            break

        if not records:
            break

        batch = []
        for r in records:
            row = process_row(r)
            if row:
                batch.append(row)

        if batch:
            try:
                execute_values(cur, insert_sql, batch)
                conn.commit()
                total += len(batch)
                pct = f" ({total * 100 // total_available}%)" if total_available else ""
                log(f"    Loaded {total:,}{pct}")
            except Exception as e:
                log(f"    Insert error: {e}")
                conn.rollback()

        offset += PAGE_SIZE
        if len(records) < PAGE_SIZE:
            break
        time.sleep(DELAY)

    cur.close()
    return total


# =============================================================================
# DATASET DEFINITIONS — each is a function that runs one scraper
# =============================================================================

def scrape_acris_parties(conn):
    """NYC ACRIS Parties — 46M buyer/seller names for every deed/lien."""
    log("=== NYC ACRIS Parties (46M) ===")

    ensure_table(conn, """
        CREATE TABLE IF NOT EXISTS acris_parties (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            document_id TEXT,
            party_type TEXT,
            name TEXT,
            address_1 TEXT,
            address_2 TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            country TEXT,
            source TEXT NOT NULL DEFAULT 'nyc_acris_parties'
        )
    """)
    cur = conn.cursor()
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS ix_acris_parties_doc ON acris_parties (document_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_acris_parties_name ON acris_parties (name)")
        conn.commit()
    except Exception:
        conn.rollback()
    cur.close()

    def process(r):
        name = r.get("name", "")
        if not name:
            return None
        return (
            str(uuid.uuid4()),
            r.get("document_id", "")[:100] or None,
            (r.get("party_type") or "")[:20] or None,
            name[:500],
            (r.get("address_1") or "")[:500] or None,
            (r.get("address_2") or "")[:500] or None,
            (r.get("city") or "")[:100] or None,
            (r.get("state") or "")[:2] or None,
            (r.get("zip") or "")[:10] or None,
            (r.get("country") or "")[:10] or None,
            "nyc_acris_parties",
        )

    return scrape_socrata(
        "https://data.cityofnewyork.us/resource/636b-3b5g.json",
        process, conn, "acris_parties",
        """INSERT INTO acris_parties (id, document_id, party_type, name, address_1, address_2,
            city, state, zip, country, source) VALUES %s""",
        "nyc_acris_parties",
    )


def scrape_oath_hearings(conn):
    """NYC OATH Hearings — 21.5M adjudicated violations with fines."""
    log("=== NYC OATH Hearings (21.5M) ===")

    def process(r):
        return (
            str(uuid.uuid4()),
            (r.get("ticket_number") or "")[:100] or None,
            (r.get("violation_location_house") or "") + " " + (r.get("violation_location_street_name") or ""),
            "New York",
            "NY",
            (r.get("violation_location_zip_code") or "")[:10] or None,
            "OATH Hearing",
            (r.get("charge_1_code") or "")[:100] or None,
            (r.get("charge_1_code_description") or r.get("violation_details") or "")[:1000] or None,
            (r.get("hearing_result") or "")[:50] or None,
            safe_date(r.get("violation_date")),
            safe_date(r.get("hearing_date")),
            None,
            safe_float(r.get("penalty_imposed")),
            None, None,
            "nyc_oath",
        )

    return scrape_socrata(
        "https://data.cityofnewyork.us/resource/jz4z-kudi.json",
        process, conn, "code_violations",
        """INSERT INTO code_violations (id, violation_id, address, city, state, zip,
            violation_type, violation_code, description, status, violation_date,
            inspection_date, resolution_date, fine_amount, lat, lng, source)
            VALUES %s ON CONFLICT DO NOTHING""",
        "nyc_oath",
    )


def scrape_maryland_assessments(conn):
    """Maryland Statewide Property Assessments — 2.4M parcels."""
    log("=== Maryland Statewide Assessments (2.4M) ===")

    ensure_table(conn, """
        CREATE TABLE IF NOT EXISTS property_assessments (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            account_id TEXT,
            address TEXT,
            city TEXT,
            state VARCHAR(2) NOT NULL DEFAULT 'MD',
            zip TEXT,
            county TEXT,
            owner_name TEXT,
            assessed_value FLOAT,
            land_value FLOAT,
            improvement_value FLOAT,
            sale_price FLOAT,
            sale_date DATE,
            year_built INTEGER,
            building_type TEXT,
            land_use TEXT,
            lot_size FLOAT,
            living_area FLOAT,
            bedrooms INTEGER,
            bathrooms FLOAT,
            source TEXT NOT NULL DEFAULT 'md_sdat'
        )
    """)
    cur = conn.cursor()
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS ix_assess_addr ON property_assessments (address)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_assess_state ON property_assessments (state, city)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_assess_owner ON property_assessments (owner_name)")
        conn.commit()
    except Exception:
        conn.rollback()
    cur.close()

    def process(r):
        addr = (r.get("premise_address") or r.get("street_address") or "")[:500]
        if not addr:
            return None
        return (
            str(uuid.uuid4()),
            (r.get("acctid") or r.get("account_id") or "")[:100] or None,
            addr,
            (r.get("city") or r.get("premise_city") or "")[:100] or None,
            "MD",
            (r.get("zip_code") or "")[:10] or None,
            (r.get("county") or "")[:100] or None,
            (r.get("owner_name") or r.get("owner_1") or "")[:500] or None,
            safe_float(r.get("cur_full_val") or r.get("assessed_value")),
            safe_float(r.get("cur_land_val") or r.get("land_value")),
            safe_float(r.get("cur_impr_val") or r.get("improvement_value")),
            safe_float(r.get("considamt1") or r.get("sale_price")),
            safe_date(r.get("saledt1") or r.get("sale_date")),
            int(float(r.get("yearblt") or r.get("year_built") or 0)) or None,
            (r.get("desclu") or r.get("building_type") or "")[:100] or None,
            (r.get("lu_desclu") or r.get("land_use") or "")[:100] or None,
            safe_float(r.get("lotsizesfla") or r.get("lot_size")),
            safe_float(r.get("sqftstrc") or r.get("living_area")),
            int(float(r.get("no_bdrms") or r.get("bedrooms") or 0)) or None,
            safe_float(r.get("no_full_ba") or r.get("bathrooms")),
            "md_sdat",
        )

    return scrape_socrata(
        "https://opendata.maryland.gov/resource/ed4q-f8tm.json",
        process, conn, "property_assessments",
        """INSERT INTO property_assessments (id, account_id, address, city, state, zip,
            county, owner_name, assessed_value, land_value, improvement_value,
            sale_price, sale_date, year_built, building_type, land_use,
            lot_size, living_area, bedrooms, bathrooms, source)
            VALUES %s ON CONFLICT DO NOTHING""",
        "md_sdat",
    )


def scrape_cook_county_sales(conn):
    """Cook County Assessor Parcel Sales — 2.6M transactions."""
    log("=== Cook County Parcel Sales (2.6M) ===")

    def process(r):
        price = safe_float(r.get("sale_price") or r.get("price"))
        return (
            str(uuid.uuid4()),
            (r.get("sale_document_num") or "")[:100] or None,
            None,  # address not in this dataset
            "Chicago",
            "IL",
            None,
            None,
            price,
            safe_date(r.get("sale_date")),
            None,
            (r.get("sale_type") or "")[:50] or None,
            None, None, None, None, None, None, None, None,
            "cook_county_sales",
        )

    return scrape_socrata(
        "https://datacatalog.cookcountyil.gov/resource/wvhk-k5uv.json",
        process, conn, "property_sales",
        """INSERT INTO property_sales (id, document_id, address, city, state, zip, borough,
            sale_price, sale_date, recorded_date, doc_type, grantor, grantee,
            property_type, building_class, residential_units, land_sqft, gross_sqft,
            lat, lng, source) VALUES %s ON CONFLICT DO NOTHING""",
        "cook_county_sales",
    )


def scrape_nyc_annualized_sales(conn):
    """NYC Annualized Sales — 761K with full property details."""
    log("=== NYC Annualized Sales (761K) ===")

    def process(r):
        price = safe_float(r.get("sale_price"))
        if not price or price <= 0:
            return None
        return (
            str(uuid.uuid4()),
            None,
            (r.get("address") or "")[:500] or None,
            "New York",
            "NY",
            (r.get("zip_code") or "")[:10] or None,
            (r.get("borough") or "")[:50] or None,
            price,
            safe_date(r.get("sale_date")),
            None,
            "SALE",
            None, None,
            (r.get("building_class_category") or "")[:100] or None,
            (r.get("building_class_at_time_of_sale") or "")[:50] or None,
            int(float(r.get("residential_units") or 0)) or None,
            safe_float(r.get("land_square_feet")),
            safe_float(r.get("gross_square_feet")),
            safe_float(r.get("latitude")),
            safe_float(r.get("longitude")),
            "nyc_annualized_sales",
        )

    return scrape_socrata(
        "https://data.cityofnewyork.us/resource/w2pb-icbu.json",
        process, conn, "property_sales",
        """INSERT INTO property_sales (id, document_id, address, city, state, zip, borough,
            sale_price, sale_date, recorded_date, doc_type, grantor, grantee,
            property_type, building_class, residential_units, land_sqft, gross_sqft,
            lat, lng, source) VALUES %s ON CONFLICT DO NOTHING""",
        "nyc_annualized_sales",
    )


def scrape_more_violations(conn):
    """Scrape additional code violation cities not yet loaded."""
    cities = [
        ("Kansas City", "MO", "https://data.kcmo.org/resource/nhtf-e75a.json", "kc_violations",
         {"address": "address", "status": "status", "violation_date": "case_opened_date", "description": "violation_type"}),
        ("Seattle", "WA", "https://cos-data.seattle.gov/resource/ez4a-iug7.json", "seattle_violations",
         {"address": "address", "status": "statuscurrent", "violation_date": "opendate", "description": "description", "zip": "zip", "lat": "latitude", "lng": "longitude"}),
        ("Orlando", "FL", "https://data.cityoforlando.net/resource/k6e8-nw6w.json", "orlando_violations",
         {"address": "address", "violation_date": "casedt", "description": "casetype", "status": None}),
        ("Norfolk", "VA", "https://data.norfolk.gov/resource/agip-sqwc.json", "norfolk_violations",
         {"address": "address", "violation_date": "created_date", "description": "ordinance", "status": "status", "lat": "latitude", "lng": "longitude"}),
        ("New Orleans", "LA", "https://data.nola.gov/resource/3ehi-je3s.json", "nola_violations",
         {"address": "location", "violation_date": "violationdate", "description": "violation", "violation_code": "codesection"}),
        ("Dallas", "TX", "https://www.dallasopendata.com/resource/x9pz-kdq9.json", "dallas_violations",
         {"address": "str_nam", "violation_date": "created", "description": "nuisance", "status": "status"}),
        ("Plano", "TX", "https://dashboard.plano.gov/resource/5e5j-txgt.json", "plano_violations",
         {"address": "site_addr", "violation_date": "date_observed", "description": "violation_type", "status": "violation_status"}),
    ]

    total_all = 0
    for city_name, state, url, source, fields in cities:
        log(f"=== {city_name}, {state} Violations ===")

        def make_processor(cn, st, flds, src):
            def process(r):
                addr = (r.get(flds.get("address", ""), "") or "")[:500]
                return (
                    str(uuid.uuid4()),
                    None,
                    addr or None,
                    cn, st, (r.get(flds.get("zip", ""), "") or "")[:10] or None,
                    None, None,
                    (r.get(flds.get("description", ""), "") or "")[:1000] or None,
                    (r.get(flds.get("status", ""), "") or "")[:50] or None if flds.get("status") else None,
                    safe_date(r.get(flds.get("violation_date", ""))),
                    None, None, None,
                    safe_float(r.get(flds.get("lat", ""))),
                    safe_float(r.get(flds.get("lng", ""))),
                    src,
                )
            return process

        count = scrape_socrata(
            url, make_processor(city_name, state, fields, source),
            conn, "code_violations",
            """INSERT INTO code_violations (id, violation_id, address, city, state, zip,
                violation_type, violation_code, description, status, violation_date,
                inspection_date, resolution_date, fine_amount, lat, lng, source)
                VALUES %s ON CONFLICT DO NOTHING""",
            source,
        )
        total_all += count

    return total_all


def scrape_utility_connections(conn):
    """NYC utility connection data — electrical permits + water/sewer permits."""
    log("=== NYC Utility Connections ===")

    ensure_table(conn, """
        CREATE TABLE IF NOT EXISTS utility_connections (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            address TEXT,
            city TEXT,
            state VARCHAR(2) NOT NULL,
            zip TEXT,
            borough TEXT,
            connection_type TEXT,
            new_meters INTEGER,
            remove_meters INTEGER,
            filing_date DATE,
            description TEXT,
            status TEXT,
            lat FLOAT,
            lng FLOAT,
            source TEXT NOT NULL
        )
    """)
    cur = conn.cursor()
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS ix_utility_addr ON utility_connections (address)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_utility_state ON utility_connections (state, city)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_utility_type ON utility_connections (connection_type)")
        conn.commit()
    except Exception:
        conn.rollback()
    cur.close()

    total = 0

    # NYC DOB Electrical Permits (550K)
    log("--- NYC DOB Electrical Permits (550K) ---")
    def process_electrical(r):
        return (
            str(uuid.uuid4()),
            ((r.get("house_number") or "") + " " + (r.get("street_name") or "")).strip()[:500] or None,
            "New York", "NY",
            (r.get("zip_code") or "")[:10] or None,
            (r.get("borough") or "")[:50] or None,
            "Electrical",
            int(float(r.get("new_meters") or 0)) or None,
            int(float(r.get("remove_meters") or 0)) or None,
            safe_date(r.get("filing_date")),
            (r.get("job_description") or "")[:500] or None,
            (r.get("filing_status") or "")[:50] or None,
            None, None,
            "nyc_dob_electrical",
        )
    total += scrape_socrata(
        "https://data.cityofnewyork.us/resource/dm9a-ab7w.json",
        process_electrical, conn, "utility_connections",
        """INSERT INTO utility_connections (id, address, city, state, zip, borough,
            connection_type, new_meters, remove_meters, filing_date, description,
            status, lat, lng, source) VALUES %s ON CONFLICT DO NOTHING""",
        "nyc_dob_electrical",
    )

    # NYC DEP Water/Sewer Permits (311K)
    log("--- NYC DEP Water/Sewer Permits (311K) ---")
    def process_water(r):
        return (
            str(uuid.uuid4()),
            None,  # No street address in this dataset
            "New York", "NY", None,
            (r.get("propertyborough") or "")[:50] or None,
            (r.get("applicationtype") or "Water/Sewer")[:100],
            None, None,
            safe_date(r.get("issuancedate")),
            (r.get("applicationtype") or "")[:500] or None,
            (r.get("requeststatus") or "")[:50] or None,
            None, None,
            "nyc_dep_water",
        )
    total += scrape_socrata(
        "https://data.cityofnewyork.us/resource/hphy-6g7m.json",
        process_water, conn, "utility_connections",
        """INSERT INTO utility_connections (id, address, city, state, zip, borough,
            connection_type, new_meters, remove_meters, filing_date, description,
            status, lat, lng, source) VALUES %s ON CONFLICT DO NOTHING""",
        "nyc_dep_water",
    )

    return total


def scrape_more_business_entities(conn):
    """Additional business entity datasets — NY filings, CT history, DE licenses."""
    total = 0

    # NY All Filings (20.5M) — extends our 4.1M active corps
    log("=== NY All Corporation Filings (20.5M) ===")
    def process_ny(r):
        name = r.get("current_entity_name") or r.get("entity_name") or ""
        if not name:
            return None
        return (
            str(uuid.uuid4()),
            name[:500],
            (r.get("entity_type") or "")[:50] or None,
            "NY",
            (r.get("dos_id") or "")[:100] or None,
            None,
            safe_date(r.get("initial_dos_filing_date")),
            None,
            (r.get("dos_process_name") or "")[:500] or None,
            None, None, None, None,
            "ny_dos_all_filings",
            date.today(),
        )
    total += scrape_socrata(
        "https://data.ny.gov/resource/63wc-4exh.json",
        process_ny, conn, "business_entities",
        """INSERT INTO business_entities (id, entity_name, entity_type, state,
            filing_number, status, formation_date, dissolution_date,
            registered_agent_name, registered_agent_address, principal_address,
            mailing_address, officers, source, scraped_at)
            VALUES %s ON CONFLICT DO NOTHING""",
        "ny_dos_all_filings",
    )

    # Delaware Historical Business Licenses (1M)
    log("=== Delaware Historical Business Licenses (1M) ===")
    def process_de(r):
        name = r.get("licensee_name") or r.get("business_name") or ""
        if not name:
            return None
        return (
            str(uuid.uuid4()),
            name[:500],
            (r.get("license_type") or "")[:50] or None,
            "DE",
            (r.get("license_number") or "")[:100] or None,
            (r.get("license_status") or "")[:50] or None,
            safe_date(r.get("issue_date") or r.get("original_issue_date")),
            safe_date(r.get("expiration_date")),
            None, None,
            ((r.get("address_line_1") or "") + ", " + (r.get("city") or "") + ", DE " + (r.get("zip_code") or ""))[:500] or None,
            None, None,
            "de_business_licenses",
            date.today(),
        )
    total += scrape_socrata(
        "https://data.delaware.gov/resource/khpy-2pnr.json",
        process_de, conn, "business_entities",
        """INSERT INTO business_entities (id, entity_name, entity_type, state,
            filing_number, status, formation_date, dissolution_date,
            registered_agent_name, registered_agent_address, principal_address,
            mailing_address, officers, source, scraped_at)
            VALUES %s ON CONFLICT DO NOTHING""",
        "de_business_licenses",
    )

    # Oregon UCC Secured Parties (222K)
    log("=== Oregon UCC Secured Parties (222K) ===")
    def process_or_ucc(r):
        return (
            str(uuid.uuid4()),
            None,
            "UCC Filing",
            (r.get("FILING_NUMBER") or r.get("filing_number") or "")[:100] or None,
            None, None, "OR", None,
            safe_float(None),
            safe_date(r.get("FILING_DATE") or r.get("filing_date")),
            safe_date(r.get("LAPSE_DATE") or r.get("lapse_date")),
            None,
            (r.get("PARTY_NAME") or r.get("secured_party_name") or "")[:500] or None,
            None, None,
            "or_ucc",
        )
    total += scrape_socrata(
        "https://data.oregon.gov/resource/2kf7-i54h.json",
        process_or_ucc, conn, "property_liens",
        """INSERT INTO property_liens (id, document_id, lien_type, filing_number,
            address, city, state, zip, amount, filing_date, lapse_date, status,
            debtor_name, creditor_name, description, source)
            VALUES %s ON CONFLICT DO NOTHING""",
        "or_ucc",
    )

    return total


# =============================================================================
# MASTER RUNNER
# =============================================================================

ALL_SCRAPERS = [
    ("NYC ACRIS Parties (46M)", scrape_acris_parties),
    ("NYC OATH Hearings (21.5M)", scrape_oath_hearings),
    ("Maryland Assessments (2.4M)", scrape_maryland_assessments),
    ("Cook County Sales (2.6M)", scrape_cook_county_sales),
    ("NYC Annualized Sales (761K)", scrape_nyc_annualized_sales),
    ("Additional Violations (7 cities)", scrape_more_violations),
    ("Utility Connections (NYC)", scrape_utility_connections),
    ("More Business Entities (NY/DE/OR)", scrape_more_business_entities),
]


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="Weekend Mega Scraper — run ALL sources")
    parser.add_argument("--db-host", default=DB_HOST)
    parser.add_argument("--skip", nargs="*", default=[], help="Source names to skip")
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to PostgreSQL at {DB_HOST}")

    grand_total = 0
    results = []

    for name, func in ALL_SCRAPERS:
        if any(s.lower() in name.lower() for s in args.skip):
            log(f"\n*** SKIPPING: {name} ***")
            continue

        log(f"\n{'='*60}")
        log(f"*** STARTING: {name} ***")
        log(f"{'='*60}")

        try:
            count = func(conn)
            grand_total += count
            results.append((name, count, "OK"))
            log(f"*** COMPLETE: {name} — {count:,} records ***")
        except Exception as e:
            log(f"*** FAILED: {name} — {e} ***")
            results.append((name, 0, f"FAILED: {e}"))
            conn.rollback()

    conn.close()

    log(f"\n{'='*60}")
    log(f"WEEKEND SCRAPER COMPLETE")
    log(f"{'='*60}")
    log(f"Grand total: {grand_total:,} new records")
    log(f"\nResults:")
    for name, count, status in results:
        log(f"  {name}: {count:,} ({status})")


if __name__ == "__main__":
    main()
