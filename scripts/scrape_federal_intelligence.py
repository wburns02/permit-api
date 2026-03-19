#!/usr/bin/env python3
"""
Federal Intelligence Scraper — free government datasets nobody else correlates with permits.

Sources:
1. BLS QCEW — Construction employment by county (3M+ quarterly records)
2. FHWA Federal Highway Projects (500K+)
3. USACE Civil Works Projects
4. FTA Transit Projects
5. HUD CDBG Grants (community development spending)
6. USDA Rural Development loans
7. Federal Procurement (USASpending construction contracts)

Usage:
    nohup python3 -u scrape_federal_intelligence.py --db-host 100.122.216.15 > /tmp/federal_intel.log 2>&1 &
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

import httpx

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"
BATCH_SIZE = 5000


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def safe_float(v):
    if v in (None, "", "NA", "N/A", ".", "(D)", "(S)"):
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def safe_int(v):
    if v in (None, "", "NA", "N/A", ".", "(D)", "(S)"):
        return None
    try:
        return int(float(str(v).replace(",", "").strip()))
    except (ValueError, TypeError):
        return None


def safe_date(v):
    if not v:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d", "%m/%d/%y"):
        try:
            return datetime.strptime(str(v).strip()[:10], fmt).date()
        except Exception:
            continue
    return None


def ensure_tables(conn):
    cur = conn.cursor()

    # Construction employment by county
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bls_construction_employment (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            state_fips VARCHAR(2),
            county_fips VARCHAR(3),
            area_title TEXT,
            industry_code TEXT,
            industry_title TEXT,
            year INTEGER,
            quarter INTEGER,
            establishments INTEGER,
            avg_monthly_employment INTEGER,
            total_wages FLOAT,
            avg_weekly_wage FLOAT,
            source TEXT NOT NULL DEFAULT 'bls_qcew'
        )
    """)

    # Federal infrastructure projects
    cur.execute("""
        CREATE TABLE IF NOT EXISTS federal_projects (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            project_id TEXT,
            project_name TEXT,
            description TEXT,
            agency TEXT,
            state VARCHAR(2),
            city TEXT,
            county TEXT,
            zip TEXT,
            amount FLOAT,
            start_date DATE,
            end_date DATE,
            status TEXT,
            project_type TEXT,
            lat FLOAT,
            lng FLOAT,
            source TEXT NOT NULL
        )
    """)

    # Federal spending / procurement (construction contracts)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS federal_spending (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            award_id TEXT,
            recipient_name TEXT,
            recipient_address TEXT,
            recipient_city TEXT,
            recipient_state VARCHAR(2),
            recipient_zip TEXT,
            award_amount FLOAT,
            award_date DATE,
            agency TEXT,
            award_type TEXT,
            naics_code TEXT,
            naics_description TEXT,
            description TEXT,
            place_of_performance_state VARCHAR(2),
            place_of_performance_city TEXT,
            place_of_performance_zip TEXT,
            source TEXT NOT NULL DEFAULT 'usaspending'
        )
    """)

    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_bls_emp_geo ON bls_construction_employment (state_fips, county_fips)",
        "CREATE INDEX IF NOT EXISTS ix_bls_emp_year ON bls_construction_employment (year, quarter)",
        "CREATE INDEX IF NOT EXISTS ix_bls_emp_industry ON bls_construction_employment (industry_code)",
        "CREATE INDEX IF NOT EXISTS ix_fed_proj_state ON federal_projects (state, project_type)",
        "CREATE INDEX IF NOT EXISTS ix_fed_proj_amount ON federal_projects (amount)",
        "CREATE INDEX IF NOT EXISTS ix_fed_spend_state ON federal_spending (place_of_performance_state)",
        "CREATE INDEX IF NOT EXISTS ix_fed_spend_naics ON federal_spending (naics_code)",
        "CREATE INDEX IF NOT EXISTS ix_fed_spend_recipient ON federal_spending (recipient_name)",
    ]:
        try:
            cur.execute(idx)
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()


def get_count(conn, table, source=None):
    cur = conn.cursor()
    if source:
        cur.execute(f"SELECT count(*) FROM {table} WHERE source = %s", (source,))
    else:
        cur.execute(f"SELECT count(*) FROM {table}")
    count = cur.fetchone()[0]
    cur.close()
    return count


# =============================================================================
# 1. BLS QCEW — Construction Employment by County
# =============================================================================

def scrape_bls_qcew(conn):
    """Download BLS Quarterly Census of Employment and Wages — construction sector."""
    log("=== BLS QCEW Construction Employment ===")

    existing = get_count(conn, "bls_construction_employment")
    if existing > 100000:
        log(f"  SKIP — already have {existing:,} QCEW records")
        return 0

    cur = conn.cursor()
    total = 0

    # QCEW data is available as CSV from BLS
    # Construction NAICS: 23 (Construction), 236 (Buildings), 237 (Heavy/Civil), 238 (Specialty)
    # URL pattern: https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_by_industry.zip
    years = [2023, 2024]

    for year in years:
        # Try the single-file CSV approach via BLS Open Data
        # BLS also publishes via Socrata-like API
        url = f"https://data.bls.gov/cew/data/api/{year}/1/area/US000.csv"
        log(f"  Trying BLS QCEW API for {year}...")

        # Alternative: use the BLS QCEW direct data files
        for qtr in range(1, 5):
            api_url = f"https://data.bls.gov/cew/data/api/{year}/{qtr}/industry/23.csv"
            log(f"    Fetching {year} Q{qtr} NAICS 23 (Construction)...")

            try:
                resp = httpx.get(api_url, timeout=60, follow_redirects=True)
                if resp.status_code != 200:
                    log(f"      HTTP {resp.status_code}, trying alternate...")
                    # Try alternate URL format
                    alt_url = f"https://data.bls.gov/cew/data/files/{year}/csv/{year}_qtrly_singlefile.zip"
                    log(f"      Alternate would be: {alt_url} (too large, skipping)")
                    continue

                reader = csv.DictReader(io.StringIO(resp.text))
                batch = []
                qtr_count = 0

                for row in reader:
                    # Filter for county-level data (area_fips length 5)
                    area_fips = row.get("area_fips") or ""
                    if len(area_fips) != 5:
                        continue

                    batch.append((
                        str(uuid.uuid4()),
                        area_fips[:2],  # state fips
                        area_fips[2:5],  # county fips
                        (row.get("area_title") or "")[:200] or None,
                        (row.get("industry_code") or "23")[:10],
                        (row.get("industry_title") or "Construction")[:200],
                        year,
                        qtr,
                        safe_int(row.get("qtrly_estabs") or row.get("annual_avg_estabs")),
                        safe_int(row.get("month1_emplvl") or row.get("annual_avg_emplvl")),
                        safe_float(row.get("total_qtrly_wages") or row.get("total_annual_wages")),
                        safe_float(row.get("avg_wkly_wage")),
                        "bls_qcew",
                    ))

                    if len(batch) >= BATCH_SIZE:
                        execute_values(cur, """
                            INSERT INTO bls_construction_employment (id, state_fips, county_fips,
                                area_title, industry_code, industry_title, year, quarter,
                                establishments, avg_monthly_employment, total_wages,
                                avg_weekly_wage, source) VALUES %s ON CONFLICT DO NOTHING
                        """, batch)
                        conn.commit()
                        qtr_count += len(batch)
                        total += len(batch)
                        batch = []

                if batch:
                    execute_values(cur, """
                        INSERT INTO bls_construction_employment (id, state_fips, county_fips,
                            area_title, industry_code, industry_title, year, quarter,
                            establishments, avg_monthly_employment, total_wages,
                            avg_weekly_wage, source) VALUES %s ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                    qtr_count += len(batch)
                    total += len(batch)

                log(f"      {year} Q{qtr}: {qtr_count:,} county records")

            except Exception as e:
                log(f"      {year} Q{qtr} error: {e}")
                conn.rollback()

            time.sleep(1)

    cur.close()
    return total


# =============================================================================
# 2. USASpending — Federal Construction Contracts
# =============================================================================

def scrape_usaspending(conn):
    """Download federal construction contracts from USASpending.gov API."""
    log("=== USASpending Federal Construction Contracts ===")

    existing = get_count(conn, "federal_spending")
    if existing > 100000:
        log(f"  SKIP — already have {existing:,} spending records")
        return 0

    cur = conn.cursor()
    total = 0

    # USASpending has a REST API
    # Search for construction contracts (NAICS 23xxxx)
    # API: https://api.usaspending.gov/api/v2/search/spending_by_award/
    construction_naics = ["236", "237", "238"]  # Buildings, Heavy/Civil, Specialty

    for naics in construction_naics:
        log(f"  Fetching NAICS {naics} contracts...")
        page = 1

        while page <= 20:  # Cap at 20 pages per NAICS
            try:
                payload = {
                    "filters": {
                        "naics_codes": [{"naics": naics, "require": [naics]}],
                        "time_period": [{"start_date": "2023-01-01", "end_date": "2026-12-31"}],
                        "award_type_codes": ["A", "B", "C", "D"],  # Contracts
                    },
                    "fields": [
                        "Award ID", "Recipient Name", "Award Amount",
                        "Start Date", "Description", "Awarding Agency",
                        "Awarding Sub Agency", "recipient_id",
                        "Place of Performance State Code",
                        "Place of Performance City Name",
                        "Place of Performance Zip5",
                        "NAICS Code", "NAICS Description",
                    ],
                    "limit": 100,
                    "page": page,
                    "sort": "Award Amount",
                    "order": "desc",
                }

                resp = httpx.post(
                    "https://api.usaspending.gov/api/v2/search/spending_by_award/",
                    json=payload, timeout=60,
                )
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])

                if not results:
                    break

                batch = []
                for r in results:
                    batch.append((
                        str(uuid.uuid4()),
                        (r.get("Award ID") or "")[:100] or None,
                        (r.get("Recipient Name") or "")[:500] or None,
                        None, None,  # address, city
                        None,  # state
                        None,  # zip
                        safe_float(r.get("Award Amount")),
                        safe_date(r.get("Start Date")),
                        (r.get("Awarding Agency") or "")[:200] or None,
                        "Contract",
                        (r.get("NAICS Code") or naics)[:10] or None,
                        (r.get("NAICS Description") or "")[:200] or None,
                        (r.get("Description") or "")[:1000] or None,
                        (r.get("Place of Performance State Code") or "")[:2] or None,
                        (r.get("Place of Performance City Name") or "")[:100] or None,
                        (r.get("Place of Performance Zip5") or "")[:10] or None,
                        "usaspending",
                    ))

                if batch:
                    execute_values(cur, """
                        INSERT INTO federal_spending (id, award_id, recipient_name,
                            recipient_address, recipient_city, recipient_state, recipient_zip,
                            award_amount, award_date, agency, award_type,
                            naics_code, naics_description, description,
                            place_of_performance_state, place_of_performance_city,
                            place_of_performance_zip, source)
                        VALUES %s ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                    total += len(batch)
                    log(f"    NAICS {naics} page {page}: {len(batch)} contracts (total: {total:,})")

                if not data.get("hasNext", False) and len(results) < 100:
                    break
                page += 1

            except Exception as e:
                log(f"    NAICS {naics} page {page} error: {e}")
                conn.rollback()
                break

            time.sleep(1)

    cur.close()
    return total


# =============================================================================
# 3. HUD CDBG + HOME Grants (Community Development)
# =============================================================================

def scrape_hud_grants(conn):
    """Download HUD Community Development Block Grant data."""
    log("=== HUD Community Development Grants ===")

    existing = get_count(conn, "federal_projects", "hud_cdbg")
    if existing > 10000:
        log(f"  SKIP — already have {existing:,} HUD records")
        return 0

    cur = conn.cursor()
    total = 0

    # HUD publishes grant data via their API
    # https://www.huduser.gov/portal/datasets/cpd.html
    # Try Socrata: data.hud.gov
    base_url = "https://data.hud.gov/resource"

    # Try known HUD Socrata datasets
    hud_datasets = [
        ("myah-tgkn", "CDBG Activity"),  # CDBG grantee activities
        ("iwny-j7bh", "HOME Production"),  # HOME investment partnerships
    ]

    for dataset_id, name in hud_datasets:
        url = f"https://data.hud.gov/resource/{dataset_id}.json"
        log(f"  Trying HUD {name} ({dataset_id})...")

        offset = 0
        dataset_total = 0

        try:
            count_resp = httpx.get(f"{url}?$select=count(*)", timeout=30)
            if count_resp.status_code == 200:
                available = int(count_resp.json()[0]["count"])
                log(f"    Available: {available:,}")
            else:
                log(f"    Dataset not found (HTTP {count_resp.status_code}), skipping")
                continue
        except Exception as e:
            log(f"    Count failed: {e}")
            continue

        while True:
            try:
                resp = httpx.get(f"{url}?$limit=50000&$offset={offset}&$order=:id", timeout=120)
                resp.raise_for_status()
                records = resp.json()
            except Exception as e:
                log(f"    Fetch error at offset {offset}: {e}")
                break

            if not records:
                break

            batch = []
            for r in records:
                proj_name = (r.get("activity_name") or r.get("project_name") or
                            r.get("grantee_name") or "")
                if not proj_name:
                    continue

                batch.append((
                    str(uuid.uuid4()),
                    (r.get("activity_id") or r.get("grant_number") or "")[:100] or None,
                    proj_name[:500],
                    (r.get("activity_description") or r.get("description") or "")[:1000] or None,
                    "HUD",
                    (r.get("grantee_state") or r.get("state") or "")[:2] or None,
                    (r.get("grantee_city") or r.get("city") or "")[:100] or None,
                    (r.get("county") or "")[:100] or None,
                    (r.get("zip") or "")[:10] or None,
                    safe_float(r.get("total_amount") or r.get("grant_amount") or r.get("amount")),
                    safe_date(r.get("start_date") or r.get("program_year")),
                    safe_date(r.get("end_date") or r.get("completion_date")),
                    (r.get("status") or "")[:50] or None,
                    (r.get("activity_type") or name)[:100] or None,
                    None, None,
                    "hud_cdbg",
                ))

            if batch:
                try:
                    execute_values(cur, """
                        INSERT INTO federal_projects (id, project_id, project_name, description,
                            agency, state, city, county, zip, amount, start_date, end_date,
                            status, project_type, lat, lng, source)
                        VALUES %s ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                    dataset_total += len(batch)
                    total += len(batch)
                    log(f"      Loaded {dataset_total:,}")
                except Exception as e:
                    log(f"      Insert error: {e}")
                    conn.rollback()

            offset += 50000
            if len(records) < 50000:
                break
            time.sleep(0.5)

        log(f"    {name}: {dataset_total:,} records")

    cur.close()
    return total


# =============================================================================
# 4. DOT/FHWA Highway Projects
# =============================================================================

def scrape_fhwa_projects(conn):
    """Download Federal Highway Administration project data."""
    log("=== FHWA Federal Highway Projects ===")

    existing = get_count(conn, "federal_projects", "fhwa")
    if existing > 10000:
        log(f"  SKIP — already have {existing:,} FHWA records")
        return 0

    cur = conn.cursor()
    total = 0

    # FHWA Fiscal Management Information System (FMIS)
    # Also: https://www.fhwa.dot.gov/bridge/nbi/ascii.cfm (bridge data)
    # Try USASpending for DOT contracts
    log("  Fetching DOT construction contracts from USASpending...")

    page = 1
    while page <= 50:
        try:
            payload = {
                "filters": {
                    "agencies": [{"type": "awarding", "tier": "toptier", "name": "Department of Transportation"}],
                    "time_period": [{"start_date": "2022-01-01", "end_date": "2026-12-31"}],
                    "award_type_codes": ["A", "B", "C", "D"],
                },
                "fields": [
                    "Award ID", "Recipient Name", "Award Amount",
                    "Start Date", "Description", "Awarding Sub Agency",
                    "Place of Performance State Code",
                    "Place of Performance City Name",
                    "NAICS Code", "NAICS Description",
                ],
                "limit": 100,
                "page": page,
                "sort": "Award Amount",
                "order": "desc",
            }

            resp = httpx.post(
                "https://api.usaspending.gov/api/v2/search/spending_by_award/",
                json=payload, timeout=60,
            )
            data = resp.json()
            results = data.get("results", [])

            if not results:
                break

            batch = []
            for r in results:
                batch.append((
                    str(uuid.uuid4()),
                    (r.get("Award ID") or "")[:100] or None,
                    (r.get("Recipient Name") or "")[:500] or None,
                    (r.get("Description") or "")[:1000] or None,
                    "DOT/" + (r.get("Awarding Sub Agency") or "FHWA")[:50],
                    (r.get("Place of Performance State Code") or "")[:2] or None,
                    (r.get("Place of Performance City Name") or "")[:100] or None,
                    None, None,
                    safe_float(r.get("Award Amount")),
                    safe_date(r.get("Start Date")),
                    None, None,
                    (r.get("NAICS Description") or "Highway")[:100],
                    None, None,
                    "fhwa",
                ))

            if batch:
                execute_values(cur, """
                    INSERT INTO federal_projects (id, project_id, project_name, description,
                        agency, state, city, county, zip, amount, start_date, end_date,
                        status, project_type, lat, lng, source)
                    VALUES %s ON CONFLICT DO NOTHING
                """, batch)
                conn.commit()
                total += len(batch)
                log(f"    Page {page}: {len(batch)} DOT contracts (total: {total:,})")

            if len(results) < 100:
                break
            page += 1

        except Exception as e:
            log(f"    Page {page} error: {e}")
            break

        time.sleep(1)

    cur.close()
    return total


# =============================================================================
# MASTER RUNNER
# =============================================================================

ALL_SCRAPERS = [
    ("BLS QCEW Construction Employment", scrape_bls_qcew),
    ("USASpending Construction Contracts", scrape_usaspending),
    ("HUD Community Development Grants", scrape_hud_grants),
    ("FHWA Highway Projects", scrape_fhwa_projects),
]


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="Federal Intelligence Scraper")
    parser.add_argument("--db-host", default=DB_HOST)
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to PostgreSQL at {DB_HOST}")
    ensure_tables(conn)

    grand_total = 0
    results = []

    for name, func in ALL_SCRAPERS:
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
    log(f"FEDERAL INTELLIGENCE SCRAPER COMPLETE")
    log(f"{'='*60}")
    log(f"Grand total: {grand_total:,} records")
    for name, count, status in results:
        log(f"  {name}: {count:,} ({status})")


if __name__ == "__main__":
    main()
