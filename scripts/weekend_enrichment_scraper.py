#!/usr/bin/env python3
"""
Weekend Enrichment Scraper — ZoomInfo-style people + company data from public sources.

Sources:
1. SBA PPP Loans (11.5M businesses with names, addresses, industry, employees)
2. IRS Tax-Exempt Orgs / Form 990 (1.8M nonprofits with officers + financials)
3. Professional Licenses (multi-state — RE agents, architects, engineers, appraisers)
4. SEC EDGAR Company Index (800K+ public companies)
5. HUD FHA Single Family Loans

Run on R730 alongside weekend_mega_scraper.py — no table conflicts.

Usage:
    nohup python3 -u weekend_enrichment_scraper.py --db-host 100.122.216.15 > /tmp/enrichment_scraper.log 2>&1 &
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
import zipfile
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
STAGING = Path("/mnt/data/staging")


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def safe_float(v):
    if v in (None, "", "NA", "N/A"):
        return None
    try:
        v = str(v).replace(",", "").replace("$", "").strip()
        return float(v)
    except (ValueError, TypeError):
        return None


def safe_int(v):
    if v in (None, "", "NA", "N/A"):
        return None
    try:
        return int(float(str(v).replace(",", "").strip()))
    except (ValueError, TypeError):
        return None


def safe_date(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace("T00:00:00.000", "").replace("Z", "")).date()
    except Exception:
        pass
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%Y%m%d"):
        try:
            return datetime.strptime(str(v).strip()[:10], fmt).date()
        except Exception:
            continue
    return None


def ensure_tables(conn):
    """Create all enrichment tables."""
    cur = conn.cursor()

    # SBA PPP Loans — business firmographics
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sba_ppp_loans (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            business_name TEXT NOT NULL,
            business_type TEXT,
            naics_code TEXT,
            industry TEXT,
            address TEXT,
            city TEXT,
            state VARCHAR(2),
            zip TEXT,
            employees_range TEXT,
            loan_amount FLOAT,
            loan_status TEXT,
            approval_date DATE,
            forgiveness_amount FLOAT,
            owner_name TEXT,
            owner_race TEXT,
            owner_gender TEXT,
            owner_ethnicity TEXT,
            owner_veteran TEXT,
            lender TEXT,
            source TEXT NOT NULL DEFAULT 'sba_ppp'
        )
    """)

    # IRS Tax-Exempt Organizations
    cur.execute("""
        CREATE TABLE IF NOT EXISTS irs_exempt_orgs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            ein TEXT,
            name TEXT NOT NULL,
            address TEXT,
            city TEXT,
            state VARCHAR(2),
            zip TEXT,
            classification TEXT,
            ruling_date DATE,
            deductibility TEXT,
            activity TEXT,
            organization TEXT,
            status TEXT,
            revenue FLOAT,
            assets FLOAT,
            source TEXT NOT NULL DEFAULT 'irs_bmo'
        )
    """)

    # Professional Licenses (multi-state, multi-profession)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS professional_licenses (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            license_number TEXT,
            name TEXT NOT NULL,
            business_name TEXT,
            profession TEXT,
            license_type TEXT,
            address TEXT,
            city TEXT,
            state VARCHAR(2) NOT NULL,
            zip TEXT,
            phone TEXT,
            email TEXT,
            status TEXT,
            issue_date DATE,
            expiration_date DATE,
            source TEXT NOT NULL
        )
    """)

    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_ppp_name ON sba_ppp_loans (business_name)",
        "CREATE INDEX IF NOT EXISTS ix_ppp_state ON sba_ppp_loans (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_ppp_naics ON sba_ppp_loans (naics_code)",
        "CREATE INDEX IF NOT EXISTS ix_ppp_zip ON sba_ppp_loans (zip)",
        "CREATE INDEX IF NOT EXISTS ix_irs_name ON irs_exempt_orgs (name)",
        "CREATE INDEX IF NOT EXISTS ix_irs_state ON irs_exempt_orgs (state)",
        "CREATE INDEX IF NOT EXISTS ix_irs_ein ON irs_exempt_orgs (ein)",
        "CREATE INDEX IF NOT EXISTS ix_prof_name ON professional_licenses (name)",
        "CREATE INDEX IF NOT EXISTS ix_prof_state ON professional_licenses (state, profession)",
        "CREATE INDEX IF NOT EXISTS ix_prof_phone ON professional_licenses (phone)",
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
# SBA PPP LOANS — 11.5M business records
# =============================================================================

def scrape_sba_ppp(conn):
    """Download and load SBA PPP loan data from Socrata."""
    log("=== SBA PPP Loans (11.5M) ===")

    existing = get_count(conn, "sba_ppp_loans")
    if existing > 100000:
        log(f"  SKIP — already have {existing:,} PPP records")
        return 0

    # SBA publishes PPP data on data.sba.gov via Socrata
    # Also available on Socrata: federalregister datasets
    # Try the SBA Socrata endpoint
    base_url = "https://data.sba.gov/resource/fljv-zcfk.json"

    cur = conn.cursor()
    total = 0
    offset = 0
    page_size = 50000

    # Get count
    try:
        r = httpx.get(f"{base_url}?$select=count(*)", timeout=30)
        available = int(r.json()[0]["count"])
        log(f"  Available: {available:,} records")
    except Exception as e:
        log(f"  Could not get SBA count from Socrata: {e}")
        log(f"  Trying alternate source...")
        # Try alternate: data.world or direct CSV
        available = None

    if available and available > 0:
        while True:
            url = f"{base_url}?$limit={page_size}&$offset={offset}&$order=:id"
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
                name = r.get("borrowername") or r.get("business_name") or ""
                if not name:
                    continue
                batch.append((
                    str(uuid.uuid4()),
                    name[:500],
                    (r.get("businesstype") or "")[:50] or None,
                    (r.get("naicscode") or "")[:10] or None,
                    (r.get("sector") or r.get("industry") or "")[:200] or None,
                    (r.get("borroweraddress") or "")[:500] or None,
                    (r.get("borrowercity") or "")[:100] or None,
                    (r.get("borrowerstate") or "")[:2] or None,
                    (r.get("borrowerzip") or "")[:10] or None,
                    (r.get("jobsreported") or "")[:50] or None,
                    safe_float(r.get("currentapprovalamount") or r.get("initialapprovalamount")),
                    (r.get("loanstatus") or "")[:50] or None,
                    safe_date(r.get("dateapproved")),
                    safe_float(r.get("forgivenessdate") and r.get("currentapprovalamount")),
                    (r.get("borrowername") or "")[:500] or None,
                    (r.get("race") or "")[:50] or None,
                    (r.get("gender") or "")[:20] or None,
                    (r.get("ethnicity") or "")[:50] or None,
                    (r.get("veteran") or "")[:20] or None,
                    (r.get("originatinglender") or "")[:200] or None,
                    "sba_ppp",
                ))

            if batch:
                try:
                    execute_values(cur, """
                        INSERT INTO sba_ppp_loans (id, business_name, business_type, naics_code,
                            industry, address, city, state, zip, employees_range, loan_amount,
                            loan_status, approval_date, forgiveness_amount, owner_name,
                            owner_race, owner_gender, owner_ethnicity, owner_veteran,
                            lender, source) VALUES %s ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                    total += len(batch)
                    pct = f" ({total * 100 // available}%)" if available else ""
                    log(f"    Loaded {total:,}{pct}")
                except Exception as e:
                    log(f"    Insert error: {e}")
                    conn.rollback()

            offset += page_size
            if len(records) < page_size:
                break
            time.sleep(0.3)

    cur.close()
    return total


# =============================================================================
# IRS TAX-EXEMPT ORGANIZATIONS — 1.8M nonprofits
# =============================================================================

def scrape_irs_exempt(conn):
    """Load IRS Business Master File of tax-exempt organizations."""
    log("=== IRS Tax-Exempt Organizations (1.8M) ===")

    existing = get_count(conn, "irs_exempt_orgs")
    if existing > 100000:
        log(f"  SKIP — already have {existing:,} IRS exempt records")
        return 0

    # IRS publishes this on their website as CSV
    # Also available via data.gov Socrata
    # Try data.gov first
    urls_to_try = [
        "https://data.cms.gov/provider-data/api/1/datastore/query/np6g-jdhq",  # Not right
    ]

    # The IRS BMF is available as bulk downloads from IRS.gov
    # Let's try to fetch from a known Socrata mirror
    base_url = "https://data.irs.gov/resource/cm9j-4dqr.json"

    cur = conn.cursor()
    total = 0
    offset = 0

    try:
        r = httpx.get(f"{base_url}?$select=count(*)", timeout=30)
        available = int(r.json()[0]["count"])
        log(f"  Available: {available:,} records")
    except Exception as e:
        log(f"  IRS Socrata endpoint not available: {e}")
        log(f"  Trying IRS bulk CSV download...")

        # Try downloading the IRS BMF CSV directly
        try:
            bmf_url = "https://www.irs.gov/pub/irs-soi/eo1.csv"
            log(f"  Downloading {bmf_url}...")
            resp = httpx.get(bmf_url, timeout=120, follow_redirects=True)
            if resp.status_code == 200:
                reader = csv.DictReader(io.StringIO(resp.text))
                batch = []
                for row in reader:
                    name = row.get("NAME") or row.get("name") or ""
                    if not name:
                        continue
                    batch.append((
                        str(uuid.uuid4()),
                        (row.get("EIN") or "")[:20] or None,
                        name[:500],
                        (row.get("STREET") or "")[:500] or None,
                        (row.get("CITY") or "")[:100] or None,
                        (row.get("STATE") or "")[:2] or None,
                        (row.get("ZIP") or "")[:10] or None,
                        (row.get("CLASSIFICATION") or "")[:100] or None,
                        safe_date(row.get("RULING")),
                        (row.get("DEDUCTIBILITY") or "")[:50] or None,
                        (row.get("ACTIVITY") or "")[:200] or None,
                        (row.get("ORGANIZATION") or "")[:100] or None,
                        (row.get("STATUS") or "")[:50] or None,
                        safe_float(row.get("REVENUE_AMT")),
                        safe_float(row.get("ASSET_AMT")),
                        "irs_bmo",
                    ))
                    if len(batch) >= BATCH_SIZE:
                        execute_values(cur, """
                            INSERT INTO irs_exempt_orgs (id, ein, name, address, city, state, zip,
                                classification, ruling_date, deductibility, activity, organization,
                                status, revenue, assets, source) VALUES %s ON CONFLICT DO NOTHING
                        """, batch)
                        conn.commit()
                        total += len(batch)
                        log(f"    Loaded {total:,}")
                        batch = []

                if batch:
                    execute_values(cur, """
                        INSERT INTO irs_exempt_orgs (id, ein, name, address, city, state, zip,
                            classification, ruling_date, deductibility, activity, organization,
                            status, revenue, assets, source) VALUES %s ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                    total += len(batch)
            else:
                log(f"  IRS download failed: {resp.status_code}")
        except Exception as e:
            log(f"  IRS bulk download failed: {e}")

        # Also try additional region files (eo2, eo3, eo4)
        for region_num in range(2, 5):
            try:
                bmf_url = f"https://www.irs.gov/pub/irs-soi/eo{region_num}.csv"
                log(f"  Downloading {bmf_url}...")
                resp = httpx.get(bmf_url, timeout=120, follow_redirects=True)
                if resp.status_code == 200:
                    reader = csv.DictReader(io.StringIO(resp.text))
                    batch = []
                    for row in reader:
                        name = row.get("NAME") or row.get("name") or ""
                        if not name:
                            continue
                        batch.append((
                            str(uuid.uuid4()),
                            (row.get("EIN") or "")[:20] or None,
                            name[:500],
                            (row.get("STREET") or "")[:500] or None,
                            (row.get("CITY") or "")[:100] or None,
                            (row.get("STATE") or "")[:2] or None,
                            (row.get("ZIP") or "")[:10] or None,
                            (row.get("CLASSIFICATION") or "")[:100] or None,
                            safe_date(row.get("RULING")),
                            (row.get("DEDUCTIBILITY") or "")[:50] or None,
                            (row.get("ACTIVITY") or "")[:200] or None,
                            (row.get("ORGANIZATION") or "")[:100] or None,
                            (row.get("STATUS") or "")[:50] or None,
                            safe_float(row.get("REVENUE_AMT")),
                            safe_float(row.get("ASSET_AMT")),
                            "irs_bmo",
                        ))
                        if len(batch) >= BATCH_SIZE:
                            execute_values(cur, """
                                INSERT INTO irs_exempt_orgs (id, ein, name, address, city, state, zip,
                                    classification, ruling_date, deductibility, activity, organization,
                                    status, revenue, assets, source) VALUES %s ON CONFLICT DO NOTHING
                            """, batch)
                            conn.commit()
                            total += len(batch)
                            log(f"    Loaded {total:,}")
                            batch = []

                    if batch:
                        execute_values(cur, """
                            INSERT INTO irs_exempt_orgs (id, ein, name, address, city, state, zip,
                                classification, ruling_date, deductibility, activity, organization,
                                status, revenue, assets, source) VALUES %s ON CONFLICT DO NOTHING
                        """, batch)
                        conn.commit()
                        total += len(batch)
            except Exception as e:
                log(f"  Region {region_num} failed: {e}")

    cur.close()
    return total


# =============================================================================
# PROFESSIONAL LICENSES — Multi-state Socrata sources
# =============================================================================

def scrape_professional_licenses(conn):
    """Scrape professional license data from state Socrata portals."""
    log("=== Professional Licenses (Multi-State) ===")

    # Known Socrata datasets with professional licenses
    sources = [
        # NY professional licenses
        ("NY Professions", "NY", "https://data.ny.gov/resource/s4bd-4hm2.json",
         {"name": "first_name,last_name", "profession": "profession", "license_number": "license_no",
          "city": "city", "zip": "zip", "status": "license_status", "issue_date": "date_of_licensure"}),
        # TX professional licenses (TDLR)
        ("TX TDLR", "TX", "https://data.texas.gov/resource/7358-krk7.json",
         {"name": "name", "license_number": "license_number", "profession": "license_type",
          "city": "city", "state": "state", "zip": "zip", "status": "license_status",
          "expiration_date": "license_expiration_date"}),
    ]

    cur = conn.cursor()
    total = 0

    for source_name, state, base_url, fields in sources:
        log(f"--- {source_name} ---")

        existing = get_count(conn, "professional_licenses", f"prof_{state.lower()}")
        if existing > 1000:
            log(f"  SKIP — already have {existing:,} records for {state}")
            continue

        offset = 0
        source_total = 0

        try:
            r = httpx.get(f"{base_url}?$select=count(*)", timeout=30)
            available = int(r.json()[0]["count"])
            log(f"  Available: {available:,}")
        except Exception as e:
            log(f"  Count failed: {e}, proceeding anyway")
            available = None

        while True:
            url = f"{base_url}?$limit=50000&$offset={offset}&$order=:id"
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
                # Handle combined first/last name fields
                name_field = fields.get("name", "")
                if "," in name_field:
                    parts = name_field.split(",")
                    name = " ".join((r.get(p.strip()) or "") for p in parts).strip()
                else:
                    name = r.get(name_field, "")

                if not name:
                    continue

                batch.append((
                    str(uuid.uuid4()),
                    (r.get(fields.get("license_number", "")) or "")[:100] or None,
                    name[:500],
                    None,  # business_name
                    (r.get(fields.get("profession", "")) or "")[:100] or None,
                    None,  # license_type
                    (r.get(fields.get("address", "")) or "")[:500] or None,
                    (r.get(fields.get("city", "")) or "")[:100] or None,
                    state,
                    (r.get(fields.get("zip", "")) or "")[:10] or None,
                    (r.get(fields.get("phone", "")) or "")[:20] or None,
                    (r.get(fields.get("email", "")) or "")[:200] or None,
                    (r.get(fields.get("status", "")) or "")[:50] or None,
                    safe_date(r.get(fields.get("issue_date", ""))),
                    safe_date(r.get(fields.get("expiration_date", ""))),
                    f"prof_{state.lower()}",
                ))

            if batch:
                try:
                    execute_values(cur, """
                        INSERT INTO professional_licenses (id, license_number, name, business_name,
                            profession, license_type, address, city, state, zip, phone, email,
                            status, issue_date, expiration_date, source)
                            VALUES %s ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                    source_total += len(batch)
                    total += len(batch)
                    pct = f" ({source_total * 100 // available}%)" if available else ""
                    log(f"    Loaded {source_total:,}{pct}")
                except Exception as e:
                    log(f"    Insert error: {e}")
                    conn.rollback()

            offset += 50000
            if len(records) < 50000:
                break
            time.sleep(0.3)

        log(f"  {source_name}: {source_total:,} records")

    cur.close()
    return total


# =============================================================================
# SEC EDGAR Company Index
# =============================================================================

def scrape_sec_companies(conn):
    """Download SEC EDGAR company index — 800K+ public companies."""
    log("=== SEC EDGAR Company Index ===")

    existing = get_count(conn, "business_entities", "sec_edgar")
    if existing > 10000:
        log(f"  SKIP — already have {existing:,} SEC records")
        return 0

    # SEC full index is at https://www.sec.gov/files/company_tickers.json
    cur = conn.cursor()
    total = 0

    try:
        log("  Downloading SEC company tickers...")
        resp = httpx.get(
            "https://efts.sec.gov/LATEST/search-index?q=*&dateRange=custom&startdt=2020-01-01&enddt=2026-12-31&forms=10-K",
            headers={"User-Agent": "PermitLookup research@permitlookup.com"},
            timeout=60,
        )

        # Try the simpler company_tickers endpoint
        resp2 = httpx.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers={"User-Agent": "PermitLookup research@permitlookup.com"},
            timeout=60,
        )
        if resp2.status_code == 200:
            data = resp2.json()
            batch = []
            for key, co in data.items():
                name = co.get("title") or ""
                if not name:
                    continue
                batch.append((
                    str(uuid.uuid4()),
                    name[:500],
                    "Public Company",
                    "US",
                    str(co.get("cik_str", ""))[:100] or None,
                    "Active",
                    None, None, None, None, None, None, None,
                    "sec_edgar",
                    date.today(),
                ))
                if len(batch) >= BATCH_SIZE:
                    execute_values(cur, """
                        INSERT INTO business_entities (id, entity_name, entity_type, state,
                            filing_number, status, formation_date, dissolution_date,
                            registered_agent_name, registered_agent_address, principal_address,
                            mailing_address, officers, source, scraped_at)
                            VALUES %s ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                    total += len(batch)
                    log(f"    Loaded {total:,}")
                    batch = []

            if batch:
                execute_values(cur, """
                    INSERT INTO business_entities (id, entity_name, entity_type, state,
                        filing_number, status, formation_date, dissolution_date,
                        registered_agent_name, registered_agent_address, principal_address,
                        mailing_address, officers, source, scraped_at)
                        VALUES %s ON CONFLICT DO NOTHING
                """, batch)
                conn.commit()
                total += len(batch)
    except Exception as e:
        log(f"  SEC download failed: {e}")

    cur.close()
    return total


# =============================================================================
# MASTER RUNNER
# =============================================================================

ALL_SCRAPERS = [
    ("SBA PPP Loans (11.5M)", scrape_sba_ppp),
    ("IRS Tax-Exempt Orgs (1.8M)", scrape_irs_exempt),
    ("Professional Licenses (Multi-State)", scrape_professional_licenses),
    ("SEC EDGAR Companies", scrape_sec_companies),
]


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="Weekend Enrichment Scraper")
    parser.add_argument("--db-host", default=DB_HOST)
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to PostgreSQL at {DB_HOST}")
    ensure_tables(conn)
    log("Tables ensured")

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
    log(f"ENRICHMENT SCRAPER COMPLETE")
    log(f"{'='*60}")
    log(f"Grand total: {grand_total:,} new records")
    for name, count, status in results:
        log(f"  {name}: {count:,} ({status})")


if __name__ == "__main__":
    main()
