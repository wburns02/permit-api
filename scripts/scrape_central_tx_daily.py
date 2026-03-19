#!/usr/bin/env python3
"""
Central Texas Daily Hot Leads Scraper

Pulls FRESH permits (last 7 days) from Austin + surrounding jurisdictions.
Designed to run daily via cron to provide hot leads to trades.

Austin has the richest data: contractor name, phone, address, company, valuation.
This is the gold mine for roofers, HVAC, plumbers, electricians, and solar.

Sources:
1. Austin Issued Construction Permits (Socrata) — ~150/day, with contractor details
2. Austin 30-day permit feed (separate dataset) — pre-filtered for recency
3. TDLR Statewide Licenses — contractor contact info

Usage:
    python3 scrape_central_tx_daily.py --db-host 100.122.216.15
    python3 scrape_central_tx_daily.py --db-host 100.122.216.15 --days 30

Cron (daily 5 AM):
    0 5 * * * python3 -u /home/will/scrape_central_tx_daily.py --db-host 100.122.216.15 >> /tmp/central_tx_daily.log 2>&1
"""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import date, datetime, timedelta

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
BATCH_SIZE = 2000

# Central TX ZIP codes (Hays, Travis, Williamson, Bastrop, Caldwell, Comal)
CENTRAL_TX_ZIPS = [
    # Travis County (Austin)
    "78701", "78702", "78703", "78704", "78705", "78712", "78717", "78719",
    "78721", "78722", "78723", "78724", "78725", "78726", "78727", "78728",
    "78729", "78730", "78731", "78732", "78733", "78734", "78735", "78736",
    "78737", "78738", "78739", "78741", "78742", "78744", "78745", "78746",
    "78747", "78748", "78749", "78750", "78751", "78752", "78753", "78754",
    "78756", "78757", "78758", "78759",
    # Williamson County
    "78613", "78626", "78628", "78633", "78634", "78641", "78660", "78664",
    "78665", "78681", "78717", "78728", "78729",
    # Hays County
    "78610", "78620", "78640", "78666", "78676",
    # Bastrop County
    "78602", "78612", "78621", "78650", "78653", "78659", "78662",
    # Caldwell County
    "78644", "78648", "78655", "78661",
    # Comal County (New Braunfels area)
    "78130", "78132", "78133", "78163",
]


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def safe_float(v):
    if v in (None, "", "NA"):
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def safe_date(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace("T00:00:00.000", "")).date()
    except Exception:
        return None


def ensure_hot_leads_table(conn):
    """Create a dedicated hot_leads table for fresh, enriched permits."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hot_leads (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            permit_number TEXT,
            permit_type TEXT,
            work_class TEXT,
            description TEXT,
            address TEXT,
            city TEXT,
            state VARCHAR(2) NOT NULL DEFAULT 'TX',
            zip TEXT,
            county TEXT,
            lat FLOAT,
            lng FLOAT,
            issue_date DATE,
            applied_date DATE,
            status TEXT,
            valuation FLOAT,
            sqft FLOAT,
            housing_units INTEGER,
            contractor_company TEXT,
            contractor_name TEXT,
            contractor_phone TEXT,
            contractor_address TEXT,
            contractor_city TEXT,
            contractor_zip TEXT,
            contractor_trade TEXT,
            applicant_name TEXT,
            applicant_org TEXT,
            applicant_phone TEXT,
            owner_name TEXT,
            jurisdiction TEXT,
            source TEXT NOT NULL,
            scraped_at DATE DEFAULT CURRENT_DATE
        )
    """)
    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_hot_leads_zip ON hot_leads (zip)",
        "CREATE INDEX IF NOT EXISTS ix_hot_leads_date ON hot_leads (issue_date DESC)",
        "CREATE INDEX IF NOT EXISTS ix_hot_leads_type ON hot_leads (permit_type, work_class)",
        "CREATE INDEX IF NOT EXISTS ix_hot_leads_contractor ON hot_leads (contractor_company)",
        "CREATE INDEX IF NOT EXISTS ix_hot_leads_trade ON hot_leads (contractor_trade)",
        "CREATE INDEX IF NOT EXISTS ix_hot_leads_city ON hot_leads (city, state)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_hot_leads_permit ON hot_leads (permit_number, source)",
    ]:
        try:
            cur.execute(idx)
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()


def scrape_austin_permits(conn, days=7):
    """Pull fresh Austin permits with full contractor details."""
    log(f"=== Austin Fresh Permits (last {days} days) ===")

    since = (date.today() - timedelta(days=days)).isoformat()
    base_url = "https://data.austintexas.gov/resource/3syk-w9eu.json"

    cur = conn.cursor()
    total = 0
    offset = 0

    # Count available
    try:
        r = httpx.get(f"{base_url}?$select=count(*)&$where=issue_date>='{since}'", timeout=30)
        available = int(r.json()[0]["count"])
        log(f"  Fresh permits since {since}: {available:,}")
    except Exception as e:
        log(f"  Count error: {e}")
        available = None

    while True:
        url = (f"{base_url}?$where=issue_date>='{since}'"
               f"&$order=issue_date DESC&$limit=5000&$offset={offset}")
        log(f"  Fetching offset {offset}...")

        try:
            resp = httpx.get(url, timeout=60)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            log(f"  Error: {e}")
            break

        if not records:
            break

        batch = []
        for r in records:
            batch.append((
                str(uuid.uuid4()),
                (r.get("permit_number") or "")[:100] or None,
                (r.get("permittype") or "")[:20] or None,
                (r.get("work_class") or "")[:50] or None,
                (r.get("description") or "")[:1000] or None,
                (r.get("original_address1") or "")[:500] or None,
                (r.get("original_city") or "Austin")[:100],
                "TX",
                (r.get("original_zip") or "")[:10] or None,
                None,  # county
                safe_float(r.get("latitude")),
                safe_float(r.get("longitude")),
                safe_date(r.get("issue_date")),
                safe_date(r.get("applieddate")),
                (r.get("status_current") or "")[:50] or None,
                safe_float(r.get("total_job_valuation")),
                safe_float(r.get("total_new_add_sqft")),
                int(float(r.get("housing_units") or 0)) or None,
                (r.get("contractor_company_name") or "")[:500] or None,
                (r.get("contractor_full_name") or "")[:500] or None,
                (r.get("contractor_phone") or "")[:20] or None,
                (r.get("contractor_address1") or "")[:500] or None,
                (r.get("contractor_city") or "")[:100] or None,
                (r.get("contractor_zip") or "")[:10] or None,
                (r.get("contractor_trade") or "")[:100] or None,
                (r.get("applicant_full_name") or "")[:500] or None,
                (r.get("applicant_org") or "")[:500] or None,
                (r.get("applicant_phone") or "")[:20] or None,
                None,  # owner_name
                (r.get("jurisdiction") or "Austin")[:100],
                "austin_socrata_daily",
                date.today(),
            ))

        if batch:
            try:
                execute_values(cur, """
                    INSERT INTO hot_leads (id, permit_number, permit_type, work_class,
                        description, address, city, state, zip, county, lat, lng,
                        issue_date, applied_date, status, valuation, sqft, housing_units,
                        contractor_company, contractor_name, contractor_phone,
                        contractor_address, contractor_city, contractor_zip, contractor_trade,
                        applicant_name, applicant_org, applicant_phone, owner_name,
                        jurisdiction, source, scraped_at)
                    VALUES %s ON CONFLICT (permit_number, source) DO NOTHING
                """, batch)
                conn.commit()
                total += len(batch)
                log(f"    Loaded {total:,}")
            except Exception as e:
                log(f"    Insert error: {e}")
                conn.rollback()

        offset += 5000
        if len(records) < 5000:
            break
        time.sleep(0.5)

    cur.close()
    log(f"  Austin: {total:,} fresh permits loaded")
    return total


def scrape_tdlr_central_tx(conn):
    """Pull TDLR licensed contractors in Central TX ZIPs."""
    log("=== TDLR Central TX Contractors ===")

    # TDLR on data.texas.gov: 7358-krk7
    base_url = "https://data.texas.gov/resource/7358-krk7.json"

    cur = conn.cursor()
    total = 0

    # Fetch contractors in Central TX ZIPs in batches
    zip_batches = [CENTRAL_TX_ZIPS[i:i+10] for i in range(0, len(CENTRAL_TX_ZIPS), 10)]

    for zip_batch in zip_batches:
        zip_filter = " OR ".join(f"zip='{z}'" for z in zip_batch)
        url = f"{base_url}?$where=({zip_filter})&$limit=50000"
        log(f"  Fetching ZIPs: {', '.join(zip_batch[:3])}...")

        try:
            resp = httpx.get(url, timeout=60)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            log(f"  Error: {e}")
            continue

        if not records:
            continue

        batch = []
        for r in records:
            name = r.get("name") or ""
            if not name:
                continue
            batch.append((
                str(uuid.uuid4()),
                (r.get("license_number") or "")[:100] or None,
                name[:500],
                None,  # business_name
                (r.get("license_type") or "")[:100] or None,
                None,
                (r.get("address") or "")[:500] or None,
                (r.get("city") or "")[:100] or None,
                "TX",
                (r.get("zip") or "")[:10] or None,
                (r.get("phone") or "")[:20] or None,
                (r.get("email") or "")[:200] or None,
                (r.get("license_status") or "")[:50] or None,
                safe_date(r.get("original_issue_date")),
                safe_date(r.get("license_expiration_date")),
                "tdlr_central_tx",
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
                total += len(batch)
                log(f"    Loaded {total:,} TDLR licenses")
            except Exception as e:
                log(f"    Insert error: {e}")
                conn.rollback()

        time.sleep(0.5)

    cur.close()
    log(f"  TDLR Central TX: {total:,} contractor licenses")
    return total


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="Central TX Daily Hot Leads")
    parser.add_argument("--db-host", default=DB_HOST)
    parser.add_argument("--days", type=int, default=7, help="Days of history to pull")
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to PostgreSQL at {DB_HOST}")
    ensure_hot_leads_table(conn)

    total = 0

    # 1. Fresh Austin permits
    total += scrape_austin_permits(conn, days=args.days)

    # 2. TDLR contractor licenses for Central TX
    total += scrape_tdlr_central_tx(conn)

    conn.close()
    log(f"\nTotal Central TX records: {total:,}")
    log(f"Done — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
