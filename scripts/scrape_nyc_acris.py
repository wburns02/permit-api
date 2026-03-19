#!/usr/bin/env python3
"""
NYC ACRIS (Automated City Register Information System) scraper via Socrata Open Data API.

Dataset: data.cityofnewyork.us — ACRIS Real Property Master (resource: bnx9-e6tj)
Total records: ~16.9M

Supports:
- Deeds: doc_type in ('DEED','DEED, TS') → property_sales table
- Liens: doc_type in ('TL&R','AL&R','RTXL','DTL') → property_liens table

Usage:
    python scrape_nyc_acris.py --type deeds
    python scrape_nyc_acris.py --type liens
    python scrape_nyc_acris.py --type all
    python scrape_nyc_acris.py --type all --db-host 100.122.216.15

Requires: pip install httpx psycopg2-binary
"""

import argparse
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
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "permits")
DB_USER = os.getenv("DB_USER", "will")

BATCH_SIZE = 5000
PAGE_SIZE = 50000  # Socrata max per request
DELAY = 0.5

BASE_URL = "https://data.cityofnewyork.us/resource/bnx9-e6tj.json"

# Borough code to name mapping
BOROUGH_MAP = {
    "1": "Manhattan",
    "2": "Bronx",
    "3": "Brooklyn",
    "4": "Queens",
    "5": "Staten Island",
}

# ACRIS doc_type to lien_type mapping
LIEN_TYPE_MAP = {
    "TL&R": "Tax Lien",
    "AL&R": "Assignment of Lien",
    "RTXL": "Return of Tax Lien",
    "DTL": "Discharge of Tax Lien",
}

# Socrata $where filters (& must be URL-encoded as %26 in doc_type values)
DEED_WHERE = "doc_type in('DEED','DEED, TS')"
LIEN_WHERE = "doc_type in('TL%26R','AL%26R','RTXL','DTL')"


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER
    )


def ensure_sales_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS property_sales (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            document_id VARCHAR(100),
            address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(2) NOT NULL,
            zip VARCHAR(10),
            borough VARCHAR(50),
            sale_price FLOAT,
            sale_date DATE,
            recorded_date DATE,
            doc_type VARCHAR(50),
            grantor VARCHAR(500),
            grantee VARCHAR(500),
            property_type VARCHAR(100),
            building_class VARCHAR(50),
            residential_units INTEGER,
            land_sqft FLOAT,
            gross_sqft FLOAT,
            lat FLOAT,
            lng FLOAT,
            source VARCHAR(50) NOT NULL
        )
    """)
    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_sales_doc_id ON property_sales (document_id)",
        "CREATE INDEX IF NOT EXISTS ix_sales_address ON property_sales (address)",
        "CREATE INDEX IF NOT EXISTS ix_sales_city ON property_sales (city)",
        "CREATE INDEX IF NOT EXISTS ix_sales_state ON property_sales (state)",
        "CREATE INDEX IF NOT EXISTS ix_sales_zip ON property_sales (zip)",
        "CREATE INDEX IF NOT EXISTS ix_sales_state_city ON property_sales (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_sales_zip_date ON property_sales (zip, sale_date)",
        "CREATE INDEX IF NOT EXISTS ix_sales_sale_date ON property_sales (sale_date)",
        "CREATE INDEX IF NOT EXISTS ix_sales_grantor ON property_sales (grantor)",
        "CREATE INDEX IF NOT EXISTS ix_sales_grantee ON property_sales (grantee)",
    ]:
        try:
            cur.execute(idx)
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()


def ensure_liens_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS property_liens (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            document_id VARCHAR(100),
            lien_type VARCHAR(100),
            filing_number VARCHAR(100),
            address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(2) NOT NULL,
            zip VARCHAR(10),
            borough VARCHAR(50),
            amount FLOAT,
            filing_date DATE,
            lapse_date DATE,
            status VARCHAR(50),
            debtor_name VARCHAR(500),
            creditor_name VARCHAR(500),
            description TEXT,
            source VARCHAR(50) NOT NULL
        )
    """)
    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_liens_doc_id ON property_liens (document_id)",
        "CREATE INDEX IF NOT EXISTS ix_liens_address ON property_liens (address)",
        "CREATE INDEX IF NOT EXISTS ix_liens_lien_type ON property_liens (lien_type)",
        "CREATE INDEX IF NOT EXISTS ix_liens_filing_number ON property_liens (filing_number)",
        "CREATE INDEX IF NOT EXISTS ix_liens_state ON property_liens (state)",
        "CREATE INDEX IF NOT EXISTS ix_liens_state_type ON property_liens (state, lien_type)",
        "CREATE INDEX IF NOT EXISTS ix_liens_filing_date ON property_liens (filing_date)",
        "CREATE INDEX IF NOT EXISTS ix_liens_debtor ON property_liens (debtor_name)",
        "CREATE INDEX IF NOT EXISTS ix_liens_filing_state ON property_liens (filing_number, state)",
        "CREATE INDEX IF NOT EXISTS ix_liens_zip ON property_liens (zip)",
    ]:
        try:
            cur.execute(idx)
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()


def safe_date(val):
    """Parse Socrata date strings into Python date objects."""
    if not val:
        return None
    try:
        # Socrata floating timestamp: 2025-06-16T00:00:00.000
        return datetime.fromisoformat(val.replace("T00:00:00.000", "")).date()
    except (ValueError, AttributeError):
        pass
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except (ValueError, AttributeError):
        return None


def safe_float(val):
    """Parse a value to float, returning None on failure."""
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def get_count(where_clause=None):
    """Get total record count from ACRIS Socrata endpoint."""
    url = f"{BASE_URL}?$select=count(*)"
    if where_clause:
        url += f"&$where={where_clause}"
    try:
        resp = httpx.get(url, timeout=30)
        return int(resp.json()[0]["count"])
    except Exception as e:
        print(f"  Could not get count: {e}")
        return None


def scrape_deeds(conn):
    """Scrape NYC ACRIS deed records into property_sales table."""
    cur = conn.cursor()
    source = "nyc_acris_deeds"

    total_count = get_count(DEED_WHERE)
    if total_count:
        print(f"  Total deed records available: {total_count:,}")

    total = 0
    offset = 0

    while True:
        url = (
            f"{BASE_URL}?$where={DEED_WHERE}"
            f"&$limit={PAGE_SIZE}&$offset={offset}&$order=:id"
        )
        print(f"  Fetching offset {offset:,}...")

        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            print(f"  Error at offset {offset}: {e}")
            break

        if not records:
            break

        batch = []
        for r in records:
            doc_id = r.get("document_id") or r.get("crfn") or ""
            borough_code = str(r.get("borough", ""))
            borough = BOROUGH_MAP.get(borough_code, borough_code)

            batch.append((
                str(uuid.uuid4()),
                str(doc_id)[:100] or None,                        # document_id
                None,                                              # address (not in ACRIS master)
                "New York",                                        # city
                "NY",                                              # state
                None,                                              # zip
                borough[:50] if borough else None,                 # borough
                safe_float(r.get("document_amt")),                 # sale_price
                safe_date(r.get("document_date")),                 # sale_date
                safe_date(r.get("recorded_datetime")),             # recorded_date
                (r.get("doc_type") or "")[:50] or None,           # doc_type
                None,                                              # grantor
                None,                                              # grantee
                None,                                              # property_type
                None,                                              # building_class
                None,                                              # residential_units
                None,                                              # land_sqft
                None,                                              # gross_sqft
                None,                                              # lat
                None,                                              # lng
                source,                                            # source
            ))

        if batch:
            for i in range(0, len(batch), BATCH_SIZE):
                sub = batch[i:i + BATCH_SIZE]
                execute_values(cur, """
                    INSERT INTO property_sales (
                        id, document_id, address, city, state, zip, borough,
                        sale_price, sale_date, recorded_date, doc_type,
                        grantor, grantee, property_type, building_class,
                        residential_units, land_sqft, gross_sqft, lat, lng, source
                    ) VALUES %s ON CONFLICT DO NOTHING
                """, sub)
            conn.commit()
            total += len(batch)
            pct = f" ({total * 100 // total_count}%)" if total_count else ""
            print(f"    Loaded {total:,}{pct}")

        offset += PAGE_SIZE
        if len(records) < PAGE_SIZE:
            break
        time.sleep(DELAY)

    cur.close()
    return total


def scrape_liens(conn):
    """Scrape NYC ACRIS lien records into property_liens table."""
    cur = conn.cursor()
    source = "nyc_acris_liens"

    total_count = get_count(LIEN_WHERE)
    if total_count:
        print(f"  Total lien records available: {total_count:,}")

    total = 0
    offset = 0

    while True:
        url = (
            f"{BASE_URL}?$where={LIEN_WHERE}"
            f"&$limit={PAGE_SIZE}&$offset={offset}&$order=:id"
        )
        print(f"  Fetching offset {offset:,}...")

        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            print(f"  Error at offset {offset}: {e}")
            break

        if not records:
            break

        batch = []
        for r in records:
            doc_type_raw = r.get("doc_type", "")
            lien_type = LIEN_TYPE_MAP.get(doc_type_raw, doc_type_raw)
            borough_code = str(r.get("borough", ""))
            borough = BOROUGH_MAP.get(borough_code, borough_code)

            batch.append((
                str(uuid.uuid4()),
                str(r.get("document_id", ""))[:100] or None,       # document_id
                lien_type[:100] if lien_type else None,              # lien_type
                None,                                                 # filing_number
                None,                                                 # address
                "New York",                                           # city
                "NY",                                                 # state
                None,                                                 # zip
                borough[:50] if borough else None,                   # borough
                safe_float(r.get("document_amt")),                   # amount
                safe_date(r.get("document_date")),                   # filing_date
                None,                                                 # lapse_date
                None,                                                 # status
                None,                                                 # debtor_name
                None,                                                 # creditor_name
                f"{doc_type_raw} recorded {r.get('recorded_datetime', '')[:10]}"
                    if r.get("recorded_datetime") else None,         # description
                source,                                               # source
            ))

        if batch:
            for i in range(0, len(batch), BATCH_SIZE):
                sub = batch[i:i + BATCH_SIZE]
                execute_values(cur, """
                    INSERT INTO property_liens (id, document_id, lien_type, filing_number,
                        address, city, state, zip, borough, amount, filing_date, lapse_date,
                        status, debtor_name, creditor_name, description, source)
                    VALUES %s ON CONFLICT DO NOTHING
                """, sub)
            conn.commit()
            total += len(batch)
            pct = f" ({total * 100 // total_count}%)" if total_count else ""
            print(f"    Loaded {total:,}{pct}")

        offset += PAGE_SIZE
        if len(records) < PAGE_SIZE:
            break
        time.sleep(DELAY)

    cur.close()
    return total


def main():
    global DB_HOST

    parser = argparse.ArgumentParser(
        description="Scrape NYC ACRIS deed and lien records via Socrata API"
    )
    parser.add_argument(
        "--type",
        default="all",
        choices=["deeds", "liens", "all"],
        help="Record type to scrape: deeds, liens, or all",
    )
    parser.add_argument("--db-host", default=DB_HOST)
    args = parser.parse_args()

    DB_HOST = args.db_host

    conn = get_conn()

    scrape_types = []
    if args.type in ("deeds", "all"):
        scrape_types.append("deeds")
        ensure_sales_table(conn)
    if args.type in ("liens", "all"):
        scrape_types.append("liens")
        ensure_liens_table(conn)

    grand_total = 0

    if "deeds" in scrape_types:
        print("\n=== Scraping NYC ACRIS Deeds → property_sales ===")
        try:
            count = scrape_deeds(conn)
            grand_total += count
            print(f"  Deeds: {count:,} records loaded into property_sales")
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()

    if "liens" in scrape_types:
        print("\n=== Scraping NYC ACRIS Liens → property_liens ===")
        try:
            count = scrape_liens(conn)
            grand_total += count
            print(f"  Liens: {count:,} records loaded into property_liens")
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()

    conn.close()
    print(f"\n{'=' * 50}")
    print(f"Grand total: {grand_total:,} ACRIS records scraped ({', '.join(scrape_types)})")


if __name__ == "__main__":
    main()
