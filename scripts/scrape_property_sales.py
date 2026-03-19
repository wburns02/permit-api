#!/usr/bin/env python3
"""
Multi-source property sale/deed transfer scraper via Socrata Open Data APIs.

Sources:
- NYC ACRIS deeds: data.cityofnewyork.us (~3.6M DEED records)
- Illinois PTAX-203: illinois-edp.data.socrata.com (~2.9M records)
- Connecticut Sales: data.ct.gov (~1.1M records)

Usage:
    python scrape_property_sales.py --source nyc_acris
    python scrape_property_sales.py --source il_ptax
    python scrape_property_sales.py --source ct_sales
    python scrape_property_sales.py --source all
    python scrape_property_sales.py --source all --db-host 100.122.216.15

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

# Borough code to name mapping (NYC ACRIS)
BOROUGH_MAP = {
    "1": "Manhattan",
    "2": "Bronx",
    "3": "Brooklyn",
    "4": "Queens",
    "5": "Staten Island",
}

# Source configurations
SOURCES = {
    "nyc_acris": {
        "base_url": "https://data.cityofnewyork.us/resource/bnx9-e6tj.json",
        "where_clause": "doc_type='DEED'",
        "state": "NY",
        "source_name": "nyc_acris",
        "description": "NYC ACRIS Deed Records",
    },
    "il_ptax": {
        "base_url": "https://illinois-edp.data.socrata.com/resource/vbnw-q5s8.json",
        "where_clause": None,
        "state": "IL",
        "source_name": "il_ptax203",
        "description": "Illinois PTAX-203 Transfer Declarations",
    },
    "ct_sales": {
        "base_url": "https://data.ct.gov/resource/5mzw-sjtu.json",
        "where_clause": None,
        "state": "CT",
        "source_name": "ct_sales",
        "description": "Connecticut Real Estate Sales",
    },
}


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER
    )


def ensure_table(conn):
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
    indexes = [
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
    ]
    for idx in indexes:
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
        pass
    try:
        # MM/DD/YYYY format
        return datetime.strptime(val[:10], "%m/%d/%Y").date()
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


def safe_int(val):
    """Parse a value to int, returning None on failure."""
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def get_total_count(base_url, where_clause=None):
    """Get total record count from Socrata API."""
    url = f"{base_url}?$select=count(*)"
    if where_clause:
        url += f"&$where={where_clause}"
    try:
        resp = httpx.get(url, timeout=30)
        return int(resp.json()[0]["count"])
    except Exception as e:
        print(f"  Could not get count: {e}")
        return None


# ---- NYC ACRIS ----

def parse_nyc_acris(record, state="NY"):
    """Parse a NYC ACRIS deed record into a property_sales tuple."""
    doc_id = record.get("document_id") or record.get("crfn") or ""
    borough_code = record.get("borough", "")
    borough = BOROUGH_MAP.get(str(borough_code), str(borough_code))

    return (
        str(uuid.uuid4()),
        str(doc_id)[:100] or None,          # document_id
        None,                                 # address (ACRIS doesn't include address directly)
        None,                                 # city
        state,                                # state
        None,                                 # zip
        borough[:50] if borough else None,    # borough
        safe_float(record.get("document_amt")),  # sale_price
        safe_date(record.get("document_date")),  # sale_date
        safe_date(record.get("recorded_datetime")),  # recorded_date
        (record.get("doc_type") or "")[:50] or None,  # doc_type
        None,                                 # grantor (not in this dataset directly)
        None,                                 # grantee
        None,                                 # property_type
        None,                                 # building_class
        None,                                 # residential_units
        None,                                 # land_sqft
        None,                                 # gross_sqft
        None,                                 # lat
        None,                                 # lng
        "nyc_acris",                          # source
    )


# ---- Illinois PTAX-203 ----

def parse_il_ptax(record, state="IL"):
    """Parse an Illinois PTAX-203 transfer record into a property_sales tuple."""
    address = (record.get("property_address") or "")[:500] or None
    city = (record.get("property_city") or "")[:100] or None

    return (
        str(uuid.uuid4()),
        (record.get("pin") or "")[:100] or None,  # document_id (use PIN)
        address,                              # address
        city,                                 # city
        state,                                # state
        None,                                 # zip
        None,                                 # borough
        safe_float(record.get("consideration")),  # sale_price
        safe_date(record.get("transfer_date")),  # sale_date
        None,                                 # recorded_date
        "TRANSFER",                           # doc_type
        (record.get("grantor") or "")[:500] or None,  # grantor
        (record.get("grantee") or "")[:500] or None,  # grantee
        (record.get("property_type") or "")[:100] or None,  # property_type
        (record.get("tax_class") or "")[:50] or None,  # building_class
        None,                                 # residential_units
        None,                                 # land_sqft
        None,                                 # gross_sqft
        None,                                 # lat
        None,                                 # lng
        "il_ptax203",                         # source
    )


# ---- Connecticut Sales ----

def parse_ct_sales(record, state="CT"):
    """Parse a Connecticut real estate sale record into a property_sales tuple."""
    address = (record.get("address") or "")[:500] or None
    town = (record.get("town") or "")[:100] or None

    return (
        str(uuid.uuid4()),
        None,                                 # document_id
        address,                              # address
        town,                                 # city (town in CT)
        state,                                # state
        None,                                 # zip
        None,                                 # borough
        safe_float(record.get("saleamount")),  # sale_price
        safe_date(record.get("daterecorded")),  # sale_date
        safe_date(record.get("daterecorded")),  # recorded_date
        "DEED",                               # doc_type
        None,                                 # grantor
        None,                                 # grantee
        (record.get("propertytype") or "")[:100] or None,  # property_type
        (record.get("residentialtype") or "")[:50] or None,  # building_class
        None,                                 # residential_units
        None,                                 # land_sqft
        None,                                 # gross_sqft
        None,                                 # lat
        None,                                 # lng
        "ct_sales",                           # source
    )


# Parser dispatch
PARSERS = {
    "nyc_acris": parse_nyc_acris,
    "il_ptax": parse_il_ptax,
    "ct_sales": parse_ct_sales,
}


def scrape_source(source_key: str, config: dict, conn):
    """Scrape all records from a single Socrata source."""
    cur = conn.cursor()
    base_url = config["base_url"]
    where_clause = config.get("where_clause")
    state = config["state"]
    parser = PARSERS[source_key]

    total_loaded = 0
    offset = 0

    # Get total count
    total_records = get_total_count(base_url, where_clause)
    if total_records is not None:
        print(f"  Total records available: {total_records:,}")

    while True:
        url = f"{base_url}?$limit={PAGE_SIZE}&$offset={offset}&$order=:id"
        if where_clause:
            url += f"&$where={where_clause}"

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
            try:
                row = parser(r, state)
                batch.append(row)
            except Exception as e:
                # Skip malformed records
                continue

        if batch:
            # Insert in sub-batches of BATCH_SIZE
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
            total_loaded += len(batch)
            pct = f" ({total_loaded * 100 // total_records}%)" if total_records else ""
            print(f"    Loaded {total_loaded:,}{pct}")

        offset += PAGE_SIZE
        if len(records) < PAGE_SIZE:
            break
        time.sleep(DELAY)

    cur.close()
    return total_loaded


def main():
    global DB_HOST

    parser = argparse.ArgumentParser(
        description="Scrape property sale/deed transfer records via Socrata APIs"
    )
    parser.add_argument(
        "--source",
        default="all",
        choices=["all", "nyc_acris", "il_ptax", "ct_sales"],
        help="Source to scrape (or 'all')",
    )
    parser.add_argument("--db-host", default=DB_HOST)
    args = parser.parse_args()

    DB_HOST = args.db_host

    conn = get_conn()
    ensure_table(conn)

    sources = list(SOURCES.keys()) if args.source == "all" else [args.source]
    grand_total = 0

    for source_key in sources:
        config = SOURCES[source_key]
        print(f"\n=== Scraping {config['description']} ({source_key}) ===")
        try:
            count = scrape_source(source_key, config, conn)
            grand_total += count
            print(f"  {source_key}: {count:,} sale records loaded")
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()

    conn.close()
    print(f"\n{'=' * 50}")
    print(f"Grand total: {grand_total:,} sale records scraped across {len(sources)} sources")


if __name__ == "__main__":
    main()
