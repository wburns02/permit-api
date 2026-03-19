#!/usr/bin/env python3
"""
Multi-city code violation scraper via Socrata Open Data APIs.

Datasets:
- NYC Housing Violations (HPD): data.cityofnewyork.us/resource/wvxf-dwi5 — 10.8M records
- Chicago Building Violations: data.cityofchicago.org/resource/22u3-xenr — 2M records
- Cincinnati Code Violations: data.cincinnati-oh.gov/resource/cncm-znd6 — 751K records
- Montgomery County MD: data.montgomerycountymd.gov/resource/k9nj-z35d — 932K records
- LA Building Safety: data.lacity.org/resource/9w5z-rg2h — 11M records

Usage:
    python scrape_code_violations.py --city nyc --db-host 100.122.216.15
    python scrape_code_violations.py --city all --db-host 100.122.216.15
    python scrape_code_violations.py --city chicago --limit 50000

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

# City Socrata dataset configurations
# Each city maps fields to our unified schema:
#   violation_id, address, city, state, zip, violation_type, violation_code,
#   description, status, violation_date, inspection_date, resolution_date,
#   fine_amount, lat, lng, source
CITY_CONFIGS = {
    "nyc": {
        "base_url": "https://data.cityofnewyork.us/resource/wvxf-dwi5.json",
        "source": "nyc_hpd",
        "city": "New York",
        "state": "NY",
        "fields": {
            "violation_id": "violationid",
            "address": ["housenumber", "streetname"],  # concat
            "zip": "zip",
            "violation_type": "class",  # A, B, C
            "violation_code": None,
            "description": "novdescription",
            "status": "currentstatus",
            "violation_date": "inspectiondate",
            "inspection_date": "inspectiondate",
            "resolution_date": "currentstatusdate",
            "fine_amount": None,
            "lat": None,
            "lng": None,
        },
    },
    "chicago": {
        "base_url": "https://data.cityofchicago.org/resource/22u3-xenr.json",
        "source": "chicago_bldg",
        "city": "Chicago",
        "state": "IL",
        "fields": {
            "violation_id": "id",
            "address": "address",
            "zip": None,
            "violation_type": None,
            "violation_code": "violation_code",
            "description": "violation_description",
            "status": "violation_status",
            "violation_date": "violation_date",
            "inspection_date": "violation_date",  # Same field, no separate inspection date
            "resolution_date": None,
            "fine_amount": None,
            "lat": "latitude",
            "lng": "longitude",
        },
    },
    "cincinnati": {
        "base_url": "https://data.cincinnati-oh.gov/resource/cncm-znd6.json",
        "source": "cincinnati_bldg",
        "city": "Cincinnati",
        "state": "OH",
        "fields": {
            "violation_id": "case_number",
            "address": "full_address",
            "zip": "zip_code",
            "violation_type": "data_status_display",
            "violation_code": "violation_code",
            "description": "description",
            "status": "status",
            "violation_date": "entered_date",
            "inspection_date": "last_inspection_date",
            "resolution_date": "closed_date",
            "fine_amount": None,
            "lat": "latitude",
            "lng": "longitude",
        },
    },
    "montgomery": {
        "base_url": "https://data.montgomerycountymd.gov/resource/k9nj-z35d.json",
        "source": "montgomery_county_md",
        "city": None,  # Varies — extract from data if available
        "state": "MD",
        "fields": {
            "violation_id": "case_number",
            "address": "street_address",
            "zip": "zip_code",
            "violation_type": "category",
            "violation_code": "violation_code",
            "description": "description",
            "status": "status",
            "violation_date": "date_filed",
            "inspection_date": "inspection_date",
            "resolution_date": "date_closed",
            "fine_amount": None,
            "lat": "latitude",
            "lng": "longitude",
        },
    },
    "la": {
        "base_url": "https://data.lacity.org/resource/9w5z-rg2h.json",
        "source": "la_building_safety",
        "city": "Los Angeles",
        "state": "CA",
        "fields": {
            "violation_id": "permit",
            "address": "address",
            "zip": None,
            "violation_type": "inspection",
            "violation_code": None,
            "description": "inspection_result",
            "status": "inspection_result",
            "violation_date": "inspection_date",
            "inspection_date": "inspection_date",
            "resolution_date": None,
            "fine_amount": None,
            "lat": "lat",
            "lng": "lon",
        },
    },
}


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER
    )


def ensure_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS code_violations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            violation_id VARCHAR(100),
            address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(2) NOT NULL,
            zip VARCHAR(10),
            violation_type VARCHAR(200),
            violation_code VARCHAR(100),
            description TEXT,
            status VARCHAR(50),
            violation_date DATE,
            inspection_date DATE,
            resolution_date DATE,
            fine_amount FLOAT,
            lat FLOAT,
            lng FLOAT,
            source VARCHAR(50) NOT NULL
        )
    """)
    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_violations_vid ON code_violations (violation_id)",
        "CREATE INDEX IF NOT EXISTS ix_violations_addr ON code_violations (address)",
        "CREATE INDEX IF NOT EXISTS ix_violations_city ON code_violations (city)",
        "CREATE INDEX IF NOT EXISTS ix_violations_state ON code_violations (state)",
        "CREATE INDEX IF NOT EXISTS ix_violations_status ON code_violations (status)",
        "CREATE INDEX IF NOT EXISTS ix_violations_date ON code_violations (violation_date)",
        "CREATE INDEX IF NOT EXISTS ix_violations_geo ON code_violations (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_violations_source_vid ON code_violations (source, violation_id)",
    ]:
        try:
            cur.execute(idx)
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()


def safe_date(val):
    """Parse Socrata date formats into a Python date."""
    if not val:
        return None
    try:
        # Socrata format: 2025-06-16T00:00:00.000
        return datetime.fromisoformat(val.replace("T00:00:00.000", "")).date()
    except (ValueError, AttributeError):
        pass
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except (ValueError, AttributeError):
        return None


def safe_float(val):
    """Safely convert to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def get_field(record, field_spec):
    """Extract a field from a record given a field spec (string or list for concat)."""
    if field_spec is None:
        return None
    if isinstance(field_spec, list):
        parts = [str(record.get(f, "") or "").strip() for f in field_spec]
        joined = " ".join(p for p in parts if p)
        return joined if joined else None
    return record.get(field_spec)


def scrape_city(city_key: str, config: dict, conn, max_records: int | None = None):
    """Scrape all violations from a city's Socrata API."""
    cur = conn.cursor()
    base_url = config["base_url"]
    source = config["source"]
    default_city = config.get("city")
    default_state = config["state"]
    fm = config["fields"]

    total = 0
    offset = 0

    # Get total count first
    count_url = f"{base_url}?$select=count(*)"
    try:
        resp = httpx.get(count_url, timeout=30)
        resp.raise_for_status()
        total_records = int(resp.json()[0]["count"])
        print(f"  Total records available: {total_records:,}")
        if max_records:
            print(f"  Limiting to: {max_records:,}")
    except Exception as e:
        print(f"  Could not get count: {e}")
        total_records = None

    while True:
        if max_records and total >= max_records:
            print(f"  Reached limit of {max_records:,} records")
            break

        url = f"{base_url}?$limit={PAGE_SIZE}&$offset={offset}&$order=:id"
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
            if max_records and (total + len(batch)) >= max_records:
                break

            # Extract violation_id
            vid = get_field(r, fm["violation_id"])
            if not vid:
                continue
            vid = str(vid)[:100]

            # Extract address
            address = get_field(r, fm["address"])
            if address:
                address = str(address)[:500]

            # Extract city — from record or use default
            city_val = default_city
            if not city_val:
                # Try to extract from data (Montgomery County etc.)
                for city_field in ("city", "municipality", "town"):
                    if r.get(city_field):
                        city_val = str(r[city_field])[:100]
                        break

            # Extract zip
            zip_val = get_field(r, fm["zip"])
            if zip_val:
                zip_val = str(zip_val)[:10]

            batch.append((
                str(uuid.uuid4()),
                vid,
                address,
                city_val,
                default_state,
                zip_val,
                (str(get_field(r, fm["violation_type"]) or "")[:200]) or None,
                (str(get_field(r, fm["violation_code"]) or "")[:100]) or None,
                get_field(r, fm["description"]),
                (str(get_field(r, fm["status"]) or "")[:50]) or None,
                safe_date(get_field(r, fm["violation_date"])),
                safe_date(get_field(r, fm["inspection_date"])),
                safe_date(get_field(r, fm["resolution_date"])),
                safe_float(get_field(r, fm["fine_amount"])),
                safe_float(get_field(r, fm["lat"])),
                safe_float(get_field(r, fm["lng"])),
                source,
            ))

        if batch:
            execute_values(cur, """
                INSERT INTO code_violations (id, violation_id, address, city, state, zip,
                    violation_type, violation_code, description, status,
                    violation_date, inspection_date, resolution_date,
                    fine_amount, lat, lng, source)
                VALUES %s ON CONFLICT DO NOTHING
            """, batch)
            conn.commit()
            total += len(batch)
            pct = f" ({total * 100 // total_records}%)" if total_records else ""
            print(f"    Loaded {total:,}{pct}")

        offset += PAGE_SIZE
        if len(records) < PAGE_SIZE:
            break
        time.sleep(DELAY)

    cur.close()
    return total


def main():
    global DB_HOST

    parser = argparse.ArgumentParser(description="Scrape code violations via Socrata APIs")
    parser.add_argument("--city", default="all",
                        choices=["all"] + list(CITY_CONFIGS.keys()),
                        help="City to scrape (or 'all')")
    parser.add_argument("--db-host", default=DB_HOST)
    parser.add_argument("--limit", type=int, default=None,
                        help="Max records per city (useful for testing)")
    parser.add_argument("--reset", action="store_true",
                        help="Drop and recreate the table before scraping")
    args = parser.parse_args()

    DB_HOST = args.db_host

    conn = get_conn()

    if args.reset:
        print("Resetting code_violations table...")
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS code_violations CASCADE")
        conn.commit()
        cur.close()

    ensure_table(conn)

    cities = list(CITY_CONFIGS.keys()) if args.city == "all" else [args.city]
    grand_total = 0

    for city in cities:
        config = CITY_CONFIGS[city]
        print(f"\n=== Scraping {city.upper()} ({config['source']}) ===")
        try:
            count = scrape_city(city, config, conn, max_records=args.limit)
            grand_total += count
            print(f"  {city.upper()}: {count:,} violations loaded")
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()

    conn.close()
    print(f"\n{'=' * 50}")
    print(f"Grand total: {grand_total:,} violations scraped across {len(cities)} cities")


if __name__ == "__main__":
    main()
