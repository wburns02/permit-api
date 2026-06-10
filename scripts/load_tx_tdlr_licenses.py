#!/usr/bin/env python3
"""
Texas TDLR (Texas Department of Licensing and Regulation) License Loader

Source: data.texas.gov Socrata dataset 7358-krk7 ("TDLR - All Licenses")
        ~962K records covering A/C techs, electricians, plumbers, cosmetology, etc.

Loads into: contractor_licenses table (existing schema)
Unique key: (license_number, state='TX')

Usage:
    python3 -u load_tx_tdlr_licenses.py --db-host 100.122.216.15
    python3 -u load_tx_tdlr_licenses.py --db-host 100.122.216.15 --dry-run
    python3 -u load_tx_tdlr_licenses.py --db-host 100.122.216.15 --license-type "Electrical Contractor"

Cron (weekly Monday 3 AM):
    0 3 * * 1 python3 -u /home/will/permit-api-live/scripts/load_tx_tdlr_licenses.py --db-host 100.122.216.15 >> /var/log/tx_tdlr_licenses.log 2>&1
"""

import argparse
import os
import re
import sys
import time
import uuid
from datetime import date, datetime

try:
    import httpx
except ImportError:
    print("ERROR: pip install httpx"); sys.exit(1)

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary"); sys.exit(1)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"
BATCH_SIZE = 2000

SOCRATA_DOMAIN = "data.texas.gov"
DATASET_ID = "7358-krk7"
SOURCE = "tx_tdlr"
PAGE_SIZE = 5000

# Contractor-relevant license types (filter to skip cosmetology, barbers, etc.)
# Set to None to load ALL types
CONTRACTOR_TYPES = {
    "A/C Contractor",
    "A/C Technician",
    "Electrical Contractor",
    "Electrical Sign Contractor",
    "Journeyman Electrician",
    "Master Electrician",
    "Apprentice Electrician",
    "Maintenance Electrician",
    "Residential Wireman",
    "Plumber",
    "Plumber's Apprentice",
    "Journeyman Plumber",
    "Master Plumber",
    "Tradesman Plumber-Limited",
    "Irrigator",
    "Installer",
    "Water Well Driller",
    "Water Well Pump Installer",
    "Elevator Contractor",
    "Elevator Inspector",
    "Boiler Inspector",
    "Property Tax Consultant",
    "Property Tax Professional",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_conn(host):
    return psycopg2.connect(host=host, port=DB_PORT, dbname=DB_NAME, user=DB_USER, connect_timeout=30)


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def clean_str(s, max_len=500):
    if not s:
        return None
    return str(s).strip()[:max_len] or None


def clean_phone(p):
    if not p:
        return None
    digits = re.sub(r"\D", "", str(p))
    # TDLR phones sometimes have leading zeros or '1' prefix
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) == 10 and digits[0] == "0":
        digits = digits[1:]
    if len(digits) == 10:
        return digits
    return digits if digits else None


def parse_date_mmddyyyy(d):
    """Parse TDLR date format MM/DD/YYYY."""
    if not d:
        return None
    try:
        return datetime.strptime(str(d).strip(), "%m/%d/%Y").date()
    except (ValueError, TypeError):
        try:
            return datetime.strptime(str(d).split("T")[0], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None


def parse_city_state_zip(csz):
    """Parse 'CITY TX 78546' or 'CITY TX 78546-1234' into (city, state, zip)."""
    if not csz:
        return None, None, None
    csz = csz.strip()
    # Match pattern: CITY NAME(s) ST ZIPCODE(-ext)
    m = re.match(r"^(.+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", csz)
    if m:
        return m.group(1).strip(), m.group(2), m.group(3)
    # Fallback: try without zip
    m2 = re.match(r"^(.+?)\s+([A-Z]{2})$", csz)
    if m2:
        return m2.group(1).strip(), m2.group(2), None
    return csz, None, None


def upsert_batch(conn, rows):
    """Insert/update a batch of contractor_licenses rows."""
    if not rows:
        return 0
    sql = """
        INSERT INTO contractor_licenses (
            id, license_number, business_name, full_business_name,
            address, city, state, zip, county, phone, business_type,
            issue_date, expiration_date, status, secondary_status,
            classifications, workers_comp_type, workers_comp_company,
            surety_company, surety_amount, source, last_updated
        ) VALUES %s
        ON CONFLICT (license_number, state) DO UPDATE SET
            business_name = COALESCE(EXCLUDED.business_name, contractor_licenses.business_name),
            full_business_name = COALESCE(EXCLUDED.full_business_name, contractor_licenses.full_business_name),
            address = COALESCE(EXCLUDED.address, contractor_licenses.address),
            city = COALESCE(EXCLUDED.city, contractor_licenses.city),
            zip = COALESCE(EXCLUDED.zip, contractor_licenses.zip),
            county = COALESCE(EXCLUDED.county, contractor_licenses.county),
            phone = COALESCE(EXCLUDED.phone, contractor_licenses.phone),
            business_type = COALESCE(EXCLUDED.business_type, contractor_licenses.business_type),
            expiration_date = COALESCE(EXCLUDED.expiration_date, contractor_licenses.expiration_date),
            status = COALESCE(EXCLUDED.status, contractor_licenses.status),
            classifications = COALESCE(EXCLUDED.classifications, contractor_licenses.classifications),
            source = EXCLUDED.source,
            last_updated = EXCLUDED.last_updated
    """
    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
    return len(rows)


def make_row(lic_num, **kw):
    """Build a tuple matching the contractor_licenses INSERT column order."""
    return (
        str(uuid.uuid4()),
        clean_str(lic_num, 100),
        clean_str(kw.get("business_name"), 500),
        clean_str(kw.get("full_business_name"), 500),
        clean_str(kw.get("address"), 500),
        clean_str(kw.get("city"), 100),
        "TX",
        clean_str(kw.get("zip"), 10),
        clean_str(kw.get("county"), 100),
        clean_phone(kw.get("phone")),
        clean_str(kw.get("business_type"), 50),
        parse_date_mmddyyyy(kw.get("issue_date")),
        parse_date_mmddyyyy(kw.get("expiration_date")),
        clean_str(kw.get("status"), 50),
        clean_str(kw.get("secondary_status"), 100),
        clean_str(kw.get("classifications"), 1000),
        None, None, None, None,  # workers_comp_*, surety_*
        SOURCE,
        date.today(),
    )


# ---------------------------------------------------------------------------
# TDLR Socrata Loader
# ---------------------------------------------------------------------------

def fetch_socrata(client, offset=0, where_clause=None):
    """Paginate the TDLR Socrata dataset and yield pages of rows."""
    url = f"https://{SOCRATA_DOMAIN}/resource/{DATASET_ID}.json"
    while True:
        params = {"$limit": PAGE_SIZE, "$offset": offset, "$order": "license_number"}
        if where_clause:
            params["$where"] = where_clause
        resp = client.get(url, params=params, timeout=90)
        if resp.status_code == 429:
            log("  Rate limited, waiting 30s...")
            time.sleep(30)
            continue
        if resp.status_code != 200:
            log(f"  HTTP {resp.status_code} at offset {offset}: {resp.text[:300]}")
            break
        data = resp.json()
        if not data:
            break
        yield data
        if len(data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.5)  # Be polite to the API


def map_row(raw):
    """Map a TDLR Socrata row to our schema fields."""
    # Parse business city/state/zip
    biz_csz = raw.get("business_city_state_zip", "")
    biz_city, _, biz_zip = parse_city_state_zip(biz_csz)

    # Build address from line1 + line2
    addr_parts = [raw.get("business_address_line1"), raw.get("business_address_line2")]
    address = ", ".join(p.strip() for p in addr_parts if p and p.strip()) or None

    license_type = raw.get("license_type", "")
    license_subtype = raw.get("license_subtype", "")
    classifications = license_type
    if license_subtype:
        classifications = f"{license_type} ({license_subtype})"

    # Determine status from expiration date
    exp_str = raw.get("license_expiration_date_mmddccyy")
    exp_date = parse_date_mmddyyyy(exp_str)
    status = None
    if exp_date:
        status = "Active" if exp_date >= date.today() else "Expired"

    biz_name = raw.get("business_name") or raw.get("owner_name") or "Unknown"

    return {
        "business_name": biz_name,
        "full_business_name": raw.get("owner_name"),
        "address": address,
        "city": biz_city,
        "zip": biz_zip,
        "county": raw.get("business_county"),
        "phone": raw.get("business_telephone") or raw.get("owner_telephone"),
        "business_type": license_type,
        "expiration_date": exp_str,
        "status": status,
        "classifications": classifications,
        "secondary_status": raw.get("continuing_education_flag"),
    }


def load_tdlr_licenses(conn, client, license_type_filter=None, all_types=False, dry_run=False):
    """Load TDLR licenses from Socrata into contractor_licenses."""
    log("=== Texas TDLR Licenses ===")
    log(f"  Dataset: {SOCRATA_DOMAIN}/resource/{DATASET_ID}")

    # Build Socrata WHERE clause
    where_clause = None
    if license_type_filter:
        safe_type = license_type_filter.replace("'", "''")
        where_clause = f"license_type='{safe_type}'"
        log(f"  Filtering: license_type = '{license_type_filter}'")
    elif not all_types and CONTRACTOR_TYPES:
        type_list = ", ".join(f"'{t.replace(chr(39), chr(39)+chr(39))}'" for t in sorted(CONTRACTOR_TYPES))
        where_clause = f"license_type in ({type_list})"
        log(f"  Filtering to {len(CONTRACTOR_TYPES)} contractor-relevant license types")

    batch = []
    seen = set()
    total = 0
    skipped = 0

    for page in fetch_socrata(client, where_clause=where_clause):
        for raw in page:
            lic_num = raw.get("license_number")
            if not lic_num or lic_num in seen:
                skipped += 1
                continue
            seen.add(lic_num)

            mapped = map_row(raw)
            batch.append(make_row(lic_num, **mapped))

            if len(batch) >= BATCH_SIZE:
                if dry_run:
                    log(f"    [DRY RUN] would upsert {len(batch)} (running: {total + len(batch)})")
                    total += len(batch)
                    batch = []
                else:
                    n = upsert_batch(conn, batch)
                    total += n
                    log(f"    upserted {n} (running: {total})")
                    batch = []

    if batch:
        if dry_run:
            log(f"    [DRY RUN] would upsert {len(batch)} (final)")
            total += len(batch)
        else:
            n = upsert_batch(conn, batch)
            total += n

    log(f"  TX TDLR: {total:,} records {'would be ' if dry_run else ''}upserted, {skipped:,} skipped/dupes")
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Load Texas TDLR contractor licenses")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host (default: %(default)s)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch data but don't write to DB")
    parser.add_argument("--license-type", default=None,
                        help="Load only a specific license type (e.g. 'Electrical Contractor')")
    parser.add_argument("--all-types", action="store_true",
                        help="Load ALL TDLR license types (not just contractor-relevant ones)")
    args = parser.parse_args()

    log("Texas TDLR License Loader")
    log(f"Database: {args.db_host}:{DB_PORT}/{DB_NAME}")
    if args.dry_run:
        log("*** DRY RUN MODE — no database writes ***")

    conn = None
    if not args.dry_run:
        conn = get_conn(args.db_host)

    client = httpx.Client(
        follow_redirects=True,
        headers={"User-Agent": "PermitLookup-DataLoader/1.0"},
        timeout=httpx.Timeout(90.0, connect=30.0),
    )

    try:
        total = load_tdlr_licenses(
            conn, client,
            license_type_filter=args.license_type,
            all_types=args.all_types,
            dry_run=args.dry_run,
        )

        if not args.dry_run and conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT business_type, count(*), count(*) FILTER (WHERE status = 'Active')
                FROM contractor_licenses
                WHERE state = 'TX' AND source = 'tx_tdlr'
                GROUP BY business_type
                ORDER BY count(*) DESC
            """)
            log("--- Summary by License Type ---")
            for row in cur.fetchall():
                log(f"  {row[0]}: {row[1]:,} total, {row[2]:,} active")
            cur.close()

            cur = conn.cursor()
            cur.execute("""
                SELECT count(*) FROM contractor_licenses
                WHERE state = 'TX' AND source = 'tx_tdlr'
            """)
            db_total = cur.fetchone()[0]
            cur.close()
            log(f"Total TX TDLR records in DB: {db_total:,}")

        log(f"DONE — {total:,} records {'would be ' if args.dry_run else ''}upserted this run")

    except Exception as e:
        log(f"FATAL: {e}")
        raise
    finally:
        client.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
