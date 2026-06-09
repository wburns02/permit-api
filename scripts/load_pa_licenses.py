#!/usr/bin/env python3
"""
Pennsylvania PALS (Professional and Occupational Affairs Licensing System) Loader

Source: pals.pa.gov Angular SPA with REST API backend
Method: JSON API calls to BPOA search endpoint

Covers: Home Improvement Contractors, General Contractors, Electricians, Plumbers,
        plus related construction trades.

NOTE: PA PALS is an Angular SPA backed by a .NET API. The search endpoint accepts
JSON POST requests and returns paginated results. We iterate through board types
and last-name 2-letter prefixes to harvest all licensees.

The PA Attorney General's Home Improvement Contractor Registration is the primary
contractor license in PA. PA also licenses electricians and plumbers at the state level.

Strategy: Iterate through board/license types, using 2-letter last-name prefixes
and pagination to harvest all licensees from the PALS API.

Loads into: contractor_licenses table (existing schema)
Unique key: (license_number, state)

Usage:
    python3 -u load_pa_licenses.py --db-host 100.122.216.15
    python3 -u load_pa_licenses.py --dry-run
    python3 -u load_pa_licenses.py --board "Home Improvement Contractor"

Cron (weekly Monday 3 AM):
    0 3 * * 1 python3 -u /home/will/permit-api-live/scripts/load_pa_licenses.py --db-host 100.122.216.15 >> /var/log/pa_licenses.log 2>&1
"""

import argparse
import json
import os
import re
import string
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

PALS_BASE = "https://www.pals.pa.gov"
PALS_API = f"{PALS_BASE}/api/Search/SearchForPersonOrFacility"
PALS_DETAIL_API = f"{PALS_BASE}/api/Search/GetPersonOrFacilityDetails"
SOURCE = "pa_pals"
PAGE_SIZE = 100

# PA PALS boards/license types for construction trades
# Board IDs and license types from PALS dropdown
LICENSE_SEARCHES = [
    {
        "board": "Home Improvement Contractor",
        "board_code": "48",
        "license_type": "",
        "search_type": "facility",
    },
    {
        "board": "Architects Licensure Board",
        "board_code": "01",
        "license_type": "",
        "search_type": "person",
    },
    {
        "board": "Engineers, Land Surveyors and Geologists",
        "board_code": "10",
        "license_type": "",
        "search_type": "person",
    },
    {
        "board": "Construction Code Officials",
        "board_code": "",
        "license_type": "Construction Code Official",
        "search_type": "person",
    },
]

# Alternative: search by license type keywords across all boards
LICENSE_TYPE_KEYWORDS = [
    "Home Improvement Contractor",
    "General Contractor",
    "Electrical Contractor",
    "Electrician",
    "Master Plumber",
    "Journeyman Plumber",
    "Plumber",
    "HVAC",
    "Construction",
]


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
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s[:max_len] or None


def clean_phone(p):
    if not p:
        return None
    digits = re.sub(r"\D", "", str(p))
    if len(digits) == 10:
        return digits
    if len(digits) == 11 and digits[0] == "1":
        return digits[1:]
    return digits if digits else None


def parse_date(d):
    if not d:
        return None
    # PALS returns dates as "/Date(1234567890000)/" or ISO strings
    if isinstance(d, str) and d.startswith("/Date("):
        ms_match = re.search(r'/Date\((-?\d+)', d)
        if ms_match:
            try:
                return datetime.fromtimestamp(int(ms_match.group(1)) / 1000).date()
            except (ValueError, OSError):
                return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(str(d).strip().split(".")[0], fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def make_row(lic_num, source, **kw):
    """Build a tuple matching the contractor_licenses INSERT column order."""
    return (
        str(uuid.uuid4()),
        clean_str(lic_num, 100),
        clean_str(kw.get("business_name"), 500),
        clean_str(kw.get("full_business_name"), 500),
        clean_str(kw.get("address"), 500),
        clean_str(kw.get("city"), 100),
        "PA",
        clean_str(kw.get("zip"), 10),
        clean_str(kw.get("county"), 100),
        clean_phone(kw.get("phone")),
        clean_str(kw.get("business_type"), 50),
        parse_date(kw.get("issue_date")),
        parse_date(kw.get("expiration_date")),
        clean_str(kw.get("status"), 50),
        None,  # secondary_status
        clean_str(kw.get("classifications"), 1000),
        None, None, None, None,  # workers_comp_*, surety_*
        source,
        date.today(),
    )


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
            phone = COALESCE(EXCLUDED.phone, contractor_licenses.phone),
            business_type = COALESCE(EXCLUDED.business_type, contractor_licenses.business_type),
            expiration_date = COALESCE(EXCLUDED.expiration_date, contractor_licenses.expiration_date),
            status = COALESCE(EXCLUDED.status, contractor_licenses.status),
            classifications = COALESCE(EXCLUDED.classifications, contractor_licenses.classifications),
            last_updated = EXCLUDED.last_updated
    """
    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
    return len(rows)


# ---------------------------------------------------------------------------
# PA PALS API client
# ---------------------------------------------------------------------------

class PALSClient:
    """Client for Pennsylvania PALS REST API."""

    def __init__(self):
        self.client = httpx.Client(
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": PALS_BASE,
                "Referer": f"{PALS_BASE}/",
            },
            timeout=httpx.Timeout(90.0, connect=30.0),
        )

    def search_person(self, last_name="", first_name="", license_number="",
                      board_code="", license_type="", city="", state="",
                      county="", zip_code="", status="", page=1, page_size=PAGE_SIZE):
        """Search for individual licensees."""
        payload = {
            "LastName": last_name,
            "FirstName": first_name,
            "LicenseNumber": license_number,
            "Board": board_code,
            "LicenseType": license_type,
            "City": city,
            "State": state,
            "County": county,
            "ZipCode": zip_code,
            "Status": status,
            "IsFacility": False,
            "PageNumber": page,
            "PageSize": page_size,
            "SortColumn": "LicenseNumber",
            "SortDirection": "ASC",
        }
        resp = self.client.post(PALS_API, content=json.dumps(payload), timeout=90)
        if resp.status_code != 200:
            raise RuntimeError(f"PALS API error {resp.status_code}: {resp.text[:200]}")
        return resp.json()

    def search_facility(self, facility_name="", license_number="",
                        board_code="", license_type="", city="", state="",
                        county="", zip_code="", status="", page=1, page_size=PAGE_SIZE):
        """Search for facility/business licensees."""
        payload = {
            "FacilityName": facility_name,
            "LicenseNumber": license_number,
            "Board": board_code,
            "LicenseType": license_type,
            "City": city,
            "State": state,
            "County": county,
            "ZipCode": zip_code,
            "Status": status,
            "IsFacility": True,
            "PageNumber": page,
            "PageSize": page_size,
            "SortColumn": "LicenseNumber",
            "SortDirection": "ASC",
        }
        resp = self.client.post(PALS_API, content=json.dumps(payload), timeout=90)
        if resp.status_code != 200:
            raise RuntimeError(f"PALS API error {resp.status_code}: {resp.text[:200]}")
        return resp.json()

    def close(self):
        self.client.close()


def extract_person_record(item):
    """Extract fields from a PALS person search result."""
    first = clean_str(item.get("FirstName", ""))
    last = clean_str(item.get("LastName", ""))
    middle = clean_str(item.get("MiddleName", ""))
    suffix = clean_str(item.get("Suffix", ""))

    name_parts = [p for p in [first, middle, last, suffix] if p]
    full_name = " ".join(name_parts) if name_parts else "Unknown"

    address_parts = [
        item.get("AddressLine1", ""),
        item.get("AddressLine2", ""),
    ]
    address = ", ".join(p for p in address_parts if p and p.strip())

    return {
        "license_number": item.get("LicenseNumber", ""),
        "business_name": full_name,
        "full_business_name": item.get("DBA") or item.get("BusinessName"),
        "address": address or None,
        "city": item.get("City"),
        "zip": item.get("ZipCode"),
        "county": item.get("County"),
        "phone": item.get("Phone"),
        "status": item.get("Status"),
        "issue_date": item.get("IssueDate") or item.get("OriginalIssueDate"),
        "expiration_date": item.get("ExpirationDate"),
        "classifications": item.get("LicenseType") or item.get("Board"),
        "business_type": "Individual",
    }


def extract_facility_record(item):
    """Extract fields from a PALS facility search result."""
    biz_name = (item.get("FacilityName") or item.get("BusinessName")
                or item.get("Name") or "Unknown")
    dba = item.get("DBA", "")

    address_parts = [
        item.get("AddressLine1", ""),
        item.get("AddressLine2", ""),
    ]
    address = ", ".join(p for p in address_parts if p and p.strip())

    return {
        "license_number": item.get("LicenseNumber", ""),
        "business_name": biz_name,
        "full_business_name": dba if dba else None,
        "address": address or None,
        "city": item.get("City"),
        "zip": item.get("ZipCode"),
        "county": item.get("County"),
        "phone": item.get("Phone"),
        "status": item.get("Status"),
        "issue_date": item.get("IssueDate") or item.get("OriginalIssueDate"),
        "expiration_date": item.get("ExpirationDate"),
        "classifications": item.get("LicenseType") or item.get("Board"),
        "business_type": "Business",
    }


# ---------------------------------------------------------------------------
# Search iteration
# ---------------------------------------------------------------------------

def generate_name_prefixes():
    """Generate 2-letter prefixes for exhaustive search."""
    for c1 in string.ascii_uppercase:
        for c2 in string.ascii_uppercase:
            yield f"{c1}{c2}"


def load_pa_person_licenses(conn, pals, board_code, board_name, dry_run=False):
    """Load PA person licenses for a given board by iterating name prefixes."""
    log(f"  Person search: {board_name}")
    seen = set()
    batch = []
    total = 0
    errors = 0

    prefixes = list(generate_name_prefixes())
    for idx, prefix in enumerate(prefixes):
        page = 1
        while True:
            try:
                result = pals.search_person(
                    last_name=prefix,
                    board_code=board_code,
                    page=page,
                    page_size=PAGE_SIZE,
                )
            except Exception as e:
                log(f"    ERROR {prefix} page {page}: {e}")
                errors += 1
                if errors > 20:
                    log("    Too many errors, pausing 30s...")
                    time.sleep(30)
                    errors = 0
                break

            # Handle different response shapes
            items = []
            total_count = 0
            if isinstance(result, dict):
                items = result.get("Results", result.get("results", result.get("Items", [])))
                total_count = result.get("TotalCount", result.get("totalCount", len(items)))
            elif isinstance(result, list):
                items = result
                total_count = len(items)

            if not items:
                break

            new_count = 0
            for item in items:
                rec = extract_person_record(item)
                lic_num = rec.pop("license_number", "").strip()
                if not lic_num or lic_num in seen:
                    continue
                seen.add(lic_num)
                new_count += 1

                if not rec.get("classifications"):
                    rec["classifications"] = board_name

                batch.append(make_row(lic_num, SOURCE, **rec))

            if new_count > 0 and idx % 50 == 0:
                log(f"    [{idx+1}/{len(prefixes)}] prefix={prefix}: {len(items)}/{total_count} results, {new_count} new (total: {len(seen)})")

            if len(batch) >= BATCH_SIZE:
                if dry_run:
                    log(f"    [DRY RUN] would upsert {len(batch)} (total seen: {len(seen)})")
                    total += len(batch)
                else:
                    n = upsert_batch(conn, batch)
                    total += n
                    log(f"    upserted {n} (running: {total})")
                batch = []

            # Check pagination
            if page * PAGE_SIZE >= total_count or len(items) < PAGE_SIZE:
                break
            page += 1
            time.sleep(0.3)

        time.sleep(0.3)

    # Flush
    if batch:
        if dry_run:
            total += len(batch)
        else:
            n = upsert_batch(conn, batch)
            total += n
        batch = []

    log(f"  {board_name} (person): {total:,} records")
    return total


def load_pa_facility_licenses(conn, pals, board_code, board_name, dry_run=False):
    """Load PA facility licenses for a given board by iterating name prefixes."""
    log(f"  Facility search: {board_name}")
    seen = set()
    batch = []
    total = 0
    errors = 0

    # For facilities, use single-letter + common construction words
    prefixes = list(string.ascii_uppercase) + [str(d) for d in range(10)]

    for idx, prefix in enumerate(prefixes):
        page = 1
        while True:
            try:
                result = pals.search_facility(
                    facility_name=prefix,
                    board_code=board_code,
                    page=page,
                    page_size=PAGE_SIZE,
                )
            except Exception as e:
                log(f"    ERROR {prefix} page {page}: {e}")
                errors += 1
                if errors > 20:
                    log("    Too many errors, pausing 30s...")
                    time.sleep(30)
                    errors = 0
                break

            items = []
            total_count = 0
            if isinstance(result, dict):
                items = result.get("Results", result.get("results", result.get("Items", [])))
                total_count = result.get("TotalCount", result.get("totalCount", len(items)))
            elif isinstance(result, list):
                items = result
                total_count = len(items)

            if not items:
                break

            new_count = 0
            for item in items:
                rec = extract_facility_record(item)
                lic_num = rec.pop("license_number", "").strip()
                if not lic_num or lic_num in seen:
                    continue
                seen.add(lic_num)
                new_count += 1

                if not rec.get("classifications"):
                    rec["classifications"] = board_name

                batch.append(make_row(lic_num, SOURCE, **rec))

            if new_count > 0:
                log(f"    prefix={prefix} page={page}: {len(items)}/{total_count}, {new_count} new")

            if len(batch) >= BATCH_SIZE:
                if dry_run:
                    log(f"    [DRY RUN] would upsert {len(batch)} (total seen: {len(seen)})")
                    total += len(batch)
                else:
                    n = upsert_batch(conn, batch)
                    total += n
                    log(f"    upserted {n} (running: {total})")
                batch = []

            if page * PAGE_SIZE >= total_count or len(items) < PAGE_SIZE:
                break
            page += 1
            time.sleep(0.3)

        # If single letter returns too many, expand to 2-letter
        if total_count and total_count >= PAGE_SIZE * 10:
            log(f"    Expanding prefix {prefix} to 2-letter...")
            for c2 in string.ascii_lowercase:
                sub_prefix = prefix + c2
                sub_page = 1
                while True:
                    try:
                        sub_result = pals.search_facility(
                            facility_name=sub_prefix,
                            board_code=board_code,
                            page=sub_page,
                            page_size=PAGE_SIZE,
                        )
                    except Exception:
                        break

                    sub_items = []
                    sub_total = 0
                    if isinstance(sub_result, dict):
                        sub_items = sub_result.get("Results", sub_result.get("results", []))
                        sub_total = sub_result.get("TotalCount", len(sub_items))
                    elif isinstance(sub_result, list):
                        sub_items = sub_result

                    if not sub_items:
                        break

                    for item in sub_items:
                        rec = extract_facility_record(item)
                        lic_num = rec.pop("license_number", "").strip()
                        if not lic_num or lic_num in seen:
                            continue
                        seen.add(lic_num)

                        if not rec.get("classifications"):
                            rec["classifications"] = board_name
                        batch.append(make_row(lic_num, SOURCE, **rec))

                    if sub_page * PAGE_SIZE >= sub_total or len(sub_items) < PAGE_SIZE:
                        break
                    sub_page += 1
                    time.sleep(0.3)

        time.sleep(0.3)

    # Flush
    if batch:
        if dry_run:
            total += len(batch)
        else:
            n = upsert_batch(conn, batch)
            total += n
        batch = []

    log(f"  {board_name} (facility): {total:,} records")
    return total


def load_pa_licenses(conn, pals, searches, dry_run=False):
    """Load PA PALS licenses across all configured boards."""
    log("=== Pennsylvania PALS Professional Licenses ===")
    grand_total = 0

    for search_cfg in searches:
        board_name = search_cfg["board"]
        board_code = search_cfg["board_code"]
        search_type = search_cfg["search_type"]

        if search_type == "facility":
            n = load_pa_facility_licenses(conn, pals, board_code, board_name, dry_run=dry_run)
        else:
            n = load_pa_person_licenses(conn, pals, board_code, board_name, dry_run=dry_run)

        grand_total += n

    log(f"PA PALS total: {grand_total:,} records")
    return grand_total


def main():
    parser = argparse.ArgumentParser(description="Load Pennsylvania PALS professional licenses")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write to DB")
    parser.add_argument("--board", help="Load only one board (e.g. 'Home Improvement Contractor')")
    args = parser.parse_args()

    log("Pennsylvania PALS Professional License Loader")
    log(f"Database: {args.db_host}:{DB_PORT}/{DB_NAME}")
    if args.dry_run:
        log("*** DRY RUN MODE ***")

    conn = None
    if not args.dry_run:
        conn = get_conn(args.db_host)

    pals = PALSClient()

    try:
        if args.board:
            searches = [s for s in LICENSE_SEARCHES if s["board"] == args.board]
            if not searches:
                log(f"Unknown board: {args.board}. Available: {[s['board'] for s in LICENSE_SEARCHES]}")
                sys.exit(1)
        else:
            searches = LICENSE_SEARCHES

        total = load_pa_licenses(conn, pals, searches, dry_run=args.dry_run)

        if conn and not args.dry_run:
            cur = conn.cursor()
            cur.execute("""
                SELECT classifications, count(*)
                FROM contractor_licenses
                WHERE state = 'PA' AND source = %s
                GROUP BY classifications
                ORDER BY count(*) DESC
            """, (SOURCE,))
            log("--- Summary by Classification ---")
            for row in cur.fetchall():
                log(f"  {row[0]}: {row[1]:,}")
            cur.close()

        log(f"DONE — {total:,} records {'would be' if args.dry_run else ''} upserted this run")

    except Exception as e:
        log(f"FATAL: {e}")
        raise
    finally:
        pals.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
