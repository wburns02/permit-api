#!/usr/bin/env python3
"""
Ohio eLicense Professional License Loader

Source: elicense.ohio.gov Salesforce Visualforce Remoting API
Boards covered: All 24 boards on elicense.ohio.gov including:
  - Architects Board
  - Engineers and Surveyors Board
  - Motor Vehicle Repair Board
  - Div. of Industrial Compliance: Elevator
  - Department of Commerce - Manufactured Homes Program

NOTE: Ohio does NOT have a centralized state-level general contractor license.
OCILB (Construction Industry Licensing Board) covers HVAC, plumbing, electrical,
hydronics, and refrigeration — but OCILB's lookup site (com.ohio.gov) appears
to be offline/restructured as of 2026-04. This scraper pulls all available
construction-adjacent licenses from elicense.ohio.gov via last-name iteration.

Strategy: Iterate through last names A-Z (and AA-AZ, BA-BZ, etc.) to harvest
all licensees across construction-related boards.

Loads into: contractor_licenses table (existing schema)
Unique key: (license_number, state)

Usage:
    python3 -u load_oh_ocilb_licenses.py --db-host 100.122.216.15
    python3 -u load_oh_ocilb_licenses.py --dry-run
    python3 -u load_oh_ocilb_licenses.py --board "Architects Board"

Cron (weekly Monday 5:30 AM):
    30 5 * * 1 python3 -u /home/will/permit-api-live/scripts/load_oh_ocilb_licenses.py --db-host 100.122.216.15 >> /var/log/oh_elicense.log 2>&1
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

ELICENSE_URL = "https://elicense.ohio.gov"
VERIFY_PAGE = f"{ELICENSE_URL}/OH_VerifyLicense"
REMOTE_URL = f"{ELICENSE_URL}/apexremote"
SOURCE = "oh_elicense"

# Boards with construction-adjacent licenses
TARGET_BOARDS = [
    "Architects Board",
    "Engineers and Surveyors Board",
    "Motor Vehicle Repair Board",
    "Div. of Industrial Compliance: Elevator",
    "Department of Commerce - Manufactured Homes Program",
]

# If --all-boards, we scrape everything
ALL_BOARDS_SENTINEL = "__ALL__"


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
    if len(digits) == 10:
        return digits
    if len(digits) == 11 and digits[0] == "1":
        return digits[1:]
    return digits if digits else None


def parse_date(d):
    if not d:
        return None
    # Salesforce returns epoch millis or ISO strings
    if isinstance(d, (int, float)):
        try:
            return datetime.fromtimestamp(d / 1000).date()
        except (ValueError, OSError):
            return None
    try:
        return datetime.strptime(str(d).split("T")[0], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        try:
            return datetime.strptime(str(d), "%m/%d/%Y").date()
        except (ValueError, TypeError):
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
        "OH",
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
# Salesforce Visualforce Remoting
# ---------------------------------------------------------------------------

class OhioElicenseClient:
    """Client for Ohio elicense.ohio.gov Visualforce Remoting API."""

    def __init__(self):
        self.client = httpx.Client(
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0"},
            timeout=httpx.Timeout(60.0, connect=30.0),
        )
        self.vid = None
        self.methods = {}
        self.tid = 0

    def init_session(self):
        """Fetch the verify license page and extract VF remoting tokens."""
        log("  Initializing elicense.ohio.gov session...")
        resp = self.client.get(VERIFY_PAGE)
        resp.raise_for_status()
        html = resp.text

        config_match = re.search(r'new \$VFRM\.RemotingProviderImpl\(({.*?})\)\)', html)
        if not config_match:
            raise RuntimeError("Could not extract VF remoting config from page")

        config = json.loads(config_match.group(1))
        self.vid = config["vf"]["vid"]

        methods = config["actions"]["OH_VerifyLicenseCtlr"]["ms"]
        self.methods = {m["name"]: m for m in methods}
        log(f"  Session initialized: vid={self.vid}, methods={list(self.methods.keys())}")

    def _call_remote(self, method_name, data):
        """Call a VF remoting method."""
        self.tid += 1
        method_info = self.methods[method_name]

        payload = {
            "action": "OH_VerifyLicenseCtlr",
            "method": method_name,
            "data": data,
            "type": "rpc",
            "tid": self.tid,
            "ctx": {
                "csrf": method_info["csrf"],
                "ns": "",
                "ver": method_info.get("ver", 41.0),
                "vid": self.vid,
                "authorization": method_info.get("authorization", ""),
            },
        }

        resp = self.client.post(
            REMOTE_URL,
            content=json.dumps(payload),
            headers={
                "Content-Type": "application/json",
                "Referer": VERIFY_PAGE,
            },
        )
        result = resp.json()
        if not result or result[0].get("statusCode") != 200:
            msg = result[0].get("message", "Unknown error") if result else "Empty response"
            raise RuntimeError(f"VF remoting error: {msg}")

        return result[0].get("result")

    def search_individual(self, last_name, board="", city=""):
        """Search for individual licensees by last name."""
        sffields = {
            "firstName": "", "lastName": last_name, "middleName": "", "contactAlias": "",
            "board": board, "licenseType": "", "licenseNumber": "",
            "city": city, "state": "", "county": "",
            "businessBoard": "", "businessLicenseType": "", "businessLicenseNumber": "",
            "businessCity": "", "businessState": "", "businessCounty": "",
            "businessName": "", "dbafileld": "",
            "searchType": "individual",
        }
        return self._call_remote("findLicensesForOwner", [sffields])

    def search_business(self, business_name, board="", city=""):
        """Search for business licensees by name."""
        sffields = {
            "firstName": "", "lastName": "", "middleName": "", "contactAlias": "",
            "board": "", "licenseType": "", "licenseNumber": "",
            "city": "", "state": "", "county": "",
            "businessBoard": board, "businessLicenseType": "", "businessLicenseNumber": "",
            "businessCity": city, "businessState": "", "businessCounty": "",
            "businessName": business_name, "dbafileld": "",
            "searchType": "business",
        }
        return self._call_remote("findLicensesForOwner", [sffields])

    def close(self):
        self.client.close()


def extract_license_data(result_item):
    """Extract license fields from an elicense.ohio.gov search result item."""
    lic_data = result_item.get("license", {})
    if isinstance(lic_data, dict) and "v" in lic_data:
        lic = lic_data["v"]
    else:
        lic = lic_data

    lic_num = lic.get("Name", "")
    if not lic_num:
        return None

    applicant_name = result_item.get("Applicant", "")
    board = result_item.get("Board", "")
    city = result_item.get("City", "")
    county = result_item.get("County", "")

    # Address from license or parcels
    address = lic.get("Public_Street_Address__c") or lic.get("Applicant_Street_Address__c")
    zip_code = lic.get("Applicant_Zip_Code__c", "")
    state = lic.get("Applicant_State__c", "OH")
    status = lic.get("MUSW__Status__c", "")
    lic_type = lic.get("MUSW__Type__c", "")
    is_business = lic.get("Business_License__c", False)

    issue_date = lic.get("MUSW__Issue_Date__c")
    expiry_date = lic.get("MUSW__Expiration_Date__c")
    effective_date = lic.get("Effective_Date__c")

    # Qualifiers/endorsements
    qualifiers = lic.get("MUSW__License2_Parcels__r") or []
    endorsements = lic.get("License_Qualifiers__r") or []
    endorse_strs = []
    for eq in (endorsements if isinstance(endorsements, list) else []):
        ev = eq.get("v", eq) if isinstance(eq, dict) else {}
        qt = ev.get("Qualifier_Type__c") or ev.get("MUSW__Qualifier_Type__c", "")
        if qt:
            endorse_strs.append(qt)

    classifications = f"{board}: {lic_type}"
    if endorse_strs:
        classifications += f" ({', '.join(endorse_strs)})"

    return {
        "license_number": lic_num,
        "business_name": applicant_name or "Unknown",
        "full_business_name": lic.get("Licensee_Name__c") or lic.get("Applicant_Full_Name__c"),
        "address": address,
        "city": city,
        "zip": zip_code,
        "county": county,
        "business_type": "Business" if is_business else "Individual",
        "issue_date": issue_date or effective_date,
        "expiration_date": expiry_date,
        "status": status,
        "classifications": classifications,
    }


# ---------------------------------------------------------------------------
# Search strategies
# ---------------------------------------------------------------------------

def generate_search_prefixes():
    """Generate 2-letter last name prefixes for exhaustive search."""
    for c1 in string.ascii_uppercase:
        for c2 in string.ascii_uppercase:
            yield f"{c1}{c2}"


def load_oh_licenses(conn, elicense, boards, dry_run=False, search_type="individual"):
    """Load Ohio licenses by iterating through name prefixes."""
    log("=== Ohio eLicense Professional Licenses ===")
    grand_total = 0
    seen = set()
    batch = []
    errors = 0

    board_filter = "" if boards == ALL_BOARDS_SENTINEL else ""

    if boards != ALL_BOARDS_SENTINEL:
        board_list = boards
    else:
        board_list = [""]  # Empty string = all boards

    for board in board_list:
        board_label = board or "All Boards"
        log(f"  Board: {board_label}")
        board_total = 0

        if search_type == "individual":
            prefixes = list(generate_search_prefixes())
        else:
            # For business search, use single letters + common construction words
            prefixes = list(string.ascii_uppercase) + [
                "Construction", "Plumbing", "Electric", "HVAC", "Mechanical",
                "Heating", "Cooling", "Roofing", "Building", "Contractor",
            ]

        for prefix in prefixes:
            try:
                if search_type == "individual":
                    result = elicense.search_individual(prefix, board=board)
                else:
                    result = elicense.search_business(prefix, board=board)
            except RuntimeError as e:
                log(f"    ERROR on prefix {prefix}: {e}")
                errors += 1
                if errors > 20:
                    log("    Too many errors, re-initializing session...")
                    elicense.init_session()
                    errors = 0
                time.sleep(2)
                continue
            except Exception as e:
                log(f"    UNEXPECTED ERROR on prefix {prefix}: {e}")
                errors += 1
                time.sleep(2)
                continue

            # Parse results
            items = []
            if isinstance(result, dict) and "v" in result:
                items = result["v"]
            elif isinstance(result, list):
                items = result

            new_count = 0
            for item in items:
                extracted = extract_license_data(item)
                if not extracted:
                    continue

                lic_num = extracted.pop("license_number")
                if not lic_num or lic_num in seen:
                    continue
                seen.add(lic_num)
                new_count += 1

                batch.append(make_row(lic_num, SOURCE, **extracted))

                if len(batch) >= BATCH_SIZE:
                    if dry_run:
                        log(f"    [DRY RUN] would upsert {len(batch)} (total seen: {len(seen)})")
                        board_total += len(batch)
                    else:
                        n = upsert_batch(conn, batch)
                        board_total += n
                        log(f"    upserted {n} (running: {board_total}, total seen: {len(seen)})")
                    batch = []

            if new_count > 0:
                log(f"    {prefix}: {len(items)} results, {new_count} new")

            # Rate limit - be respectful to Salesforce
            time.sleep(0.8)

        # Flush remaining batch for this board
        if batch:
            if dry_run:
                log(f"    [DRY RUN] would upsert {len(batch)}")
                board_total += len(batch)
            else:
                n = upsert_batch(conn, batch)
                board_total += n
            batch = []

        log(f"  {board_label}: {board_total:,} records")
        grand_total += board_total

    log(f"OH eLicense total: {grand_total:,} unique records")
    return grand_total


def main():
    parser = argparse.ArgumentParser(description="Load Ohio eLicense professional licenses")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write to DB")
    parser.add_argument("--board", help="Search only one board (e.g. 'Architects Board')")
    parser.add_argument("--all-boards", action="store_true",
                        help="Scrape ALL boards (not just construction-related)")
    parser.add_argument("--search-type", choices=["individual", "business"], default="individual",
                        help="Search by individual names or business names")
    args = parser.parse_args()

    log("Ohio eLicense Professional License Loader")
    log(f"Database: {args.db_host}:{DB_PORT}/{DB_NAME}")
    if args.dry_run:
        log("*** DRY RUN MODE ***")

    conn = None
    if not args.dry_run:
        conn = get_conn(args.db_host)

    elicense = OhioElicenseClient()

    try:
        elicense.init_session()

        if args.board:
            boards = [args.board]
        elif args.all_boards:
            boards = ALL_BOARDS_SENTINEL
        else:
            boards = TARGET_BOARDS

        total = load_oh_licenses(conn, elicense, boards, dry_run=args.dry_run,
                                 search_type=args.search_type)

        if conn and not args.dry_run:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    split_part(classifications, ':', 1) as board,
                    count(*)
                FROM contractor_licenses
                WHERE state = 'OH' AND source = %s
                GROUP BY 1
                ORDER BY 2 DESC
            """, (SOURCE,))
            log("--- Summary by Board ---")
            for row in cur.fetchall():
                log(f"  {row[0]}: {row[1]:,}")
            cur.close()

        log(f"DONE — {total:,} records {'would be' if args.dry_run else ''} upserted this run")

    except Exception as e:
        log(f"FATAL: {e}")
        raise
    finally:
        elicense.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
