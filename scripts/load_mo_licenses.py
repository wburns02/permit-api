#!/usr/bin/env python3
"""
Missouri Division of Professional Registration License Loader

Source: pr.mo.gov licensee search (ASP classic / POST form)
Method: POST form scraping with pagination

Covers: Electricians (Electrical Contractors Board), Plumbers (Plumbing Industry
        Board / State Board of Plumbing Examiners), HVAC contractors.

NOTE: Missouri does NOT have a Socrata open data portal for professional licenses.
The Division of Professional Registration runs pr.mo.gov with a web search form.
Most records have business/company names but very few (0.4%) have owner names.

Strategy: Iterate through board types, then paginate through letter-based search
results (A-Z company name prefixes, plus 0-9) to harvest all licensees.

Loads into: contractor_licenses table (existing schema)
Unique key: (license_number, state)

Usage:
    python3 -u load_mo_licenses.py --db-host 100.122.216.15
    python3 -u load_mo_licenses.py --dry-run
    python3 -u load_mo_licenses.py --board "Electrical Contractors"

Cron (weekly Monday 3:30 AM):
    30 3 * * 1 python3 -u /home/will/permit-api-live/scripts/load_mo_licenses.py --db-host 100.122.216.15 >> /var/log/mo_licenses.log 2>&1
"""

import argparse
import os
import re
import string
import sys
import time
import uuid
from datetime import date, datetime
from html import unescape
from urllib.parse import urljoin

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

BASE_URL = "https://pr.mo.gov"
SEARCH_URL = f"{BASE_URL}/licensee-search.asp"
SOURCE = "mo_dpr"

# Construction-related boards on pr.mo.gov
BOARDS = {
    "Electrical Contractors": {
        "board_id": "2087",
        "types": [
            "Electrical Contractor",
            "Master Electrician",
            "Journeyman Electrician",
            "Residential Electrician",
        ],
    },
    "Plumbing": {
        "board_id": "2101",
        "types": [
            "Master Plumber",
            "Journeyman Plumber",
            "Plumbing Contractor",
        ],
    },
    "HVAC": {
        "board_id": "2094",
        "types": [
            "HVAC Contractor",
            "Mechanical Contractor",
        ],
    },
    "Architects": {
        "board_id": "2080",
        "types": [
            "Architect",
        ],
    },
    "Engineers": {
        "board_id": "2091",
        "types": [
            "Professional Engineer",
        ],
    },
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
    s = unescape(str(s)).strip()
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
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(str(d).strip().split("T")[0].split(" ")[0], fmt).date()
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
        "MO",
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
# Missouri pr.mo.gov scraper
# ---------------------------------------------------------------------------

class MissouriLicenseClient:
    """Client for Missouri pr.mo.gov licensee search."""

    def __init__(self):
        self.client = httpx.Client(
            follow_redirects=True,
            verify=False,  # MO cert chain sometimes has issues
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=httpx.Timeout(60.0, connect=30.0),
        )

    def search(self, board_id="", last_name="", first_name="", license_number="",
               city="", state="", page=1):
        """Search Missouri licensee database."""
        form_data = {
            "board": board_id,
            "lastname": last_name,
            "firstname": first_name,
            "licensenumber": license_number,
            "city": city,
            "state": state,
            "page": str(page),
            "submit": "Search",
        }
        resp = self.client.post(
            SEARCH_URL,
            data=form_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=60,
        )
        return resp.text

    def get_detail(self, detail_url):
        """Fetch a license detail page."""
        url = urljoin(BASE_URL, detail_url)
        resp = self.client.get(url, timeout=30)
        return resp.text

    def close(self):
        self.client.close()


def parse_search_results(html):
    """Parse Missouri licensee search results."""
    results = []

    # Look for result table rows
    # Typical format: table with columns for Name, License #, Board, Type, Status, City, State
    table_match = re.search(
        r'<table[^>]*class="[^"]*(?:results|listing|data|search)[^"]*"[^>]*>(.*?)</table>',
        html, re.DOTALL | re.IGNORECASE,
    )
    if not table_match:
        # Try finding any table after "Search Results" heading
        table_match = re.search(
            r'(?:results|found|records)(.*?<table[^>]*>)(.*?)</table>',
            html, re.DOTALL | re.IGNORECASE,
        )
        if table_match:
            table_html = table_match.group(2)
        else:
            # Try the broadest pattern — any data table
            tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL)
            table_html = max(tables, key=len) if tables else ""
    else:
        table_html = table_match.group(1)

    if not table_html:
        return results

    # Parse rows
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)
    for row_html in rows[1:]:  # Skip header
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.DOTALL)
        if len(cells) < 3:
            continue

        # Extract detail link if present
        detail_link = None
        link_match = re.search(r'href="([^"]*)"', row_html)
        if link_match:
            detail_link = link_match.group(1)

        # Clean cells
        cells = [re.sub(r'<[^>]+>', ' ', c).strip() for c in cells]
        cells = [re.sub(r'\s+', ' ', unescape(c)).strip() for c in cells]

        record = {"detail_url": detail_link}

        # Map columns based on count
        if len(cells) >= 7:
            record["name"] = cells[0]
            record["license_number"] = cells[1]
            record["board"] = cells[2]
            record["license_type"] = cells[3]
            record["status"] = cells[4]
            record["city"] = cells[5]
            record["state"] = cells[6]
        elif len(cells) >= 5:
            record["name"] = cells[0]
            record["license_number"] = cells[1]
            record["license_type"] = cells[2]
            record["status"] = cells[3]
            record["city"] = cells[4] if len(cells) > 4 else None
        elif len(cells) >= 3:
            record["name"] = cells[0]
            record["license_number"] = cells[1]
            record["status"] = cells[2]

        if record.get("license_number"):
            results.append(record)

    return results


def parse_detail_page(html):
    """Parse a Missouri license detail page for additional fields."""
    detail = {}

    def extract_field(label):
        pattern = re.compile(
            rf'(?:<b>|<strong>|<th[^>]*>)\s*{re.escape(label)}\s*(?:</b>|</strong>|</th>)'
            rf'\s*(?:</td>\s*<td[^>]*>|:?\s*)(.*?)(?:</td>|<br|</p>)',
            re.DOTALL | re.IGNORECASE,
        )
        m = pattern.search(html)
        if m:
            val = re.sub(r'<[^>]+>', ' ', m.group(1)).strip()
            val = re.sub(r'\s+', ' ', val)
            return unescape(val) if val else None
        return None

    detail["name"] = extract_field("Name") or extract_field("Licensee")
    detail["business_name"] = extract_field("Business") or extract_field("Company") or extract_field("DBA")
    detail["address"] = extract_field("Address")
    detail["city"] = extract_field("City")
    detail["state"] = extract_field("State")
    detail["zip"] = extract_field("Zip") or extract_field("Zip Code")
    detail["phone"] = extract_field("Phone")
    detail["license_type"] = extract_field("License Type") or extract_field("Type")
    detail["issue_date"] = extract_field("Issue Date") or extract_field("Original Issue")
    detail["expiration_date"] = extract_field("Expiration") or extract_field("Expiration Date")
    detail["status"] = extract_field("Status")
    detail["board"] = extract_field("Board")

    return detail


def get_total_pages(html):
    """Extract total pages from pagination."""
    # Look for "Page X of Y" or pagination links
    page_match = re.search(r'Page\s+\d+\s+of\s+(\d+)', html, re.IGNORECASE)
    if page_match:
        return int(page_match.group(1))

    # Look for last page link
    pages = re.findall(r'page=(\d+)', html)
    if pages:
        return max(int(p) for p in pages)

    return 1


# ---------------------------------------------------------------------------
# Search iteration
# ---------------------------------------------------------------------------

def load_mo_licenses(conn, mo_client, boards, dry_run=False, fetch_details=False):
    """Load Missouri licenses by iterating through boards and name prefixes."""
    log("=== Missouri Division of Professional Registration ===")
    grand_total = 0
    seen = set()
    batch = []
    errors = 0

    for board_name, board_info in boards.items():
        board_id = board_info["board_id"]
        board_types = board_info["types"]
        log(f"  Board: {board_name} (ID: {board_id})")
        board_total = 0

        # Iterate A-Z + 0-9 for last name prefixes
        prefixes = list(string.ascii_uppercase) + [str(d) for d in range(10)]

        for prefix in prefixes:
            page = 1
            while True:
                try:
                    html = mo_client.search(board_id=board_id, last_name=prefix, page=page)
                    results = parse_search_results(html)
                except Exception as e:
                    log(f"    ERROR board={board_name} prefix={prefix} page={page}: {e}")
                    errors += 1
                    if errors > 30:
                        log("    Too many errors, pausing 30s...")
                        time.sleep(30)
                        errors = 0
                    break

                if not results:
                    break

                new_count = 0
                for rec in results:
                    lic_num = rec.get("license_number", "").strip()
                    if not lic_num or lic_num in seen:
                        continue
                    seen.add(lic_num)
                    new_count += 1

                    # Optionally fetch detail page for more info
                    detail = {}
                    if fetch_details and rec.get("detail_url"):
                        try:
                            detail_html = mo_client.get_detail(rec["detail_url"])
                            detail = parse_detail_page(detail_html)
                            time.sleep(0.3)
                        except Exception:
                            pass

                    name = detail.get("name") or rec.get("name", "")
                    biz_name = detail.get("business_name")
                    lic_type = detail.get("license_type") or rec.get("license_type", "")
                    classification = f"{board_name}: {lic_type}" if lic_type else board_name

                    batch.append(make_row(lic_num, SOURCE,
                        business_name=biz_name or name,
                        full_business_name=name if biz_name else None,
                        address=detail.get("address"),
                        city=detail.get("city") or rec.get("city"),
                        zip=detail.get("zip"),
                        phone=detail.get("phone"),
                        issue_date=detail.get("issue_date"),
                        expiration_date=detail.get("expiration_date"),
                        status=detail.get("status") or rec.get("status"),
                        classifications=classification,
                        business_type="Business" if biz_name else "Individual",
                    ))

                if new_count > 0:
                    log(f"    {board_name} prefix={prefix} page={page}: {len(results)} results, {new_count} new")

                if len(batch) >= BATCH_SIZE:
                    if dry_run:
                        log(f"    [DRY RUN] would upsert {len(batch)} (total seen: {len(seen)})")
                        grand_total += len(batch)
                    else:
                        n = upsert_batch(conn, batch)
                        grand_total += n
                        log(f"    upserted {n} (running: {grand_total})")
                    batch = []

                # Check if there are more pages
                total_pages = get_total_pages(html)
                if page >= total_pages:
                    break
                page += 1
                time.sleep(0.5)

            # If single-letter prefix returns too many, expand to 2-letter
            if results and len(results) >= 500:
                log(f"    Expanding prefix {prefix} to 2-letter...")
                for c2 in string.ascii_lowercase:
                    sub_prefix = prefix + c2
                    sub_page = 1
                    while True:
                        try:
                            sub_html = mo_client.search(board_id=board_id, last_name=sub_prefix, page=sub_page)
                            sub_results = parse_search_results(sub_html)
                        except Exception:
                            break

                        if not sub_results:
                            break

                        for rec in sub_results:
                            lic_num = rec.get("license_number", "").strip()
                            if not lic_num or lic_num in seen:
                                continue
                            seen.add(lic_num)

                            name = rec.get("name", "")
                            lic_type = rec.get("license_type", "")
                            classification = f"{board_name}: {lic_type}" if lic_type else board_name

                            batch.append(make_row(lic_num, SOURCE,
                                business_name=name,
                                city=rec.get("city"),
                                status=rec.get("status"),
                                classifications=classification,
                                business_type="Individual",
                            ))

                        sub_total_pages = get_total_pages(sub_html)
                        if sub_page >= sub_total_pages:
                            break
                        sub_page += 1
                        time.sleep(0.3)

            time.sleep(0.5)

        # Flush per board
        if batch:
            if dry_run:
                log(f"    [DRY RUN] would upsert {len(batch)}")
                board_total += len(batch)
                grand_total += len(batch)
            else:
                n = upsert_batch(conn, batch)
                board_total += n
                grand_total += n
            batch = []

        log(f"  {board_name}: {board_total:,} records")

    log(f"MO DPR total: {grand_total:,} unique records (seen: {len(seen):,})")
    return grand_total


def main():
    parser = argparse.ArgumentParser(description="Load Missouri Division of Professional Registration licenses")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write to DB")
    parser.add_argument("--board", help="Load only one board (e.g. 'Electrical Contractors')")
    parser.add_argument("--fetch-details", action="store_true",
                        help="Fetch detail pages for extra data (slower)")
    args = parser.parse_args()

    log("Missouri Division of Professional Registration License Loader")
    log(f"Database: {args.db_host}:{DB_PORT}/{DB_NAME}")
    if args.dry_run:
        log("*** DRY RUN MODE ***")

    conn = None
    if not args.dry_run:
        conn = get_conn(args.db_host)

    mo_client = MissouriLicenseClient()

    try:
        if args.board:
            if args.board not in BOARDS:
                log(f"Unknown board: {args.board}. Available: {list(BOARDS.keys())}")
                sys.exit(1)
            boards = {args.board: BOARDS[args.board]}
        else:
            boards = BOARDS

        total = load_mo_licenses(conn, mo_client, boards, dry_run=args.dry_run,
                                 fetch_details=args.fetch_details)

        if conn and not args.dry_run:
            cur = conn.cursor()
            cur.execute("""
                SELECT classifications, count(*)
                FROM contractor_licenses
                WHERE state = 'MO' AND source = %s
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
        mo_client.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
