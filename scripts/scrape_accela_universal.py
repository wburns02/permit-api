#!/usr/bin/env python3
"""
Accela Citizen Access Universal Permit Scraper

Scrapes building permits from any Accela-powered city portal using the
GlobalSearchResults endpoint (no API key or authentication required).

Confirmed working with ~35+ US cities on aca-prod.accela.com.

Usage:
    python3 scrape_accela_universal.py                    # All known cities
    python3 scrape_accela_universal.py --city DENVER      # Single city
    python3 scrape_accela_universal.py --city DENVER SACRAMENTO  # Multiple cities
    python3 scrape_accela_universal.py --list             # List known cities
    python3 scrape_accela_universal.py --dry-run          # Don't write to DB
    python3 scrape_accela_universal.py --pages 5          # Max pages per city (default 5)

API Pattern Discovered:
    GET https://aca-prod.accela.com/{CITY_CODE}/Cap/GlobalSearchResults.aspx
        ?isNewQuery=yes&QueryText=permit
        &pg={page_number}

    Returns HTML with a table of permits. No auth, no API key.
    10 results per page, pagination via &pg= parameter.

Cron (daily):
    0 6 * * * cd /home/will/permit-api && python3 scripts/scrape_accela_universal.py >> /tmp/accela_daily.log 2>&1
"""

import argparse
import json
import os
import re
import sys
import time
import uuid
from datetime import datetime, date
from urllib.parse import quote

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("pip install psycopg2-binary")
    sys.exit(1)

try:
    import httpx
except ImportError:
    print("pip install httpx")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("pip install beautifulsoup4")
    sys.exit(1)

# ── Config ─────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

BASE_URL = "https://aca-prod.accela.com"
RATE_LIMIT_SECONDS = 2     # Between pages
CITY_PAUSE_SECONDS = 3     # Between cities
DEFAULT_QUERY = "permit"   # Search term — broad enough to catch all permit types
DEFAULT_MAX_PAGES = 5      # 5 pages = 50 permits per city per run

# ── Known working Accela city portals ──────────────────────────────────────
# Discovered via brute-force testing of city codes on aca-prod.accela.com
# Format: CODE -> (city_name, state)
ACCELA_CITIES = {
    # California
    "SACRAMENTO":   ("Sacramento", "CA"),
    "SANLEANDRO":   ("San Leandro", "CA"),
    "SANDIEGO":     ("San Diego", "CA"),
    "OAKLAND":      ("Oakland", "CA"),
    "STOCKTON":     ("Stockton", "CA"),
    "FONTANA":      ("Fontana", "CA"),
    "SANTACLARITA": ("Santa Clarita", "CA"),
    "TORRANCE":     ("Torrance", "CA"),
    "SANTAROSA":    ("Santa Rosa", "CA"),
    "RICHMOND":     ("Richmond", "CA"),
    "VISALIA":      ("Visalia", "CA"),
    "CONCORD":      ("Concord", "CA"),
    "ANAHEIM":      ("Anaheim", "CA"),
    "SANTAANA":     ("Santa Ana", "CA"),
    # Colorado
    "DENVER":       ("Denver", "CO"),
    # Texas
    "ELPASO":       ("El Paso", "TX"),
    # Indiana
    "INDY":         ("Indianapolis", "IN"),
    # Tennessee
    "KNOXVILLE":    ("Knoxville", "TN"),
    # North Carolina
    "CHARLOTTE":    ("Charlotte", "NC"),
    # Oregon
    "HILLSBORO":    ("Hillsboro", "OR"),
    # Washington
    "TACOMA":       ("Tacoma", "WA"),
    # Arizona
    "MESA":         ("Mesa", "AZ"),
    "CHANDLER":     ("Chandler", "AZ"),
    "SCOTTSDALE":   ("Scottsdale", "AZ"),
    "AVONDALE":     ("Avondale", "AZ"),
    "GOODYEAR":     ("Goodyear", "AZ"),
    # Nevada
    "RENO":         ("Reno", "NV"),
    # Florida
    "TAMPA":        ("Tampa", "FL"),
    "CLEARWATER":   ("Clearwater", "FL"),
    # Missouri (SLC code is St. Louis County)
    "SLC":          ("St. Louis County", "MO"),
    # Nebraska
    "OMAHA":        ("Omaha", "NE"),
    # Ohio
    "COLUMBUS":     ("Columbus", "OH"),
    "CINCINNATI":   ("Cincinnati", "OH"),
    # Maryland
    "BALTIMORE":    ("Baltimore", "MD"),
    # Alaska
    "ANCHORAGE":    ("Anchorage", "AK"),
}


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def make_session(city_code):
    """Create an HTTP client that looks like a browser."""
    session = httpx.Client(
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": f"{BASE_URL}/{city_code}/Default.aspx",
        },
        timeout=30,
        follow_redirects=True,
    )
    # Warm up the session to get cookies
    try:
        session.get(f"{BASE_URL}/{city_code}/Default.aspx")
    except Exception:
        pass
    return session


def parse_permits_from_html(html, city_code, city_name, state):
    """
    Parse Accela GlobalSearchResults HTML to extract permit records.

    The page structure has deeply nested tables. We find all <tr> elements,
    identify the header row by looking for rows where cells contain exactly
    the header labels (Date, Record Number, Record Type, etc.) as direct
    cell text — NOT nested in child elements.

    The actual header row looks like:
      Row N: ['Date', 'Record Number', 'Record Type', 'Module', ...]
    Data rows follow immediately with date pattern MM/DD/YYYY in first cell.
    """
    soup = BeautifulSoup(html, "html.parser")
    permits = []

    # Find the header row — it's the row with EXACTLY these cell texts
    # (not parent rows that contain nested tables with these values)
    all_rows = soup.find_all("tr")
    header_row_idx = -1
    header_texts = []

    for idx, row in enumerate(all_rows):
        # Only look at direct td/th children — not nested ones
        cells = row.find_all(["th", "td"], recursive=False)
        if not cells:
            # No direct td/th; check if the tr has nested structure
            # The actual header has direct td children
            cells = row.find_all(["th", "td"])

        texts = [c.get_text(strip=True) for c in cells]

        # Header row: starts with "Date" and has "Record Number" right after
        if (len(texts) >= 4 and texts[0] == "Date" and
                "Record Number" in texts and "Record Type" in texts):
            header_row_idx = idx
            header_texts = texts
            break

    if header_row_idx < 0:
        return permits  # No permit table found (city may use different structure)

    # Determine column positions from header
    col_map = {}
    for i, h in enumerate(header_texts):
        col_map[h] = i

    date_idx = col_map.get("Date", 0)
    number_idx = col_map.get("Record Number", 1)
    type_idx = col_map.get("Record Type", 2)
    module_idx = col_map.get("Module", 3)
    notes_idx = col_map.get("Short Notes", 4)
    name_idx = col_map.get("Project Name", 5)
    # Sacramento has "Address" as column 6, Status varies
    addr_idx = col_map.get("Address", -1)
    status_idx = col_map.get("Status", len(header_texts) - 1)

    # Extract data rows
    for row in all_rows[header_row_idx + 1:]:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        texts = [c.get_text(strip=True) for c in cells]

        # Data rows start with a date in MM/DD/YYYY format
        if not re.match(r"\d{2}/\d{2}/\d{4}", texts[0] if texts else ""):
            continue

        def get_col(idx, default=""):
            return texts[idx] if 0 <= idx < len(texts) else default

        issue_date_str = get_col(date_idx)
        permit_number = get_col(number_idx)

        if not permit_number:
            continue

        # Parse date
        issue_date = None
        try:
            issue_date = datetime.strptime(issue_date_str, "%m/%d/%Y").date()
        except ValueError:
            pass

        # Get detail URL for this record
        record_link = row.find("a", href=re.compile(r"Cap", re.IGNORECASE))
        detail_href = record_link.get("href", "") if record_link else ""
        detail_url = (BASE_URL + detail_href) if detail_href.startswith("/") else detail_href

        # Address handling — some cities include it inline, some don't
        raw_address = get_col(addr_idx) if addr_idx >= 0 else ""
        # Clean up duplicate addresses (some portals repeat "123 Main St, City, ST 12345 123 Main St")
        if raw_address:
            # Remove trailing duplicate — the first occurrence of the address
            parts = raw_address.split(" ", 1)
            if parts:
                addr_clean = raw_address.split("  ")[0].strip()
            else:
                addr_clean = raw_address.strip()
        else:
            addr_clean = ""

        description = get_col(notes_idx)
        project_name = get_col(name_idx)

        # Combine description and project_name for maximum context
        full_desc = description or project_name or ""
        if description and project_name and description != project_name:
            full_desc = f"{project_name} — {description}"

        # Use permit type to infer module if module is blank
        permit_type = get_col(type_idx)
        module = get_col(module_idx)
        status = get_col(status_idx)

        permit = {
            "permit_number": permit_number[:100],
            "permit_type": permit_type[:100] if permit_type else None,
            "work_class": module[:100] if module else None,
            "description": full_desc[:500] if full_desc else None,
            "address": addr_clean[:200] if addr_clean else None,
            "city": city_name,
            "state": state,
            "zip": None,
            "issue_date": issue_date,
            "status": status[:100] if status else None,
            "jurisdiction": f"{city_name}, {state}",
            "source": f"accela_{city_code.lower()}",
            "detail_url": detail_url,
        }

        permits.append(permit)

    return permits


def get_total_results(html):
    """Extract total result count from search page HTML."""
    match = re.search(r"Showing\s+\d+-\d+\s+of\s+([\d,+]+)", html)
    if match:
        total_str = match.group(1).replace(",", "").replace("+", "")
        try:
            return int(total_str)
        except ValueError:
            return None
    return None


def scrape_city(city_code, city_name, state, query=DEFAULT_QUERY, max_pages=DEFAULT_MAX_PAGES):
    """
    Scrape permits from a single Accela city portal.

    Returns list of normalized permit dicts.
    """
    all_permits = []
    session = make_session(city_code)

    for page_num in range(1, max_pages + 1):
        if page_num == 1:
            url = (f"{BASE_URL}/{city_code}/Cap/GlobalSearchResults.aspx"
                   f"?isNewQuery=yes&QueryText={quote(query)}")
        else:
            url = (f"{BASE_URL}/{city_code}/Cap/GlobalSearchResults.aspx"
                   f"?QueryText={quote(query)}&pg={page_num}")

        log(f"  [{city_code}] Page {page_num}: {url}")

        try:
            r = session.get(url)
        except Exception as e:
            log(f"  [{city_code}] Request error: {e}")
            break

        if r.status_code == 404:
            log(f"  [{city_code}] 404 — city portal not found")
            break

        if r.status_code == 503:
            log(f"  [{city_code}] 503 — portal temporarily unavailable")
            break

        if "Error.aspx" in str(r.url):
            log(f"  [{city_code}] Redirected to error page")
            break

        if r.status_code != 200:
            log(f"  [{city_code}] HTTP {r.status_code}")
            break

        page_permits = parse_permits_from_html(r.text, city_code, city_name, state)
        log(f"  [{city_code}] Page {page_num}: {len(page_permits)} permits")

        if not page_permits:
            break

        all_permits.extend(page_permits)

        # Log total on first page
        if page_num == 1:
            total = get_total_results(r.text)
            if total:
                log(f"  [{city_code}] Total available: {total}")

        # Stop if no Next > pagination (appears as "Next &gt;" or "Next >" in HTML)
        if "Next &gt;" not in r.text and "Next >" not in r.text:
            log(f"  [{city_code}] Last page reached")
            break

        time.sleep(RATE_LIMIT_SECONDS)

    return all_permits


def load_to_hot_leads(conn, permits):
    """Batch upsert permits into hot_leads table."""
    if not permits:
        return 0

    # Deduplicate by (permit_number, source) within batch — keep last occurrence
    deduped = {}
    for p in permits:
        key = (p.get("permit_number"), p.get("source"))
        deduped[key] = p
    permits = list(deduped.values())

    cur = conn.cursor()
    batch = []
    for p in permits:
        batch.append((
            str(uuid.uuid4()),
            p.get("permit_number"),
            p.get("permit_type"),
            p.get("work_class"),
            p.get("description"),
            p.get("address"),
            p.get("city"),
            p.get("state", "XX"),
            p.get("zip"),
            None,      # valuation
            None,      # sqft
            p.get("issue_date"),
            p.get("status"),
            None,      # contractor_company
            None,      # contractor_name
            None,      # contractor_phone
            None,      # applicant_name
            None,      # applicant_phone
            p.get("jurisdiction"),
            p.get("source"),
        ))

    sql = """
        INSERT INTO hot_leads (
            id, permit_number, permit_type, work_class, description,
            address, city, state, zip, valuation, sqft, issue_date,
            status,
            contractor_company, contractor_name, contractor_phone,
            applicant_name, applicant_phone, jurisdiction, source
        ) VALUES %s
        ON CONFLICT (permit_number, source)
        DO UPDATE SET
            issue_date = COALESCE(EXCLUDED.issue_date, hot_leads.issue_date),
            description = COALESCE(EXCLUDED.description, hot_leads.description),
            address = COALESCE(EXCLUDED.address, hot_leads.address),
            status = COALESCE(EXCLUDED.status, hot_leads.status),
            scraped_at = CURRENT_DATE
    """

    try:
        execute_values(cur, sql, batch, page_size=500)
        conn.commit()
        return len(batch)
    except Exception as e:
        conn.rollback()
        log(f"  DB insert error: {e}")
        return 0
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser(
        description="Universal Accela Citizen Access permit scraper"
    )
    parser.add_argument(
        "--city", nargs="+", metavar="CODE",
        help="City code(s) to scrape (e.g. DENVER SACRAMENTO). Default: all known cities."
    )
    parser.add_argument(
        "--query", default=DEFAULT_QUERY,
        help=f"Search query (default: '{DEFAULT_QUERY}')"
    )
    parser.add_argument(
        "--pages", type=int, default=DEFAULT_MAX_PAGES,
        help=f"Max pages per city (default: {DEFAULT_MAX_PAGES}, 10 permits/page)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Scrape but don't write to database"
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List all known city codes and exit"
    )
    args = parser.parse_args()

    if args.list:
        print(f"\n{'Code':<16} {'City':<25} State")
        print("-" * 50)
        for code, (city, state) in sorted(ACCELA_CITIES.items(), key=lambda x: (x[1][1], x[1][0])):
            print(f"{code:<16} {city:<25} {state}")
        print(f"\nTotal: {len(ACCELA_CITIES)} cities")
        return

    # Determine which cities to scrape
    if args.city:
        cities_to_scrape = {}
        for code in args.city:
            code_upper = code.upper()
            if code_upper in ACCELA_CITIES:
                cities_to_scrape[code_upper] = ACCELA_CITIES[code_upper]
            else:
                log(f"WARNING: Unknown city code '{code_upper}' — skipping")
        if not cities_to_scrape:
            log("No valid cities specified. Run with --list to see available cities.")
            sys.exit(1)
    else:
        cities_to_scrape = ACCELA_CITIES

    log("=" * 60)
    log("ACCELA UNIVERSAL SCRAPER")
    log(f"Cities: {len(cities_to_scrape)}, Pages/city: {args.pages}, Query: '{args.query}'")
    log(f"Dry run: {args.dry_run}")
    log("=" * 60)

    conn = None
    if not args.dry_run:
        try:
            conn = get_conn()
            log("Connected to database")
        except Exception as e:
            log(f"DB connection failed: {e}")
            log("Running in dry-run mode (no DB writes)")
            conn = None

    total_scraped = 0
    total_loaded = 0
    city_results = {}

    for city_code, (city_name, state) in cities_to_scrape.items():
        log(f"\n{'─'*50}")
        log(f"Scraping {city_name}, {state} [{city_code}]")

        permits = scrape_city(
            city_code=city_code,
            city_name=city_name,
            state=state,
            query=args.query,
            max_pages=args.pages,
        )

        log(f"  Scraped {len(permits)} permits from {city_name}")
        total_scraped += len(permits)
        city_results[city_code] = len(permits)

        if permits and conn:
            loaded = load_to_hot_leads(conn, permits)
            log(f"  Loaded {loaded} permits into hot_leads")
            total_loaded += loaded
        elif permits and args.dry_run:
            log(f"  [dry-run] Would load {len(permits)} permits")
            # Show the first permit that has an address for best sample
            sample = next((p for p in permits if p.get("address")), permits[0])
            sample_clean = {k: v for k, v in sample.items() if v is not None}
            log(f"  Sample:\n{json.dumps({k: str(v) for k, v in sample_clean.items()}, indent=4)}")

        time.sleep(CITY_PAUSE_SECONDS)

    if conn:
        conn.close()

    log("\n" + "=" * 60)
    log("ACCELA SCRAPER COMPLETE")
    log(f"Total scraped: {total_scraped} permits")
    log(f"Total loaded:  {total_loaded} permits")
    log("\nPer-city results:")
    for code, count in sorted(city_results.items(), key=lambda x: -x[1]):
        city_name, state = ACCELA_CITIES.get(code, (code, "??"))
        log(f"  {code:<16} {city_name:<25} {state}  {count:>5} permits")
    log("=" * 60)


if __name__ == "__main__":
    main()
