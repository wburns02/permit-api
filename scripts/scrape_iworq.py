#!/usr/bin/env python3
"""
iWorQ Citizen Portal — Universal Permit Scraper

Scrapes building permits from iWorQ-powered municipal portals using the
default paginated listing that loads WITHOUT CAPTCHA on many portals.

Discovery:
    Some iWorQ portals render a paginated list of recent permits on the
    base permits page (no search required, no CAPTCHA). Jackson County FL
    and Fort Myers Beach FL were confirmed to expose 14-15+ pages of the
    most recent ~200 permits this way.

    Portals that require CAPTCHA for search still expose this default view —
    the scraper simply hits GET /{slug}/permits/600?page=N without any
    search parameters.

URL patterns:
    New subdomain style (most portals):
        https://{slug}.portal.iworq.net/{SLUG}/permits/600
    Legacy path style (older portals):
        https://portal.iworq.net/{SLUG}/permits/600

Detail page (CAPTCHA-free, loads full permit data):
    https://{base}/{SLUG}/permit/600/{detail_id}

Usage:
    python3 scrape_iworq.py                         # All portals with default lists
    python3 scrape_iworq.py --portal jacksoncounty  # Single portal by slug
    python3 scrape_iworq.py --list                  # List known portals
    python3 scrape_iworq.py --dry-run               # Parse only, no DB write
    python3 scrape_iworq.py --pages 20              # Max pages per portal (default 30)
    python3 scrape_iworq.py --details               # Also fetch detail pages (slower)
    python3 scrape_iworq.py --discover              # Probe candidate slugs for new portals

Cron (daily):
    0 7 * * * cd /home/will/permit-api && python3 scripts/scrape_iworq.py >> /tmp/iworq_daily.log 2>&1
"""

import argparse
import re
import sys
import time
import uuid
import os
from datetime import datetime, date
from urllib.parse import urljoin

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

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("pip install psycopg2-binary")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

RATE_LIMIT_SECONDS = 1.5   # Between pages
PORTAL_PAUSE_SECONDS = 4   # Between portals
DEFAULT_MAX_PAGES = 30     # 30 pages × 15 permits = 450 max per portal per run

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Known iWorQ portals with confirmed default permit listing ─────────────────
# Format: slug -> {city, state, url_style, has_default_list}
# url_style: "subdomain" = {slug}.portal.iworq.net/{SLUG}/permits/600
#            "legacy"    = portal.iworq.net/{SLUG}/permits/600
#
# has_default_list=True  → confirmed shows permits without CAPTCHA
# has_default_list=False → requires search (CAPTCHA); kept for discovery tracking
IWORQ_PORTALS = {
    # ── Confirmed default list (CAPTCHA-free pagination) ──────────────────────
    "jacksoncounty": {
        "city": "Jackson County",
        "state": "FL",
        "county": "Jackson County",
        "url_style": "subdomain",
        "has_default_list": True,
        "notes": "15 permits/page, ~14+ pages, rural FL septic territory",
    },
    "FORTMYERSBEACH": {
        "city": "Fort Myers Beach",
        "state": "FL",
        "county": "Lee County",
        "url_style": "legacy",
        "has_default_list": True,
        "notes": "15 permits/page, ~15 pages, coastal FL construction",
    },

    # ── Confirmed portals — require CAPTCHA for search (no default list) ──────
    "hitchcock": {
        "city": "Hitchcock",
        "state": "TX",
        "county": "Galveston County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Portal ID 693, small TX gulf coast town, septic territory",
    },
    "northogden": {
        "city": "North Ogden",
        "state": "UT",
        "county": "Weber County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "UT suburban",
    },
    "farmingtonut": {
        "city": "Farmington",
        "state": "UT",
        "county": "Davis County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "UT suburban",
    },
    "vernal": {
        "city": "Vernal",
        "state": "UT",
        "county": "Uintah County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Rural UT, septic territory",
    },
    "moab": {
        "city": "Moab",
        "state": "UT",
        "county": "Grand County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Rural UT tourist town",
    },
    "pimaaz": {
        "city": "Pima",
        "state": "AZ",
        "county": "Graham County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Small AZ town, Portal ID 3590",
    },
    "butlertownship": {
        "city": "Butler Township",
        "state": "OH",
        "county": "Montgomery County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "OH township, Portal ID 1655",
    },

    # ── Additional confirmed portal homes (public permit search not yet tested) ─
    "trumannar": {
        "city": "Trumann",
        "state": "AR",
        "county": "Poinsett County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Small AR town, septic territory — CAPTCHA required",
    },
    "paragould": {
        "city": "Paragould",
        "state": "AR",
        "county": "Greene County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "AR city — CAPTCHA required",
    },
    "hardeecounty": {
        "city": "Hardee County",
        "state": "FL",
        "county": "Hardee County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Very rural FL, strong septic territory — CAPTCHA required",
    },
    "greenecounty": {
        "city": "Greene County",
        "state": "TN",   # Likely Greene County TN based on inspection types
        "county": "Greene County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Has septic hook-up inspection type — CAPTCHA required",
    },
    "fayettecountytn2": {
        "city": "Fayette County",
        "state": "TN",
        "county": "Fayette County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Rural TN county, septic territory — CAPTCHA required",
    },
    "gravette": {
        "city": "Gravette",
        "state": "AR",
        "county": "Benton County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Small AR town — CAPTCHA required",
    },
    "staridpermit": {
        "city": "Star",
        "state": "ID",
        "county": "Ada County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Small ID town — CAPTCHA required",
    },
    "winder": {
        "city": "Winder",
        "state": "GA",
        "county": "Barrow County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Small GA city — CAPTCHA required",
    },

    # ── Discovered via --discover probe (2026-04-07) ──────────────────────────
    "saginaw": {
        "city": "Saginaw",
        "state": "TX",
        "county": "Tarrant County",
        "url_style": "subdomain",
        "has_default_list": True,
        "notes": "25 pages default list, DFW suburb, mix of new construction",
    },
    "pearsall": {
        "city": "Pearsall",
        "state": "TX",
        "county": "Frio County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Small TX town, South Texas septic territory — CAPTCHA required",
    },
    "yoakum": {
        "city": "Yoakum",
        "state": "TX",
        "county": "DeWitt County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Small TX town, rural septic territory — CAPTCHA required",
    },
    "elcampo": {
        "city": "El Campo",
        "state": "TX",
        "county": "Wharton County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Small TX town, rural septic territory — CAPTCHA required",
    },
    "lavernia": {
        "city": "La Vernia",
        "state": "TX",
        "county": "Wilson County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Small TX town, rural Wilson County — CAPTCHA required",
    },
    "freeport": {
        "city": "Freeport",
        "state": "TX",
        "county": "Brazoria County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "TX gulf coast town — CAPTCHA required",
    },
    "azle": {
        "city": "Azle",
        "state": "TX",
        "county": "Tarrant/Parker County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "DFW exurb, septic territory at city fringe — CAPTCHA required",
    },
    "gladescounty": {
        "city": "Glades County",
        "state": "FL",
        "county": "Glades County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Very rural FL, strong septic territory — CAPTCHA required",
    },
    "dixiecounty": {
        "city": "Dixie County",
        "state": "FL",
        "county": "Dixie County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Very rural north FL, septic territory — CAPTCHA required",
    },
    "libertycountyfl": {
        "city": "Liberty County",
        "state": "FL",
        "county": "Liberty County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Very rural FL panhandle, septic territory — CAPTCHA required",
    },
    "hardincounty": {
        "city": "Hardin County",
        "state": "TN",
        "county": "Hardin County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Rural TN, septic territory — CAPTCHA required",
    },
    "forrestcountyms": {
        "city": "Forrest County",
        "state": "MS",
        "county": "Forrest County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "MS county near Hattiesburg, septic territory — CAPTCHA required",
    },
    "fulton": {
        "city": "Fulton",
        "state": "AR",
        "county": "Baxter County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "Small AR town — CAPTCHA required",
    },
    "searcy": {
        "city": "Searcy",
        "state": "AR",
        "county": "White County",
        "url_style": "subdomain",
        "has_default_list": False,
        "notes": "AR city, rural White County septic territory — CAPTCHA required",
    },
}

# ── Candidate slugs for --discover mode ──────────────────────────────────────
# Small towns in TX, FL, TN, MS, AR, GA, AL — prime septic territory
CANDIDATE_SLUGS = [
    # Texas small towns
    "aransas", "anahuac", "hamlin", "dilley", "pearsall", "floresville",
    "kenedy", "cuero", "gonzales", "yoakum", "edna", "wharton", "el_campo",
    "elcampo", "lavernia", "pleasanton", "jourdanton", "cotulla",
    "galvestoncounty", "brasoria", "brazoriacounty", "chasetx", "clute",
    "freeport", "lakeworth", "saginaw", "azle", "granburytx", "glen_rose",
    # Florida small towns / rural counties
    "glades", "gladescounty", "hendrycounty", "dixiecounty", "jeffersoncountyfl",
    "libertycountyfl", "calhounfl", "calhoun", "gilchristcounty", "gilchrist",
    "columbiacountyfl", "suwanneecounty", "lafayettecountyfl", "madisoncountyfl",
    "taylorfl", "taylorcountyfl", "leeflflorida", "desolocounty",
    "okeechobeecounty", "okeechobee", "highlands", "highlandscounty",
    "chariotteflorida", "hardee",
    # Tennessee rural counties
    "mcnairycounty", "hardincounty", "waynetn", "lowrencecountytn",
    "lewiscountytn", "hickmancountytn", "gilestu", "linchcountytn",
    "grundycountytn", "grundy", "sequatchie", "bledsoetn",
    # Mississippi (low priority but check)
    "forrestcountyms", "lamarcoams", "stonecountyms", "pearl_river",
    "pearlrivercounty", "georgecoams", "greene_ms", "greenecountyms",
    "perry_ms", "waynecoams", "clarkecountyms",
    # Arkansas rural
    "izard", "sharp", "fulton", "randolph", "cleburne", "stone",
    "newton", "searcy", "van_buren", "vanburen", "mccurtain",
    # Georgia rural
    "barrow", "walton", "waltoncountyga", "butts", "henry",
    "spalding", "lamar", "upson", "pike", "meriwether",
    # Alabama rural (iWorQ less common here but worth trying)
    "cleburne_al", "randolph_al", "chambers_al", "tallapoosa",
]


def log(msg: str):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  {msg}", flush=True)


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER
    )


def build_portal_url(slug: str, url_style: str) -> str:
    """Build the base URL for a portal's permits page."""
    if url_style == "legacy":
        return f"https://portal.iworq.net/{slug}/permits/600"
    else:
        return f"https://{slug}.portal.iworq.net/{slug}/permits/600"


def build_detail_url(slug: str, url_style: str, detail_id: str) -> str:
    """Build the URL for a specific permit detail page."""
    if url_style == "legacy":
        return f"https://portal.iworq.net/{slug}/permit/600/{detail_id}"
    else:
        return f"https://{slug}.portal.iworq.net/{slug}/permit/600/{detail_id}"


def make_client() -> httpx.Client:
    return httpx.Client(
        headers=HEADERS,
        timeout=30,
        follow_redirects=True,
    )


def parse_list_page(html: str, slug: str, portal_info: dict) -> list[dict]:
    """
    Parse the default permits listing page.

    The iWorQ listing table structure:
      <table> with <thead> containing column headers and <tbody> with data rows.
      Each data row has a link to the permit detail page.

    Columns vary by portal but common set:
      Permit #, Date, Parcel Number, Building Type, Permit Type, Project Type,
      Purpose, Property Owner, Parcel Address, City/State/Zip, Status
    """
    soup = BeautifulSoup(html, "html.parser")
    permits = []

    # Find the permits table — has thead with "Permit #" or "Permit Number"
    table = None
    for t in soup.find_all("table"):
        thead = t.find("thead")
        if thead:
            header_text = thead.get_text(" ", strip=True).lower()
            if "permit" in header_text and ("address" in header_text or "status" in header_text):
                table = t
                break

    if not table:
        return permits

    # Parse header columns
    thead = table.find("thead")
    headers = []
    if thead:
        for th in thead.find_all(["th", "td"]):
            headers.append(th.get_text(strip=True).lower())

    if not headers:
        return permits

    # Map column names to indices — use exact-match first, then substring
    def col_idx(exact_patterns, fallback_patterns=None):
        """Find column index matching exact patterns first, then fallbacks."""
        # Try exact match
        for pat in exact_patterns:
            for i, h in enumerate(headers):
                if h == pat:
                    return i
        # Try substring match on exact patterns
        for pat in exact_patterns:
            for i, h in enumerate(headers):
                if pat in h:
                    return i
        # Try fallback substring patterns
        if fallback_patterns:
            for pat in fallback_patterns:
                for i, h in enumerate(headers):
                    if pat in h:
                        return i
        return -1

    idx_permit_num = col_idx(["permit #", "permit number", "permit no"])
    idx_date       = col_idx(["date"])
    # Address: prefer "property address" or "parcel address" over bare "address"
    # to avoid accidentally matching "parcel #" which has no "address"
    idx_address    = col_idx(
        ["property address", "parcel address", "building address"],
        ["address"]
    )
    # Permit type: "permit type" exact, NOT "report code" or "building type"
    idx_type       = col_idx(["permit type"], ["report code", "type"])
    idx_build_type = col_idx(["building type"])
    idx_status     = col_idx(["status"])
    # Owner: "property owner name" preferred, NOT contractor/company
    idx_owner      = col_idx(
        ["property owner name", "property owner"],
        ["owner name", "applicant"]
    )
    idx_subtype    = col_idx(["sub type", "project type", "subtype"])
    idx_purpose    = col_idx(["purpose", "scope of work", "scope"])
    idx_parcel     = col_idx(["parcel #", "parcel number", "parcel no"])
    idx_contractor = col_idx(["primary contractor", "company name", "contractor"])

    city  = portal_info.get("city", slug)
    state = portal_info.get("state", "")
    county = portal_info.get("county", "")

    # Parse data rows
    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]

    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        def get_cell(idx):
            if 0 <= idx < len(cells):
                return cells[idx].get_text(strip=True)
            return ""

        permit_num = get_cell(idx_permit_num)
        if not permit_num or permit_num.lower() in ("permit #", "permit number"):
            continue

        # Extract detail page link and its ID
        link_tag = row.find("a", href=re.compile(r"/permit/\d+/\d+"))
        detail_id = None
        detail_url = None
        if link_tag:
            href = link_tag.get("href", "")
            m = re.search(r"/permit/\d+/(\d+)", href)
            if m:
                detail_id = m.group(1)
            detail_url = build_detail_url(
                slug, portal_info.get("url_style", "subdomain"), detail_id or ""
            ) if detail_id else None

        # Parse issue date
        date_str = get_cell(idx_date)
        issue_date = None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
            try:
                issue_date = datetime.strptime(date_str, fmt).date()
                break
            except (ValueError, TypeError):
                pass

        permit_type   = get_cell(idx_type)
        build_type    = get_cell(idx_build_type)
        status        = get_cell(idx_status)
        owner         = get_cell(idx_owner)
        subtype       = get_cell(idx_subtype)
        purpose       = get_cell(idx_purpose)
        parcel_num    = get_cell(idx_parcel)
        contractor    = get_cell(idx_contractor)

        # Build combined work_class from building type + sub type
        work_class_parts = [p for p in [build_type, subtype] if p]
        work_class = " / ".join(work_class_parts) if work_class_parts else None

        # Build description from purpose + parcel
        desc_parts = []
        if purpose:
            desc_parts.append(purpose)
        if parcel_num:
            desc_parts.append(f"Parcel: {parcel_num}")
        description = " | ".join(desc_parts) if desc_parts else None

        raw_address = get_cell(idx_address)

        permit = {
            "permit_number": permit_num[:100],
            "permit_type": permit_type[:100] if permit_type else None,
            "work_class": work_class[:100] if work_class else None,
            "description": description[:500] if description else None,
            "address": raw_address[:200] if raw_address else None,
            "city": city,
            "state": state,
            "county": county,
            "issue_date": issue_date,
            "status": status[:100] if status else None,
            "owner_name": owner[:200] if owner else None,
            "contractor_company": contractor[:200] if contractor else None,
            "jurisdiction": f"{city}, {state}" if state else city,
            "source": f"iworq_{slug.lower()}",
            "detail_id": detail_id,
            "detail_url": detail_url,
        }

        permits.append(permit)

    return permits


def detect_more_pages(html: str, current_page: int) -> bool:
    """Check if there's a next page of results."""
    soup = BeautifulSoup(html, "html.parser")

    # Look for pagination — iWorQ uses numbered links
    # Pattern: aria-label="Next" or link with page=N+1
    next_page = str(current_page + 1)

    # Check for ?page= links pointing to next page
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if f"page={next_page}" in href:
            return True
        if a.get("aria-label", "").lower() in ("next", "next page"):
            return True
        if a.get_text(strip=True) == ">":
            return True

    # Fallback: check for pagination container with higher numbered pages
    page_links = soup.find_all("a", href=re.compile(r"[?&]page=\d+"))
    if page_links:
        max_page = 0
        for a in page_links:
            m = re.search(r"page=(\d+)", a.get("href", ""))
            if m:
                max_page = max(max_page, int(m.group(1)))
        return max_page >= current_page + 1

    return False


def get_total_pages(html: str) -> int | None:
    """Try to extract total page count from pagination."""
    soup = BeautifulSoup(html, "html.parser")
    page_nums = []
    for a in soup.find_all("a", href=re.compile(r"[?&]page=\d+")):
        m = re.search(r"page=(\d+)", a.get("href", ""))
        if m:
            page_nums.append(int(m.group(1)))
    if page_nums:
        return max(page_nums)
    return None


def has_default_list(html: str) -> bool:
    """Check if the page has a populated permit table (not just empty search form)."""
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table"):
        tbody = table.find("tbody")
        if tbody:
            rows = tbody.find_all("tr")
            # Check if any row has a permit-detail link
            for row in rows:
                if row.find("a", href=re.compile(r"/permit/\d+/\d+")):
                    return True
    return False


def scrape_portal(
    slug: str,
    portal_info: dict,
    max_pages: int = DEFAULT_MAX_PAGES,
    fetch_details: bool = False,
) -> list[dict]:
    """
    Scrape all paginated permits from one iWorQ portal.

    Returns list of normalized permit dicts.
    """
    base_url = build_portal_url(slug, portal_info.get("url_style", "subdomain"))
    city = portal_info.get("city", slug)
    state = portal_info.get("state", "")
    all_permits = []

    client = make_client()

    for page_num in range(1, max_pages + 1):
        url = f"{base_url}?page={page_num}" if page_num > 1 else base_url
        log(f"  [{slug}] Page {page_num}: {url}")

        try:
            r = client.get(url)
        except Exception as e:
            log(f"  [{slug}] Request error: {e}")
            break

        if r.status_code == 404:
            log(f"  [{slug}] 404 — portal not found or module unavailable")
            break

        if r.status_code != 200:
            log(f"  [{slug}] HTTP {r.status_code}")
            break

        # Check for redirect to error/home page
        final_url = str(r.url)
        if "/portalhome/" in final_url and "/permits/" not in final_url:
            log(f"  [{slug}] Redirected to portal home — no public permit list")
            break

        # On first page, check if there's a default list
        if page_num == 1:
            if not has_default_list(r.text):
                if portal_info.get("has_default_list") is True:
                    log(f"  [{slug}] WARNING: expected default list but page is empty")
                else:
                    log(f"  [{slug}] No default permit list — CAPTCHA required for search")
                break

            total_pages = get_total_pages(r.text)
            if total_pages:
                log(f"  [{slug}] Total pages available: {total_pages}")

        page_permits = parse_list_page(r.text, slug, portal_info)
        log(f"  [{slug}] Page {page_num}: {len(page_permits)} permits parsed")

        if not page_permits:
            log(f"  [{slug}] Empty page — stopping")
            break

        all_permits.extend(page_permits)

        # Check for more pages
        if not detect_more_pages(r.text, page_num):
            log(f"  [{slug}] Last page reached at page {page_num}")
            break

        time.sleep(RATE_LIMIT_SECONDS)

    # Optionally enrich with detail page data
    if fetch_details and all_permits:
        log(f"  [{slug}] Fetching detail pages for {len(all_permits)} permits...")
        for i, permit in enumerate(all_permits):
            if not permit.get("detail_url"):
                continue
            try:
                dr = client.get(permit["detail_url"])
                if dr.status_code == 200:
                    enrich_from_detail(permit, dr.text)
            except Exception as e:
                log(f"  [{slug}] Detail page error: {e}")
            if i % 10 == 0 and i > 0:
                log(f"  [{slug}] Detailed {i}/{len(all_permits)}")
                time.sleep(RATE_LIMIT_SECONDS)
            else:
                time.sleep(0.5)

    return all_permits


def enrich_from_detail(permit: dict, html: str):
    """
    Enrich a permit record with data from its detail page.

    Detail pages expose: owner name, parcel number, contractor info,
    project cost, square feet, project description.
    """
    soup = BeautifulSoup(html, "html.parser")

    def find_value(label_patterns: list[str]) -> str:
        """Find a value next to a label in the detail page."""
        for pat in label_patterns:
            # Look for elements containing the label text
            for tag in soup.find_all(string=re.compile(pat, re.IGNORECASE)):
                parent = tag.parent
                # Try sibling
                nxt = parent.find_next_sibling()
                if nxt:
                    val = nxt.get_text(strip=True)
                    if val and val.lower() not in ("n/a", "none", ""):
                        return val
                # Try parent's next sibling
                grandparent = parent.parent
                if grandparent:
                    nxt2 = grandparent.find_next_sibling()
                    if nxt2:
                        val = nxt2.get_text(strip=True)
                        if val and val.lower() not in ("n/a", "none", ""):
                            return val
        return ""

    # Extract additional fields from detail page
    owner = find_value(["property owner", "owner name"])
    contractor = find_value(["contractor", "contractor name"])
    contractor_company = find_value(["contractor business", "company name", "business name"])
    project_cost = find_value(["project cost", "valuation", "estimated cost"])
    sqft = find_value(["square feet", "sq ft", "area"])
    description = find_value(["purpose", "project description", "description", "scope"])
    parcel = find_value(["parcel number", "parcel #", "parcel no", "parcel id"])

    if owner and not permit.get("owner_name"):
        permit["owner_name"] = owner[:200]
    if contractor_company and not permit.get("contractor_company"):
        permit["contractor_company"] = contractor_company[:200]
    if contractor and not permit.get("contractor_name"):
        permit["contractor_name"] = contractor[:200]
    if description and not permit.get("description"):
        permit["description"] = description[:500]
    if parcel:
        # Append parcel to description if not already there
        desc = permit.get("description") or ""
        if parcel not in desc:
            permit["description"] = (f"{desc} | Parcel: {parcel}" if desc else f"Parcel: {parcel}")[:500]

    # Parse project cost as valuation
    if project_cost and not permit.get("valuation"):
        cost_clean = re.sub(r"[^0-9.]", "", project_cost)
        try:
            permit["valuation"] = float(cost_clean)
        except (ValueError, TypeError):
            pass

    # Parse sqft
    if sqft and not permit.get("sqft"):
        sqft_clean = re.sub(r"[^0-9.]", "", sqft)
        try:
            permit["sqft"] = int(float(sqft_clean))
        except (ValueError, TypeError):
            pass


def probe_portal(slug: str) -> dict | None:
    """
    Probe a candidate slug to check if it's a valid iWorQ portal with public permits.

    Returns portal info dict if found, None otherwise.
    """
    client = make_client()

    # Try subdomain style first
    for url_style, test_url in [
        ("subdomain", f"https://{slug}.portal.iworq.net/{slug}/permits/600"),
        ("legacy",    f"https://portal.iworq.net/{slug}/permits/600"),
    ]:
        try:
            r = client.get(test_url, timeout=10)
        except Exception:
            continue

        if r.status_code != 200:
            continue

        # Reject error pages
        if "this page does not exist" in r.text.lower():
            continue

        # Check if it has a permit table (even empty search form counts)
        soup = BeautifulSoup(r.text, "html.parser")
        has_permit_table = bool(soup.find_all("table"))
        if not has_permit_table:
            continue

        # Check for default list
        default_list = has_default_list(r.text)
        total_pages = get_total_pages(r.text) if default_list else None

        return {
            "slug": slug,
            "url_style": url_style,
            "has_default_list": default_list,
            "total_pages": total_pages,
            "test_url": test_url,
        }

    return None


def load_to_db(conn, permits: list[dict]) -> int:
    """Batch upsert permits into hot_leads table."""
    if not permits:
        return 0

    # Deduplicate within batch by (permit_number, source)
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
            p.get("state", ""),
            p.get("zip"),
            p.get("valuation"),
            p.get("sqft"),
            p.get("issue_date"),
            p.get("status"),
            p.get("contractor_company"),
            p.get("contractor_name"),
            None,   # contractor_phone
            None,   # applicant_name (not same as owner)
            None,   # applicant_phone
            p.get("owner_name"),
            p.get("jurisdiction"),
            p.get("source"),
        ))

    sql = """
        INSERT INTO hot_leads (
            id, permit_number, permit_type, work_class, description,
            address, city, state, zip, valuation, sqft, issue_date,
            status,
            contractor_company, contractor_name, contractor_phone,
            applicant_name, applicant_phone, owner_name,
            jurisdiction, source
        ) VALUES %s
        ON CONFLICT (permit_number, source)
        DO UPDATE SET
            issue_date        = COALESCE(EXCLUDED.issue_date,        hot_leads.issue_date),
            description       = COALESCE(EXCLUDED.description,       hot_leads.description),
            address           = COALESCE(EXCLUDED.address,           hot_leads.address),
            status            = COALESCE(EXCLUDED.status,            hot_leads.status),
            contractor_company = COALESCE(EXCLUDED.contractor_company, hot_leads.contractor_company),
            owner_name        = COALESCE(EXCLUDED.owner_name,        hot_leads.owner_name),
            scraped_at        = CURRENT_DATE
    """

    execute_values(cur, sql, batch)
    conn.commit()
    return len(batch)


def cmd_list():
    """Print all known portals."""
    print(f"\n{'Slug':<25} {'City':<25} {'State':<6} {'Default List':<14} Notes")
    print("-" * 90)
    for slug, info in sorted(IWORQ_PORTALS.items()):
        dl = {True: "YES", False: "no", None: "?"}[info.get("has_default_list")]
        print(
            f"{slug:<25} {info.get('city',''):<25} {(info.get('state') or ''):<6} "
            f"{dl:<14} {info.get('notes','')}"
        )
    print(f"\nTotal: {len(IWORQ_PORTALS)} portals\n")


def cmd_discover(dry_run: bool = False):
    """Probe candidate slugs to find new iWorQ portals."""
    log(f"Probing {len(CANDIDATE_SLUGS)} candidate slugs...")
    found = []

    for i, slug in enumerate(CANDIDATE_SLUGS):
        if slug in IWORQ_PORTALS:
            continue

        log(f"[{i+1}/{len(CANDIDATE_SLUGS)}] Probing: {slug}")
        result = probe_portal(slug)

        if result:
            log(f"  FOUND: {slug} | default_list={result['has_default_list']} | "
                f"pages={result.get('total_pages')} | {result['test_url']}")
            found.append(result)
        else:
            log(f"  not found")

        time.sleep(1)

    log(f"\nDiscovery complete: {len(found)} new portals found")
    for r in found:
        print(f"  {r['slug']:<30} {r['url_style']:<12} default={r['has_default_list']} "
              f"pages={r.get('total_pages')} {r['test_url']}")

    return found


def main():
    parser = argparse.ArgumentParser(description="iWorQ Permit Portal Scraper")
    parser.add_argument(
        "--portal", nargs="+", metavar="SLUG",
        help="Scrape specific portal(s) by slug"
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List all known portals and exit"
    )
    parser.add_argument(
        "--discover", action="store_true",
        help="Probe candidate slugs to find new portals"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Scrape all portals (default: only portals with confirmed default list)"
    )
    parser.add_argument(
        "--pages", type=int, default=DEFAULT_MAX_PAGES,
        help=f"Max pages per portal (default: {DEFAULT_MAX_PAGES})"
    )
    parser.add_argument(
        "--details", action="store_true",
        help="Also fetch individual permit detail pages (slower, more data)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse permits but don't write to database"
    )
    args = parser.parse_args()

    if args.list:
        cmd_list()
        return

    if args.discover:
        cmd_discover(dry_run=args.dry_run)
        return

    # Select portals to scrape
    if args.portal:
        portals_to_scrape = {}
        for slug in args.portal:
            if slug in IWORQ_PORTALS:
                portals_to_scrape[slug] = IWORQ_PORTALS[slug]
            else:
                # Treat as unknown — probe it first
                log(f"Unknown slug '{slug}' — probing...")
                result = probe_portal(slug)
                if result:
                    portals_to_scrape[slug] = {
                        "city": slug.replace("-", " ").title(),
                        "state": "XX",
                        "county": "",
                        "url_style": result["url_style"],
                        "has_default_list": result["has_default_list"],
                    }
                else:
                    log(f"  '{slug}' not reachable — skipping")
    elif args.all:
        portals_to_scrape = IWORQ_PORTALS
    else:
        # Default: only portals confirmed to have a default permit list
        portals_to_scrape = {
            slug: info
            for slug, info in IWORQ_PORTALS.items()
            if info.get("has_default_list") is True
        }

    if not portals_to_scrape:
        log("No portals selected. Use --list to see options, --all to scrape all.")
        return

    log(f"Scraping {len(portals_to_scrape)} iWorQ portal(s)...")
    if args.dry_run:
        log("DRY RUN — no database writes")

    conn = None
    if not args.dry_run:
        try:
            conn = get_conn()
            log("Database connected")
        except Exception as e:
            log(f"DB connection failed: {e}")
            sys.exit(1)

    total_inserted = 0
    total_parsed = 0

    for slug, portal_info in portals_to_scrape.items():
        city = portal_info.get("city", slug)
        state = portal_info.get("state", "")
        log(f"\n=== {city}, {state} [{slug}] ===")

        try:
            permits = scrape_portal(
                slug, portal_info,
                max_pages=args.pages,
                fetch_details=args.details,
            )
            total_parsed += len(permits)
            log(f"  Parsed {len(permits)} permits total")

            if permits and not args.dry_run:
                inserted = load_to_db(conn, permits)
                total_inserted += inserted
                log(f"  Loaded {inserted} records to hot_leads")
            elif permits and args.dry_run:
                log(f"  [dry-run] Would load {len(permits)} records")
                # Print sample
                for p in permits[:3]:
                    log(f"    {p['permit_number']} | {p['address']} | "
                        f"{p['permit_type']} | {p['status']} | {p['issue_date']}")

        except Exception as e:
            log(f"  ERROR scraping {slug}: {e}")
            import traceback
            traceback.print_exc()

        time.sleep(PORTAL_PAUSE_SECONDS)

    if conn:
        conn.close()

    log(f"\nDone. Parsed: {total_parsed} | Inserted/updated: {total_inserted}")


if __name__ == "__main__":
    main()
