#!/usr/bin/env python3
"""
MGO Connect San Marcos Scraper — pulls building permits from the MGO Connect
portal for San Marcos, TX and loads them into hot_leads on T430 PostgreSQL.

Uses Playwright to authenticate, then calls the search API directly with the
discovered request format: POST /api/v3/cp/project/search-projects
with filters: {JURISDICTIONID: 43, PROJECTTYPEID: 3} (ALL CAPS keys).

Usage:
    python3 scrape_mgo_sanmarcos.py              # Default: last 90 days
    python3 scrape_mgo_sanmarcos.py --days 30    # Last 30 days
    python3 scrape_mgo_sanmarcos.py --all        # All permits (no date filter)

Cron (daily 5:30 AM):
    30 5 * * * cd /home/will/permit-api-live && python3 scripts/scrape_mgo_sanmarcos.py >> /tmp/mgo_sanmarcos.log 2>&1
"""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import date, datetime, timedelta

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("pip install psycopg2-binary")
    sys.exit(1)

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("pip install playwright && playwright install chromium")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

MGO_EMAIL = os.getenv("MGO_EMAIL", "willwalterburns@gmail.com")
MGO_PASSWORD = os.getenv("MGO_PASSWORD", "#Espn202512")

LOGIN_URL = "https://www.mgoconnect.org/cp/login"
API_BASE = "https://api.mgoconnect.org"
SEARCH_ENDPOINT = f"{API_BASE}/api/v3/cp/project/search-projects"

SAN_MARCOS_JURISDICTION_ID = 43
PERMIT_TYPE_ID = 3  # "Permit" project type

SOURCE = "mgo_san_marcos"
CITY = "San Marcos"
STATE = "TX"

PAGE_SIZE = 500  # Max rows per API request


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def authenticate():
    """Log into MGO Connect and return the auth token."""
    log("Authenticating with MGO Connect...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        page = context.new_page()

        auth_token = None

        def on_response(response):
            nonlocal auth_token
            if response.request.resource_type in ("xhr", "fetch"):
                hdr = response.request.headers.get("authorization-token")
                if hdr:
                    auth_token = hdr

        page.on("response", on_response)

        # Login
        page.goto(LOGIN_URL, wait_until="networkidle", timeout=30000)
        time.sleep(2)

        page.locator("input[type=email]").fill(MGO_EMAIL)
        page.locator("input[type=password]").fill(MGO_PASSWORD)
        page.locator("button").filter(has_text="Login").click()
        time.sleep(5)
        page.wait_for_load_state("networkidle")

        # Set jurisdiction in localStorage for the session
        page.evaluate("""() => {
            localStorage.setItem('CPJurisdiction', 'San Marcos');
            localStorage.setItem('CPJurisdictionID', '43');
            localStorage.setItem('CPState', 'TX');
            localStorage.setItem('CPStateName', 'TX');
        }""")

        # Navigate to search page to trigger API calls and capture token
        page.goto("https://www.mgoconnect.org/cp/search", wait_until="networkidle", timeout=30000)
        time.sleep(3)

        browser.close()

    if not auth_token:
        log("ERROR: Failed to capture auth token")
        return None

    log(f"  Auth token captured: {auth_token[:8]}...")
    return auth_token


def fetch_permits(auth_token, days=None, offset=0):
    """Fetch a page of permits from the MGO Connect API."""
    filters = {
        "JURISDICTIONID": SAN_MARCOS_JURISDICTION_ID,
        "PROJECTTYPEID": PERMIT_TYPE_ID,
    }

    # Add date filter if specified
    # Note: The filter key is CREATEDATEAFTER (not CREATEDAFTER) per the Angular bundle
    if days is not None:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        filters["CREATEDATEAFTER"] = cutoff

    body = {
        "filters": filters,
        "Rows": PAGE_SIZE,
        "OffSet": offset,
        "SortField": "createdDate",
        "SortOrder": -1,
    }

    # Use httpx for the API call (simpler than Playwright for REST)
    try:
        import httpx
        client = httpx.Client(timeout=60)
        resp = client.post(
            SEARCH_ENDPOINT,
            json=body,
            headers={
                "Content-Type": "application/json",
                "Authorization-Token": auth_token,
                "SourcePlatform": "MGO Connect Web",
            },
        )
        client.close()

        if resp.status_code != 200:
            log(f"  API error: {resp.status_code} — {resp.text[:200]}")
            return [], 0

        data = resp.json()
        items = data.get("data", [])
        total_rows = items[0].get("totalRows", 0) if items else 0
        return items, total_rows

    except ImportError:
        # Fallback: use Playwright page.evaluate for the API call
        return _fetch_via_playwright(auth_token, body)
    except Exception as e:
        log(f"  Fetch error: {e}")
        return [], 0


def _fetch_via_playwright(auth_token, body):
    """Fallback: use Playwright to make the API call via browser context."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://www.mgoconnect.org/cp/login", wait_until="networkidle", timeout=15000)
        time.sleep(2)

        result = page.evaluate("""
            async (args) => {
                const [url, body, token] = args;
                try {
                    const resp = await fetch(url, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Authorization-Token': token,
                            'SourcePlatform': 'MGO Connect Web'
                        },
                        body: JSON.stringify(body)
                    });
                    const data = await resp.json();
                    return {status: resp.status, data: data};
                } catch(e) {
                    return {error: e.message};
                }
            }
        """, [SEARCH_ENDPOINT, body, auth_token])

        browser.close()

        if result.get("error"):
            log(f"  Playwright fetch error: {result['error']}")
            return [], 0

        data = result.get("data", {})
        items = data.get("data", [])
        total_rows = items[0].get("totalRows", 0) if items else 0
        return items, total_rows


def normalize_permit(item):
    """Normalize an MGO Connect project record into our permit schema."""
    permit = {
        "city": CITY,
        "state": STATE,
        "source": SOURCE,
    }

    # Skip header/template rows
    pn = item.get("projectNumber", "")
    if pn in ("Request", "Project Number", ""):
        return None

    # Direct field mappings
    permit["permit_number"] = pn
    permit["address"] = item.get("projectAddress")
    permit["description"] = item.get("projectDescription")
    permit["status"] = item.get("projectStatus")
    permit["permit_type"] = item.get("designationType")  # Residential, Commercial, etc.
    permit["work_class"] = item.get("workType")  # Plumbing, Electrical, etc.
    permit["zip"] = item.get("projectZip")

    # Date parsing
    date_str = item.get("dateCreated")
    if date_str:
        try:
            # Format: "2026-04-07T08:54:16"
            permit["issue_date"] = datetime.fromisoformat(date_str.split("T")[0]).date()
        except (ValueError, AttributeError):
            pass

    # Location data
    lat = item.get("projectLat")
    lng = item.get("projectLng")
    if lat:
        try:
            permit["lat"] = float(lat)
        except (ValueError, TypeError):
            pass
    if lng:
        try:
            permit["lng"] = float(lng)
        except (ValueError, TypeError):
            pass

    # Additional fields
    permit["applicant_name"] = item.get("specificUse") or item.get("sepcificUse")  # typo in API
    permit["contractor_company"] = item.get("subdivision") if item.get("subdivision") else None

    return permit


def scrape_all_permits(auth_token, days=None):
    """Fetch all permits with pagination."""
    all_permits = []
    offset = 0

    # First page
    items, total_rows = fetch_permits(auth_token, days=days, offset=0)
    if not items:
        log("  No permits returned from first page")
        return all_permits

    log(f"  Total available: {total_rows} permits")
    all_permits.extend(items)
    log(f"  Page 1: {len(items)} permits (offset 0)")

    # Paginate through remaining results
    max_permits = min(total_rows, 50000)  # Safety cap at 50k
    while len(all_permits) < max_permits:
        offset += PAGE_SIZE
        items, _ = fetch_permits(auth_token, days=days, offset=offset)
        if not items:
            break
        all_permits.extend(items)
        page_num = (offset // PAGE_SIZE) + 1
        log(f"  Page {page_num}: {len(items)} permits (offset {offset}, total so far: {len(all_permits)})")
        time.sleep(0.5)  # Be polite

    return all_permits


def load_to_hot_leads(conn, permits):
    """Insert permits into hot_leads with dedup."""
    if not permits:
        return 0

    cur = conn.cursor()
    batch = []
    for p in permits:
        # Skip entries without permit_number AND address (can't dedup)
        if not p.get("permit_number") and not p.get("address"):
            continue

        batch.append((
            str(uuid.uuid4()),
            p.get("permit_number"),
            p.get("permit_type"),
            p.get("work_class"),
            p.get("description"),
            p.get("address"),
            p.get("city", CITY),
            p.get("state", STATE),
            p.get("zip"),
            p.get("valuation"),
            p.get("sqft"),
            p.get("issue_date"),
            p.get("contractor_company"),
            p.get("contractor_name"),
            p.get("contractor_phone"),
            p.get("applicant_name"),
            p.get("applicant_phone"),
            "San Marcos, TX",  # jurisdiction
            SOURCE,
        ))

    if not batch:
        log("  No valid permits to insert")
        return 0

    # Use the permit_number + source unique index for dedup (ix_hot_leads_permit)
    # This avoids conflicts with the other partial unique index on (permit_number, address, state)
    sql = """
        INSERT INTO hot_leads (
            id, permit_number, permit_type, work_class, description,
            address, city, state, zip, valuation, sqft, issue_date,
            contractor_company, contractor_name, contractor_phone,
            applicant_name, applicant_phone, jurisdiction, source
        ) VALUES %s
        ON CONFLICT (permit_number, source)
        DO UPDATE SET
            issue_date = COALESCE(EXCLUDED.issue_date, hot_leads.issue_date),
            description = COALESCE(EXCLUDED.description, hot_leads.description),
            address = COALESCE(EXCLUDED.address, hot_leads.address),
            permit_type = COALESCE(EXCLUDED.permit_type, hot_leads.permit_type),
            work_class = COALESCE(EXCLUDED.work_class, hot_leads.work_class),
            zip = COALESCE(EXCLUDED.zip, hot_leads.zip)
    """
    try:
        execute_values(cur, sql, batch, page_size=500)
        conn.commit()
        loaded = len(batch)
        log(f"  Inserted/updated {loaded} permits into hot_leads")
    except Exception as e:
        conn.rollback()
        log(f"  Batch insert error: {e}")
        # Try smaller batches for partial success
        loaded = 0
        chunk_size = 100
        for i in range(0, len(batch), chunk_size):
            chunk = batch[i:i + chunk_size]
            try:
                execute_values(cur, sql, chunk)
                conn.commit()
                loaded += len(chunk)
            except Exception as chunk_err:
                conn.rollback()
                # Fall back to single inserts for this chunk
                for row in chunk:
                    try:
                        execute_values(cur, sql, [row])
                        conn.commit()
                        loaded += 1
                    except Exception:
                        conn.rollback()
        log(f"  Recovered {loaded}/{len(batch)} via chunked inserts")
    finally:
        cur.close()

    return loaded


def main():
    parser = argparse.ArgumentParser(description="Scrape MGO Connect for San Marcos, TX permits")
    parser.add_argument("--days", type=int, default=90, help="Days of history to search (default: 90)")
    parser.add_argument("--all", action="store_true", help="Fetch all permits (no date filter)")
    args = parser.parse_args()

    days = None if args.all else args.days

    log("=" * 60)
    log("MGO CONNECT — SAN MARCOS, TX PERMIT SCRAPER")
    log(f"  Date filter: {'all time' if days is None else f'last {days} days'}")
    log("=" * 60)

    # Step 1: Authenticate
    auth_token = authenticate()
    if not auth_token:
        log("FATAL: Could not authenticate")
        sys.exit(1)

    # Step 2: Fetch permits via API
    log("Fetching permits via API...")
    raw_permits = scrape_all_permits(auth_token, days=days)
    log(f"  Raw permits fetched: {len(raw_permits)}")

    if not raw_permits:
        log("No permits found")
        sys.exit(0)

    # Step 3: Normalize
    permits = []
    for item in raw_permits:
        try:
            p = normalize_permit(item)
            if p and (p.get("permit_number") or p.get("address")):
                permits.append(p)
        except Exception as e:
            log(f"  Normalize error: {e}")

    log(f"  Normalized permits: {len(permits)}")

    # Step 4: Deduplicate by permit_number (primary dedup key for ON CONFLICT)
    seen = set()
    unique_permits = []
    for p in permits:
        pn = p.get("permit_number", "")
        if pn and pn not in seen:
            seen.add(pn)
            unique_permits.append(p)
        elif not pn:
            # No permit number — deduplicate by address
            addr = p.get("address", "")
            addr_key = f"_addr_{addr}"
            if addr_key not in seen:
                seen.add(addr_key)
                unique_permits.append(p)

    log(f"  Unique permits: {len(unique_permits)}")

    # Step 5: Load into database
    try:
        conn = get_conn()
        log(f"Connected to {DB_HOST}")
        loaded = load_to_hot_leads(conn, unique_permits)
        conn.close()
        log(f"Done: {loaded} permits loaded into hot_leads")
    except Exception as e:
        log(f"Database error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    log("=" * 60)
    log("COMPLETE")
    log("=" * 60)


if __name__ == "__main__":
    main()
