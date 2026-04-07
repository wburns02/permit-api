#!/usr/bin/env python3
"""
MGO Connect Central Texas Scraper — pulls permits from ALL CTX jurisdictions
in MGO Connect and loads them into hot_leads.

Wraps the San Marcos scraper's auth + API logic for 20 CTX jurisdictions.

Usage:
    python3 scrape_mgo_ctx.py                    # All CTX, last 90 days
    python3 scrape_mgo_ctx.py --days 30          # Last 30 days
    python3 scrape_mgo_ctx.py --jurisdiction 43  # Single jurisdiction
    python3 scrape_mgo_ctx.py --all              # All time

Cron (daily 5:20 AM, after Central TX Socrata):
    20 5 * * * cd /home/will/permit-api-live && python3 scripts/scrape_mgo_ctx.py --days 7 >> /tmp/mgo_ctx.log 2>&1
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
    import httpx
except ImportError:
    print("pip install httpx")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

MGO_EMAIL = "willwalterburns@gmail.com"
MGO_PASSWORD = "#Espn202512"
API_BASE = "https://api.mgoconnect.org"
LEGACY_API = "https://www.mygovernmentonline.org/api"
SEARCH_ENDPOINT = f"{API_BASE}/api/v3/cp/project/search-projects"

PERMIT_TYPE_ID = 3  # "Permit" project type
PAGE_SIZE = 500

# All Central Texas jurisdictions in MGO Connect
CTX_JURISDICTIONS = {
    43: "San Marcos",
    123: "Hays County",
    129: "Buda",
    142: "Burnet",
    494: "Caldwell County",
    47: "Cedar Park",
    486: "City of Bastrop",
    205: "Bastrop County",
    180: "Dripping Springs",
    241: "Elgin",
    48: "Georgetown",
    184: "Liberty Hill",
    513: "Lockhart",
    155: "Manor",
    132: "Marble Falls",
    152: "Taylor",
    125: "Travis County",
    231: "Williamson County",
}


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def authenticate():
    """Log into MGO Connect via legacy API and return auth token."""
    import urllib.parse
    session = httpx.Client(timeout=30, follow_redirects=True)
    session.headers.update({
        "accept": "application/json",
        "sourceplatform": "MGO Connect Web",
        "user-agent": "Mozilla/5.0",
        "referer": "https://www.mgoconnect.org/",
    })

    body = "=" + urllib.parse.quote(json.dumps({
        "Email": MGO_EMAIL,
        "Password": MGO_PASSWORD,
    }))

    resp = session.post(
        f"{LEGACY_API}/user/login/-",
        content=body,
        headers={"content-type": "application/x-www-form-urlencoded; charset=UTF-8"},
    )
    data = resp.json()
    token = data.get("UserToken")

    if not token:
        log(f"ERROR: Login failed: {data}")
        return None, None

    log(f"Authenticated (UserID: {data.get('UserID')})")
    session.headers["authorization-token"] = token
    return token, session


def fetch_permits(session, jurisdiction_id, jurisdiction_name, days=None):
    """Fetch permits for a single jurisdiction."""
    filters = {
        "JURISDICTIONID": jurisdiction_id,
        "PROJECTTYPEID": PERMIT_TYPE_ID,
    }

    if days:
        since = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
        filters["CREATEDATEAFTER"] = since

    all_permits = []
    offset = 0

    while True:
        payload = {
            "filters": filters,
            "Rows": PAGE_SIZE,
            "OffSet": offset,
            "SortField": "dateCreated",
            "SortOrder": "desc",
        }

        try:
            resp = session.post(SEARCH_ENDPOINT, json=payload)
            if resp.status_code != 200:
                log(f"    API error {resp.status_code} for {jurisdiction_name}")
                break

            data = resp.json()
            items = data.get("data", data.get("rows", []))

            if not items:
                break

            for item in items:
                permit = normalize_permit(item, jurisdiction_name)
                if permit:
                    all_permits.append(permit)

            total = data.get("totalCount", data.get("total", 0))
            offset += PAGE_SIZE

            if offset >= total or len(items) < PAGE_SIZE:
                break

        except Exception as e:
            log(f"    Fetch error for {jurisdiction_name}: {e}")
            break

    return all_permits


def normalize_permit(item, jurisdiction_name):
    """Convert MGO API response to hot_leads format."""
    address = (item.get("address") or item.get("projectAddress") or
               item.get("siteAddress") or "").strip()
    permit_number = (item.get("projectUID") or item.get("permitNumber") or
                     item.get("projectNumber") or "").strip()

    if not address and not permit_number:
        return None

    # Parse date
    issue_date = None
    date_str = item.get("dateCreated") or item.get("createdDate") or item.get("issueDate")
    if date_str:
        try:
            if "T" in str(date_str):
                issue_date = datetime.fromisoformat(str(date_str).replace("Z", "+00:00")).date()
            else:
                issue_date = datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
        except Exception:
            pass

    # Extract city from address or use jurisdiction name
    city = item.get("city") or jurisdiction_name
    zip_code = item.get("zip") or item.get("zipCode")
    if zip_code:
        zip_code = str(zip_code)[:5]

    description = (item.get("description") or item.get("projectDescription") or
                   item.get("projectName") or "").strip()
    permit_type = item.get("projectTypeName") or item.get("permitType") or item.get("category")
    work_class = item.get("workClass") or item.get("subType") or item.get("subcategory")

    valuation = None
    val_str = item.get("valuation") or item.get("estimatedCost") or item.get("projectValue")
    if val_str:
        try:
            valuation = float(str(val_str).replace(",", "").replace("$", ""))
        except (ValueError, TypeError):
            pass

    contractor = item.get("contractorName") or item.get("applicantName")

    source = f"mgo_{jurisdiction_name.lower().replace(' ', '_')}"

    return {
        "permit_number": permit_number[:100] if permit_number else None,
        "permit_type": str(permit_type)[:50] if permit_type else None,
        "work_class": str(work_class)[:100] if work_class else None,
        "description": description[:500] if description else None,
        "address": address[:200] if address else None,
        "city": city[:100] if city else jurisdiction_name,
        "state": "TX",
        "zip": zip_code,
        "valuation": valuation,
        "issue_date": issue_date,
        "contractor_name": str(contractor)[:200] if contractor else None,
        "source": source[:100],
        "jurisdiction": f"{jurisdiction_name}, TX",
    }


def load_to_hot_leads(conn, permits):
    """Batch insert into hot_leads with dedup."""
    if not permits:
        return 0

    cur = conn.cursor()
    batch = []
    for p in permits:
        batch.append((
            str(uuid.uuid4()),
            p["permit_number"], p["permit_type"], p["work_class"], p["description"],
            p["address"], p["city"], p["state"], p["zip"],
            p["valuation"], None,  # sqft
            p["issue_date"],
            None, p["contractor_name"], None,  # company, name, phone
            None, None,  # applicant name/phone
            p["jurisdiction"], p["source"],
        ))

    sql = """
        INSERT INTO hot_leads (
            id, permit_number, permit_type, work_class, description,
            address, city, state, zip, valuation, sqft, issue_date,
            contractor_company, contractor_name, contractor_phone,
            applicant_name, applicant_phone, jurisdiction, source
        ) VALUES %s
        ON CONFLICT (permit_number, address, state)
        WHERE permit_number IS NOT NULL AND address IS NOT NULL
        DO UPDATE SET
            issue_date = COALESCE(EXCLUDED.issue_date, hot_leads.issue_date),
            description = COALESCE(EXCLUDED.description, hot_leads.description),
            contractor_name = COALESCE(EXCLUDED.contractor_name, hot_leads.contractor_name),
            source = EXCLUDED.source
    """
    try:
        execute_values(cur, sql, batch, page_size=500)
        conn.commit()
        return len(batch)
    except Exception as e:
        conn.rollback()
        log(f"  Insert error: {e}")
        return 0
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser(description="MGO Connect CTX Scraper")
    parser.add_argument("--days", type=int, default=90, help="Days of history (default 90)")
    parser.add_argument("--jurisdiction", type=int, help="Single jurisdiction ID")
    parser.add_argument("--all", action="store_true", help="Fetch all time (no date filter)")
    args = parser.parse_args()

    days = None if args.all else args.days

    log("=" * 60)
    log("MGO CONNECT — CENTRAL TEXAS SCRAPER")
    log(f"Jurisdictions: {len(CTX_JURISDICTIONS)}, Days: {'all' if days is None else days}")
    log("=" * 60)

    # Authenticate
    token, session = authenticate()
    if not token:
        log("ERROR: Authentication failed")
        sys.exit(1)

    # Connect to DB
    conn = get_conn()
    log(f"Connected to {DB_HOST}")

    # Select jurisdictions
    if args.jurisdiction:
        jurisdictions = {args.jurisdiction: CTX_JURISDICTIONS.get(args.jurisdiction, f"ID-{args.jurisdiction}")}
    else:
        jurisdictions = CTX_JURISDICTIONS

    total_loaded = 0
    results = []

    for jid, jname in jurisdictions.items():
        log(f"  [{jname}] (ID: {jid})...")
        permits = fetch_permits(session, jid, jname, days=days)
        loaded = load_to_hot_leads(conn, permits)
        total_loaded += loaded
        results.append((jname, len(permits), loaded))
        log(f"    → {len(permits)} fetched, {loaded} loaded")

    # Summary
    log("")
    log("=" * 60)
    log(f"COMPLETE — {total_loaded} total permits loaded")
    log("=" * 60)
    for name, fetched, loaded in sorted(results, key=lambda x: -x[2]):
        if loaded > 0:
            log(f"  {name}: {loaded}")

    # Final count
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM hot_leads WHERE source LIKE 'mgo_%'")
    mgo_total = cur.fetchone()[0]
    cur.close()
    log(f"\nTotal MGO hot_leads: {mgo_total:,}")

    conn.close()
    session.close()
    log("Done.")


if __name__ == "__main__":
    main()
