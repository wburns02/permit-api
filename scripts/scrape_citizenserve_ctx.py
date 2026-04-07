#!/usr/bin/env python3
"""
Citizenserve CTX Scraper — pulls permits from San Marcos, Kyle, Buda and other
Central Texas cities that use the Citizenserve permit portal.

Loads directly into hot_leads on T430.

Usage:
    python3 scrape_citizenserve_ctx.py              # All CTX cities
    python3 scrape_citizenserve_ctx.py --city 427   # San Marcos only
    python3 scrape_citizenserve_ctx.py --days 30    # Last 30 days

Cron (daily 5:15 AM):
    15 5 * * * cd /home/will/permit-api-live && python3 scripts/scrape_citizenserve_ctx.py >> /tmp/citizenserve_ctx.log 2>&1
"""

import argparse
import json
import os
import re
import sys
import time
import uuid
from datetime import date, datetime, timedelta

import httpx

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("pip install psycopg2-binary")
    sys.exit(1)

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

# Citizenserve installations for Central Texas
CTX_CITIES = {
    427: {"name": "San Marcos", "state": "TX", "county": "Hays"},
    284: {"name": "Kyle", "state": "TX", "county": "Hays"},
    353: {"name": "Buda", "state": "TX", "county": "Hays"},
}

BASE_URL = "https://citizenserve.com/Portal/PortalController"


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def scrape_citizenserve(installation_id: int, city_info: dict, days: int = 7) -> list:
    """Scrape permits from a Citizenserve portal."""
    city = city_info["name"]
    state = city_info["state"]
    log(f"  Scraping {city}, {state} (ID: {installation_id}, last {days} days)")

    permits = []
    client = httpx.Client(timeout=30, follow_redirects=True)

    # Citizenserve public portal search
    try:
        # First, load the search page to get session
        search_url = f"{BASE_URL}?Action=showSearchPage&ctzPagePrefix=Portal_&installationID={installation_id}"
        resp = client.get(search_url)

        # Try the permit list endpoint
        list_url = f"{BASE_URL}?Action=showPortalPermitList&ctzPagePrefix=Portal_&installationID={installation_id}"
        resp = client.get(list_url)
        html = resp.text

        # Parse permit data from HTML
        # Citizenserve renders permits in table rows with class "listRow"
        row_pattern = re.compile(
            r'class="listRow[^"]*"[^>]*>(.*?)</tr>',
            re.DOTALL | re.IGNORECASE
        )
        cell_pattern = re.compile(r'<td[^>]*>(.*?)</td>', re.DOTALL | re.IGNORECASE)
        link_pattern = re.compile(r'permitNumber=([^&"]+)', re.IGNORECASE)

        rows = row_pattern.findall(html)
        log(f"  Found {len(rows)} rows in permit list")

        for row in rows:
            cells = cell_pattern.findall(row)
            cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]

            if len(cells) < 3:
                continue

            # Extract permit number from link
            permit_link = link_pattern.search(row)
            permit_number = permit_link.group(1) if permit_link else (cells[0] if cells else None)

            # Try to identify columns (varies by installation)
            permit = {
                "permit_number": permit_number,
                "city": city,
                "state": state,
                "source": f"citizenserve_{city.lower().replace(' ', '_')}",
            }

            # Map cells to fields based on position and content
            for i, cell in enumerate(cells):
                if not cell:
                    continue
                # Address detection (has numbers + street name)
                if re.match(r'^\d+\s+\w', cell) and len(cell) > 10 and not permit.get("address"):
                    permit["address"] = cell
                # Date detection
                elif re.match(r'\d{1,2}/\d{1,2}/\d{2,4}', cell) and not permit.get("issue_date"):
                    try:
                        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
                            try:
                                permit["issue_date"] = datetime.strptime(cell, fmt).date()
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pass
                # Status
                elif cell.lower() in ("issued", "approved", "active", "complete", "closed", "pending", "review"):
                    permit["status"] = cell
                # Description (longer text)
                elif len(cell) > 30 and not permit.get("description"):
                    permit["description"] = cell[:500]
                # Permit type (short codes)
                elif len(cell) < 30 and i < 3 and not permit.get("permit_type"):
                    permit["permit_type"] = cell

            if permit.get("address") or permit.get("permit_number"):
                permits.append(permit)

        # If HTML parsing didn't work well, try the detail pages
        if len(permits) < 3:
            log(f"  Low results from HTML ({len(permits)}), trying detail scrape...")
            # Get individual permit details
            detail_links = re.findall(r'Action=getPermitDetail[^"\']*permitNumber=([^&"\']+)', html)
            for pnum in detail_links[:50]:  # Cap at 50 per city
                try:
                    detail_url = f"{BASE_URL}?Action=getPermitDetail&ctzPagePrefix=Portal_&installationID={installation_id}&permitNumber={pnum}"
                    dresp = client.get(detail_url)
                    dhtml = dresp.text

                    permit = parse_permit_detail(dhtml, pnum, city, state)
                    if permit:
                        permits.append(permit)
                    time.sleep(0.5)  # Be polite
                except Exception as e:
                    log(f"  Detail error for {pnum}: {e}")

    except Exception as e:
        log(f"  Error scraping {city}: {e}")
    finally:
        client.close()

    log(f"  {city}: {len(permits)} permits found")
    return permits


def parse_permit_detail(html: str, permit_number: str, city: str, state: str) -> dict | None:
    """Parse a single permit detail page."""
    permit = {
        "permit_number": permit_number,
        "city": city,
        "state": state,
        "source": f"citizenserve_{city.lower().replace(' ', '_')}",
    }

    # Extract fields from label/value pairs
    patterns = {
        "address": r'(?:Address|Location|Site)\s*:?\s*</[^>]+>\s*<[^>]+>\s*([^<]+)',
        "permit_type": r'(?:Permit Type|Type|Category)\s*:?\s*</[^>]+>\s*<[^>]+>\s*([^<]+)',
        "description": r'(?:Description|Work Description|Scope)\s*:?\s*</[^>]+>\s*<[^>]+>\s*([^<]+)',
        "status": r'(?:Status)\s*:?\s*</[^>]+>\s*<[^>]+>\s*([^<]+)',
        "contractor_name": r'(?:Contractor|Builder|Applicant)\s*:?\s*</[^>]+>\s*<[^>]+>\s*([^<]+)',
        "valuation": r'(?:Valuation|Value|Cost|Estimated)\s*:?\s*</[^>]+>\s*<[^>]+>\s*\$?([0-9,.]+)',
    }

    for field, pattern in patterns.items():
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            val = match.group(1).strip()
            if val and val.lower() not in ('n/a', 'none', ''):
                if field == "valuation":
                    try:
                        permit[field] = float(val.replace(",", ""))
                    except ValueError:
                        pass
                else:
                    permit[field] = val

    # Extract date
    date_match = re.search(r'(?:Issue|Issued|Applied|Open)\s*(?:Date)?\s*:?\s*</[^>]+>\s*<[^>]+>\s*(\d{1,2}/\d{1,2}/\d{2,4})', html, re.IGNORECASE)
    if date_match:
        try:
            for fmt in ("%m/%d/%Y", "%m/%d/%y"):
                try:
                    permit["issue_date"] = datetime.strptime(date_match.group(1), fmt).date()
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    if permit.get("address"):
        return permit
    return None


def load_to_hot_leads(conn, permits: list) -> int:
    """Insert permits into hot_leads with dedup."""
    if not permits:
        return 0

    cur = conn.cursor()
    batch = []
    for p in permits:
        batch.append((
            str(uuid.uuid4()),
            p.get("permit_number"),
            p.get("permit_type"),
            None,  # work_class
            p.get("description"),
            p.get("address"),
            p.get("city"),
            p.get("state", "TX"),
            p.get("zip"),
            p.get("valuation"),
            None,  # sqft
            p.get("issue_date"),
            p.get("contractor_company"),
            p.get("contractor_name"),
            p.get("contractor_phone"),
            None,  # applicant_name
            None,  # applicant_phone
            p.get("source", "citizenserve"),
            p.get("source", "citizenserve"),
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
        loaded = len(batch)
    except Exception as e:
        conn.rollback()
        log(f"  Insert error: {e}")
        loaded = 0
    finally:
        cur.close()

    return loaded


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", type=int, help="Specific installation ID")
    parser.add_argument("--days", type=int, default=7, help="Days of history")
    args = parser.parse_args()

    log("=" * 50)
    log("CITIZENSERVE CTX SCRAPER")
    log("=" * 50)

    conn = get_conn()
    log(f"Connected to {DB_HOST}")

    cities = {args.city: CTX_CITIES[args.city]} if args.city else CTX_CITIES
    total_loaded = 0

    for install_id, city_info in cities.items():
        permits = scrape_citizenserve(install_id, city_info, days=args.days)
        loaded = load_to_hot_leads(conn, permits)
        total_loaded += loaded
        log(f"  → {loaded} loaded into hot_leads")

    conn.close()
    log(f"\nTotal: {total_loaded} permits loaded from {len(cities)} cities")
    log("Done.")


if __name__ == "__main__":
    main()
