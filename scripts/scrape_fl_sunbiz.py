#!/usr/bin/env python3
"""
Florida Sunbiz (Secretary of State) — Business entity scraper.

Scrapes Florida Division of Corporations (sunbiz.org) for LLCs, Corps, LPs.
Florida has ~4M+ active business entities.

Usage:
    python scrape_fl_sunbiz.py --pages 100 --db-host 100.122.216.15
    python scrape_fl_sunbiz.py --search "HOLDINGS LLC" --db-host 100.122.216.15

Requires: pip install httpx psycopg2-binary beautifulsoup4 lxml
"""

import argparse
import json
import os
import re
import sys
import time
import uuid
from datetime import date, datetime

import httpx
from bs4 import BeautifulSoup

try:
    import psycopg2
    from psycopg2.extras import execute_values, Json
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "permits")
DB_USER = os.getenv("DB_USER", "will")

BASE_URL = "https://search.sunbiz.org"
BATCH_SIZE = 500
DELAY = 0.5  # Be respectful

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER
    )


def ensure_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS business_entities (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            entity_name VARCHAR(500) NOT NULL,
            entity_type VARCHAR(50),
            state VARCHAR(2) NOT NULL,
            filing_number VARCHAR(100),
            status VARCHAR(50),
            formation_date DATE,
            dissolution_date DATE,
            registered_agent_name VARCHAR(500),
            registered_agent_address VARCHAR(500),
            principal_address VARCHAR(500),
            mailing_address VARCHAR(500),
            officers JSONB,
            source VARCHAR(50) NOT NULL,
            scraped_at DATE
        )
    """)
    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_entity_name ON business_entities (entity_name)",
        "CREATE INDEX IF NOT EXISTS ix_entity_filing ON business_entities (filing_number, state)",
        "CREATE INDEX IF NOT EXISTS ix_entity_state ON business_entities (state, entity_type)",
        "CREATE INDEX IF NOT EXISTS ix_entity_agent ON business_entities (registered_agent_name)",
    ]:
        cur.execute(idx)
    conn.commit()
    cur.close()


def safe_date(text):
    if not text:
        return None
    text = text.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_detail_page(html: str) -> dict:
    """Parse a Sunbiz entity detail page."""
    soup = BeautifulSoup(html, "lxml")
    data = {
        "entity_name": None,
        "entity_type": None,
        "filing_number": None,
        "status": None,
        "formation_date": None,
        "dissolution_date": None,
        "registered_agent_name": None,
        "registered_agent_address": None,
        "principal_address": None,
        "mailing_address": None,
        "officers": [],
    }

    # Entity name is usually in a large heading or specific div
    detail_section = soup.find("div", class_="detailSection")
    if detail_section:
        name_span = detail_section.find("span", class_="documentDataContent")
        if name_span:
            data["entity_name"] = name_span.get_text(strip=True)

    # Look for the main info table
    for row in soup.select("div.detailSection span"):
        text = row.get_text(strip=True)
        label = row.find_previous("label")
        if not label:
            continue
        label_text = label.get_text(strip=True).lower()

        if "document number" in label_text:
            data["filing_number"] = text
        elif "fei/ein" in label_text:
            pass  # Skip EIN for privacy
        elif "date filed" in label_text:
            data["formation_date"] = safe_date(text)
        elif "effective date" in label_text and not data["formation_date"]:
            data["formation_date"] = safe_date(text)
        elif "state" in label_text and "status" not in label_text:
            pass
        elif "status" in label_text:
            data["status"] = text
        elif "last event" in label_text:
            if "DISSOLUTION" in text.upper() or "WITHDRAWAL" in text.upper():
                data["status"] = data.get("status") or "Dissolved"
        elif "event date" in label_text and data.get("status", "").lower() in ("dissolved", "inactive"):
            data["dissolution_date"] = safe_date(text)

    # Entity type from the title/heading
    title = soup.find("title")
    if title:
        title_text = title.get_text()
        if "LLC" in title_text or "Limited Liability" in title_text:
            data["entity_type"] = "LLC"
        elif "Corporation" in title_text or "Corp" in title_text:
            data["entity_type"] = "Corporation"
        elif "Limited Partnership" in title_text or " LP" in title_text:
            data["entity_type"] = "LP"
        elif "LLP" in title_text:
            data["entity_type"] = "LLP"

    # Fallback: detect type from entity name
    if not data["entity_type"] and data["entity_name"]:
        name = data["entity_name"].upper()
        if "LLC" in name or "L.L.C" in name:
            data["entity_type"] = "LLC"
        elif "INC" in name or "CORP" in name:
            data["entity_type"] = "Corporation"
        elif " LP" in name or "L.P." in name:
            data["entity_type"] = "LP"

    # Registered Agent section
    agent_section = None
    for h2 in soup.find_all(["h2", "span"]):
        if "registered agent" in h2.get_text(strip=True).lower():
            agent_section = h2.find_next("div")
            break

    if agent_section:
        spans = agent_section.find_all("span")
        if spans:
            data["registered_agent_name"] = spans[0].get_text(strip=True)
            addr_parts = [s.get_text(strip=True) for s in spans[1:] if s.get_text(strip=True)]
            data["registered_agent_address"] = ", ".join(addr_parts)[:500] if addr_parts else None

    # Principal Address section
    for h2 in soup.find_all(["h2", "span"]):
        if "principal address" in h2.get_text(strip=True).lower():
            addr_div = h2.find_next("div")
            if addr_div:
                parts = [s.get_text(strip=True) for s in addr_div.find_all("span") if s.get_text(strip=True)]
                data["principal_address"] = ", ".join(parts)[:500] if parts else None
            break

    # Mailing Address section
    for h2 in soup.find_all(["h2", "span"]):
        if "mailing address" in h2.get_text(strip=True).lower():
            addr_div = h2.find_next("div")
            if addr_div:
                parts = [s.get_text(strip=True) for s in addr_div.find_all("span") if s.get_text(strip=True)]
                data["mailing_address"] = ", ".join(parts)[:500] if parts else None
            break

    # Officers/Directors section
    for h2 in soup.find_all(["h2", "span"]):
        if "officer" in h2.get_text(strip=True).lower() or "director" in h2.get_text(strip=True).lower():
            officer_div = h2.find_next("div")
            if officer_div:
                # Officers are typically listed in groups
                current_officer = {}
                for span in officer_div.find_all("span"):
                    text = span.get_text(strip=True)
                    if not text:
                        continue
                    # Title lines typically contain "President", "Director", "Manager", etc.
                    title_keywords = ["president", "director", "manager", "member", "secretary",
                                      "treasurer", "agent", "officer", "vice", "ceo", "cfo", "authorized"]
                    if any(kw in text.lower() for kw in title_keywords) and len(text) < 50:
                        if current_officer.get("name"):
                            data["officers"].append(current_officer)
                        current_officer = {"title": text}
                    elif not current_officer.get("name") and text and not text[0].isdigit():
                        current_officer["name"] = text
                    elif text:
                        current_officer.setdefault("address", "")
                        if current_officer["address"]:
                            current_officer["address"] += ", " + text
                        else:
                            current_officer["address"] = text

                if current_officer.get("name"):
                    data["officers"].append(current_officer)
            break

    return data


def scrape_search_page(client: httpx.Client, search_name: str, page_num: int = 1) -> list[str]:
    """Search Sunbiz and return detail page URLs."""
    url = f"{BASE_URL}/Inquiry/CorporationSearch/SearchByName"
    params = {
        "searchNameOrder": search_name,
        "searchTypeOrder": "Entity Name",
        "Page": page_num,
    }

    try:
        resp = client.get(url, params=params, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  Search error: {e}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")
    links = []
    for a in soup.select("a[href*='/Inquiry/CorporationSearch/SearchResultDetail']"):
        href = a.get("href", "")
        if href:
            full_url = BASE_URL + href if href.startswith("/") else href
            links.append(full_url)

    return links


def scrape_entity(client: httpx.Client, detail_url: str) -> dict | None:
    """Scrape a single entity detail page."""
    try:
        resp = client.get(detail_url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  Detail error: {e}")
        return None

    data = parse_detail_page(resp.text)
    if not data["entity_name"] and not data["filing_number"]:
        return None

    data["state"] = "FL"
    data["source"] = "fl_sunbiz"
    data["scraped_at"] = date.today()
    return data


def scrape_by_prefix(client: httpx.Client, conn, prefix: str, max_pages: int = 10):
    """Scrape all entities starting with a given prefix."""
    cur = conn.cursor()
    total = 0

    for page_num in range(1, max_pages + 1):
        print(f"  Searching '{prefix}' page {page_num}...")
        urls = scrape_search_page(client, prefix, page_num)
        if not urls:
            break

        batch = []
        for url in urls:
            time.sleep(DELAY)
            data = scrape_entity(client, url)
            if not data or not data.get("entity_name"):
                continue

            batch.append((
                str(uuid.uuid4()),
                (data["entity_name"] or "")[:500],
                (data.get("entity_type") or "")[:50] or None,
                "FL",
                (data.get("filing_number") or "")[:100] or None,
                (data.get("status") or "")[:50] or None,
                data.get("formation_date"),
                data.get("dissolution_date"),
                (data.get("registered_agent_name") or "")[:500] or None,
                (data.get("registered_agent_address") or "")[:500] or None,
                (data.get("principal_address") or "")[:500] or None,
                (data.get("mailing_address") or "")[:500] or None,
                Json(data.get("officers") or []),
                "fl_sunbiz",
                date.today(),
            ))

        if batch:
            execute_values(cur, """
                INSERT INTO business_entities (id, entity_name, entity_type, state,
                    filing_number, status, formation_date, dissolution_date,
                    registered_agent_name, registered_agent_address,
                    principal_address, mailing_address, officers, source, scraped_at)
                VALUES %s ON CONFLICT DO NOTHING
            """, batch)
            conn.commit()
            total += len(batch)
            print(f"    Saved {len(batch)} entities (total: {total})")

    cur.close()
    return total


def main():
    global DB_HOST

    parser = argparse.ArgumentParser(description="Scrape Florida Sunbiz business entities")
    parser.add_argument("--search", default=None, help="Specific search term")
    parser.add_argument("--pages", type=int, default=5, help="Max pages per search prefix")
    parser.add_argument("--db-host", default=DB_HOST)
    parser.add_argument("--prefixes", default=None,
                        help="Comma-separated prefixes to search (default: A-Z + common LLC names)")
    args = parser.parse_args()

    DB_HOST = args.db_host

    conn = get_conn()
    ensure_table(conn)

    client = httpx.Client(headers=HEADERS, follow_redirects=True)
    grand_total = 0

    if args.search:
        # Single search
        prefixes = [args.search]
    elif args.prefixes:
        prefixes = [p.strip() for p in args.prefixes.split(",")]
    else:
        # Default: search common LLC name patterns
        prefixes = [
            "HOLDINGS LLC", "PROPERTIES LLC", "INVESTMENTS LLC", "MANAGEMENT LLC",
            "ENTERPRISES LLC", "VENTURES LLC", "CAPITAL LLC", "REALTY LLC",
            "DEVELOPMENT LLC", "CONSTRUCTION LLC", "CONSULTING LLC", "SERVICES LLC",
            "GROUP LLC", "PARTNERS LLC", "ASSOCIATES LLC", "SOLUTIONS LLC",
        ]

    for prefix in prefixes:
        print(f"\n=== Searching: {prefix} ===")
        count = scrape_by_prefix(client, conn, prefix, max_pages=args.pages)
        grand_total += count

    client.close()
    conn.close()
    print(f"\n{'='*50}")
    print(f"Grand total: {grand_total} FL entities scraped")


if __name__ == "__main__":
    main()
