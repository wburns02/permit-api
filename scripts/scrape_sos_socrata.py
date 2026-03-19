#!/usr/bin/env python3
"""
Multi-state Secretary of State business entity scraper via Socrata Open Data APIs.

States with Socrata-hosted SoS data:
- Colorado: data.colorado.gov/resource/4ykn-tg5h (3M+ entities)
- More states to be added as discovered

Usage:
    python scrape_sos_socrata.py --state CO --db-host 100.122.216.15
    python scrape_sos_socrata.py --state all --db-host 100.122.216.15

Requires: pip install httpx psycopg2-binary
"""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import date, datetime

import httpx

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

BATCH_SIZE = 5000
PAGE_SIZE = 50000  # Socrata max per request
DELAY = 0.5

# State Socrata dataset configurations
# Format: state -> {url, resource_id, field_mapping}
SOCRATA_STATES = {
    "CO": {
        "base_url": "https://data.colorado.gov/resource/4ykn-tg5h.json",
        "source": "co_sos_socrata",
        "fields": {
            "entity_name": "entityname",
            "entity_type": "entitytype",
            "filing_number": "entityid",
            "status": "entitystatus",
            "formation_date": "entityformdate",
            "principal_address": ["principaladdress1", "principalcity", "principalstate", "principalzipcode"],
            "mailing_address": ["mailingaddress1", "mailingcity", "mailingstate", "mailingzipcode"],
            "registered_agent_name": ["agentfirstname", "agentlastname"],
            "registered_agent_address": ["agentprincipaladdress1", "agentprincipalcity", "agentprincipalstate", "agentprincipalzipcode"],
        },
    },
    # Add more states as we discover their Socrata endpoints
}

# Entity type mapping — normalize state-specific codes
ENTITY_TYPE_MAP = {
    "DLLC": "LLC",
    "LLC": "LLC",
    "DLLP": "LLP",
    "LLP": "LLP",
    "DLLLP": "LLLP",
    "CORPORATION": "Corporation",
    "PROFIT": "Corporation",
    "NONPROFIT": "Nonprofit",
    "DPC": "Corporation",
    "DFPC": "Corporation",
    "DLP": "LP",
    "LP": "LP",
    "FLLC": "LLC",  # Foreign LLC
    "FPC": "Corporation",  # Foreign Corp
    "FNPC": "Nonprofit",  # Foreign Nonprofit
    "FLP": "LP",
    "FLLP": "LLP",
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
        try:
            cur.execute(idx)
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()


def safe_date(val):
    if not val:
        return None
    try:
        # Socrata format: 2025-06-16T00:00:00.000
        return datetime.fromisoformat(val.replace("T00:00:00.000", "")).date()
    except (ValueError, AttributeError):
        pass
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except (ValueError, AttributeError):
        return None


def join_address(*parts):
    """Join address parts, filtering None/empty."""
    clean = [str(p).strip() for p in parts if p and str(p).strip()]
    return ", ".join(clean)[:500] if clean else None


def normalize_entity_type(raw_type):
    if not raw_type:
        return None
    return ENTITY_TYPE_MAP.get(raw_type.upper().strip(), raw_type[:50])


def scrape_state(state: str, config: dict, conn):
    """Scrape all entities from a state's Socrata API."""
    cur = conn.cursor()
    base_url = config["base_url"]
    source = config["source"]
    fm = config["fields"]  # field mapping

    total = 0
    offset = 0

    # Get total count first
    count_url = f"{base_url}?$select=count(*)"
    try:
        resp = httpx.get(count_url, timeout=30)
        total_records = int(resp.json()[0]["count"])
        print(f"  Total records available: {total_records:,}")
    except Exception as e:
        print(f"  Could not get count: {e}")
        total_records = None

    while True:
        url = f"{base_url}?$limit={PAGE_SIZE}&$offset={offset}&$order=:id"
        print(f"  Fetching offset {offset:,}...")

        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            print(f"  Error at offset {offset}: {e}")
            break

        if not records:
            break

        batch = []
        for r in records:
            name = r.get(fm["entity_name"], "")
            if not name:
                continue

            # Build agent name
            agent_name_field = fm.get("registered_agent_name")
            if isinstance(agent_name_field, list):
                agent_parts = [r.get(f, "") for f in agent_name_field]
                agent_name = " ".join(p for p in agent_parts if p).strip()
            else:
                agent_name = r.get(agent_name_field, "")

            # Build agent address
            agent_addr_field = fm.get("registered_agent_address")
            if isinstance(agent_addr_field, list):
                agent_addr = join_address(*[r.get(f) for f in agent_addr_field])
            else:
                agent_addr = r.get(agent_addr_field, "")

            # Build principal address
            principal_field = fm.get("principal_address")
            if isinstance(principal_field, list):
                principal_addr = join_address(*[r.get(f) for f in principal_field])
            else:
                principal_addr = r.get(principal_field, "")

            # Build mailing address
            mailing_field = fm.get("mailing_address")
            if isinstance(mailing_field, list):
                mailing_addr = join_address(*[r.get(f) for f in mailing_field])
            else:
                mailing_addr = r.get(mailing_field, "")

            batch.append((
                str(uuid.uuid4()),
                name[:500],
                normalize_entity_type(r.get(fm.get("entity_type", ""), "")),
                state,
                str(r.get(fm.get("filing_number", ""), ""))[:100] or None,
                (r.get(fm.get("status", ""), "") or "")[:50] or None,
                safe_date(r.get(fm.get("formation_date", ""))),
                None,  # dissolution_date — not in most Socrata datasets
                (agent_name or "")[:500] or None,
                (agent_addr or "")[:500] or None,
                (principal_addr or "")[:500] or None,
                (mailing_addr or "")[:500] or None,
                None,  # officers — not in Socrata bulk data
                source,
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
            pct = f" ({total*100//total_records}%)" if total_records else ""
            print(f"    Loaded {total:,}{pct}")

        offset += PAGE_SIZE
        if len(records) < PAGE_SIZE:
            break
        time.sleep(DELAY)

    cur.close()
    return total


def main():
    global DB_HOST

    parser = argparse.ArgumentParser(description="Scrape SoS business entities via Socrata APIs")
    parser.add_argument("--state", default="all",
                        choices=["all"] + list(SOCRATA_STATES.keys()),
                        help="State to scrape (or 'all')")
    parser.add_argument("--db-host", default=DB_HOST)
    args = parser.parse_args()

    DB_HOST = args.db_host

    conn = get_conn()
    ensure_table(conn)

    states = list(SOCRATA_STATES.keys()) if args.state == "all" else [args.state]
    grand_total = 0

    for state in states:
        config = SOCRATA_STATES[state]
        print(f"\n=== Scraping {state} ({config['source']}) ===")
        try:
            count = scrape_state(state, config, conn)
            grand_total += count
            print(f"  {state}: {count:,} entities loaded")
        except Exception as e:
            print(f"  ERROR: {e}")
            conn.rollback()

    conn.close()
    print(f"\n{'='*50}")
    print(f"Grand total: {grand_total:,} entities scraped across {len(states)} states")


if __name__ == "__main__":
    main()
