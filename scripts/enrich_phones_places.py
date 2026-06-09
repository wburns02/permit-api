#!/usr/bin/env python3
"""
Google Places Phone Enrichment for A-grade Hot Leads.

Looks up contractor phone numbers via Google Places API for high-value
leads that have a company name but no phone. Uses two-step lookup:
  1. Text Search → get place_id
  2. Place Details → get formatted_phone_number

Caches results by (company, city, state) to avoid duplicate API calls.

Usage:
    python3 enrich_phones_places.py --limit 50 --dry-run
    python3 enrich_phones_places.py --limit 200
    python3 enrich_phones_places.py --db-host 100.122.216.15 --limit 500
"""

import argparse
import os
import sys
import time
from collections import defaultdict

import httpx
import psycopg2
import psycopg2.extras

# ── Config ────────────────────────────────────────────────────────────────
DB_HOST_DEFAULT = "100.122.216.15"
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

PLACES_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# Rate limiting: Google allows 10 QPS but we stay conservative
QPS_LIMIT = 5
REQUEST_INTERVAL = 1.0 / QPS_LIMIT  # 200ms between requests


def get_db_conn(db_host: str):
    return psycopg2.connect(
        host=db_host,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
    )


def fetch_a_grade_leads(conn, limit: int) -> list[dict]:
    """
    Fetch A-grade leads missing contractor_phone but having contractor_company.
    Returns list of dicts with id, contractor_company, city, state.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT id, contractor_company, city, state
        FROM hot_leads
        WHERE lead_score = 'A'
          AND contractor_phone IS NULL
          AND contractor_company IS NOT NULL
          AND contractor_company != ''
          AND city IS NOT NULL
          AND state IS NOT NULL
        ORDER BY issue_date DESC NULLS LAST
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    return [dict(r) for r in rows]


def build_cache_key(company: str, city: str, state: str) -> str:
    """Normalize company+city+state into a cache key."""
    return f"{company.strip().lower()}|{city.strip().lower()}|{state.strip().upper()}"


def search_place(client: httpx.Client, api_key: str, company: str, city: str, state: str) -> str | None:
    """
    Step 1: Text Search to find place_id.
    Returns place_id or None.
    """
    query = f"{company} {city} {state} contractor"
    resp = client.get(PLACES_TEXT_SEARCH_URL, params={
        "query": query,
        "key": api_key,
    }, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "OK" or not data.get("results"):
        return None

    return data["results"][0].get("place_id")


def get_place_phone(client: httpx.Client, api_key: str, place_id: str) -> str | None:
    """
    Step 2: Place Details to get phone number.
    Only requests the phone field to minimize cost.
    """
    resp = client.get(PLACES_DETAILS_URL, params={
        "place_id": place_id,
        "fields": "formatted_phone_number",
        "key": api_key,
    }, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    result = data.get("result", {})
    return result.get("formatted_phone_number")


def update_phone(conn, lead_ids: list, phone: str):
    """Update contractor_phone for a list of lead IDs."""
    if not lead_ids:
        return
    cur = conn.cursor()
    cur.execute("""
        UPDATE hot_leads
        SET contractor_phone = %s
        WHERE id = ANY(%s)
    """, (phone, lead_ids))
    conn.commit()
    cur.close()


def main():
    parser = argparse.ArgumentParser(
        description="Enrich A-grade hot leads with Google Places phone numbers"
    )
    parser.add_argument("--db-host", default=DB_HOST_DEFAULT, help="Database host")
    parser.add_argument("--limit", type=int, default=100, help="Max leads to process (default: 100)")
    parser.add_argument("--dry-run", action="store_true", help="Query and lookup but don't update DB")
    args = parser.parse_args()

    api_key = os.environ.get("GOOGLE_PLACES_API_KEY")
    if not api_key:
        print("ERROR: Set GOOGLE_PLACES_API_KEY environment variable")
        sys.exit(1)

    print(f"Config: db_host={args.db_host}, limit={args.limit}, dry_run={args.dry_run}")

    # ── Fetch leads ──────────────────────────────────────────────────────
    conn = get_db_conn(args.db_host)
    leads = fetch_a_grade_leads(conn, args.limit)
    print(f"Fetched {len(leads)} A-grade leads needing phone enrichment")

    if not leads:
        print("No leads to process.")
        conn.close()
        return

    # ── Group by (company, city, state) for caching ──────────────────────
    groups: dict[str, list] = defaultdict(list)
    for lead in leads:
        key = build_cache_key(lead["contractor_company"], lead["city"], lead["state"])
        groups[key].append(lead)

    unique_lookups = len(groups)
    print(f"Unique company+city+state combinations: {unique_lookups}")
    print(f"Estimated max API calls: {unique_lookups * 2} (text search + details each)")

    # ── Lookup phones via Google Places ──────────────────────────────────
    cache: dict[str, str | None] = {}  # cache_key -> phone or None
    stats = {
        "api_calls": 0,
        "phones_found": 0,
        "cache_hits": 0,
        "leads_updated": 0,
        "errors": 0,
    }

    client = httpx.Client()

    try:
        for i, (cache_key, group_leads) in enumerate(groups.items()):
            lead = group_leads[0]  # representative lead for this group
            company = lead["contractor_company"]
            city = lead["city"]
            state = lead["state"]

            # Check cache (from earlier iteration in same run)
            if cache_key in cache:
                stats["cache_hits"] += len(group_leads)
                phone = cache[cache_key]
                if phone and not args.dry_run:
                    ids = [l["id"] for l in group_leads]
                    update_phone(conn, ids, phone)
                    stats["leads_updated"] += len(group_leads)
                continue

            # Step 1: Text Search
            print(f"  [{i+1}/{unique_lookups}] Searching: {company} | {city}, {state} ...", end="", flush=True)
            try:
                time.sleep(REQUEST_INTERVAL)
                place_id = search_place(client, api_key, company, city, state)
                stats["api_calls"] += 1
            except Exception as e:
                print(f" ERROR (search): {e}")
                stats["errors"] += 1
                cache[cache_key] = None
                continue

            if not place_id:
                print(" no results")
                cache[cache_key] = None
                continue

            # Step 2: Place Details
            try:
                time.sleep(REQUEST_INTERVAL)
                phone = get_place_phone(client, api_key, place_id)
                stats["api_calls"] += 1
            except Exception as e:
                print(f" ERROR (details): {e}")
                stats["errors"] += 1
                cache[cache_key] = None
                continue

            cache[cache_key] = phone

            if phone:
                print(f" FOUND: {phone}")
                stats["phones_found"] += 1
                if not args.dry_run:
                    ids = [l["id"] for l in group_leads]
                    update_phone(conn, ids, phone)
                    stats["leads_updated"] += len(group_leads)
                else:
                    stats["leads_updated"] += len(group_leads)  # would-update count
            else:
                print(" no phone in listing")

    finally:
        client.close()

    # ── Report ───────────────────────────────────────────────────────────
    total_leads = len(leads)
    cache_hit_rate = (stats["cache_hits"] / total_leads * 100) if total_leads > 0 else 0

    print("\n" + "=" * 60)
    print("ENRICHMENT RESULTS")
    print("=" * 60)
    print(f"  Leads processed:     {total_leads}")
    print(f"  Unique lookups:      {unique_lookups}")
    print(f"  API calls made:      {stats['api_calls']}")
    print(f"  Phones found:        {stats['phones_found']}")
    print(f"  Cache hits (leads):  {stats['cache_hits']} ({cache_hit_rate:.1f}%)")
    print(f"  Leads updated:       {stats['leads_updated']}" + (" (dry-run)" if args.dry_run else ""))
    print(f"  Errors:              {stats['errors']}")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()
