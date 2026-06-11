#!/usr/bin/env python3
"""
Bridge: hot_leads → permits table

Copies fresh records from hot_leads into the partitioned permits table
so the PermitLookup website shows current data.

Runs daily after hot_leads loader and enrichment.
Cron suggestion: 15 6 * * * cd /home/will/permit-api-live && python3 scripts/bridge_hot_leads_to_permits.py --db-host 100.122.216.15 >> /tmp/bridge_hot_leads.log 2>&1

Usage:
    python3 scripts/bridge_hot_leads_to_permits.py --db-host 100.122.216.15
    python3 scripts/bridge_hot_leads_to_permits.py --db-host 100.122.216.15 --days 30
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta

try:
    import psycopg2
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

# Burns Industries Layer 4 emitter — fires `permitlookup.permit.detected`
# after each successful bridge INSERT. Gated by BURNS_L4_EMIT_ENABLED env
# (default OFF). If the import or the emit raises, we swallow it so the
# pipeline never breaks.
try:
    # Add repo root to sys.path so `burns_events` resolves when this script
    # is invoked as `python3 scripts/bridge_hot_leads_to_permits.py` from
    # the repo root (matches daily_enrichment_pipeline.sh cron).
    _REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _REPO_ROOT not in sys.path:
        sys.path.insert(0, _REPO_ROOT)
    from burns_events import emit_permit_detected, is_enabled as _burns_l4_is_enabled
    _BURNS_L4_AVAILABLE = True
except Exception as _burns_l4_import_exc:  # noqa: BLE001
    emit_permit_detected = None  # type: ignore[assignment]
    _burns_l4_is_enabled = lambda: False  # type: ignore[assignment]
    _BURNS_L4_AVAILABLE = False


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}")


# Map hot_leads.permit_type / work_class onto the Burns L4 trade enum.
# Anything we don't recognise falls back to "other" (the schema's safe value).
_BURNS_TRADE_KEYWORDS = (
    ("septic", "septic"),
    ("ossf", "septic"),
    ("electric", "electrical"),
    ("plumb", "plumbing"),
    ("roof", "roofing"),
    ("hvac", "hvac"),
    ("mechanical", "hvac"),
    ("a/c", "hvac"),
)


def _burns_l4_classify_trade(*parts):
    haystack = " ".join((p or "").lower() for p in parts)
    for needle, trade in _BURNS_TRADE_KEYWORDS:
        if needle in haystack:
            return trade
    return "other"


def _burns_l4_county_slug(county):
    if not county:
        return "unknown"
    return (
        county.strip().lower()
        .replace(" county", "")
        .replace(" parish", "")
        .replace(" ", "_")
        .replace("'", "")
        .replace(".", "")
    )


def _burns_l4_emit_row(row, state):
    """Fire one permitlookup.permit.detected event. NEVER raises.

    `row` is the tuple returned by the bridge INSERT ... RETURNING clause:
      (permit_number, address, city, state_code, county, owner_name, project_type, work_type)
    """
    if not _BURNS_L4_AVAILABLE:
        return
    if not _burns_l4_is_enabled():
        return
    try:
        permit_number, address, _city, state_code, county, owner_name, project_type, work_type = row
        if not permit_number or not address:
            return
        st = (state_code or state or "TX").upper()
        permit_id = f"permit:{st.lower()}-{_burns_l4_county_slug(county)}-{permit_number}"
        emit_permit_detected(
            permit_id=permit_id,
            address=address,
            trade=_burns_l4_classify_trade(project_type, work_type),
            county=county or "Unknown",
            state=st,
            owner_name_raw=owner_name or "Unknown",
            permit_number=str(permit_number),
        )
    except Exception as exc:  # noqa: BLE001
        # The emitter already catches its own errors. This is belt-and-suspenders
        # so a future refactor can't turn an emit bug into a bridge outage.
        logging.getLogger("burns.l4.bridge").warning(
            "burns_l4 emit failed (non-fatal): %s", exc
        )


def get_conn(host):
    return psycopg2.connect(host=host, port="5432", dbname="permits", user="will")


def bridge(host, days, batch_size=5000):
    conn = get_conn(host)
    conn.autocommit = False
    cur = conn.cursor()

    since = (datetime.now() - timedelta(days=days)).date()
    log(f"Bridging hot_leads → permits for records since {since}")

    # Get states that have partitions
    cur.execute("""
        SELECT DISTINCT state FROM hot_leads
        WHERE issue_date >= %s AND state IS NOT NULL
        ORDER BY state
    """, (since,))
    states = [r[0] for r in cur.fetchall()]
    log(f"Found {len(states)} states with fresh data")

    total_inserted = 0
    burns_emit_on = _BURNS_L4_AVAILABLE and _burns_l4_is_enabled()
    if burns_emit_on:
        log("Burns L4 emit ENABLED — will fire permitlookup.permit.detected per inserted row")

    for state in states:
        # Use a temp approach: insert and let partition routing handle it
        # Skip records that already exist by permit_number + source.
        # RETURNING surfaces the inserted rows so the Burns L4 emitter can
        # fan out one CloudEvent per new permit. When the L4 flag is OFF
        # the returned rows are simply discarded.
        cur.execute("""
            INSERT INTO permits (permit_number, address, city, state_code, zip_code,
                county, lat, lng, project_type, work_type, description, status,
                date_created, owner_name, applicant_name, source)
            SELECT
                h.permit_number, h.address, h.city, h.state, h.zip,
                h.county, h.lat, h.lng, h.permit_type, h.work_class,
                h.description, h.status, h.issue_date,
                COALESCE(h.owner_name, h.contractor_name), h.applicant_name,
                'bridge_' || h.source
            FROM hot_leads h
            WHERE h.state = %s
              AND h.issue_date >= %s
              AND h.address IS NOT NULL
              AND h.permit_number IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM permits p
                  WHERE p.permit_number = h.permit_number
                    AND p.state_code = h.state
                    AND p.source LIKE 'bridge_%%'
              )
            LIMIT %s
            RETURNING permit_number, address, city, state_code, county,
                      owner_name, project_type, work_type
        """, (state, since, batch_size))

        inserted_rows = cur.fetchall() if burns_emit_on else []
        inserted = cur.rowcount
        if inserted > 0:
            conn.commit()
            total_inserted += inserted
            log(f"  {state}: +{inserted:,} permits")
            if burns_emit_on:
                emit_count = 0
                for row in inserted_rows:
                    _burns_l4_emit_row(row, state)
                    emit_count += 1
                log(f"  {state}: burns_l4 emitted {emit_count:,} permit.detected events")
        else:
            conn.rollback()

    # Report
    log(f"\nTotal bridged: {total_inserted:,} new permits")

    cur.execute("""
        SELECT state_code, COUNT(*), MAX(date_created)::date as latest
        FROM permits
        WHERE source LIKE 'bridge_%%'
        GROUP BY state_code
        ORDER BY COUNT(*) DESC
        LIMIT 20
    """)
    log("\nBridged permits by state:")
    for row in cur.fetchall():
        log(f"  {row[0]}: {row[1]:,} (latest: {row[2]})")

    cur.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Bridge hot_leads → permits")
    parser.add_argument("--db-host", default="100.122.216.15")
    parser.add_argument("--days", type=int, default=14, help="How many days back to bridge")
    parser.add_argument("--batch-size", type=int, default=10000)
    args = parser.parse_args()

    bridge(args.db_host, args.days, args.batch_size)


if __name__ == "__main__":
    main()
