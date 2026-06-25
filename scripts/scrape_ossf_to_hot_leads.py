#!/usr/bin/env python3
"""
OSSF / Septic → hot_leads adapter — rural new-build leading-indicator.

Surfaces TCEQ On-Site Sewage Facility (OSSF / septic) permits already held in
the warehouse table `ossf_permits_tx` into the `hot_leads` lead feed as a
**rural new-build trigger**. In unincorporated / rural Texas there is often no
city building permit at all — the OSSF permit is the earliest public record
that a new house is going up (you cannot build a rural home without an approved
septic system first). For a roofing / new-build lead platform this is the rural
analogue of the 911 address-point trigger.

Why this is generic
--------------------
`ossf_permits_tx` is a canonical, already-normalized table (county /
permit_number / address_norm / work_type / status / created_date / project_name).
The county registry below maps a county key to its `county` value in that table
plus a source tag. Adding a county = one COUNTIES entry, no new code. The same
adapter trivially generalizes to every TX county whose OSSF issuance we hold.

What we DO and DON'T have for Brazoria
--------------------------------------
Brazoria County is a TCEQ **Authorized Agent**: it issues and holds its own OSSF
permits with **no state-level API or bulk feed**. We currently hold **0** Brazoria
rows in `ossf_permits_tx` (verified). The fresh Brazoria path is a **monthly
county Public Information Act request** to the Brazoria County Environmental
Health / OSSF office (no automated feed exists). This adapter therefore wires in
the Central-Texas OSSF inventory we DO hold (Travis, Williamson, Bastrop, Ellis,
Grayson, Fannin, Hays, Cooke — ~165K rows) and documents Brazoria as PIA-only.
We do NOT fabricate a Brazoria feed.

hot_leads landing
-----------------
OSSF rows carry a real permit_number (e.g. `OSSF-2025-4523`), so they upsert on
the standard UNIQUE (permit_number, source) index and ARE eligible for the
building-permit bridge. permit_type is stamped "SEPTIC (OSSF)" and work_class
"NEW-BUILD TRIGGER (OSSF)" so downstream trade-classification routes them to the
septic / new-build buckets. owner_name is taken from project_name.

Usage:
    python3 scrape_ossf_to_hot_leads.py --county hays
    python3 scrape_ossf_to_hot_leads.py --county hays --since-days 365
    python3 scrape_ossf_to_hot_leads.py --all
    python3 scrape_ossf_to_hot_leads.py --county hays --full       # ignore high-water mark
    python3 scrape_ossf_to_hot_leads.py --county hays --dry-run
    python3 scrape_ossf_to_hot_leads.py --list

Database target (defaults):
    host=100.122.216.15 dbname=permits user=will
    reads ossf_permits_tx, upserts hot_leads ON CONFLICT (permit_number, source),
    writes a hot_leads_sources ledger row per run (drives /v1/freshness).

Cron (PREPARED — DO NOT enable without sign-off):
    # OSSF septic new-build triggers — daily 05:25 CT, incremental from ledger
    25 5 * * * cd /home/will/permit-api-live && python3 scripts/scrape_ossf_to_hot_leads.py --all >> /tmp/ossf_hot_leads.log 2>&1
"""

import argparse
import sys
import uuid
from datetime import date, datetime, timedelta
from typing import Optional

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

DB_HOST_DEFAULT = "100.122.216.15"
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

# County registry. county_value matches ossf_permits_tx.county exactly.
# Add a county we hold = one entry. No code changes.
COUNTIES = {
    "travis":     {"county_value": "Travis",     "county": "Travis",     "state": "TX", "source": "ossf_travis"},
    "williamson": {"county_value": "Williamson", "county": "Williamson", "state": "TX", "source": "ossf_williamson"},
    "bastrop":    {"county_value": "Bastrop",    "county": "Bastrop",    "state": "TX", "source": "ossf_bastrop"},
    "ellis":      {"county_value": "Ellis",      "county": "Ellis",      "state": "TX", "source": "ossf_ellis"},
    "grayson":    {"county_value": "Grayson",    "county": "Grayson",    "state": "TX", "source": "ossf_grayson"},
    "fannin":     {"county_value": "Fannin",     "county": "Fannin",     "state": "TX", "source": "ossf_fannin"},
    "hays":       {"county_value": "Hays",       "county": "Hays",       "state": "TX", "source": "ossf_hays"},
    "cooke":      {"county_value": "Cooke",      "county": "Cooke",      "state": "TX", "source": "ossf_cooke"},
}

# Counties we do NOT hold OSSF issuance for and the documented fresh path.
# These are surfaced by --list so the gap is explicit, never silently implied.
NOT_HELD = {
    "brazoria": (
        "Brazoria County is a TCEQ Authorized Agent — it issues and holds its own "
        "OSSF permits with NO state API or bulk feed. 0 rows held. Fresh path = "
        "monthly Public Information Act request to Brazoria County Environmental "
        "Health (OSSF program). No automated feed exists."
    ),
}


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn(db_host):
    return psycopg2.connect(host=db_host, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def ensure_sources_table(conn):
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hot_leads_sources (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source_name TEXT NOT NULL,
                state TEXT,
                file_name TEXT,
                records_loaded INTEGER DEFAULT 0,
                records_skipped INTEGER DEFAULT 0,
                latest_issue_date DATE,
                loaded_at TIMESTAMPTZ DEFAULT NOW(),
                error_message TEXT
            )
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"  (sources table ensure: {e})")
    finally:
        cur.close()


def get_high_water_mark(conn, source_name) -> Optional[date]:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT MAX(latest_issue_date) FROM hot_leads_sources WHERE source_name = %s",
            (source_name,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None
    finally:
        cur.close()


def record_source(conn, source_name, state, loaded, latest_date, error=None):
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO hot_leads_sources
               (source_name, state, file_name, records_loaded, records_skipped, latest_issue_date, error_message)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (source_name, state, "ossf_permits_tx", loaded, 0, latest_date, error),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"  (record_source: {e})")
    finally:
        cur.close()


def fetch_ossf(conn, county_value, since_dt: Optional[date]):
    """Read OSSF rows for a county from the warehouse table, optionally newer
    than since_dt (on created_date). Returns list of dict rows."""
    cur = conn.cursor()
    try:
        cur.execute("SET statement_timeout = 120000")
        if since_dt is None:
            cur.execute(
                """SELECT permit_number, address_norm, address_raw, city, zip,
                          work_type, specific_use, description, status,
                          created_date, project_name, parcel_number
                   FROM ossf_permits_tx
                   WHERE county = %s AND permit_number IS NOT NULL
                   ORDER BY created_date DESC NULLS LAST""",
                (county_value,),
            )
        else:
            cur.execute(
                """SELECT permit_number, address_norm, address_raw, city, zip,
                          work_type, specific_use, description, status,
                          created_date, project_name, parcel_number
                   FROM ossf_permits_tx
                   WHERE county = %s AND permit_number IS NOT NULL
                     AND created_date >= %s
                   ORDER BY created_date DESC NULLS LAST""",
                (county_value, since_dt),
            )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        cur.close()


def normalize_ossf(raw, cfg):
    permit_number = (raw.get("permit_number") or "").strip()
    if not permit_number:
        return None
    address = (raw.get("address_norm") or raw.get("address_raw") or "").strip() or None
    work_type = raw.get("work_type")
    specific_use = raw.get("specific_use")
    description = raw.get("description") or work_type or specific_use
    owner = (raw.get("project_name") or "").strip() or None
    created = raw.get("created_date")
    parcel = raw.get("parcel_number")
    desc_bits = []
    if work_type:
        desc_bits.append(str(work_type))
    if parcel:
        desc_bits.append(f"Parcel: {parcel}")
    full_desc = " | ".join(desc_bits) if desc_bits else (str(description) if description else None)

    return {
        "permit_number": permit_number[:100],
        "permit_type": "SEPTIC (OSSF)",
        "work_class": "NEW-BUILD TRIGGER (OSSF)",
        "description": (full_desc[:500]) if full_desc else None,
        "address": (address[:200]) if address else None,
        "city": (str(raw.get("city")).strip().upper()[:100]) if raw.get("city") else None,
        "state": cfg["state"],
        "zip": (str(raw.get("zip")).strip()[:5]) if raw.get("zip") else None,
        "county": cfg["county"],
        "issue_date": created,
        "applied_date": created,
        "status": (str(raw.get("status"))[:80]) if raw.get("status") else None,
        "owner_name": (owner[:200]) if owner else None,
        "jurisdiction": f"{cfg['county']} County, {cfg['state']}",
        "source": cfg["source"],
    }


HOT_LEADS_COLS = [
    "id", "permit_number", "permit_type", "work_class", "description",
    "address", "city", "state", "zip", "county",
    "issue_date", "applied_date", "status", "owner_name",
    "jurisdiction", "source",
]


def upsert_hot_leads(conn, rows_in):
    if not rows_in:
        return 0
    deduped = {}
    for p in rows_in:
        if not p.get("permit_number"):
            continue
        deduped[(p["permit_number"], p["source"])] = p
    rows = [
        (
            str(uuid.uuid4()),
            p["permit_number"], p["permit_type"], p["work_class"], p["description"],
            p["address"], p["city"], p["state"], p["zip"], p["county"],
            p["issue_date"], p["applied_date"], p["status"], p["owner_name"],
            p["jurisdiction"], p["source"],
        )
        for p in deduped.values()
    ]
    sql = f"""
        INSERT INTO hot_leads ({', '.join(HOT_LEADS_COLS)})
        VALUES %s
        ON CONFLICT (permit_number, source) DO UPDATE SET
            permit_type = COALESCE(EXCLUDED.permit_type, hot_leads.permit_type),
            work_class = COALESCE(EXCLUDED.work_class, hot_leads.work_class),
            description = COALESCE(EXCLUDED.description, hot_leads.description),
            address = COALESCE(EXCLUDED.address, hot_leads.address),
            city = COALESCE(EXCLUDED.city, hot_leads.city),
            zip = COALESCE(EXCLUDED.zip, hot_leads.zip),
            county = COALESCE(EXCLUDED.county, hot_leads.county),
            issue_date = COALESCE(EXCLUDED.issue_date, hot_leads.issue_date),
            applied_date = COALESCE(EXCLUDED.applied_date, hot_leads.applied_date),
            status = COALESCE(EXCLUDED.status, hot_leads.status),
            owner_name = COALESCE(EXCLUDED.owner_name, hot_leads.owner_name),
            scraped_at = CURRENT_DATE
    """
    cur = conn.cursor()
    try:
        execute_values(cur, sql, rows, page_size=1000)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        log(f"  hot_leads upsert error: {e}")
        return 0
    finally:
        cur.close()


def run_county(county_key, cfg, conn, since_days, full, dry_run):
    log(f"[{county_key}] source={cfg['source']} county={cfg['county_value']}")
    since_dt = None
    if not full:
        hwm = get_high_water_mark(conn, cfg["source"]) if conn is not None else None
        if hwm is not None:
            since_dt = hwm - timedelta(days=1)
            log(f"  incremental from ledger high-water mark {hwm} (re-pull since {since_dt})")
        else:
            since_dt = date.today() - timedelta(days=since_days)
            log(f"  no ledger history — first run, look-back {since_days} days (since {since_dt})")

    raw = fetch_ossf(conn, cfg["county_value"], since_dt)
    normalized = [normalize_ossf(r, cfg) for r in raw]
    normalized = [p for p in normalized if p]
    log(f"  read {len(raw)} OSSF rows, normalized {len(normalized)}")

    latest_date = None
    for p in normalized:
        if p["issue_date"] and (latest_date is None or p["issue_date"] > latest_date):
            latest_date = p["issue_date"]

    loaded = 0
    if dry_run:
        log("  DRY RUN — not writing. Sample:")
        for p in normalized[:8]:
            log(f"    {p['issue_date']} | {p['permit_number']} | {p['address']} | "
                f"owner={p['owner_name']} | {p['status']}")
    elif normalized:
        loaded = upsert_hot_leads(conn, normalized)
        record_source(conn, cfg["source"], cfg["state"], loaded, latest_date)
        log(f"  upserted {loaded} into hot_leads; ledger latest_issue_date={latest_date}")
    else:
        log("  nothing new to load")
        if conn is not None:
            record_source(conn, cfg["source"], cfg["state"], 0, latest_date)

    return len(normalized), loaded


def main():
    parser = argparse.ArgumentParser(description="OSSF/septic → hot_leads adapter")
    parser.add_argument("--county", choices=list(COUNTIES.keys()), help="County to pull")
    parser.add_argument("--all", action="store_true", help="Pull every registered county")
    parser.add_argument("--since-days", type=int, default=365,
                        help="First-run look-back window in days (default 365)")
    parser.add_argument("--full", action="store_true",
                        help="Ignore high-water mark; pull all held rows")
    parser.add_argument("--db-host", default=DB_HOST_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    if args.list:
        print("Registered OSSF counties (held in ossf_permits_tx):")
        for k, v in COUNTIES.items():
            print(f"  {k:12s} {v['county']} County, {v['state']:2s}  source={v['source']}")
        print("\nNOT held (no automated feed — documented PIA path):")
        for k, reason in NOT_HELD.items():
            print(f"  {k:12s} — {reason}")
        return

    if not args.county and not args.all:
        parser.error("must provide --county or --all (or --list)")

    targets = [args.county] if args.county else list(COUNTIES.keys())

    log("=" * 64)
    log(f"OSSF → HOT_LEADS ADAPTER — {len(targets)} county/counties")
    log(f"Targets: {', '.join(targets)} | full={args.full} since_days={args.since_days}")
    log("=" * 64)

    conn = get_conn(args.db_host)
    if not args.dry_run:
        ensure_sources_table(conn)
    log(f"DB connected: {args.db_host}/{DB_NAME}")

    summary = []
    for ck in targets:
        try:
            fetched, loaded = run_county(
                ck, COUNTIES[ck], conn, args.since_days, args.full, args.dry_run
            )
            summary.append((ck, fetched, loaded))
        except Exception as e:
            log(f"  ERROR {ck}: {e}")
            conn.rollback()
            summary.append((ck, 0, 0))

    log("=" * 64)
    for ck, f, l in summary:
        log(f"  {ck:12s} normalized={f:>7} loaded={l:>7}")
    log("=" * 64)
    conn.close()
    log("Done.")


if __name__ == "__main__":
    main()
