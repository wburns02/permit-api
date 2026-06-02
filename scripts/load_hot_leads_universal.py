#!/usr/bin/env python3
"""
Universal Hot Leads Loader — loads ALL R730 scraper .ndjson outputs into hot_leads.

Scans /home/will/crown_scrapers/data/ for today's scraper outputs, extracts
permit-relevant records, fuzzy-maps fields to hot_leads schema, and bulk-inserts
into T430 PostgreSQL.

Usage:
    python3 load_hot_leads_universal.py                  # Today's files
    python3 load_hot_leads_universal.py --days 7         # Last 7 days
    python3 load_hot_leads_universal.py --dry-run        # Count only
    python3 load_hot_leads_universal.py --file /path.ndjson  # Single file
"""

import argparse
import json
import os
import re
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

# ── Config ────────────────────────────────────────────────────────────────
DATA_DIR = Path("/home/will/crown_scrapers/data")
DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"
BATCH_SIZE = 1000
MAX_DAYS_OLD = 90  # Only load records with issue_date within 90 days

# ── File filtering ────────────────────────────────────────────────────────
INCLUDE_PATTERNS = re.compile(
    r"permit|building|construction|code_enforcement|violation|inspection",
    re.IGNORECASE,
)
EXCLUDE_PATTERNS = re.compile(
    r"address_database|parcel|cadastre|water_dist|shellfish|land_records|"
    r"property_owner|buyout|assessed_value|zoning|flood|septic|census|"
    r"blight_ticket|derive|opengov|single_family.*middle|black_architect|"
    r"shpo.*building_data|buildings.*age|recertification",
    re.IGNORECASE,
)

# ── Fuzzy field mapping ──────────────────────────────────────────────────
FIELD_MAP = {
    "permit_number": [
        "permit_number", "record_id", "permitnumber", "permit_no", "case_number",
        "job__", "permit_num", "application_number", "folder_number",
    ],
    "address": [
        "address", "property_address", "location", "street_address", "site_address",
        "project_address", "work_location",
    ],
    "city": [
        "city", "property_city", "municipality", "borough", "jurisdiction_name",
    ],
    "zip": [
        "zip", "zip_code", "postal_code", "zipcode", "property_zip",
    ],
    "description": [
        "description", "project_description", "work_description", "scope_of_work",
        "job_description", "permit_description", "comments",
    ],
    "permit_type": [
        "permit_type", "record_type", "type_of_work", "b1_app_type_alias",
        "permit_category", "permit_class",
    ],
    "work_class": [
        "work_class", "construction_type", "record_type_type", "work_type",
        "typeofwork",
    ],
    "issue_date": [
        "issue_date", "date_opened", "issuance_date", "issued_date", "issuedate",
        "permitnumbercreateddate", "date_issued", "approval_date", "final_date",
        "applied_date",
    ],
    "valuation": [
        "valuation", "project_value", "estimated_cost", "value", "reported_cost",
        "total_cost", "job_value", "construction_cost", "projectvalue",
    ],
    "sqft": [
        "sqft", "total_floor_area", "square_feet", "sq_ft", "gross_area",
    ],
    "contractor_name": [
        "contractor_name", "contractor", "contractor_1_name", "builder_name",
    ],
    "contractor_phone": [
        "contractor_phone", "permittee_s_phone__", "contractor_phone_number",
    ],
    "contractor_company": [
        "contractor_company", "company", "business_name",
        "permittee_s_business_name", "builder_company",
    ],
    "applicant_name": [
        "applicant_name", "owner_name", "owner_s_business_name", "owner",
        "property_owner",
    ],
    "applicant_phone": [
        "applicant_phone", "owner_s_phone__", "owner_phone",
    ],
}


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def parse_date(val):
    """Try to parse a date from various formats."""
    if val is None or val == "":
        return None
    val = str(val).strip()

    # Epoch milliseconds (> 1e12)
    try:
        n = float(val)
        if n > 1e12:
            dt = datetime.fromtimestamp(n / 1000)
            if datetime(2015, 1, 1) <= dt <= datetime(2027, 1, 1):
                return dt.date()
        elif n > 1e9:
            dt = datetime.fromtimestamp(n)
            if datetime(2015, 1, 1) <= dt <= datetime(2027, 1, 1):
                return dt.date()
    except (ValueError, OSError):
        pass

    # String date formats
    for fmt in (
        "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f",
        "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y",
        "%Y/%m/%d", "%d-%b-%Y",
    ):
        try:
            dt = datetime.strptime(val[:26], fmt)
            if datetime(2015, 1, 1) <= dt <= datetime(2027, 1, 1):
                return dt.date()
        except (ValueError, IndexError):
            continue

    return None


def safe_float(val):
    if val is None or val == "":
        return None
    try:
        return float(str(val).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def fuzzy_map(raw: dict) -> dict:
    """Map raw_data fields to hot_leads schema using fuzzy matching."""
    # Normalize keys to lowercase
    lower = {k.lower(): v for k, v in raw.items()}
    result = {}

    for target_col, patterns in FIELD_MAP.items():
        for pat in patterns:
            if pat in lower and lower[pat] not in (None, "", "null", "NULL"):
                result[target_col] = lower[pat]
                break

    return result


def source_from_filename(fname: str) -> str:
    """Extract a source name from filename like 'ct_building_permits_20260403.ndjson'."""
    name = Path(fname).stem  # Remove .ndjson
    # Remove date suffix (YYYYMMDD or _YYYYMMDD_to_current_YYYYMMDD)
    name = re.sub(r"_\d{8}$", "", name)
    name = re.sub(r"_\d{8}_to_current$", "", name)
    name = re.sub(r"_\d{8}_to_\d{8}$", "", name)
    name = re.sub(r"_20\d{6}$", "", name)
    return name


def should_include_file(fname: str) -> bool:
    """Check if a file should be loaded based on include/exclude patterns."""
    if not INCLUDE_PATTERNS.search(fname):
        return False
    if EXCLUDE_PATTERNS.search(fname):
        return False
    return True


def load_file(filepath: Path, conn, dry_run=False) -> dict:
    """Load a single .ndjson file into hot_leads. Returns stats."""
    fname = filepath.name
    source = source_from_filename(fname)
    state_prefix = fname[:2].upper() if len(fname) >= 2 and fname[1] == '_' else None

    stats = {"file": fname, "source": source, "loaded": 0, "skipped": 0, "errors": 0}
    cutoff = date.today() - timedelta(days=MAX_DAYS_OLD)
    batch = []
    latest_date = None

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            for line_num, line in enumerate(f):
                if line_num > 100000:  # Cap per file to avoid huge loads
                    break
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    stats["errors"] += 1
                    continue

                # Get state from top-level field or filename
                state = record.get("state", state_prefix)
                if not state or len(state) != 2:
                    stats["skipped"] += 1
                    continue

                # Parse raw_data if present
                raw = record.get("raw_data")
                if isinstance(raw, str):
                    try:
                        raw = json.loads(raw)
                    except json.JSONDecodeError:
                        raw = {}
                elif not isinstance(raw, dict):
                    raw = record  # Use the record itself if no raw_data

                mapped = fuzzy_map(raw)

                # Must have at least address or permit_number
                if not mapped.get("address") and not mapped.get("permit_number"):
                    stats["skipped"] += 1
                    continue

                # Parse issue_date
                issue_date = parse_date(mapped.get("issue_date"))
                if issue_date and issue_date < cutoff:
                    stats["skipped"] += 1
                    continue

                if issue_date and (latest_date is None or issue_date > latest_date):
                    latest_date = issue_date

                # Build the row
                row = (
                    str(uuid.uuid4()),                          # id
                    str(mapped.get("permit_number", ""))[:100] or None,
                    str(mapped.get("permit_type", ""))[:50] or None,
                    str(mapped.get("work_class", ""))[:100] or None,
                    str(mapped.get("description", ""))[:500] or None,
                    str(mapped.get("address", ""))[:200] or None,
                    str(mapped.get("city", ""))[:100] or None,
                    state.upper()[:2],
                    str(mapped.get("zip", ""))[:10] or None,
                    safe_float(mapped.get("valuation")),
                    safe_float(mapped.get("sqft")),
                    issue_date,
                    str(mapped.get("contractor_company", ""))[:200] or None,
                    str(mapped.get("contractor_name", ""))[:200] or None,
                    str(mapped.get("contractor_phone", ""))[:20] or None,
                    str(mapped.get("applicant_name", ""))[:200] or None,
                    str(mapped.get("applicant_phone", ""))[:20] or None,
                    source[:100],                                # jurisdiction
                    source[:100],                                # source
                )
                batch.append(row)

                if len(batch) >= BATCH_SIZE and not dry_run:
                    _insert_batch(conn, batch)
                    stats["loaded"] += len(batch)
                    batch = []

        # Final batch
        if batch and not dry_run:
            _insert_batch(conn, batch)
            stats["loaded"] += len(batch)
        elif batch and dry_run:
            stats["loaded"] += len(batch)

    except Exception as e:
        stats["errors"] += 1
        log(f"  ERROR reading {fname}: {e}")

    stats["latest_date"] = str(latest_date) if latest_date else None
    return stats


def _insert_batch(conn, batch):
    """Bulk upsert a batch into hot_leads."""
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
            valuation = COALESCE(EXCLUDED.valuation, hot_leads.valuation),
            contractor_name = COALESCE(EXCLUDED.contractor_name, hot_leads.contractor_name),
            contractor_phone = COALESCE(EXCLUDED.contractor_phone, hot_leads.contractor_phone),
            contractor_company = COALESCE(EXCLUDED.contractor_company, hot_leads.contractor_company),
            description = COALESCE(EXCLUDED.description, hot_leads.description),
            source = EXCLUDED.source
    """
    cur = conn.cursor()
    try:
        execute_values(cur, sql, batch, page_size=BATCH_SIZE)
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"  Batch insert error: {e}")
    finally:
        cur.close()


def ensure_dedup_index(conn):
    """Create the unique index for dedup if it doesn't exist."""
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_hot_leads_dedup
            ON hot_leads (permit_number, address, state)
            WHERE permit_number IS NOT NULL AND address IS NOT NULL
        """)
        conn.commit()
        log("Dedup index ready")
    except Exception as e:
        conn.rollback()
        log(f"Index creation note: {e}")
    finally:
        cur.close()


def ensure_tracking_table(conn):
    """Create the freshness tracking table if it doesn't exist."""
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
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_hls_source ON hot_leads_sources(source_name)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_hls_loaded ON hot_leads_sources(loaded_at DESC)
        """)
        conn.commit()
        log("Tracking table ready")
    except Exception as e:
        conn.rollback()
        log(f"Tracking table note: {e}")
    finally:
        cur.close()


def record_source(conn, stats):
    """Record load results in tracking table."""
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO hot_leads_sources (source_name, state, file_name, records_loaded, records_skipped, latest_issue_date, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            stats["source"],
            stats.get("state"),
            stats["file"],
            stats["loaded"],
            stats["skipped"],
            stats.get("latest_date"),
            None if stats["errors"] == 0 else f"{stats['errors']} errors",
        ))
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser(description="Universal Hot Leads Loader")
    parser.add_argument("--days", type=int, default=1, help="Load files from last N days")
    parser.add_argument("--date", type=str, help="Load files for specific date (YYYYMMDD)")
    parser.add_argument("--file", type=str, help="Load a single file")
    parser.add_argument("--dry-run", action="store_true", help="Count only, don't insert")
    args = parser.parse_args()

    log("=" * 60)
    log("UNIVERSAL HOT LEADS LOADER")
    log("=" * 60)

    conn = get_conn()
    log(f"Connected to {DB_HOST}:{DB_PORT}/{DB_NAME}")

    if not args.dry_run:
        ensure_dedup_index(conn)
        ensure_tracking_table(conn)

    # Determine which files to load
    if args.file:
        files = [Path(args.file)]
    else:
        target_dates = []
        if args.date:
            target_dates.append(args.date)
        else:
            for i in range(args.days):
                d = date.today() - timedelta(days=i)
                target_dates.append(d.strftime("%Y%m%d"))

        # Find all matching files
        files = []
        for d in target_dates:
            pattern_files = sorted(DATA_DIR.glob(f"*_{d}.ndjson"))
            for f in pattern_files:
                if should_include_file(f.name):
                    files.append(f)

        log(f"Found {len(files)} permit files for dates: {', '.join(target_dates)}")

    if not files:
        log("No files to load. Exiting.")
        conn.close()
        return

    # Load each file
    total_loaded = 0
    total_skipped = 0
    total_errors = 0
    results = []

    for i, filepath in enumerate(files):
        log(f"[{i+1}/{len(files)}] Loading {filepath.name}...")
        stats = load_file(filepath, conn, dry_run=args.dry_run)
        log(f"  → {stats['loaded']} loaded, {stats['skipped']} skipped, {stats['errors']} errors, latest: {stats.get('latest_date', 'N/A')}")

        total_loaded += stats["loaded"]
        total_skipped += stats["skipped"]
        total_errors += stats["errors"]
        results.append(stats)

        if not args.dry_run:
            record_source(conn, stats)

    # Summary
    log("")
    log("=" * 60)
    log(f"COMPLETE — {total_loaded} loaded, {total_skipped} skipped, {total_errors} errors")
    log("=" * 60)

    # Show top sources
    results.sort(key=lambda x: x["loaded"], reverse=True)
    for r in results[:20]:
        if r["loaded"] > 0:
            log(f"  {r['source']}: {r['loaded']} records (latest: {r.get('latest_date', 'N/A')})")

    # Final count
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM hot_leads")
    total = cur.fetchone()[0]
    cur.close()
    log(f"\nTotal hot_leads records: {total:,}")

    conn.close()
    log("Done.")


if __name__ == "__main__":
    main()
