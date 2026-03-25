#!/usr/bin/env python3
"""
CFPB Consumer Complaints Loader — downloads via CFPB search API.

The Socrata endpoint (s6ew-h6mp) returns empty responses as of 2026.
This uses the CFPB search API which has 14M+ complaints.

Loads into: consumer_complaints table (already exists)

Usage:
    nohup python3 -u load_cfpb_complaints.py --db-host 100.122.216.15 > /tmp/cfpb_load.log 2>&1 &

Cron (weekly Sunday 5 AM):
    0 5 * * 0 python3 -u /home/will/permit-api/scripts/load_cfpb_complaints.py --db-host 100.122.216.15 >> /tmp/cfpb_weekly.log 2>&1
"""

import argparse, json, os, sys, time, uuid
from datetime import date, datetime

import httpx

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary"); sys.exit(1)

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"; DB_NAME = "permits"; DB_USER = "will"
BATCH_SIZE = 5000


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def sd(v):
    """Safe date from ISO string."""
    if not v: return None
    try: return datetime.fromisoformat(v.replace("T12:00:00-05:00", "").replace("Z", "")).date()
    except Exception: pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try: return datetime.strptime(str(v).strip()[:10], fmt).date()
        except Exception: continue
    return None


def s(v, m=500):
    """Safe string with max length."""
    if not v: return None
    return str(v).strip()[:m] or None


def sb(v):
    """Safe boolean."""
    if v is None: return None
    if isinstance(v, bool): return v
    sv = str(v).strip().lower()
    if sv in ("n/a", "na", "", "none"): return None
    return sv in ("true", "yes", "1", "y")


def get_count(conn, source):
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM consumer_complaints WHERE source = %s", (source,))
    c = cur.fetchone()[0]; cur.close(); return c


def ensure_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS consumer_complaints (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            complaint_id TEXT, date_received DATE, product TEXT, sub_product TEXT,
            issue TEXT, company TEXT, company_response TEXT, state VARCHAR(2),
            zip TEXT, consumer_disputed BOOLEAN, timely_response BOOLEAN,
            source TEXT NOT NULL DEFAULT 'cfpb')
    """)
    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_cc_state ON consumer_complaints (state)",
        "CREATE INDEX IF NOT EXISTS ix_cc_company ON consumer_complaints (company)",
        "CREATE INDEX IF NOT EXISTS ix_cc_product ON consumer_complaints (product)",
        "CREATE INDEX IF NOT EXISTS ix_cc_date ON consumer_complaints (date_received)",
        "CREATE INDEX IF NOT EXISTS ix_cc_source ON consumer_complaints (source)",
        "CREATE INDEX IF NOT EXISTS ix_cc_complaint_id ON consumer_complaints (complaint_id)",
    ]:
        try: cur.execute(idx)
        except Exception: conn.rollback()
    conn.commit(); cur.close()


COMPLAINT_SQL = """INSERT INTO consumer_complaints
    (id, complaint_id, date_received, product, sub_product, issue, company,
     company_response, state, zip, consumer_disputed, timely_response, source)
    VALUES %s ON CONFLICT DO NOTHING"""


def load_cfpb_complaints(conn):
    """Load CFPB complaints via their search API (Elasticsearch-based)."""
    log("=== CFPB Consumer Complaints ===")

    existing = get_count(conn, "cfpb")
    if existing > 100000:
        log(f"  SKIP -- already {existing:,} records (source=cfpb)")
        return 0

    # The CFPB search API supports pagination via search_after
    # Max page size is 10000, but we use 5000 to be safe
    base_url = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
    page_size = 5000

    # Get total count first
    try:
        resp = httpx.get(base_url, params={"size": 0, "no_aggs": "true"}, timeout=60)
        resp.raise_for_status()
        avail = resp.json()["hits"]["total"]["value"]
        log(f"  Available: {avail:,}")
    except Exception as e:
        log(f"  Count failed: {e}"); avail = None

    cur = conn.cursor()
    total = 0
    search_after = None
    page = 0
    max_pages = 3000  # Safety limit: 3000 * 5000 = 15M records max

    while page < max_pages:
        try:
            params = {
                "size": page_size,
                "no_aggs": "true",
                "sort": "created_date_desc",
            }
            if search_after:
                params["search_after"] = search_after

            resp = httpx.get(base_url, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
        except Exception as e:
            log(f"  Error on page {page}: {e}")
            break

        if not hits:
            break

        batch = []
        for hit in hits:
            r = hit.get("_source", {})
            batch.append((
                str(uuid.uuid4()),
                s(r.get("complaint_id"), 50),
                sd(r.get("date_received")),
                s(r.get("product"), 200),
                s(r.get("sub_product"), 200),
                s(r.get("issue"), 300),
                s(r.get("company"), 300),
                s(r.get("company_response"), 200),
                s(r.get("state"), 2),
                s(r.get("zip_code"), 10),
                sb(r.get("consumer_disputed")),
                sb(r.get("timely")),
                "cfpb",
            ))

        if batch:
            try:
                execute_values(cur, COMPLAINT_SQL, batch)
                conn.commit()
                total += len(batch)
                pct = f" ({total * 100 // avail}%)" if avail else ""
                log(f"    Page {page}: {total:,} total{pct}")
            except Exception as e:
                log(f"    Insert error page {page}: {e}")
                conn.rollback()

        # Get the sort value from the last hit for search_after pagination
        last_hit = hits[-1]
        if "sort" in last_hit:
            # search_after needs the sort values as a comma-separated string
            search_after = ",".join(str(sv) for sv in last_hit["sort"])
        else:
            break

        page += 1
        if len(hits) < page_size:
            break
        time.sleep(0.3)

    cur.close()
    return total


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="CFPB Consumer Complaints Loader")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to {DB_HOST}")
    ensure_table(conn)

    try:
        count = load_cfpb_complaints(conn)
        log(f"COMPLETE -- {count:,} complaints loaded")
    except Exception as e:
        log(f"FATAL: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
