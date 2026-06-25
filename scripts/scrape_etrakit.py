#!/usr/bin/env python3
"""
eTRAKiT (CentralSquare) — generic public permit-search adapter.

eTRAKiT3 is a legacy ASP.NET WebForms permit portal (now a CentralSquare
product) used by many Texas cities. Its public "Search > Permit" page lets you
query permits by a field (Permit No / Address / Contractor / Parcel / etc.), an
operator (Begins With / Contains / Equals), and a value, then renders a paged
results grid. There is no clean JSON API — the page is ViewState-driven — so
this adapter drives the real search form in a headless browser (Playwright),
which is robust across eTRAKiT deployments, and scrapes the results grid.

Why this is generic
-------------------
Every eTRAKiT3 instance shares the same /etrakit3/Search/permit.aspx page and
the same search-by / operator / value / results-grid structure. A city is a
single JURISDICTIONS registry entry (host + city/county/source). The search
strategy (which field to enumerate over, e.g. address "Contains" a street
token, or permit-number year prefix) is configurable per jurisdiction. Adding a
reachable eTRAKiT city = one registry entry, no code.

Reachability note (HONESTY)
---------------------------
eTRAKiT is being sunset across Texas. Pearland (etrakit.pearlandtx.gov, the
largest Brazoria city and the intended Phase-1b high-value source) resolves but
DROPS all inbound TCP on :80 and :443 from our host (HTTP 000, TLS never
completes) — a network/IP-level filter we cannot bypass without a Texas-resident
/ residential egress. Round Rock's eTRAKiT was decommissioned (migration notice).
This adapter is written and config-driven so it runs the moment a reachable
eTRAKiT host is registered; Pearland is registered and flagged network-blocked.
We do NOT fabricate Pearland rows.

Usage:
    python3 scrape_etrakit.py --city pearland --days 30 --dry-run
    python3 scrape_etrakit.py --city pearland --days 30        # writes hot_leads
    python3 scrape_etrakit.py --list
    python3 scrape_etrakit.py --probe pearland                 # connectivity check only

Database target (defaults):
    host=100.122.216.15 dbname=permits user=will
    hot_leads upsert ON CONFLICT (permit_number, source).
    hot_leads_sources ledger row written per run.

Cron (PREPARED — DO NOT enable without sign-off; only after a reachable host):
    # eTRAKiT Pearland building permits — daily 05:35 CT (BLOCKED until reachable)
    35 5 * * * cd /home/will/permit-api-live && python3 scripts/scrape_etrakit.py --city pearland --days 7 >> /tmp/etrakit_pearland.log 2>&1
"""

import argparse
import re
import socket
import sys
import uuid
from datetime import date, datetime, timedelta
from typing import Optional
from urllib.parse import urlparse

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

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

# eTRAKiT city registry. host = scheme+host of the eTRAKiT3 deployment.
# search_field / search_tokens drive the enumeration strategy:
#   search_field="Address" with search_tokens=[street tokens] → address "Contains"
#   search_field="Permit_No" with search_tokens=[year prefixes] → permit-number "Begins With"
# building_types optionally filters the results to building/new-construction.
JURISDICTIONS = {
    "pearland": {
        "host": "https://etrakit.pearlandtx.gov",
        "path": "/etrakit3/Search/permit.aspx",
        "city_upper": "PEARLAND",
        "county": "Brazoria",
        "state": "TX",
        "source": "pearland_etrakit",
        # enumerate by address street tokens; building permits surface in the grid
        "search_field": "Address",
        "search_op": "Contains",
        "search_tokens": ["DR", "LN", "CT", "RD", "ST", "BLVD", "WAY", "CIR", "PL"],
        # network-blocked from this host; documented, not fabricated.
        "blocked": "network-level IP filter: :80/:443 drop all inbound (HTTP 000, TLS never completes). "
                   "Needs a Texas/residential egress. Adapter is ready; host is unreachable from us.",
    },
}


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn(db_host):
    return psycopg2.connect(host=db_host, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def probe_reachable(host, timeout=8) -> bool:
    """TCP-connect to :443. Returns True if reachable, False if filtered/down.
    Pearland drops the SYN so this returns False — exactly the network block."""
    netloc = urlparse(host).netloc or host
    try:
        with socket.create_connection((netloc, 443), timeout=timeout):
            return True
    except Exception as e:
        log(f"  probe {netloc}:443 -> unreachable ({type(e).__name__}: {e})")
        return False


def extract_zip(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    m = re.search(r"\b(\d{5})(?:-\d{4})?\b", str(s))
    return m.group(1) if m else None


def parse_date(v) -> Optional[date]:
    if not v:
        return None
    s = str(v).strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            continue
    return None


def fetch_etrakit(cfg, days, max_tokens=None):
    """Drive the eTRAKiT3 public permit search in a headless browser and scrape
    the results grid. Returns a list of normalized permit dicts.

    Imports Playwright lazily so --list / --probe work without it installed.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log("  ERROR: pip install playwright && playwright install chromium")
        return []

    base = cfg["host"].rstrip("/") + cfg["path"]
    field = cfg.get("search_field", "Address")
    op = cfg.get("search_op", "Contains")
    tokens = cfg.get("search_tokens", ["DR"])
    if max_tokens:
        tokens = tokens[:max_tokens]
    cutoff = date.today() - timedelta(days=days) if days else None

    results = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(user_agent=UA).new_page()
        try:
            page.goto(base, wait_until="networkidle", timeout=45000)
        except Exception as e:
            log(f"  navigation failed: {e}")
            browser.close()
            return []
        page.wait_for_timeout(2000)

        # eTRAKiT3 search controls are ASP.NET-named. We locate by visible role
        # rather than hard-coded ids so it survives minor per-instance skinning:
        #   - a "Search By" <select> (field)
        #   - an operator <select> (Begins With / Contains / Equals)
        #   - a value <input>
        #   - a "Search" button
        for token in tokens:
            try:
                selects = page.query_selector_all("select")
                # field select = first select whose options include the field label
                for s in selects:
                    opts = [o.inner_text().strip().lower() for o in s.query_selector_all("option")]
                    if any(field.lower().replace("_", " ") in o for o in opts):
                        s.select_option(label=re.compile(field.replace("_", " "), re.I))
                        break
                # operator select = one whose options include the operator
                for s in selects:
                    opts = [o.inner_text().strip().lower() for o in s.query_selector_all("option")]
                    if any(op.lower() in o for o in opts):
                        s.select_option(label=re.compile(op, re.I))
                        break
                # value input = a visible text input near the search controls
                vinput = None
                for inp in page.query_selector_all("input[type=text]"):
                    if inp.is_visible():
                        vinput = inp
                        break
                if vinput:
                    vinput.fill(token)
                # click Search
                btn = page.get_by_role("button", name=re.compile("search", re.I))
                if btn.count() == 0:
                    btn = page.locator("input[type=submit][value*='Search' i], a:has-text('Search')")
                btn.first.click(timeout=8000)
                page.wait_for_load_state("networkidle", timeout=30000)
                page.wait_for_timeout(1500)

                # scrape the results grid
                rows = page.eval_on_selector_all(
                    "table tr",
                    "els=>els.map(r=>Array.from(r.querySelectorAll('td')).map(c=>c.innerText.trim())).filter(c=>c.length>=3)",
                )
                for r in rows:
                    rec = normalize_etrakit_row(r, cfg)
                    if rec and rec["permit_number"]:
                        if cutoff and rec["issue_date"] and rec["issue_date"] < cutoff:
                            continue
                        results[(rec["permit_number"], rec["source"])] = rec
                log(f"  token '{token}': grid rows={len(rows)} cumulative permits={len(results)}")
            except Exception as e:
                log(f"  token '{token}' error: {e}")
                continue
        browser.close()
    return list(results.values())


def normalize_etrakit_row(cells, cfg):
    """Map an eTRAKiT results-grid row (list of cell strings) to a hot_leads dict.
    eTRAKiT grids vary; we heuristically locate the permit number (matches a
    permit-like token), an address (contains digits + a street word), and a date."""
    if not cells:
        return None
    permit_number = None
    address = None
    issue_date = None
    status = None
    permit_type = None
    for c in cells:
        if not c:
            continue
        if permit_number is None and re.match(r"^[A-Z]{0,4}[-_ ]?\d{2,}", c) and len(c) <= 40:
            permit_number = c.strip()
            continue
        if issue_date is None:
            d = parse_date(c)
            if d:
                issue_date = d
                continue
        if address is None and re.search(r"\d", c) and re.search(r"[A-Za-z]{2,}", c) and len(c) >= 6:
            address = c.strip()
            continue
    if not permit_number:
        return None
    # remaining unclassified cells → type/status best-effort
    leftovers = [c for c in cells if c not in (permit_number, address) and not parse_date(c)]
    if leftovers:
        permit_type = leftovers[0][:80]
        if len(leftovers) > 1:
            status = leftovers[-1][:80]
    return {
        "permit_number": permit_number[:100],
        "permit_type": permit_type,
        "work_class": None,
        "description": None,
        "address": (address[:200]) if address else None,
        "city": cfg["city_upper"],
        "state": cfg["state"],
        "zip": extract_zip(address),
        "county": cfg["county"],
        "issue_date": issue_date,
        "applied_date": issue_date,
        "status": status,
        "owner_name": None,
        "jurisdiction": f"{cfg['city_upper'].title()}, {cfg['state']}",
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
    rows = [
        (
            str(uuid.uuid4()),
            p["permit_number"], p["permit_type"], p["work_class"], p["description"],
            p["address"], p["city"], p["state"], p["zip"], p["county"],
            p["issue_date"], p["applied_date"], p["status"], p["owner_name"],
            p["jurisdiction"], p["source"],
        )
        for p in rows_in if p.get("permit_number")
    ]
    sql = f"""
        INSERT INTO hot_leads ({', '.join(HOT_LEADS_COLS)})
        VALUES %s
        ON CONFLICT (permit_number, source) DO UPDATE SET
            permit_type = COALESCE(EXCLUDED.permit_type, hot_leads.permit_type),
            description = COALESCE(EXCLUDED.description, hot_leads.description),
            address = COALESCE(EXCLUDED.address, hot_leads.address),
            issue_date = COALESCE(EXCLUDED.issue_date, hot_leads.issue_date),
            applied_date = COALESCE(EXCLUDED.applied_date, hot_leads.applied_date),
            status = COALESCE(EXCLUDED.status, hot_leads.status),
            scraped_at = CURRENT_DATE
    """
    cur = conn.cursor()
    try:
        execute_values(cur, sql, rows, page_size=500)
        conn.commit()
        return len(rows)
    except Exception as e:
        conn.rollback()
        log(f"  hot_leads upsert error: {e}")
        return 0
    finally:
        cur.close()


def ensure_sources_table(conn):
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hot_leads_sources (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                source_name TEXT NOT NULL, state TEXT, file_name TEXT,
                records_loaded INTEGER DEFAULT 0, records_skipped INTEGER DEFAULT 0,
                latest_issue_date DATE, loaded_at TIMESTAMPTZ DEFAULT NOW(),
                error_message TEXT
            )
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"  (sources table ensure: {e})")
    finally:
        cur.close()


def record_source(conn, source_name, state, loaded, latest_date, error=None):
    cur = conn.cursor()
    try:
        cur.execute(
            """INSERT INTO hot_leads_sources
               (source_name, state, file_name, records_loaded, records_skipped, latest_issue_date, error_message)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (source_name, state, "etrakit_permit_search", loaded, 0, latest_date, error),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        log(f"  (record_source: {e})")
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser(description="eTRAKiT generic permit-search adapter")
    parser.add_argument("--city", choices=list(JURISDICTIONS.keys()))
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--max-tokens", type=int, default=None,
                        help="Limit number of enumeration tokens (testing)")
    parser.add_argument("--db-host", default=DB_HOST_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--probe", choices=list(JURISDICTIONS.keys()),
                        help="Connectivity probe only (no scrape)")
    args = parser.parse_args()

    if args.list:
        print("Registered eTRAKiT cities:")
        for k, v in JURISDICTIONS.items():
            flag = f"  [BLOCKED: {v['blocked']}]" if v.get("blocked") else ""
            print(f"  {k:12s} {v['city_upper']}, {v['state']}  source={v['source']}{flag}")
        return

    if args.probe:
        cfg = JURISDICTIONS[args.probe]
        ok = probe_reachable(cfg["host"])
        print(f"{args.probe}: {'REACHABLE' if ok else 'UNREACHABLE (network-blocked)'}")
        sys.exit(0 if ok else 3)

    if not args.city:
        parser.error("must provide --city (or --list / --probe)")

    cfg = JURISDICTIONS[args.city]
    log("=" * 64)
    log(f"eTRAKiT ADAPTER — {args.city} ({cfg['city_upper']}, {cfg['state']}) days={args.days}")
    log("=" * 64)

    # connectivity gate — if unreachable, report and exit honestly (no fake rows)
    if not probe_reachable(cfg["host"]):
        log(f"  {args.city} is NETWORK-BLOCKED: {cfg.get('blocked','unreachable')}")
        log("  No rows fetched. Documented as not-automatable from this host.")
        sys.exit(3)

    permits = fetch_etrakit(cfg, args.days, max_tokens=args.max_tokens)
    log(f"  fetched {len(permits)} permits")

    latest_date = None
    for p in permits:
        if p["issue_date"] and (latest_date is None or p["issue_date"] > latest_date):
            latest_date = p["issue_date"]

    if args.dry_run:
        log("  DRY RUN — not writing. Sample:")
        for p in permits[:10]:
            log(f"    {p['issue_date']} | {p['permit_number']} | {p['address']} | {p['status']}")
        return

    conn = get_conn(args.db_host)
    ensure_sources_table(conn)
    loaded = upsert_hot_leads(conn, permits) if permits else 0
    record_source(conn, cfg["source"], cfg["state"], loaded, latest_date)
    log(f"  upserted {loaded} into hot_leads; ledger latest_issue_date={latest_date}")
    conn.close()
    log("Done.")


if __name__ == "__main__":
    main()
