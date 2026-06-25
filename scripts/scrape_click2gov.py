#!/usr/bin/env python3
"""
Click2Gov / CentralSquare CEP — generic public permit-search adapter.

Click2Gov Building Permit (a CentralSquare "Citizen Engagement Portal" app) is a
JSP/jQuery portal at /Click2GovBP/. Its public "Select Permit" search supports
search-by Application Number / Address / Parcel / Name, then renders a results
grid (Application Number, Address, Parcel, Contractor/Other Name, Application
Type, Application Status). Detail pages add Application Date / Type / Status /
Valuation / Owner. Every request carries an OWASP_CSRFTOKEN session token, so
this adapter drives the real search in a headless browser (Playwright).

Why this is generic
-------------------
Every Click2GovBP instance shares the same /Click2GovBP/selectpermit.html page,
the same searchMethod dropdown (0=AppNo 1=Address 2=Parcel 3=Name), and the same
results grid. A city is a single JURISDICTIONS registry entry (host + city /
county / source). The enumeration strategy (address street tokens) is per-city.

IMPORTANT BLOCKER — Lake Jackson (HONESTY)
------------------------------------------
Lake Jackson (lkjk-egov.aspgov.com/Click2GovBP) was probed live. Two hard walls:
  1. The server returns a MAX of 10 results per search regardless of query
     breadth (verified across 7 different street tokens — always exactly 10).
     The "Show 25/50/100 entries" control is a cosmetic client-side DataTable;
     the server never returns more than 10 matches. There is no pagination past
     10 and no way to enumerate the full permit set.
  2. There is NO date-range / "browse recent" search and the 10 returned rows
     skew to ancient permits (1990s–2010s), so even brute-forcing street tokens
     yields stale, non-enumerable data — not a usable fresh-lead feed.
Additionally the county's CentralSquare ArcGIS folder (maps.brazoriacountytx.gov
/arcgis/.../CentralSquare) is token-gated ("Token Required") — the easier GIS
path is NOT anonymously available either.
Lake Jackson is therefore documented as NOT-AUTOMATABLE as a bulk feed. The
adapter is kept generic so a Click2Gov city WITHOUT the 10-row cap could use it,
and a `--probe` mode reports the cap so the wall is measurable, not assumed.

Usage:
    python3 scrape_click2gov.py --city lake_jackson --probe   # measure the result cap
    python3 scrape_click2gov.py --city lake_jackson --dry-run # attempt scrape, show rows
    python3 scrape_click2gov.py --list

Database target (defaults):
    host=100.122.216.15 dbname=permits user=will
"""

import argparse
import re
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
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

JURISDICTIONS = {
    "lake_jackson": {
        "host": "https://lkjk-egov.aspgov.com",
        "path": "/Click2GovBP/selectpermit.html?initialSearchView=true",
        "city_upper": "LAKE JACKSON",
        "county": "Brazoria",
        "state": "TX",
        "source": "lake_jackson_click2gov",
        "search_tokens": ["OAK", "PINE", "MAIN", "PARK", "LAKE", "CIR", "DR", "LN", "RD", "ST"],
        "blocked": "server hard-caps results at 10 per search (verified across 7 tokens), no "
                   "date browse, returns mostly ancient permits — not enumerable as a fresh feed. "
                   "CentralSquare county ArcGIS folder is token-gated too.",
    },
}


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn(db_host):
    return psycopg2.connect(host=db_host, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def extract_zip(s):
    if not s:
        return None
    m = re.search(r"\b(\d{5})(?:-\d{4})?\b", str(s))
    return m.group(1) if m else None


def _search_token(page, base, token):
    """Run one Click2Gov address search for `token` (Contains). Returns the list
    of grid rows (each a list of cell strings)."""
    page.goto(base, wait_until="networkidle", timeout=45000)
    page.select_option("#searchMethod", "1")
    page.wait_for_timeout(600)
    page.locator("input[name='parcel.streetName']").fill(token)
    try:
        page.locator("#streetSearchType").select_option("contains")
    except Exception:
        pass
    btns = page.locator("input[name='target1']")
    for i in range(btns.count()):
        if btns.nth(i).is_visible():
            btns.nth(i).click()
            break
    page.wait_for_load_state("networkidle", timeout=45000)
    page.wait_for_timeout(1200)
    rows = page.eval_on_selector_all(
        "table tr",
        "els=>els.map(r=>Array.from(r.querySelectorAll('td')).map(c=>c.innerText.trim())).filter(c=>c.length>=3)",
    )
    return rows


def probe_cap(cfg):
    """Measure how many rows the server returns across several tokens — proves the
    10-result cap empirically rather than asserting it."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log("  ERROR: pip install playwright && playwright install chromium")
        return
    base = cfg["host"].rstrip("/") + cfg["path"]
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(user_agent=UA).new_page()
        for token in cfg["search_tokens"][:6]:
            try:
                rows = _search_token(page, base, token)
                permit_rows = [r for r in rows if r and re.match(r"\d{2}-\d", r[0] or "")]
                log(f"  token '{token}': server returned {len(permit_rows)} permit rows")
            except Exception as e:
                log(f"  token '{token}' error: {e}")
        browser.close()


def normalize_row(cells, cfg):
    # Lake Jackson grid: [AppNo, Address, Parcel, Contractor/Other, Type, Status]
    if not cells or not re.match(r"\d{2}-\d", cells[0] or ""):
        return None
    appno = cells[0].strip()
    address = cells[1].strip() if len(cells) > 1 else None
    contractor = cells[3].strip() if len(cells) > 3 else None
    ptype = cells[4].strip() if len(cells) > 4 else None
    status = cells[5].strip() if len(cells) > 5 else None
    # app-number prefix encodes the 2-digit year (e.g. 23-... = 2023)
    yr = None
    m = re.match(r"(\d{2})-", appno)
    if m:
        yy = int(m.group(1))
        yr = 2000 + yy if yy < 80 else 1900 + yy
    issue = date(yr, 1, 1) if yr else None
    return {
        "permit_number": appno[:100],
        "permit_type": (ptype[:80]) if ptype else None,
        "work_class": None,
        "description": None,
        "address": (address[:200]) if address else None,
        "city": cfg["city_upper"],
        "state": cfg["state"],
        "zip": extract_zip(address),
        "county": cfg["county"],
        "issue_date": issue,
        "applied_date": issue,
        "status": (status[:80]) if status else None,
        "owner_name": None,
        "contractor_name": (contractor[:200]) if contractor and "," in (contractor or "") else None,
        "jurisdiction": f"{cfg['city_upper'].title()}, {cfg['state']}",
        "source": cfg["source"],
    }


def fetch(cfg, max_tokens=None):
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log("  ERROR: pip install playwright && playwright install chromium")
        return []
    base = cfg["host"].rstrip("/") + cfg["path"]
    tokens = cfg["search_tokens"][: (max_tokens or len(cfg["search_tokens"]))]
    out = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(user_agent=UA).new_page()
        for token in tokens:
            try:
                rows = _search_token(page, base, token)
                for r in rows:
                    rec = normalize_row(r, cfg)
                    if rec:
                        out[(rec["permit_number"], rec["source"])] = rec
                log(f"  token '{token}': cumulative permits={len(out)}")
            except Exception as e:
                log(f"  token '{token}' error: {e}")
        browser.close()
    return list(out.values())


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
            address = COALESCE(EXCLUDED.address, hot_leads.address),
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


def main():
    parser = argparse.ArgumentParser(description="Click2Gov generic permit-search adapter")
    parser.add_argument("--city", choices=list(JURISDICTIONS.keys()))
    parser.add_argument("--max-tokens", type=int, default=None)
    parser.add_argument("--db-host", default=DB_HOST_DEFAULT)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--probe", action="store_true",
                        help="Measure the server result cap (proves the 10-row wall)")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    if args.list:
        print("Registered Click2Gov cities:")
        for k, v in JURISDICTIONS.items():
            flag = f"  [BLOCKED: {v['blocked']}]" if v.get("blocked") else ""
            print(f"  {k:14s} {v['city_upper']}, {v['state']}  source={v['source']}{flag}")
        return

    if not args.city:
        parser.error("must provide --city (or --list)")

    cfg = JURISDICTIONS[args.city]
    log("=" * 64)
    log(f"Click2Gov ADAPTER — {args.city} ({cfg['city_upper']}, {cfg['state']})")
    if cfg.get("blocked"):
        log(f"  NOTE: registered as BLOCKED — {cfg['blocked']}")
    log("=" * 64)

    if args.probe:
        probe_cap(cfg)
        return

    permits = fetch(cfg, max_tokens=args.max_tokens)
    log(f"  fetched {len(permits)} distinct permits (capped by server)")
    if args.dry_run:
        for p in permits[:12]:
            log(f"    {p['issue_date']} | {p['permit_number']} | {p['address']} | {p['status']}")
        return

    conn = get_conn(args.db_host)
    loaded = upsert_hot_leads(conn, permits) if permits else 0
    log(f"  upserted {loaded} into hot_leads")
    conn.close()
    log("Done.")


if __name__ == "__main__":
    main()
