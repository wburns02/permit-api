#!/usr/bin/env python3
"""
Contractor Website Scraper for PermitLookup.

Finds phone numbers and emails for hot_leads that have contractor_company
but no phone, by searching DuckDuckGo for the company website and scraping
contact info from the homepage and /contact page.

No API keys needed — uses DuckDuckGo HTML search.

Usage:
    python3 scrape_contractor_websites.py --limit 50 --dry-run
    python3 scrape_contractor_websites.py --limit 500
    python3 scrape_contractor_websites.py --db-host 100.122.216.15 --limit 1000
"""

import argparse
import re
import sys
import time
import urllib.parse
from collections import defaultdict

import httpx
import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────
DB_HOST_DEFAULT = "100.122.216.15"
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

DDG_URL = "https://html.duckduckgo.com/html/"
DDG_DELAY = 2.0       # seconds between DuckDuckGo searches
FETCH_DELAY = 1.0     # seconds between page fetches
FETCH_TIMEOUT = 10.0  # seconds per HTTP request

# Skip these domains — they're directories, not the actual company site
SKIP_DOMAINS = {
    "facebook.com", "yelp.com", "bbb.org", "yellowpages.com",
    "manta.com", "angi.com", "angieslist.com", "homeadvisor.com",
    "thumbtack.com", "linkedin.com", "twitter.com", "instagram.com",
    "nextdoor.com", "mapquest.com", "google.com", "apple.com",
    "houzz.com", "porch.com", "buildzoom.com", "chamberofcommerce.com",
    "dnb.com", "buzzfile.com", "superpages.com", "whitepages.com",
    "expertise.com", "bark.com", "trustpilot.com", "glassdoor.com",
    "indeed.com", "crunchbase.com", "bloomberg.com", "zoominfo.com",
    "pitchbook.com", "opencorporates.com", "wikipedia.org",
}

# Skip generic email prefixes
SKIP_EMAIL_PREFIXES = {
    "noreply", "no-reply", "donotreply", "admin", "webmaster",
    "postmaster", "mailer-daemon", "root", "abuse", "support",
    "sales", "marketing", "newsletter", "subscribe", "unsubscribe",
}

# Phone regex: matches US phone numbers in various formats
PHONE_RE = re.compile(
    r"""
    (?<!\d)                      # not preceded by digit
    (?:
        \+?1[\s.-]?              # optional country code
    )?
    (?:
        \(?\d{3}\)?[\s.-]?       # area code
        \d{3}[\s.-]?             # exchange
        \d{4}                    # subscriber
    )
    (?!\d)                       # not followed by digit
    """,
    re.VERBOSE,
)

# Email regex
EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def get_db_conn(db_host: str):
    return psycopg2.connect(
        host=db_host,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
    )


def ensure_email_column(conn) -> bool:
    """Add contractor_email column to hot_leads if it doesn't exist.
    Returns True if column exists (or was created), False if creation failed."""
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'hot_leads' AND column_name = 'contractor_email'
    """)
    if cur.fetchone():
        cur.close()
        return True
    print("[SCHEMA] Adding contractor_email column to hot_leads...")
    try:
        cur.execute("SET lock_timeout = '30s'")
        cur.execute("ALTER TABLE hot_leads ADD COLUMN contractor_email TEXT")
        conn.commit()
        print("[SCHEMA] Done.")
        cur.close()
        return True
    except Exception as e:
        conn.rollback()
        print(f"[SCHEMA] WARNING: Could not add contractor_email column: {e}")
        print("[SCHEMA] Will skip email updates. Run again later when table is not locked.")
        cur.close()
        return False


def fetch_distinct_companies(conn, limit: int) -> list[dict]:
    """
    Fetch distinct (contractor_company, city, state) combos
    where contractor_phone IS NULL.

    Strategy: fetch a batch of raw rows (10x limit), dedupe in Python.
    This avoids an expensive GROUP BY / DISTINCT on the full table.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SET statement_timeout = '120s'")
    # Fetch raw rows — much faster than DISTINCT/GROUP BY on unindexed table
    fetch_limit = limit * 20  # oversample to get enough distinct companies
    cur.execute("""
        SELECT contractor_company, city, state
        FROM hot_leads
        WHERE contractor_phone IS NULL
          AND contractor_company IS NOT NULL
          AND contractor_company != ''
          AND length(contractor_company) > 2
          AND city IS NOT NULL
          AND state IS NOT NULL
        LIMIT %s
    """, (fetch_limit,))
    rows = cur.fetchall()
    cur.execute("RESET statement_timeout")
    cur.close()

    # Dedupe in Python
    seen = set()
    results = []
    for r in rows:
        company = r["contractor_company"]
        city = r["city"]
        state = r["state"]
        # Skip junk company names (only special chars)
        if not any(c.isalnum() for c in company):
            continue
        key = cache_key(company, city, state)
        if key not in seen:
            seen.add(key)
            results.append(dict(r))
            if len(results) >= limit:
                break

    return results


def cache_key(company: str, city: str, state: str) -> str:
    return f"{company.strip().lower()}|{city.strip().lower()}|{state.strip().upper()}"


def extract_domain(url: str) -> str:
    """Extract the root domain from a URL."""
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or ""
    # strip www.
    if host.startswith("www."):
        host = host[4:]
    return host.lower()


def search_ddg(client: httpx.Client, company: str, city: str, state: str) -> list[str]:
    """
    Search DuckDuckGo HTML for company website.
    Returns list of candidate URLs (up to 5).
    """
    query = f"{company} {city} {state} contractor phone"
    try:
        resp = client.post(
            DDG_URL,
            data={"q": query, "b": ""},
            headers=HEADERS,
            timeout=FETCH_TIMEOUT,
            follow_redirects=True,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"  [DDG] Search failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    urls = []

    # DuckDuckGo HTML results are in <a class="result__a"> tags
    for link in soup.select("a.result__a"):
        href = link.get("href", "")
        # DDG wraps URLs in a redirect — extract the actual URL
        if "uddg=" in href:
            parsed = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
            actual = parsed.get("uddg", [""])[0]
            if actual:
                href = actual

        if not href.startswith("http"):
            continue

        domain = extract_domain(href)
        if any(skip in domain for skip in SKIP_DOMAINS):
            continue

        urls.append(href)
        if len(urls) >= 5:
            break

    return urls


def normalize_phone(raw: str) -> str | None:
    """Normalize a phone match to xxx-xxx-xxxx format. Returns None if invalid."""
    digits = re.sub(r"\D", "", raw)
    # Strip leading 1 (country code)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    # Skip obvious non-phones (e.g. fax patterns starting with 800 are ok)
    # Skip numbers starting with 0 or 1
    if digits[0] in ("0", "1"):
        return None
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"


def extract_contact_info(html: str) -> tuple[str | None, str | None]:
    """
    Extract first valid phone and best email from HTML content.
    Returns (phone, email).
    """
    # Strip script/style tags to reduce noise
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ")

    # ── Phone ──
    phone = None
    for match in PHONE_RE.finditer(text):
        candidate = normalize_phone(match.group())
        if candidate:
            phone = candidate
            break

    # Also check href="tel:..." links in original HTML
    if not phone:
        tel_re = re.compile(r'href=["\']tel:([^"\']+)["\']', re.IGNORECASE)
        for m in tel_re.finditer(html):
            candidate = normalize_phone(m.group(1))
            if candidate:
                phone = candidate
                break

    # ── Email ──
    email = None
    for match in EMAIL_RE.finditer(text):
        addr = match.group().lower()
        # Skip image file extensions that look like emails
        if any(addr.endswith(ext) for ext in (".png", ".jpg", ".gif", ".svg", ".webp")):
            continue
        prefix = addr.split("@")[0]
        if prefix in SKIP_EMAIL_PREFIXES:
            continue
        email = addr
        break

    # Also check mailto: links
    if not email:
        mailto_re = re.compile(r'href=["\']mailto:([^"\'?]+)', re.IGNORECASE)
        for m in mailto_re.finditer(html):
            addr = m.group(1).lower().strip()
            if any(addr.endswith(ext) for ext in (".png", ".jpg", ".gif")):
                continue
            prefix = addr.split("@")[0]
            if prefix in SKIP_EMAIL_PREFIXES:
                continue
            email = addr
            break

    return phone, email


def fetch_page(client: httpx.Client, url: str) -> str | None:
    """Fetch a page and return its HTML, or None on failure."""
    try:
        resp = client.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT, follow_redirects=True)
        if resp.status_code != 200:
            return None
        content_type = resp.headers.get("content-type", "")
        if "text/html" not in content_type and "application/xhtml" not in content_type:
            return None
        return resp.text
    except Exception:
        return None


def scrape_company(client: httpx.Client, company: str, city: str, state: str) -> tuple[str | None, str | None]:
    """
    Search for a company and scrape phone/email from their website.
    Returns (phone, email).
    """
    urls = search_ddg(client, company, city, state)
    if not urls:
        print(f"  [DDG] No results for {company}")
        return None, None

    phone = None
    email = None

    for url in urls[:3]:  # Try up to 3 URLs
        print(f"  [FETCH] {url}")
        time.sleep(FETCH_DELAY)

        html = fetch_page(client, url)
        if not html:
            continue

        p, e = extract_contact_info(html)
        if p and not phone:
            phone = p
        if e and not email:
            email = e

        # If we found a phone, also try /contact page
        if phone and email:
            break

        # Try contact page variants
        base = url.rstrip("/")
        for contact_path in ["/contact", "/contact-us", "/about"]:
            contact_url = base + contact_path
            time.sleep(FETCH_DELAY)
            contact_html = fetch_page(client, contact_url)
            if not contact_html:
                continue
            p2, e2 = extract_contact_info(contact_html)
            if p2 and not phone:
                phone = p2
            if e2 and not email:
                email = e2
            if phone and email:
                break

        if phone:
            break  # Phone found, good enough

    return phone, email


def update_leads(conn, company: str, city: str, state: str, phone: str | None, email: str | None, dry_run: bool, has_email_col: bool = True) -> int:
    """
    Update all hot_leads matching company+city+state with the found phone/email.
    Returns number of rows updated.
    """
    if not phone and (not email or not has_email_col):
        return 0

    sets = []
    params = []
    if phone:
        sets.append("contractor_phone = %s")
        params.append(phone)
    if email and has_email_col:
        sets.append("contractor_email = %s")
        params.append(email)

    params.extend([company, city, state])

    sql = f"""
        UPDATE hot_leads
        SET {', '.join(sets)}
        WHERE contractor_company = %s
          AND city = %s
          AND state = %s
          AND contractor_phone IS NULL
    """

    if dry_run:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM hot_leads
            WHERE contractor_company = %s AND city = %s AND state = %s
              AND contractor_phone IS NULL
        """, (company, city, state))
        count = cur.fetchone()[0]
        cur.close()
        return count

    cur = conn.cursor()
    cur.execute(sql, params)
    count = cur.rowcount
    conn.commit()
    cur.close()
    return count


def main():
    parser = argparse.ArgumentParser(description="Scrape contractor websites for phone/email")
    parser.add_argument("--db-host", default=DB_HOST_DEFAULT, help="Database host")
    parser.add_argument("--limit", type=int, default=100, help="Max distinct companies to search")
    parser.add_argument("--dry-run", action="store_true", help="Don't update database")
    args = parser.parse_args()

    print(f"=== Contractor Website Scraper ===")
    print(f"DB: {args.db_host} | Limit: {args.limit} | Dry-run: {args.dry_run}")
    print()

    conn = get_db_conn(args.db_host)
    has_email_col = ensure_email_column(conn)

    print("Querying distinct companies (this may take a minute on large tables)...")
    companies = fetch_distinct_companies(conn, args.limit)
    print(f"Found {len(companies)} distinct companies to search")
    print()

    # Stats
    stats = {
        "searched": 0,
        "phones_found": 0,
        "emails_found": 0,
        "cache_hits": 0,
        "leads_updated": 0,
        "errors": 0,
    }

    # Cache: cache_key → (phone, email)
    cache: dict[str, tuple[str | None, str | None]] = {}

    client = httpx.Client(http2=False, verify=True)

    try:
        for i, row in enumerate(companies, 1):
            company = row["contractor_company"]
            city = row["city"]
            state = row["state"]
            key = cache_key(company, city, state)

            print(f"[{i}/{len(companies)}] {company} — {city}, {state}")

            if key in cache:
                phone, email = cache[key]
                stats["cache_hits"] += 1
                print(f"  [CACHE] phone={phone}, email={email}")
            else:
                stats["searched"] += 1
                try:
                    phone, email = scrape_company(client, company, city, state)
                    cache[key] = (phone, email)
                except Exception as e:
                    print(f"  [ERROR] {e}")
                    stats["errors"] += 1
                    cache[key] = (None, None)
                    continue

                # Rate limit DDG
                time.sleep(DDG_DELAY)

            if phone:
                stats["phones_found"] += 1
                print(f"  [PHONE] {phone}")
            if email:
                stats["emails_found"] += 1
                print(f"  [EMAIL] {email}")

            if phone or email:
                updated = update_leads(conn, company, city, state, phone, email, args.dry_run, has_email_col)
                stats["leads_updated"] += updated
                action = "Would update" if args.dry_run else "Updated"
                print(f"  [{action}] {updated} leads")

            if not phone and not email:
                print(f"  [MISS] No contact info found")

            print()

    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
    finally:
        client.close()
        conn.close()

    # ── Report ────────────────────────────────────────────────────────────
    print("=" * 50)
    print("REPORT")
    print("=" * 50)
    print(f"Companies searched:    {stats['searched']}")
    print(f"Cache hits:            {stats['cache_hits']}")
    print(f"Phones found:          {stats['phones_found']}")
    print(f"Emails found:          {stats['emails_found']}")
    print(f"Leads updated:         {stats['leads_updated']}")
    print(f"Errors:                {stats['errors']}")
    hit_rate = (stats['phones_found'] / max(stats['searched'], 1)) * 100
    print(f"Phone hit rate:        {hit_rate:.1f}%")
    print()


if __name__ == "__main__":
    main()
