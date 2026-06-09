#!/usr/bin/env python3
"""
New Hampshire Office of Professional Licensure (OPLC) License Loader

Source: nhlicenses.nh.gov ASP.NET Verification portal
Method: POST form scraping with ASP.NET ViewState management

Covers: Electricians (Master, Journeyman, Apprentice), Plumbers (Master, Journeyman,
        Apprentice), Gas Fitters, General Contractor (if available), plus HVAC/Refrigeration.

NOTE: NH licenses are managed by OPLC (Office of Professional Licensure and Certification).
The verification portal at nhlicenses.nh.gov uses an ASP.NET form similar to Indiana's
mylicense.in.gov. We iterate profession types x 2-letter last name prefixes.

Strategy: Iterate through board/profession types, then last-name 2-letter prefixes
to harvest all licensees.

Loads into: contractor_licenses table (existing schema)
Unique key: (license_number, state)

Usage:
    python3 -u load_nh_licenses.py --db-host 100.122.216.15
    python3 -u load_nh_licenses.py --dry-run
    python3 -u load_nh_licenses.py --profession "Master Electrician"

Cron (weekly Monday 4:30 AM):
    30 4 * * 1 python3 -u /home/will/permit-api-live/scripts/load_nh_licenses.py --db-host 100.122.216.15 >> /var/log/nh_licenses.log 2>&1
"""

import argparse
import os
import re
import string
import sys
import time
import uuid
from datetime import date, datetime
from html import unescape

try:
    import httpx
except ImportError:
    print("ERROR: pip install httpx"); sys.exit(1)

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary"); sys.exit(1)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"
BATCH_SIZE = 2000

BASE_URL = "https://nhlicenses.nh.gov"
SEARCH_URL = f"{BASE_URL}/verification/Search.aspx"
SOURCE = "nh_oplc"

# Construction-related professions/boards on nhlicenses.nh.gov
# NH OPLC manages: Electricians Board, Plumbers Board, Mechanical Licensing Board (HVAC)
PROFESSIONS = [
    # Electricians Board
    "Master Electrician",
    "Journeyman Electrician",
    "Apprentice Electrician",
    "Electrical Contractor",
    # Plumbers Board
    "Master Plumber",
    "Journeyman Plumber",
    "Apprentice Plumber",
    "Plumbing Contractor",
    # Gas Fitters
    "Master Gas Fitter",
    "Gas Fitter",
    "Journeyman Gas Fitter",
    # Mechanical / HVAC
    "Mechanical Contractor",
    "HVAC Contractor",
    "Refrigeration Contractor",
    # General / Home
    "Home Inspector",
    "General Contractor",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_conn(host):
    return psycopg2.connect(host=host, port=DB_PORT, dbname=DB_NAME, user=DB_USER, connect_timeout=30)


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def clean_str(s, max_len=500):
    if not s:
        return None
    s = unescape(str(s)).strip()
    s = re.sub(r"\s+", " ", s)
    return s[:max_len] or None


def clean_phone(p):
    if not p:
        return None
    digits = re.sub(r"\D", "", str(p))
    if len(digits) == 10:
        return digits
    if len(digits) == 11 and digits[0] == "1":
        return digits[1:]
    return digits if digits else None


def parse_date(d):
    if not d:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(str(d).strip().split("T")[0].split(" ")[0], fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def make_row(lic_num, source, **kw):
    """Build a tuple matching the contractor_licenses INSERT column order."""
    return (
        str(uuid.uuid4()),
        clean_str(lic_num, 100),
        clean_str(kw.get("business_name"), 500),
        clean_str(kw.get("full_business_name"), 500),
        clean_str(kw.get("address"), 500),
        clean_str(kw.get("city"), 100),
        "NH",
        clean_str(kw.get("zip"), 10),
        clean_str(kw.get("county"), 100),
        clean_phone(kw.get("phone")),
        clean_str(kw.get("business_type"), 50),
        parse_date(kw.get("issue_date")),
        parse_date(kw.get("expiration_date")),
        clean_str(kw.get("status"), 50),
        None,  # secondary_status
        clean_str(kw.get("classifications"), 1000),
        None, None, None, None,  # workers_comp_*, surety_*
        source,
        date.today(),
    )


def upsert_batch(conn, rows):
    """Insert/update a batch of contractor_licenses rows."""
    if not rows:
        return 0
    sql = """
        INSERT INTO contractor_licenses (
            id, license_number, business_name, full_business_name,
            address, city, state, zip, county, phone, business_type,
            issue_date, expiration_date, status, secondary_status,
            classifications, workers_comp_type, workers_comp_company,
            surety_company, surety_amount, source, last_updated
        ) VALUES %s
        ON CONFLICT (license_number, state) DO UPDATE SET
            business_name = COALESCE(EXCLUDED.business_name, contractor_licenses.business_name),
            full_business_name = COALESCE(EXCLUDED.full_business_name, contractor_licenses.full_business_name),
            address = COALESCE(EXCLUDED.address, contractor_licenses.address),
            city = COALESCE(EXCLUDED.city, contractor_licenses.city),
            zip = COALESCE(EXCLUDED.zip, contractor_licenses.zip),
            phone = COALESCE(EXCLUDED.phone, contractor_licenses.phone),
            business_type = COALESCE(EXCLUDED.business_type, contractor_licenses.business_type),
            expiration_date = COALESCE(EXCLUDED.expiration_date, contractor_licenses.expiration_date),
            status = COALESCE(EXCLUDED.status, contractor_licenses.status),
            classifications = COALESCE(EXCLUDED.classifications, contractor_licenses.classifications),
            last_updated = EXCLUDED.last_updated
    """
    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=500)
    return len(rows)


# ---------------------------------------------------------------------------
# ASP.NET ViewState scraper
# ---------------------------------------------------------------------------

class NHLicenseClient:
    """Client for NH nhlicenses.nh.gov ASP.NET verification portal."""

    def __init__(self):
        self.client = httpx.Client(
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0",
            },
            timeout=httpx.Timeout(60.0, connect=30.0),
        )
        self.viewstate = ""
        self.viewstate_gen = ""
        self.event_validation = ""

    def init_session(self):
        """GET the search page to extract ASP.NET form tokens."""
        log("  Initializing nhlicenses.nh.gov session...")
        resp = self.client.get(SEARCH_URL)
        resp.raise_for_status()
        self._extract_viewstate(resp.text)
        log(f"  Session initialized (viewstate len={len(self.viewstate)})")

    def _extract_viewstate(self, html):
        """Extract ASP.NET form tokens from HTML."""
        vs = re.search(r'id="__VIEWSTATE"\s+value="([^"]*)"', html)
        vsg = re.search(r'id="__VIEWSTATEGENERATOR"\s+value="([^"]*)"', html)
        ev = re.search(r'id="__EVENTVALIDATION"\s+value="([^"]*)"', html)
        if vs:
            self.viewstate = vs.group(1)
        if vsg:
            self.viewstate_gen = vsg.group(1)
        if ev:
            self.event_validation = ev.group(1)

    def search(self, profession, last_name="*"):
        """Search for licensees by profession and last name prefix."""
        form_data = {
            "__VIEWSTATE": self.viewstate,
            "__VIEWSTATEGENERATOR": self.viewstate_gen,
            "__EVENTVALIDATION": self.event_validation,
            "t_web_lookup__profession_name": profession,
            "t_web_lookup__last_name": last_name,
            "t_web_lookup__first_name": "",
            "t_web_lookup__license_type_name": "",
            "t_web_lookup__license_no": "",
            "t_web_lookup__addr_city": "",
            "t_web_lookup__addr_state": "",
            "t_web_lookup__addr_county": "",
            "t_web_lookup__addr_zipcode": "",
            "t_web_lookup__dba_name": "",
            "t_web_lookup__license_status_name": "",
            "sch_button": "Search",
        }
        resp = self.client.post(
            SEARCH_URL,
            data=form_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=60,
        )
        self._extract_viewstate(resp.text)
        return resp.text

    def close(self):
        self.client.close()


def parse_results_table(html):
    """Parse the ASP.NET GridView results table from NH verification portal."""
    results = []

    # Find the results grid
    table_match = re.search(
        r'<table[^>]*id="datagrid_results"[^>]*>(.*?)</table>',
        html, re.DOTALL,
    )
    if not table_match:
        # Try alternate table IDs
        table_match = re.search(
            r'<table[^>]*id="[^"]*(?:grid|results|list)[^"]*"[^>]*>(.*?)</table>',
            html, re.DOTALL | re.IGNORECASE,
        )
    if not table_match:
        return results

    table_html = table_match.group(1)

    # Parse data rows (skip header)
    row_pattern = re.compile(r'<tr[^>]*class="(?:alt)?row[^"]*"[^>]*>(.*?)</tr>', re.DOTALL)
    for row_match in row_pattern.finditer(table_html):
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row_match.group(1), re.DOTALL)
        if len(cells) < 4:
            continue

        # Clean cell contents
        cells = [re.sub(r'<[^>]+>', ' ', c).strip() for c in cells]
        cells = [re.sub(r'\s+', ' ', unescape(c)).strip() for c in cells]

        # Typical columns: Name, License #, Profession/Type, Status, City, State, Zip
        record = {}
        if len(cells) >= 7:
            record["name"] = cells[0]
            record["license_number"] = cells[1]
            record["profession"] = cells[2]
            record["status"] = cells[3]
            record["city"] = cells[4]
            record["state"] = cells[5]
            record["zip"] = cells[6]
        elif len(cells) >= 5:
            record["name"] = cells[0]
            record["license_number"] = cells[1]
            record["profession"] = cells[2]
            record["status"] = cells[3]
            record["city"] = cells[4] if len(cells) > 4 else None
        elif len(cells) >= 4:
            record["name"] = cells[0]
            record["license_number"] = cells[1]
            record["profession"] = cells[2]
            record["status"] = cells[3]

        if record.get("license_number"):
            results.append(record)

    return results


def has_more_results(html):
    """Check if results were truncated."""
    return ("your search returned more" in html.lower()
            or "too many results" in html.lower()
            or "narrow your search" in html.lower()
            or "maximum" in html.lower()
            or "exceed" in html.lower())


# ---------------------------------------------------------------------------
# Search iteration
# ---------------------------------------------------------------------------

def generate_name_prefixes():
    """Generate 2-letter last name prefixes for exhaustive search."""
    for c1 in string.ascii_uppercase:
        for c2 in string.ascii_uppercase:
            yield f"{c1}{c2}*"


def load_nh_licenses(conn, nh_client, professions, dry_run=False):
    """Load NH OPLC licenses by iterating through professions and name prefixes."""
    log("=== New Hampshire OPLC Professional Licenses ===")
    grand_total = 0
    seen = set()
    batch = []
    errors = 0

    for profession in professions:
        log(f"  Profession: {profession}")
        prof_total = 0

        # Try wildcard first
        try:
            html = nh_client.search(profession, "*")
            results = parse_results_table(html)

            if results and not has_more_results(html):
                for rec in results:
                    lic_num = rec.get("license_number", "").strip()
                    if not lic_num or lic_num in seen:
                        continue
                    seen.add(lic_num)
                    prof_total += 1

                    batch.append(make_row(lic_num, SOURCE,
                        business_name=rec.get("name"),
                        city=rec.get("city"),
                        zip=rec.get("zip"),
                        status=rec.get("status"),
                        classifications=rec.get("profession") or profession,
                        business_type="Individual",
                    ))

                log(f"    Wildcard got {len(results)} results, {prof_total} new")

                if len(batch) >= BATCH_SIZE:
                    if dry_run:
                        log(f"    [DRY RUN] would upsert {len(batch)}")
                        grand_total += len(batch)
                    else:
                        n = upsert_batch(conn, batch)
                        grand_total += n
                        log(f"    upserted {n}")
                    batch = []

                time.sleep(1)
                continue
        except Exception as e:
            log(f"    Wildcard search error: {e}")

        # Iterate through prefixes
        prefixes = list(generate_name_prefixes())
        log(f"    Iterating {len(prefixes)} name prefixes...")

        for idx, prefix in enumerate(prefixes):
            try:
                html = nh_client.search(profession, prefix)
                results = parse_results_table(html)
            except Exception as e:
                log(f"    ERROR prefix {prefix}: {e}")
                errors += 1
                if errors > 20:
                    log("    Re-initializing session...")
                    try:
                        nh_client.init_session()
                    except Exception:
                        pass
                    errors = 0
                time.sleep(2)
                continue

            new_count = 0
            for rec in results:
                lic_num = rec.get("license_number", "").strip()
                if not lic_num or lic_num in seen:
                    continue
                seen.add(lic_num)
                new_count += 1
                prof_total += 1

                batch.append(make_row(lic_num, SOURCE,
                    business_name=rec.get("name"),
                    city=rec.get("city"),
                    zip=rec.get("zip"),
                    status=rec.get("status"),
                    classifications=rec.get("profession") or profession,
                    business_type="Individual",
                ))

            if new_count > 0 and idx % 50 == 0:
                log(f"    [{idx+1}/{len(prefixes)}] prefix={prefix}: {len(results)} results, {new_count} new (total: {len(seen)})")

            if len(batch) >= BATCH_SIZE:
                if dry_run:
                    log(f"    [DRY RUN] would upsert {len(batch)} (total seen: {len(seen)})")
                    grand_total += len(batch)
                else:
                    n = upsert_batch(conn, batch)
                    grand_total += n
                    log(f"    upserted {n} (running: {grand_total})")
                batch = []

            # Expand to 3-letter if truncated
            if has_more_results(html):
                for c3 in string.ascii_uppercase:
                    three_letter = prefix.rstrip("*") + c3 + "*"
                    try:
                        sub_html = nh_client.search(profession, three_letter)
                        sub_results = parse_results_table(sub_html)
                        for rec in sub_results:
                            lic_num = rec.get("license_number", "").strip()
                            if not lic_num or lic_num in seen:
                                continue
                            seen.add(lic_num)
                            prof_total += 1
                            batch.append(make_row(lic_num, SOURCE,
                                business_name=rec.get("name"),
                                city=rec.get("city"),
                                zip=rec.get("zip"),
                                status=rec.get("status"),
                                classifications=rec.get("profession") or profession,
                                business_type="Individual",
                            ))
                    except Exception:
                        pass
                    time.sleep(0.5)

            time.sleep(0.5)

        log(f"  {profession}: {prof_total:,} records")

    # Flush remaining
    if batch:
        if dry_run:
            log(f"    [DRY RUN] would upsert {len(batch)}")
            grand_total += len(batch)
        else:
            n = upsert_batch(conn, batch)
            grand_total += n

    log(f"NH OPLC total: {grand_total:,} unique records (seen: {len(seen):,})")
    return grand_total


def main():
    parser = argparse.ArgumentParser(description="Load NH OPLC professional licenses")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write to DB")
    parser.add_argument("--profession", help="Load only one profession (e.g. 'Master Electrician')")
    args = parser.parse_args()

    log("New Hampshire OPLC Professional License Loader")
    log(f"Database: {args.db_host}:{DB_PORT}/{DB_NAME}")
    if args.dry_run:
        log("*** DRY RUN MODE ***")

    conn = None
    if not args.dry_run:
        conn = get_conn(args.db_host)

    nh_client = NHLicenseClient()

    try:
        nh_client.init_session()

        profs = [args.profession] if args.profession else PROFESSIONS
        total = load_nh_licenses(conn, nh_client, profs, dry_run=args.dry_run)

        if conn and not args.dry_run:
            cur = conn.cursor()
            cur.execute("""
                SELECT classifications, count(*)
                FROM contractor_licenses
                WHERE state = 'NH' AND source = %s
                GROUP BY classifications
                ORDER BY count(*) DESC
            """, (SOURCE,))
            log("--- Summary by Profession ---")
            for row in cur.fetchall():
                log(f"  {row[0]}: {row[1]:,}")
            cur.close()

        log(f"DONE — {total:,} records {'would be' if args.dry_run else ''} upserted this run")

    except Exception as e:
        log(f"FATAL: {e}")
        raise
    finally:
        nh_client.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
