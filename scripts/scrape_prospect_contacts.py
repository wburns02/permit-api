#!/usr/bin/env python3
"""
Professional License Database Scraper — SALES PROSPECT contacts.

Downloads professional license databases from every state that publishes them
via Socrata open data portals. These are PermitLookup sales prospects:
contractors, electricians, plumbers, HVAC techs, RE agents, inspectors,
appraisers — everyone who needs property intelligence data.

Verified Sources (10 states, ~10M+ records):
 1. TX TDLR All Licenses          — 949K   (contractors, electricians, HVAC, plumbers, roofers)
 2. CT Professional Licenses       — 2.6M   (all professions)
 3. IL Professional Licenses       — 4.7M   (all professions)
 4. CO Professional Licenses       — 1.57M  (DORA regulated professions)
 5. VT DFS Master List             — 11.3K  (gas, electric, plumbing, boiler)
 6. WA L&I Contractor Licenses     — 160K   (contractors with phone numbers)
 7. OR Active Contractor Licenses  — 49K    (electrical, plumbing, boiler, elevator)
 8. DE Professional Licenses       — 347K   (60+ professions)
 9. NY Mold Contractor Licenses    — 2.3K   (mold assessment & remediation — has phone)
10. MO Onsite System Inspectors    — 225    (septic inspectors — has phone)

Skips: CA, FL (already in contractor_licenses table)

Usage:
    python3 scrape_prospect_contacts.py --db-host 100.122.216.15
    nohup python3 -u scrape_prospect_contacts.py --db-host 100.122.216.15 > /tmp/prospects.log 2>&1 &
"""

import argparse
import os
import sys
import time
import uuid
from datetime import datetime

import httpx

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

# ─── Config ───────────────────────────────────────────────────────────────────

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "permits")
DB_USER = os.getenv("DB_USER", "will")
BATCH_SIZE = 5000
PAGE_SIZE = 50000
DELAY = 0.3


def get_conn(host):
    return psycopg2.connect(host=host, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def s(v, maxlen=500):
    """Safe string: strip, truncate, return None for blanks."""
    if v in (None, "", "NA", "N/A", ".", "-", "NONE", "None", "none"):
        return None
    val = str(v).strip()
    # Socrata sometimes uses <Null> for missing values
    if val.lower() in ("<null>", "null", "n/a", "na", "none", ".", "-", ""):
        return None
    return val[:maxlen] or None


def sd(v):
    """Safe date parse."""
    if not v:
        return None
    v = str(v).strip()
    # Handle MM/DD/YYYY
    for fmt in ("%m/%d/%Y", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d", "%m/%d/%y", "%Y%m%d"):
        try:
            return datetime.strptime(v[:min(len(v), 26)], fmt).date()
        except (ValueError, TypeError):
            continue
    # ISO with trailing Z
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        pass
    return None


def clean_phone(v):
    """Normalize phone to digits only, return None if too short."""
    if not v:
        return None
    digits = "".join(c for c in str(v) if c.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) < 7:
        return None
    return digits[:10] if len(digits) >= 10 else digits


def clean_email(v):
    """Basic email validation."""
    if not v:
        return None
    e = str(v).strip().lower()
    if "@" in e and "." in e and len(e) > 5:
        return e[:200]
    return None


# ─── Table Setup ──────────────────────────────────────────────────────────────

def ensure_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS prospect_contacts (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL,
            company TEXT,
            phone TEXT,
            email TEXT,
            license_type TEXT,
            license_number TEXT,
            address TEXT,
            city TEXT,
            state VARCHAR(2) NOT NULL,
            zip TEXT,
            status TEXT,
            expiration_date DATE,
            source TEXT NOT NULL
        )
    """)
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS ix_pc_state_type ON prospect_contacts (state, license_type)",
        "CREATE INDEX IF NOT EXISTS ix_pc_phone ON prospect_contacts (phone) WHERE phone IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS ix_pc_email ON prospect_contacts (email) WHERE email IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS ix_pc_name ON prospect_contacts (name)",
        "CREATE INDEX IF NOT EXISTS ix_pc_city_state ON prospect_contacts (city, state)",
        "CREATE INDEX IF NOT EXISTS ix_pc_source ON prospect_contacts (source)",
    ]:
        try:
            cur.execute(idx_sql)
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()
    log("Table prospect_contacts ready with indexes")


def get_count(conn, source):
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM prospect_contacts WHERE source = %s", (source,))
    c = cur.fetchone()[0]
    cur.close()
    return c


INSERT_SQL = """INSERT INTO prospect_contacts
    (id, name, company, phone, email, license_type, license_number,
     address, city, state, zip, status, expiration_date, source)
    VALUES %s ON CONFLICT DO NOTHING"""


# ─── Generic Socrata Scraper ─────────────────────────────────────────────────

def scrape_socrata(conn, url, process_row, source, label, min_skip=1000):
    """Generic Socrata scraper that pages through all records."""
    existing = get_count(conn, source)
    if existing > min_skip:
        log(f"  SKIP {label} -- already {existing:,} records")
        return 0

    # Get total count
    avail = None
    try:
        r = httpx.get(f"{url}?$select=count(*)", timeout=30)
        r.raise_for_status()
        avail = int(r.json()[0]["count"])
        log(f"  {label}: {avail:,} available")
    except Exception as e:
        log(f"  {label} count failed: {e}")

    cur = conn.cursor()
    total = 0
    offset = 0

    while True:
        try:
            resp = httpx.get(
                f"{url}?$limit={PAGE_SIZE}&$offset={offset}&$order=:id",
                timeout=180
            )
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            log(f"  Error at offset {offset}: {e}")
            break

        if not records:
            break

        batch = []
        for r in records:
            try:
                row = process_row(r)
                if row and row[1]:  # Must have name (index 1)
                    batch.append(row)
            except Exception:
                continue

        if batch:
            try:
                execute_values(cur, INSERT_SQL, batch, page_size=BATCH_SIZE)
                conn.commit()
                total += len(batch)
                pct = f" ({total * 100 // avail}%)" if avail else ""
                log(f"    {total:,}{pct}")
            except Exception as e:
                log(f"    Insert error: {e}")
                conn.rollback()

        offset += PAGE_SIZE
        if len(records) < PAGE_SIZE:
            break
        time.sleep(DELAY)

    cur.close()
    return total


# =============================================================================
# 1. TX TDLR All Licenses — 949K
#    Fields: license_type, license_number, business_name, owner_name,
#            license_expiration_date_mmddccyy, license_subtype,
#            business_county, mailing_address_county
# =============================================================================

def scrape_tx_tdlr(conn):
    log("=== TX TDLR All Licenses (949K) ===")
    url = "https://data.texas.gov/resource/7358-krk7.json"

    def proc(r):
        name = s(r.get("owner_name") or r.get("business_name"))
        if not name:
            return None
        company = s(r.get("business_name")) if r.get("owner_name") else None
        lic_type = s(r.get("license_type"), 200)
        subtype = s(r.get("license_subtype"), 100)
        if subtype and lic_type:
            lic_type = f"{lic_type} - {subtype}"
        county = s(r.get("business_county") or r.get("mailing_address_county"), 100)
        return (
            str(uuid.uuid4()), name, company, None, None,
            lic_type, s(r.get("license_number"), 100),
            None, county, "TX", None,  # city is county for TX
            None, sd(r.get("license_expiration_date_mmddccyy")),
            "tx_tdlr"
        )

    return scrape_socrata(conn, url, proc, "tx_tdlr", "TX TDLR All Licenses")


# =============================================================================
# 2. CT Professional Licenses — 2.6M
#    Fields: credentialid, name, type, fullcredentialcode, credentialtype,
#            credentialnumber, credential, status, statusreason, active,
#            issuedate, effectivedate, expirationdate, address, city, state, zip
# =============================================================================

def scrape_ct_licenses(conn):
    log("=== CT Professional Licenses (2.6M) ===")
    url = "https://data.ct.gov/resource/ngch-56tr.json"

    def proc(r):
        name = s(r.get("name"))
        if not name:
            return None
        return (
            str(uuid.uuid4()), name, None, None, None,
            s(r.get("credential") or r.get("credentialtype"), 200),
            s(r.get("credentialnumber"), 100),
            s(r.get("address"), 500),
            s(r.get("city"), 100), s(r.get("state"), 2) or "CT",
            s(r.get("zip"), 10),
            s(r.get("status"), 50),
            sd(r.get("expirationdate")),
            "ct_elicense"
        )

    return scrape_socrata(conn, url, proc, "ct_elicense", "CT Professional Licenses")


# =============================================================================
# 3. IL Professional Licenses — 4.7M
#    Fields: license_type, description, license_number, license_status,
#            business, title, first_name, middle, last_name, prefix, suffix,
#            business_name, businessdba, original_issue_date, effective_date,
#            expiration_date, city, state, zip, county, ever_disciplined
# =============================================================================

def scrape_il_licenses(conn):
    log("=== IL Professional Licenses (4.7M) ===")
    url = "https://illinois-edp.data.socrata.com/resource/pzzh-kp68.json"

    def proc(r):
        # Build name from parts
        parts = []
        for k in ("prefix", "first_name", "middle", "last_name", "suffix"):
            v = s(r.get(k), 100)
            if v:
                parts.append(v)
        name = " ".join(parts) if parts else s(r.get("business_name"))
        if not name:
            return None
        company = s(r.get("business_name") or r.get("businessdba"), 300)
        lic_type = s(r.get("description") or r.get("license_type"), 200)
        return (
            str(uuid.uuid4()), name, company, None, None,
            lic_type, s(r.get("license_number"), 100),
            None,
            s(r.get("city"), 100), s(r.get("state"), 2) or "IL",
            s(r.get("zip"), 10),
            s(r.get("license_status"), 50),
            sd(r.get("expiration_date")),
            "il_idfpr"
        )

    return scrape_socrata(conn, url, proc, "il_idfpr", "IL Professional Licenses")


# =============================================================================
# 4. CO Professional Licenses — 1.57M
#    Fields: lastname, firstname, middlename, city, state, mailzipcode,
#            licensetype, licensenumber, licensefirstissuedate,
#            licenselastreneweddate, licenseexpirationdate,
#            licensestatusdescription
# =============================================================================

def scrape_co_licenses(conn):
    log("=== CO Professional Licenses (1.57M) ===")
    url = "https://data.colorado.gov/resource/7s5z-vewr.json"

    def proc(r):
        parts = []
        for k in ("firstname", "middlename", "lastname"):
            v = s(r.get(k), 100)
            if v:
                parts.append(v)
        name = " ".join(parts) if parts else None
        if not name:
            return None
        return (
            str(uuid.uuid4()), name, None, None, None,
            s(r.get("licensetype"), 200),
            s(r.get("licensenumber"), 100),
            None,
            s(r.get("city"), 100), s(r.get("state"), 2) or "CO",
            s(r.get("mailzipcode"), 10),
            s(r.get("licensestatusdescription"), 50),
            sd(r.get("licenseexpirationdate")),
            "co_dora"
        )

    return scrape_socrata(conn, url, proc, "co_dora", "CO Professional Licenses")


# =============================================================================
# 5. VT DFS Master List — 11.3K
#    Fields: last_name, first_name, street_address, city, state, zip_code,
#            license_number, license_exp_date, type_desc, level_desc
# =============================================================================

def scrape_vt_licenses(conn):
    log("=== VT DFS Master List (11.3K) ===")
    url = "https://data.vermont.gov/resource/cy8e-89cz.json"

    def proc(r):
        first = s(r.get("first_name"), 100)
        last = s(r.get("last_name"), 100)
        if not last:
            return None
        name = f"{first} {last}" if first else last
        lic_type = s(r.get("type_desc"), 100)
        level = s(r.get("level_desc"), 100)
        if level and lic_type:
            lic_type = f"{lic_type} - {level}"
        return (
            str(uuid.uuid4()), name, None, None, None,
            lic_type, s(r.get("license_number"), 100),
            s(r.get("street_address"), 500),
            s(r.get("city"), 100), s(r.get("state"), 2) or "VT",
            s(r.get("zip_code"), 10),
            None, sd(r.get("license_exp_date")),
            "vt_dfs"
        )

    return scrape_socrata(conn, url, proc, "vt_dfs", "VT DFS Master List")


# =============================================================================
# 6. WA L&I Contractor Licenses — 160K  ** HAS PHONE **
#    Fields: businessname, contractorlicensenumber, contractorlicensetypecode,
#            contractorlicensetypecodedesc, address1, city, state, zip,
#            phonenumber, licenseeffectivedate, licenseexpirationdate,
#            businesstypecode, businesstypecodedesc, specialtycode1,
#            specialtycode1desc, ubi, primaryprincipalname, statuscode,
#            contractorlicensestatus
# =============================================================================

def scrape_wa_contractors(conn):
    log("=== WA L&I Contractor Licenses (160K) ===")
    url = "https://data.wa.gov/resource/m8qx-ubtq.json"

    def proc(r):
        name = s(r.get("primaryprincipalname") or r.get("businessname"))
        if not name:
            return None
        company = s(r.get("businessname"), 300)
        lic_type = s(r.get("contractorlicensetypecodedesc"), 200)
        spec = s(r.get("specialtycode1desc"), 200)
        if spec and lic_type:
            lic_type = f"{lic_type} - {spec}"
        return (
            str(uuid.uuid4()), name, company,
            clean_phone(r.get("phonenumber")), None,
            lic_type,
            s(r.get("contractorlicensenumber"), 100),
            s(r.get("address1"), 500),
            s(r.get("city"), 100), s(r.get("state"), 2) or "WA",
            s(r.get("zip"), 10),
            s(r.get("contractorlicensestatus"), 50),
            sd(r.get("licenseexpirationdate")),
            "wa_lni"
        )

    return scrape_socrata(conn, url, proc, "wa_lni", "WA L&I Contractors")


# =============================================================================
# 7. OR Active Contractor Licenses — 49K
#    Fields: licnbr, profession, lictype, full_name, dba, addr1, addr4,
#            city, state, zipcode, county, lic_status, expiration_date
# =============================================================================

def scrape_or_contractors(conn):
    log("=== OR Active Contractor Licenses (49K) ===")
    url = "https://data.oregon.gov/resource/vhbr-cuaq.json"

    def proc(r):
        name = s(r.get("full_name"))
        if not name:
            return None
        company = s(r.get("dba"), 300)
        lic_type = s(r.get("profession"), 200)
        sub = s(r.get("lictype"), 100)
        if sub and lic_type:
            lic_type = f"{lic_type} - {sub}"
        return (
            str(uuid.uuid4()), name, company, None, None,
            lic_type, s(r.get("licnbr"), 100),
            s(r.get("addr1"), 500),
            s(r.get("city"), 100), s(r.get("state"), 2) or "OR",
            s(r.get("zipcode"), 10),
            s(r.get("lic_status"), 50),
            sd(r.get("expiration_date")),
            "or_bcd"
        )

    return scrape_socrata(conn, url, proc, "or_bcd", "OR Active Contractors")


# =============================================================================
# 8. DE Professional Licenses — 347K
#    Fields: last_name, first_name, combined_name, license_no, profession_id,
#            license_type, city, state, zip_code, country, issue_date,
#            expiration_date, disciplinary_action, license_status
# =============================================================================

def scrape_de_licenses(conn):
    log("=== DE Professional Licenses (347K) ===")
    url = "https://data.delaware.gov/resource/pjnv-eaih.json"

    def proc(r):
        name = s(r.get("combined_name"))
        if not name:
            first = s(r.get("first_name"), 100)
            last = s(r.get("last_name"), 100)
            if not last:
                return None
            name = f"{first} {last}" if first else last
        return (
            str(uuid.uuid4()), name, None, None, None,
            s(r.get("license_type"), 200),
            s(r.get("license_no"), 100),
            None,
            s(r.get("city"), 100), s(r.get("state"), 2) or "DE",
            s(r.get("zip_code"), 10),
            s(r.get("license_status"), 50),
            sd(r.get("expiration_date")),
            "de_dpr"
        )

    return scrape_socrata(conn, url, proc, "de_dpr", "DE Professional Licenses")


# =============================================================================
# 9. NY Mold Contractor Licenses — 2.3K  ** HAS PHONE **
#    Fields: license_number, license_type, business_name, address, address_2,
#            city, state, zip_code, phone, issued_date, expiration_date,
#            license_status
# =============================================================================

def scrape_ny_mold(conn):
    log("=== NY Mold Contractor Licenses (2.3K) ===")
    url = "https://data.ny.gov/resource/ikqx-ispy.json"

    def proc(r):
        name = s(r.get("business_name"))
        if not name:
            return None
        addr = s(r.get("address"), 500)
        addr2 = s(r.get("address_2"), 200)
        if addr and addr2:
            addr = f"{addr}, {addr2}"
        return (
            str(uuid.uuid4()), name, name,
            clean_phone(r.get("phone")), None,
            s(r.get("license_type"), 200),
            s(r.get("license_number"), 100),
            addr,
            s(r.get("city"), 100), s(r.get("state"), 2) or "NY",
            s(r.get("zip_code"), 10),
            s(r.get("license_status"), 50),
            sd(r.get("expiration_date")),
            "ny_mold"
        )

    return scrape_socrata(conn, url, proc, "ny_mold", "NY Mold Contractors")


# =============================================================================
# 10. MO Onsite System Inspectors — 225  ** HAS PHONE **
#     Fields: last_name, first_name, date_of_expiration, business,
#             business_phone, counties_served, insp_id, city, state
# =============================================================================

def scrape_mo_inspectors(conn):
    log("=== MO Onsite System Inspectors (225) ===")
    url = "https://data.mo.gov/resource/ppia-3k9s.json"

    def proc(r):
        first = s(r.get("first_name"), 100)
        last = s(r.get("last_name"), 100)
        if not last:
            return None
        name = f"{first} {last}" if first else last
        return (
            str(uuid.uuid4()), name,
            s(r.get("business"), 300),
            clean_phone(r.get("business_phone")), None,
            "Onsite System Inspector/Evaluator",
            s(r.get("insp_id"), 100),
            None,
            s(r.get("city"), 100),
            s(r.get("state"), 2) or "MO",
            None, None,
            sd(r.get("date_of_expiration")),
            "mo_dnr"
        )

    return scrape_socrata(conn, url, proc, "mo_dnr", "MO Onsite Inspectors")


# =============================================================================
# MASTER RUNNER
# =============================================================================

ALL_SOURCES = [
    ("TX TDLR All Licenses (949K)", scrape_tx_tdlr),
    ("CT Professional Licenses (2.6M)", scrape_ct_licenses),
    ("IL Professional Licenses (4.7M)", scrape_il_licenses),
    ("CO Professional Licenses (1.57M)", scrape_co_licenses),
    ("VT DFS Master List (11.3K)", scrape_vt_licenses),
    ("WA L&I Contractor Licenses (160K)", scrape_wa_contractors),
    ("OR Active Contractor Licenses (49K)", scrape_or_contractors),
    ("DE Professional Licenses (347K)", scrape_de_licenses),
    ("NY Mold Contractor Licenses (2.3K)", scrape_ny_mold),
    ("MO Onsite System Inspectors (225)", scrape_mo_inspectors),
]


def print_report(conn):
    """Print summary report after loading."""
    cur = conn.cursor()
    log("\n" + "=" * 70)
    log("PROSPECT CONTACTS REPORT")
    log("=" * 70)

    # Total by state
    cur.execute("""
        SELECT state, count(*) as cnt
        FROM prospect_contacts
        GROUP BY state
        ORDER BY cnt DESC
    """)
    rows = cur.fetchall()
    grand = sum(r[1] for r in rows)
    log(f"\nTotal contacts: {grand:,}")
    log(f"\nBy State:")
    for state, cnt in rows:
        log(f"  {state}: {cnt:,}")

    # Total with phone
    cur.execute("SELECT count(*) FROM prospect_contacts WHERE phone IS NOT NULL")
    phone_ct = cur.fetchone()[0]
    log(f"\nWith phone numbers: {phone_ct:,} ({phone_ct * 100 // max(grand, 1)}%)")

    # Total with email
    cur.execute("SELECT count(*) FROM prospect_contacts WHERE email IS NOT NULL")
    email_ct = cur.fetchone()[0]
    log(f"With email addresses: {email_ct:,} ({email_ct * 100 // max(grand, 1)}%)")

    # Top license types
    cur.execute("""
        SELECT license_type, count(*) as cnt
        FROM prospect_contacts
        WHERE license_type IS NOT NULL
        GROUP BY license_type
        ORDER BY cnt DESC
        LIMIT 30
    """)
    rows = cur.fetchall()
    log(f"\nTop License Types:")
    for lt, cnt in rows:
        log(f"  {lt}: {cnt:,}")

    # By source
    cur.execute("""
        SELECT source, count(*) as cnt
        FROM prospect_contacts
        GROUP BY source
        ORDER BY cnt DESC
    """)
    rows = cur.fetchall()
    log(f"\nBy Source:")
    for src, cnt in rows:
        log(f"  {src}: {cnt:,}")

    cur.close()


def main():
    parser = argparse.ArgumentParser(description="Scrape professional license databases for prospect contacts")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    parser.add_argument("--source", default="all",
                        help="Specific source to run (e.g. tx_tdlr, wa_lni) or 'all'")
    args = parser.parse_args()

    conn = get_conn(args.db_host)
    log(f"Connected to {args.db_host}")
    ensure_tables(conn)

    # Map source names to functions for selective runs
    source_map = {
        "tx_tdlr": ("TX TDLR All Licenses (949K)", scrape_tx_tdlr),
        "ct_elicense": ("CT Professional Licenses (2.6M)", scrape_ct_licenses),
        "il_idfpr": ("IL Professional Licenses (4.7M)", scrape_il_licenses),
        "co_dora": ("CO Professional Licenses (1.57M)", scrape_co_licenses),
        "vt_dfs": ("VT DFS Master List (11.3K)", scrape_vt_licenses),
        "wa_lni": ("WA L&I Contractor Licenses (160K)", scrape_wa_contractors),
        "or_bcd": ("OR Active Contractor Licenses (49K)", scrape_or_contractors),
        "de_dpr": ("DE Professional Licenses (347K)", scrape_de_licenses),
        "ny_mold": ("NY Mold Contractor Licenses (2.3K)", scrape_ny_mold),
        "mo_dnr": ("MO Onsite System Inspectors (225)", scrape_mo_inspectors),
    }

    if args.source != "all":
        if args.source not in source_map:
            log(f"Unknown source: {args.source}. Available: {', '.join(source_map.keys())}")
            sys.exit(1)
        sources = [source_map[args.source]]
    else:
        sources = ALL_SOURCES

    grand = 0
    results = []
    for name, func in sources:
        log(f"\n{'=' * 60}")
        log(f"*** {name} ***")
        log(f"{'=' * 60}")
        try:
            c = func(conn)
            grand += c
            results.append((name, c, "OK"))
            log(f"*** DONE: {name} -- {c:,} loaded ***")
        except Exception as e:
            log(f"*** FAIL: {name} -- {e} ***")
            results.append((name, 0, f"FAIL: {e}"))
            conn.rollback()

    # Print summary
    log(f"\n{'=' * 60}")
    log(f"COMPLETE -- {grand:,} new records loaded")
    log(f"{'=' * 60}")
    for n, c, status in results:
        log(f"  {n}: {c:,} ({status})")

    # Full report
    print_report(conn)
    conn.close()


if __name__ == "__main__":
    main()
