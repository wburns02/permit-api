#!/usr/bin/env python3
"""
Multi-source property lien and judgment scraper via Socrata Open Data APIs.

Sources:
- NYC ACRIS liens (Tax Liens, Assignments, Returns, Discharges) — ~500K records
- Colorado UCC filings — ~2.5M records
- Connecticut UCC filings — ~833K records
- Cook County IL tax sales — ~201K records

Usage:
    python scrape_property_liens.py --source nyc_acris
    python scrape_property_liens.py --source co_ucc
    python scrape_property_liens.py --source ct_ucc
    python scrape_property_liens.py --source cook_tax
    python scrape_property_liens.py --source all

Requires: pip install httpx psycopg2-binary
"""

import argparse
import os
import sys
import time
import uuid
from datetime import date, datetime

import httpx

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "permits")
DB_USER = os.getenv("DB_USER", "will")

BATCH_SIZE = 5000
PAGE_SIZE = 50000  # Socrata max per request
DELAY = 0.5

# NYC ACRIS doc_type to lien_type mapping
ACRIS_LIEN_TYPE_MAP = {
    "TL&R": "Tax Lien",
    "AL&R": "Assignment of Lien",
    "RTXL": "Return of Tax Lien",
    "DTL": "Discharge of Tax Lien",
}

# NYC borough code mapping
BOROUGH_MAP = {
    "1": "Manhattan",
    "2": "Bronx",
    "3": "Brooklyn",
    "4": "Queens",
    "5": "Staten Island",
}


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER
    )


def ensure_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS property_liens (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            document_id VARCHAR(100),
            lien_type VARCHAR(100),
            filing_number VARCHAR(100),
            address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(2) NOT NULL,
            zip VARCHAR(10),
            borough VARCHAR(50),
            amount FLOAT,
            filing_date DATE,
            lapse_date DATE,
            status VARCHAR(50),
            debtor_name VARCHAR(500),
            creditor_name VARCHAR(500),
            description TEXT,
            source VARCHAR(50) NOT NULL
        )
    """)
    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_liens_doc_id ON property_liens (document_id)",
        "CREATE INDEX IF NOT EXISTS ix_liens_address ON property_liens (address)",
        "CREATE INDEX IF NOT EXISTS ix_liens_lien_type ON property_liens (lien_type)",
        "CREATE INDEX IF NOT EXISTS ix_liens_filing_number ON property_liens (filing_number)",
        "CREATE INDEX IF NOT EXISTS ix_liens_state ON property_liens (state)",
        "CREATE INDEX IF NOT EXISTS ix_liens_state_type ON property_liens (state, lien_type)",
        "CREATE INDEX IF NOT EXISTS ix_liens_filing_date ON property_liens (filing_date)",
        "CREATE INDEX IF NOT EXISTS ix_liens_debtor ON property_liens (debtor_name)",
        "CREATE INDEX IF NOT EXISTS ix_liens_filing_state ON property_liens (filing_number, state)",
        "CREATE INDEX IF NOT EXISTS ix_liens_zip ON property_liens (zip)",
    ]:
        try:
            cur.execute(idx)
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()


def safe_date(val):
    """Parse various date formats from Socrata APIs."""
    if not val:
        return None
    try:
        # Socrata format: 2025-06-16T00:00:00.000
        return datetime.fromisoformat(val.replace("T00:00:00.000", "")).date()
    except (ValueError, AttributeError):
        pass
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except (ValueError, AttributeError):
        return None


def safe_float(val):
    """Parse a float value safely."""
    if not val:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def get_count(url, where_clause=None):
    """Get total count from a Socrata endpoint."""
    count_url = f"{url}?$select=count(*)"
    if where_clause:
        count_url += f"&$where={where_clause}"
    try:
        resp = httpx.get(count_url, timeout=30)
        return int(resp.json()[0]["count"])
    except Exception as e:
        print(f"  Could not get count: {e}")
        return None


# ─── NYC ACRIS Liens ───

def scrape_nyc_acris(conn):
    """
    Scrape NYC ACRIS lien documents.
    Source: data.cityofnewyork.us/resource/bnx9-e6tj.json
    Doc types: TL&R (Tax Lien), AL&R (Assignment), RTXL (Return), DTL (Discharge)
    """
    cur = conn.cursor()
    base_url = "https://data.cityofnewyork.us/resource/bnx9-e6tj.json"
    where_clause = "doc_type in('TL%26R','AL%26R','RTXL','DTL')"
    source = "nyc_acris_liens"

    total_count = get_count(base_url, where_clause)
    if total_count:
        print(f"  Total records available: {total_count:,}")

    total = 0
    offset = 0

    while True:
        url = f"{base_url}?$where={where_clause}&$limit={PAGE_SIZE}&$offset={offset}&$order=:id"
        print(f"  Fetching offset {offset:,}...")

        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            print(f"  Error at offset {offset}: {e}")
            break

        if not records:
            break

        batch = []
        for r in records:
            doc_type_raw = r.get("doc_type", "")
            lien_type = ACRIS_LIEN_TYPE_MAP.get(doc_type_raw, doc_type_raw)
            borough_code = str(r.get("borough", ""))
            borough = BOROUGH_MAP.get(borough_code, borough_code)

            batch.append((
                str(uuid.uuid4()),
                str(r.get("document_id", ""))[:100] or None,       # document_id
                lien_type[:100] if lien_type else None,              # lien_type
                None,                                                 # filing_number
                None,                                                 # address (not in this dataset)
                None,                                                 # city
                "NY",                                                 # state
                None,                                                 # zip
                borough[:50] if borough else None,                   # borough
                safe_float(r.get("document_amt")),                   # amount
                safe_date(r.get("document_date")),                   # filing_date
                None,                                                 # lapse_date
                None,                                                 # status
                None,                                                 # debtor_name
                None,                                                 # creditor_name
                f"{doc_type_raw} recorded {r.get('recorded_datetime', '')[:10]}" if r.get("recorded_datetime") else None,  # description
                source,                                               # source
            ))

        if batch:
            execute_values(cur, """
                INSERT INTO property_liens (id, document_id, lien_type, filing_number,
                    address, city, state, zip, borough, amount, filing_date, lapse_date,
                    status, debtor_name, creditor_name, description, source)
                VALUES %s ON CONFLICT DO NOTHING
            """, batch)
            conn.commit()
            total += len(batch)
            pct = f" ({total * 100 // total_count}%)" if total_count else ""
            print(f"    Loaded {total:,}{pct}")

        offset += PAGE_SIZE
        if len(records) < PAGE_SIZE:
            break
        time.sleep(DELAY)

    cur.close()
    return total


# ─── Colorado UCC Filings ───

def scrape_co_ucc(conn):
    """
    Scrape Colorado UCC filings.
    Source: data.colorado.gov/resource/wffy-3uut.json
    """
    cur = conn.cursor()
    base_url = "https://data.colorado.gov/resource/wffy-3uut.json"
    source = "co_ucc"

    total_count = get_count(base_url)
    if total_count:
        print(f"  Total records available: {total_count:,}")

    total = 0
    offset = 0

    while True:
        url = f"{base_url}?$limit={PAGE_SIZE}&$offset={offset}&$order=:id"
        print(f"  Fetching offset {offset:,}...")

        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            print(f"  Error at offset {offset}: {e}")
            break

        if not records:
            break

        batch = []
        for r in records:
            filing_number = str(r.get("fileId", r.get("fileid", "")))[:100] or None
            termination_flag = r.get("terminationFlag", r.get("terminationflag", ""))
            if termination_flag and str(termination_flag).upper() in ("Y", "TRUE", "1"):
                status = "Terminated"
            else:
                status = "Active"

            doc_type = r.get("documentType", r.get("documenttype", ""))
            fin_type = r.get("financialStatementType", r.get("financialstatementtype", ""))
            description_parts = []
            if doc_type:
                description_parts.append(f"DocType: {doc_type}")
            if fin_type:
                description_parts.append(f"FinType: {fin_type}")

            batch.append((
                str(uuid.uuid4()),
                None,                                                  # document_id
                "UCC Filing",                                          # lien_type
                filing_number,                                         # filing_number
                None,                                                  # address
                None,                                                  # city
                "CO",                                                  # state
                None,                                                  # zip
                None,                                                  # borough
                None,                                                  # amount
                safe_date(r.get("filingDate", r.get("filingdate"))),  # filing_date
                safe_date(r.get("lapseDate", r.get("lapsedate"))),    # lapse_date
                status,                                                # status
                None,                                                  # debtor_name
                None,                                                  # creditor_name
                "; ".join(description_parts)[:500] if description_parts else None,  # description
                source,                                                # source
            ))

        if batch:
            execute_values(cur, """
                INSERT INTO property_liens (id, document_id, lien_type, filing_number,
                    address, city, state, zip, borough, amount, filing_date, lapse_date,
                    status, debtor_name, creditor_name, description, source)
                VALUES %s ON CONFLICT DO NOTHING
            """, batch)
            conn.commit()
            total += len(batch)
            pct = f" ({total * 100 // total_count}%)" if total_count else ""
            print(f"    Loaded {total:,}{pct}")

        offset += PAGE_SIZE
        if len(records) < PAGE_SIZE:
            break
        time.sleep(DELAY)

    cur.close()
    return total


# ─── Connecticut UCC Filings ───

def scrape_ct_ucc(conn):
    """
    Scrape Connecticut UCC filings.
    Source: data.ct.gov/resource/xfev-8smz.json
    """
    cur = conn.cursor()
    base_url = "https://data.ct.gov/resource/xfev-8smz.json"
    source = "ct_ucc"

    total_count = get_count(base_url)
    if total_count:
        print(f"  Total records available: {total_count:,}")

    total = 0
    offset = 0

    while True:
        url = f"{base_url}?$limit={PAGE_SIZE}&$offset={offset}&$order=:id"
        print(f"  Fetching offset {offset:,}...")

        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            print(f"  Error at offset {offset}: {e}")
            break

        if not records:
            break

        batch = []
        for r in records:
            filing_number = str(r.get("id_lien_flng_nbr", ""))[:100] or None
            lien_status = r.get("lien_status", "")
            filing_type = r.get("cd_flng_type", "")
            debtor_name = (r.get("debtor_nm_bus", "") or "")[:500] or None
            debtor_addr = (r.get("debtor_ad_str", "") or "")[:500] or None
            creditor_name = (r.get("sec_party_nm_bus", "") or "")[:500] or None

            # Map filing type to lien_type
            lien_type = "UCC Filing"
            if filing_type:
                ft = filing_type.upper().strip()
                if "TAX" in ft:
                    lien_type = "Tax Lien"
                elif "JUDGMENT" in ft or "JDG" in ft:
                    lien_type = "Judgment"
                elif "MECHANIC" in ft:
                    lien_type = "Mechanic's Lien"

            batch.append((
                str(uuid.uuid4()),
                None,                                                     # document_id
                lien_type[:100],                                          # lien_type
                filing_number,                                            # filing_number
                debtor_addr,                                              # address (debtor address)
                None,                                                     # city
                "CT",                                                     # state
                None,                                                     # zip
                None,                                                     # borough
                None,                                                     # amount
                safe_date(r.get("dt_accept")),                           # filing_date
                safe_date(r.get("dt_lapse")),                            # lapse_date
                (lien_status or "")[:50] or None,                        # status
                debtor_name,                                              # debtor_name
                creditor_name,                                            # creditor_name
                f"Filing type: {filing_type}" if filing_type else None,  # description
                source,                                                   # source
            ))

        if batch:
            execute_values(cur, """
                INSERT INTO property_liens (id, document_id, lien_type, filing_number,
                    address, city, state, zip, borough, amount, filing_date, lapse_date,
                    status, debtor_name, creditor_name, description, source)
                VALUES %s ON CONFLICT DO NOTHING
            """, batch)
            conn.commit()
            total += len(batch)
            pct = f" ({total * 100 // total_count}%)" if total_count else ""
            print(f"    Loaded {total:,}{pct}")

        offset += PAGE_SIZE
        if len(records) < PAGE_SIZE:
            break
        time.sleep(DELAY)

    cur.close()
    return total


# ─── Cook County Tax Sales ───

def scrape_cook_tax(conn):
    """
    Scrape Cook County IL tax sale records.
    Source: datacatalog.cookcountyil.gov/resource/55ju-2fs9.json
    """
    cur = conn.cursor()
    base_url = "https://datacatalog.cookcountyil.gov/resource/55ju-2fs9.json"
    source = "cook_county_tax_sale"

    total_count = get_count(base_url)
    if total_count:
        print(f"  Total records available: {total_count:,}")

    total = 0
    offset = 0

    while True:
        url = f"{base_url}?$limit={PAGE_SIZE}&$offset={offset}&$order=:id"
        print(f"  Fetching offset {offset:,}...")

        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            print(f"  Error at offset {offset}: {e}")
            break

        if not records:
            break

        batch = []
        for r in records:
            buyer_name = (r.get("buyer_name", "") or "")[:500] or None
            amount = safe_float(r.get("total_tax_penalty_amount"))
            amount_paid = safe_float(r.get("total_amount_paid"))
            classification = r.get("classification", "")
            location = r.get("location", "")

            # Use location as address if available
            address = (location or "")[:500] or None

            description_parts = []
            if classification:
                description_parts.append(f"Class: {classification}")
            if amount_paid:
                description_parts.append(f"Amount paid: ${amount_paid:,.2f}")

            batch.append((
                str(uuid.uuid4()),
                None,                                                           # document_id
                "Tax Sale",                                                     # lien_type
                None,                                                           # filing_number
                address,                                                        # address
                None,                                                           # city
                "IL",                                                           # state
                None,                                                           # zip
                None,                                                           # borough
                amount,                                                         # amount
                None,                                                           # filing_date
                None,                                                           # lapse_date
                None,                                                           # status
                None,                                                           # debtor_name
                buyer_name,                                                     # creditor_name (buyer)
                "; ".join(description_parts)[:500] if description_parts else None,  # description
                source,                                                         # source
            ))

        if batch:
            execute_values(cur, """
                INSERT INTO property_liens (id, document_id, lien_type, filing_number,
                    address, city, state, zip, borough, amount, filing_date, lapse_date,
                    status, debtor_name, creditor_name, description, source)
                VALUES %s ON CONFLICT DO NOTHING
            """, batch)
            conn.commit()
            total += len(batch)
            pct = f" ({total * 100 // total_count}%)" if total_count else ""
            print(f"    Loaded {total:,}{pct}")

        offset += PAGE_SIZE
        if len(records) < PAGE_SIZE:
            break
        time.sleep(DELAY)

    cur.close()
    return total


# ─── Main ───

SOURCE_MAP = {
    "nyc_acris": ("NYC ACRIS Liens (NY)", scrape_nyc_acris),
    "co_ucc": ("Colorado UCC Filings (CO)", scrape_co_ucc),
    "ct_ucc": ("Connecticut UCC Filings (CT)", scrape_ct_ucc),
    "cook_tax": ("Cook County Tax Sales (IL)", scrape_cook_tax),
}


def main():
    global DB_HOST

    parser = argparse.ArgumentParser(description="Scrape property liens and judgments via Socrata APIs")
    parser.add_argument("--source", default="all",
                        choices=["all"] + list(SOURCE_MAP.keys()),
                        help="Data source to scrape (or 'all')")
    parser.add_argument("--db-host", default=DB_HOST)
    args = parser.parse_args()

    DB_HOST = args.db_host

    conn = get_conn()
    ensure_table(conn)

    sources = list(SOURCE_MAP.keys()) if args.source == "all" else [args.source]
    grand_total = 0

    for source_key in sources:
        label, scrape_fn = SOURCE_MAP[source_key]
        print(f"\n=== Scraping {label} ===")
        try:
            count = scrape_fn(conn)
            grand_total += count
            print(f"  {label}: {count:,} liens loaded")
        except Exception as e:
            print(f"  ERROR: {e}")
            conn.rollback()

    conn.close()
    print(f"\n{'=' * 50}")
    print(f"Grand total: {grand_total:,} liens scraped across {len(sources)} sources")


if __name__ == "__main__":
    main()
