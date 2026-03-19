#!/usr/bin/env python3
"""
All-Metro Daily Permit Scraper — pulls fresh permits from 20+ US metros via Socrata.

Each metro has contractor details, valuations, and/or contact info.
Loads into hot_leads table for immediate use by trades.

Usage:
    nohup python3 -u scrape_all_metros_daily.py --db-host 100.122.216.15 > /tmp/all_metros.log 2>&1 &
    python3 scrape_all_metros_daily.py --db-host 100.122.216.15 --days 30  # seed with 30 days
    python3 scrape_all_metros_daily.py --db-host 100.122.216.15 --metro nyc  # single metro

Cron (daily 5:30 AM, after Central TX):
    30 5 * * * python3 -u /home/will/scrape_all_metros_daily.py --db-host 100.122.216.15 >> /tmp/all_metros_daily.log 2>&1
"""

import argparse
import json
import os
import re
import sys
import time
import uuid
from datetime import date, datetime, timedelta

import httpx

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"
BATCH_SIZE = 2000

def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)

def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)

def sf(v):
    """Safe float."""
    if v in (None, "", "NA"): return None
    try: return float(str(v).replace(",","").replace("$","").strip())
    except: return None

def sd(v):
    """Safe date."""
    if not v: return None
    try: return datetime.fromisoformat(v.replace("T00:00:00.000","").replace("Z","")).date()
    except: return None

def s(v, maxlen=500):
    """Safe string truncate."""
    if not v: return None
    return str(v).strip()[:maxlen] or None


# =============================================================================
# METRO CONFIGS — each metro's Socrata API, field mapping, and contact fields
# =============================================================================

METROS = {
    "nyc": {
        "name": "New York City", "state": "NY",
        "url": "https://data.cityofnewyork.us/resource/ipu4-2q9a.json",
        "date_field": "issuance_date", "records": 3985625,
        "fields": {
            "permit_number": "job__", "permit_type": "permit_type",
            "work_class": "permit_subtype", "description": "job_description",
            "address": lambda r: f"{r.get('house__','')} {r.get('street_name','')}".strip(),
            "city": "borough", "zip": "zip_code",
            "contractor_company": "permittee_s_business_name",
            "contractor_phone": "permittee_s_phone__",
            "applicant_name": "owner_s_business_name",
            "applicant_phone": "owner_s_phone__",
            "lat": "gis_latitude", "lng": "gis_longitude",
        },
    },
    "nj_statewide": {
        "name": "New Jersey (Statewide)", "state": "NJ",
        "url": "https://data.nj.gov/resource/w9se-dmra.json",
        "date_field": "date_cert_of_occupancy", "records": 2676188,
        "fields": {
            "permit_number": "permit_number", "permit_type": "use_group",
            "work_class": "construction_type", "description": "type_of_work",
            "address": lambda r: f"{r.get('street_no','')} {r.get('street_name','')}".strip(),
            "city": "municipality", "zip": None,
            "valuation": "estimated_cost", "sqft": "total_floor_area",
        },
    },
    "orlando": {
        "name": "Orlando", "state": "FL",
        "url": "https://data.cityoforlando.net/resource/ryhf-m453.json",
        "date_field": "issue_date", "records": 1092099,
        "fields": {
            "permit_number": "permit_number", "permit_type": "permit_type",
            "work_class": "work_class", "description": "description",
            "address": "address", "city": lambda r: "Orlando", "zip": "zip",
            "valuation": "value", "sqft": "sqft",
            "applicant_name": "owner_name",
        },
    },
    "chicago": {
        "name": "Chicago", "state": "IL",
        "url": "https://data.cityofchicago.org/resource/ydr8-5enu.json",
        "date_field": "issue_date", "records": 830437,
        "fields": {
            "permit_number": "id", "permit_type": "permit_type",
            "work_class": "work_description",
            "address": lambda r: f"{r.get('street_number','')} {r.get('street_direction','')} {r.get('street_name','')} {r.get('suffix','')}".strip(),
            "city": lambda r: "Chicago", "zip": "zip_code",
            "contractor_company": "contractor_1_name",
            "valuation": "reported_cost",
            "lat": "latitude", "lng": "longitude",
        },
    },
    "la": {
        "name": "Los Angeles", "state": "CA",
        "url": "https://data.lacity.org/resource/vdg9-hy7c.json",
        "date_field": "issue_date", "records": 544931,
        "fields": {
            "permit_number": "permit_number", "permit_type": "permit_type",
            "work_class": "permit_sub_type", "description": "work_description",
            "address": lambda r: f"{r.get('address_start','')} {r.get('street_name','')} {r.get('suffix','')}".strip(),
            "city": lambda r: "Los Angeles", "zip": "zip_code",
            "contractor_company": "contractors_business_name",
            "valuation": "valuation", "sqft": "floor_area_l_a_zoning_code_definition",
            "applicant_name": lambda r: f"{r.get('applicant_first_name','')} {r.get('applicant_last_name','')}".strip(),
            "lat": "latitude", "lng": "longitude",
        },
    },
    "henderson": {
        "name": "Henderson (Las Vegas metro)", "state": "NV",
        "url": "https://performance.cityofhenderson.com/resource/fpc9-568j.json",
        "date_field": "issueddate", "records": 260867,
        "fields": {
            "permit_number": "permitnumber", "permit_type": "permittype",
            "work_class": "workclass", "description": "permitdescription",
            "address": "originaladdress", "city": lambda r: "Henderson", "zip": "originalzip",
            "contractor_company": "professionalname",
            "contractor_phone": "professionalphone",
            "valuation": "valuationtotal",
            "lat": "gisx", "lng": "gisy",
        },
    },
    "san_diego_county": {
        "name": "San Diego County", "state": "CA",
        "url": "https://internal-sandiegocounty.data.socrata.com/resource/dyzh-7eat.json",
        "date_field": "issued_date", "records": 236722,
        "fields": {
            "permit_number": "record_id", "permit_type": "record_type",
            "work_class": "record_category", "description": "description",
            "address": "street_address", "city": "city", "zip": "zip_code",
            "contractor_company": "contractor_name",
            "contractor_phone": "contractor_phone",
            "contractor_address": "contractor_address",
        },
    },
    "seattle": {
        "name": "Seattle", "state": "WA",
        "url": "https://cos-data.seattle.gov/resource/76t5-zqzr.json",
        "date_field": "issue_date", "records": 188501,
        "fields": {
            "permit_number": "application_permit_number", "permit_type": "permit_type",
            "work_class": "action_type", "description": "description",
            "address": "address", "city": lambda r: "Seattle", "zip": "zip",
            "contractor_company": "contractor",
            "valuation": "value",
            "lat": "latitude", "lng": "longitude",
        },
    },
    "mesa": {
        "name": "Mesa (Phoenix metro)", "state": "AZ",
        "url": "https://citydata.mesaaz.gov/resource/m2kk-w2hz.json",
        "date_field": "applied_date", "records": 153661,
        "fields": {
            "permit_number": "permit_number", "permit_type": "permit_type",
            "work_class": "work_class", "description": "description",
            "address": "original_address", "city": lambda r: "Mesa", "zip": "original_zip",
            "contractor_company": "contractor_name",
            "valuation": "valuation", "sqft": "total_sqft",
            "lat": "latitude", "lng": "longitude",
        },
    },
    "honolulu": {
        "name": "Honolulu", "state": "HI",
        "url": "https://data.honolulu.gov/resource/4vab-c87q.json",
        "date_field": "issueddate", "records": 432021,
        "fields": {
            "permit_number": "tmk", "permit_type": "permittype",
            "work_class": "status", "description": "description",
            "address": "address", "city": lambda r: "Honolulu",
            "contractor_company": "applicantcontractorbusinessname",
            "valuation": "estimatedvaluation",
        },
    },
    "baton_rouge": {
        "name": "Baton Rouge", "state": "LA",
        "url": "https://data.brla.gov/resource/7fq7-8j7r.json",
        "date_field": "applied_date", "records": 139087,
        "fields": {
            "permit_number": "permit_number", "permit_type": "permit_type",
            "work_class": "work_class", "description": "description",
            "address": "original_address_1", "city": "original_city", "zip": "original_zip",
            "contractor_company": "contractor_name",
            "valuation": "valuation", "sqft": "total_sqft",
        },
    },
    "dallas": {
        "name": "Dallas", "state": "TX",
        "url": "https://www.dallasopendata.com/resource/e7gq-4sah.json",
        "date_field": "issue_date", "records": 126840,
        "fields": {
            "permit_number": "permit_num", "permit_type": "type",
            "description": "work",
            "address": "address", "city": lambda r: "Dallas", "zip": "zip",
            "contractor_company": "contractor",  # has phone embedded, needs parsing
            "valuation": "value", "sqft": "area",
        },
    },
    "cincinnati": {
        "name": "Cincinnati", "state": "OH",
        "url": "https://data.cincinnati-oh.gov/resource/uhjb-xac9.json",
        "date_field": "issueddate", "records": 174228,
        "fields": {
            "permit_number": "permitnum", "permit_type": "permittype",
            "work_class": "workclass", "description": "description",
            "address": "originaladdress", "city": lambda r: "Cincinnati", "zip": "originalzip",
            "contractor_company": "contractorname",
            "valuation": "estprojectcost",
        },
    },
    "kc": {
        "name": "Kansas City", "state": "MO",
        "url": "https://data.kcmo.org/resource/jnga-5v37.json",
        "date_field": "issued_date", "records": 153265,
        "fields": {
            "permit_number": "permit_number", "permit_type": "permit_type",
            "work_class": "work_class", "description": "description",
            "address": "address", "city": lambda r: "Kansas City",
            "contractor_company": "contractor_name",
            "valuation": "valuation",
        },
    },
    "gainesville": {
        "name": "Gainesville", "state": "FL",
        "url": "https://data.cityofgainesville.org/resource/p798-x3nx.json",
        "date_field": "issuedate", "records": 96691,
        "fields": {
            "permit_number": "permitnumber", "permit_type": "permittype",
            "description": "description",
            "address": "location", "city": lambda r: "Gainesville",
            "contractor_company": "contractorname",
        },
    },
    "new_orleans": {
        "name": "New Orleans", "state": "LA",
        "url": "https://data.nola.gov/resource/nbcf-m6c2.json",
        "date_field": "issue_date", "records": 35720,
        "fields": {
            "permit_number": "permit_number", "permit_type": "permit_type",
            "description": "scope_of_work",
            "address": "street_address", "city": lambda r: "New Orleans", "zip": "zip_code",
            "valuation": "total_fee",
            "lat": "latitude", "lng": "longitude",
        },
    },
    "collin_county": {
        "name": "Collin County TX (Plano/McKinney/Allen)", "state": "TX",
        "url": "https://data.texas.gov/resource/82ee-gbj5.json",
        "date_field": "permit_date", "records": 95626,
        "fields": {
            "permit_number": "permit_number",
            "description": "type_of_construction",
            "address": "address", "city": "city", "zip": "zip",
            "contractor_company": "builder_name",
            "valuation": "construction_cost", "sqft": "area_sq_ft",
        },
    },
}


def ensure_hot_leads_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hot_leads (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            permit_number TEXT, permit_type TEXT, work_class TEXT, description TEXT,
            address TEXT, city TEXT, state VARCHAR(2) NOT NULL DEFAULT 'TX', zip TEXT,
            county TEXT, lat FLOAT, lng FLOAT,
            issue_date DATE, applied_date DATE, status TEXT,
            valuation FLOAT, sqft FLOAT, housing_units INTEGER,
            contractor_company TEXT, contractor_name TEXT, contractor_phone TEXT,
            contractor_address TEXT, contractor_city TEXT, contractor_zip TEXT,
            contractor_trade TEXT,
            applicant_name TEXT, applicant_org TEXT, applicant_phone TEXT,
            owner_name TEXT, jurisdiction TEXT,
            source TEXT NOT NULL, scraped_at DATE DEFAULT CURRENT_DATE
        )
    """)
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ix_hot_leads_permit ON hot_leads (permit_number, source)")
    except Exception:
        conn.rollback()
    conn.commit()
    cur.close()


def get_field(r, field_def):
    """Extract field value — handles string keys and lambda functions."""
    if field_def is None:
        return None
    if callable(field_def):
        return field_def(r)
    return r.get(field_def, "")


def scrape_metro(conn, metro_key, config, days=7):
    """Scrape fresh permits for one metro."""
    name = config["name"]
    state = config["state"]
    base_url = config["url"]
    date_field = config["date_field"]
    fields = config["fields"]
    source = f"metro_{metro_key}"

    since = (date.today() - timedelta(days=days)).isoformat()
    log(f"=== {name}, {state} (last {days} days) ===")

    cur = conn.cursor()
    total = 0
    offset = 0

    # Count fresh
    try:
        r = httpx.get(f"{base_url}?$select=count(*)&$where={date_field}>='{since}'", timeout=30)
        available = int(r.json()[0]["count"])
        log(f"  Fresh permits: {available:,}")
    except Exception as e:
        log(f"  Count failed: {e}, proceeding anyway")
        available = None

    while True:
        url = f"{base_url}?$where={date_field}>='{since}'&$order={date_field} DESC&$limit=5000&$offset={offset}"
        try:
            resp = httpx.get(url, timeout=60)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            log(f"  Error at offset {offset}: {e}")
            break

        if not records:
            break

        batch = []
        for r in records:
            addr = get_field(r, fields.get("address"))
            batch.append((
                str(uuid.uuid4()),
                s(get_field(r, fields.get("permit_number")), 100),
                s(get_field(r, fields.get("permit_type")), 50),
                s(get_field(r, fields.get("work_class")), 50),
                s(get_field(r, fields.get("description")), 1000),
                s(addr),
                s(get_field(r, fields.get("city")), 100),
                state,
                s(get_field(r, fields.get("zip")), 10),
                None,  # county
                sf(get_field(r, fields.get("lat"))),
                sf(get_field(r, fields.get("lng"))),
                sd(r.get(date_field)),
                None,  # applied_date
                None,  # status
                sf(get_field(r, fields.get("valuation"))),
                sf(get_field(r, fields.get("sqft"))),
                None,  # housing_units
                s(get_field(r, fields.get("contractor_company"))),
                s(get_field(r, fields.get("contractor_name"))),
                s(get_field(r, fields.get("contractor_phone")), 20),
                s(get_field(r, fields.get("contractor_address"))),
                None, None, None,  # contractor city/zip/trade
                s(get_field(r, fields.get("applicant_name"))),
                None,  # applicant_org
                s(get_field(r, fields.get("applicant_phone")), 20),
                None,  # owner_name
                name,  # jurisdiction
                source,
                date.today(),
            ))

        if batch:
            try:
                execute_values(cur, """
                    INSERT INTO hot_leads (id, permit_number, permit_type, work_class,
                        description, address, city, state, zip, county, lat, lng,
                        issue_date, applied_date, status, valuation, sqft, housing_units,
                        contractor_company, contractor_name, contractor_phone,
                        contractor_address, contractor_city, contractor_zip, contractor_trade,
                        applicant_name, applicant_org, applicant_phone, owner_name,
                        jurisdiction, source, scraped_at)
                    VALUES %s ON CONFLICT (permit_number, source) DO NOTHING
                """, batch)
                conn.commit()
                total += len(batch)
            except Exception as e:
                log(f"  Insert error: {e}")
                conn.rollback()

        offset += 5000
        if len(records) < 5000:
            break
        time.sleep(0.3)

    cur.close()
    log(f"  {name}: {total:,} fresh permits loaded")
    return total


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="All-Metro Daily Permit Scraper")
    parser.add_argument("--db-host", default=DB_HOST)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--metro", default="all", help="Single metro key or 'all'")
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to PostgreSQL at {DB_HOST}")
    ensure_hot_leads_table(conn)

    metros = METROS if args.metro == "all" else {args.metro: METROS[args.metro]}
    grand_total = 0
    results = []

    for key, config in metros.items():
        try:
            count = scrape_metro(conn, key, config, days=args.days)
            grand_total += count
            results.append((config["name"], count, "OK"))
        except Exception as e:
            log(f"  FAILED: {config['name']} — {e}")
            results.append((config["name"], 0, f"FAILED: {e}"))
            conn.rollback()

    conn.close()
    log(f"\n{'='*60}")
    log(f"ALL-METRO SCRAPER COMPLETE — {grand_total:,} total permits")
    log(f"{'='*60}")
    for name, count, status in results:
        log(f"  {name}: {count:,} ({status})")


if __name__ == "__main__":
    main()
