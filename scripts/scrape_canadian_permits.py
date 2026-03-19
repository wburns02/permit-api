#!/usr/bin/env python3
"""
Multi-city Canadian building permit scraper.

Loads permits from Calgary, Edmonton, Winnipeg, Toronto, and Vancouver
into the existing `permits` table on T430 PostgreSQL.

APIs:
- Calgary:   Socrata  — data.calgary.ca          (486K records)
- Edmonton:  Socrata  — data.edmonton.ca         (238K records)
- Winnipeg:  Socrata  — data.winnipeg.ca         (157K records)
- Toronto:   CKAN     — ckan0.cf.opendata.inter.prod-toronto.ca (621K records)
- Vancouver: OpenDataSoft — opendata.vancouver.ca (50K records)

Usage:
    python scrape_canadian_permits.py --city calgary
    python scrape_canadian_permits.py --city toronto
    python scrape_canadian_permits.py --city all
    python scrape_canadian_permits.py --city all --db-host 100.122.216.15

Requires: pip install httpx psycopg2-binary
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

import httpx

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

# ── Database config ──────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "permits")
DB_USER = os.getenv("DB_USER", "will")

BATCH_SIZE = 5000
DELAY = 0.5  # seconds between API pages

# ── Province code mapping ────────────────────────────────────────────────────
PROVINCE_CODES = {
    "calgary": "AB",
    "edmonton": "AB",
    "winnipeg": "MB",
    "toronto": "ON",
    "vancouver": "BC",
}

# ── City API configurations ──────────────────────────────────────────────────
CITY_CONFIGS = {
    "calgary": {
        "api_type": "socrata",
        "base_url": "https://data.calgary.ca/resource/c2es-76ed.json",
        "source": "calgary_socrata",
        "city_name": "Calgary",
        "province": "AB",
        "page_size": 50000,
        "fields": {
            "permit_number": "permitnum",
            "status": "statuscurrent",
            "date_created": "applieddate",
            "issue_date": "issueddate",
            "completed_date": "completeddate",
            "project_type": "permittype",
            "work_type": "workclassgroup",
            "description": "description",
            "contractor_name": "contractorname",
            "valuation": "estprojectcost",
            "address": "originaladdress",
            "neighbourhood": "communityname",
            "lat": "latitude",
            "lng": "longitude",
        },
    },
    "edmonton": {
        "api_type": "socrata",
        "base_url": "https://data.edmonton.ca/resource/24uj-dj8v.json",
        "source": "edmonton_socrata",
        "city_name": "Edmonton",
        "province": "AB",
        "page_size": 50000,
        "fields": {
            "permit_number": None,  # No permit number field
            "status": None,
            "date_created": "permit_date",
            "issue_date": "issue_date",
            "completed_date": None,
            "project_type": "job_category",
            "work_type": "work_type",
            "description": "job_description",
            "contractor_name": None,
            "valuation": "construction_value",
            "address": "address",
            "neighbourhood": "neighbourhood",
            "lat": None,
            "lng": None,
            # Edmonton-specific extras
            "building_type": "building_type",
            "floor_area": "floor_area",
        },
    },
    "winnipeg": {
        "api_type": "socrata",
        "base_url": "https://data.winnipeg.ca/resource/it4w-cpf4.json",
        "source": "winnipeg_socrata",
        "city_name": "Winnipeg",
        "province": "MB",
        "page_size": 50000,
        "fields": {
            "permit_number": "permit_number",
            "status": "status",
            "date_created": None,
            "issue_date": "issue_date",
            "completed_date": None,
            "project_type": "permit_type",
            "work_type": "work_type",
            "description": "sub_type",
            "contractor_name": "applicant_business_name",
            "valuation": None,
            "address_parts": ["street_number", "street_name"],
            "neighbourhood": "neighbourhood_name",
            "lat": None,
            "lng": None,
        },
    },
    "toronto": {
        "api_type": "ckan",
        "base_url": "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/datastore_search",
        "resource_ids": [
            "6d0229af-bc54-46de-9c2b-26759b01dd05",  # Active permits
            "a96c0ba4-3026-402b-b09d-5b1268b8f810",  # Cleared permits
        ],
        "source": "toronto_ckan",
        "city_name": "Toronto",
        "province": "ON",
        "page_size": 5000,  # CKAN default max is 32000 but 5000 is safer
        "fields": {
            "permit_number": "PERMIT_NUM",
            "status": "STATUS",
            "date_created": "APPLICATION_DATE",
            "issue_date": "ISSUED_DATE",
            "completed_date": None,
            "project_type": "PERMIT_TYPE",
            "work_type": "WORK",
            "description": "DESCRIPTION",
            "contractor_name": "BUILDER_NAME",
            "valuation": "EST_CONST_COST",
            "address_parts": ["STREET_NUM", "STREET_NAME"],
            "zip_code": "POSTAL",
            "neighbourhood": None,
            "lat": None,
            "lng": None,
        },
    },
    "vancouver": {
        "api_type": "opendatasoft",
        "base_url": "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/issued-building-permits/records",
        "source": "vancouver_ods",
        "city_name": "Vancouver",
        "province": "BC",
        "page_size": 100,  # ODS v2.1 max is 100
        "fields": {
            "permit_number": "permitnumber",
            "status": None,  # All are "issued" by definition
            "date_created": None,
            "issue_date": "issuedate",
            "completed_date": None,
            "project_type": "typeofwork",
            "work_type": None,
            "description": "projectdescription",
            "contractor_name": "buildingcontractor",
            "valuation": "projectvalue",
            "address": "address",
            "neighbourhood": None,
            "lat": None,
            "lng": None,
        },
    },
}


# ── Helpers ──────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER,
        connect_timeout=10
    )


def ensure_partitions(conn):
    """Create partitions for Canadian provinces if they don't exist.

    The permits table has a DEFAULT partition, so Canadian province codes will
    land there automatically. Creating dedicated partitions requires detaching
    the default partition first (very slow on large tables), so we only create
    them if no default exists. Otherwise we just verify and move on.
    """
    provinces = {"AB", "BC", "MB", "ON", "QC"}
    cur = conn.cursor()

    # Check for default partition
    cur.execute("SELECT 1 FROM pg_class WHERE relname = 'permits_default'")
    has_default = bool(cur.fetchone())

    for prov in sorted(provinces):
        part_name = f"permits_{prov.lower()}"
        cur.execute(
            "SELECT 1 FROM pg_class WHERE relname = %s", (part_name,)
        )
        if cur.fetchone():
            print(f"  Partition {part_name} already exists")
        elif has_default:
            print(f"  Partition {part_name} not found — rows will go to permits_default")
        else:
            print(f"  Creating partition {part_name} for province {prov}")
            cur.execute(
                f"CREATE TABLE {part_name} PARTITION OF permits "
                f"FOR VALUES IN ('{prov}')"
            )
            conn.commit()

    cur.close()


def safe_date(val):
    """Parse various date formats into a datetime or None."""
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    val = str(val).strip()
    if not val:
        return None
    # Try common formats
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",   # Socrata: 2025-06-16T00:00:00.000
        "%Y-%m-%dT%H:%M:%S",       # ISO without millis
        "%Y-%m-%d",                 # Plain date
        "%m/%d/%Y",                 # US format (sometimes used)
        "%d/%m/%Y",                 # CA format (rare)
    ):
        try:
            return datetime.strptime(val[:26], fmt)
        except (ValueError, IndexError):
            continue
    # Last resort: try the first 10 chars as YYYY-MM-DD
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d")
    except (ValueError, IndexError):
        return None


def safe_float(val):
    """Parse a numeric value into float or None."""
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def safe_str(val, max_len=None):
    """Safely convert to string, truncate if needed."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if max_len:
        s = s[:max_len]
    return s


def build_address_from_parts(record, field_list):
    """Join address parts from a list of field names."""
    parts = []
    for f in field_list:
        v = record.get(f)
        if v and str(v).strip():
            parts.append(str(v).strip())
    return " ".join(parts) if parts else None


# ── Insert logic ─────────────────────────────────────────────────────────────

INSERT_SQL = """
    INSERT INTO permits (
        permit_number, address, city, state_code, zip_code,
        lat, lng, project_type, work_type, description,
        status, date_created, applicant_name, subdivision,
        source, raw_data, loaded_at
    ) VALUES %s
"""

INSERT_TEMPLATE = (
    "%(permit_number)s, %(address)s, %(city)s, %(state_code)s, %(zip_code)s, "
    "%(lat)s, %(lng)s, %(project_type)s, %(work_type)s, %(description)s, "
    "%(status)s, %(date_created)s, %(applicant_name)s, %(subdivision)s, "
    "%(source)s, %(raw_data)s, %(loaded_at)s"
)


def make_row(record, config):
    """Transform a raw API record into a dict matching the permits table."""
    fm = config["fields"]

    # Address: either a single field or parts to join
    if "address_parts" in fm:
        address = build_address_from_parts(record, fm["address_parts"])
    else:
        addr_field = fm.get("address")
        address = safe_str(record.get(addr_field)) if addr_field else None

    # Permit number
    pn_field = fm.get("permit_number")
    permit_number = safe_str(record.get(pn_field)) if pn_field else None

    # Status
    status_field = fm.get("status")
    status = safe_str(record.get(status_field)) if status_field else None
    # Vancouver: all permits in this dataset are issued
    if config["source"] == "vancouver_ods" and not status:
        status = "Issued"

    # Dates — use the best available
    issue_field = fm.get("issue_date")
    created_field = fm.get("date_created")
    issue_dt = safe_date(record.get(issue_field)) if issue_field else None
    created_dt = safe_date(record.get(created_field)) if created_field else None
    # Use whichever date is available; prefer created_date, fallback to issue_date
    date_created = created_dt or issue_dt

    # Project type / work type
    pt_field = fm.get("project_type")
    wt_field = fm.get("work_type")
    project_type = safe_str(record.get(pt_field)) if pt_field else None
    work_type = safe_str(record.get(wt_field)) if wt_field else None

    # Description
    desc_field = fm.get("description")
    description = safe_str(record.get(desc_field), max_len=2000) if desc_field else None

    # Contractor / applicant
    cn_field = fm.get("contractor_name")
    contractor = safe_str(record.get(cn_field)) if cn_field else None

    # Valuation
    val_field = fm.get("valuation")
    valuation = safe_float(record.get(val_field)) if val_field else None

    # Lat/Lng
    lat_field = fm.get("lat")
    lng_field = fm.get("lng")
    lat = safe_float(record.get(lat_field)) if lat_field else None
    lng = safe_float(record.get(lng_field)) if lng_field else None

    # Neighbourhood / subdivision
    nb_field = fm.get("neighbourhood")
    neighbourhood = safe_str(record.get(nb_field)) if nb_field else None

    # Zip / postal code
    zip_field = fm.get("zip_code")
    zip_code = safe_str(record.get(zip_field)) if zip_field else None

    # Skip rows with no address AND no permit number (essentially empty)
    if not address and not permit_number:
        return None

    return {
        "permit_number": permit_number,
        "address": address,
        "city": config["city_name"],
        "state_code": config["province"],
        "zip_code": zip_code,
        "lat": lat,
        "lng": lng,
        "project_type": project_type,
        "work_type": work_type,
        "description": description,
        "status": status,
        "date_created": date_created,
        "applicant_name": contractor,
        "subdivision": neighbourhood,
        "source": config["source"],
        "raw_data": psycopg2.extras.Json(record),
        "loaded_at": datetime.now(timezone.utc),
    }


def flush_batch(cur, conn, batch):
    """Insert a batch of rows using execute_values."""
    if not batch:
        return
    execute_values(
        cur,
        INSERT_SQL,
        batch,
        template=(
            "(%(permit_number)s, %(address)s, %(city)s, %(state_code)s, %(zip_code)s, "
            "%(lat)s, %(lng)s, %(project_type)s, %(work_type)s, %(description)s, "
            "%(status)s, %(date_created)s, %(applicant_name)s, %(subdivision)s, "
            "%(source)s, %(raw_data)s, %(loaded_at)s)"
        ),
    )
    conn.commit()


# ── Socrata scraper ──────────────────────────────────────────────────────────

def scrape_socrata(city: str, config: dict, conn):
    """Scrape permits from a Socrata open data API."""
    cur = conn.cursor()
    base_url = config["base_url"]
    page_size = config["page_size"]
    total = 0
    offset = 0

    # Get total record count
    count_url = f"{base_url}?$select=count(*)"
    try:
        resp = httpx.get(count_url, timeout=30)
        total_records = int(resp.json()[0]["count"])
        print(f"  Total records available: {total_records:,}")
    except Exception as e:
        print(f"  Could not get count: {e}")
        total_records = None

    batch = []
    while True:
        url = f"{base_url}?$limit={page_size}&$offset={offset}&$order=:id"
        print(f"  Fetching offset {offset:,}...")

        try:
            resp = httpx.get(url, timeout=120)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            print(f"  ERROR at offset {offset}: {e}")
            break

        if not records:
            break

        for r in records:
            row = make_row(r, config)
            if row:
                batch.append(row)

            if len(batch) >= BATCH_SIZE:
                flush_batch(cur, conn, batch)
                total += len(batch)
                pct = f" ({total * 100 // total_records}%)" if total_records else ""
                print(f"    Loaded {total:,}{pct}")
                batch = []

        offset += page_size
        if len(records) < page_size:
            break
        time.sleep(DELAY)

    # Flush remaining
    if batch:
        flush_batch(cur, conn, batch)
        total += len(batch)

    cur.close()
    return total


# ── CKAN scraper (Toronto) ───────────────────────────────────────────────────

def scrape_ckan(city: str, config: dict, conn):
    """Scrape permits from a CKAN datastore API (Toronto)."""
    cur = conn.cursor()
    base_url = config["base_url"]
    resource_ids = config["resource_ids"]
    page_size = config["page_size"]
    grand_total = 0

    for resource_id in resource_ids:
        print(f"  CKAN resource: {resource_id}")
        offset = 0
        resource_total = 0
        batch = []

        # Get total count for this resource
        try:
            resp = httpx.get(
                base_url,
                params={"resource_id": resource_id, "limit": 0},
                timeout=30,
            )
            data = resp.json()
            total_records = data.get("result", {}).get("total", None)
            if total_records:
                print(f"    Records in resource: {total_records:,}")
        except Exception as e:
            print(f"    Could not get count: {e}")
            total_records = None

        while True:
            print(f"    Fetching offset {offset:,}...")
            try:
                resp = httpx.get(
                    base_url,
                    params={
                        "resource_id": resource_id,
                        "limit": page_size,
                        "offset": offset,
                    },
                    timeout=120,
                )
                resp.raise_for_status()
                data = resp.json()
                records = data.get("result", {}).get("records", [])
            except Exception as e:
                print(f"    ERROR at offset {offset}: {e}")
                break

            if not records:
                break

            for r in records:
                row = make_row(r, config)
                if row:
                    batch.append(row)

                if len(batch) >= BATCH_SIZE:
                    flush_batch(cur, conn, batch)
                    resource_total += len(batch)
                    pct = (
                        f" ({resource_total * 100 // total_records}%)"
                        if total_records
                        else ""
                    )
                    print(f"      Loaded {resource_total:,}{pct}")
                    batch = []

            offset += page_size
            if len(records) < page_size:
                break
            time.sleep(DELAY)

        # Flush remaining for this resource
        if batch:
            flush_batch(cur, conn, batch)
            resource_total += len(batch)

        print(f"    Resource done: {resource_total:,} rows")
        grand_total += resource_total

    cur.close()
    return grand_total


# ── OpenDataSoft scraper (Vancouver) ─────────────────────────────────────────

def scrape_opendatasoft(city: str, config: dict, conn):
    """Scrape permits from an OpenDataSoft v2.1 API (Vancouver)."""
    cur = conn.cursor()
    base_url = config["base_url"]
    page_size = config["page_size"]
    total = 0
    offset = 0
    batch = []

    # Get total count
    try:
        resp = httpx.get(base_url, params={"limit": 0}, timeout=30)
        data = resp.json()
        total_records = data.get("total_count", None)
        if total_records:
            print(f"  Total records available: {total_records:,}")
    except Exception as e:
        print(f"  Could not get count: {e}")
        total_records = None

    while True:
        print(f"  Fetching offset {offset:,}...")
        try:
            resp = httpx.get(
                base_url,
                params={"limit": page_size, "offset": offset},
                timeout=120,
            )
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
        except Exception as e:
            print(f"  ERROR at offset {offset}: {e}")
            break

        if not results:
            break

        for r in results:
            row = make_row(r, config)
            if row:
                batch.append(row)

            if len(batch) >= BATCH_SIZE:
                flush_batch(cur, conn, batch)
                total += len(batch)
                pct = f" ({total * 100 // total_records}%)" if total_records else ""
                print(f"    Loaded {total:,}{pct}")
                batch = []

        offset += page_size
        if len(results) < page_size:
            break
        time.sleep(DELAY)

    # Flush remaining
    if batch:
        flush_batch(cur, conn, batch)
        total += len(batch)

    cur.close()
    return total


# ── Dispatcher ───────────────────────────────────────────────────────────────

SCRAPERS = {
    "socrata": scrape_socrata,
    "ckan": scrape_ckan,
    "opendatasoft": scrape_opendatasoft,
}


def scrape_city(city: str, conn):
    """Scrape permits for a given city."""
    config = CITY_CONFIGS[city]
    api_type = config["api_type"]
    scraper_fn = SCRAPERS[api_type]
    return scraper_fn(city, config, conn)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    global DB_HOST

    parser = argparse.ArgumentParser(
        description="Scrape Canadian building permits from open data APIs"
    )
    parser.add_argument(
        "--city",
        default="all",
        choices=["all"] + list(CITY_CONFIGS.keys()),
        help="City to scrape (or 'all')",
    )
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch first page only, don't insert (for testing)",
    )
    args = parser.parse_args()

    DB_HOST = args.db_host

    conn = get_conn()
    print("Connected to database.")

    # Ensure Canadian province partitions exist
    ensure_partitions(conn)

    cities = list(CITY_CONFIGS.keys()) if args.city == "all" else [args.city]
    grand_total = 0

    for city in cities:
        config = CITY_CONFIGS[city]
        print(f"\n{'='*60}")
        print(f"  Scraping {city.upper()} ({config['source']}, {config['api_type']})")
        print(f"{'='*60}")

        if args.dry_run:
            print("  [DRY RUN] Fetching first page only...")
            # Quick test: fetch one page and print sample
            if config["api_type"] == "socrata":
                url = f"{config['base_url']}?$limit=5"
                resp = httpx.get(url, timeout=30)
                records = resp.json()
            elif config["api_type"] == "ckan":
                resp = httpx.get(
                    config["base_url"],
                    params={"resource_id": config["resource_ids"][0], "limit": 5},
                    timeout=30,
                )
                records = resp.json().get("result", {}).get("records", [])
            elif config["api_type"] == "opendatasoft":
                resp = httpx.get(
                    config["base_url"], params={"limit": 5}, timeout=30
                )
                records = resp.json().get("results", [])
            else:
                records = []

            for i, r in enumerate(records[:3]):
                row = make_row(r, config)
                print(f"  Sample {i+1}:")
                if row:
                    for k, v in row.items():
                        if k != "raw_data" and v is not None:
                            print(f"    {k}: {v}")
                print()
            continue

        try:
            count = scrape_city(city, conn)
            grand_total += count
            print(f"\n  {city.upper()}: {count:,} permits loaded")
        except Exception as e:
            print(f"\n  ERROR scraping {city}: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()

    conn.close()
    print(f"\n{'='*60}")
    print(f"  DONE — {grand_total:,} permits loaded across {len(cities)} cities")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
