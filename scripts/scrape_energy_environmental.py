#!/usr/bin/env python3
"""
Energy & Environmental Intelligence Scraper — oil/gas, UST, TRI, water wells.

Opens the energy industry as customers: landmen, mineral rights investors,
oil companies, environmental consultants.

Sources:
1. TX Railroad Commission — well permits + production (400K+ wells)
2. EPA Underground Storage Tanks (600K+ tanks)
3. EPA Toxic Release Inventory (4M+ releases)
4. PA DEP Oil & Gas wells (Socrata)
5. OK Corporation Commission wells
6. CO COGCC wells
7. EIA bulk energy data
8. USDA crop data by county

Usage:
    nohup python3 -u scrape_energy_environmental.py --db-host 100.122.216.15 > /tmp/energy_scraper.log 2>&1 &
"""

import argparse
import csv
import gzip
import io
import json
import os
import sys
import time
import uuid
import zipfile
from datetime import date, datetime

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
BATCH_SIZE = 5000


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def sf(v):
    if v in (None, "", "NA", "N/A", ".", "-"): return None
    try: return float(str(v).replace(",", "").replace("$", "").strip())
    except: return None


def si(v):
    if v in (None, "", "NA", "N/A", "."): return None
    try: return int(float(str(v).replace(",", "").strip()))
    except: return None


def sd(v):
    if not v: return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d", "%m/%d/%y"):
        try: return datetime.strptime(str(v).strip()[:10], fmt).date()
        except: continue
    try: return datetime.fromisoformat(v.replace("T00:00:00.000","").replace("Z","")).date()
    except: return None


def s(v, m=500):
    if not v: return None
    return str(v).strip()[:m] or None


def get_count(conn, table, source=None):
    cur = conn.cursor()
    if source:
        cur.execute(f"SELECT count(*) FROM {table} WHERE source = %s", (source,))
    else:
        cur.execute(f"SELECT count(*) FROM {table}")
    c = cur.fetchone()[0]
    cur.close()
    return c


def ensure_tables(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS oil_gas_wells (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            well_id TEXT,
            well_name TEXT,
            operator TEXT,
            well_type TEXT,
            well_status TEXT,
            state VARCHAR(2) NOT NULL,
            county TEXT,
            lat FLOAT,
            lng FLOAT,
            permit_date DATE,
            spud_date DATE,
            completion_date DATE,
            total_depth FLOAT,
            formation TEXT,
            api_number TEXT,
            field_name TEXT,
            district TEXT,
            source TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS underground_storage_tanks (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            facility_id TEXT,
            facility_name TEXT,
            address TEXT,
            city TEXT,
            state VARCHAR(2) NOT NULL,
            zip TEXT,
            county TEXT,
            lat FLOAT,
            lng FLOAT,
            tank_count INTEGER,
            substance TEXT,
            tank_status TEXT,
            install_date DATE,
            closure_date DATE,
            owner_name TEXT,
            source TEXT NOT NULL DEFAULT 'epa_ust'
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS toxic_releases (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            facility_name TEXT NOT NULL,
            address TEXT,
            city TEXT,
            state VARCHAR(2) NOT NULL,
            zip TEXT,
            county TEXT,
            lat FLOAT,
            lng FLOAT,
            chemical TEXT,
            release_type TEXT,
            total_release_lbs FLOAT,
            year INTEGER,
            industry TEXT,
            naics_code TEXT,
            parent_company TEXT,
            source TEXT NOT NULL DEFAULT 'epa_tri'
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS agriculture_data (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            state_fips VARCHAR(2),
            county_fips VARCHAR(3),
            state_name TEXT,
            county_name TEXT,
            year INTEGER,
            commodity TEXT,
            data_item TEXT,
            value FLOAT,
            unit TEXT,
            source TEXT NOT NULL DEFAULT 'usda_nass'
        )
    """)

    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_wells_state ON oil_gas_wells (state, county)",
        "CREATE INDEX IF NOT EXISTS ix_wells_operator ON oil_gas_wells (operator)",
        "CREATE INDEX IF NOT EXISTS ix_wells_geo ON oil_gas_wells (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_wells_api ON oil_gas_wells (api_number)",
        "CREATE INDEX IF NOT EXISTS ix_wells_permit ON oil_gas_wells (permit_date)",
        "CREATE INDEX IF NOT EXISTS ix_ust_state ON underground_storage_tanks (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_ust_geo ON underground_storage_tanks (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_ust_address ON underground_storage_tanks (address)",
        "CREATE INDEX IF NOT EXISTS ix_tri_state ON toxic_releases (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_tri_chemical ON toxic_releases (chemical)",
        "CREATE INDEX IF NOT EXISTS ix_tri_year ON toxic_releases (year)",
        "CREATE INDEX IF NOT EXISTS ix_tri_geo ON toxic_releases (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_ag_state ON agriculture_data (state_fips, county_fips)",
        "CREATE INDEX IF NOT EXISTS ix_ag_year ON agriculture_data (year)",
    ]:
        try:
            cur.execute(idx)
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()


def scrape_socrata_generic(conn, url, process_row, table, insert_sql, source_name, label=""):
    """Generic Socrata scraper with skip-if-exists."""
    existing = get_count(conn, table, source_name)
    if existing > 1000:
        log(f"  SKIP — {source_name} already has {existing:,} records")
        return 0

    try:
        r = httpx.get(f"{url}?$select=count(*)", timeout=30)
        available = int(r.json()[0]["count"])
        log(f"  {label} available: {available:,}")
    except Exception as e:
        log(f"  Count failed: {e}")
        available = None

    cur = conn.cursor()
    total = 0
    offset = 0

    while True:
        fetch_url = f"{url}?$limit=50000&$offset={offset}&$order=:id"
        log(f"  Fetching offset {offset:,}...")
        try:
            resp = httpx.get(fetch_url, timeout=120)
            resp.raise_for_status()
            records = resp.json()
        except Exception as e:
            log(f"  Error: {e}")
            break

        if not records:
            break

        batch = [process_row(r) for r in records]
        batch = [b for b in batch if b]

        if batch:
            try:
                execute_values(cur, insert_sql, batch)
                conn.commit()
                total += len(batch)
                pct = f" ({total*100//available}%)" if available else ""
                log(f"    Loaded {total:,}{pct}")
            except Exception as e:
                log(f"    Insert error: {e}")
                conn.rollback()

        offset += 50000
        if len(records) < 50000:
            break
        time.sleep(0.3)

    cur.close()
    return total


# =============================================================================
# 1. PA DEP Oil & Gas Wells (Socrata)
# =============================================================================

def scrape_pa_wells(conn):
    log("=== PA DEP Oil & Gas Wells ===")

    def process(r):
        return (
            str(uuid.uuid4()),
            s(r.get("well_permit_num"), 100),
            s(r.get("well_name"), 200),
            s(r.get("operator_name"), 200),
            s(r.get("well_type"), 50),
            s(r.get("well_status"), 50),
            "PA",
            s(r.get("county"), 100),
            sf(r.get("latitude")),
            sf(r.get("longitude")),
            sd(r.get("permit_issued_date")),
            sd(r.get("spud_date")),
            sd(r.get("well_completion_date")),
            sf(r.get("total_depth")),
            s(r.get("target_formation"), 100),
            s(r.get("api_well_number"), 50),
            s(r.get("farm_name"), 100),
            s(r.get("region"), 50),
            "pa_dep",
        )

    # Try PA DEP Socrata
    urls = [
        "https://data.pa.gov/resource/tgm8-bgbi.json",  # Spud data
        "https://data.pa.gov/resource/fcfs-2gvm.json",  # Permit data
    ]
    total = 0
    for url in urls:
        name = "pa_dep_" + url.split("/")[-1].split(".")[0]
        total += scrape_socrata_generic(
            conn, url, process, "oil_gas_wells",
            """INSERT INTO oil_gas_wells (id, well_id, well_name, operator, well_type,
                well_status, state, county, lat, lng, permit_date, spud_date,
                completion_date, total_depth, formation, api_number, field_name,
                district, source) VALUES %s ON CONFLICT DO NOTHING""",
            name, f"PA Wells ({name})",
        )
    return total


# =============================================================================
# 2. EPA Underground Storage Tanks
# =============================================================================

def scrape_epa_ust(conn):
    """Download EPA UST finder data."""
    log("=== EPA Underground Storage Tanks ===")

    existing = get_count(conn, "underground_storage_tanks")
    if existing > 10000:
        log(f"  SKIP — already have {existing:,} UST records")
        return 0

    # EPA UST data is available via the UST Finder export
    # Try Socrata on data.epa.gov
    urls_to_try = [
        "https://edg.epa.gov/data/Public/OUST/UST_Finder/UST_Finder_Results.csv",
    ]

    # Also try via EPA's Envirofacts API
    cur = conn.cursor()
    total = 0

    # EPA Envirofacts UST API
    log("  Trying EPA Envirofacts UST API...")
    states = ["TX", "CA", "FL", "NY", "PA", "OH", "IL", "GA", "NC", "MI",
              "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MD", "MO", "WI",
              "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT"]

    for state in states:
        log(f"    UST for {state}...")
        try:
            url = f"https://data.epa.gov/efservice/ust_facility/state_cd/{state}/rows/0:50000/JSON"
            resp = httpx.get(url, timeout=120, follow_redirects=True)
            if resp.status_code != 200:
                log(f"      HTTP {resp.status_code}, skipping")
                continue

            records = resp.json()
            batch = []
            for r in records:
                batch.append((
                    str(uuid.uuid4()),
                    s(r.get("facility_id") or r.get("FACILITY_ID"), 100),
                    s(r.get("facility_name") or r.get("FACILITY_NAME"), 200),
                    s(r.get("address") or r.get("ADDRESS"), 500),
                    s(r.get("city") or r.get("CITY"), 100),
                    state,
                    s(r.get("zip") or r.get("ZIP"), 10),
                    s(r.get("county") or r.get("COUNTY"), 100),
                    sf(r.get("latitude") or r.get("LATITUDE")),
                    sf(r.get("longitude") or r.get("LONGITUDE")),
                    si(r.get("tank_count") or r.get("TANK_COUNT")),
                    s(r.get("substance") or r.get("SUBSTANCE"), 100),
                    s(r.get("status") or r.get("STATUS"), 50),
                    None, None,
                    s(r.get("owner_name") or r.get("OWNER_NAME"), 200),
                    "epa_ust",
                ))

            if batch:
                execute_values(cur, """
                    INSERT INTO underground_storage_tanks (id, facility_id, facility_name,
                        address, city, state, zip, county, lat, lng, tank_count,
                        substance, tank_status, install_date, closure_date, owner_name, source)
                    VALUES %s ON CONFLICT DO NOTHING
                """, batch)
                conn.commit()
                total += len(batch)
                log(f"      {state}: {len(batch):,} tanks")

        except Exception as e:
            log(f"      {state} error: {e}")
            conn.rollback()

        time.sleep(0.5)

    cur.close()
    return total


# =============================================================================
# 3. EPA Toxic Release Inventory
# =============================================================================

def scrape_epa_tri(conn):
    """Download EPA TRI data via Envirofacts API."""
    log("=== EPA Toxic Release Inventory ===")

    existing = get_count(conn, "toxic_releases")
    if existing > 100000:
        log(f"  SKIP — already have {existing:,} TRI records")
        return 0

    cur = conn.cursor()
    total = 0

    # TRI data by year from Envirofacts
    years = [2022, 2023]

    for year in years:
        log(f"  TRI {year}...")
        offset = 0

        while True:
            url = f"https://data.epa.gov/efservice/tri_release_form_r/reporting_year/{year}/rows/{offset}:{offset+10000}/JSON"
            try:
                resp = httpx.get(url, timeout=120, follow_redirects=True)
                if resp.status_code != 200:
                    break
                records = resp.json()
                if not records:
                    break

                batch = []
                for r in records:
                    batch.append((
                        str(uuid.uuid4()),
                        s(r.get("facility_name") or r.get("FACILITY_NAME"), 200),
                        s(r.get("street_address") or r.get("STREET_ADDRESS"), 500),
                        s(r.get("city") or r.get("CITY"), 100),
                        s(r.get("state") or r.get("ST"), 2),
                        s(r.get("zip") or r.get("ZIP"), 10),
                        s(r.get("county") or r.get("COUNTY"), 100),
                        sf(r.get("latitude") or r.get("LATITUDE")),
                        sf(r.get("longitude") or r.get("LONGITUDE")),
                        s(r.get("chemical") or r.get("CHEMICAL"), 200),
                        s(r.get("release_type") or "On-site", 50),
                        sf(r.get("total_releases") or r.get("TOTAL_RELEASES")),
                        year,
                        s(r.get("industry_sector") or r.get("INDUSTRY_SECTOR"), 100),
                        s(r.get("naics") or r.get("PRIMARY_NAICS"), 10),
                        s(r.get("parent_co_name") or r.get("PARENT_CO_NAME"), 200),
                        "epa_tri",
                    ))

                if batch:
                    execute_values(cur, """
                        INSERT INTO toxic_releases (id, facility_name, address, city,
                            state, zip, county, lat, lng, chemical, release_type,
                            total_release_lbs, year, industry, naics_code,
                            parent_company, source) VALUES %s ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                    total += len(batch)
                    log(f"    {year} offset {offset}: {total:,} total")

                offset += 10000
                if len(records) < 10000:
                    break

            except Exception as e:
                log(f"    {year} error at offset {offset}: {e}")
                conn.rollback()
                break

            time.sleep(0.5)

    cur.close()
    return total


# =============================================================================
# 4. CO COGCC Oil & Gas Wells (Socrata)
# =============================================================================

def scrape_co_wells(conn):
    log("=== CO COGCC Oil & Gas Wells ===")

    def process(r):
        return (
            str(uuid.uuid4()),
            s(r.get("api") or r.get("facility_id"), 100),
            s(r.get("well_name") or r.get("facility_name"), 200),
            s(r.get("operator_name") or r.get("operator"), 200),
            s(r.get("well_type") or r.get("facility_type"), 50),
            s(r.get("well_status") or r.get("facility_status"), 50),
            "CO",
            s(r.get("county"), 100),
            sf(r.get("latitude")),
            sf(r.get("longitude")),
            sd(r.get("permit_date") or r.get("first_prod_date")),
            sd(r.get("spud_date")),
            sd(r.get("completion_date")),
            sf(r.get("total_depth") or r.get("td")),
            s(r.get("formation") or r.get("target_formation"), 100),
            s(r.get("api") or r.get("api_number"), 50),
            s(r.get("field_name"), 100),
            None,
            "co_cogcc",
        )

    # Try CO data portal
    return scrape_socrata_generic(
        conn, "https://data.colorado.gov/resource/ene7-v85h.json",
        process, "oil_gas_wells",
        """INSERT INTO oil_gas_wells (id, well_id, well_name, operator, well_type,
            well_status, state, county, lat, lng, permit_date, spud_date,
            completion_date, total_depth, formation, api_number, field_name,
            district, source) VALUES %s ON CONFLICT DO NOTHING""",
        "co_cogcc", "CO COGCC Wells",
    )


# =============================================================================
# 5. OK Corporation Commission Wells
# =============================================================================

def scrape_ok_wells(conn):
    log("=== OK Corporation Commission Wells ===")

    def process(r):
        return (
            str(uuid.uuid4()),
            s(r.get("api_number") or r.get("api"), 100),
            s(r.get("well_name"), 200),
            s(r.get("operator_name") or r.get("operator"), 200),
            s(r.get("well_type"), 50),
            s(r.get("well_status") or r.get("status"), 50),
            "OK",
            s(r.get("county"), 100),
            sf(r.get("latitude") or r.get("lat")),
            sf(r.get("longitude") or r.get("lng")),
            sd(r.get("permit_date") or r.get("permit_issued")),
            sd(r.get("spud_date")),
            sd(r.get("completion_date")),
            sf(r.get("total_depth")),
            s(r.get("formation"), 100),
            s(r.get("api_number") or r.get("api"), 50),
            s(r.get("field_name"), 100),
            None,
            "ok_occ",
        )

    # Try OK open data
    return scrape_socrata_generic(
        conn, "https://data.ok.gov/resource/jzbi-gx5t.json",
        process, "oil_gas_wells",
        """INSERT INTO oil_gas_wells (id, well_id, well_name, operator, well_type,
            well_status, state, county, lat, lng, permit_date, spud_date,
            completion_date, total_depth, formation, api_number, field_name,
            district, source) VALUES %s ON CONFLICT DO NOTHING""",
        "ok_occ", "OK OCC Wells",
    )


# =============================================================================
# 6. USDA NASS Crop Data
# =============================================================================

def scrape_usda_crops(conn):
    """Download USDA NASS crop data via API."""
    log("=== USDA NASS Crop Data ===")

    existing = get_count(conn, "agriculture_data")
    if existing > 100000:
        log(f"  SKIP — already have {existing:,} ag records")
        return 0

    cur = conn.cursor()
    total = 0

    # USDA NASS QuickStats API — free, no key needed for basic queries
    # Get major crops by county for recent years
    commodities = ["CORN", "SOYBEANS", "WHEAT", "COTTON", "CATTLE"]
    years = [2023, 2024]

    for commodity in commodities:
        for year in years:
            log(f"  {commodity} {year}...")
            try:
                url = (f"https://quickstats.nass.usda.gov/api/api_GET/"
                       f"?key=D263FA78-0CFF-3E87-B353-5D16C99D39E9"
                       f"&source_desc=SURVEY&commodity_desc={commodity}"
                       f"&year={year}&agg_level_desc=COUNTY"
                       f"&statisticcat_desc=PRODUCTION"
                       f"&format=JSON")
                resp = httpx.get(url, timeout=60)
                if resp.status_code != 200:
                    # Try without API key
                    url2 = (f"https://quickstats.nass.usda.gov/api/api_GET/"
                            f"?source_desc=SURVEY&commodity_desc={commodity}"
                            f"&year={year}&agg_level_desc=COUNTY"
                            f"&statisticcat_desc=PRODUCTION"
                            f"&format=JSON")
                    resp = httpx.get(url2, timeout=60)

                if resp.status_code == 200:
                    data = resp.json()
                    records = data.get("data", [])
                    batch = []

                    for r in records:
                        val = sf(r.get("Value"))
                        if val is None:
                            continue
                        batch.append((
                            str(uuid.uuid4()),
                            s(r.get("state_fips_code"), 2),
                            s(r.get("county_code"), 3),
                            s(r.get("state_name"), 50),
                            s(r.get("county_name"), 100),
                            year,
                            commodity,
                            s(r.get("short_desc"), 200),
                            val,
                            s(r.get("unit_desc"), 50),
                            "usda_nass",
                        ))

                    if batch:
                        execute_values(cur, """
                            INSERT INTO agriculture_data (id, state_fips, county_fips,
                                state_name, county_name, year, commodity, data_item,
                                value, unit, source) VALUES %s ON CONFLICT DO NOTHING
                        """, batch)
                        conn.commit()
                        total += len(batch)
                        log(f"    {commodity} {year}: {len(batch):,} county records")

            except Exception as e:
                log(f"    {commodity} {year} error: {e}")
                conn.rollback()

            time.sleep(1)  # NASS rate limit

    cur.close()
    return total


# =============================================================================
# 7. EIA Energy Data (state-level production + prices)
# =============================================================================

def scrape_eia_energy(conn):
    """Download EIA bulk energy data."""
    log("=== EIA State Energy Data ===")

    # EIA has a REST API — get state energy profiles
    # We'll store this in a generic energy_data table
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS energy_data (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            series_id TEXT,
            series_name TEXT,
            state VARCHAR(2),
            year INTEGER,
            month INTEGER,
            value FLOAT,
            unit TEXT,
            source TEXT NOT NULL DEFAULT 'eia'
        )
    """)
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS ix_energy_state ON energy_data (state, year)")
        cur.execute("CREATE INDEX IF NOT EXISTS ix_energy_series ON energy_data (series_id)")
    except Exception:
        conn.rollback()
    conn.commit()

    existing = get_count(conn, "energy_data")
    if existing > 10000:
        log(f"  SKIP — already have {existing:,} energy records")
        cur.close()
        return 0

    total = 0

    # EIA API v2 — crude oil production by state
    # Try without API key first (limited but works)
    series_list = [
        ("PET.MCRFP{state}1.M", "Crude Oil Production"),
        ("NG.N9050{state}2.M", "Natural Gas Production"),
        ("ELEC.GEN.ALL-{state}-99.M", "Electricity Generation"),
    ]

    states = ["TX", "CA", "PA", "OK", "CO", "ND", "NM", "WY", "LA", "OH",
              "FL", "NY", "IL", "MI", "WV", "KS", "UT", "MT", "AK", "MS"]

    for series_template, series_name in series_list:
        for state in states:
            series_id = series_template.replace("{state}", state)
            try:
                url = f"https://api.eia.gov/v2/seriesid/{series_id}?api_key=DEMO_KEY"
                resp = httpx.get(url, timeout=30)
                if resp.status_code != 200:
                    continue

                data = resp.json()
                series_data = data.get("response", {}).get("data", [])

                batch = []
                for d in series_data:
                    period = str(d.get("period", ""))
                    yr = si(period[:4])
                    mo = si(period[4:6]) if len(period) >= 6 else None

                    batch.append((
                        str(uuid.uuid4()),
                        series_id,
                        series_name,
                        state,
                        yr,
                        mo,
                        sf(d.get("value")),
                        s(d.get("unit"), 50),
                        "eia",
                    ))

                if batch:
                    execute_values(cur, """
                        INSERT INTO energy_data (id, series_id, series_name, state,
                            year, month, value, unit, source)
                        VALUES %s ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                    total += len(batch)

            except Exception:
                conn.rollback()

            time.sleep(0.5)

    cur.close()
    log(f"  EIA energy: {total:,} data points")
    return total


# =============================================================================
# 8. BLM Mineral Leases (federal land)
# =============================================================================

def scrape_blm_leases(conn):
    """Download BLM mineral lease data."""
    log("=== BLM Mineral Leases ===")

    # BLM publishes lease data — try their API
    # Also available via data.doi.gov Socrata
    def process(r):
        return (
            str(uuid.uuid4()),
            s(r.get("lease_number") or r.get("case_id"), 100),
            s(r.get("lease_name") or r.get("case_name"), 200),
            s(r.get("lessee") or r.get("holder_name"), 200),
            s(r.get("lease_type") or r.get("case_type"), 50),
            s(r.get("status") or r.get("case_disposition"), 50),
            s(r.get("state") or r.get("admin_state"), 2),
            s(r.get("county"), 100),
            sf(r.get("latitude")),
            sf(r.get("longitude")),
            sd(r.get("effective_date") or r.get("issued_date")),
            None, None, None,
            s(r.get("commodity") or r.get("mineral"), 100),
            None,
            s(r.get("field_office"), 100),
            None,
            "blm_leases",
        )

    return scrape_socrata_generic(
        conn, "https://data.doi.gov/resource/jqk5-gza2.json",
        process, "oil_gas_wells",
        """INSERT INTO oil_gas_wells (id, well_id, well_name, operator, well_type,
            well_status, state, county, lat, lng, permit_date, spud_date,
            completion_date, total_depth, formation, api_number, field_name,
            district, source) VALUES %s ON CONFLICT DO NOTHING""",
        "blm_leases", "BLM Mineral Leases",
    )


# =============================================================================
# MASTER RUNNER
# =============================================================================

ALL_SCRAPERS = [
    ("PA DEP Oil & Gas Wells", scrape_pa_wells),
    ("EPA Underground Storage Tanks", scrape_epa_ust),
    ("EPA Toxic Release Inventory", scrape_epa_tri),
    ("CO COGCC Oil & Gas Wells", scrape_co_wells),
    ("OK Corporation Commission Wells", scrape_ok_wells),
    ("USDA NASS Crop Data", scrape_usda_crops),
    ("EIA State Energy Data", scrape_eia_energy),
    ("BLM Mineral Leases", scrape_blm_leases),
]


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="Energy & Environmental Intelligence Scraper")
    parser.add_argument("--db-host", default=DB_HOST)
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to PostgreSQL at {DB_HOST}")
    ensure_tables(conn)

    grand_total = 0
    results = []

    for name, func in ALL_SCRAPERS:
        log(f"\n{'='*60}")
        log(f"*** STARTING: {name} ***")
        log(f"{'='*60}")
        try:
            count = func(conn)
            grand_total += count
            results.append((name, count, "OK"))
            log(f"*** COMPLETE: {name} — {count:,} records ***")
        except Exception as e:
            log(f"*** FAILED: {name} — {e} ***")
            results.append((name, 0, f"FAILED: {e}"))
            conn.rollback()

    conn.close()
    log(f"\n{'='*60}")
    log(f"ENERGY & ENVIRONMENTAL SCRAPER COMPLETE")
    log(f"{'='*60}")
    log(f"Grand total: {grand_total:,} records")
    for name, count, status in results:
        log(f"  {name}: {count:,} ({status})")


if __name__ == "__main__":
    main()
