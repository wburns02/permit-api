#!/usr/bin/env python3
"""
Unique Intelligence Scraper — data NO competitor has.

Sources that differentiate us from ATTOM, Shovels, CoStar:
1. HMDA Mortgage Data (20M+/year from CFPB)
2. NOAA Storm Events (1.5M+ weather→permit correlation)
3. BLS Construction Cost Indices (monthly material prices)
4. DOE Solar Installations (4M+)
5. FHFA House Price Index (ZIP-level price trends)
6. Census Building Permits Survey (official benchmark)

Usage:
    nohup python3 -u scrape_unique_intelligence.py --db-host 100.122.216.15 > /tmp/unique_intel.log 2>&1 &
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


def safe_float(v):
    if v in (None, "", "NA", "N/A", "."):
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def safe_int(v):
    if v in (None, "", "NA", "N/A", "."):
        return None
    try:
        return int(float(str(v).replace(",", "").strip()))
    except (ValueError, TypeError):
        return None


def safe_date(v):
    if not v:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(str(v).strip()[:10], fmt).date()
        except Exception:
            continue
    return None


def ensure_tables(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS hmda_mortgages (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            activity_year INTEGER,
            lei TEXT,
            lender_name TEXT,
            loan_type TEXT,
            loan_purpose TEXT,
            loan_amount FLOAT,
            action_taken TEXT,
            state_code VARCHAR(2),
            county_code TEXT,
            census_tract TEXT,
            income FLOAT,
            property_type TEXT,
            occupancy_type TEXT,
            interest_rate FLOAT,
            loan_term INTEGER,
            race TEXT,
            ethnicity TEXT,
            sex TEXT,
            source TEXT NOT NULL DEFAULT 'cfpb_hmda'
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS noaa_storm_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            event_id TEXT,
            state VARCHAR(2),
            state_fips VARCHAR(2),
            county TEXT,
            county_fips VARCHAR(3),
            event_type TEXT,
            begin_date DATE,
            end_date DATE,
            injuries_direct INTEGER,
            injuries_indirect INTEGER,
            deaths_direct INTEGER,
            deaths_indirect INTEGER,
            damage_property TEXT,
            damage_crops TEXT,
            begin_lat FLOAT,
            begin_lng FLOAT,
            end_lat FLOAT,
            end_lng FLOAT,
            episode_narrative TEXT,
            event_narrative TEXT,
            source TEXT NOT NULL DEFAULT 'noaa_storms'
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bls_construction_costs (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            series_id TEXT,
            series_title TEXT,
            year INTEGER,
            period TEXT,
            value FLOAT,
            footnotes TEXT,
            source TEXT NOT NULL DEFAULT 'bls_ppi'
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS solar_installations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            state VARCHAR(2),
            zip TEXT,
            city TEXT,
            county TEXT,
            system_size_kw FLOAT,
            total_cost FLOAT,
            install_date DATE,
            installer_name TEXT,
            utility TEXT,
            technology TEXT,
            source TEXT NOT NULL DEFAULT 'doe_solar'
        )
    """)

    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_hmda_state ON hmda_mortgages (state_code, county_code)",
        "CREATE INDEX IF NOT EXISTS ix_hmda_tract ON hmda_mortgages (census_tract)",
        "CREATE INDEX IF NOT EXISTS ix_hmda_year ON hmda_mortgages (activity_year)",
        "CREATE INDEX IF NOT EXISTS ix_hmda_purpose ON hmda_mortgages (loan_purpose)",
        "CREATE INDEX IF NOT EXISTS ix_noaa_state ON noaa_storm_events (state, event_type)",
        "CREATE INDEX IF NOT EXISTS ix_noaa_date ON noaa_storm_events (begin_date)",
        "CREATE INDEX IF NOT EXISTS ix_noaa_county ON noaa_storm_events (state_fips, county_fips)",
        "CREATE INDEX IF NOT EXISTS ix_noaa_geo ON noaa_storm_events (begin_lat, begin_lng)",
        "CREATE INDEX IF NOT EXISTS ix_bls_series ON bls_construction_costs (series_id, year)",
        "CREATE INDEX IF NOT EXISTS ix_solar_state ON solar_installations (state, zip)",
    ]:
        try:
            cur.execute(idx)
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()


def get_count(conn, table, source=None):
    cur = conn.cursor()
    if source:
        cur.execute(f"SELECT count(*) FROM {table} WHERE source = %s", (source,))
    else:
        cur.execute(f"SELECT count(*) FROM {table}")
    count = cur.fetchone()[0]
    cur.close()
    return count


# =============================================================================
# 1. HMDA MORTGAGE DATA — 20M+ records from CFPB
# =============================================================================

def scrape_hmda(conn):
    """Download HMDA mortgage data from CFPB Socrata API."""
    log("=== HMDA Mortgage Data (CFPB) ===")

    existing = get_count(conn, "hmda_mortgages")
    if existing > 1000000:
        log(f"  SKIP — already have {existing:,} HMDA records")
        return 0

    # CFPB publishes HMDA on their Socrata instance
    # 2022 data: https://ffiec.cfpb.gov/v2/data-browser-api/view/csv?states=&years=2022
    # Or via Socrata: data snapshots
    # Try the CFPB API
    years = [2023, 2022]
    cur = conn.cursor()
    total = 0

    for year in years:
        log(f"--- HMDA {year} ---")

        # CFPB data browser API with state-by-state download
        states = ["CA", "TX", "FL", "NY", "IL", "PA", "OH", "GA", "NC", "MI",
                  "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MD", "MO", "WI",
                  "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT"]

        for state in states:
            existing_state = 0  # Can't easily check per-state, just load
            url = f"https://ffiec.cfpb.gov/v2/data-browser-api/view/csv?states={state}&years={year}&actions_taken=1,2,3"
            log(f"  Downloading {state} {year}...")

            try:
                resp = httpx.get(url, timeout=300, follow_redirects=True)
                if resp.status_code != 200:
                    log(f"    HTTP {resp.status_code}, skipping")
                    continue

                reader = csv.DictReader(io.StringIO(resp.text))
                batch = []
                state_count = 0

                for row in reader:
                    batch.append((
                        str(uuid.uuid4()),
                        safe_int(row.get("activity_year")),
                        (row.get("lei") or "")[:50] or None,
                        (row.get("derived_msa-md") or row.get("respondent_name") or "")[:200] or None,
                        (row.get("loan_type") or "")[:20] or None,
                        (row.get("loan_purpose") or "")[:20] or None,
                        safe_float(row.get("loan_amount")),
                        (row.get("action_taken") or "")[:10] or None,
                        (row.get("state_code") or state)[:2] or None,
                        (row.get("county_code") or "")[:5] or None,
                        (row.get("census_tract") or "")[:15] or None,
                        safe_float(row.get("income")),
                        (row.get("derived_dwelling_category") or "")[:50] or None,
                        (row.get("occupancy_type") or "")[:10] or None,
                        safe_float(row.get("interest_rate")),
                        safe_int(row.get("loan_term")),
                        (row.get("derived_race") or "")[:100] or None,
                        (row.get("derived_ethnicity") or "")[:100] or None,
                        (row.get("derived_sex") or "")[:50] or None,
                        "cfpb_hmda",
                    ))

                    if len(batch) >= BATCH_SIZE:
                        execute_values(cur, """
                            INSERT INTO hmda_mortgages (id, activity_year, lei, lender_name,
                                loan_type, loan_purpose, loan_amount, action_taken,
                                state_code, county_code, census_tract, income,
                                property_type, occupancy_type, interest_rate, loan_term,
                                race, ethnicity, sex, source)
                            VALUES %s ON CONFLICT DO NOTHING
                        """, batch)
                        conn.commit()
                        state_count += len(batch)
                        total += len(batch)
                        batch = []

                if batch:
                    execute_values(cur, """
                        INSERT INTO hmda_mortgages (id, activity_year, lei, lender_name,
                            loan_type, loan_purpose, loan_amount, action_taken,
                            state_code, county_code, census_tract, income,
                            property_type, occupancy_type, interest_rate, loan_term,
                            race, ethnicity, sex, source)
                        VALUES %s ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                    state_count += len(batch)
                    total += len(batch)

                log(f"    {state} {year}: {state_count:,} records")

            except Exception as e:
                log(f"    {state} {year} error: {e}")
                conn.rollback()

            time.sleep(1)  # Be respectful to CFPB

    cur.close()
    return total


# =============================================================================
# 2. NOAA STORM EVENTS — 1.5M+ weather events
# =============================================================================

def scrape_noaa_storms(conn):
    """Download NOAA Storm Events Database."""
    log("=== NOAA Storm Events ===")

    existing = get_count(conn, "noaa_storm_events")
    if existing > 100000:
        log(f"  SKIP — already have {existing:,} storm records")
        return 0

    cur = conn.cursor()
    total = 0

    # NOAA publishes storm events as gzipped CSVs by year
    # https://www.ncdc.noaa.gov/stormevents/ftp.jsp
    # Bulk CSV: https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/
    years = range(2020, 2026)

    for year in years:
        url = f"https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d{year}_c20250101.csv.gz"
        log(f"  Downloading {year} storm events...")

        try:
            resp = httpx.get(url, timeout=120, follow_redirects=True)
            if resp.status_code != 200:
                # Try alternate filename patterns
                alt_url = f"https://www1.ncdc.noaa.gov/pub/data/swdi/stormevents/csvfiles/StormEvents_details-ftp_v1.0_d{year}.csv.gz"
                resp = httpx.get(alt_url, timeout=120, follow_redirects=True)
                if resp.status_code != 200:
                    log(f"    {year}: HTTP {resp.status_code}, skipping")
                    continue

            content = gzip.decompress(resp.content)
            reader = csv.DictReader(io.StringIO(content.decode("utf-8", errors="replace")))
            batch = []
            year_count = 0

            for row in reader:
                state = (row.get("STATE") or "")[:2]
                event_type = row.get("EVENT_TYPE") or ""

                # Focus on property-damage-relevant events
                relevant_types = {"Hail", "Thunderstorm Wind", "Tornado", "Flash Flood",
                                  "Flood", "Hurricane", "Wildfire", "Winter Storm",
                                  "Ice Storm", "Strong Wind", "High Wind"}
                if event_type not in relevant_types:
                    continue

                batch.append((
                    str(uuid.uuid4()),
                    (row.get("EVENT_ID") or "")[:50] or None,
                    state or None,
                    (row.get("STATE_FIPS") or "")[:2] or None,
                    (row.get("CZ_NAME") or "")[:100] or None,
                    (row.get("CZ_FIPS") or "")[:3] or None,
                    event_type[:50],
                    safe_date(row.get("BEGIN_DATE_TIME", "")[:10]),
                    safe_date(row.get("END_DATE_TIME", "")[:10]),
                    safe_int(row.get("INJURIES_DIRECT")),
                    safe_int(row.get("INJURIES_INDIRECT")),
                    safe_int(row.get("DEATHS_DIRECT")),
                    safe_int(row.get("DEATHS_INDIRECT")),
                    (row.get("DAMAGE_PROPERTY") or "")[:50] or None,
                    (row.get("DAMAGE_CROPS") or "")[:50] or None,
                    safe_float(row.get("BEGIN_LAT")),
                    safe_float(row.get("BEGIN_LON")),
                    safe_float(row.get("END_LAT")),
                    safe_float(row.get("END_LON")),
                    (row.get("EPISODE_NARRATIVE") or "")[:2000] or None,
                    (row.get("EVENT_NARRATIVE") or "")[:2000] or None,
                    "noaa_storms",
                ))

                if len(batch) >= BATCH_SIZE:
                    execute_values(cur, """
                        INSERT INTO noaa_storm_events (id, event_id, state, state_fips,
                            county, county_fips, event_type, begin_date, end_date,
                            injuries_direct, injuries_indirect, deaths_direct, deaths_indirect,
                            damage_property, damage_crops, begin_lat, begin_lng,
                            end_lat, end_lng, episode_narrative, event_narrative, source)
                        VALUES %s ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                    year_count += len(batch)
                    total += len(batch)
                    batch = []

            if batch:
                execute_values(cur, """
                    INSERT INTO noaa_storm_events (id, event_id, state, state_fips,
                        county, county_fips, event_type, begin_date, end_date,
                        injuries_direct, injuries_indirect, deaths_direct, deaths_indirect,
                        damage_property, damage_crops, begin_lat, begin_lng,
                        end_lat, end_lng, episode_narrative, event_narrative, source)
                    VALUES %s ON CONFLICT DO NOTHING
                """, batch)
                conn.commit()
                year_count += len(batch)
                total += len(batch)

            log(f"    {year}: {year_count:,} property-relevant events")

        except Exception as e:
            log(f"    {year} error: {e}")
            conn.rollback()

    cur.close()
    return total


# =============================================================================
# 3. BLS CONSTRUCTION COST INDICES
# =============================================================================

def scrape_bls_costs(conn):
    """Download BLS Producer Price Index for construction materials."""
    log("=== BLS Construction Cost Indices ===")

    existing = get_count(conn, "bls_construction_costs")
    if existing > 1000:
        log(f"  SKIP — already have {existing:,} BLS records")
        return 0

    cur = conn.cursor()
    total = 0

    # Key PPI series for construction
    series = {
        "PCU23----23----": "Construction",
        "PCU236----236----": "Construction of buildings",
        "PCU238----238----": "Specialty trade contractors",
        "WPU081": "Lumber and wood products",
        "WPU101": "Iron and steel",
        "WPU0553": "Concrete products",
        "WPU102502": "Copper wire and cable",
        "WPU0812": "Plywood",
        "WPU072105": "Ready-mixed concrete",
        "WPU1381": "Fabricated structural metal",
    }

    headers = {"Content-Type": "application/json"}

    for series_id, title in series.items():
        log(f"  {title} ({series_id})...")
        try:
            payload = json.dumps({
                "seriesid": [series_id],
                "startyear": "2018",
                "endyear": "2026",
                "registrationkey": None,  # BLS allows 25 queries/day without key
            })
            resp = httpx.post(
                "https://api.bls.gov/publicAPI/v2/timeseries/data/",
                content=payload, headers=headers, timeout=30,
            )
            data = resp.json()

            if data.get("status") != "REQUEST_SUCCEEDED":
                log(f"    BLS API error: {data.get('message', 'unknown')}")
                continue

            for series_data in data.get("Results", {}).get("series", []):
                batch = []
                for d in series_data.get("data", []):
                    batch.append((
                        str(uuid.uuid4()),
                        series_id,
                        title,
                        int(d["year"]),
                        d["period"],
                        safe_float(d["value"]),
                        json.dumps(d.get("footnotes", [])),
                        "bls_ppi",
                    ))

                if batch:
                    execute_values(cur, """
                        INSERT INTO bls_construction_costs (id, series_id, series_title,
                            year, period, value, footnotes, source)
                        VALUES %s ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                    total += len(batch)
                    log(f"    {title}: {len(batch)} data points")

        except Exception as e:
            log(f"    {title} error: {e}")
            conn.rollback()

        time.sleep(2)  # BLS rate limit

    cur.close()
    return total


# =============================================================================
# 4. CENSUS BUILDING PERMITS SURVEY
# =============================================================================

def scrape_census_bps(conn):
    """Census Building Permits Survey — official monthly counts by county."""
    log("=== Census Building Permits Survey ===")

    # Already have this in staging on R730
    staging = "/mnt/data/staging/census_building_permits"
    total = 0
    cur = conn.cursor()

    # Create table if needed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS census_building_permits (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            year INTEGER,
            month INTEGER,
            state_fips VARCHAR(2),
            county_fips VARCHAR(3),
            county_name TEXT,
            permits_1unit INTEGER,
            permits_2units INTEGER,
            permits_3_4units INTEGER,
            permits_5plus INTEGER,
            total_permits INTEGER,
            valuation_1unit FLOAT,
            source TEXT NOT NULL DEFAULT 'census_bps'
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS ix_census_bps_geo ON census_building_permits (state_fips, county_fips)")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_census_bps_year ON census_building_permits (year, month)")
    conn.commit()

    # Try loading from staging files
    import glob
    for filepath in sorted(glob.glob(f"{staging}/co*.txt")):
        log(f"  Loading {os.path.basename(filepath)}...")
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                batch = []
                for row in reader:
                    if len(row) < 10:
                        continue
                    batch.append((
                        str(uuid.uuid4()),
                        safe_int(row[0]) if len(row) > 0 else None,  # year
                        safe_int(row[1]) if len(row) > 1 else None,  # month
                        (row[2] or "")[:2] if len(row) > 2 else None,  # state fips
                        (row[3] or "")[:3] if len(row) > 3 else None,  # county fips
                        (row[4] or "")[:200] if len(row) > 4 else None,  # county name
                        safe_int(row[5]) if len(row) > 5 else None,  # 1 unit
                        safe_int(row[6]) if len(row) > 6 else None,  # 2 units
                        safe_int(row[7]) if len(row) > 7 else None,  # 3-4 units
                        safe_int(row[8]) if len(row) > 8 else None,  # 5+ units
                        safe_int(row[9]) if len(row) > 9 else None,  # total
                        safe_float(row[10]) if len(row) > 10 else None,  # valuation
                        "census_bps",
                    ))
                    if len(batch) >= BATCH_SIZE:
                        execute_values(cur, """
                            INSERT INTO census_building_permits (id, year, month, state_fips,
                                county_fips, county_name, permits_1unit, permits_2units,
                                permits_3_4units, permits_5plus, total_permits,
                                valuation_1unit, source) VALUES %s
                        """, batch)
                        conn.commit()
                        total += len(batch)
                        batch = []

                if batch:
                    execute_values(cur, """
                        INSERT INTO census_building_permits (id, year, month, state_fips,
                            county_fips, county_name, permits_1unit, permits_2units,
                            permits_3_4units, permits_5plus, total_permits,
                            valuation_1unit, source) VALUES %s
                    """, batch)
                    conn.commit()
                    total += len(batch)

        except Exception as e:
            log(f"    Error: {e}")
            conn.rollback()

    cur.close()
    log(f"  Census BPS: {total:,} records loaded")
    return total


# =============================================================================
# MASTER RUNNER
# =============================================================================

ALL_SCRAPERS = [
    ("HMDA Mortgages (20M+)", scrape_hmda),
    ("NOAA Storm Events", scrape_noaa_storms),
    ("BLS Construction Costs", scrape_bls_costs),
    ("Census Building Permits Survey", scrape_census_bps),
]


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="Unique Intelligence Scraper")
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
    log(f"UNIQUE INTELLIGENCE SCRAPER COMPLETE")
    log(f"{'='*60}")
    log(f"Grand total: {grand_total:,} records")
    for name, count, status in results:
        log(f"  {name}: {count:,} ({status})")


if __name__ == "__main__":
    main()
