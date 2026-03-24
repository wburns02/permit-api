#!/usr/bin/env python3
"""
Palantir Gap Scraper -- datasets Palantir uses that we're missing.

Closes the gap to Palantir-level intelligence by pulling:
1. FRED Economic Indicators (10 key series: mortgage rates, unemployment, housing starts, etc.)
2. FAA Airport Facilities (75K worldwide from OurAirports, filtered to US)
3. OFAC Sanctions Entities (~12K from Treasury SDN list)
4. USGS Earthquake Events (M2.5+ since 2020)
5. FBI Crime Data (state-level from UCR/CDE API)
6. FCC Antenna Structure Registration (tower data via Socrata search)

Usage:
    nohup python3 -u scrape_palantir_gap.py --db-host 100.122.216.15 > /tmp/palantir_gap.log 2>&1 &

Cron (weekly Sunday 4 AM):
    0 4 * * 0 python3 -u /home/will/permit-api/scripts/scrape_palantir_gap.py --db-host 100.122.216.15 >> /tmp/palantir_gap_weekly.log 2>&1
"""

import argparse
import csv
import io
import json
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
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"
BATCH_SIZE = 5000

# FRED API key -- DEMO_KEY or register free at https://fred.stlouisfed.org/docs/api/api_key.html
FRED_API_KEY = os.getenv("FRED_API_KEY", "DEMO_KEY")
# FBI CDE API key
FBI_API_KEY = os.getenv("FBI_API_KEY", "iiHnOKfno2Mgkt5AynpvPpUQTEyxE77jo1RU8PIv")


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def sf(v):
    """Safe float."""
    if v in (None, "", "NA", "N/A", ".", "-", "ND"):
        return None
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def si(v):
    """Safe int."""
    if v in (None, "", "NA", "N/A", ".", "-", "ND"):
        return None
    try:
        return int(float(str(v).replace(",", "").strip()))
    except (ValueError, TypeError):
        return None


def sd(v):
    """Safe date."""
    if not v:
        return None
    v_str = str(v).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d", "%m/%d/%y"):
        try:
            return datetime.strptime(v_str[:10], fmt).date()
        except (ValueError, TypeError):
            continue
    # Handle ISO format with time
    try:
        return datetime.fromisoformat(v_str.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        pass
    return None


def s(v, maxlen=500):
    """Safe string."""
    if not v:
        return None
    return str(v).strip()[:maxlen] or None


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
        CREATE TABLE IF NOT EXISTS crime_data (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            state VARCHAR(2), state_name TEXT, county TEXT, city TEXT,
            population INTEGER, year INTEGER,
            violent_crime INTEGER, murder INTEGER, rape INTEGER,
            robbery INTEGER, aggravated_assault INTEGER,
            property_crime INTEGER, burglary INTEGER, larceny INTEGER,
            motor_vehicle_theft INTEGER, arson INTEGER,
            source TEXT NOT NULL)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS economic_indicators (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            series_id TEXT NOT NULL, series_name TEXT,
            date DATE, value FLOAT, unit TEXT,
            frequency TEXT, source TEXT NOT NULL DEFAULT 'fred')
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fcc_towers (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            registration_number TEXT, tower_owner TEXT,
            address TEXT, city TEXT, state VARCHAR(2) NOT NULL, zip TEXT,
            lat FLOAT, lng FLOAT, height_meters FLOAT,
            structure_type TEXT, status TEXT,
            source TEXT NOT NULL DEFAULT 'fcc_asr')
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS faa_facilities (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            facility_id TEXT, facility_name TEXT, facility_type TEXT,
            city TEXT, state VARCHAR(2) NOT NULL, county TEXT,
            lat FLOAT, lng FLOAT, elevation FLOAT,
            ownership TEXT, use_type TEXT,
            source TEXT NOT NULL DEFAULT 'faa')
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sanctions_entities (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            entity_name TEXT NOT NULL, entity_type TEXT,
            address TEXT, city TEXT, state TEXT, country TEXT,
            program TEXT, list_type TEXT,
            source TEXT NOT NULL DEFAULT 'ofac')
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS earthquake_events (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            event_id TEXT, magnitude FLOAT, depth_km FLOAT,
            lat FLOAT, lng FLOAT, place TEXT, state TEXT,
            event_time TIMESTAMPTZ, event_type TEXT,
            source TEXT NOT NULL DEFAULT 'usgs')
    """)

    # Indexes
    indexes = [
        # crime_data
        "CREATE INDEX IF NOT EXISTS ix_crime_state_city ON crime_data (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_crime_year ON crime_data (year)",
        "CREATE INDEX IF NOT EXISTS ix_crime_state_year ON crime_data (state, year)",
        # economic_indicators
        "CREATE INDEX IF NOT EXISTS ix_econ_series ON economic_indicators (series_id, date)",
        "CREATE INDEX IF NOT EXISTS ix_econ_date ON economic_indicators (date)",
        # fcc_towers
        "CREATE INDEX IF NOT EXISTS ix_fcc_state_city ON fcc_towers (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_fcc_geo ON fcc_towers (lat, lng)",
        # faa_facilities
        "CREATE INDEX IF NOT EXISTS ix_faa_state_city ON faa_facilities (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_faa_geo ON faa_facilities (lat, lng)",
        # sanctions_entities
        "CREATE INDEX IF NOT EXISTS ix_sanctions_country ON sanctions_entities (country)",
        "CREATE INDEX IF NOT EXISTS ix_sanctions_name ON sanctions_entities (entity_name)",
        # earthquake_events
        "CREATE INDEX IF NOT EXISTS ix_eq_geo ON earthquake_events (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_eq_state ON earthquake_events (state)",
        "CREATE INDEX IF NOT EXISTS ix_eq_time ON earthquake_events (event_time)",
        "CREATE INDEX IF NOT EXISTS ix_eq_magnitude ON earthquake_events (magnitude)",
    ]
    for idx_sql in indexes:
        try:
            cur.execute(idx_sql)
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()
    log("Tables and indexes ensured.")


# =============================================================================
# 1. FRED Economic Indicators
# =============================================================================

FRED_SERIES = {
    "MORTGAGE30US": ("30-Year Fixed Mortgage Rate", "%", "Weekly"),
    "UNRATE": ("Unemployment Rate", "%", "Monthly"),
    "HOUST": ("Housing Starts", "Thousands of Units", "Monthly"),
    "PERMIT": ("Building Permits (National)", "Thousands of Units", "Monthly"),
    "CSUSHPINSA": ("Case-Shiller Home Price Index", "Index", "Monthly"),
    "WPUSI012011": ("PPI Construction Materials", "Index", "Monthly"),
    "GDP": ("Gross Domestic Product", "Billions $", "Quarterly"),
    "CPIAUCSL": ("Consumer Price Index", "Index", "Monthly"),
    "FEDFUNDS": ("Federal Funds Rate", "%", "Monthly"),
    "RSAFS": ("Retail Sales", "Millions $", "Monthly"),
}


def scrape_fred(conn):
    log("=== FRED Economic Indicators (10 series) ===")
    existing = get_count(conn, "economic_indicators", "fred")
    if existing > 5000:
        log(f"  SKIP -- already {existing:,} records")
        return 0

    cur = conn.cursor()
    total = 0

    for series_id, (name, unit, freq) in FRED_SERIES.items():
        log(f"  Fetching {series_id} ({name})...")
        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}"
            f"&api_key={FRED_API_KEY}"
            f"&file_type=json"
            f"&observation_start=2015-01-01"
        )
        try:
            resp = httpx.get(url, timeout=30, follow_redirects=True)
            if resp.status_code != 200:
                log(f"    {series_id}: HTTP {resp.status_code} -- needs API key registration, skipping")
                continue
            data = resp.json()
            observations = data.get("observations", [])
            if not observations:
                log(f"    {series_id}: no observations returned")
                continue

            batch = []
            for obs in observations:
                val = sf(obs.get("value"))
                if val is None:
                    continue
                batch.append((
                    str(uuid.uuid4()),
                    series_id,
                    name,
                    sd(obs.get("date")),
                    val,
                    unit,
                    freq,
                    "fred",
                ))
            if batch:
                for i in range(0, len(batch), BATCH_SIZE):
                    chunk = batch[i:i + BATCH_SIZE]
                    execute_values(cur, """
                        INSERT INTO economic_indicators
                        (id, series_id, series_name, date, value, unit, frequency, source)
                        VALUES %s ON CONFLICT DO NOTHING
                    """, chunk)
                    conn.commit()
                total += len(batch)
                log(f"    {series_id}: {len(batch):,} observations loaded")
        except Exception as e:
            log(f"    {series_id} error: {e}")
            conn.rollback()
        time.sleep(0.5)

    cur.close()
    return total


# =============================================================================
# 2. FAA Airport Facilities (OurAirports CSV -- 75K worldwide, filter US)
# =============================================================================

# Map ISO region codes (US-XX) to state abbreviations
def extract_us_state(iso_region):
    """Extract 2-letter state from ISO region like 'US-TX'."""
    if not iso_region or not iso_region.startswith("US-"):
        return None
    return iso_region[3:5]


def scrape_faa_airports(conn):
    log("=== FAA Airport Facilities (OurAirports) ===")
    existing = get_count(conn, "faa_facilities", "faa")
    if existing > 5000:
        log(f"  SKIP -- already {existing:,} records")
        return 0

    url = "https://ourairports.com/data/airports.csv"
    log(f"  Downloading {url}...")
    try:
        resp = httpx.get(url, timeout=120, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        log(f"  Download failed: {e}")
        return 0

    text = resp.text
    reader = csv.DictReader(io.StringIO(text))
    cur = conn.cursor()
    total = 0
    batch = []

    for row in reader:
        # Filter to US only
        if row.get("iso_country") != "US":
            continue
        state = extract_us_state(row.get("iso_region"))
        if not state:
            continue

        # Map facility type
        ftype = row.get("type", "")
        # Determine ownership/use from scheduling info
        scheduled = row.get("scheduled_service", "")

        batch.append((
            str(uuid.uuid4()),
            s(row.get("ident"), 20),
            s(row.get("name"), 200),
            s(ftype, 50),
            s(row.get("municipality"), 100),
            state,
            None,  # county not in this dataset
            sf(row.get("latitude_deg")),
            sf(row.get("longitude_deg")),
            sf(row.get("elevation_ft")),
            None,  # ownership
            "public" if scheduled == "yes" else "private",
            "faa",
        ))

        if len(batch) >= BATCH_SIZE:
            execute_values(cur, """
                INSERT INTO faa_facilities
                (id, facility_id, facility_name, facility_type, city, state, county,
                 lat, lng, elevation, ownership, use_type, source)
                VALUES %s ON CONFLICT DO NOTHING
            """, batch)
            conn.commit()
            total += len(batch)
            log(f"    {total:,} loaded...")
            batch = []

    if batch:
        execute_values(cur, """
            INSERT INTO faa_facilities
            (id, facility_id, facility_name, facility_type, city, state, county,
             lat, lng, elevation, ownership, use_type, source)
            VALUES %s ON CONFLICT DO NOTHING
        """, batch)
        conn.commit()
        total += len(batch)

    cur.close()
    log(f"  FAA airports: {total:,} US facilities loaded")
    return total


# =============================================================================
# 3. OFAC Sanctions List (Treasury SDN CSV)
# =============================================================================

def scrape_ofac_sanctions(conn):
    log("=== OFAC Sanctions Entities (Treasury SDN) ===")
    existing = get_count(conn, "sanctions_entities", "ofac")
    if existing > 5000:
        log(f"  SKIP -- already {existing:,} records")
        return 0

    # The SDN CSV is pipe-delimited, not comma-delimited
    # Format: ENT_NUM | SDN_NAME | SDN_TYPE | PROGRAM | TITLE | CALL_SIGN |
    #         VESSEL_TYPE | TONNAGE | GRT | VESSEL_FLAG | VESSEL_OWNER | REMARKS
    url = "https://www.treasury.gov/ofac/downloads/sdn.csv"
    log(f"  Downloading {url}...")
    try:
        resp = httpx.get(url, timeout=120, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        log(f"  Download failed: {e}")
        return 0

    cur = conn.cursor()
    total = 0
    batch = []

    # SDN CSV uses comma delimiter but fields may be quoted
    # Each row: ENT_NUM,SDN_Name,SDN_Type,Program,Title,Call_Sign,...
    lines = resp.text.split("\n")
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # Parse with csv module for proper quote handling
        try:
            parts = list(csv.reader([line]))[0]
        except Exception:
            continue

        if len(parts) < 4:
            continue

        ent_num = s(parts[0], 50)
        name = s(parts[1], 500)
        sdn_type = s(parts[2], 50)
        program = s(parts[3], 200)

        if not name:
            continue

        # Map SDN type to entity type
        entity_type = None
        if sdn_type:
            if "individual" in sdn_type.lower():
                entity_type = "Individual"
            elif "entity" in sdn_type.lower():
                entity_type = "Entity"
            elif "vessel" in sdn_type.lower():
                entity_type = "Vessel"
            elif "aircraft" in sdn_type.lower():
                entity_type = "Aircraft"
            else:
                entity_type = sdn_type

        # Parse remarks for address/country if available
        remarks = parts[11] if len(parts) > 11 else ""
        country = None
        if remarks:
            # Look for country info in remarks
            for token in remarks.split(";"):
                token = token.strip()
                if token.startswith("Nationality") or token.startswith("Country"):
                    country = s(token.split(":", 1)[-1] if ":" in token else token.split(" ", 1)[-1], 100)

        batch.append((
            str(uuid.uuid4()),
            name,
            entity_type,
            None,  # address -- in separate OFAC file (add.csv)
            None,  # city
            None,  # state
            country,
            program,
            "SDN",
            "ofac",
        ))

        if len(batch) >= BATCH_SIZE:
            execute_values(cur, """
                INSERT INTO sanctions_entities
                (id, entity_name, entity_type, address, city, state, country,
                 program, list_type, source)
                VALUES %s ON CONFLICT DO NOTHING
            """, batch)
            conn.commit()
            total += len(batch)
            log(f"    {total:,} loaded...")
            batch = []

    if batch:
        execute_values(cur, """
            INSERT INTO sanctions_entities
            (id, entity_name, entity_type, address, city, state, country,
             program, list_type, source)
            VALUES %s ON CONFLICT DO NOTHING
        """, batch)
        conn.commit()
        total += len(batch)

    # Now try to load the address file for enrichment
    log("  Fetching OFAC address supplement (add.csv)...")
    try:
        addr_resp = httpx.get("https://www.treasury.gov/ofac/downloads/add.csv",
                              timeout=120, follow_redirects=True)
        if addr_resp.status_code == 200:
            addr_lines = addr_resp.text.split("\n")
            addr_count = 0
            for line in addr_lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    parts = list(csv.reader([line]))[0]
                except Exception:
                    continue
                # add.csv: ENT_NUM, ADD_NUM, ADDRESS, CITY, STATE, COUNTRY, ...
                if len(parts) >= 6:
                    ent_name = None  # We'd need to cross-reference, skip for now
                    addr_count += 1
            log(f"    OFAC address file: {addr_count:,} address records available (cross-ref not implemented)")
    except Exception as e:
        log(f"    Address file: {e}")

    cur.close()
    log(f"  OFAC sanctions: {total:,} entities loaded")
    return total


# =============================================================================
# 4. USGS Earthquake Events (M2.5+ since 2020)
# =============================================================================

US_STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "puerto rico": "PR", "us virgin islands": "VI", "guam": "GU",
}


def extract_state_from_place(place):
    """Try to extract US state abbreviation from USGS place string like '5km NW of Ridgecrest, CA'."""
    if not place:
        return None
    # Check for 2-letter state at end: "Place, CA"
    parts = place.rsplit(",", 1)
    if len(parts) == 2:
        candidate = parts[1].strip().upper()
        if len(candidate) == 2 and candidate in US_STATE_NAMES.values():
            return candidate
    # Check for full state name
    lower = place.lower()
    for name, abbr in US_STATE_NAMES.items():
        if name in lower:
            return abbr
    return None


def scrape_usgs_earthquakes(conn):
    log("=== USGS Earthquake Events (M2.5+ since 2020) ===")
    existing = get_count(conn, "earthquake_events", "usgs")
    if existing > 10000:
        log(f"  SKIP -- already {existing:,} records")
        return 0

    cur = conn.cursor()
    total = 0

    # USGS limits to 20000 per request, so we chunk by year
    years = [
        ("2020-01-01", "2020-12-31"),
        ("2021-01-01", "2021-12-31"),
        ("2022-01-01", "2022-12-31"),
        ("2023-01-01", "2023-12-31"),
        ("2024-01-01", "2024-12-31"),
        ("2025-01-01", "2025-12-31"),
        ("2026-01-01", "2026-12-31"),
    ]

    for start, end in years:
        log(f"  Fetching earthquakes {start} to {end}...")
        url = (
            f"https://earthquake.usgs.gov/fdsnws/event/1/query"
            f"?format=geojson"
            f"&starttime={start}"
            f"&endtime={end}"
            f"&minmagnitude=2.5"
            f"&limit=20000"
        )
        try:
            resp = httpx.get(url, timeout=120, follow_redirects=True)
            if resp.status_code != 200:
                log(f"    HTTP {resp.status_code}")
                continue
            data = resp.json()
            features = data.get("features", [])
            if not features:
                log(f"    No features for {start[:4]}")
                continue

            batch = []
            for feat in features:
                props = feat.get("properties", {})
                geom = feat.get("geometry", {})
                coords = geom.get("coordinates", [None, None, None])

                lng = coords[0] if len(coords) > 0 else None
                lat = coords[1] if len(coords) > 1 else None
                depth = coords[2] if len(coords) > 2 else None

                place = props.get("place")
                state = extract_state_from_place(place)

                # Convert epoch ms to timestamptz
                event_time = None
                ts = props.get("time")
                if ts:
                    try:
                        event_time = datetime.utcfromtimestamp(ts / 1000.0)
                    except (ValueError, TypeError, OSError):
                        pass

                batch.append((
                    str(uuid.uuid4()),
                    s(feat.get("id"), 50),
                    sf(props.get("mag")),
                    sf(depth),
                    sf(lat),
                    sf(lng),
                    s(place, 500),
                    state,
                    event_time,
                    s(props.get("type"), 50),
                    "usgs",
                ))

            if batch:
                for i in range(0, len(batch), BATCH_SIZE):
                    chunk = batch[i:i + BATCH_SIZE]
                    execute_values(cur, """
                        INSERT INTO earthquake_events
                        (id, event_id, magnitude, depth_km, lat, lng, place, state,
                         event_time, event_type, source)
                        VALUES %s ON CONFLICT DO NOTHING
                    """, chunk)
                    conn.commit()
                total += len(batch)
                log(f"    {start[:4]}: {len(batch):,} events")
        except Exception as e:
            log(f"    {start[:4]} error: {e}")
            conn.rollback()
        time.sleep(1)

    cur.close()
    return total


# =============================================================================
# 5. FBI Crime Data (state-level via CDE API)
# =============================================================================

US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]

STATE_NAMES_BY_ABBR = {v: k.title() for k, v in US_STATE_NAMES.items()}


def scrape_fbi_crime(conn):
    log("=== FBI Crime Data (state-level CDE API) ===")
    existing = get_count(conn, "crime_data", "fbi_cde")
    if existing > 100:
        log(f"  SKIP -- already {existing:,} records")
        return 0

    cur = conn.cursor()
    total = 0
    headers = {"Accept": "application/json"}

    for state in US_STATES:
        log(f"  Fetching crime data for {state}...")
        url = (
            f"https://api.usa.gov/crime/fbi/cde/arrest/state/{state}/all"
            f"?from=2019&to=2022"
            f"&API_KEY={FBI_API_KEY}"
        )
        try:
            resp = httpx.get(url, timeout=30, follow_redirects=True, headers=headers)
            if resp.status_code != 200:
                log(f"    {state}: HTTP {resp.status_code} -- skipping")
                continue

            data = resp.json()
            if not data or not isinstance(data, dict):
                log(f"    {state}: unexpected response format")
                continue

            # The CDE API returns arrest data keyed by offense type
            # We need to aggregate into yearly totals
            # Structure varies -- try to extract what we can

            # Try parsing as arrest data -- the CDE returns data keyed by keys
            # like "Aggravated Assault", "Burglary", etc., each containing yearly arrays
            state_name = STATE_NAMES_BY_ABBR.get(state, state)
            yearly = {}  # year -> {field: value}

            # The CDE arrest endpoint returns {data: [{key, data: [{x: year, y: count}]}]}
            data_entries = data.get("data", data)
            if isinstance(data_entries, list):
                for entry in data_entries:
                    key = entry.get("key", "")
                    points = entry.get("data", [])
                    if not isinstance(points, list):
                        continue
                    for pt in points:
                        year = si(pt.get("x"))
                        val = si(pt.get("y"))
                        if not year:
                            continue
                        if year not in yearly:
                            yearly[year] = {}
                        # Map FBI offense keys to our columns
                        kl = key.lower()
                        if "murder" in kl or "manslaughter" in kl:
                            yearly[year]["murder"] = (yearly[year].get("murder") or 0) + (val or 0)
                        elif "rape" in kl:
                            yearly[year]["rape"] = (yearly[year].get("rape") or 0) + (val or 0)
                        elif "robbery" in kl:
                            yearly[year]["robbery"] = (yearly[year].get("robbery") or 0) + (val or 0)
                        elif "aggravated" in kl and "assault" in kl:
                            yearly[year]["aggravated_assault"] = (yearly[year].get("aggravated_assault") or 0) + (val or 0)
                        elif "burglary" in kl or "breaking" in kl:
                            yearly[year]["burglary"] = (yearly[year].get("burglary") or 0) + (val or 0)
                        elif "larceny" in kl or "theft" in kl and "motor" not in kl:
                            yearly[year]["larceny"] = (yearly[year].get("larceny") or 0) + (val or 0)
                        elif "motor" in kl and ("theft" in kl or "vehicle" in kl):
                            yearly[year]["motor_vehicle_theft"] = (yearly[year].get("motor_vehicle_theft") or 0) + (val or 0)
                        elif "arson" in kl:
                            yearly[year]["arson"] = (yearly[year].get("arson") or 0) + (val or 0)

            batch = []
            for year, fields in yearly.items():
                violent = (
                    (fields.get("murder") or 0)
                    + (fields.get("rape") or 0)
                    + (fields.get("robbery") or 0)
                    + (fields.get("aggravated_assault") or 0)
                )
                prop_crime = (
                    (fields.get("burglary") or 0)
                    + (fields.get("larceny") or 0)
                    + (fields.get("motor_vehicle_theft") or 0)
                )
                batch.append((
                    str(uuid.uuid4()),
                    state,
                    state_name,
                    None,  # county
                    None,  # city
                    None,  # population
                    year,
                    violent or None,
                    fields.get("murder"),
                    fields.get("rape"),
                    fields.get("robbery"),
                    fields.get("aggravated_assault"),
                    prop_crime or None,
                    fields.get("burglary"),
                    fields.get("larceny"),
                    fields.get("motor_vehicle_theft"),
                    fields.get("arson"),
                    "fbi_cde",
                ))

            if batch:
                execute_values(cur, """
                    INSERT INTO crime_data
                    (id, state, state_name, county, city, population, year,
                     violent_crime, murder, rape, robbery, aggravated_assault,
                     property_crime, burglary, larceny, motor_vehicle_theft,
                     arson, source)
                    VALUES %s ON CONFLICT DO NOTHING
                """, batch)
                conn.commit()
                total += len(batch)
                log(f"    {state}: {len(batch)} year-records")

        except Exception as e:
            log(f"    {state} error: {e}")
            conn.rollback()
        time.sleep(0.3)

    cur.close()
    return total


# =============================================================================
# 6. FCC Antenna/Tower Data (via Socrata search, then direct scrape)
# =============================================================================

def scrape_fcc_towers(conn):
    log("=== FCC Antenna Structure Registration ===")
    existing = get_count(conn, "fcc_towers", "fcc_asr")
    if existing > 5000:
        log(f"  SKIP -- already {existing:,} records")
        return 0

    cur = conn.cursor()
    total = 0

    # Strategy 1: Search Socrata catalog for FCC tower/antenna datasets
    log("  Searching Socrata catalog for FCC tower data...")
    socrata_datasets = []
    try:
        resp = httpx.get(
            "https://api.us.socrata.com/api/catalog/v1",
            params={"q": "FCC antenna tower registration", "limit": 10},
            timeout=30,
        )
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            for r in results:
                resource = r.get("resource", {})
                name = resource.get("name", "")
                domain = r.get("metadata", {}).get("domain", "")
                rid = resource.get("id", "")
                log(f"    Found: {name} ({domain}/{rid})")
                socrata_datasets.append((domain, rid, name))
    except Exception as e:
        log(f"    Socrata search error: {e}")

    # Strategy 2: Try known FCC datasets on Socrata
    known_fcc_socrata = [
        # FCC ASR data is sometimes mirrored on data.gov
        ("data.fcc.gov", None, "FCC Direct"),
    ]

    # Strategy 3: Try the FCC's direct bulk data API
    log("  Trying FCC direct data endpoints...")
    fcc_urls = [
        "https://opendata.fcc.gov/resource/i5zd-bafz.json",  # Common FCC Socrata endpoint
    ]

    for fcc_url in fcc_urls:
        log(f"  Trying {fcc_url}...")
        try:
            resp = httpx.get(f"{fcc_url}?$limit=1", timeout=30, follow_redirects=True)
            if resp.status_code == 200:
                sample = resp.json()
                log(f"    Response sample keys: {list(sample[0].keys()) if sample else 'empty'}")

                # If it works, paginate through
                offset = 0
                while True:
                    resp = httpx.get(
                        f"{fcc_url}?$limit=50000&$offset={offset}&$order=:id",
                        timeout=120, follow_redirects=True,
                    )
                    if resp.status_code != 200:
                        break
                    records = resp.json()
                    if not records:
                        break

                    batch = []
                    for r in records:
                        state = s(r.get("state") or r.get("state_code"), 2)
                        if not state:
                            continue
                        batch.append((
                            str(uuid.uuid4()),
                            s(r.get("registration_number") or r.get("reg_num") or r.get("asr_num"), 50),
                            s(r.get("tower_owner") or r.get("owner_name") or r.get("entity_name"), 200),
                            s(r.get("address") or r.get("street_address"), 500),
                            s(r.get("city"), 100),
                            state,
                            s(r.get("zip") or r.get("zip_code"), 10),
                            sf(r.get("latitude") or r.get("lat")),
                            sf(r.get("longitude") or r.get("lng") or r.get("lon")),
                            sf(r.get("overall_height_above_ground") or r.get("height_meters") or r.get("struc_height")),
                            s(r.get("structure_type"), 100),
                            s(r.get("status_code") or r.get("status"), 50),
                            "fcc_asr",
                        ))

                    if batch:
                        execute_values(cur, """
                            INSERT INTO fcc_towers
                            (id, registration_number, tower_owner, address, city, state, zip,
                             lat, lng, height_meters, structure_type, status, source)
                            VALUES %s ON CONFLICT DO NOTHING
                        """, batch)
                        conn.commit()
                        total += len(batch)
                        log(f"    {total:,} loaded...")

                    offset += 50000
                    if len(records) < 50000:
                        break
                    time.sleep(0.3)

                if total > 0:
                    break
            else:
                log(f"    HTTP {resp.status_code}")
        except Exception as e:
            log(f"    Error: {e}")

    # Strategy 4: If Socrata yielded datasets, try those
    if total == 0 and socrata_datasets:
        for domain, rid, name in socrata_datasets[:3]:
            if not rid:
                continue
            socrata_url = f"https://{domain}/resource/{rid}.json"
            log(f"  Trying Socrata dataset: {name} ({socrata_url})...")
            try:
                resp = httpx.get(f"{socrata_url}?$limit=1", timeout=30, follow_redirects=True)
                if resp.status_code == 200:
                    sample = resp.json()
                    if sample:
                        keys = list(sample[0].keys())
                        log(f"    Keys: {keys[:15]}")
                        # Try to map fields generically
                        state_key = next((k for k in keys if "state" in k.lower()), None)
                        if not state_key:
                            log(f"    No state field found, skipping")
                            continue

                        offset = 0
                        while True:
                            resp = httpx.get(
                                f"{socrata_url}?$limit=50000&$offset={offset}",
                                timeout=120, follow_redirects=True,
                            )
                            if resp.status_code != 200:
                                break
                            records = resp.json()
                            if not records:
                                break

                            batch = []
                            for r in records:
                                state = s(r.get(state_key), 2)
                                if not state or len(state) != 2:
                                    continue
                                lat_key = next((k for k in keys if "lat" in k.lower()), None)
                                lng_key = next((k for k in keys if "lon" in k.lower() or "lng" in k.lower()), None)
                                batch.append((
                                    str(uuid.uuid4()),
                                    s(r.get("registration_number") or r.get(next((k for k in keys if "reg" in k.lower()), ""), ""), 50),
                                    s(r.get("owner") or r.get(next((k for k in keys if "owner" in k.lower()), ""), ""), 200),
                                    s(r.get("address") or r.get(next((k for k in keys if "addr" in k.lower()), ""), ""), 500),
                                    s(r.get("city") or r.get(next((k for k in keys if "city" in k.lower()), ""), ""), 100),
                                    state,
                                    s(r.get("zip") or r.get(next((k for k in keys if "zip" in k.lower()), ""), ""), 10),
                                    sf(r.get(lat_key)) if lat_key else None,
                                    sf(r.get(lng_key)) if lng_key else None,
                                    sf(r.get(next((k for k in keys if "height" in k.lower()), ""))),
                                    s(r.get(next((k for k in keys if "struct" in k.lower() or "type" in k.lower()), "")), 100),
                                    s(r.get(next((k for k in keys if "status" in k.lower()), "")), 50),
                                    "fcc_asr",
                                ))

                            if batch:
                                execute_values(cur, """
                                    INSERT INTO fcc_towers
                                    (id, registration_number, tower_owner, address, city, state, zip,
                                     lat, lng, height_meters, structure_type, status, source)
                                    VALUES %s ON CONFLICT DO NOTHING
                                """, batch)
                                conn.commit()
                                total += len(batch)
                                log(f"    {total:,} loaded...")

                            offset += 50000
                            if len(records) < 50000:
                                break
                            time.sleep(0.3)

                        if total > 0:
                            break
            except Exception as e:
                log(f"    Error: {e}")

    if total == 0:
        log("  FCC tower data: no working endpoint found -- needs manual bulk download from FCC ASR")
        log("  See: https://www.fcc.gov/wireless/bureau-divisions/mobility-division/antenna-structure-registration")

    cur.close()
    return total


# =============================================================================
# MASTER RUNNER
# =============================================================================

ALL = [
    ("FRED Economic Indicators (10 series)", scrape_fred),
    ("FAA Airport Facilities (US)", scrape_faa_airports),
    ("OFAC Sanctions Entities (SDN)", scrape_ofac_sanctions),
    ("USGS Earthquake Events (M2.5+)", scrape_usgs_earthquakes),
    ("FBI Crime Data (state-level)", scrape_fbi_crime),
    ("FCC Antenna Towers (ASR)", scrape_fcc_towers),
]


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="Palantir Gap Scraper")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    parser.add_argument("--only", help="Run only specific source (fred,faa,ofac,usgs,fbi,fcc)")
    args = parser.parse_args()
    DB_HOST = args.db_host

    conn = get_conn()
    log(f"Connected to PostgreSQL at {DB_HOST}")
    ensure_tables(conn)

    # Filter sources if --only specified
    source_map = {
        "fred": 0, "faa": 1, "ofac": 2, "usgs": 3, "fbi": 4, "fcc": 5,
    }
    if args.only:
        selected = [s.strip().lower() for s in args.only.split(",")]
        run_list = [(name, func) for i, (name, func) in enumerate(ALL)
                    if any(source_map.get(sel) == i for sel in selected)]
    else:
        run_list = ALL

    grand = 0
    results = []
    for name, func in run_list:
        log(f"\n{'=' * 60}\n*** {name} ***\n{'=' * 60}")
        try:
            c = func(conn)
            grand += c
            results.append((name, c, "OK"))
            log(f"*** DONE: {name} -- {c:,} ***")
        except Exception as e:
            log(f"*** FAIL: {name} -- {e} ***")
            results.append((name, 0, f"FAIL: {e}"))
            conn.rollback()

    conn.close()
    log(f"\n{'=' * 60}")
    log(f"COMPLETE -- {grand:,} total records loaded")
    log(f"{'=' * 60}")
    for n, c, st in results:
        log(f"  {n}: {c:,} ({st})")


if __name__ == "__main__":
    main()
