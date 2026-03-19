#!/usr/bin/env python3
"""
Load Tier 1 data layers into T430 PostgreSQL.

Usage (run on R730 or any machine with access to staging data + T430 DB):
    python load_data_layers.py --layer all
    python load_data_layers.py --layer contractor_licenses
    python load_data_layers.py --layer epa_facilities
    python load_data_layers.py --layer fema_flood_zones
    python load_data_layers.py --layer census_demographics
    python load_data_layers.py --layer septic_systems
    python load_data_layers.py --layer property_valuations

Requires: pip install psycopg2-binary pandas
"""

import argparse
import csv
import gzip
import io
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

# Try psycopg2 for direct PostgreSQL access (sync, for bulk loading)
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

# Database connection — T430 PostgreSQL
DB_HOST = os.getenv("DB_HOST", "100.122.216.15")  # T430 Tailscale IP
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "permits")
DB_USER = os.getenv("DB_USER", "will")

# Staging data root — adjust for the machine you're running on
STAGING = Path(os.getenv("STAGING_DIR", "/mnt/data/staging"))

BATCH_SIZE = 5000


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER
    )


def safe_float(val):
    if val in (None, "", "NA", "-9999.0", "-9999"):
        return None
    try:
        f = float(val)
        return f if f != -9999.0 else None
    except (ValueError, TypeError):
        return None


def safe_int(val):
    if val in (None, "", "NA"):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def safe_date(val, fmt=None):
    if not val or val in ("NA", ""):
        return None
    try:
        if fmt:
            return datetime.strptime(val, fmt).date()
        # Try common formats
        for f in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%Y%m%d"):
            try:
                return datetime.strptime(val.strip(), f).date()
            except ValueError:
                continue
        # Try epoch milliseconds
        if val.isdigit() and len(val) > 10:
            return datetime.fromtimestamp(int(val) / 1000).date()
    except Exception:
        return None
    return None


def load_contractor_licenses(conn):
    """Load CA CSLB + FL DBPR contractor licenses."""
    cur = conn.cursor()

    # Ensure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contractor_licenses (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            license_number VARCHAR(100) NOT NULL,
            business_name VARCHAR(500) NOT NULL,
            full_business_name VARCHAR(500),
            address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(2) NOT NULL,
            zip VARCHAR(10),
            county VARCHAR(100),
            phone VARCHAR(20),
            business_type VARCHAR(50),
            issue_date DATE,
            expiration_date DATE,
            status VARCHAR(50),
            secondary_status VARCHAR(100),
            classifications TEXT,
            workers_comp_type VARCHAR(100),
            workers_comp_company VARCHAR(255),
            surety_company VARCHAR(255),
            surety_amount FLOAT,
            source VARCHAR(50) NOT NULL,
            last_updated DATE
        )
    """)
    conn.commit()

    total = 0

    # California CSLB
    ca_dir = STAGING / "contractor_licenses" / "california_cslb"
    if ca_dir.exists():
        for csv_file in ca_dir.glob("*.csv"):
            print(f"  Loading {csv_file.name}...")
            batch = []
            with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    batch.append((
                        str(uuid.uuid4()),
                        (row.get("LicenseNo") or "")[:100],
                        (row.get("BusinessName") or "Unknown")[:500],
                        (row.get("FullBusinessName") or "")[:500] or None,
                        (row.get("MailingAddress") or "")[:500] or None,
                        (row.get("City") or "")[:100] or None,
                        "CA",
                        (row.get("ZIPCode") or "")[:10] or None,
                        (row.get("County") or "")[:100] or None,
                        (row.get("BusinessPhone") or "")[:20] or None,
                        (row.get("BusinessType") or "")[:50] or None,
                        safe_date(row.get("IssueDate")),
                        safe_date(row.get("ExpirationDate")),
                        (row.get("PrimaryStatus") or "")[:50] or None,
                        (row.get("SecondaryStatus") or "")[:100] or None,
                        row.get("Classifications(s)") or row.get("Classifications") or None,
                        (row.get("WorkersCompCoverageType") or "")[:100] or None,
                        (row.get("WCInsuranceCompany") or "")[:255] or None,
                        (row.get("CBSuretyCompany") or "")[:255] or None,
                        safe_float(row.get("CBAmount")),
                        "california_cslb",
                        safe_date(row.get("LastUpdate")),
                    ))
                    if len(batch) >= BATCH_SIZE:
                        execute_values(cur, """
                            INSERT INTO contractor_licenses (id, license_number, business_name,
                                full_business_name, address, city, state, zip, county, phone,
                                business_type, issue_date, expiration_date, status, secondary_status,
                                classifications, workers_comp_type, workers_comp_company,
                                surety_company, surety_amount, source, last_updated)
                            VALUES %s ON CONFLICT DO NOTHING
                        """, batch)
                        conn.commit()
                        total += len(batch)
                        batch = []

            if batch:
                execute_values(cur, """
                    INSERT INTO contractor_licenses (id, license_number, business_name,
                        full_business_name, address, city, state, zip, county, phone,
                        business_type, issue_date, expiration_date, status, secondary_status,
                        classifications, workers_comp_type, workers_comp_company,
                        surety_company, surety_amount, source, last_updated)
                    VALUES %s ON CONFLICT DO NOTHING
                """, batch)
                conn.commit()
                total += len(batch)

    # Florida DBPR
    fl_dir = STAGING / "contractor_licenses" / "florida_dbpr"
    if fl_dir.exists():
        for csv_file in fl_dir.glob("*.csv"):
            print(f"  Loading {csv_file.name}...")
            batch = []
            with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    license_no = row.get("LicenseNumber") or row.get("LICENSE_NUMBER") or ""
                    name = row.get("BusinessName") or row.get("BUSINESS_NAME") or row.get("Name") or "Unknown"
                    batch.append((
                        str(uuid.uuid4()),
                        license_no[:100],
                        name[:500],
                        None,
                        (row.get("Address") or row.get("ADDRESS") or "")[:500] or None,
                        (row.get("City") or row.get("CITY") or "")[:100] or None,
                        "FL",
                        (row.get("ZipCode") or row.get("ZIP") or "")[:10] or None,
                        (row.get("County") or row.get("COUNTY") or "")[:100] or None,
                        None,
                        (row.get("LicenseType") or row.get("LICENSE_TYPE") or "")[:50] or None,
                        safe_date(row.get("OriginalDate") or row.get("ORIGINAL_DATE")),
                        safe_date(row.get("ExpirationDate") or row.get("EXPIRATION_DATE")),
                        (row.get("Status") or row.get("STATUS") or "")[:50] or None,
                        None,
                        row.get("Classification") or row.get("CLASSIFICATION") or None,
                        None, None, None, None,
                        "florida_dbpr",
                        None,
                    ))
                    if len(batch) >= BATCH_SIZE:
                        execute_values(cur, """
                            INSERT INTO contractor_licenses (id, license_number, business_name,
                                full_business_name, address, city, state, zip, county, phone,
                                business_type, issue_date, expiration_date, status, secondary_status,
                                classifications, workers_comp_type, workers_comp_company,
                                surety_company, surety_amount, source, last_updated)
                            VALUES %s ON CONFLICT DO NOTHING
                        """, batch)
                        conn.commit()
                        total += len(batch)
                        batch = []

            if batch:
                execute_values(cur, """
                    INSERT INTO contractor_licenses (id, license_number, business_name,
                        full_business_name, address, city, state, zip, county, phone,
                        business_type, issue_date, expiration_date, status, secondary_status,
                        classifications, workers_comp_type, workers_comp_company,
                        surety_company, surety_amount, source, last_updated)
                    VALUES %s ON CONFLICT DO NOTHING
                """, batch)
                conn.commit()
                total += len(batch)

    # Create indexes
    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_cl_license ON contractor_licenses (license_number)",
        "CREATE INDEX IF NOT EXISTS ix_cl_name ON contractor_licenses (business_name)",
        "CREATE INDEX IF NOT EXISTS ix_cl_state ON contractor_licenses (state)",
        "CREATE INDEX IF NOT EXISTS ix_cl_state_status ON contractor_licenses (state, status)",
    ]:
        cur.execute(idx)
    conn.commit()
    cur.close()
    print(f"  Contractor licenses: {total:,} records loaded")
    return total


def load_epa_facilities(conn):
    """Load EPA FRS facilities."""
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS epa_facilities (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            registry_id VARCHAR(50) NOT NULL UNIQUE,
            name VARCHAR(500) NOT NULL,
            address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(2) NOT NULL,
            zip VARCHAR(10),
            county VARCHAR(100),
            epa_region VARCHAR(5),
            lat FLOAT,
            lng FLOAT,
            source VARCHAR(50) DEFAULT 'epa_frs'
        )
    """)
    conn.commit()

    frs_file = STAGING / "epa_frs" / "FRS_FACILITIES.csv"
    if not frs_file.exists():
        print("  EPA FRS file not found, skipping")
        return 0

    total = 0
    batch = []
    print(f"  Loading {frs_file.name}...")
    with open(frs_file, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            state = (row.get("FAC_STATE") or "")[:2]
            if not state:
                continue
            batch.append((
                str(uuid.uuid4()),
                (row.get("REGISTRY_ID") or "")[:50],
                (row.get("FAC_NAME") or "Unknown")[:500],
                (row.get("FAC_STREET") or "")[:500] or None,
                (row.get("FAC_CITY") or "")[:100] or None,
                state,
                (row.get("FAC_ZIP") or "")[:10] or None,
                (row.get("FAC_COUNTY") or "")[:100] or None,
                (row.get("FAC_EPA_REGION") or "")[:5] or None,
                safe_float(row.get("LATITUDE_MEASURE")),
                safe_float(row.get("LONGITUDE_MEASURE")),
                "epa_frs",
            ))
            if len(batch) >= BATCH_SIZE:
                execute_values(cur, """
                    INSERT INTO epa_facilities (id, registry_id, name, address, city,
                        state, zip, county, epa_region, lat, lng, source)
                    VALUES %s ON CONFLICT (registry_id) DO NOTHING
                """, batch)
                conn.commit()
                total += len(batch)
                print(f"    {total:,} EPA records...", end="\r")
                batch = []

    if batch:
        execute_values(cur, """
            INSERT INTO epa_facilities (id, registry_id, name, address, city,
                state, zip, county, epa_region, lat, lng, source)
            VALUES %s ON CONFLICT (registry_id) DO NOTHING
        """, batch)
        conn.commit()
        total += len(batch)

    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_epa_registry ON epa_facilities (registry_id)",
        "CREATE INDEX IF NOT EXISTS ix_epa_geo ON epa_facilities (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_epa_state ON epa_facilities (state, city)",
    ]:
        cur.execute(idx)
    conn.commit()
    cur.close()
    print(f"  EPA facilities: {total:,} records loaded")
    return total


def load_fema_flood_zones(conn):
    """Load FEMA NFHL flood zone data for all 50 states."""
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fema_flood_zones (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            dfirm_id VARCHAR(20) NOT NULL,
            fld_zone VARCHAR(20) NOT NULL,
            zone_subtype VARCHAR(100),
            sfha_tf VARCHAR(1),
            static_bfe FLOAT,
            state_fips VARCHAR(2) NOT NULL,
            state_abbrev VARCHAR(2) NOT NULL,
            county_fips VARCHAR(5)
        )
    """)
    conn.commit()

    fema_dir = STAGING / "fema_nfhl"
    if not fema_dir.exists():
        print("  FEMA directory not found, skipping")
        return 0

    total = 0
    for csv_file in sorted(fema_dir.glob("nfhl_flood_zones_*.csv")):
        state = csv_file.stem.split("_")[-1]
        print(f"  Loading {csv_file.name} ({state})...")
        batch = []
        with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                dfirm_id = (row.get("dfirm_id") or "")[:20]
                # Extract county FIPS from dfirm_id (first 5 chars often contain it)
                county_fips = dfirm_id[:5] if len(dfirm_id) >= 5 else None

                batch.append((
                    str(uuid.uuid4()),
                    dfirm_id,
                    (row.get("fld_zone") or "")[:20],
                    (row.get("zone_subtype") or "")[:100] or None,
                    (row.get("sfha_tf") or "")[:1] or None,
                    safe_float(row.get("static_bfe")),
                    (row.get("state_fips") or "")[:2],
                    (row.get("state_abbrev") or state)[:2],
                    county_fips,
                ))
                if len(batch) >= BATCH_SIZE:
                    execute_values(cur, """
                        INSERT INTO fema_flood_zones (id, dfirm_id, fld_zone, zone_subtype,
                            sfha_tf, static_bfe, state_fips, state_abbrev, county_fips)
                        VALUES %s
                    """, batch)
                    conn.commit()
                    total += len(batch)
                    batch = []

        if batch:
            execute_values(cur, """
                INSERT INTO fema_flood_zones (id, dfirm_id, fld_zone, zone_subtype,
                    sfha_tf, static_bfe, state_fips, state_abbrev, county_fips)
                VALUES %s
            """, batch)
            conn.commit()
            total += len(batch)

    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_fema_state ON fema_flood_zones (state_abbrev, fld_zone)",
        "CREATE INDEX IF NOT EXISTS ix_fema_dfirm ON fema_flood_zones (dfirm_id)",
        "CREATE INDEX IF NOT EXISTS ix_fema_county ON fema_flood_zones (county_fips)",
    ]:
        cur.execute(idx)
    conn.commit()
    cur.close()
    print(f"  FEMA flood zones: {total:,} records loaded")
    return total


def load_census_demographics(conn):
    """Load Census ACS 2023 data — merge income, home value, population by block group."""
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS census_demographics (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            state_fips VARCHAR(2) NOT NULL,
            county_fips VARCHAR(3) NOT NULL,
            tract VARCHAR(6) NOT NULL,
            block_group VARCHAR(1),
            name VARCHAR(500),
            population INTEGER,
            median_income INTEGER,
            median_home_value INTEGER,
            homeownership_rate FLOAT,
            median_year_built INTEGER,
            total_housing_units INTEGER,
            occupied_units INTEGER,
            vacancy_rate FLOAT
        )
    """)
    conn.commit()

    census_dir = STAGING / "census_acs_2023"
    if not census_dir.exists():
        print("  Census ACS 2023 directory not found, skipping")
        return 0

    # Load each file into a dict keyed by (state, county, tract, block_group)
    data = {}

    def load_csv(filename, field_name, value_col):
        filepath = census_dir / filename
        if not filepath.exists():
            print(f"    {filename} not found, skipping")
            return
        print(f"    Loading {filename}...")
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (
                    row.get("state", "")[:2],
                    row.get("county", "")[:3],
                    row.get("tract", "")[:6],
                    row.get("block_group", "")[:1],
                )
                if not all(key[:3]):
                    continue
                if key not in data:
                    data[key] = {"name": row.get("name", "")}
                val = safe_int(row.get(value_col))
                if val is not None:
                    data[key][field_name] = val

    load_csv("acs5_median_household_income.csv", "median_income", "median_income")
    load_csv("acs5_median_home_value.csv", "median_home_value", "median_value")
    load_csv("acs5_total_population.csv", "population", "population")
    load_csv("acs5_median_year_built.csv", "median_year_built", "median_year_built")

    # Load tenure data for homeownership rate
    tenure_file = census_dir / "acs5_tenure.csv"
    if tenure_file.exists():
        print("    Loading acs5_tenure.csv...")
        with open(tenure_file, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (
                    row.get("state", "")[:2],
                    row.get("county", "")[:3],
                    row.get("tract", "")[:6],
                    row.get("block_group", "")[:1],
                )
                if not all(key[:3]):
                    continue
                if key not in data:
                    data[key] = {"name": row.get("name", "")}
                total = safe_int(row.get("total_occupied"))
                owner = safe_int(row.get("owner_occupied"))
                if total and total > 0 and owner is not None:
                    data[key]["homeownership_rate"] = round(owner / total * 100, 1)
                    data[key]["total_housing_units"] = total
                    data[key]["occupied_units"] = total

    # Load occupancy for vacancy rate
    occ_file = census_dir / "acs5_occupancy_status.csv"
    if occ_file.exists():
        print("    Loading acs5_occupancy_status.csv...")
        with open(occ_file, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (
                    row.get("state", "")[:2],
                    row.get("county", "")[:3],
                    row.get("tract", "")[:6],
                    row.get("block_group", "")[:1],
                )
                if not all(key[:3]):
                    continue
                if key not in data:
                    data[key] = {"name": row.get("name", "")}
                total = safe_int(row.get("total"))
                vacant = safe_int(row.get("vacant"))
                if total and total > 0 and vacant is not None:
                    data[key]["vacancy_rate"] = round(vacant / total * 100, 1)
                    if "total_housing_units" not in data[key]:
                        data[key]["total_housing_units"] = total

    # Insert into DB
    total = 0
    batch = []
    for (state_fips, county_fips, tract, block_group), vals in data.items():
        batch.append((
            str(uuid.uuid4()),
            state_fips,
            county_fips,
            tract,
            block_group or None,
            (vals.get("name") or "")[:500] or None,
            vals.get("population"),
            vals.get("median_income"),
            vals.get("median_home_value"),
            vals.get("homeownership_rate"),
            vals.get("median_year_built"),
            vals.get("total_housing_units"),
            vals.get("occupied_units"),
            vals.get("vacancy_rate"),
        ))
        if len(batch) >= BATCH_SIZE:
            execute_values(cur, """
                INSERT INTO census_demographics (id, state_fips, county_fips, tract,
                    block_group, name, population, median_income, median_home_value,
                    homeownership_rate, median_year_built, total_housing_units,
                    occupied_units, vacancy_rate)
                VALUES %s ON CONFLICT DO NOTHING
            """, batch)
            conn.commit()
            total += len(batch)
            print(f"    {total:,} census records...", end="\r")
            batch = []

    if batch:
        execute_values(cur, """
            INSERT INTO census_demographics (id, state_fips, county_fips, tract,
                block_group, name, population, median_income, median_home_value,
                homeownership_rate, median_year_built, total_housing_units,
                occupied_units, vacancy_rate)
            VALUES %s ON CONFLICT DO NOTHING
        """, batch)
        conn.commit()
        total += len(batch)

    for idx in [
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_census_geo ON census_demographics (state_fips, county_fips, tract, block_group)",
        "CREATE INDEX IF NOT EXISTS ix_census_state ON census_demographics (state_fips, county_fips)",
    ]:
        try:
            cur.execute(idx)
        except Exception:
            conn.rollback()
    conn.commit()
    cur.close()
    print(f"  Census demographics: {total:,} records loaded")
    return total


def load_septic_systems(conn):
    """Load septic/wastewater data from FL DOH and other states."""
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS septic_systems (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            address VARCHAR(500),
            city VARCHAR(100),
            state VARCHAR(2) NOT NULL,
            zip VARCHAR(10),
            county VARCHAR(100),
            parcel_id VARCHAR(200),
            lat FLOAT,
            lng FLOAT,
            system_type VARCHAR(100),
            wastewater_source VARCHAR(200),
            install_date DATE,
            last_inspection DATE,
            land_use VARCHAR(50),
            status VARCHAR(50),
            source VARCHAR(50) NOT NULL
        )
    """)
    conn.commit()

    total = 0

    # Florida DOH wastewater
    fl_file = STAGING / "fl_doh_septic" / "fl_doh_wastewater_with_coords.csv"
    if fl_file.exists():
        print(f"  Loading FL DOH wastewater...")
        batch = []
        with open(fl_file, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                batch.append((
                    str(uuid.uuid4()),
                    (row.get("PHY_ADD1") or "")[:500] or None,
                    (row.get("PHY_CITY") or "")[:100] or None,
                    "FL",
                    (row.get("PHY_ZIPCD") or "")[:10] or None,
                    None,  # county from CO_NO would need mapping
                    (row.get("PARCELNO") or "")[:200] or None,
                    safe_float(row.get("latitude")),
                    safe_float(row.get("longitude")),
                    (row.get("WW") or "")[:100] or None,
                    (row.get("WW_SRC_NAME") or "")[:200] or None,
                    None,  # install_date not in this dataset
                    None,  # last_inspection
                    (row.get("LANDUSE") or "")[:50] or None,
                    (row.get("APP_STATUS") or "")[:50] or None,
                    "fl_doh",
                ))
                if len(batch) >= BATCH_SIZE:
                    execute_values(cur, """
                        INSERT INTO septic_systems (id, address, city, state, zip, county,
                            parcel_id, lat, lng, system_type, wastewater_source,
                            install_date, last_inspection, land_use, status, source)
                        VALUES %s
                    """, batch)
                    conn.commit()
                    total += len(batch)
                    print(f"    {total:,} septic records...", end="\r")
                    batch = []

        if batch:
            execute_values(cur, """
                INSERT INTO septic_systems (id, address, city, state, zip, county,
                    parcel_id, lat, lng, system_type, wastewater_source,
                    install_date, last_inspection, land_use, status, source)
                VALUES %s
            """, batch)
            conn.commit()
            total += len(batch)

    # Florida DOH Miami-Dade septic
    fl_md_file = STAGING / "fl_doh_septic" / "fl_doh_septic_miamidade.csv"
    if fl_md_file.exists():
        print(f"  Loading FL DOH Miami-Dade septic...")
        batch = []
        with open(fl_md_file, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                batch.append((
                    str(uuid.uuid4()),
                    (row.get("SYSTADDR") or row.get("STNDADDR") or "")[:500] or None,
                    (row.get("CITY") or "")[:100] or None,
                    "FL",
                    (row.get("ZIPCODE") or "")[:10] or None,
                    "Miami-Dade",
                    (row.get("FOLIO") or "")[:200] or None,
                    safe_float(row.get("LAT") or row.get("LATORIG")),
                    safe_float(row.get("LON") or row.get("LONORIG")),
                    (row.get("SYSTTYPE") or "")[:100] or None,
                    None,
                    safe_date(row.get("APPRDATE")),
                    safe_date(row.get("FINALINSP")),
                    (row.get("COMRESID") or "")[:50] or None,
                    (row.get("FINSYSAPRV") or "")[:50] or None,
                    "fl_doh_miamidade",
                ))
                if len(batch) >= BATCH_SIZE:
                    execute_values(cur, """
                        INSERT INTO septic_systems (id, address, city, state, zip, county,
                            parcel_id, lat, lng, system_type, wastewater_source,
                            install_date, last_inspection, land_use, status, source)
                        VALUES %s
                    """, batch)
                    conn.commit()
                    total += len(batch)
                    batch = []

        if batch:
            execute_values(cur, """
                INSERT INTO septic_systems (id, address, city, state, zip, county,
                    parcel_id, lat, lng, system_type, wastewater_source,
                    install_date, last_inspection, land_use, status, source)
                VALUES %s
            """, batch)
            conn.commit()
            total += len(batch)

    # Multi-state septic data
    multi_dir = STAGING / "multistate_septic"
    if multi_dir.exists():
        for csv_file in multi_dir.glob("*.csv"):
            print(f"  Loading {csv_file.name}...")
            batch = []
            with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    state = (row.get("state") or row.get("STATE") or "")[:2].upper()
                    if not state:
                        continue
                    batch.append((
                        str(uuid.uuid4()),
                        (row.get("address") or row.get("ADDRESS") or "")[:500] or None,
                        (row.get("city") or row.get("CITY") or "")[:100] or None,
                        state,
                        (row.get("zip") or row.get("ZIP") or row.get("zipcode") or "")[:10] or None,
                        (row.get("county") or row.get("COUNTY") or "")[:100] or None,
                        (row.get("parcel_id") or row.get("PARCEL") or "")[:200] or None,
                        safe_float(row.get("lat") or row.get("latitude") or row.get("LAT")),
                        safe_float(row.get("lng") or row.get("longitude") or row.get("LON")),
                        (row.get("system_type") or row.get("type") or row.get("SYSTEM_TYPE") or "")[:100] or None,
                        (row.get("source_name") or row.get("SOURCE") or "")[:200] or None,
                        safe_date(row.get("install_date") or row.get("INSTALL_DATE")),
                        safe_date(row.get("inspection_date") or row.get("LAST_INSPECTION")),
                        (row.get("land_use") or row.get("LANDUSE") or "")[:50] or None,
                        (row.get("status") or row.get("STATUS") or "")[:50] or None,
                        f"multistate_{csv_file.stem}",
                    ))
                    if len(batch) >= BATCH_SIZE:
                        execute_values(cur, """
                            INSERT INTO septic_systems (id, address, city, state, zip, county,
                                parcel_id, lat, lng, system_type, wastewater_source,
                                install_date, last_inspection, land_use, status, source)
                            VALUES %s
                        """, batch)
                        conn.commit()
                        total += len(batch)
                        batch = []

            if batch:
                execute_values(cur, """
                    INSERT INTO septic_systems (id, address, city, state, zip, county,
                        parcel_id, lat, lng, system_type, wastewater_source,
                        install_date, last_inspection, land_use, status, source)
                    VALUES %s
                """, batch)
                conn.commit()
                total += len(batch)

    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_septic_state ON septic_systems (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_septic_geo ON septic_systems (lat, lng)",
        "CREATE INDEX IF NOT EXISTS ix_septic_addr ON septic_systems (address)",
        "CREATE INDEX IF NOT EXISTS ix_septic_zip ON septic_systems (zip)",
    ]:
        cur.execute(idx)
    conn.commit()
    cur.close()
    print(f"  Septic systems: {total:,} records loaded")
    return total


def load_property_valuations(conn):
    """Load Redfin ZIP-level market data."""
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS property_valuations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            zip VARCHAR(10) NOT NULL,
            state VARCHAR(2),
            state_code VARCHAR(2),
            city VARCHAR(100),
            region VARCHAR(200),
            property_type VARCHAR(50),
            period_begin DATE NOT NULL,
            period_end DATE NOT NULL,
            median_sale_price FLOAT,
            median_list_price FLOAT,
            median_ppsf FLOAT,
            median_list_ppsf FLOAT,
            homes_sold INTEGER,
            pending_sales INTEGER,
            new_listings INTEGER,
            inventory INTEGER,
            months_of_supply FLOAT,
            median_dom INTEGER,
            avg_sale_to_list FLOAT,
            sold_above_list FLOAT,
            price_drops FLOAT,
            parent_metro VARCHAR(200)
        )
    """)
    conn.commit()

    redfin_file = STAGING / "redfin_housing" / "redfin_zip_market_tracker.tsv.gz"
    if not redfin_file.exists():
        print("  Redfin ZIP data not found, skipping")
        return 0

    print(f"  Loading Redfin ZIP market data (gzipped)...")
    total = 0
    batch = []

    with gzip.open(redfin_file, "rt", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            # Extract ZIP from region name
            region = row.get("REGION") or ""
            zip_code = ""
            if "Zip Code:" in region:
                zip_code = region.replace("Zip Code:", "").strip()
            elif row.get("REGION_TYPE_ID") == "2":
                zip_code = region.strip()

            if not zip_code or len(zip_code) < 5:
                continue

            period_begin = safe_date(row.get("PERIOD_BEGIN"))
            period_end = safe_date(row.get("PERIOD_END"))
            if not period_begin or not period_end:
                continue

            batch.append((
                str(uuid.uuid4()),
                zip_code[:10],
                (row.get("STATE") or "")[:2] or None,
                (row.get("STATE_CODE") or "")[:2] or None,
                (row.get("CITY") or "")[:100] or None,
                region[:200],
                (row.get("PROPERTY_TYPE") or "")[:50] or None,
                period_begin,
                period_end,
                safe_float(row.get("MEDIAN_SALE_PRICE")),
                safe_float(row.get("MEDIAN_LIST_PRICE")),
                safe_float(row.get("MEDIAN_PPSF")),
                safe_float(row.get("MEDIAN_LIST_PPSF")),
                safe_int(row.get("HOMES_SOLD")),
                safe_int(row.get("PENDING_SALES")),
                safe_int(row.get("NEW_LISTINGS")),
                safe_int(row.get("INVENTORY")),
                safe_float(row.get("MONTHS_OF_SUPPLY")),
                safe_int(row.get("MEDIAN_DOM")),
                safe_float(row.get("AVG_SALE_TO_LIST")),
                safe_float(row.get("SOLD_ABOVE_LIST")),
                safe_float(row.get("PRICE_DROPS")),
                (row.get("PARENT_METRO_REGION") or "")[:200] or None,
            ))
            if len(batch) >= BATCH_SIZE:
                execute_values(cur, """
                    INSERT INTO property_valuations (id, zip, state, state_code, city,
                        region, property_type, period_begin, period_end,
                        median_sale_price, median_list_price, median_ppsf, median_list_ppsf,
                        homes_sold, pending_sales, new_listings, inventory,
                        months_of_supply, median_dom, avg_sale_to_list,
                        sold_above_list, price_drops, parent_metro)
                    VALUES %s
                """, batch)
                conn.commit()
                total += len(batch)
                print(f"    {total:,} valuation records...", end="\r")
                batch = []

    if batch:
        execute_values(cur, """
            INSERT INTO property_valuations (id, zip, state, state_code, city,
                region, property_type, period_begin, period_end,
                median_sale_price, median_list_price, median_ppsf, median_list_ppsf,
                homes_sold, pending_sales, new_listings, inventory,
                months_of_supply, median_dom, avg_sale_to_list,
                sold_above_list, price_drops, parent_metro)
            VALUES %s
        """, batch)
        conn.commit()
        total += len(batch)

    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_val_zip ON property_valuations (zip, period_end)",
        "CREATE INDEX IF NOT EXISTS ix_val_state ON property_valuations (state, zip)",
    ]:
        cur.execute(idx)
    conn.commit()
    cur.close()
    print(f"  Property valuations: {total:,} records loaded")
    return total


LOADERS = {
    "contractor_licenses": load_contractor_licenses,
    "epa_facilities": load_epa_facilities,
    "fema_flood_zones": load_fema_flood_zones,
    "census_demographics": load_census_demographics,
    "septic_systems": load_septic_systems,
    "property_valuations": load_property_valuations,
}


def main():
    parser = argparse.ArgumentParser(description="Load data layers into T430 PostgreSQL")
    parser.add_argument("--layer", default="all",
                        choices=["all"] + list(LOADERS.keys()),
                        help="Which data layer to load")
    parser.add_argument("--db-host", default=DB_HOST)
    parser.add_argument("--staging-dir", default=str(STAGING))
    args = parser.parse_args()

    global DB_HOST, STAGING
    DB_HOST = args.db_host
    STAGING = Path(args.staging_dir)

    print(f"Connecting to PostgreSQL at {DB_HOST}...")
    conn = get_conn()
    print("Connected.\n")

    layers = list(LOADERS.keys()) if args.layer == "all" else [args.layer]
    grand_total = 0

    for layer in layers:
        print(f"=== Loading {layer} ===")
        try:
            count = LOADERS[layer](conn)
            grand_total += count
        except Exception as e:
            print(f"  ERROR loading {layer}: {e}")
            conn.rollback()

    conn.close()
    print(f"\n{'='*50}")
    print(f"Grand total: {grand_total:,} records loaded across {len(layers)} layers")


if __name__ == "__main__":
    main()
