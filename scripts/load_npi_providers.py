#!/usr/bin/env python3
"""
NPI Healthcare Provider Bulk Loader — parses CMS NPI bulk ZIP file.

The NPI bulk download (~1GB ZIP) contains 7M+ healthcare providers with:
  NPI, name, credentials, specialty, organization, address, phone

Loads into: doctors_clinicians table (already exists)

Prerequisites:
  - NPI bulk ZIP file downloaded to R730 staging: /mnt/data/staging/npi_march2026.zip
  - Download from: https://download.cms.gov/nppes/NPI_Files.html

Usage (run on R730):
    nohup python3 -u load_npi_providers.py --db-host 100.122.216.15 --zip-path /mnt/data/staging/npi_march2026.zip > /tmp/npi_load.log 2>&1 &

Fields mapped:
    NPI → npi
    Provider First Name → first_name
    Provider Last Name (Legal Name) → last_name
    Provider Organization Name (Legal Business Name) → facility_name
    Healthcare Provider Taxonomy Code_1 → specialty
    Provider First Line Business Practice Location Address → address
    Provider Business Practice Location Address City Name → city
    Provider Business Practice Location Address State Name → state
    Provider Business Practice Location Address Postal Code → zip
    Provider Business Practice Location Address Telephone Number → phone
"""

import argparse, csv, os, sys, time, uuid, zipfile
from datetime import datetime
from pathlib import Path

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: pip install psycopg2-binary"); sys.exit(1)

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"; DB_NAME = "permits"; DB_USER = "will"
BATCH_SIZE = 10000
SOURCE = "cms_npi_bulk"


def get_conn():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER)


def log(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def s(v, m=500):
    """Safe string with max length."""
    if not v: return None
    val = str(v).strip()
    if not val or val.upper() in ("", "NULL", "NONE"):
        return None
    return val[:m]


def get_count(conn, source):
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM doctors_clinicians WHERE source = %s", (source,))
    c = cur.fetchone()[0]; cur.close(); return c


def ensure_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS doctors_clinicians (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            npi TEXT, first_name TEXT, last_name TEXT, specialty TEXT,
            facility_name TEXT, address TEXT, city TEXT, state VARCHAR(2) NOT NULL,
            zip TEXT, phone TEXT, source TEXT NOT NULL DEFAULT 'cms_physicians')
    """)
    for idx in [
        "CREATE INDEX IF NOT EXISTS ix_doc_state ON doctors_clinicians (state)",
        "CREATE INDEX IF NOT EXISTS ix_doc_npi ON doctors_clinicians (npi)",
        "CREATE INDEX IF NOT EXISTS ix_doc_specialty ON doctors_clinicians (specialty)",
        "CREATE INDEX IF NOT EXISTS ix_doc_source ON doctors_clinicians (source)",
        "CREATE INDEX IF NOT EXISTS ix_doc_name ON doctors_clinicians (last_name, first_name)",
        "CREATE INDEX IF NOT EXISTS ix_doc_city ON doctors_clinicians (state, city)",
        "CREATE INDEX IF NOT EXISTS ix_doc_zip ON doctors_clinicians (zip)",
    ]:
        try: cur.execute(idx)
        except Exception: conn.rollback()
    conn.commit(); cur.close()


DOC_SQL = """INSERT INTO doctors_clinicians
    (id, npi, first_name, last_name, specialty, facility_name,
     address, city, state, zip, phone, source)
    VALUES %s ON CONFLICT DO NOTHING"""

# NPI CSV column names (verbose CMS naming convention)
COL_NPI = "NPI"
COL_FIRST = "Provider First Name"
COL_LAST = "Provider Last Name (Legal Name)"
COL_ORG = "Provider Organization Name (Legal Business Name)"
COL_TAXONOMY = "Healthcare Provider Taxonomy Code_1"
COL_ADDRESS = "Provider First Line Business Practice Location Address"
COL_CITY = "Provider Business Practice Location Address City Name"
COL_STATE = "Provider Business Practice Location Address State Name"
COL_ZIP = "Provider Business Practice Location Address Postal Code"
COL_PHONE = "Provider Business Practice Location Address Telephone Number"
COL_ENTITY_TYPE = "Entity Type Code"


# Common taxonomy code -> specialty mapping (top ~200 codes)
TAXONOMY_MAP = {
    "207R00000X": "Internal Medicine",
    "207Q00000X": "Family Medicine",
    "208D00000X": "General Practice",
    "207V00000X": "Obstetrics & Gynecology",
    "2085R0202X": "Diagnostic Radiology",
    "208600000X": "Surgery",
    "207X00000X": "Orthopaedic Surgery",
    "207Y00000X": "Otolaryngology",
    "2084N0400X": "Neurology",
    "208C00000X": "Colon & Rectal Surgery",
    "2086S0120X": "Pediatric Surgery",
    "207T00000X": "Neurological Surgery",
    "2086S0122X": "Plastic & Reconstructive Surgery",
    "208G00000X": "Thoracic Surgery",
    "2086S0105X": "Hand Surgery",
    "207W00000X": "Ophthalmology",
    "2085R0001X": "Radiology",
    "2084P0800X": "Psychiatry",
    "207RG0100X": "Gastroenterology",
    "207RC0000X": "Cardiovascular Disease",
    "207RP1001X": "Pulmonary Disease",
    "207RH0000X": "Hematology",
    "207RX0202X": "Medical Oncology",
    "207RE0101X": "Endocrinology",
    "207RN0300X": "Nephrology",
    "207RI0200X": "Infectious Disease",
    "207RR0500X": "Rheumatology",
    "207RA0401X": "Addiction Medicine",
    "207RC0200X": "Critical Care Medicine",
    "207RA0000X": "Adolescent Medicine",
    "208000000X": "Pediatrics",
    "207RG0300X": "Geriatric Medicine",
    "207RS0010X": "Sports Medicine",
    "208100000X": "Physical Medicine & Rehabilitation",
    "2083P0500X": "Preventive Medicine",
    "207K00000X": "Allergy & Immunology",
    "207L00000X": "Anesthesiology",
    "207N00000X": "Dermatology",
    "207P00000X": "Emergency Medicine",
    "207U00000X": "Nuclear Medicine",
    "207ZP0102X": "Pathology",
    "208200000X": "Plastic Surgery",
    "2084P0015X": "Psychosomatic Medicine",
    "208VP0014X": "Pain Medicine",
    "207SG0202X": "Pediatric Cardiology",
    "2080P0006X": "Developmental-Behavioral Pediatrics",
    "363L00000X": "Nurse Practitioner",
    "363A00000X": "Physician Assistant",
    "261QM1300X": "Multi-Specialty Clinic",
    "261QR1300X": "Rural Health Clinic",
    "261QF0400X": "Federally Qualified Health Center",
    "261QU0200X": "Urgent Care Clinic",
    "282N00000X": "General Acute Care Hospital",
    "291U00000X": "Clinical Medical Laboratory",
    "332B00000X": "Durable Medical Equipment",
    "174400000X": "Specialist",
    "1223G0001X": "General Practice Dentistry",
    "122300000X": "Dentist",
    "133V00000X": "Dietitian",
    "152W00000X": "Optometrist",
    "111N00000X": "Chiropractor",
    "225100000X": "Physical Therapist",
    "225500000X": "Respiratory Therapist",
    "163W00000X": "Registered Nurse",
    "367500000X": "Certified Registered Nurse Anesthetist",
    "364S00000X": "Clinical Nurse Specialist",
    "171100000X": "Acupuncturist",
    "103T00000X": "Psychologist",
    "104100000X": "Social Worker",
    "106H00000X": "Marriage & Family Therapist",
    "101Y00000X": "Counselor",
    "183500000X": "Pharmacist",
    "3336C0003X": "Community/Retail Pharmacy",
    "261QP2300X": "Primary Care Clinic",
    "251E00000X": "Home Health Agency",
    "314000000X": "Skilled Nursing Facility",
    "313M00000X": "Nursing Facility",
    "311500000X": "Alzheimer Center",
    "3416L0300X": "Ambulance",
    "174200000X": "Meals on Wheels",
    "193200000X": "Multi-Specialty Group",
    "193400000X": "Single Specialty Group",
    "207RG0600X": "Geriatric Medicine (IM)",
    "2080P0203X": "Pediatric Cardiology",
    "2080P0202X": "Pediatric Endocrinology",
    "2080P0205X": "Pediatric Gastroenterology",
    "2080P0206X": "Pediatric Hematology-Oncology",
    "2080P0210X": "Pediatric Infectious Diseases",
    "2080P0214X": "Pediatric Nephrology",
    "2080N0001X": "Neonatal-Perinatal Medicine",
    "2080P0216X": "Pediatric Pulmonology",
}


def resolve_specialty(taxonomy_code):
    """Resolve taxonomy code to human-readable specialty name."""
    if not taxonomy_code:
        return None
    code = taxonomy_code.strip()
    return TAXONOMY_MAP.get(code, code)


def find_npi_csv_in_zip(zip_path):
    """Find the main NPI data CSV file inside the ZIP."""
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for name in zf.namelist():
            # The main file is named like npidata_pfile_20050523-20260309.csv
            if name.startswith("npidata_pfile_") and name.endswith(".csv"):
                return name
        # Fallback: find the largest CSV
        csvs = [(name, zf.getinfo(name).file_size) for name in zf.namelist()
                if name.endswith(".csv")]
        if csvs:
            csvs.sort(key=lambda x: x[1], reverse=True)
            return csvs[0][0]
    return None


def load_npi_providers(conn, zip_path):
    """Parse NPI bulk CSV from ZIP and load into doctors_clinicians."""
    log(f"=== NPI Healthcare Provider Bulk Load ===")
    log(f"  ZIP: {zip_path}")

    existing = get_count(conn, SOURCE)
    if existing > 1000000:
        log(f"  SKIP -- already {existing:,} records (source={SOURCE})")
        return 0

    # Find the CSV inside the ZIP
    csv_name = find_npi_csv_in_zip(zip_path)
    if not csv_name:
        log("  ERROR: No npidata_pfile_*.csv found in ZIP")
        return 0
    log(f"  CSV: {csv_name}")

    cur = conn.cursor()
    total = 0
    skipped = 0
    batch = []

    with zipfile.ZipFile(zip_path, 'r') as zf:
        with zf.open(csv_name) as f:
            # Wrap binary file in text wrapper for csv.DictReader
            import io
            text_file = io.TextIOWrapper(f, encoding='utf-8', errors='replace')
            reader = csv.DictReader(text_file)

            # Log the columns we found
            log(f"  Columns: {len(reader.fieldnames)}")

            for row_num, row in enumerate(reader, 1):
                # Get state — required field
                state = s(row.get(COL_STATE), 2)
                if not state or len(state) != 2:
                    skipped += 1
                    continue

                # Get NPI — required
                npi = s(row.get(COL_NPI), 20)
                if not npi:
                    skipped += 1
                    continue

                # Determine if individual (1) or organization (2)
                entity_type = s(row.get(COL_ENTITY_TYPE), 1)

                first_name = s(row.get(COL_FIRST), 200)
                last_name = s(row.get(COL_LAST), 200)
                org_name = s(row.get(COL_ORG), 300)

                # Skip if no name at all
                if not first_name and not last_name and not org_name:
                    skipped += 1
                    continue

                # Resolve specialty from taxonomy code
                taxonomy = s(row.get(COL_TAXONOMY), 50)
                specialty = resolve_specialty(taxonomy)

                # Clean phone: remove non-digits, format
                phone_raw = s(row.get(COL_PHONE), 20)
                phone = None
                if phone_raw:
                    digits = ''.join(c for c in phone_raw if c.isdigit())
                    if len(digits) == 10:
                        phone = f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
                    elif len(digits) == 11 and digits[0] == '1':
                        phone = f"{digits[1:4]}-{digits[4:7]}-{digits[7:]}"
                    else:
                        phone = phone_raw

                # Clean zip: take first 5 digits
                zip_raw = s(row.get(COL_ZIP), 20)
                zip_code = None
                if zip_raw:
                    zip_digits = ''.join(c for c in zip_raw if c.isdigit())
                    zip_code = zip_digits[:5] if len(zip_digits) >= 5 else zip_raw[:10]

                batch.append((
                    str(uuid.uuid4()),
                    npi,
                    first_name,
                    last_name,
                    specialty,
                    org_name,
                    s(row.get(COL_ADDRESS), 500),
                    s(row.get(COL_CITY), 100),
                    state,
                    zip_code,
                    phone,
                    SOURCE,
                ))

                if len(batch) >= BATCH_SIZE:
                    try:
                        execute_values(cur, DOC_SQL, batch)
                        conn.commit()
                        total += len(batch)
                        if total % 100000 == 0:
                            log(f"    {total:,} loaded ({skipped:,} skipped)")
                    except Exception as e:
                        log(f"    Insert error at row ~{row_num}: {e}")
                        conn.rollback()
                    batch = []

            # Final batch
            if batch:
                try:
                    execute_values(cur, DOC_SQL, batch)
                    conn.commit()
                    total += len(batch)
                except Exception as e:
                    log(f"    Final batch error: {e}")
                    conn.rollback()

    cur.close()
    log(f"  Loaded: {total:,} providers ({skipped:,} skipped)")
    return total


def main():
    global DB_HOST
    parser = argparse.ArgumentParser(description="NPI Healthcare Provider Bulk Loader")
    parser.add_argument("--db-host", default=DB_HOST, help="PostgreSQL host")
    parser.add_argument("--zip-path", default="/mnt/data/staging/npi_march2026.zip",
                        help="Path to NPI bulk ZIP file")
    args = parser.parse_args()
    DB_HOST = args.db_host

    zip_path = Path(args.zip_path)
    if not zip_path.exists():
        log(f"ERROR: ZIP file not found: {zip_path}")
        log("Download from: https://download.cms.gov/nppes/NPI_Files.html")
        sys.exit(1)

    conn = get_conn()
    log(f"Connected to {DB_HOST}")
    ensure_table(conn)

    try:
        count = load_npi_providers(conn, str(zip_path))
        log(f"COMPLETE -- {count:,} NPI providers loaded")
    except Exception as e:
        log(f"FATAL: {e}")
        import traceback; traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
