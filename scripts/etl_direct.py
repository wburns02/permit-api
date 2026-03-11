"""
Direct ETL: SQLite → Railway PostgreSQL using asyncpg (no SQLAlchemy).
Designed to run on T430 where SQLAlchemy isn't installed.

Usage:
    python3 scripts/etl_direct.py \
        --sqlite /dataPool/data/databases/crm_permits.db \
        --pg "postgresql://postgres:PASSWORD@maglev.proxy.rlwy.net:35206/railway" \
        --batch-size 5000 \
        --offset 0
"""

import argparse
import sqlite3
import asyncio
import re
import logging
from datetime import date, datetime
from uuid import uuid4

import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Address normalization (copied from search_service.py)
STREET_ABBREV = {
    r"\bstreet\b": "ST", r"\bst\b": "ST", r"\bavenue\b": "AVE", r"\bave\b": "AVE",
    r"\bdrive\b": "DR", r"\bdr\b": "DR", r"\broad\b": "RD", r"\brd\b": "RD",
    r"\bboulevard\b": "BLVD", r"\bblvd\b": "BLVD", r"\blane\b": "LN", r"\bln\b": "LN",
    r"\bcourt\b": "CT", r"\bct\b": "CT", r"\bcircle\b": "CIR", r"\bcir\b": "CIR",
    r"\bplace\b": "PL", r"\bpl\b": "PL", r"\bway\b": "WAY",
    r"\bnorth\b": "N", r"\bsouth\b": "S", r"\beast\b": "E", r"\bwest\b": "W",
    r"\bnortheast\b": "NE", r"\bnorthwest\b": "NW", r"\bsoutheast\b": "SE", r"\bsouthwest\b": "SW",
    r"\bapartment\b": "APT", r"\bapt\b": "APT", r"\bsuite\b": "STE", r"\bste\b": "STE",
    r"\bunit\b": "UNIT", r"\b#\b": "UNIT",
}

PERMIT_TYPE_MAP = {
    "building": "building", "electrical": "electrical", "plumbing": "plumbing",
    "mechanical": "mechanical", "demolition": "demolition", "fire": "fire",
    "roofing": "building", "septic": "plumbing", "ossf": "plumbing",
    "hvac": "mechanical", "gas": "mechanical", "fence": "building",
    "sign": "building", "pool": "building", "solar": "electrical",
    "grading": "building", "right-of-way": "building",
}


def normalize_address(addr: str) -> str:
    if not addr:
        return ""
    result = addr.upper().strip()
    result = re.sub(r"[^\w\s]", " ", result)
    for pattern, replacement in STREET_ABBREV.items():
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", result).strip()


def map_permit_type(trade, project_type=None):
    trade_lower = (trade or "").lower()
    for key, val in PERMIT_TYPE_MAP.items():
        if key in trade_lower:
            return val
    if project_type:
        pt_lower = project_type.lower()
        for key, val in PERMIT_TYPE_MAP.items():
            if key in pt_lower:
                return val
    return trade_lower or "building"


def parse_date(date_str):
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


INSERT_SQL = """
    INSERT INTO permits (
        id, permit_number, original_id,
        address, address_normalized, city, state, zip, lat, lng, parcel_id,
        permit_type, work_type, trade, status, description,
        owner_name, contractor_name, contractor_company, applicant_name,
        jurisdiction, source,
        issue_date, created_date, expired_date, completed_date, scraped_at
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
        $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22,
        $23, $24, $25, $26, $27
    )
    ON CONFLICT DO NOTHING
"""


async def run_etl(args):
    logger.info(f"Connecting to PostgreSQL...")
    pool = await asyncpg.create_pool(args.pg, min_size=2, max_size=5)

    conn_sqlite = sqlite3.connect(args.sqlite)
    conn_sqlite.row_factory = sqlite3.Row

    max_rowid = conn_sqlite.execute("SELECT MAX(rowid) FROM permits").fetchone()[0]
    logger.info(f"Max rowid: {max_rowid:,}, starting from offset {args.offset:,}")

    processed = args.offset
    inserted = 0
    skipped = 0
    batch_size = args.batch_size

    while processed < max_rowid:
        cursor = conn_sqlite.execute(
            "SELECT * FROM permits WHERE rowid > ? ORDER BY rowid LIMIT ?",
            (processed, batch_size),
        )
        rows = cursor.fetchall()
        if not rows:
            break

        batch = []
        for row in rows:
            address = row["address"]
            if not address:
                skipped += 1
                continue

            batch.append((
                str(uuid4()),                               # id
                row["permit_number"],                       # permit_number
                row["original_id"],                         # original_id
                address,                                    # address
                normalize_address(address),                 # address_normalized
                row["city"],                                # city
                row["state"],                               # state
                row["zip"],                                 # zip
                row["lat"],                                 # lat
                row["lng"],                                 # lng
                row["parcel_id"],                           # parcel_id
                map_permit_type(row["trade"], row["project_type"]),  # permit_type
                row["work_type"],                           # work_type
                row["trade"],                               # trade
                row["status"],                              # status
                row["description"],                         # description
                row["owner_name"],                          # owner_name
                row["applicant_name"],                      # contractor_name
                row["applicant_company"],                   # contractor_company
                row["applicant_name"],                      # applicant_name
                row["jurisdiction_name"] or "",             # jurisdiction
                row["source"],                              # source
                parse_date(row["issued_date"]),             # issue_date
                parse_date(row["created_date"]),            # created_date
                parse_date(row["expired_date"]),            # expired_date
                parse_date(row["completed_date"]),          # completed_date
                parse_date(row["scraped_at"]),              # scraped_at
            ))

        if batch:
            async with pool.acquire() as pg_conn:
                await pg_conn.executemany(INSERT_SQL, batch)
            inserted += len(batch)

        # Track by max rowid seen in this batch
        last_rowid = conn_sqlite.execute(
            "SELECT rowid FROM permits WHERE rowid > ? ORDER BY rowid LIMIT 1 OFFSET ?",
            (processed, len(rows) - 1),
        ).fetchone()
        processed = last_rowid[0] if last_rowid else processed + len(rows)

        if inserted % 50000 < batch_size:
            logger.info(
                f"Progress: ~{processed:,}/{max_rowid:,} ({processed*100//max_rowid}%) — "
                f"inserted: {inserted:,}, skipped: {skipped:,}"
            )

    conn_sqlite.close()
    await pool.close()
    logger.info(f"ETL complete: {inserted:,} inserted, {skipped:,} skipped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Direct ETL: SQLite → PostgreSQL")
    parser.add_argument("--sqlite", required=True, help="Path to crm_permits.db")
    parser.add_argument("--pg", required=True, help="PostgreSQL connection URL")
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--offset", type=int, default=0, help="Resume from rowid offset")
    args = parser.parse_args()
    asyncio.run(run_etl(args))
