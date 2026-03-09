"""
ETL: Load permit data from SQLite → PostgreSQL in batches.

Usage:
    python scripts/etl_sqlite_to_pg.py --sqlite /path/to/crm_permits.db --batch-size 10000

Handles ~1B records by streaming in configurable batches.
"""

import argparse
import sqlite3
import asyncio
import re
import logging
from datetime import date, datetime
from uuid import uuid4

from sqlalchemy import text
from app.database import engine, async_session_maker
from app.models.permit import Permit, Jurisdiction
from app.services.search_service import normalize_address

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Map SQLite trade/project_type to standard permit types
PERMIT_TYPE_MAP = {
    "building": "building",
    "electrical": "electrical",
    "plumbing": "plumbing",
    "mechanical": "mechanical",
    "demolition": "demolition",
    "fire": "fire",
    "roofing": "building",
    "septic": "plumbing",
    "ossf": "plumbing",
    "hvac": "mechanical",
    "gas": "mechanical",
    "fence": "building",
    "sign": "building",
    "pool": "building",
    "solar": "electrical",
    "grading": "building",
    "right-of-way": "building",
}


def map_permit_type(trade: str, project_type: str = None) -> str:
    """Map source trade/project_type to standard permit type."""
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


def parse_date(date_str: str | None) -> date | None:
    """Parse various date formats from source data."""
    if not date_str:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except (ValueError, AttributeError):
            continue
    return None


async def ensure_extensions():
    """Create required PostgreSQL extensions."""
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""))
    logger.info("PostgreSQL extensions ensured (pg_trgm, uuid-ossp)")


async def load_jurisdictions(sqlite_path: str):
    """Load jurisdiction metadata from SQLite."""
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        "SELECT jurisdiction_name, state, COUNT(*) as cnt "
        "FROM permits GROUP BY jurisdiction_name, state ORDER BY cnt DESC"
    )

    jurisdictions = []
    for row in cursor:
        jurisdictions.append({
            "name": row["jurisdiction_name"],
            "state": row["state"],
            "record_count": row["cnt"],
        })

    conn.close()

    async with async_session_maker() as db:
        for j in jurisdictions:
            if not j["name"]:
                continue
            await db.execute(
                text("""
                    INSERT INTO jurisdictions (name, state, record_count)
                    VALUES (:name, :state, :record_count)
                    ON CONFLICT (name, state) DO UPDATE SET record_count = :record_count
                """),
                j,
            )
        await db.commit()

    logger.info(f"Loaded {len(jurisdictions)} jurisdictions")


async def load_permits(sqlite_path: str, batch_size: int = 10000, offset: int = 0):
    """Stream permits from SQLite to PostgreSQL in batches."""
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row

    total = conn.execute("SELECT COUNT(*) FROM permits").fetchone()[0]
    logger.info(f"Total source records: {total:,}")

    processed = offset
    inserted = 0
    skipped = 0

    while processed < total:
        cursor = conn.execute(
            "SELECT * FROM permits ORDER BY rowid LIMIT ? OFFSET ?",
            (batch_size, processed),
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

            batch.append({
                "id": str(uuid4()),
                "permit_number": row["permit_number"],
                "original_id": row["original_id"],
                "address": address,
                "address_normalized": normalize_address(address),
                "city": row["city"],
                "state": row["state"],
                "zip": row["zip"],
                "lat": row["lat"],
                "lng": row["lng"],
                "parcel_id": row["parcel_id"],
                "permit_type": map_permit_type(row["trade"], row["project_type"]),
                "work_type": row["work_type"],
                "trade": row["trade"],
                "status": row["status"],
                "description": row["description"],
                "owner_name": row["owner_name"],
                "contractor_name": row["applicant_name"],
                "contractor_company": row["applicant_company"],
                "applicant_name": row["applicant_name"],
                "jurisdiction": row["jurisdiction_name"] or "",
                "source": row["source"],
                "issue_date": parse_date(row["issued_date"]),
                "created_date": parse_date(row["created_date"]),
                "expired_date": parse_date(row["expired_date"]),
                "completed_date": parse_date(row["completed_date"]),
                "scraped_at": parse_date(row["scraped_at"]),
            })

        if batch:
            async with async_session_maker() as db:
                await db.execute(
                    text("""
                        INSERT INTO permits (
                            id, permit_number, original_id,
                            address, address_normalized, city, state, zip, lat, lng, parcel_id,
                            permit_type, work_type, trade, status, description,
                            owner_name, contractor_name, contractor_company, applicant_name,
                            jurisdiction, source,
                            issue_date, created_date, expired_date, completed_date, scraped_at
                        ) VALUES (
                            :id, :permit_number, :original_id,
                            :address, :address_normalized, :city, :state, :zip, :lat, :lng, :parcel_id,
                            :permit_type, :work_type, :trade, :status, :description,
                            :owner_name, :contractor_name, :contractor_company, :applicant_name,
                            :jurisdiction, :source,
                            :issue_date, :created_date, :expired_date, :completed_date, :scraped_at
                        )
                        ON CONFLICT DO NOTHING
                    """),
                    batch,
                )
                await db.commit()
                inserted += len(batch)

        processed += len(rows)
        if processed % 100000 == 0 or processed >= total:
            logger.info(
                f"Progress: {processed:,}/{total:,} ({processed*100//total}%) — "
                f"inserted: {inserted:,}, skipped: {skipped:,}"
            )

    conn.close()
    logger.info(f"ETL complete: {inserted:,} inserted, {skipped:,} skipped")


async def update_search_vectors():
    """Build tsvector search column after data load."""
    logger.info("Updating search vectors (this may take a while for large datasets)...")
    async with engine.begin() as conn:
        await conn.execute(text("""
            UPDATE permits SET search_vector = to_tsvector('english',
                coalesce(address, '') || ' ' ||
                coalesce(city, '') || ' ' ||
                coalesce(state, '') || ' ' ||
                coalesce(permit_number, '') || ' ' ||
                coalesce(owner_name, '') || ' ' ||
                coalesce(contractor_name, '')
            )
            WHERE search_vector IS NULL
        """))
    logger.info("Search vectors updated")


async def main(args):
    await ensure_extensions()

    # Create tables
    from app.database import Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Tables created")

    if not args.skip_jurisdictions:
        await load_jurisdictions(args.sqlite)

    await load_permits(args.sqlite, batch_size=args.batch_size, offset=args.offset)

    if not args.skip_vectors:
        await update_search_vectors()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL: SQLite → PostgreSQL for PermitLookup")
    parser.add_argument("--sqlite", required=True, help="Path to crm_permits.db")
    parser.add_argument("--batch-size", type=int, default=10000)
    parser.add_argument("--offset", type=int, default=0, help="Resume from offset")
    parser.add_argument("--skip-jurisdictions", action="store_true")
    parser.add_argument("--skip-vectors", action="store_true")
    args = parser.parse_args()

    asyncio.run(main(args))
