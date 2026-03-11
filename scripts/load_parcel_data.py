"""Load parcel/property records and link to permits."""

import asyncio
import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.config import settings
from app.services.search_service import normalize_address

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def ensure_properties_table(db: AsyncSession):
    """Create properties table if not exists."""
    await db.execute(text("""
        CREATE TABLE IF NOT EXISTS properties (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            address_normalized TEXT NOT NULL UNIQUE,
            parcel_id TEXT,
            lat FLOAT,
            lng FLOAT,
            permit_count INTEGER DEFAULT 0,
            last_permit_date DATE,
            city TEXT,
            state TEXT,
            zip TEXT
        )
    """))
    await db.execute(text("CREATE INDEX IF NOT EXISTS ix_properties_addr ON properties (address_normalized)"))
    await db.execute(text("CREATE INDEX IF NOT EXISTS ix_properties_parcel ON properties (parcel_id)"))
    await db.execute(text("CREATE INDEX IF NOT EXISTS ix_properties_geo ON properties (lat, lng)"))
    await db.commit()


async def link_permits_to_properties(db: AsyncSession):
    """Aggregate permits into properties table."""
    logger.info("Aggregating permits into properties...")
    await db.execute(text("""
        INSERT INTO properties (address_normalized, city, state, zip, lat, lng, permit_count, last_permit_date)
        SELECT
            address_normalized,
            mode() WITHIN GROUP (ORDER BY city) as city,
            mode() WITHIN GROUP (ORDER BY state) as state,
            mode() WITHIN GROUP (ORDER BY zip) as zip,
            AVG(lat) as lat,
            AVG(lng) as lng,
            COUNT(*) as permit_count,
            MAX(issue_date) as last_permit_date
        FROM permits
        WHERE address_normalized IS NOT NULL
        GROUP BY address_normalized
        ON CONFLICT (address_normalized) DO UPDATE SET
            permit_count = EXCLUDED.permit_count,
            last_permit_date = EXCLUDED.last_permit_date,
            lat = COALESCE(EXCLUDED.lat, properties.lat),
            lng = COALESCE(EXCLUDED.lng, properties.lng)
    """))
    await db.commit()

    result = await db.execute(text("SELECT COUNT(*) FROM properties"))
    count = result.scalar()
    logger.info("Properties table now has %d records", count)


async def main():
    engine = create_async_engine(settings.DATABASE_URL, pool_size=5)
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as db:
        await ensure_properties_table(db)
        await link_permits_to_properties(db)

    await engine.dispose()
    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
