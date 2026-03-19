"""Normalize contractor names and build aggregated contractors table."""

import asyncio
import re
import logging
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.config import settings
from app.models.permit import Permit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suffixes to strip for normalization
STRIP_SUFFIXES = re.compile(
    r'\b(LLC|INC|INCORPORATED|CORP|CORPORATION|CO|COMPANY|LTD|LIMITED|LP|LLP|PC|PLLC|DBA)\b\.?',
    re.IGNORECASE,
)


def normalize_contractor(name: str) -> str:
    """Normalize a contractor name for deduplication."""
    if not name:
        return ""
    n = name.strip().upper()
    n = STRIP_SUFFIXES.sub("", n)
    n = re.sub(r'[.,\-\'"]', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


async def main():
    engine = create_async_engine(settings.DATABASE_URL, pool_size=5)
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as db:
        # Create contractors table if not exists
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS contractors (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                name TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                permit_count INTEGER DEFAULT 0,
                avg_valuation FLOAT,
                active_states TEXT[],
                specialties TEXT[],
                first_active DATE,
                last_active DATE,
                UNIQUE(normalized_name)
            )
        """))
        await db.execute(text("CREATE INDEX IF NOT EXISTS ix_contractors_normalized ON contractors (normalized_name)"))
        await db.commit()

        # Aggregate from permits (T430 uses applicant_name, no valuation column)
        logger.info("Aggregating contractors from permits table...")

        query = (
            select(
                Permit.applicant_name.label("name"),
                func.count().label("permit_count"),
                func.array_agg(func.distinct(Permit.state)).label("active_states"),
                func.array_agg(func.distinct(Permit.permit_type)).label("specialties"),
                func.min(Permit.issue_date).label("first_active"),
                func.max(Permit.issue_date).label("last_active"),
            )
            .where(Permit.applicant_name.isnot(None))
            .group_by(Permit.applicant_name)
            .having(func.count() >= 2)  # Skip one-off entries
        )

        result = await db.execute(query)
        rows = result.all()
        logger.info("Found %d unique contractors with 2+ permits", len(rows))

        inserted = 0
        for r in rows:
            normalized = normalize_contractor(r.name)
            if not normalized:
                continue
            try:
                await db.execute(text("""
                    INSERT INTO contractors (name, normalized_name, permit_count, active_states, specialties, first_active, last_active)
                    VALUES (:name, :norm, :count, :states, :specs, :first, :last)
                    ON CONFLICT (normalized_name) DO UPDATE SET
                        permit_count = EXCLUDED.permit_count,
                        active_states = EXCLUDED.active_states,
                        specialties = EXCLUDED.specialties,
                        first_active = EXCLUDED.first_active,
                        last_active = EXCLUDED.last_active
                """), {
                    "name": r.name,
                    "norm": normalized,
                    "count": r.permit_count,
                    "states": [s for s in (r.active_states or []) if s],
                    "specs": [s for s in (r.specialties or []) if s],
                    "first": r.first_active,
                    "last": r.last_active,
                })
                inserted += 1
            except Exception as e:
                logger.warning("Failed to insert %s: %s", r.name, e)

            if inserted % 1000 == 0:
                await db.commit()

        await db.commit()
        logger.info("Done. Inserted/updated %d contractors", inserted)

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
