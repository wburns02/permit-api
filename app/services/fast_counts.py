"""Fast approximate row counts using PostgreSQL reltuples.

For stats endpoints, COUNT(*) on tables with millions of rows is too slow
when accessed through a SOCKS proxy. PostgreSQL tracks approximate row
counts in pg_class.reltuples which is updated by ANALYZE and returns instantly.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def fast_count(db: AsyncSession, table_name: str) -> int:
    """Get approximate row count from pg_class.reltuples. Instant, no table scan."""
    result = await db.execute(
        text("SELECT reltuples::bigint FROM pg_class WHERE relname = :tbl"),
        {"tbl": table_name},
    )
    count = result.scalar()
    return max(int(count), 0) if count else 0
