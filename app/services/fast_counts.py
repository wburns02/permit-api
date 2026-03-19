"""Fast approximate row counts and safe grouped queries for PostgreSQL.

For stats endpoints, COUNT(*) and GROUP BY on tables with millions of rows
are too slow when accessed through a SOCKS proxy. This module provides:
- fast_count: instant approximate counts via pg_class.reltuples
- safe_grouped_query: GROUP BY with a statement_timeout fallback
"""

import asyncio
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


async def safe_query(db: AsyncSession, query, timeout_ms: int = 8000, fallback=None):
    """Execute a query with a statement timeout. Returns fallback on timeout."""
    try:
        await db.execute(text(f"SET LOCAL statement_timeout = '{timeout_ms}'"))
        result = await db.execute(query)
        return result.all()
    except Exception:
        await db.rollback()
        return fallback if fallback is not None else []
