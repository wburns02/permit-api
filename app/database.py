"""Async SQLAlchemy database setup with read/write splitting.

Primary engine (T430) handles all writes: signup, login, CRM CRUD, call logging.
Replica engine (R730-2) handles all reads: search, stats, analyst, trends.
Falls back to primary if REPLICA_DATABASE_URL is not configured.
"""

import logging
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase
from app.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Primary engine (T430) — all writes go here
# ---------------------------------------------------------------------------
# Server-side PG settings applied at every connection startup.
# `idle_in_transaction_session_timeout`: kills any session that holds a
# transaction idle for >60s. Stops the runaway leak where a list-query
# timeout leaves an asyncpg conn pinned in idle-in-transaction, eventually
# exhausting the pool and triggering the watchdog SIGTERM cycle.
# `lock_timeout`: bounds how long any DDL or write waits for a lock.
_PG_SERVER_SETTINGS = {
    "idle_in_transaction_session_timeout": "3000",   # 3s, in ms
    "lock_timeout": "30000",                         # 30s, in ms
    "statement_timeout": "20000",                    # 20s, in ms
}

primary_engine = create_async_engine(
    settings.DATABASE_URL,
    pool_size=30,
    max_overflow=20,
    pool_recycle=3600,
    pool_pre_ping=True,
    pool_timeout=10,   # fail fast instead of hanging 30s when pool exhausted
    connect_args={"server_settings": _PG_SERVER_SETTINGS},
    echo=settings.DEBUG and not settings.is_production,
)

primary_session_maker = async_sessionmaker(
    primary_engine, class_=AsyncSession, expire_on_commit=False
)

# ---------------------------------------------------------------------------
# Replica engine (R730-2) — all reads go here; falls back to primary
# ---------------------------------------------------------------------------
_replica_url = settings.REPLICA_DATABASE_URL or settings.DATABASE_URL
_replica_is_separate = _replica_url != settings.DATABASE_URL

if _replica_is_separate:
    replica_engine = create_async_engine(
        _replica_url,
        pool_size=30,       # reads are heavier, give more connections
        max_overflow=20,
        pool_recycle=3600,
        pool_pre_ping=True,
        pool_timeout=10,
        connect_args={"server_settings": _PG_SERVER_SETTINGS},
        echo=settings.DEBUG and not settings.is_production,
    )
    logger.info("Read replica configured: reads will go to replica")
else:
    replica_engine = primary_engine
    logger.info("No REPLICA_DATABASE_URL set: all queries go to primary")

replica_session_maker = async_sessionmaker(
    replica_engine, class_=AsyncSession, expire_on_commit=False
)

# ---------------------------------------------------------------------------
# Backward compatibility aliases
# ---------------------------------------------------------------------------
engine = primary_engine
async_session_maker = primary_session_maker


class Base(DeclarativeBase):
    pass


async def get_db():
    """Primary (write) session — used for endpoints that mutate data."""
    async with primary_session_maker() as session:
        try:
            yield session
        finally:
            await session.close()


async def get_read_db():
    """Read-only session — uses primary unless REPLICA_DATABASE_URL is set.

    Simplified: async with handles cleanup. Previous explicit rollback+close
    wrapped in wait_for was deadlocking DB-heavy read endpoints.
    """
    async with primary_session_maker() as session:
        yield session


async def init_db():
    async with primary_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
