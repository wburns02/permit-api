"""Fire-and-forget usage logging that writes to the PRIMARY database.

All read endpoints use get_read_db (replica). Usage logs are writes,
so they must go to the primary. This service handles that asynchronously
without blocking the API response.
"""

import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

from app.database import primary_session_maker
from app.models.api_key import UsageLog

logger = logging.getLogger(__name__)


async def _write_usage_log(
    user_id: UUID,
    api_key_id: UUID,
    endpoint: str,
    lookup_count: int,
    ip_address: str | None,
    result_count: int | None = None,
    response_bytes: int | None = None,
    query_hash: str | None = None,
    abuse_score: int | None = None,
) -> None:
    """Write a usage log entry to the primary database."""
    try:
        async with primary_session_maker() as db:
            log = UsageLog(
                user_id=user_id,
                api_key_id=api_key_id,
                endpoint=endpoint,
                lookup_count=lookup_count,
                ip_address=ip_address,
                result_count=result_count,
                response_bytes=response_bytes,
                query_hash=query_hash,
                abuse_score=abuse_score,
                created_at=datetime.now(timezone.utc),
            )
            db.add(log)
            await db.commit()
    except Exception as e:
        logger.warning("Usage log write failed (non-critical): %s", e)


def log_usage(
    user_id: UUID,
    api_key_id: UUID,
    endpoint: str,
    lookup_count: int = 1,
    ip_address: str | None = None,
    result_count: int | None = None,
    response_bytes: int | None = None,
    query_hash: str | None = None,
    abuse_score: int | None = None,
) -> None:
    """Fire-and-forget: schedule usage log write to primary. Never blocks."""
    asyncio.create_task(_write_usage_log(
        user_id=user_id,
        api_key_id=api_key_id,
        endpoint=endpoint,
        lookup_count=lookup_count,
        ip_address=ip_address,
        result_count=result_count,
        response_bytes=response_bytes,
        query_hash=query_hash,
        abuse_score=abuse_score,
    ))
