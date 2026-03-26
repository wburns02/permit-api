"""API key authentication middleware."""

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from fastapi import Request, HTTPException
from fastapi.security import APIKeyHeader
from sqlalchemy import select, update
from app.database import replica_session_maker, primary_session_maker
from app.models.api_key import ApiKey, ApiUser

logger = logging.getLogger(__name__)

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def hash_api_key(key: str) -> str:
    """Hash an API key for storage/lookup."""
    return hashlib.sha256(key.encode()).hexdigest()


async def _update_last_used(key_id) -> None:
    """Fire-and-forget: update last_used_at on the primary. Never blocks auth."""
    try:
        async with primary_session_maker() as db:
            await db.execute(
                update(ApiKey)
                .where(ApiKey.id == key_id)
                .values(last_used_at=datetime.now(timezone.utc))
            )
            await db.commit()
    except Exception:
        pass  # Non-critical — don't block requests for a timestamp update


async def get_current_user(request: Request) -> ApiUser | None:
    """Extract and validate API key from request, return user."""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing API key. Include X-API-Key header.")

    key_hash = hash_api_key(api_key)

    # Read from REPLICA (fast, local on R730-2)
    async with replica_session_maker() as db:
        result = await db.execute(
            select(ApiKey)
            .where(ApiKey.key_hash == key_hash, ApiKey.is_active.is_(True))
        )
        api_key_obj = result.scalar_one_or_none()

        if not api_key_obj:
            raise HTTPException(status_code=401, detail="Invalid API key.")

        user_result = await db.execute(
            select(ApiUser).where(ApiUser.id == api_key_obj.user_id, ApiUser.is_active.is_(True))
        )
        user = user_result.scalar_one_or_none()

        if not user:
            raise HTTPException(status_code=403, detail="Account disabled.")

    # Fire-and-forget last_used_at update to primary — don't await, don't block
    asyncio.create_task(_update_last_used(api_key_obj.id))

    # Attach to request state for downstream use
    request.state.user = user
    request.state.api_key = api_key_obj
    return user
