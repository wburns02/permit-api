"""API key authentication middleware."""

import hashlib
from datetime import datetime, timezone
from fastapi import Request, HTTPException
from fastapi.security import APIKeyHeader
from sqlalchemy import select, update
from app.database import primary_session_maker as async_session_maker
from app.models.api_key import ApiKey, ApiUser

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def hash_api_key(key: str) -> str:
    """Hash an API key for storage/lookup."""
    return hashlib.sha256(key.encode()).hexdigest()


async def get_current_user(request: Request) -> ApiUser | None:
    """Extract and validate API key from request, return user."""
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing API key. Include X-API-Key header.")

    key_hash = hash_api_key(api_key)

    async with async_session_maker() as db:
        result = await db.execute(
            select(ApiKey)
            .where(ApiKey.key_hash == key_hash, ApiKey.is_active.is_(True))
        )
        api_key_obj = result.scalar_one_or_none()

        if not api_key_obj:
            raise HTTPException(status_code=401, detail="Invalid API key.")

        # Load user
        user_result = await db.execute(
            select(ApiUser).where(ApiUser.id == api_key_obj.user_id, ApiUser.is_active.is_(True))
        )
        user = user_result.scalar_one_or_none()

        if not user:
            raise HTTPException(status_code=403, detail="Account disabled.")

        # Update last_used_at — fire and forget, don't block the response
        try:
            await db.execute(
                update(ApiKey)
                .where(ApiKey.id == api_key_obj.id)
                .values(last_used_at=datetime.now(timezone.utc))
            )
            await db.commit()
        except Exception:
            pass  # Non-critical — don't block requests for a timestamp update

        # Attach to request state for downstream use
        request.state.user = user
        request.state.api_key = api_key_obj
        return user
