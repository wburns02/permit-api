"""API key authentication middleware."""

import asyncio
import hashlib
import logging
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from fastapi import Request, HTTPException
from fastapi.security import APIKeyHeader
from sqlalchemy import select, update
from app.config import settings
from app.database import replica_session_maker, primary_session_maker
from app.models.api_key import ApiKey, ApiUser, PlanTier

logger = logging.getLogger(__name__)

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
INTERNAL_KEY_HEADER = "X-Internal-Key"

# Stable synthetic IDs for the internal service caller (used in usage_logs FK).
# These are NOT real rows in api_users / api_keys — log writes that target them
# will fail FK constraints, so the log_usage path is short-circuited for internal
# callers via the is_internal flag on request.state.
_INTERNAL_USER_ID = uuid.UUID("00000000-0000-0000-0000-00000000ace1")
_INTERNAL_KEY_ID = uuid.UUID("00000000-0000-0000-0000-00000000ace2")


def hash_api_key(key: str) -> str:
    """Hash an API key for storage/lookup."""
    return hashlib.sha256(key.encode()).hexdigest()


def _internal_user_stub() -> SimpleNamespace:
    """Return a synthetic enterprise-tier ApiUser for service-to-service calls."""
    return SimpleNamespace(
        id=_INTERNAL_USER_ID,
        email="internal@service.local",
        company_name="Mac CRM (internal)",
        plan=PlanTier.ENTERPRISE,
        stripe_customer_id=None,
        stripe_subscription_id=None,
        webhook_url=None,
        is_active=True,
    )


def _internal_key_stub() -> SimpleNamespace:
    return SimpleNamespace(
        id=_INTERNAL_KEY_ID,
        user_id=_INTERNAL_USER_ID,
        key_prefix="pl_intrnl",
        name="Internal service key",
        is_active=True,
    )


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
    """Extract and validate API key from request, return user.

    Short-circuit branch: when `X-Internal-Key` matches settings.INTERNAL_API_KEY
    (must be non-empty), the request is treated as a trusted service-to-service
    call from a sibling Railway service (e.g. Mac CRM react-crm-api). The
    request gets a synthetic enterprise-tier user — no DB lookup, no rate limit
    overrides, no usage_logs writes (request.state.is_internal flags downstream
    code to skip log_usage to avoid FK violations against the synthetic IDs).
    """
    internal_key_expected = (settings.INTERNAL_API_KEY or "").strip()
    internal_key_provided = request.headers.get(INTERNAL_KEY_HEADER, "").strip()
    if internal_key_expected and internal_key_provided and internal_key_provided == internal_key_expected:
        user = _internal_user_stub()
        request.state.user = user
        request.state.api_key = _internal_key_stub()
        request.state.is_internal = True
        return user  # type: ignore[return-value]

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
    request.state.is_internal = False
    return user
