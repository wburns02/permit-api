"""Daily result-cap tracking middleware.

Limits how many total records a user can download per day. Works with Redis
when available, falls back to an in-memory dict (same pattern as rate_limit.py).

Key format: ``resultcap:{user_id}:{YYYY-MM-DD}``
"""

import logging
from datetime import datetime, timezone

from fastapi import Request, HTTPException

from app.config import settings
from app.models.api_key import resolve_plan, PlanTier
from app.services.stripe_service import get_result_cap

logger = logging.getLogger(__name__)

# In-memory fallback when Redis unavailable
_memory_store: dict[str, dict] = {}

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None


async def _get_redis():
    if not settings.REDIS_URL or not aioredis:
        return None
    try:
        r = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        await r.ping()
        return r
    except Exception:
        return None


async def check_result_cap(request: Request, result_count: int) -> dict:
    """
    Check and increment the daily result cap for the current user.

    Parameters
    ----------
    request : Request
        Must have ``request.state.user`` set by the auth middleware.
    result_count : int
        Number of result rows about to be returned.

    Returns
    -------
    dict with keys:
        allowed  – number of rows the user may actually receive (may be < result_count)
        capped   – True if the response was truncated
        used_today – total rows consumed today *after* this call
        daily_cap – the user's daily cap
    """
    user = request.state.user
    user_id = str(user.id)
    plan = resolve_plan(user.plan)
    daily_cap = get_result_cap(plan)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"resultcap:{user_id}:{today}"

    redis = await _get_redis()

    if redis:
        try:
            current = await redis.get(key)
            current = int(current) if current else 0

            remaining = max(0, daily_cap - current)
            allowed = min(result_count, remaining)
            capped = allowed < result_count

            if allowed == 0 and plan == PlanTier.FREE:
                await redis.close()
                raise HTTPException(
                    status_code=429,
                    detail={
                        "error": "Daily result cap exceeded",
                        "daily_cap": daily_cap,
                        "used_today": current,
                        "plan": plan.value,
                        "upgrade_url": f"{settings.FRONTEND_URL}/pricing",
                    },
                )

            if allowed > 0:
                new_count = await redis.incrby(key, allowed)
                await redis.expire(key, 172800)  # 48h TTL
            else:
                new_count = current

            await redis.close()
        except HTTPException:
            raise
        except Exception:
            # Redis error — fall through to in-memory
            redis = None

    if redis is None:
        # In-memory fallback
        if key not in _memory_store:
            _memory_store[key] = {"count": 0}
        current = _memory_store[key]["count"]

        remaining = max(0, daily_cap - current)
        allowed = min(result_count, remaining)
        capped = allowed < result_count

        if allowed == 0 and plan == PlanTier.FREE:
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "Daily result cap exceeded",
                    "daily_cap": daily_cap,
                    "used_today": current,
                    "plan": plan.value,
                },
            )

        if allowed > 0:
            _memory_store[key]["count"] += allowed

        new_count = _memory_store[key]["count"]

    if capped:
        logger.warning(
            "User %s on %s plan hit result cap (%d/%d). Allowed %d of %d requested.",
            user_id, plan.value, new_count, daily_cap, allowed, result_count,
        )

    return {
        "allowed": allowed,
        "capped": capped,
        "used_today": new_count,
        "daily_cap": daily_cap,
    }
