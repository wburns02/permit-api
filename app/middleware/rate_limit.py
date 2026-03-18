"""Redis-based rate limiting middleware."""

import logging
from datetime import datetime, timezone
from fastapi import Request, HTTPException
from app.config import settings
from app.models.api_key import PlanTier, resolve_plan
from app.services.stripe_service import get_daily_limit

logger = logging.getLogger(__name__)

# In-memory fallback when Redis unavailable
_memory_store: dict[str, dict] = {}

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None


async def get_redis():
    if not settings.REDIS_URL or not aioredis:
        return None
    try:
        r = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        await r.ping()
        return r
    except Exception:
        return None


async def check_rate_limit(request: Request, lookup_count: int = 1) -> dict:
    """
    Check and increment rate limit for the current user.
    Returns usage info dict. Raises 429 if over limit.
    """
    user = request.state.user
    user_id = str(user.id)
    plan = resolve_plan(user.plan)
    daily_limit = get_daily_limit(plan)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"ratelimit:{user_id}:{today}"

    redis = await get_redis()

    if redis:
        current = await redis.get(key)
        current = int(current) if current else 0

        if current + lookup_count > daily_limit:
            if plan == PlanTier.FREE:
                raise HTTPException(
                    status_code=429,
                    detail={
                        "error": "Daily lookup limit exceeded",
                        "limit": daily_limit,
                        "used": current,
                        "plan": plan.value,
                        "upgrade_url": f"{settings.FRONTEND_URL}/pricing",
                    },
                )
            # Paid plans: allow overage but log it
            logger.warning(
                "User %s on %s plan exceeded daily limit (%d/%d)",
                user_id, plan.value, current + lookup_count, daily_limit,
            )

        new_count = await redis.incrby(key, lookup_count)
        await redis.expire(key, 172800)
        await redis.close()
    else:
        # In-memory fallback
        if key not in _memory_store:
            _memory_store[key] = {"count": 0}
        current = _memory_store[key]["count"]

        if current + lookup_count > daily_limit:
            if plan == PlanTier.FREE:
                raise HTTPException(
                    status_code=429,
                    detail={
                        "error": "Daily lookup limit exceeded",
                        "limit": daily_limit,
                        "used": current,
                        "plan": plan.value,
                    },
                )
            logger.warning(
                "User %s on %s plan exceeded daily limit (%d/%d)",
                user_id, plan.value, current + lookup_count, daily_limit,
            )

        _memory_store[key]["count"] += lookup_count
        new_count = _memory_store[key]["count"]

    return {
        "used_today": new_count,
        "daily_limit": daily_limit,
        "plan": plan.value,
        "overage": max(0, new_count - daily_limit),
    }
