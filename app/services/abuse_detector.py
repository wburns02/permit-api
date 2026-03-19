"""Abuse detection engine — scores API keys based on 6 behavioural signals.

Uses a rolling 1-hour window with in-memory storage (Redis-ready pattern
matching rate_limit.py / result_caps.py).

Key format: ``abuse:{user_id}``
"""

import logging
import time
import statistics
from datetime import datetime, timezone
from fastapi import Request

logger = logging.getLogger(__name__)

# In-memory store keyed by "abuse:{user_id}"
# Each value: {
#   "timestamps": [float],       # request epoch times
#   "pages": [int],              # page numbers accessed
#   "zips": set[str],            # unique ZIP codes queried
#   "states": set[str],          # unique states queried
#   "request_count": int,
#   "window_start": float,       # epoch of first request in window
# }
_memory_store: dict[str, dict] = {}

# How long a window lasts (seconds)
_WINDOW_SECONDS = 3600  # 1 hour

# Alerts cache — keys with score > 50 in last hour
_recent_alerts: dict[str, dict] = {}


def _get_or_create_bucket(user_id: str) -> dict:
    """Return the abuse-tracking bucket for a user, pruning stale data."""
    key = f"abuse:{user_id}"
    now = time.time()

    if key in _memory_store:
        bucket = _memory_store[key]
        # Prune if window has expired entirely
        if now - bucket["window_start"] > _WINDOW_SECONDS:
            bucket = _new_bucket(now)
            _memory_store[key] = bucket
        else:
            # Prune old timestamps
            cutoff = now - _WINDOW_SECONDS
            bucket["timestamps"] = [
                t for t in bucket["timestamps"] if t > cutoff
            ]
            bucket["request_count"] = len(bucket["timestamps"])
    else:
        bucket = _new_bucket(now)
        _memory_store[key] = bucket

    return bucket


def _new_bucket(now: float) -> dict:
    return {
        "timestamps": [],
        "pages": [],
        "zips": set(),
        "states": set(),
        "request_count": 0,
        "window_start": now,
    }


async def record_request(
    request: Request,
    page: int | None = None,
    zip_code: str | None = None,
    state: str | None = None,
) -> None:
    """Record a request and update all tracking signals for the user."""
    user = request.state.user
    user_id = str(user.id)
    now = time.time()

    bucket = _get_or_create_bucket(user_id)

    # Always record timestamp
    bucket["timestamps"].append(now)
    bucket["request_count"] = len(bucket["timestamps"])

    # Track page number if provided
    if page is not None:
        bucket["pages"].append(page)

    # Track geographic signals
    if zip_code:
        bucket["zips"].add(zip_code)
    if state:
        bucket["states"].add(state.upper())


def _detect_sequential_pages(pages: list[int]) -> bool:
    """Detect 3+ consecutive page numbers (e.g. 1,2,3,4,5)."""
    if len(pages) < 3:
        return False

    sorted_pages = sorted(set(pages))
    consecutive = 1
    for i in range(1, len(sorted_pages)):
        if sorted_pages[i] == sorted_pages[i - 1] + 1:
            consecutive += 1
            if consecutive >= 3:
                return True
        else:
            consecutive = 1
    return False


def _detect_bot_timing(timestamps: list[float]) -> bool:
    """Detect bot-like regularity: coefficient of variation < 0.1 with 10+ requests."""
    if len(timestamps) < 10:
        return False

    sorted_ts = sorted(timestamps)
    intervals = [
        sorted_ts[i] - sorted_ts[i - 1] for i in range(1, len(sorted_ts))
    ]

    if not intervals:
        return False

    mean = statistics.mean(intervals)
    if mean == 0:
        return True  # All requests at exact same time = definitely a bot

    stdev = statistics.stdev(intervals) if len(intervals) > 1 else 0.0
    cv = stdev / mean
    return cv < 0.1


def _calculate_utilization(request_count: int) -> float:
    """Calculate daily utilization percentage (rough estimate based on hourly rate)."""
    # Assume a reasonable daily limit baseline of 250 requests
    # Hourly rate * 24 vs daily limit
    hourly_rate = request_count
    projected_daily = hourly_rate * 24
    return min(100.0, (projected_daily / 250) * 100)


async def get_abuse_score(user_id: str) -> dict:
    """
    Calculate abuse score for a user based on 6 signals.

    Returns
    -------
    dict with keys:
        score   – integer 0-105 (sum of triggered signal weights)
        signals – dict of each signal's contribution
        level   – "normal" | "elevated" | "shadow" | "alert"
    """
    bucket = _get_or_create_bucket(user_id)

    signals = {}
    score = 0

    # 1. High utilization (>80%): +10
    utilization = _calculate_utilization(bucket["request_count"])
    if utilization > 80:
        signals["high_utilization"] = {
            "triggered": True,
            "value": round(utilization, 1),
            "points": 10,
        }
        score += 10
    else:
        signals["high_utilization"] = {
            "triggered": False,
            "value": round(utilization, 1),
            "points": 0,
        }

    # 2. Sequential pages (3+ consecutive): +20
    seq = _detect_sequential_pages(bucket["pages"])
    if seq:
        signals["sequential_pages"] = {
            "triggered": True,
            "value": sorted(set(bucket["pages"])),
            "points": 20,
        }
        score += 20
    else:
        signals["sequential_pages"] = {
            "triggered": False,
            "value": sorted(set(bucket["pages"])),
            "points": 0,
        }

    # 3. Fast requests (<1s avg, 10+ requests): +25
    timestamps = bucket["timestamps"]
    fast_requests = False
    avg_interval = None
    if len(timestamps) >= 10:
        sorted_ts = sorted(timestamps)
        intervals = [
            sorted_ts[i] - sorted_ts[i - 1] for i in range(1, len(sorted_ts))
        ]
        if intervals:
            avg_interval = statistics.mean(intervals)
            if avg_interval < 1.0:
                fast_requests = True

    if fast_requests:
        signals["fast_requests"] = {
            "triggered": True,
            "value": round(avg_interval, 3) if avg_interval else 0,
            "points": 25,
        }
        score += 25
    else:
        signals["fast_requests"] = {
            "triggered": False,
            "value": round(avg_interval, 3) if avg_interval else None,
            "points": 0,
        }

    # 4. Geographic sweep (>50 ZIPs/hr): +15
    zip_count = len(bucket["zips"])
    if zip_count > 50:
        signals["geographic_sweep"] = {
            "triggered": True,
            "value": zip_count,
            "points": 15,
        }
        score += 15
    else:
        signals["geographic_sweep"] = {
            "triggered": False,
            "value": zip_count,
            "points": 0,
        }

    # 5. State sweep (>10 states/hr): +15
    state_count = len(bucket["states"])
    if state_count > 10:
        signals["state_sweep"] = {
            "triggered": True,
            "value": state_count,
            "points": 15,
        }
        score += 15
    else:
        signals["state_sweep"] = {
            "triggered": False,
            "value": state_count,
            "points": 0,
        }

    # 6. Bot timing (CV < 0.1, 10+ requests): +20
    bot = _detect_bot_timing(timestamps)
    if bot:
        signals["bot_timing"] = {
            "triggered": True,
            "points": 20,
        }
        score += 20
    else:
        signals["bot_timing"] = {
            "triggered": False,
            "points": 0,
        }

    # Determine level
    if score <= 50:
        level = "normal"
    elif score <= 70:
        level = "elevated"
    elif score <= 90:
        level = "shadow"
    else:
        level = "alert"

    result = {
        "score": score,
        "signals": signals,
        "level": level,
    }

    # Track alerts
    if score > 50:
        _recent_alerts[user_id] = {
            "user_id": user_id,
            "score": score,
            "level": level,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "signals": {
                k: v for k, v in signals.items() if v.get("triggered")
            },
        }

    return result


async def get_recent_alerts() -> list:
    """Return keys with score > 50 in the last hour."""
    now = time.time()
    cutoff = datetime.fromtimestamp(now - _WINDOW_SECONDS, tz=timezone.utc)
    alerts = []

    for user_id, alert in list(_recent_alerts.items()):
        alert_time = datetime.fromisoformat(alert["timestamp"])
        if alert_time > cutoff:
            alerts.append(alert)
        else:
            # Prune stale alerts
            del _recent_alerts[user_id]

    return sorted(alerts, key=lambda a: a["score"], reverse=True)
