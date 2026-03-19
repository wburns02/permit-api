"""Response guard — applies all security layers to API responses."""

import hashlib
import json
from fastapi import Request

from app.middleware.result_caps import check_result_cap
from app.services.fingerprint import apply_fingerprint
from app.services.shadow_throttle import should_throttle, apply_throttle, throttle_delay
from app.services.abuse_detector import record_request, get_abuse_score


async def guard_response(
    request: Request,
    results: list[dict],
    page: int = None,
    zip_code: str = None,
    state: str = None,
) -> tuple[list[dict], dict]:
    """
    Apply all security layers to a response.
    Call this just before returning search results.
    Returns (modified_results, security_metadata).
    """
    api_key_id = str(request.state.api_key.id)
    user_id = str(request.state.user.id)

    # Layer 3: Record request for abuse detection
    await record_request(request, page=page, zip_code=zip_code, state=state)
    score_info = await get_abuse_score(user_id)
    abuse_score = score_info["score"]

    # Layer 4: Shadow-throttle if flagged
    throttle_config = should_throttle(abuse_score)
    if throttle_config["active"]:
        await throttle_delay(throttle_config)
        results = apply_throttle(results, throttle_config)

    # Layer 1: Result caps
    cap_info = await check_result_cap(request, len(results))
    if cap_info["capped"]:
        results = results[:cap_info["allowed"]]

    # Layer 2: Fingerprint
    results = apply_fingerprint(results, api_key_id)

    metadata = {
        "abuse_score": abuse_score,
        "abuse_level": score_info["level"],
        "capped": cap_info["capped"],
        "result_cap_used": cap_info["used_today"],
        "result_cap_limit": cap_info["daily_cap"],
    }

    return results, metadata
