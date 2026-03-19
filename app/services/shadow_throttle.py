"""Shadow throttle — silently degrades API responses for flagged keys.

Instead of hard-blocking abusive API keys (which tips them off), this module
applies invisible degradation: added latency, fewer results, stripped fields,
and stale-only data.
"""

import asyncio
import logging
from copy import deepcopy

logger = logging.getLogger(__name__)

# Fields stripped from results at elevated / alert levels
_STRIP_FIELDS = [
    "description",
    "contractor_name",
    "contractor_company",
    "lat",
    "lng",
]


def should_throttle(abuse_score: int) -> dict:
    """
    Determine throttle configuration based on abuse score.

    Parameters
    ----------
    abuse_score : int
        The current abuse score for the API key (0-105).

    Returns
    -------
    dict with keys:
        active        – bool, whether throttling is active
        delay_seconds – float, artificial delay to inject
        max_results   – int, maximum result rows to return
        strip_fields  – list[str], field names to remove from results
        force_cold    – bool, only return old/stale records
    """
    if abuse_score <= 50:
        return {"active": False}

    if abuse_score <= 70:
        return {
            "active": True,
            "delay_seconds": 2,
            "max_results": 10,
            "strip_fields": list(_STRIP_FIELDS),
            "force_cold": True,
        }

    # 71+
    return {
        "active": True,
        "delay_seconds": 5,
        "max_results": 10,
        "strip_fields": list(_STRIP_FIELDS),
        "force_cold": True,
    }


def apply_throttle(results: list[dict], config: dict) -> list[dict]:
    """
    Apply throttle degradation to a list of result dicts.

    - Truncates to ``max_results``
    - Strips fields listed in ``strip_fields``
    - (force_cold is handled by the caller at query time)

    Parameters
    ----------
    results : list[dict]
        The original result rows.
    config : dict
        Output of ``should_throttle()``.

    Returns
    -------
    list[dict] — degraded results (deep-copied to avoid mutation).
    """
    if not config.get("active"):
        return results

    max_results = config.get("max_results", len(results))
    strip_fields = config.get("strip_fields", [])

    truncated = results[:max_results]
    degraded = []

    for row in truncated:
        row_copy = deepcopy(row)
        for field in strip_fields:
            row_copy.pop(field, None)
        degraded.append(row_copy)

    if len(results) > max_results:
        logger.info(
            "Shadow throttle truncated %d results to %d",
            len(results),
            max_results,
        )

    return degraded


async def throttle_delay(config: dict) -> None:
    """
    Inject artificial delay if throttling is active.

    Parameters
    ----------
    config : dict
        Output of ``should_throttle()``.
    """
    if not config.get("active"):
        return

    delay = config.get("delay_seconds", 0)
    if delay > 0:
        logger.debug("Shadow throttle injecting %.1fs delay", delay)
        await asyncio.sleep(delay)
