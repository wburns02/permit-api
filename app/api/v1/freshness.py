"""Data freshness health endpoints — monitor hot_leads staleness."""

import logging
from datetime import date

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_read_db
from app.services.endpoint_cache import freshness_cache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/freshness", tags=["Freshness"])


# Statement timeout for the per-source GROUP BY. The full scan over
# hot_leads (~15M rows) takes ~80s on a cold cache; we allow up to 90s
# so the background-refresh path can actually complete, then serve the
# result from in-process cache for 5 minutes.
_GROUP_BY_TIMEOUT_MS = 90_000


async def _compute_hot_leads_freshness(db: AsyncSession) -> dict:
    """Refresh path — only called when the TTL cache is cold or stale."""
    # Approximate total via pg_class.reltuples. Refreshes after each
    # VACUUM/ANALYZE; effectively instant. Avoids a 30s+ COUNT(*) scan
    # that was causing the endpoint to 500.
    total_result = await db.execute(
        text("SELECT reltuples::bigint AS approx FROM pg_class WHERE relname = 'hot_leads'")
    )
    total_count = int(total_result.scalar() or 0)

    sources: list[dict] = []
    timed_out = False
    try:
        # SET LOCAL applies for the rest of this transaction only.
        await db.execute(text(f"SET LOCAL statement_timeout = '{_GROUP_BY_TIMEOUT_MS}'"))
        result = await db.execute(text("""
            SELECT
                source,
                state,
                COUNT(*) AS record_count,
                MAX(issue_date) AS latest_date,
                MIN(issue_date) AS oldest_date
            FROM hot_leads
            GROUP BY source, state
            ORDER BY record_count DESC
            LIMIT 50
        """))
        for src, state, count, latest, oldest in result.fetchall():
            status = "fresh" if latest and (date.today() - latest).days <= 7 else "stale"
            sources.append({
                "source": src,
                "state": state,
                "records": count,
                "latest_date": str(latest) if latest else None,
                "oldest_date": str(oldest) if oldest else None,
                "status": status,
            })
    except Exception as e:
        # statement_timeout, connection drop, etc. — return partial data
        # (totals are valid) rather than 500.
        timed_out = True
        logger.warning("hot_leads GROUP BY failed/timed out: %s", e)
        try:
            await db.rollback()
        except Exception:
            pass

    stale = [s for s in sources if s["status"] == "stale"]

    return {
        "total_records": total_count,
        "approximate_total": True,
        "source_count": len(sources),
        "stale_count": len(stale),
        "sources": sources,
        "stale_sources": [s["source"] for s in stale],
        "group_by_timed_out": timed_out,
    }


@router.get("/hot-leads")
async def hot_leads_freshness(db: AsyncSession = Depends(get_read_db)):
    """Show per-source freshness data for hot_leads.

    Cached for 5 minutes in-process. The first call after a cold start
    can take up to 90 seconds because of the GROUP BY scan over 15M rows;
    subsequent calls within the TTL are instant.
    """
    return await freshness_cache.get_or_set(
        "hot_leads_freshness",
        lambda: _compute_hot_leads_freshness(db),
    )
