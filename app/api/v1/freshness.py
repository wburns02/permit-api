"""Data freshness health endpoints — monitor hot_leads staleness."""

from datetime import date

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_read_db

router = APIRouter(prefix="/freshness", tags=["Freshness"])


@router.get("/hot-leads")
async def hot_leads_freshness(db: AsyncSession = Depends(get_read_db)):
    """Show per-source freshness data for hot_leads."""
    result = await db.execute(text("""
        SELECT
            source,
            state,
            COUNT(*) as record_count,
            MAX(issue_date) as latest_date,
            MIN(issue_date) as oldest_date
        FROM hot_leads
        GROUP BY source, state
        ORDER BY record_count DESC
        LIMIT 50
    """))
    sources = []
    for row in result.fetchall():
        src, state, count, latest, oldest = row
        status = "fresh" if latest and (date.today() - latest).days <= 7 else "stale"
        sources.append({
            "source": src,
            "state": state,
            "records": count,
            "latest_date": str(latest) if latest else None,
            "oldest_date": str(oldest) if oldest else None,
            "status": status,
        })

    total = await db.execute(text("SELECT COUNT(*) FROM hot_leads"))
    total_count = total.scalar()

    stale = [s for s in sources if s["status"] == "stale"]

    return {
        "total_records": total_count,
        "source_count": len(sources),
        "stale_count": len(stale),
        "sources": sources,
        "stale_sources": [s["source"] for s in stale],
    }
