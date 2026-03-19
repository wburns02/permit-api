"""Coverage and stats endpoints (public, no auth required)."""

from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.permit import Permit, Jurisdiction
from app.services.search_service import get_coverage
from app.services.fast_counts import fast_count

router = APIRouter(tags=["Coverage"])


@router.get("/coverage")
async def coverage(db: AsyncSession = Depends(get_db)):
    """
    Get list of supported jurisdictions with record counts.

    No authentication required — used on the landing page.
    """
    jurisdictions = await get_coverage(db)

    total_records = await fast_count(db, "permits")

    # State counts from jurisdictions table (much smaller than permits)
    from app.services.fast_counts import safe_query
    state_rows = await safe_query(db,
        select(Jurisdiction.state, func.sum(Jurisdiction.record_count).label("count"))
        .group_by(Jurisdiction.state)
        .order_by(func.sum(Jurisdiction.record_count).desc())
    )
    states = {row.state: row.count for row in state_rows if row.state}

    return {
        "total_records": total_records,
        "total_jurisdictions": len(jurisdictions),
        "total_states": len(states),
        "states": states,
        "jurisdictions": jurisdictions,
    }


@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Quick stats for the landing page hero section.

    Now connected directly to T430 via Tailscale — uses fast reltuples
    for permit count, and known constants for jurisdictions/states.
    """
    total_permits = await fast_count(db, "permits")

    # Jurisdiction and state counts are relatively stable — use fast estimates
    total_jurisdictions = await fast_count(db, "jurisdictions")
    if total_jurisdictions == 0:
        total_jurisdictions = 3143  # fallback

    # States: 50 US + DC + 3 Canadian provinces
    total_states = 54

    return {
        "total_permits": total_permits,
        "total_jurisdictions": total_jurisdictions,
        "total_states": total_states,
    }
