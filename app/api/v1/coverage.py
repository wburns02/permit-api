"""Coverage and stats endpoints (public, no auth required)."""

from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.permit import Permit, Jurisdiction
from app.services.search_service import get_coverage

router = APIRouter(tags=["Coverage"])

# Tailscale userspace TCP response size limit workaround
_SMALL_BATCH = 5


@router.get("/coverage")
async def coverage(db: AsyncSession = Depends(get_db)):
    """
    Get list of supported jurisdictions with record counts.

    No authentication required — used on the landing page.
    """
    jurisdictions = await get_coverage(db)

    # Aggregate stats (single-value queries, always under size limit)
    total_result = await db.execute(select(func.count()).select_from(Permit))
    total_records = total_result.scalar()

    # Get state counts in batches
    states = {}
    offset = 0
    while True:
        q = (
            select(Permit.state, func.count())
            .group_by(Permit.state)
            .order_by(func.count().desc())
            .offset(offset)
            .limit(_SMALL_BATCH)
        )
        batch = (await db.execute(q)).all()
        for row in batch:
            states[row[0]] = row[1]
        if len(batch) < _SMALL_BATCH:
            break
        offset += _SMALL_BATCH

    return {
        "total_records": total_records,
        "total_jurisdictions": len(jurisdictions),
        "total_states": len(states),
        "states": states,
        "jurisdictions": jurisdictions,
    }


@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Quick stats for the landing page hero section."""
    total = await db.execute(select(func.count()).select_from(Permit))
    jurisdictions = await db.execute(select(func.count()).select_from(Jurisdiction))
    states = await db.execute(
        select(func.count(func.distinct(Permit.state)))
    )

    return {
        "total_permits": total.scalar(),
        "total_jurisdictions": jurisdictions.scalar(),
        "total_states": states.scalar(),
    }
