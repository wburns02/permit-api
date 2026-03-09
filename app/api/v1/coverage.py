"""Coverage and stats endpoints (public, no auth required)."""

from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.permit import Permit, Jurisdiction

router = APIRouter(tags=["Coverage"])


@router.get("/coverage")
async def get_coverage(db: AsyncSession = Depends(get_db)):
    """
    Get list of supported jurisdictions with record counts.

    No authentication required — used on the landing page.
    """
    result = await db.execute(
        select(Jurisdiction).order_by(Jurisdiction.record_count.desc())
    )
    jurisdictions = result.scalars().all()

    # Aggregate stats
    total_result = await db.execute(select(func.count()).select_from(Permit))
    total_records = total_result.scalar()

    state_result = await db.execute(
        select(Permit.state, func.count())
        .group_by(Permit.state)
        .order_by(func.count().desc())
    )
    states = {row[0]: row[1] for row in state_result.all()}

    return {
        "total_records": total_records,
        "total_jurisdictions": len(jurisdictions),
        "total_states": len(states),
        "states": states,
        "jurisdictions": [
            {
                "name": j.name,
                "state": j.state,
                "record_count": j.record_count,
                "source": j.source,
                "last_updated": j.last_updated.isoformat() if j.last_updated else None,
            }
            for j in jurisdictions
        ],
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
