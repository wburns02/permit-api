"""Contractor search and profile endpoints."""

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_, or_, case, extract
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, UsageLog
from app.models.permit import Permit

router = APIRouter(prefix="/contractors", tags=["Contractors"])


@router.get("/search")
async def search_contractors(
    request: Request,
    name: str = Query(..., min_length=2, description="Contractor name or company"),
    state: str | None = Query(None, max_length=2),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Search contractors by name or company. Returns aggregated contractor profiles
    with permit counts, active jurisdictions, and specialties.
    """
    await check_rate_limit(request, lookup_count=1)

    conditions = [
        or_(
            Permit.contractor_name.ilike(f"%{name}%"),
            Permit.contractor_company.ilike(f"%{name}%"),
        ),
        or_(
            Permit.contractor_name.is_not(None),
            Permit.contractor_company.is_not(None),
        ),
    ]
    if state:
        conditions.append(Permit.state == state.upper())

    where = and_(*conditions)

    # Aggregate by contractor_company (primary) or contractor_name
    contractor_key = func.coalesce(Permit.contractor_company, Permit.contractor_name)

    query = (
        select(
            contractor_key.label("contractor"),
            func.count().label("total_permits"),
            func.count(func.distinct(Permit.jurisdiction)).label("jurisdictions"),
            func.count(func.distinct(Permit.state)).label("states"),
            func.min(Permit.issue_date).label("first_permit"),
            func.max(Permit.issue_date).label("last_permit"),
            func.array_agg(func.distinct(Permit.permit_type)).label("permit_types"),
            func.array_agg(func.distinct(Permit.state)).label("active_states"),
        )
        .where(where)
        .group_by(contractor_key)
        .order_by(func.count().desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )

    result = await db.execute(query)
    rows = result.all()

    # Count total
    count_q = (
        select(func.count(func.distinct(contractor_key)))
        .where(where)
    )
    total = (await db.execute(count_q)).scalar() or 0

    # Log usage
    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/contractors/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "results": [
            {
                "contractor": r.contractor,
                "total_permits": r.total_permits,
                "jurisdictions": r.jurisdictions,
                "states": r.states,
                "first_permit": r.first_permit.isoformat() if r.first_permit else None,
                "last_permit": r.last_permit.isoformat() if r.last_permit else None,
                "permit_types": [t for t in (r.permit_types or []) if t],
                "active_states": [s for s in (r.active_states or []) if s],
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/{contractor_name}/permits")
async def contractor_permits(
    contractor_name: str,
    request: Request,
    state: str | None = Query(None, max_length=2),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get all permits for a specific contractor. Shows their complete work history.
    """
    await check_rate_limit(request, lookup_count=1)

    from app.services.search_service import PERMIT_COLUMNS, row_to_dict

    conditions = [
        or_(
            Permit.contractor_name.ilike(f"%{contractor_name}%"),
            Permit.contractor_company.ilike(f"%{contractor_name}%"),
        ),
    ]
    if state:
        conditions.append(Permit.state == state.upper())

    where = and_(*conditions)

    query = (
        select(*PERMIT_COLUMNS)
        .where(where)
        .order_by(Permit.issue_date.desc().nullslast())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )

    result = await db.execute(query)
    rows = result.all()

    total = 0
    if rows:
        if len(rows) < page_size:
            total = (page - 1) * page_size + len(rows)
        else:
            count_q = select(func.count()).select_from(Permit).where(where)
            total = (await db.execute(count_q)).scalar()

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/contractors/permits",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "contractor": contractor_name,
        "results": [row_to_dict(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
    }
