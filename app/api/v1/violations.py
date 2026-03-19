"""Code violation intelligence endpoints."""

import math
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_, or_, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.data_layers import CodeViolation
from app.services.fast_counts import fast_count

router = APIRouter(prefix="/violations", tags=["Code Violations"])


@router.get("/search")
async def violation_search(
    request: Request,
    address: str | None = Query(None, min_length=3, description="Street address"),
    city: str | None = Query(None, description="City name"),
    state: str | None = Query(None, max_length=2, description="2-letter state code"),
    zip: str | None = Query(None, max_length=10),
    status: str | None = Query(None, description="Violation status: Open, Closed, Pending"),
    violation_type: str | None = Query(None, description="Violation class/type"),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Search code violations by address, city, state, status, and type.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Code violation data requires Pro Leads plan or higher."
        )

    if not any([address, city, state, zip]):
        raise HTTPException(
            status_code=400,
            detail="Provide at least one search parameter: address, city, state, or zip."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = []
    if address:
        conditions.append(CodeViolation.address.ilike(f"%{address}%"))
    if city:
        conditions.append(CodeViolation.city.ilike(f"%{city}%"))
    if state:
        conditions.append(CodeViolation.state == state.upper())
    if zip:
        conditions.append(CodeViolation.zip == zip)
    if status:
        conditions.append(CodeViolation.status.ilike(f"%{status}%"))
    if violation_type:
        conditions.append(CodeViolation.violation_type.ilike(f"%{violation_type}%"))

    offset = (page - 1) * limit

    # Count query
    count_q = select(func.count()).select_from(CodeViolation).where(and_(*conditions))
    total = (await db.execute(count_q)).scalar() or 0

    # Data query
    query = (
        select(CodeViolation)
        .where(and_(*conditions))
        .order_by(CodeViolation.violation_date.desc().nullslast())
        .offset(offset)
        .limit(limit)
    )
    result = await db.execute(query)
    violations = result.scalars().all()

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/violations/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "query": {
            "address": address, "city": city, "state": state,
            "zip": zip, "status": status, "violation_type": violation_type,
        },
        "results": [_serialize(v) for v in violations],
        "total": total,
        "page": page,
        "pages": math.ceil(total / limit) if total else 0,
    }


@router.get("/property")
async def property_violations(
    request: Request,
    address: str = Query(..., min_length=3, description="Property address"),
    state: str | None = Query(None, max_length=2, description="2-letter state code"),
    city: str | None = Query(None),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get all code violations for a specific property address.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Code violation data requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = [CodeViolation.address.ilike(f"%{address}%")]
    if state:
        conditions.append(CodeViolation.state == state.upper())
    if city:
        conditions.append(CodeViolation.city.ilike(f"%{city}%"))

    query = (
        select(CodeViolation)
        .where(and_(*conditions))
        .order_by(CodeViolation.violation_date.desc().nullslast())
        .limit(200)
    )
    result = await db.execute(query)
    violations = result.scalars().all()

    # Aggregate stats
    open_count = sum(1 for v in violations if v.status and v.status.lower() in ("open", "pending"))
    closed_count = sum(1 for v in violations if v.status and v.status.lower() == "closed")
    total_fines = sum(v.fine_amount for v in violations if v.fine_amount)

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/violations/property",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "query": {"address": address, "state": state, "city": city},
        "summary": {
            "total_violations": len(violations),
            "open": open_count,
            "closed": closed_count,
            "total_fines": round(total_fines, 2),
        },
        "results": [_serialize(v) for v in violations],
    }


@router.get("/stats")
async def violation_stats(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Public endpoint — code violation database statistics."""
    total = await fast_count(db, "code_violations")

    cities = (await db.execute(
        select(CodeViolation.city, func.count().label("count"))
        .group_by(CodeViolation.city)
        .order_by(func.count().desc())
        .limit(10)
    )).all()

    statuses = (await db.execute(
        select(CodeViolation.status, func.count().label("count"))
        .group_by(CodeViolation.status)
        .order_by(func.count().desc())
    )).all()

    sources = (await db.execute(
        select(CodeViolation.source, func.count().label("count"))
        .group_by(CodeViolation.source)
        .order_by(func.count().desc())
    )).all()

    return {
        "total_records": total,
        "cities": {r.city: r.count for r in cities if r.city},
        "statuses": {r.status: r.count for r in statuses if r.status},
        "sources": {r.source: r.count for r in sources if r.source},
    }


def _serialize(v: CodeViolation) -> dict:
    """Serialize a CodeViolation to a dict."""
    return {
        "violation_id": v.violation_id,
        "address": v.address,
        "city": v.city,
        "state": v.state,
        "zip": v.zip,
        "violation_type": v.violation_type,
        "violation_code": v.violation_code,
        "description": v.description,
        "status": v.status,
        "violation_date": v.violation_date.isoformat() if v.violation_date else None,
        "inspection_date": v.inspection_date.isoformat() if v.inspection_date else None,
        "resolution_date": v.resolution_date.isoformat() if v.resolution_date else None,
        "fine_amount": v.fine_amount,
        "lat": v.lat,
        "lng": v.lng,
        "source": v.source,
    }
