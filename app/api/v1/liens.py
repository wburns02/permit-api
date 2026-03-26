"""Property lien and judgment intelligence endpoints."""

import math
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_read_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, resolve_plan
from app.services.usage_logger import log_usage
from app.models.data_layers import PropertyLien
from app.services.fast_counts import fast_count

router = APIRouter(prefix="/liens", tags=["Liens & Judgments"])


@router.get("/search")
async def lien_search(
    request: Request,
    debtor: str | None = Query(None, min_length=2, description="Debtor name"),
    state: str | None = Query(None, max_length=2, description="2-letter state code"),
    lien_type: str | None = Query(None, description="Lien type: Tax Lien, UCC, Judgment, etc."),
    address: str | None = Query(None, min_length=3, description="Property address"),
    filing_number: str | None = Query(None, description="Filing/document number"),
    status: str | None = Query(None, description="Status: Active, Satisfied, Terminated"),
    page: int = Query(1, ge=1, le=20),
    limit: int = Query(25, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Search liens and judgments by debtor, state, lien type, address, or filing number.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Lien and judgment data requires Pro Leads plan or higher."
        )

    if not any([debtor, state, address, filing_number]):
        raise HTTPException(
            status_code=400,
            detail="Provide at least one search parameter: debtor, state, address, or filing_number."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = []
    if debtor:
        conditions.append(PropertyLien.debtor_name.ilike(f"%{debtor}%"))
    if state:
        conditions.append(PropertyLien.state == state.upper())
    if lien_type:
        conditions.append(PropertyLien.lien_type.ilike(f"%{lien_type}%"))
    if address:
        conditions.append(PropertyLien.address.ilike(f"%{address}%"))
    if filing_number:
        conditions.append(PropertyLien.filing_number == filing_number)
    if status:
        conditions.append(PropertyLien.status.ilike(f"%{status}%"))

    offset = (page - 1) * limit

    # Count query
    count_q = select(func.count()).select_from(PropertyLien).where(and_(*conditions))
    total = (await db.execute(count_q)).scalar() or 0

    # Data query
    query = (
        select(PropertyLien)
        .where(and_(*conditions))
        .order_by(PropertyLien.filing_date.desc().nullslast())
        .offset(offset)
        .limit(limit)
    )
    result = await db.execute(query)
    liens = result.scalars().all()

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/liens/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    return {
        "query": {
            "debtor": debtor, "state": state, "lien_type": lien_type,
            "address": address, "filing_number": filing_number, "status": status,
        },
        "results": [_serialize(l) for l in liens],
        "total": total,
        "page": page,
        "pages": math.ceil(total / limit) if total else 0,
    }


@router.get("/property")
async def property_liens(
    request: Request,
    address: str = Query(..., min_length=3, description="Property address"),
    state: str | None = Query(None, max_length=2, description="2-letter state code"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Get all liens for a specific property address.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Lien and judgment data requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = [PropertyLien.address.ilike(f"%{address}%")]
    if state:
        conditions.append(PropertyLien.state == state.upper())

    query = (
        select(PropertyLien)
        .where(and_(*conditions))
        .order_by(PropertyLien.filing_date.desc().nullslast())
        .limit(200)
    )
    result = await db.execute(query)
    liens = result.scalars().all()

    # Aggregate stats
    active_count = sum(1 for l in liens if l.status and l.status.lower() in ("active", "pending"))
    satisfied_count = sum(1 for l in liens if l.status and l.status.lower() in ("satisfied", "terminated", "released"))
    total_amount = sum(l.amount for l in liens if l.amount)

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/liens/property",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    return {
        "query": {"address": address, "state": state},
        "summary": {
            "total_liens": len(liens),
            "active": active_count,
            "satisfied": satisfied_count,
            "total_amount": round(total_amount, 2),
        },
        "results": [_serialize(l) for l in liens],
    }


@router.get("/stats")
async def lien_stats(
    request: Request,
    db: AsyncSession = Depends(get_read_db),
):
    """Public endpoint — lien and judgment database statistics."""
    total = await fast_count(db, "property_liens")

    states = (await db.execute(
        select(PropertyLien.state, func.count().label("count"))
        .group_by(PropertyLien.state)
        .order_by(func.count().desc())
    )).all()

    lien_types = (await db.execute(
        select(PropertyLien.lien_type, func.count().label("count"))
        .group_by(PropertyLien.lien_type)
        .order_by(func.count().desc())
        .limit(15)
    )).all()

    sources = (await db.execute(
        select(PropertyLien.source, func.count().label("count"))
        .group_by(PropertyLien.source)
        .order_by(func.count().desc())
    )).all()

    return {
        "total_records": total,
        "states": {r.state: r.count for r in states if r.state},
        "lien_types": {r.lien_type: r.count for r in lien_types if r.lien_type},
        "sources": {r.source: r.count for r in sources if r.source},
    }


def _serialize(l: PropertyLien) -> dict:
    """Serialize a PropertyLien to a dict."""
    return {
        "document_id": l.document_id,
        "lien_type": l.lien_type,
        "filing_number": l.filing_number,
        "address": l.address,
        "city": l.city,
        "state": l.state,
        "zip": l.zip,
        "borough": l.borough,
        "amount": l.amount,
        "filing_date": l.filing_date.isoformat() if l.filing_date else None,
        "lapse_date": l.lapse_date.isoformat() if l.lapse_date else None,
        "status": l.status,
        "debtor_name": l.debtor_name,
        "creditor_name": l.creditor_name,
        "description": l.description,
        "source": l.source,
    }
