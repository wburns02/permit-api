"""Business entity / LLC registration endpoints."""

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_read_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, resolve_plan
from app.services.usage_logger import log_usage
from app.models.data_layers import BusinessEntity
from app.services.fast_counts import fast_count
from app.services.response_guard import guard_response

router = APIRouter(prefix="/entities", tags=["Business Entities"])


@router.get("/search")
async def search_entities(
    request: Request,
    name: str = Query(..., min_length=2, description="Entity name (LLC, Corp, etc.)"),
    state: str | None = Query(None, max_length=2, description="State abbreviation"),
    entity_type: str | None = Query(None, description="Entity type filter (LLC, Corporation, etc.)"),
    status: str | None = Query(None, description="Status filter (Active, Inactive, Dissolved)"),
    page: int = Query(1, ge=1, le=20),
    page_size: int = Query(25, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Search business entities (LLCs, Corporations, LPs) by name.
    Returns entity details, registered agent, officers, and filing info.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Business entity search requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = [BusinessEntity.entity_name.ilike(f"%{name}%")]
    if state:
        conditions.append(BusinessEntity.state == state.upper())
    if entity_type:
        conditions.append(BusinessEntity.entity_type.ilike(f"%{entity_type}%"))
    if status:
        conditions.append(BusinessEntity.status.ilike(f"%{status}%"))

    where = and_(*conditions)

    query = (
        select(BusinessEntity)
        .where(where)
        .order_by(BusinessEntity.entity_name)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(query)
    entities = result.scalars().all()

    count_q = select(func.count()).select_from(BusinessEntity).where(where)
    total = (await db.execute(count_q)).scalar() or 0

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/entities/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    results_list = [_entity_dict(e) for e in entities]

    # Apply security layers
    guarded_results, sec_meta = await guard_response(request, results_list, page=page, state=state)

    return {
        "query": {"name": name, "state": state, "entity_type": entity_type},
        "results": guarded_results,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/lookup")
async def lookup_entity(
    request: Request,
    filing_number: str = Query(..., description="Filing/document number"),
    state: str = Query(..., max_length=2, description="State"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """Look up a specific business entity by filing number and state."""
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Business entity lookup requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    query = (
        select(BusinessEntity)
        .where(
            and_(
                BusinessEntity.filing_number == filing_number,
                BusinessEntity.state == state.upper(),
            )
        )
    )
    result = await db.execute(query)
    entity = result.scalar_one_or_none()

    if not entity:
        raise HTTPException(status_code=404, detail="Entity not found.")

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/entities/lookup",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    return _entity_dict(entity)


@router.get("/by-agent")
async def search_by_registered_agent(
    request: Request,
    agent_name: str = Query(..., min_length=2, description="Registered agent name"),
    state: str | None = Query(None, max_length=2),
    page: int = Query(1, ge=1, le=20),
    page_size: int = Query(25, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Find all entities with a specific registered agent.
    Useful for finding all LLCs owned by the same person/company.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Registered agent search requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = [BusinessEntity.registered_agent_name.ilike(f"%{agent_name}%")]
    if state:
        conditions.append(BusinessEntity.state == state.upper())

    where = and_(*conditions)

    query = (
        select(BusinessEntity)
        .where(where)
        .order_by(BusinessEntity.entity_name)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(query)
    entities = result.scalars().all()

    count_q = select(func.count()).select_from(BusinessEntity).where(where)
    total = (await db.execute(count_q)).scalar() or 0

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/entities/by-agent",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    return {
        "query": {"agent_name": agent_name, "state": state},
        "results": [_entity_dict(e) for e in entities],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/stats")
async def entity_stats(
    request: Request,
    db: AsyncSession = Depends(get_read_db),
):
    """Public endpoint — business entity database statistics."""
    total = await fast_count(db, "business_entities")

    states = (await db.execute(
        select(BusinessEntity.state, func.count().label("count"))
        .group_by(BusinessEntity.state)
        .order_by(func.count().desc())
        .limit(15)
    )).all()

    types = (await db.execute(
        select(BusinessEntity.entity_type, func.count().label("count"))
        .group_by(BusinessEntity.entity_type)
        .order_by(func.count().desc())
        .limit(10)
    )).all()

    statuses = (await db.execute(
        select(BusinessEntity.status, func.count().label("count"))
        .group_by(BusinessEntity.status)
        .order_by(func.count().desc())
        .limit(10)
    )).all()

    return {
        "total_entities": total,
        "states": {r.state: r.count for r in states},
        "entity_types": {r.entity_type: r.count for r in types if r.entity_type},
        "status_breakdown": {r.status: r.count for r in statuses if r.status},
    }


def _entity_dict(e: BusinessEntity) -> dict:
    return {
        "entity_name": e.entity_name,
        "entity_type": e.entity_type,
        "state": e.state,
        "filing_number": e.filing_number,
        "status": e.status,
        "formation_date": e.formation_date.isoformat() if e.formation_date else None,
        "dissolution_date": e.dissolution_date.isoformat() if e.dissolution_date else None,
        "registered_agent": {
            "name": e.registered_agent_name,
            "address": e.registered_agent_address,
        },
        "principal_address": e.principal_address,
        "mailing_address": e.mailing_address,
        "officers": e.officers,
        "source": e.source,
    }
