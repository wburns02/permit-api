"""Contractor license verification endpoints."""

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import select, func, or_, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.data_layers import ContractorLicense

router = APIRouter(prefix="/licenses", tags=["Contractor Licenses"])


@router.get("/verify")
async def verify_license(
    request: Request,
    name: str = Query(..., min_length=2, description="Contractor or business name"),
    state: str | None = Query(None, max_length=2, description="State abbreviation"),
    license_number: str | None = Query(None, description="License number"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Verify a contractor's license status. Returns license details, status,
    classifications, insurance, and bond information.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        from fastapi import HTTPException
        raise HTTPException(
            status_code=403,
            detail="License verification requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = []
    if license_number:
        conditions.append(ContractorLicense.license_number == license_number)
    else:
        conditions.append(
            or_(
                ContractorLicense.business_name.ilike(f"%{name}%"),
                ContractorLicense.full_business_name.ilike(f"%{name}%"),
            )
        )
    if state:
        conditions.append(ContractorLicense.state == state.upper())

    query = (
        select(ContractorLicense)
        .where(and_(*conditions))
        .order_by(ContractorLicense.business_name)
        .limit(25)
    )
    result = await db.execute(query)
    licenses = result.scalars().all()

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/licenses/verify",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "query": {"name": name, "state": state, "license_number": license_number},
        "results": [
            {
                "license_number": lic.license_number,
                "business_name": lic.business_name,
                "full_name": lic.full_business_name,
                "address": lic.address,
                "city": lic.city,
                "state": lic.state,
                "zip": lic.zip,
                "county": lic.county,
                "phone": lic.phone,
                "business_type": lic.business_type,
                "status": lic.status,
                "secondary_status": lic.secondary_status,
                "classifications": lic.classifications,
                "issue_date": lic.issue_date.isoformat() if lic.issue_date else None,
                "expiration_date": lic.expiration_date.isoformat() if lic.expiration_date else None,
                "workers_comp": {
                    "type": lic.workers_comp_type,
                    "company": lic.workers_comp_company,
                },
                "surety_bond": {
                    "company": lic.surety_company,
                    "amount": lic.surety_amount,
                },
                "source": lic.source,
                "last_updated": lic.last_updated.isoformat() if lic.last_updated else None,
            }
            for lic in licenses
        ],
        "total": len(licenses),
    }


@router.get("/search")
async def search_licenses(
    request: Request,
    state: str = Query(..., max_length=2, description="State abbreviation"),
    status: str | None = Query(None, description="License status filter (e.g., CLEAR, SUSPENDED)"),
    classification: str | None = Query(None, description="License classification filter"),
    city: str | None = Query(None, description="City filter"),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Search contractor licenses by state, status, classification, or city."""
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        from fastapi import HTTPException
        raise HTTPException(
            status_code=403,
            detail="License search requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = [ContractorLicense.state == state.upper()]
    if status:
        conditions.append(ContractorLicense.status.ilike(f"%{status}%"))
    if classification:
        conditions.append(ContractorLicense.classifications.ilike(f"%{classification}%"))
    if city:
        conditions.append(ContractorLicense.city.ilike(f"%{city}%"))

    where = and_(*conditions)

    query = (
        select(ContractorLicense)
        .where(where)
        .order_by(ContractorLicense.business_name)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(query)
    licenses = result.scalars().all()

    count_q = select(func.count()).select_from(ContractorLicense).where(where)
    total = (await db.execute(count_q)).scalar() or 0

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/licenses/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "results": [
            {
                "license_number": lic.license_number,
                "business_name": lic.business_name,
                "city": lic.city,
                "state": lic.state,
                "status": lic.status,
                "classifications": lic.classifications,
                "expiration_date": lic.expiration_date.isoformat() if lic.expiration_date else None,
                "source": lic.source,
            }
            for lic in licenses
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/stats")
async def license_stats(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Public endpoint — license database statistics."""
    total_q = select(func.count()).select_from(ContractorLicense)
    total = (await db.execute(total_q)).scalar() or 0

    states_q = select(
        ContractorLicense.state,
        func.count().label("count"),
    ).group_by(ContractorLicense.state).order_by(func.count().desc())
    states = (await db.execute(states_q)).all()

    status_q = select(
        ContractorLicense.status,
        func.count().label("count"),
    ).group_by(ContractorLicense.status).order_by(func.count().desc()).limit(10)
    statuses = (await db.execute(status_q)).all()

    return {
        "total_licenses": total,
        "states": {r.state: r.count for r in states},
        "status_breakdown": {r.status: r.count for r in statuses if r.status},
    }
