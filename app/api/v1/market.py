"""Market intelligence endpoints for real estate and PropTech."""

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_, extract, case, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.permit import Permit

router = APIRouter(prefix="/market", tags=["Market Intelligence"])


def _require_pro(user: ApiUser):
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER):
        raise HTTPException(status_code=403, detail="Market intelligence requires a Pro Leads plan or higher.")


@router.get("/activity")
async def market_activity(
    request: Request,
    zip: str | None = Query(None, description="ZIP code"),
    city: str | None = Query(None),
    state: str | None = Query(None, max_length=2),
    months: int = Query(6, ge=1, le=24, description="Months of history"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Monthly permit volume, top contractors, avg valuation, type breakdown for an area."""
    _require_pro(user)
    await check_rate_limit(request, lookup_count=1)

    if not any([zip, city, state]):
        raise HTTPException(status_code=400, detail="Provide at least one of: zip, city, state")

    conditions = [
        Permit.issue_date.isnot(None),
        Permit.issue_date >= text(f"CURRENT_DATE - INTERVAL '{months} months'"),
    ]
    if zip:
        conditions.append(Permit.zip == zip)
    if city:
        conditions.append(Permit.city.ilike(city))
    if state:
        conditions.append(Permit.state.ilike(state))

    where = and_(*conditions)

    # Monthly volume
    monthly_q = (
        select(
            extract("year", Permit.issue_date).label("year"),
            extract("month", Permit.issue_date).label("month"),
            func.count().label("permit_count"),
            func.avg(Permit.valuation).label("avg_valuation"),
        )
        .where(where)
        .group_by("year", "month")
        .order_by("year", "month")
    )
    monthly_rows = (await db.execute(monthly_q)).all()

    # Top contractors
    contractor_key = func.coalesce(Permit.contractor_company, Permit.contractor_name)
    top_contractors_q = (
        select(
            contractor_key.label("contractor"),
            func.count().label("permits"),
        )
        .where(and_(where, contractor_key.isnot(None)))
        .group_by(contractor_key)
        .order_by(func.count().desc())
        .limit(10)
    )
    contractor_rows = (await db.execute(top_contractors_q)).all()

    # Type breakdown
    type_q = (
        select(Permit.permit_type, func.count().label("count"))
        .where(and_(where, Permit.permit_type.isnot(None)))
        .group_by(Permit.permit_type)
        .order_by(func.count().desc())
    )
    type_rows = (await db.execute(type_q)).all()

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/market/activity",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "monthly_volume": [
            {
                "year": int(r.year),
                "month": int(r.month),
                "permit_count": r.permit_count,
                "avg_valuation": round(r.avg_valuation, 2) if r.avg_valuation else None,
            }
            for r in monthly_rows
        ],
        "top_contractors": [
            {"contractor": r.contractor, "permits": r.permits}
            for r in contractor_rows
        ],
        "permit_type_breakdown": {r.permit_type: r.count for r in type_rows},
    }


@router.get("/hotspots")
async def market_hotspots(
    request: Request,
    state: str = Query(..., max_length=2, description="State code"),
    months: int = Query(6, ge=1, le=24),
    min_permits: int = Query(50, ge=1),
    limit: int = Query(25, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """ZIP codes ranked by permit activity and growth rate."""
    _require_pro(user)
    await check_rate_limit(request, lookup_count=1)

    # Recent period
    recent_q = (
        select(
            Permit.zip,
            func.count().label("recent_count"),
            func.avg(Permit.valuation).label("avg_valuation"),
        )
        .where(and_(
            Permit.state.ilike(state),
            Permit.zip.isnot(None),
            Permit.issue_date.isnot(None),
            Permit.issue_date >= text(f"CURRENT_DATE - INTERVAL '{months} months'"),
        ))
        .group_by(Permit.zip)
        .having(func.count() >= min_permits)
        .order_by(func.count().desc())
        .limit(limit)
    )
    recent_rows = (await db.execute(recent_q)).all()

    # Get prior period counts for growth calculation
    zips = [r.zip for r in recent_rows]
    growth_data = {}
    if zips:
        prior_q = (
            select(
                Permit.zip,
                func.count().label("prior_count"),
            )
            .where(and_(
                Permit.state.ilike(state),
                Permit.zip.in_(zips),
                Permit.issue_date.isnot(None),
                Permit.issue_date >= text(f"CURRENT_DATE - INTERVAL '{months * 2} months'"),
                Permit.issue_date < text(f"CURRENT_DATE - INTERVAL '{months} months'"),
            ))
            .group_by(Permit.zip)
        )
        prior_rows = (await db.execute(prior_q)).all()
        growth_data = {r.zip: r.prior_count for r in prior_rows}

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/market/hotspots",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    results = []
    for r in recent_rows:
        prior = growth_data.get(r.zip, 0)
        growth_pct = None
        if prior > 0:
            growth_pct = round(((r.recent_count - prior) / prior) * 100, 1)
        results.append({
            "zip": r.zip,
            "permit_count": r.recent_count,
            "avg_valuation": round(r.avg_valuation, 2) if r.avg_valuation else None,
            "prior_period_count": prior,
            "growth_pct": growth_pct,
        })

    return {"state": state.upper(), "months": months, "hotspots": results}
