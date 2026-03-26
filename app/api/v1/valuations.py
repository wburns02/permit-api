"""Property valuation endpoints — Redfin market data by ZIP."""

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_read_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.data_layers import PropertyValuation
from app.services.fast_counts import fast_count, safe_query

router = APIRouter(prefix="/valuations", tags=["Property Valuations"])


@router.get("/zip")
async def zip_valuation(
    request: Request,
    zip: str = Query(..., min_length=5, max_length=5, description="5-digit ZIP code"),
    property_type: str | None = Query(None, description="Filter: Single Family Residential, All Residential, etc."),
    quarters: int = Query(4, ge=1, le=20, description="Number of quarters of history"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Get property valuation and market data for a ZIP code.
    Returns median sale/list prices, days on market, inventory, and trends.

    Requires Explorer plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan == PlanTier.FREE:
        raise HTTPException(
            status_code=403,
            detail="Property valuation data requires Explorer plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = [PropertyValuation.zip == zip]
    if property_type:
        conditions.append(PropertyValuation.property_type.ilike(f"%{property_type}%"))

    query = (
        select(PropertyValuation)
        .where(and_(*conditions))
        .order_by(PropertyValuation.period_end.desc())
        .limit(quarters)
    )
    result = await db.execute(query)
    valuations = result.scalars().all()

    if not valuations:
        raise HTTPException(status_code=404, detail=f"No valuation data found for ZIP {zip}")

    latest = valuations[0]

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/valuations/zip",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "zip": zip,
        "state": latest.state,
        "region": latest.region,
        "parent_metro": latest.parent_metro,
        "current": {
            "period": f"{latest.period_begin.isoformat()} to {latest.period_end.isoformat()}",
            "median_sale_price": latest.median_sale_price,
            "median_list_price": latest.median_list_price,
            "median_ppsf": latest.median_ppsf,
            "homes_sold": latest.homes_sold,
            "pending_sales": latest.pending_sales,
            "new_listings": latest.new_listings,
            "inventory": latest.inventory,
            "months_of_supply": latest.months_of_supply,
            "median_dom": latest.median_dom,
            "avg_sale_to_list": latest.avg_sale_to_list,
            "sold_above_list_pct": latest.sold_above_list,
            "price_drops_pct": latest.price_drops,
        },
        "history": [
            {
                "period_begin": v.period_begin.isoformat(),
                "period_end": v.period_end.isoformat(),
                "property_type": v.property_type,
                "median_sale_price": v.median_sale_price,
                "median_list_price": v.median_list_price,
                "homes_sold": v.homes_sold,
                "inventory": v.inventory,
                "median_dom": v.median_dom,
            }
            for v in valuations
        ],
    }


@router.get("/compare")
async def compare_zips(
    request: Request,
    zips: str = Query(..., description="Comma-separated ZIP codes (max 10)"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """Compare latest market data across multiple ZIP codes."""
    plan = resolve_plan(user.plan)
    if plan == PlanTier.FREE:
        raise HTTPException(
            status_code=403,
            detail="Property valuation data requires Explorer plan or higher."
        )

    zip_list = [z.strip() for z in zips.split(",") if z.strip()][:10]
    if not zip_list:
        raise HTTPException(status_code=400, detail="Provide at least one ZIP code.")

    await check_rate_limit(request, lookup_count=len(zip_list))

    comparisons = []
    for z in zip_list:
        query = (
            select(PropertyValuation)
            .where(PropertyValuation.zip == z)
            .order_by(PropertyValuation.period_end.desc())
            .limit(1)
        )
        result = await db.execute(query)
        v = result.scalar_one_or_none()
        if v:
            comparisons.append({
                "zip": z,
                "state": v.state,
                "region": v.region,
                "median_sale_price": v.median_sale_price,
                "median_list_price": v.median_list_price,
                "median_ppsf": v.median_ppsf,
                "homes_sold": v.homes_sold,
                "inventory": v.inventory,
                "median_dom": v.median_dom,
                "months_of_supply": v.months_of_supply,
                "period": f"{v.period_begin.isoformat()} to {v.period_end.isoformat()}",
            })

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/valuations/compare",
        lookup_count=len(zip_list),
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "zip_codes": zip_list,
        "comparisons": comparisons,
    }


@router.get("/hottest")
async def hottest_markets(
    request: Request,
    state: str | None = Query(None, max_length=2, description="Filter by state"),
    metric: str = Query("median_sale_price", description="Sort metric"),
    limit: int = Query(25, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """Get the hottest real estate markets ranked by various metrics."""
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Market rankings require Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    # Get most recent period for each ZIP
    subq = (
        select(
            PropertyValuation.zip,
            func.max(PropertyValuation.period_end).label("latest_period"),
        )
        .group_by(PropertyValuation.zip)
        .subquery()
    )

    conditions = [
        PropertyValuation.zip == subq.c.zip,
        PropertyValuation.period_end == subq.c.latest_period,
    ]
    if state:
        conditions.append(PropertyValuation.state == state.upper())

    # Sort by chosen metric
    sort_col = getattr(PropertyValuation, metric, PropertyValuation.median_sale_price)

    query = (
        select(PropertyValuation)
        .join(subq, and_(*conditions))
        .where(sort_col.is_not(None))
        .order_by(desc(sort_col))
        .limit(limit)
    )
    result = await db.execute(query)
    markets = result.scalars().all()

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/valuations/hottest",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "metric": metric,
        "state": state,
        "markets": [
            {
                "rank": i + 1,
                "zip": m.zip,
                "state": m.state,
                "region": m.region,
                "parent_metro": m.parent_metro,
                "median_sale_price": m.median_sale_price,
                "median_list_price": m.median_list_price,
                "homes_sold": m.homes_sold,
                "inventory": m.inventory,
                "median_dom": m.median_dom,
                "period": f"{m.period_begin.isoformat()} to {m.period_end.isoformat()}",
            }
            for i, m in enumerate(markets)
        ],
    }


@router.get("/stats")
async def valuation_stats(
    request: Request,
    db: AsyncSession = Depends(get_read_db),
):
    """Public endpoint — property valuation database statistics."""
    total = await fast_count(db, "property_valuations")

    zips = (await db.execute(
        select(func.count(func.distinct(PropertyValuation.zip)))
    )).scalar() or 0

    states = await safe_query(db,
        select(PropertyValuation.state, func.count(func.distinct(PropertyValuation.zip)).label("zips"))
        .group_by(PropertyValuation.state)
        .order_by(func.count(func.distinct(PropertyValuation.zip)).desc())
        .limit(15)
    )

    date_range_rows = await safe_query(db,
        select(
            func.min(PropertyValuation.period_begin).label("earliest"),
            func.max(PropertyValuation.period_end).label("latest"),
        )
    )
    date_range = date_range_rows[0] if date_range_rows else None

    return {
        "total_records": total,
        "unique_zips": zips,
        "date_range": {
            "earliest": date_range.earliest.isoformat() if date_range and date_range.earliest else None,
            "latest": date_range.latest.isoformat() if date_range and date_range.latest else None,
        },
        "states": {r.state: r.zips for r in states if r.state},
    }
