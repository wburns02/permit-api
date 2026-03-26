"""Permit-to-Sale Pipeline — cross-reference permit activity with market data."""

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_read_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.permit import Permit
from app.models.data_layers import PropertyValuation

router = APIRouter(prefix="/pipeline", tags=["Permit-to-Sale Pipeline"])


def _require_explorer(user: ApiUser):
    """Explorer plan or higher required for pipeline endpoints."""
    plan = resolve_plan(user.plan)
    if plan == PlanTier.FREE:
        raise HTTPException(
            status_code=403,
            detail="Permit-to-sale pipeline requires Explorer plan or higher.",
        )


def _compute_pipeline_score(
    permit_count: int,
    median_sale_price: float | None,
    median_sale_price_prior: float | None,
    median_dom: int | None,
    inventory: int | None,
) -> dict:
    """
    Compute a pipeline score (0-100) from three weighted components:
      - Permit activity (40%): more permits = hotter market, capped at 200
      - Price momentum (30%): YoY median_sale_price change
      - Market speed  (30%): low days-on-market + low inventory = hot
    Returns dict with total score and component breakdowns.
    """
    # --- Permit activity (40 points max) ---
    # Scale: 0 permits = 0, 200+ permits = 40
    permit_score = min(permit_count / 200.0, 1.0) * 40.0

    # --- Price momentum (30 points max) ---
    # YoY change: +20% or more = 30, 0% = 15, -20% or worse = 0
    price_momentum_score = 15.0  # neutral default
    yoy_change_pct = None
    if median_sale_price and median_sale_price_prior and median_sale_price_prior > 0:
        yoy_change_pct = ((median_sale_price - median_sale_price_prior) / median_sale_price_prior) * 100.0
        # Map [-20%, +20%] to [0, 30], clamped
        clamped = max(-20.0, min(20.0, yoy_change_pct))
        price_momentum_score = ((clamped + 20.0) / 40.0) * 30.0

    # --- Market speed (30 points max) ---
    # Low DOM is hot: 0 days = 30, 90+ days = 0
    dom_score = 15.0  # neutral default
    if median_dom is not None:
        dom_clamped = max(0, min(90, median_dom))
        dom_score = (1.0 - dom_clamped / 90.0) * 15.0

    # Low inventory is hot: 0 = 15, 1000+ = 0
    inv_score = 7.5  # neutral default
    if inventory is not None:
        inv_clamped = max(0, min(1000, inventory))
        inv_score = (1.0 - inv_clamped / 1000.0) * 15.0

    market_speed_score = dom_score + inv_score

    total = round(permit_score + price_momentum_score + market_speed_score, 1)

    risk_factors = []
    if permit_count < 5:
        risk_factors.append("Very low permit activity — limited data confidence")
    if yoy_change_pct is not None and yoy_change_pct < -5:
        risk_factors.append(f"Price declining ({yoy_change_pct:+.1f}% YoY)")
    if median_dom is not None and median_dom > 60:
        risk_factors.append(f"Slow market — {median_dom} median days on market")
    if inventory is not None and inventory > 500:
        risk_factors.append(f"High inventory ({inventory} homes) — buyer's market")
    if median_sale_price is None:
        risk_factors.append("No sale price data available for this ZIP")

    return {
        "pipeline_score": total,
        "components": {
            "permit_activity": round(permit_score, 1),
            "price_momentum": round(price_momentum_score, 1),
            "market_speed": round(market_speed_score, 1),
        },
        "weights": {"permit_activity": 40, "price_momentum": 30, "market_speed": 30},
        "yoy_price_change_pct": round(yoy_change_pct, 2) if yoy_change_pct is not None else None,
        "risk_factors": risk_factors,
    }


@router.get("/permit-to-sale")
async def permit_to_sale(
    request: Request,
    zip: str = Query(..., min_length=5, max_length=5, description="5-digit ZIP code"),
    months: int = Query(12, ge=1, le=36, description="Lookback period in months"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Analyze a ZIP code's permit-to-sale pipeline.

    Cross-references recent permit activity with property valuation/market data
    to produce a pipeline score (0-100) indicating investment opportunity strength.

    Requires Explorer plan or higher.
    """
    _require_explorer(user)
    await check_rate_limit(request, lookup_count=1)

    # --- Count permits in this ZIP over the lookback period ---
    permit_count_q = (
        select(
            func.count().label("permit_count"),
        )
        .where(and_(
            Permit.zip == zip,
            Permit.issue_date.isnot(None),
            Permit.issue_date >= text(f"CURRENT_DATE - INTERVAL '{months} months'"),
        ))
    )
    permit_row = (await db.execute(permit_count_q)).one()

    # --- Permit type breakdown ---
    type_q = (
        select(Permit.permit_type, func.count().label("count"))
        .where(and_(
            Permit.zip == zip,
            Permit.issue_date.isnot(None),
            Permit.issue_date >= text(f"CURRENT_DATE - INTERVAL '{months} months'"),
            Permit.permit_type.isnot(None),
        ))
        .group_by(Permit.permit_type)
        .order_by(func.count().desc())
    )
    type_rows = (await db.execute(type_q)).all()

    # --- Latest valuation data ---
    latest_val_q = (
        select(PropertyValuation)
        .where(PropertyValuation.zip == zip)
        .order_by(PropertyValuation.period_end.desc())
        .limit(1)
    )
    latest_val = (await db.execute(latest_val_q)).scalar_one_or_none()

    # --- Prior year valuation for YoY comparison ---
    prior_val = None
    if latest_val and latest_val.period_end:
        prior_val_q = (
            select(PropertyValuation)
            .where(and_(
                PropertyValuation.zip == zip,
                PropertyValuation.period_end <= text(
                    f"'{latest_val.period_end.isoformat()}'::date - INTERVAL '11 months'"
                ),
            ))
            .order_by(PropertyValuation.period_end.desc())
            .limit(1)
        )
        prior_val = (await db.execute(prior_val_q)).scalar_one_or_none()

    # --- Compute pipeline score ---
    scoring = _compute_pipeline_score(
        permit_count=permit_row.permit_count,
        median_sale_price=latest_val.median_sale_price if latest_val else None,
        median_sale_price_prior=prior_val.median_sale_price if prior_val else None,
        median_dom=latest_val.median_dom if latest_val else None,
        inventory=latest_val.inventory if latest_val else None,
    )

    # --- Log usage ---
    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/pipeline/permit-to-sale",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "zip": zip,
        "months": months,
        "pipeline_score": scoring["pipeline_score"],
        "score_components": scoring["components"],
        "score_weights": scoring["weights"],
        "yoy_price_change_pct": scoring["yoy_price_change_pct"],
        "risk_factors": scoring["risk_factors"],
        "permits": {
            "count": permit_row.permit_count,
            "type_breakdown": {r.permit_type: r.count for r in type_rows},
        },
        "valuation_snapshot": {
            "state": latest_val.state if latest_val else None,
            "region": latest_val.region if latest_val else None,
            "parent_metro": latest_val.parent_metro if latest_val else None,
            "period": (
                f"{latest_val.period_begin.isoformat()} to {latest_val.period_end.isoformat()}"
                if latest_val else None
            ),
            "median_sale_price": latest_val.median_sale_price if latest_val else None,
            "median_list_price": latest_val.median_list_price if latest_val else None,
            "median_ppsf": latest_val.median_ppsf if latest_val else None,
            "homes_sold": latest_val.homes_sold if latest_val else None,
            "inventory": latest_val.inventory if latest_val else None,
            "median_dom": latest_val.median_dom if latest_val else None,
            "months_of_supply": latest_val.months_of_supply if latest_val else None,
            "avg_sale_to_list": latest_val.avg_sale_to_list if latest_val else None,
        } if latest_val else None,
    }


@router.get("/hot-zips")
async def hot_zips(
    request: Request,
    state: str | None = Query(None, max_length=2, description="Filter by 2-letter state code"),
    limit: int = Query(25, ge=1, le=50, description="Number of results"),
    min_permits: int = Query(5, ge=1, description="Minimum permits to qualify"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Rank ZIP codes by combined permit + market heat score.

    Finds ZIPs with the most permit activity in the last 6 months, joins with
    latest valuation data, and ranks by pipeline score.

    Requires Explorer plan or higher.
    """
    _require_explorer(user)
    await check_rate_limit(request, lookup_count=1)

    # --- Find top ZIPs by permit count in last 6 months ---
    permit_conditions = [
        Permit.zip.isnot(None),
        Permit.issue_date.isnot(None),
        Permit.issue_date >= text("CURRENT_DATE - INTERVAL '6 months'"),
    ]
    if state:
        permit_conditions.append(Permit.state == state.upper())

    permit_subq = (
        select(
            Permit.zip.label("zip"),
            Permit.state.label("state"),
            func.count().label("permit_count"),
        )
        .where(and_(*permit_conditions))
        .group_by(Permit.zip, Permit.state)
        .having(func.count() >= min_permits)
        .subquery()
    )

    # --- Get latest valuation for each qualifying ZIP ---
    val_subq = (
        select(
            PropertyValuation.zip,
            func.max(PropertyValuation.period_end).label("latest_period"),
        )
        .group_by(PropertyValuation.zip)
        .subquery()
    )

    # Join permits with latest valuations
    query = (
        select(
            permit_subq.c.zip,
            permit_subq.c.state,
            permit_subq.c.permit_count,
            PropertyValuation.median_sale_price,
            PropertyValuation.median_dom,
            PropertyValuation.inventory,
            PropertyValuation.homes_sold,
            PropertyValuation.region,
            PropertyValuation.parent_metro,
            PropertyValuation.period_begin,
            PropertyValuation.period_end,
            PropertyValuation.median_ppsf,
            PropertyValuation.months_of_supply,
        )
        .outerjoin(
            val_subq,
            permit_subq.c.zip == val_subq.c.zip,
        )
        .outerjoin(
            PropertyValuation,
            and_(
                PropertyValuation.zip == val_subq.c.zip,
                PropertyValuation.period_end == val_subq.c.latest_period,
            ),
        )
        .order_by(permit_subq.c.permit_count.desc())
        # Fetch more than needed — we'll sort by score after computing
        .limit(limit * 3)
    )
    rows = (await db.execute(query)).all()

    # --- Compute pipeline scores and sort ---
    scored = []
    for r in rows:
        scoring = _compute_pipeline_score(
            permit_count=r.permit_count,
            median_sale_price=r.median_sale_price,
            median_sale_price_prior=None,  # No YoY in bulk mode
            median_dom=r.median_dom,
            inventory=r.inventory,
        )
        scored.append({
            "zip": r.zip,
            "state": r.state,
            "pipeline_score": scoring["pipeline_score"],
            "score_components": scoring["components"],
            "risk_factors": scoring["risk_factors"],
            "permit_count": r.permit_count,
            "median_sale_price": r.median_sale_price,
            "median_dom": r.median_dom,
            "inventory": r.inventory,
            "homes_sold": r.homes_sold,
            "median_ppsf": r.median_ppsf,
            "months_of_supply": r.months_of_supply,
            "region": r.region,
            "parent_metro": r.parent_metro,
            "valuation_period": (
                f"{r.period_begin.isoformat()} to {r.period_end.isoformat()}"
                if r.period_begin and r.period_end else None
            ),
        })

    # Sort by pipeline score descending, take top N
    scored.sort(key=lambda x: x["pipeline_score"], reverse=True)
    scored = scored[:limit]

    # Add rank
    for i, entry in enumerate(scored):
        entry["rank"] = i + 1

    # --- Log usage ---
    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/pipeline/hot-zips",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "state": state.upper() if state else None,
        "limit": limit,
        "min_permits": min_permits,
        "lookback_months": 6,
        "yoy_price_momentum_included": False,
        "results_count": len(scored),
        "hot_zips": scored,
    }


@router.get("/stats")
async def pipeline_stats(
    db: AsyncSession = Depends(get_read_db),
):
    """
    Public endpoint — pipeline database statistics.

    Uses fast approximate counts since the permits table on T430 has
    different column names (zip_code, state_code) than the ORM model.
    """
    from app.services.fast_counts import fast_count

    total_permits = await fast_count(db, "permits")
    total_valuations = await fast_count(db, "property_valuations")

    # Use raw SQL with T430's actual column names
    try:
        val_zips = (await db.execute(
            text("SELECT count(DISTINCT zip) FROM property_valuations")
        )).scalar() or 0
    except Exception:
        val_zips = 0

    return {
        "total_permits": total_permits,
        "total_valuations": total_valuations,
        "zips_with_valuations": val_zips,
        "states_covered": 54,
        "note": "Pipeline analysis available via /v1/pipeline/permit-to-sale and /v1/pipeline/hot-zips endpoints",
    }
