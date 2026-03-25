"""Temporal Trends API — time-series analysis across all data layers.

Palantir-level temporal intelligence: ZIP trends, contractor trajectories,
market momentum, and entity timelines.
"""

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.services.temporal_analysis import (
    get_zip_trends,
    get_contractor_trajectory,
    get_market_momentum,
    get_entity_timeline,
)

router = APIRouter(prefix="/trends", tags=["Temporal Trends"])


# ---------------------------------------------------------------------------
# Plan gating
# ---------------------------------------------------------------------------

def _require_explorer(user: ApiUser):
    """Explorer+ can use market-level trends."""
    plan = resolve_plan(user.plan)
    if plan == PlanTier.FREE:
        raise HTTPException(
            status_code=403,
            detail="Trends require Explorer plan or higher. Upgrade at /pricing",
        )


def _require_pro(user: ApiUser):
    """Pro Leads+ for detailed ZIP, contractor, and entity trends."""
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER):
        raise HTTPException(
            status_code=403,
            detail="Detailed trends require Pro Leads plan or higher. Upgrade at /pricing",
        )


def _log_usage(user: ApiUser, request: Request, endpoint: str) -> UsageLog:
    return UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint=endpoint,
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )


# ---------------------------------------------------------------------------
# GET /v1/trends/zip — ZIP-level time series (Pro Leads+)
# ---------------------------------------------------------------------------

@router.get("/zip")
async def zip_trends(
    request: Request,
    zip: str = Query(..., min_length=5, max_length=10, description="ZIP code"),
    months: int = Query(12, ge=1, le=36, description="Months of history"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    ZIP-level temporal analysis: permit velocity, price trends, violations,
    and sales over time. Includes MoM/YoY changes and 3-month forecast.
    """
    _require_pro(user)
    await check_rate_limit(request, lookup_count=1)

    result = await get_zip_trends(db, zip, months)

    db.add(_log_usage(user, request, "/v1/trends/zip"))
    await db.commit()

    return result


# ---------------------------------------------------------------------------
# GET /v1/trends/contractor — Contractor trajectory (Pro Leads+)
# ---------------------------------------------------------------------------

@router.get("/contractor")
async def contractor_trends(
    request: Request,
    name: str = Query(..., min_length=2, max_length=300, description="Contractor name"),
    months: int = Query(24, ge=1, le=60, description="Months of history"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Track a contractor's activity and risk trajectory over time.
    Monthly permit counts, jurisdiction spread, license status, violations.
    """
    _require_pro(user)
    await check_rate_limit(request, lookup_count=1)

    result = await get_contractor_trajectory(db, name, months)

    db.add(_log_usage(user, request, "/v1/trends/contractor"))
    await db.commit()

    return result


# ---------------------------------------------------------------------------
# GET /v1/trends/market — State market momentum (Explorer+)
# ---------------------------------------------------------------------------

@router.get("/market")
async def market_trends(
    request: Request,
    state: str = Query(..., min_length=2, max_length=2, description="Two-letter state code"),
    months: int = Query(12, ge=1, le=36, description="Months of history"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    State-level market momentum score combining permit velocity, home prices,
    sales volume, and violation trends. Returns 0-100 momentum score plus
    per-signal trends and forecasts.
    """
    _require_explorer(user)
    await check_rate_limit(request, lookup_count=1)

    result = await get_market_momentum(db, state, months)

    db.add(_log_usage(user, request, "/v1/trends/market"))
    await db.commit()

    return result


# ---------------------------------------------------------------------------
# GET /v1/trends/entity — Entity timeline (Pro Leads+)
# ---------------------------------------------------------------------------

@router.get("/entity")
async def entity_trends(
    request: Request,
    name: str = Query(..., min_length=2, max_length=300, description="Entity/LLC name"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Chronological timeline of an LLC/entity across all data layers:
    formation, property purchases, permits filed, liens, violations.
    """
    _require_pro(user)
    await check_rate_limit(request, lookup_count=1)

    result = await get_entity_timeline(db, name)

    db.add(_log_usage(user, request, "/v1/trends/entity"))
    await db.commit()

    return result


# ---------------------------------------------------------------------------
# GET /v1/trends/stats — Public stats (no auth required)
# ---------------------------------------------------------------------------

@router.get("/stats")
async def trends_stats(
    db: AsyncSession = Depends(get_db),
):
    """Public stats for the temporal analysis engine."""
    from app.services.fast_counts import fast_count

    permits = await fast_count(db, "permits")
    hot_leads = await fast_count(db, "hot_leads")
    violations = await fast_count(db, "code_violations")
    sales = await fast_count(db, "property_sales")
    liens = await fast_count(db, "property_liens")
    entities = await fast_count(db, "business_entities")

    return {
        "engine": "temporal_analysis",
        "data_layers": {
            "permits": permits,
            "hot_leads": hot_leads,
            "code_violations": violations,
            "property_sales": sales,
            "property_liens": liens,
            "business_entities": entities,
        },
        "capabilities": [
            "zip_trends",
            "contractor_trajectory",
            "market_momentum",
            "entity_timeline",
        ],
        "trend_indicators": [
            "mom_change",
            "yoy_change",
            "trend_direction",
            "forecast_next_3mo",
            "momentum_score",
        ],
    }


# ---------------------------------------------------------------------------
# GET /v1/trends/anomalies — Market anomaly detection (public)
# ---------------------------------------------------------------------------

@router.get("/anomalies")
async def market_anomalies(
    state: str = Query(None, min_length=2, max_length=2, description="Two-letter state code (optional)"),
    limit: int = Query(20, ge=1, le=50, description="Max anomalies to return"),
    db: AsyncSession = Depends(get_db),
):
    """Detect unusual patterns across all data layers.

    Returns permit velocity spikes, storm-permit correlations, price anomalies,
    violation surges, and new entity clusters. Public endpoint — no auth required.
    Great for marketing and general market awareness.
    """
    from app.services.anomaly_detector import detect_anomalies

    anomalies = await detect_anomalies(
        db,
        state=state.upper() if state else None,
        limit=limit,
    )

    return {
        "anomalies": anomalies,
        "count": len(anomalies),
        "filters": {
            "state": state.upper() if state else None,
        },
        "types": [
            "permit_velocity_spike",
            "storm_permit_correlation",
            "price_anomaly",
            "violation_surge",
            "entity_cluster",
        ],
    }
