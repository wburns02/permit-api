"""Predictive permit analytics endpoints — ML-powered ZIP code predictions."""

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_read_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, resolve_plan
from app.services.usage_logger import log_usage
from app.models.data_layers import PermitPrediction
from app.services.fast_counts import fast_count

router = APIRouter(prefix="/predictions", tags=["Predictive Analytics"])


@router.get("/zip")
async def zip_prediction(
    request: Request,
    zip: str = Query(..., min_length=5, max_length=5, description="5-digit ZIP code"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Get permit prediction for a single ZIP code.
    Returns prediction score, expected permits, confidence, and risk factors.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Predictive analytics requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    query = (
        select(PermitPrediction)
        .where(PermitPrediction.zip == zip)
        .order_by(PermitPrediction.scored_at.desc())
        .limit(1)
    )
    result = await db.execute(query)
    prediction = result.scalar_one_or_none()

    if not prediction:
        raise HTTPException(status_code=404, detail=f"No prediction found for ZIP {zip}")

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/predictions/zip",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    return {
        "zip": prediction.zip,
        "state": prediction.state,
        "prediction_score": prediction.prediction_score,
        "predicted_permits": prediction.predicted_permits,
        "confidence": prediction.confidence,
        "risk_factors": prediction.risk_factors,
        "features": prediction.features,
        "model_version": prediction.model_version,
        "scored_at": prediction.scored_at.isoformat() if prediction.scored_at else None,
    }


@router.get("/hotspots")
async def hotspots(
    request: Request,
    state: str | None = Query(None, max_length=2, description="Filter by 2-letter state code"),
    limit: int = Query(50, ge=1, le=50, description="Number of results"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Get top predicted ZIP codes ranked by prediction score.
    Optional state filter. Returns the hottest predicted areas.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Predictive analytics requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    # Get the latest scored_at to only return current predictions
    latest_scored = (await db.execute(
        select(func.max(PermitPrediction.scored_at))
    )).scalar()

    if not latest_scored:
        return {"state": state, "hotspots": [], "total": 0}

    conditions = [PermitPrediction.scored_at == latest_scored]
    if state:
        conditions.append(PermitPrediction.state == state.upper())

    query = (
        select(PermitPrediction)
        .where(and_(*conditions))
        .order_by(desc(PermitPrediction.prediction_score))
        .limit(limit)
    )
    result = await db.execute(query)
    predictions = result.scalars().all()

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/predictions/hotspots",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    return {
        "state": state,
        "model_version": predictions[0].model_version if predictions else None,
        "scored_at": latest_scored.isoformat() if latest_scored else None,
        "total": len(predictions),
        "hotspots": [
            {
                "rank": i + 1,
                "zip": p.zip,
                "state": p.state,
                "prediction_score": p.prediction_score,
                "predicted_permits": p.predicted_permits,
                "confidence": p.confidence,
                "risk_factors": p.risk_factors,
            }
            for i, p in enumerate(predictions)
        ],
    }


@router.get("/stats")
async def prediction_stats(
    request: Request,
    db: AsyncSession = Depends(get_read_db),
):
    """Public endpoint — predictive model metadata and statistics."""
    total_predictions = await fast_count(db, "permit_predictions")

    unique_zips = (await db.execute(
        select(func.count(func.distinct(PermitPrediction.zip)))
    )).scalar() or 0

    latest_scored = (await db.execute(
        select(func.max(PermitPrediction.scored_at))
    )).scalar()

    model_version = None
    if latest_scored:
        mv_result = (await db.execute(
            select(PermitPrediction.model_version)
            .where(PermitPrediction.scored_at == latest_scored)
            .limit(1)
        )).scalar()
        model_version = mv_result

    avg_score = (await db.execute(
        select(func.avg(PermitPrediction.prediction_score))
    )).scalar()

    avg_confidence = (await db.execute(
        select(func.avg(PermitPrediction.confidence))
    )).scalar()

    # State distribution
    states = (await db.execute(
        select(
            PermitPrediction.state,
            func.count(func.distinct(PermitPrediction.zip)).label("zips"),
        )
        .group_by(PermitPrediction.state)
        .order_by(func.count(func.distinct(PermitPrediction.zip)).desc())
        .limit(15)
    )).all()

    return {
        "total_predictions": total_predictions,
        "unique_zips_scored": unique_zips,
        "model_version": model_version,
        "last_scored_at": latest_scored.isoformat() if latest_scored else None,
        "avg_prediction_score": round(avg_score, 2) if avg_score else None,
        "avg_confidence": round(avg_confidence, 3) if avg_confidence else None,
        "states": {r.state: r.zips for r in states if r.state},
    }
