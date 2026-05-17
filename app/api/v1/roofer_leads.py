"""Roofer Leads — storm-strike dispatch endpoints.

After a NOAA hail event, return ranked rooftop addresses within N days of the
storm, ordered by composite score:
    composite = storm_severity (0-30)
              + home_age_score (0-25)
              + mortgage_score (0-20)
              + roof_permit_recency_penalty (0..-20)
              × (1 - distance_falloff)

Endpoints:
  GET /v1/roofer-leads/by-hail-event?event_id=...&days_after=120&radius=20
  GET /v1/roofer-leads/recent?state=TX&days_back=14&min_score=70

Auth: standard X-API-Key (counts toward daily lookup quota).
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser
from app.services.roofer_leads import (
    fetch_storm_event,
    list_roofer_leads_recent,
    score_properties_for_event,
)
from app.services.usage_logger import log_usage
from app.schemas.roofer_leads import (
    RooferLeadsByEventResponse,
    RooferLeadsRecentResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/roofer-leads", tags=["Roofer Leads"])


# ---------------------------------------------------------------------------
# GET /v1/roofer-leads/by-hail-event
# ---------------------------------------------------------------------------


@router.get("/by-hail-event", response_model=RooferLeadsByEventResponse)
async def by_hail_event(
    request: Request,
    event_id: int = Query(..., description="noaa_storm_events_details.event_id"),
    days_after: int = Query(120, ge=1, le=730),
    radius_miles: float = Query(20.0, ge=0.5, le=50.0),
    min_magnitude: float = Query(1.0, ge=0.0, le=10.0),
    min_score: int = Query(0, ge=0, le=100),
    limit: int = Query(100, ge=1, le=1000),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return ranked rooftop leads inside a single storm event's footprint."""
    # 1 lookup per 25 leads requested, min 1.
    cost = max(1, (limit + 24) // 25)
    await check_rate_limit(request, lookup_count=cost)

    event = await fetch_storm_event(db, event_id=event_id)
    if event is None:
        raise HTTPException(status_code=404, detail=f"Storm event {event_id} not found.")

    if event.magnitude is not None and event.magnitude < min_magnitude:
        # Honor the caller's filter — return empty rather than 404.
        leads = []
    else:
        leads = await score_properties_for_event(
            db, event=event,
            days_after=days_after,
            radius_miles=radius_miles,
            limit=limit,
            min_score=float(min_score),
        )

    if not getattr(request.state, "is_internal", False):
        log_usage(
            user_id=user.id,
            api_key_id=request.state.api_key.id,
            endpoint="/v1/roofer-leads/by-hail-event",
            lookup_count=cost,
            result_count=len(leads),
            ip_address=request.client.host if request.client else None,
        )

    return RooferLeadsByEventResponse(
        event=event,
        days_after=days_after,
        min_magnitude=min_magnitude,
        radius_miles=radius_miles,
        count=len(leads),
        leads=leads,
    )


# ---------------------------------------------------------------------------
# GET /v1/roofer-leads/recent
# ---------------------------------------------------------------------------


@router.get("/recent", response_model=RooferLeadsRecentResponse)
async def recent(
    request: Request,
    state: str = Query(..., min_length=2, max_length=2),
    days_back: int = Query(14, ge=1, le=180),
    min_magnitude: float = Query(1.0, ge=0.0, le=10.0),
    radius_miles: float = Query(20.0, ge=0.5, le=50.0),
    min_score: int = Query(50, ge=0, le=100),
    limit: int = Query(100, ge=1, le=1000),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Rolling cross-event hail leads for a state."""
    cost = max(1, (limit + 24) // 25)
    await check_rate_limit(request, lookup_count=cost)

    state_up = state.upper()
    if not (len(state_up) == 2 and state_up.isalpha()):
        raise HTTPException(status_code=400, detail="Invalid state code.")

    event_count, leads = await list_roofer_leads_recent(
        db,
        state=state_up,
        days_back=days_back,
        min_score=min_score,
        limit=limit,
        min_magnitude=min_magnitude,
        radius_miles=radius_miles,
    )

    if not getattr(request.state, "is_internal", False):
        log_usage(
            user_id=user.id,
            api_key_id=request.state.api_key.id,
            endpoint="/v1/roofer-leads/recent",
            lookup_count=cost,
            result_count=len(leads),
            ip_address=request.client.host if request.client else None,
        )

    return RooferLeadsRecentResponse(
        state=state_up,
        days_back=days_back,
        min_score=min_score,
        event_count=event_count,
        count=len(leads),
        leads=leads,
    )
