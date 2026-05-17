"""Broadband / rural-septic v2 endpoints.

Three endpoints (matches the existing /v1/* style):
  - GET /v1/broadband/lookup        — every ISP at an address
  - GET /v1/septic-score/lookup     — v2 rural_septic_score for an address (TX)
  - GET /v1/rural-leads/county      — ranked leads by county (TX)

Auth: standard X-API-Key. Rate-limited via the existing middleware — Free
tier hits its daily limit naturally (one lookup_count per call).
"""

import logging

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_read_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser
from app.services.usage_logger import log_usage
from app.services.broadband import (
    list_rural_leads_by_county,
    lookup_broadband,
    lookup_septic_score,
)
from app.schemas.broadband import (
    BroadbandLookupResponse,
    RuralLead,
    RuralLeadsResponse,
    SepticScoreResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Broadband & Rural-Septic"])


# ---------------------------------------------------------------------------
# /v1/broadband/lookup
# ---------------------------------------------------------------------------

@router.get("/broadband/lookup", response_model=BroadbandLookupResponse)
async def broadband_lookup(
    request: Request,
    address: str = Query(..., min_length=3, description="Street address"),
    city: str | None = Query(None),
    state: str = Query(..., min_length=2, max_length=2, description="2-letter state code"),
    zip: str | None = Query(None, max_length=10, alias="zip"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """Look up every ISP serving an address (tech + speeds).

    Available on all paid plans and Free (counts as 1 lookup).
    Resolution: property_sales fuzzy match → tract centroid → FCC BDC tract aggregate.
    """
    usage = await check_rate_limit(request, lookup_count=1)

    state_up = state.upper()
    if not (len(state_up) == 2 and state_up.isalpha()):
        raise HTTPException(status_code=400, detail="Invalid state code.")

    response = await lookup_broadband(
        db, address=address, city=city, state=state_up, zip_code=zip,
    )

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/broadband/lookup",
        lookup_count=1,
        result_count=response.isp_count,
        ip_address=request.client.host if request.client else None,
    )
    return response


# ---------------------------------------------------------------------------
# /v1/septic-score/lookup
# ---------------------------------------------------------------------------

@router.get("/septic-score/lookup", response_model=SepticScoreResponse)
async def septic_score_lookup(
    request: Request,
    address: str = Query(..., min_length=3, description="Street address"),
    city: str | None = Query(None),
    state: str = Query("TX", min_length=2, max_length=2, description="State (TX only for now)"),
    zip: str | None = Query(None, max_length=10, alias="zip"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """Return v2 rural_septic_score for an address (currently TX only).

    Tries a direct match in `rural_septic_score_v2` first; falls back to an
    on-the-fly compute using the same signals (urban-area flag, pop density,
    broadband availability).
    """
    await check_rate_limit(request, lookup_count=1)

    state_up = state.upper()
    if state_up != "TX":
        raise HTTPException(
            status_code=400,
            detail="Septic-score v2 model is currently TX-only. More states coming.",
        )

    response = await lookup_septic_score(
        db, address=address, city=city, state=state_up, zip_code=zip,
    )
    if response is None:
        raise HTTPException(status_code=404, detail="No score available for this address.")

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/septic-score/lookup",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    return response


# ---------------------------------------------------------------------------
# /v1/rural-leads/county
# ---------------------------------------------------------------------------

@router.get("/rural-leads/county", response_model=RuralLeadsResponse)
async def rural_leads_by_county(
    request: Request,
    county: str = Query(..., min_length=2, description="County name (case-insensitive)"),
    state: str = Query("TX", min_length=2, max_length=2),
    min_score: int = Query(70, ge=0, le=100),
    limit: int = Query(100, ge=1, le=1000),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """Ranked rural-septic leads by county (TX-only for now).

    `limit` counts toward your daily lookup quota (1 lookup per 25 leads, min 1).
    """
    # Charge: 1 lookup per 25 leads requested, min 1, max 40 (=1000 leads)
    cost = max(1, (limit + 24) // 25)
    await check_rate_limit(request, lookup_count=cost)

    state_up = state.upper()
    if state_up != "TX":
        raise HTTPException(
            status_code=400,
            detail="Rural-leads model is currently TX-only. More states coming.",
        )

    leads: list[RuralLead] = await list_rural_leads_by_county(
        db, county=county, state=state_up, min_score=min_score, limit=limit,
    )

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/rural-leads/county",
        lookup_count=cost,
        result_count=len(leads),
        ip_address=request.client.host if request.client else None,
    )

    return RuralLeadsResponse(
        county=county,
        state=state_up,
        min_score=min_score,
        count=len(leads),
        leads=leads,
    )
