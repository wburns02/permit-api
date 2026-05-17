"""Real-time rural-score lookup for ANY address (not limited to MV addresses).

Endpoint:
  GET /v1/rural-score/lookup?address=...&city=...&state=TX&zip=...

Pipeline (in order of cost):
  1. property_sales fuzzy match → lat/lon (no network call)
  2. geocoded_addresses cache → lat/lon (one PG lookup)
  3. Census Geocoder API → lat/lon (free, ~5k/day rate limit, cached on success)

Once we have lat/lon we compute:
  - in_urban_area via is_in_urban_area()
  - population_density via census_acs_2023_zcta (or zcta_pop_density_<state>)
  - lot_acres via parcel_lookup_v5
  - broadband signal via the broadband resolver
  - score via the v5 formula
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser
from app.schemas.rural_score import RuralScoreLookupResponse
from app.services.rural_score import real_time_rural_score
from app.services.usage_logger import log_usage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/rural-score", tags=["Rural Score"])


@router.get("/lookup", response_model=RuralScoreLookupResponse)
async def rural_score_lookup(
    request: Request,
    address: str = Query(..., min_length=3),
    city: str | None = Query(None),
    state: str = Query(..., min_length=2, max_length=2),
    zip: str | None = Query(None, max_length=10, alias="zip"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return a v5-style rural_septic_score for an arbitrary address.

    Counts as 1 lookup per call (geocoder is cached, so repeat lookups for the
    same address don't hit Census twice).
    """
    await check_rate_limit(request, lookup_count=1)

    state_up = state.upper()
    if not (len(state_up) == 2 and state_up.isalpha()):
        raise HTTPException(status_code=400, detail="Invalid state code.")

    try:
        response = await real_time_rural_score(
            db, address=address, city=city, state=state_up, zip_code=zip,
        )
    except Exception as e:
        logger.exception("real_time_rural_score failed")
        try:
            await db.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"rural-score lookup failed: {e}")

    if not getattr(request.state, "is_internal", False):
        log_usage(
            user_id=user.id,
            api_key_id=request.state.api_key.id,
            endpoint="/v1/rural-score/lookup",
            lookup_count=1,
            ip_address=request.client.host if request.client else None,
        )
    return response
