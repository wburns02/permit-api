"""Cross-product enrichment endpoints.

Currently exposes:
  POST /v1/enrichment/broadband-score-bulk  — batch broadband summary
                                              (max 500 rows per call)

The per-product `?include_broadband=true` opt-in flag is wired directly into
each product router (permits, hail-leads, etc.) so existing response shapes
don't change unless the caller asks for the extra field.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser
from app.schemas.enrichment import (
    BulkBroadbandRequest,
    BulkBroadbandResponse,
)
from app.services.enrichment import bulk_broadband
from app.services.usage_logger import log_usage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/enrichment", tags=["Enrichment"])


@router.post(
    "/broadband-score-bulk",
    response_model=BulkBroadbandResponse,
)
async def broadband_score_bulk(
    request: Request,
    body: BulkBroadbandRequest,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Bulk broadband enrichment for CRM imports.

    Submit up to 500 addresses (or lat/lon pairs) per call. Each row returns
    a compact broadband signal: isp_count, has_fiber, only_satellite, max_dl.

    Cost: 1 lookup per 25 rows, min 1. So a 500-row call burns 20 lookups
    against the daily quota.
    """
    n = len(body.items)
    if n == 0:
        raise HTTPException(status_code=400, detail="items must be non-empty.")
    if n > 500:
        # Pydantic max_length should catch this, belt + suspenders.
        raise HTTPException(status_code=400, detail="max 500 items per call.")

    cost = max(1, (n + 24) // 25)
    await check_rate_limit(request, lookup_count=cost)

    results = await bulk_broadband(db, body.items)
    succeeded = sum(1 for r in results if r.broadband is not None)
    failed = n - succeeded

    if not getattr(request.state, "is_internal", False):
        log_usage(
            user_id=user.id,
            api_key_id=request.state.api_key.id,
            endpoint="/v1/enrichment/broadband-score-bulk",
            lookup_count=cost,
            result_count=succeeded,
            ip_address=request.client.host if request.client else None,
        )

    return BulkBroadbandResponse(
        count=n,
        succeeded=succeeded,
        failed=failed,
        results=results,
    )
