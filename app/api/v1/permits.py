"""Permit search API endpoints."""

from fastapi import APIRouter, Depends, Query, HTTPException, Request, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
import csv
import io

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog
from app.services.search_service import (
    search_permits,
    geo_search_permits,
    get_coverage,
)

router = APIRouter(prefix="/permits", tags=["Permits"])


@router.get("/search")
async def search(
    request: Request,
    address: str | None = Query(None, description="Street address to search"),
    city: str | None = Query(None),
    state: str | None = Query(None, max_length=2),
    zip: str | None = Query(None, alias="zip_code"),
    permit_type: str | None = Query(None, description="building, electrical, plumbing, mechanical, demolition"),
    status: str | None = Query(None),
    jurisdiction: str | None = Query(None),
    contractor: str | None = Query(None),
    date_from: str | None = Query(None, description="YYYY-MM-DD"),
    date_to: str | None = Query(None, description="YYYY-MM-DD"),
    lat: float | None = Query(None, description="Latitude for geo search"),
    lng: float | None = Query(None, description="Longitude for geo search"),
    radius: float | None = Query(None, description="Radius in miles (default 0.5)", le=25),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Search permits by address, location, or filters.

    Requires X-API-Key header. Each call counts as 1 lookup against your daily limit.
    """
    import logging, time
    _log = logging.getLogger(__name__)
    _t0 = time.time()
    _log.info("SEARCH: endpoint entered, user=%s", user.email if user else "none")

    # Rate limit check
    usage = await check_rate_limit(request)
    _log.info("SEARCH: rate limit done in %.2fs", time.time() - _t0)

    # Geo search if lat/lng provided
    if lat is not None and lng is not None:
        results = await geo_search_permits(
            db, lat=lat, lng=lng,
            radius_miles=radius or 0.5,
            permit_type=permit_type,
            page=page, page_size=page_size,
        )
    elif not any([address, city, state, zip, permit_type, status, jurisdiction, contractor]):
        raise HTTPException(
            status_code=400,
            detail="At least one search parameter is required (address, city, state, zip_code, permit_type, etc.)",
        )
    else:
        results = await search_permits(
            db,
            address=address, city=city, state=state, zip_code=zip,
            permit_type=permit_type, status=status,
            jurisdiction=jurisdiction, contractor=contractor,
            date_from=date_from, date_to=date_to,
            page=page, page_size=page_size,
        )

    # Log usage
    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/permits/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        **results,
        "usage": usage,
    }


@router.post("/bulk")
async def bulk_search(
    request: Request,
    file: UploadFile = File(..., description="CSV with 'address' column"),
    state: str | None = Query(None, max_length=2),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Bulk permit search. Upload a CSV with an 'address' column.

    Each row counts as 1 lookup. Requires Starter plan or above.
    Max 500 addresses per request.
    """
    if user.plan == PlanTier.FREE:
        raise HTTPException(
            status_code=403,
            detail="Bulk search requires Starter plan or above.",
        )

    content = await file.read()
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        text = content.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))

    if "address" not in (reader.fieldnames or []):
        raise HTTPException(status_code=400, detail="CSV must have an 'address' column.")

    rows = list(reader)
    if len(rows) > 500:
        raise HTTPException(status_code=400, detail="Maximum 500 addresses per bulk request.")

    # Rate limit for total lookups
    usage = await check_rate_limit(request, lookup_count=len(rows))

    results = []
    for row in rows:
        addr = row.get("address", "").strip()
        row_city = row.get("city")
        row_state = row.get("state") or state
        row_zip = row.get("zip") or row.get("zip_code")

        if not addr:
            results.append({"input_address": "", "permits": [], "match_count": 0})
            continue

        search_result = await search_permits(
            db,
            address=addr,
            city=row_city,
            state=row_state,
            zip_code=row_zip,
            page=1,
            page_size=10,
        )

        results.append({
            "input_address": addr,
            "permits": search_result["results"],
            "match_count": search_result["total"],
        })

    # Log usage
    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/permits/bulk",
        lookup_count=len(rows),
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "results": results,
        "total_addresses": len(rows),
        "usage": usage,
    }
