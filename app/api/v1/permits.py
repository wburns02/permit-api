"""Permit search API endpoints."""

import logging
from datetime import date, timedelta
from enum import Enum

from fastapi import APIRouter, Depends, Query, HTTPException, Request, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
import csv
import io

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.services.search_service import (
    search_permits,
    geo_search_permits,
    get_coverage,
)
from app.services.stripe_service import (
    get_freshness_limit,
    get_enrichment_cost,
    ENRICHMENT_MIN_PLAN,
    FRESHNESS_LIMITS,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/permits", tags=["Permits"])


class EnrichmentType(str, Enum):
    """Available per-lead enrichment options."""
    NONE = "none"
    PHONE = "phone"
    EMAIL = "email"
    PROPERTY = "property"
    FULL = "full"


# Ordered list of plans for comparison (index = access level)
_PLAN_ORDER = [PlanTier.FREE, PlanTier.EXPLORER, PlanTier.PRO_LEADS, PlanTier.REALTIME, PlanTier.ENTERPRISE]


def _plan_at_least(user_plan: PlanTier, required: PlanTier) -> bool:
    """Check if user's plan meets or exceeds the required plan level."""
    resolved = resolve_plan(user_plan)
    try:
        user_idx = _PLAN_ORDER.index(resolved)
    except ValueError:
        user_idx = 0
    try:
        req_idx = _PLAN_ORDER.index(required)
    except ValueError:
        req_idx = 0
    return user_idx >= req_idx


@router.get("/freshness-info")
async def get_freshness_info(
    user: ApiUser = Depends(get_current_user),
):
    """Show what data freshness tiers the user can access based on their plan."""
    plan = resolve_plan(user.plan)
    limit = get_freshness_limit(plan)
    return {
        "plan": plan.value,
        "freshness_limit_days": limit,
        "can_access_hot": limit == 0,        # 0-30 days old
        "can_access_warm": limit <= 30,       # 30-90 days old
        "can_access_mild": limit <= 90,       # 90-180 days old
        "can_access_cold": True,              # 180+ days old (everyone)
        "oldest_accessible_date": (date.today() - timedelta(days=limit)).isoformat() if limit > 0 else None,
        "upgrade_url": "/pricing" if limit > 0 else None,
        "data_tiers": {
            "hot": {"days": "0-30", "requires": "realtime", "accessible": limit == 0},
            "warm": {"days": "30-90", "requires": "pro_leads", "accessible": limit <= 30},
            "mild": {"days": "90-180", "requires": "explorer", "accessible": limit <= 90},
            "cold": {"days": "180+", "requires": "free", "accessible": True},
        },
    }


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
    enrichment: EnrichmentType = Query(EnrichmentType.NONE, description="Lead enrichment: none, phone, email, property, full"),
    page: int = Query(1, ge=1, le=20),
    page_size: int = Query(25, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Search permits by address, location, or filters.

    Requires X-API-Key header. Each call counts as 1 lookup against your daily limit.
    Results are filtered by your plan's data freshness tier.
    """
    usage = await check_rate_limit(request, lookup_count=1)

    # Determine freshness limit from user's plan
    plan = resolve_plan(user.plan)
    freshness_days = get_freshness_limit(plan)

    # Validate enrichment access
    enrichment_cost_cents = 0
    if enrichment != EnrichmentType.NONE:
        if not _plan_at_least(plan, ENRICHMENT_MIN_PLAN):
            raise HTTPException(
                status_code=403,
                detail=f"Lead enrichment requires {ENRICHMENT_MIN_PLAN.value} plan or higher. "
                       f"Current plan: {plan.value}",
            )
        enrichment_cost_cents = get_enrichment_cost(enrichment.value)

    # Geo search if lat/lng provided
    if lat is not None and lng is not None:
        results = await geo_search_permits(
            db, lat=lat, lng=lng,
            radius_miles=radius or 0.5,
            permit_type=permit_type,
            page=page, page_size=page_size,
            freshness_limit_days=freshness_days,
        )
        if results["total"] == 0 and not any([address, city, state]):
            results["note"] = (
                "Geo search returned 0 results. Geographic coordinates are not yet "
                "available for all records. Try searching by address instead."
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
            freshness_limit_days=freshness_days,
        )

    # Apply enrichment placeholders to results if requested
    # (actual enrichment data would come from an enrichment service)
    enrichment_info = None
    if enrichment != EnrichmentType.NONE and results.get("results"):
        result_count = len(results["results"])
        total_enrichment_cents = enrichment_cost_cents * result_count
        enrichment_info = {
            "type": enrichment.value,
            "cost_per_lead_cents": enrichment_cost_cents,
            "leads_enriched": result_count,
            "total_cost_cents": total_enrichment_cents,
        }
        # Add enrichment marker to each result
        for r in results["results"]:
            r["enrichment_requested"] = enrichment.value
            # Placeholder fields — would be populated by enrichment service
            if enrichment in (EnrichmentType.PHONE, EnrichmentType.FULL):
                r["phone"] = r.get("phone")
            if enrichment in (EnrichmentType.EMAIL, EnrichmentType.FULL):
                r["email"] = r.get("email")
            if enrichment in (EnrichmentType.PROPERTY, EnrichmentType.FULL):
                r["property_value"] = r.get("property_value")
                r["owner_details"] = r.get("owner_details")

        # Log enrichment usage for billing
        enrichment_log = UsageLog(
            user_id=user.id,
            api_key_id=request.state.api_key.id,
            endpoint=f"/v1/permits/search?enrichment={enrichment.value}",
            lookup_count=result_count,
            ip_address=request.client.host if request.client else None,
        )
        db.add(enrichment_log)

    # Log search usage
    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/permits/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    response = {
        **results,
        "usage": usage,
        "freshness": {
            "plan": plan.value,
            "limit_days": freshness_days,
            "cutoff_date": (date.today() - timedelta(days=freshness_days)).isoformat() if freshness_days > 0 else None,
            "note": "Results filtered to your plan's data freshness tier." if freshness_days > 0 else "Full access — no freshness restriction.",
        },
    }
    if enrichment_info:
        response["enrichment"] = enrichment_info

    return response


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

    Each row counts as 1 lookup. Requires Explorer plan or above.
    Max 500 addresses per request.
    """
    plan = resolve_plan(user.plan)
    if plan == PlanTier.FREE:
        raise HTTPException(
            status_code=403,
            detail="Bulk search requires Explorer plan or above.",
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

    # Determine freshness limit from user's plan
    freshness_days = get_freshness_limit(plan)

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
            freshness_limit_days=freshness_days,
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
        "freshness": {
            "plan": plan.value,
            "limit_days": freshness_days,
            "cutoff_date": (date.today() - timedelta(days=freshness_days)).isoformat() if freshness_days > 0 else None,
        },
    }
