"""Property-centric endpoints for insurance and underwriting."""

import csv
import io
from fastapi import APIRouter, Depends, Query, HTTPException, Request, UploadFile, File
from sqlalchemy import select, func, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.permit import Permit
from app.services.search_service import normalize_address, PERMIT_COLUMNS, row_to_dict
from app.services.risk_service import compute_risk_signals

router = APIRouter(prefix="/properties", tags=["Properties"])

BULK_LIMITS = {
    PlanTier.FREE: 0,
    PlanTier.EXPLORER: 100,
    PlanTier.PRO_LEADS: 1000,
    PlanTier.REALTIME: 5000,
    PlanTier.ENTERPRISE: 10000,
    # Legacy aliases
    PlanTier.STARTER: 100,
    PlanTier.PRO: 1000,
}


async def _property_history(db: AsyncSession, address: str) -> dict:
    """Get all permits for a single property address with risk signals."""
    normalized = normalize_address(address)
    if not normalized:
        return {"address": address, "permits": [], "risk_signals": compute_risk_signals([])}

    query = (
        select(*PERMIT_COLUMNS)
        .where(text("similarity(address_normalized, :addr) > 0.7").bindparams(addr=normalized))
        .order_by(
            text("similarity(address_normalized, :addr) DESC").bindparams(addr=normalized),
            Permit.issue_date.desc().nullslast(),
        )
        .limit(200)
    )
    result = await db.execute(query)
    permits = [row_to_dict(r) for r in result.all()]

    return {
        "address": address,
        "permits": permits,
        "risk_signals": compute_risk_signals(permits),
    }


@router.get("/history")
async def property_history(
    request: Request,
    address: str = Query(..., min_length=3, description="Property address"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get full permit history and risk signals for a single property."""
    await check_rate_limit(request, lookup_count=1)

    result = await _property_history(db, address)

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/properties/history",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return result


@router.post("/bulk-report")
async def bulk_property_report(
    request: Request,
    file: UploadFile = File(..., description="CSV with 'address' column"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Upload CSV of addresses, get per-property permit history and risk signals."""
    plan = resolve_plan(user.plan)
    limit = BULK_LIMITS.get(plan, 0)
    if limit == 0:
        raise HTTPException(status_code=403, detail="Bulk reports require an Explorer plan or higher.")

    content = await file.read()
    try:
        text_content = content.decode("utf-8")
    except UnicodeDecodeError:
        text_content = content.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text_content))
    if "address" not in (reader.fieldnames or []):
        raise HTTPException(status_code=400, detail="CSV must have an 'address' column.")

    addresses = []
    for row in reader:
        addr = (row.get("address") or "").strip()
        if addr:
            addresses.append(addr)
        if len(addresses) >= limit:
            break

    if not addresses:
        raise HTTPException(status_code=400, detail="No valid addresses found in CSV.")

    # Rate limit: each address = 1 lookup
    await check_rate_limit(request, lookup_count=len(addresses))

    results = []
    for addr in addresses:
        results.append(await _property_history(db, addr))

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/properties/bulk-report",
        lookup_count=len(addresses),
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "total_addresses": len(addresses),
        "results": results,
    }


@router.post("/portfolio-analysis")
async def portfolio_analysis(
    request: Request,
    file: UploadFile = File(..., description="CSV with 'address' column"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Aggregate risk analysis across a portfolio of properties."""
    plan = resolve_plan(user.plan)
    limit = BULK_LIMITS.get(plan, 0)
    if limit == 0:
        raise HTTPException(status_code=403, detail="Portfolio analysis requires an Explorer plan or higher.")

    content = await file.read()
    try:
        text_content = content.decode("utf-8")
    except UnicodeDecodeError:
        text_content = content.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text_content))
    if "address" not in (reader.fieldnames or []):
        raise HTTPException(status_code=400, detail="CSV must have an 'address' column.")

    addresses = []
    for row in reader:
        addr = (row.get("address") or "").strip()
        if addr:
            addresses.append(addr)
        if len(addresses) >= limit:
            break

    if not addresses:
        raise HTTPException(status_code=400, detail="No valid addresses found in CSV.")

    await check_rate_limit(request, lookup_count=len(addresses))

    # Gather per-property data
    all_permits = []
    properties_with_permits = 0
    properties_with_gaps = 0
    type_counts = {}

    for addr in addresses:
        data = await _property_history(db, addr)
        permits = data["permits"]
        signals = data["risk_signals"]
        all_permits.extend(permits)
        if permits:
            properties_with_permits += 1
        if signals.get("has_unpermitted_gap"):
            properties_with_gaps += 1
        for pt, cnt in signals.get("permit_type_breakdown", {}).items():
            type_counts[pt] = type_counts.get(pt, 0) + cnt

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/properties/portfolio-analysis",
        lookup_count=len(addresses),
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "total_addresses": len(addresses),
        "properties_with_permits": properties_with_permits,
        "properties_with_no_permits": len(addresses) - properties_with_permits,
        "properties_with_unpermitted_gaps": properties_with_gaps,
        "total_permits_found": len(all_permits),
        "permit_type_breakdown": type_counts,
        "aggregate_risk_signals": compute_risk_signals(all_permits),
    }
