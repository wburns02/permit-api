"""Deed/sale transfer intelligence endpoints."""

import math
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_, or_, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.data_layers import PropertySale
from app.services.fast_counts import fast_count

router = APIRouter(prefix="/sales", tags=["Property Sales"])


@router.get("/search")
async def sales_search(
    request: Request,
    address: str | None = Query(None, min_length=3, description="Street address"),
    city: str | None = Query(None, description="City name"),
    state: str | None = Query(None, max_length=2, description="2-letter state code"),
    zip: str | None = Query(None, max_length=10),
    grantor: str | None = Query(None, description="Seller name"),
    grantee: str | None = Query(None, description="Buyer name"),
    doc_type: str | None = Query(None, description="Document type: DEED, TRANSFER, etc."),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Search property sale/deed transfer records by address, city, state, parties, etc.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Property sale data requires Pro Leads plan or higher."
        )

    if not any([address, city, state, zip, grantor, grantee]):
        raise HTTPException(
            status_code=400,
            detail="Provide at least one search parameter: address, city, state, zip, grantor, or grantee."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = []
    if address:
        conditions.append(PropertySale.address.ilike(f"%{address}%"))
    if city:
        conditions.append(PropertySale.city.ilike(f"%{city}%"))
    if state:
        conditions.append(PropertySale.state == state.upper())
    if zip:
        conditions.append(PropertySale.zip == zip)
    if grantor:
        conditions.append(PropertySale.grantor.ilike(f"%{grantor}%"))
    if grantee:
        conditions.append(PropertySale.grantee.ilike(f"%{grantee}%"))
    if doc_type:
        conditions.append(PropertySale.doc_type.ilike(f"%{doc_type}%"))

    offset = (page - 1) * limit

    # Count query
    count_q = select(func.count()).select_from(PropertySale).where(and_(*conditions))
    total = (await db.execute(count_q)).scalar() or 0

    # Data query
    query = (
        select(PropertySale)
        .where(and_(*conditions))
        .order_by(PropertySale.sale_date.desc().nullslast())
        .offset(offset)
        .limit(limit)
    )
    result = await db.execute(query)
    sales = result.scalars().all()

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/sales/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "query": {
            "address": address, "city": city, "state": state,
            "zip": zip, "grantor": grantor, "grantee": grantee,
            "doc_type": doc_type,
        },
        "results": [_serialize(s) for s in sales],
        "total": total,
        "page": page,
        "pages": math.ceil(total / limit) if total else 0,
    }


@router.get("/property")
async def property_sales(
    request: Request,
    address: str = Query(..., min_length=3, description="Property address"),
    state: str | None = Query(None, max_length=2, description="2-letter state code"),
    city: str | None = Query(None),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get all sale/transfer records for a specific property address.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Property sale data requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = [PropertySale.address.ilike(f"%{address}%")]
    if state:
        conditions.append(PropertySale.state == state.upper())
    if city:
        conditions.append(PropertySale.city.ilike(f"%{city}%"))

    query = (
        select(PropertySale)
        .where(and_(*conditions))
        .order_by(PropertySale.sale_date.desc().nullslast())
        .limit(200)
    )
    result = await db.execute(query)
    sales = result.scalars().all()

    # Aggregate stats
    prices = [s.sale_price for s in sales if s.sale_price and s.sale_price > 0]
    total_value = sum(prices) if prices else 0
    avg_price = total_value / len(prices) if prices else 0

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/sales/property",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "query": {"address": address, "state": state, "city": city},
        "summary": {
            "total_sales": len(sales),
            "total_value": round(total_value, 2),
            "average_price": round(avg_price, 2),
            "min_price": min(prices) if prices else None,
            "max_price": max(prices) if prices else None,
        },
        "results": [_serialize(s) for s in sales],
    }


@router.get("/stats")
async def sales_stats(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Public endpoint -- property sale database statistics."""
    total = await fast_count(db, "property_sales")

    states = (await db.execute(
        select(PropertySale.state, func.count().label("count"))
        .group_by(PropertySale.state)
        .order_by(func.count().desc())
    )).all()

    sources = (await db.execute(
        select(PropertySale.source, func.count().label("count"))
        .group_by(PropertySale.source)
        .order_by(func.count().desc())
    )).all()

    date_range = (await db.execute(
        select(
            func.min(PropertySale.sale_date).label("earliest"),
            func.max(PropertySale.sale_date).label("latest"),
        )
    )).one()

    return {
        "total_records": total,
        "states": {r.state: r.count for r in states if r.state},
        "sources": {r.source: r.count for r in sources if r.source},
        "date_range": {
            "earliest": date_range.earliest.isoformat() if date_range.earliest else None,
            "latest": date_range.latest.isoformat() if date_range.latest else None,
        },
    }


def _serialize(s: PropertySale) -> dict:
    """Serialize a PropertySale to a dict."""
    return {
        "document_id": s.document_id,
        "address": s.address,
        "city": s.city,
        "state": s.state,
        "zip": s.zip,
        "borough": s.borough,
        "sale_price": s.sale_price,
        "sale_date": s.sale_date.isoformat() if s.sale_date else None,
        "recorded_date": s.recorded_date.isoformat() if s.recorded_date else None,
        "doc_type": s.doc_type,
        "grantor": s.grantor,
        "grantee": s.grantee,
        "property_type": s.property_type,
        "building_class": s.building_class,
        "residential_units": s.residential_units,
        "land_sqft": s.land_sqft,
        "gross_sqft": s.gross_sqft,
        "lat": s.lat,
        "lng": s.lng,
        "source": s.source,
    }
