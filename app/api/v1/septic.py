"""Septic system intelligence endpoints."""

import math
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_, or_, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.data_layers import SepticSystem

router = APIRouter(prefix="/septic", tags=["Septic Systems"])


@router.get("/lookup")
async def septic_lookup(
    request: Request,
    address: str | None = Query(None, min_length=3, description="Property address"),
    parcel_id: str | None = Query(None, description="Parcel ID"),
    city: str | None = Query(None),
    state: str | None = Query(None, max_length=2),
    zip: str | None = Query(None, max_length=10),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Look up septic/wastewater system info for a property.
    Returns system type, install date, inspection history, and status.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Septic system data requires Pro Leads plan or higher."
        )

    if not address and not parcel_id:
        raise HTTPException(
            status_code=400,
            detail="Provide either 'address' or 'parcel_id' parameter."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = []
    if parcel_id:
        conditions.append(SepticSystem.parcel_id == parcel_id)
    if address:
        conditions.append(SepticSystem.address.ilike(f"%{address}%"))
    if state:
        conditions.append(SepticSystem.state == state.upper())
    if city:
        conditions.append(SepticSystem.city.ilike(f"%{city}%"))
    if zip:
        conditions.append(SepticSystem.zip == zip)

    query = (
        select(SepticSystem)
        .where(and_(*conditions))
        .order_by(SepticSystem.address)
        .limit(25)
    )
    result = await db.execute(query)
    systems = result.scalars().all()

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/septic/lookup",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "query": {"address": address, "parcel_id": parcel_id, "state": state},
        "results": [
            {
                "address": s.address,
                "city": s.city,
                "state": s.state,
                "zip": s.zip,
                "county": s.county,
                "parcel_id": s.parcel_id,
                "system_type": s.system_type,
                "wastewater_source": s.wastewater_source,
                "install_date": s.install_date.isoformat() if s.install_date else None,
                "last_inspection": s.last_inspection.isoformat() if s.last_inspection else None,
                "land_use": s.land_use,
                "status": s.status,
                "lat": s.lat,
                "lng": s.lng,
                "source": s.source,
            }
            for s in systems
        ],
        "total": len(systems),
    }


@router.get("/nearby")
async def nearby_septic(
    request: Request,
    lat: float = Query(..., description="Latitude"),
    lng: float = Query(..., description="Longitude"),
    radius_miles: float = Query(1.0, ge=0.1, le=10.0),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Find septic systems within a radius of a location."""
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Septic system data requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    deg_per_mile = 1 / 69.0
    lat_range = radius_miles * deg_per_mile
    lng_range = radius_miles * deg_per_mile / max(math.cos(math.radians(lat)), 0.01)

    query = (
        select(SepticSystem)
        .where(
            and_(
                SepticSystem.lat.between(lat - lat_range, lat + lat_range),
                SepticSystem.lng.between(lng - lng_range, lng + lng_range),
                SepticSystem.lat.is_not(None),
                SepticSystem.lng.is_not(None),
            )
        )
        .limit(200)
    )
    result = await db.execute(query)
    systems = result.scalars().all()

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/septic/nearby",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "location": {"lat": lat, "lng": lng, "radius_miles": radius_miles},
        "results": [
            {
                "address": s.address,
                "city": s.city,
                "state": s.state,
                "system_type": s.system_type,
                "install_date": s.install_date.isoformat() if s.install_date else None,
                "status": s.status,
                "lat": s.lat,
                "lng": s.lng,
            }
            for s in systems
        ],
        "total": len(systems),
    }


@router.get("/stats")
async def septic_stats(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Public endpoint — septic system database statistics."""
    total = (await db.execute(
        select(func.count()).select_from(SepticSystem)
    )).scalar() or 0

    states = (await db.execute(
        select(SepticSystem.state, func.count().label("count"))
        .group_by(SepticSystem.state)
        .order_by(func.count().desc())
    )).all()

    types = (await db.execute(
        select(SepticSystem.system_type, func.count().label("count"))
        .group_by(SepticSystem.system_type)
        .order_by(func.count().desc())
        .limit(10)
    )).all()

    return {
        "total_records": total,
        "states": {r.state: r.count for r in states},
        "system_types": {r.system_type: r.count for r in types if r.system_type},
    }
