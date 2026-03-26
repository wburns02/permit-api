"""Environmental risk endpoints — EPA facilities + FEMA flood zones."""

import math
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_, case, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_read_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, resolve_plan
from app.services.usage_logger import log_usage
from app.models.data_layers import EpaFacility, FemaFloodZone
from app.services.fast_counts import fast_count, safe_query

router = APIRouter(prefix="/environmental", tags=["Environmental Risk"])

# FEMA zone risk levels
FLOOD_RISK = {
    "V": "Very High",  # Coastal high velocity
    "VE": "Very High",
    "A": "High",        # 1% annual chance
    "AE": "High",
    "AH": "High",
    "AO": "High",
    "AR": "High",
    "A99": "Moderate",
    "D": "Undetermined",
    "X": "Low/Minimal",
    "B": "Moderate",
    "C": "Low",
}


def _haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Approximate distance in miles between two points."""
    R = 3959  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlng / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


@router.get("/risk")
async def environmental_risk(
    request: Request,
    lat: float = Query(..., description="Latitude"),
    lng: float = Query(..., description="Longitude"),
    radius_miles: float = Query(1.0, ge=0.1, le=25.0, description="Search radius in miles"),
    state: str | None = Query(None, max_length=2, description="State (for flood zone lookup)"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Get environmental risk assessment for a location.
    Returns nearby EPA-regulated facilities and FEMA flood zone information.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Environmental risk data requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    # EPA facilities within radius — use bounding box then refine
    deg_per_mile = 1 / 69.0  # Approximate
    lat_range = radius_miles * deg_per_mile
    lng_range = radius_miles * deg_per_mile / max(math.cos(math.radians(lat)), 0.01)

    epa_query = (
        select(EpaFacility)
        .where(
            and_(
                EpaFacility.lat.between(lat - lat_range, lat + lat_range),
                EpaFacility.lng.between(lng - lng_range, lng + lng_range),
                EpaFacility.lat.is_not(None),
                EpaFacility.lng.is_not(None),
            )
        )
        .limit(100)
    )
    epa_result = await db.execute(epa_query)
    epa_facilities = epa_result.scalars().all()

    # Refine to actual radius and sort by distance
    nearby = []
    for f in epa_facilities:
        dist = _haversine_miles(lat, lng, f.lat, f.lng)
        if dist <= radius_miles:
            nearby.append({
                "registry_id": f.registry_id,
                "name": f.name,
                "address": f.address,
                "city": f.city,
                "state": f.state,
                "zip": f.zip,
                "county": f.county,
                "distance_miles": round(dist, 2),
                "lat": f.lat,
                "lng": f.lng,
                "source": f.source,
            })
    nearby.sort(key=lambda x: x["distance_miles"])

    # FEMA flood zone data for the state
    flood_info = None
    if state:
        state_upper = state.upper()
        # Get flood zone distribution for the state's county matching the FIPS
        flood_q = (
            select(
                FemaFloodZone.fld_zone,
                FemaFloodZone.zone_subtype,
                func.count().label("zone_count"),
            )
            .where(FemaFloodZone.state_abbrev == state_upper)
            .group_by(FemaFloodZone.fld_zone, FemaFloodZone.zone_subtype)
            .order_by(func.count().desc())
            .limit(20)
        )
        flood_result = await db.execute(flood_q)
        zones = flood_result.all()

        # Count high-risk zones
        total_zones = sum(r.zone_count for r in zones)
        high_risk_zones = sum(
            r.zone_count for r in zones
            if FLOOD_RISK.get(r.fld_zone, "").startswith(("Very High", "High"))
        )

        flood_info = {
            "state": state_upper,
            "total_flood_areas": total_zones,
            "high_risk_areas": high_risk_zones,
            "high_risk_pct": round(high_risk_zones / total_zones * 100, 1) if total_zones else 0,
            "zone_breakdown": [
                {
                    "zone": r.fld_zone,
                    "subtype": r.zone_subtype,
                    "count": r.zone_count,
                    "risk_level": FLOOD_RISK.get(r.fld_zone, "Unknown"),
                }
                for r in zones
            ],
        }

    # Risk summary
    risk_score = "Low"
    if len(nearby) >= 5:
        risk_score = "High"
    elif len(nearby) >= 2:
        risk_score = "Moderate"
    elif len(nearby) >= 1:
        risk_score = "Low-Moderate"

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/environmental/risk",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    return {
        "location": {"lat": lat, "lng": lng, "radius_miles": radius_miles},
        "environmental_risk_score": risk_score,
        "epa_facilities": {
            "total_nearby": len(nearby),
            "facilities": nearby[:50],
        },
        "flood_zones": flood_info,
    }


@router.get("/epa/search")
async def search_epa_facilities(
    request: Request,
    state: str = Query(..., max_length=2, description="State abbreviation"),
    city: str | None = Query(None),
    name: str | None = Query(None, description="Facility name search"),
    page: int = Query(1, ge=1, le=20),
    page_size: int = Query(25, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """Search EPA-regulated facilities by state, city, or name."""
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="EPA facility search requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    conditions = [EpaFacility.state == state.upper()]
    if city:
        conditions.append(EpaFacility.city.ilike(f"%{city}%"))
    if name:
        conditions.append(EpaFacility.name.ilike(f"%{name}%"))

    where = and_(*conditions)

    query = (
        select(EpaFacility)
        .where(where)
        .order_by(EpaFacility.name)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(query)
    facilities = result.scalars().all()

    count_q = select(func.count()).select_from(EpaFacility).where(where)
    total = (await db.execute(count_q)).scalar() or 0

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/environmental/epa/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    return {
        "results": [
            {
                "registry_id": f.registry_id,
                "name": f.name,
                "address": f.address,
                "city": f.city,
                "state": f.state,
                "zip": f.zip,
                "county": f.county,
                "lat": f.lat,
                "lng": f.lng,
            }
            for f in facilities
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/flood-zones")
async def flood_zone_stats(
    request: Request,
    state: str = Query(..., max_length=2, description="State abbreviation"),
    db: AsyncSession = Depends(get_read_db),
):
    """Public endpoint — FEMA flood zone statistics by state."""
    state_upper = state.upper()
    zone_q = (
        select(
            FemaFloodZone.fld_zone,
            func.count().label("count"),
        )
        .where(FemaFloodZone.state_abbrev == state_upper)
        .group_by(FemaFloodZone.fld_zone)
        .order_by(func.count().desc())
    )
    zones = (await db.execute(zone_q)).all()

    total = sum(r.count for r in zones)
    sfha_count_q = (
        select(func.count())
        .select_from(FemaFloodZone)
        .where(
            and_(
                FemaFloodZone.state_abbrev == state_upper,
                FemaFloodZone.sfha_tf == "T",
            )
        )
    )
    sfha_count = (await db.execute(sfha_count_q)).scalar() or 0

    return {
        "state": state_upper,
        "total_flood_areas": total,
        "sfha_areas": sfha_count,
        "sfha_pct": round(sfha_count / total * 100, 1) if total else 0,
        "zone_breakdown": {r.fld_zone: r.count for r in zones},
    }


@router.get("/stats")
async def environmental_stats(
    request: Request,
    db: AsyncSession = Depends(get_read_db),
):
    """Public endpoint — environmental database statistics."""
    epa_total = await fast_count(db, "epa_facilities")

    epa_states = await safe_query(db,
        select(EpaFacility.state, func.count().label("count"))
        .group_by(EpaFacility.state)
        .order_by(func.count().desc())
        .limit(10)
    )

    fema_total = await fast_count(db, "fema_flood_zones")

    fema_states = await safe_query(db,
        select(FemaFloodZone.state_abbrev, func.count().label("count"))
        .group_by(FemaFloodZone.state_abbrev)
        .order_by(func.count().desc())
        .limit(10)
    )

    return {
        "epa_facilities": {
            "total": epa_total,
            "top_states": {r.state: r.count for r in epa_states},
        },
        "fema_flood_zones": {
            "total": fema_total,
            "top_states": {r.state_abbrev: r.count for r in fema_states},
        },
    }
