"""Oil & gas well intelligence endpoints.

Data: canonical.wells (1.0M TX wellbores, RRC wellbore EWA, 92% geocoded),
canonical.well_permits (850K W-1 drilling permits, refreshed nightly from
RRC daf420), canonical.operators (78K P-5 organizations).

Search endpoints require Pro Leads plan or higher (same gate as the other
premium layers). Stats endpoints are public.
"""

import math
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy import and_, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

# canonical.* lives only on the primary; r730-2 replica does not
# replicate the canonical schema yet. Pin to get_db until it does.
from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, resolve_plan
from app.models.oil_gas import Operator, Well, WellPermit
from app.services.fast_counts import fast_count, safe_query
from app.services.usage_logger import log_usage

wells_router = APIRouter(prefix="/wells", tags=["Oil & Gas Wells"])
well_permits_router = APIRouter(prefix="/well-permits", tags=["Oil & Gas Drilling Permits"])

BBOX_HELP = "Bounding box: min_lng,min_lat,max_lng,max_lat (WGS84)"


def _require_pro(user: ApiUser) -> None:
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Oil & gas data requires Pro Leads plan or higher.",
        )


def _bbox_clause(bbox: str | None):
    if not bbox:
        return None
    try:
        min_lng, min_lat, max_lng, max_lat = (float(p) for p in bbox.split(","))
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid bbox. {BBOX_HELP}")
    if not (min_lng < max_lng and min_lat < max_lat):
        raise HTTPException(status_code=400, detail=f"Invalid bbox extent. {BBOX_HELP}")
    return text(
        "geom && ST_MakeEnvelope(:min_lng, :min_lat, :max_lng, :max_lat, 4326)"
    ).bindparams(min_lng=min_lng, min_lat=min_lat, max_lng=max_lng, max_lat=max_lat)


def _serialize_well(w: Well) -> dict:
    return {
        "api14": w.api14,
        "api10": w.api10,
        "state": w.state,
        "well_name": w.well_name,
        "well_number": w.well_number,
        "operator_name": w.operator_name_raw,
        "lease_name": w.lease_name,
        "lease_number": w.lease_number,
        "district": w.district,
        "county": w.county,
        "field_name": w.field_name,
        "well_type": w.well_type,
        "status": w.status,
        "completion_date": w.completion_date.isoformat() if w.completion_date else None,
        "plug_date": w.plug_date.isoformat() if w.plug_date else None,
        "total_depth": float(w.total_depth) if w.total_depth is not None else None,
        "lat": w.lat,
        "lng": w.lng,
        "source": w.source,
        "freshness_at": w.freshness_at.isoformat() if w.freshness_at else None,
    }


def _serialize_permit(p: WellPermit) -> dict:
    return {
        "permit_number": p.permit_number,
        "state": p.state,
        "api10": p.api10,
        "operator_number": p.operator_number,
        "operator_name": p.operator_name_raw,
        "lease_name": p.lease_name,
        "well_number": p.well_number,
        "district": p.district,
        "county": p.county,
        "field_name": p.field_name,
        "wellbore_profile": p.wellbore_profile,
        "filing_purpose": p.filing_purpose,
        "amended": p.amended,
        "total_depth": float(p.total_depth) if p.total_depth is not None else None,
        "status": p.current_status,
        "status_date": p.status_date.isoformat() if p.status_date else None,
        "submitted_date": p.submitted_date.isoformat() if p.submitted_date else None,
        "approved_date": p.approved_date.isoformat() if p.approved_date else None,
        "spud_date": p.spud_date.isoformat() if p.spud_date else None,
        "lat": p.lat,
        "lng": p.lng,
        "source": p.source,
        "freshness_at": p.freshness_at.isoformat() if p.freshness_at else None,
    }


# ---------------------------------------------------------------- wells ----

@wells_router.get("/search")
async def well_search(
    request: Request,
    county: str | None = Query(None, description="County name (exact, case-insensitive)"),
    district: str | None = Query(None, description="RRC district (01-10, 6E, 7B, 7C, 8A)"),
    operator: str | None = Query(None, min_length=3, description="Operator name (substring)"),
    operator_number: str | None = Query(None, description="P-5 operator number"),
    field: str | None = Query(None, min_length=3, description="Field name (substring)"),
    well_type: str | None = Query(None, description="oil | gas"),
    status: str | None = Query(None, description="Well status, e.g. PRODUCING, SHUT IN"),
    api: str | None = Query(None, description="API number (10 or 14 digit, with or without 42 prefix)"),
    state: str = Query("TX", max_length=2),
    completed_after: date | None = Query(None),
    completed_before: date | None = Query(None),
    bbox: str | None = Query(None, description=BBOX_HELP),
    page: int = Query(1, ge=1, le=400),
    limit: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Search oil & gas wellbores. Requires Pro Leads plan or higher."""
    _require_pro(user)
    await check_rate_limit(request, lookup_count=1)

    conditions = [Well.state == state.upper()]
    if county:
        conditions.append(func.upper(Well.county) == county.upper())
    if district:
        conditions.append(Well.district == district.zfill(2) if district.isdigit() else Well.district == district.upper())
    if operator:
        conditions.append(Well.operator_name_raw.ilike(f"%{operator}%"))
    if operator_number:
        conditions.append(Well.lineage["operator_number"].astext == operator_number)
    if field:
        conditions.append(Well.field_name.ilike(f"%{field}%"))
    if well_type:
        conditions.append(Well.well_type == well_type.lower())
    if status:
        conditions.append(Well.status.ilike(f"%{status}%"))
    if api:
        digits = "".join(c for c in api if c.isdigit())
        if len(digits) == 8:
            digits = "42" + digits
        conditions.append(Well.api14 == digits if len(digits) == 14 else Well.api10 == digits)
    if completed_after:
        conditions.append(Well.completion_date >= completed_after)
    if completed_before:
        conditions.append(Well.completion_date <= completed_before)
    bbox_clause = _bbox_clause(bbox)
    if bbox_clause is not None:
        conditions.append(bbox_clause)

    count_q = select(func.count()).select_from(Well).where(and_(*conditions))
    total = (await db.execute(count_q)).scalar() or 0

    rows = (await db.execute(
        select(Well)
        .where(and_(*conditions))
        .order_by(Well.completion_date.desc().nullslast())
        .offset((page - 1) * limit)
        .limit(limit)
    )).scalars().all()

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/wells/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    return {
        "query": {
            "state": state, "county": county, "district": district,
            "operator": operator, "field": field, "well_type": well_type,
            "status": status, "api": api, "bbox": bbox,
        },
        "results": [_serialize_well(w) for w in rows],
        "total": total,
        "page": page,
        "pages": math.ceil(total / limit) if total else 0,
    }


@wells_router.get("/stats")
async def well_stats(db: AsyncSession = Depends(get_db)):
    """Public endpoint — well database statistics."""
    total = await fast_count(db, "wells", schema="canonical")

    by_status = await safe_query(db,
        select(Well.status, func.count().label("count"))
        .where(Well.state == "TX")
        .group_by(Well.status).order_by(func.count().desc()).limit(12)
    )
    by_county = await safe_query(db,
        select(Well.county, func.count().label("count"))
        .where(Well.state == "TX")
        .group_by(Well.county).order_by(func.count().desc()).limit(15)
    )
    return {
        "total_records": total,
        "statuses": {r.status: r.count for r in by_status if r.status},
        "top_counties": {r.county: r.count for r in by_county if r.county},
    }


@wells_router.get("/{api_number}")
async def well_detail(
    api_number: str,
    request: Request,
    state: str = Query("TX", max_length=2),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Well detail by API number (8/10/14-digit), incl. operator + permits."""
    _require_pro(user)
    await check_rate_limit(request, lookup_count=1)

    digits = "".join(c for c in api_number if c.isdigit())
    if len(digits) == 8:
        digits = "42" + digits
    if len(digits) not in (10, 14):
        raise HTTPException(status_code=400, detail="API number must be 8, 10, or 14 digits.")
    api10 = digits[:10]

    well = (await db.execute(
        select(Well).where(Well.state == state.upper(), Well.api10 == api10).limit(1)
    )).scalars().first()
    if not well:
        raise HTTPException(status_code=404, detail=f"No well found for API {api10}.")

    operator = None
    if well.operator_id:
        operator = (await db.execute(
            select(Operator).where(Operator.id == well.operator_id)
        )).scalars().first()

    permits = (await db.execute(
        select(WellPermit)
        .where(WellPermit.state == state.upper(), WellPermit.api10 == api10)
        .order_by(WellPermit.approved_date.desc().nullslast())
        .limit(25)
    )).scalars().all()

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/wells/detail",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    body = _serialize_well(well)
    body["operator"] = {
        "operator_number": operator.operator_number,
        "name": operator.name,
        "p5_status": operator.p5_status,
        "organization_kind": operator.organization_kind,
    } if operator else None
    body["drilling_permits"] = [_serialize_permit(p) for p in permits]
    return body


# --------------------------------------------------------- well permits ----

@well_permits_router.get("/search")
async def well_permit_search(
    request: Request,
    county: str | None = Query(None, description="County name (exact, case-insensitive)"),
    district: str | None = Query(None, description="RRC district"),
    operator: str | None = Query(None, min_length=3, description="Operator name (substring)"),
    operator_number: str | None = Query(None, description="P-5 operator number"),
    status: str | None = Query(None, description="approved | pending | cancelled | withdrawn ..."),
    purpose: str | None = Query(None, description="Filing purpose, e.g. 'new drill', 'recompletion'"),
    profile: str | None = Query(None, description="horizontal | vertical | directional | sidetrack"),
    lease: str | None = Query(None, min_length=3, description="Lease name (substring)"),
    state: str = Query("TX", max_length=2),
    approved_after: date | None = Query(None),
    approved_before: date | None = Query(None),
    submitted_after: date | None = Query(None),
    bbox: str | None = Query(None, description=BBOX_HELP),
    page: int = Query(1, ge=1, le=400),
    limit: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Search W-1 drilling permits (nightly RRC refresh).

    Requires Pro Leads plan or higher. Default sort: approved_date desc, so
    `county=MIDLAND&approved_after=...` is the "what's being drilled near me
    this week" query.
    """
    _require_pro(user)
    await check_rate_limit(request, lookup_count=1)

    conditions = [WellPermit.state == state.upper()]
    if county:
        conditions.append(func.upper(WellPermit.county) == county.upper())
    if district:
        conditions.append(WellPermit.district == (district.zfill(2) if district.isdigit() else district.upper()))
    if operator:
        conditions.append(WellPermit.operator_name_raw.ilike(f"%{operator}%"))
    if operator_number:
        conditions.append(WellPermit.operator_number == operator_number)
    if status:
        conditions.append(WellPermit.current_status == status.lower())
    if purpose:
        conditions.append(WellPermit.filing_purpose.ilike(f"%{purpose}%"))
    if profile:
        conditions.append(WellPermit.wellbore_profile == profile.lower())
    if lease:
        conditions.append(WellPermit.lease_name.ilike(f"%{lease}%"))
    if approved_after:
        conditions.append(WellPermit.approved_date >= approved_after)
    if approved_before:
        conditions.append(WellPermit.approved_date <= approved_before)
    if submitted_after:
        conditions.append(WellPermit.submitted_date >= submitted_after)
    bbox_clause = _bbox_clause(bbox)
    if bbox_clause is not None:
        conditions.append(bbox_clause)

    count_q = select(func.count()).select_from(WellPermit).where(and_(*conditions))
    total = (await db.execute(count_q)).scalar() or 0

    rows = (await db.execute(
        select(WellPermit)
        .where(and_(*conditions))
        .order_by(WellPermit.approved_date.desc().nullslast())
        .offset((page - 1) * limit)
        .limit(limit)
    )).scalars().all()

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/well-permits/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    return {
        "query": {
            "state": state, "county": county, "district": district,
            "operator": operator, "status": status, "purpose": purpose,
            "profile": profile, "lease": lease,
            "approved_after": approved_after.isoformat() if approved_after else None,
            "bbox": bbox,
        },
        "results": [_serialize_permit(p) for p in rows],
        "total": total,
        "page": page,
        "pages": math.ceil(total / limit) if total else 0,
    }


@well_permits_router.get("/stats")
async def well_permit_stats(db: AsyncSession = Depends(get_db)):
    """Public endpoint — drilling permit statistics + freshness."""
    total = await fast_count(db, "well_permits", schema="canonical")

    latest = (await db.execute(
        select(func.max(WellPermit.approved_date)).where(WellPermit.state == "TX")
    )).scalar()

    last30 = (await db.execute(
        select(func.count()).select_from(WellPermit).where(
            WellPermit.state == "TX",
            WellPermit.approved_date >= func.current_date() - 30,
        )
    )).scalar() or 0

    by_county_30d = await safe_query(db,
        select(WellPermit.county, func.count().label("count"))
        .where(
            WellPermit.state == "TX",
            WellPermit.approved_date >= func.current_date() - 30,
        )
        .group_by(WellPermit.county).order_by(func.count().desc()).limit(15)
    )
    by_profile_30d = await safe_query(db,
        select(WellPermit.wellbore_profile, func.count().label("count"))
        .where(
            WellPermit.state == "TX",
            WellPermit.approved_date >= func.current_date() - 30,
        )
        .group_by(WellPermit.wellbore_profile).order_by(func.count().desc())
    )
    return {
        "total_records": total,
        "latest_approved_date": latest.isoformat() if latest else None,
        "approved_last_30_days": last30,
        "top_counties_30d": {r.county: r.count for r in by_county_30d if r.county},
        "profiles_30d": {r.wellbore_profile: r.count for r in by_profile_30d if r.wellbore_profile},
    }


@well_permits_router.get("/{permit_number}")
async def well_permit_detail(
    permit_number: str,
    request: Request,
    state: str = Query("TX", max_length=2),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Drilling permit detail by RRC status/permit number."""
    _require_pro(user)
    await check_rate_limit(request, lookup_count=1)

    permit = (await db.execute(
        select(WellPermit).where(
            WellPermit.state == state.upper(),
            WellPermit.permit_number == permit_number.lstrip("0").zfill(7),
        ).limit(1)
    )).scalars().first()
    if not permit:
        raise HTTPException(status_code=404, detail=f"No permit {permit_number} found.")

    operator = None
    if permit.operator_id:
        operator = (await db.execute(
            select(Operator).where(Operator.id == permit.operator_id)
        )).scalars().first()

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/well-permits/detail",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    body = _serialize_permit(permit)
    body["lineage"] = permit.lineage
    body["operator"] = {
        "operator_number": operator.operator_number,
        "name": operator.name,
        "p5_status": operator.p5_status,
        "organization_kind": operator.organization_kind,
    } if operator else None
    return body
