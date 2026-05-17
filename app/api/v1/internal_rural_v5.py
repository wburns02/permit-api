"""Internal Rural v5 endpoints — service-to-service proxy target.

Surfaces the v5 materialized views from the T430 permits DB so the Mac CRM
(react-crm-api) on Railway can reach them WITHOUT a direct Tailscale
connection. Auth is via the X-Internal-Key header (config.INTERNAL_API_KEY),
which short-circuits the standard API-key middleware to a synthetic
enterprise-tier user.

Schemas mirror react-crm-api `app/schemas/rural_leads.py` exactly so the CRM
side can deserialize without remapping.

Materialized views queried (all on T430 `permits` db):
  - rural_septic_score_v5        — 228k TX OSSF permits (prospect queue)
  - mac_customers_scored_v5      — 4,145 high-value Mac customers (retention)

This router is mounted at /v1/internal — every endpoint requires the
internal key. Routes:
  GET  /v1/internal/rural-leads/v5/prospects
  GET  /v1/internal/rural-leads/v5/customers
  GET  /v1/internal/rural-leads/v5/by-permit/{permit_id}
  GET  /v1/internal/rural-leads/v5/by-permits        (bulk fetch for /sync)
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/internal/rural-leads/v5", tags=["Internal — Rural v5"])


def _require_internal(request: Request) -> None:
    if not getattr(request.state, "is_internal", False):
        raise HTTPException(
            status_code=403,
            detail="This endpoint requires service-to-service auth (X-Internal-Key).",
        )


# ──────────────────────────────────────────────────────────────────────
#  Response schemas — MUST mirror react-crm-api/app/schemas/rural_leads.py
# ──────────────────────────────────────────────────────────────────────


class RuralProspectV5(BaseModel):
    permit_id: int
    permit_number: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    zip_code: Optional[str] = None
    county_name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    source: Optional[str] = None
    system_type: Optional[str] = None
    date_created: Optional[datetime] = None
    fiber_isp_count: int = 0
    cable_isp_count: int = 0
    satellite_isp_count: int = 0
    has_fiber: bool = False
    has_cable: bool = False
    only_satellite: bool = False
    in_urban_area: bool = False
    effective_lot_acres: Optional[float] = None
    parcel_id: Optional[str] = None
    rural_septic_score: int


class RuralProspectsV5Response(BaseModel):
    items: List[RuralProspectV5]
    total: int
    limit: int
    offset: int
    min_score: int


class RuralCustomerV5(BaseModel):
    id: int
    address_norm: Optional[str] = None
    city: Optional[str] = None
    zip: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    effective_lot_acres: Optional[float] = None
    fiber_isp_count: int = 0
    cable_isp_count: int = 0
    satellite_isp_count: int = 0
    only_satellite: bool = False
    has_fiber: bool = False
    in_urban_area: bool = False
    parcel_id: Optional[str] = None
    rural_septic_score: int


class RuralCustomersV5Response(BaseModel):
    items: List[RuralCustomerV5]
    total: int
    limit: int


# ──────────────────────────────────────────────────────────────────────
#  Endpoints
# ──────────────────────────────────────────────────────────────────────


_PROSPECT_COLUMNS = """
    permit_id, permit_number, address, city, zip_code, county_name,
    lat, lon, source, system_type, date_created,
    fiber_isp_count, cable_isp_count, satellite_isp_count,
    has_fiber, has_cable, only_satellite, in_urban_area,
    effective_lot_acres::float8 AS effective_lot_acres,
    parcel_id, rural_septic_score
"""


@router.get("/prospects", response_model=RuralProspectsV5Response)
async def internal_prospects(
    request: Request,
    min_score: int = Query(70, ge=0, le=100),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    county: Optional[str] = Query(None, description="Filter by county_name (ILIKE)"),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _require_internal(request)

    params: dict = {"min_score": min_score, "lim": limit, "off": offset}
    where_extra = ""
    if county:
        params["county"] = f"%{county}%"
        where_extra = " AND county_name ILIKE :county"

    rows_sql = text(f"""
        SELECT {_PROSPECT_COLUMNS}
        FROM rural_septic_score_v5
        WHERE rural_septic_score >= :min_score{where_extra}
        ORDER BY rural_septic_score DESC, permit_id
        LIMIT :lim OFFSET :off
    """)
    count_sql = text(f"""
        SELECT COUNT(*) FROM rural_septic_score_v5
        WHERE rural_septic_score >= :min_score{where_extra}
    """)

    try:
        rows = (await db.execute(rows_sql, params)).mappings().all()
        total = (await db.execute(count_sql, params)).scalar_one()
    except Exception as e:
        await db.rollback()
        logger.exception("internal rural v5 /prospects failed")
        raise HTTPException(status_code=500, detail=f"rural v5 prospects query failed: {e}")

    items = [RuralProspectV5(**dict(r)) for r in rows]
    return RuralProspectsV5Response(
        items=items, total=int(total or 0), limit=limit, offset=offset, min_score=min_score,
    )


@router.get("/customers", response_model=RuralCustomersV5Response)
async def internal_customers(
    request: Request,
    min_score: int = Query(70, ge=0, le=100),
    limit: int = Query(100, ge=1, le=500),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _require_internal(request)

    rows_sql = text("""
        SELECT
          id, address_norm, city, zip, lat, lon,
          effective_lot_acres::float8 AS effective_lot_acres,
          fiber_isp_count, cable_isp_count, satellite_isp_count,
          only_satellite, has_fiber, in_urban_area, parcel_id,
          rural_septic_score
        FROM mac_customers_scored_v5
        WHERE rural_septic_score >= :min_score
        ORDER BY rural_septic_score DESC, id
        LIMIT :lim
    """)
    count_sql = text("""
        SELECT COUNT(*) FROM mac_customers_scored_v5
        WHERE rural_septic_score >= :min_score
    """)
    params = {"min_score": min_score, "lim": limit}

    try:
        rows = (await db.execute(rows_sql, params)).mappings().all()
        total = (await db.execute(count_sql, params)).scalar_one()
    except Exception as e:
        await db.rollback()
        logger.exception("internal rural v5 /customers failed")
        raise HTTPException(status_code=500, detail=f"rural v5 customers query failed: {e}")

    items = [RuralCustomerV5(**dict(r)) for r in rows]
    return RuralCustomersV5Response(items=items, total=int(total or 0), limit=limit)


@router.get("/by-permit/{permit_id}", response_model=RuralProspectV5)
async def internal_by_permit(
    permit_id: int,
    request: Request,
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _require_internal(request)
    try:
        row = (await db.execute(
            text(f"""
                SELECT {_PROSPECT_COLUMNS}
                FROM rural_septic_score_v5
                WHERE permit_id = :pid
            """),
            {"pid": permit_id},
        )).mappings().first()
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"rural v5 by-permit query failed: {e}")

    if not row:
        raise HTTPException(status_code=404, detail=f"Permit {permit_id} not in rural_septic_score_v5")
    return RuralProspectV5(**dict(row))


class _PermitIdsRequest(BaseModel):
    permit_ids: List[int]


@router.post("/by-permits", response_model=List[RuralProspectV5])
async def internal_by_permits(
    body: _PermitIdsRequest,
    request: Request,
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Bulk fetch — used by /sync to pull top-N rows in one round-trip."""
    _require_internal(request)
    if not body.permit_ids:
        return []
    if len(body.permit_ids) > 2000:
        raise HTTPException(status_code=400, detail="max 2000 permit_ids per call")

    try:
        rows = (await db.execute(
            text(f"""
                SELECT {_PROSPECT_COLUMNS}
                FROM rural_septic_score_v5
                WHERE permit_id = ANY(:ids)
                ORDER BY rural_septic_score DESC, permit_id
            """),
            {"ids": body.permit_ids},
        )).mappings().all()
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"rural v5 by-permits query failed: {e}")

    return [RuralProspectV5(**dict(r)) for r in rows]


@router.get("/top-permit-ids")
async def internal_top_permit_ids(
    request: Request,
    min_score: int = Query(80, ge=0, le=100),
    max_count: int = Query(200, ge=1, le=2000),
    county: Optional[str] = Query(None),
    _user=Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Tiny endpoint that returns just the top-N permit_ids + minimal fields.

    Used by /sync on the CRM side to pre-compute the candidate set, dedupe
    against outbound_campaign_contacts, then call /by-permits to hydrate.
    Could be folded into /prospects but separating keeps the wire-size low.
    """
    _require_internal(request)
    params: dict = {"min_score": min_score, "lim": max_count}
    where_extra = ""
    if county:
        params["county"] = f"%{county}%"
        where_extra = " AND county_name ILIKE :county"

    try:
        rows = (await db.execute(
            text(f"""
                SELECT permit_id, address, city, zip_code, county_name, rural_septic_score
                FROM rural_septic_score_v5
                WHERE rural_septic_score >= :min_score{where_extra}
                ORDER BY rural_septic_score DESC, permit_id
                LIMIT :lim
            """),
            params,
        )).mappings().all()
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"rural v5 top-permit-ids query failed: {e}")

    return {
        "items": [dict(r) for r in rows],
        "count": len(rows),
        "min_score": min_score,
        "county": county,
    }
