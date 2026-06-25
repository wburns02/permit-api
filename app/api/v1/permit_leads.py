"""Permit Leads API — Brazoria TX building-permit / new-build lead feed (Phase 3).

Serves the `brazoria_permit_leads` materialized view: one deduplicated,
classified, geocoded lead per property address, built from the Brazoria
`hot_leads` sources (Angleton MGO permits + Brazoria County 911 new-address
triggers, plus any future Brazoria source registered in
`app/services/permit_lead_classify.BRAZORIA_SOURCES`).

Endpoints:
    GET /v1/permit-leads/             — paginated, filterable list
    GET /v1/permit-leads/stats        — counts by lead_class + coverage gaps
    GET /v1/permit-leads/export.csv   — CSV export (cap 25,000)

Auth: demo key via X-API-Key (settings.DEMO_API_KEY), same posture as
/v1/hail-leads (reuses `require_demo_key`).

The backing MV is created WITH NO DATA and only populated by the nightly
mv_refresh job, so an unpopulated MV is the EXPECTED initial state — a SELECT
raises Postgres SQLSTATE 55000 which we treat as "no leads yet" (empty list),
never a 500.
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.hail_leads import _is_mv_unpopulated, require_demo_key
from app.database import get_read_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/permit-leads", tags=["Permit Leads"])

_PAGE_SIZE_CAP = 200
_EXPORT_MAX_ROWS = 25_000
_STATEMENT_TIMEOUT = "20s"

_LEAD_CLASSES = ("new_construction", "addition", "remodel", "other")


class PermitLead(BaseModel):
    """One deduplicated Brazoria permit lead (frozen MV column contract)."""
    address: str | None
    city: str | None
    zip: str | None
    county: str | None
    owner_name: str | None
    lead_class: str | None
    event_date: date | None
    last_event_date: date | None
    primary_source: str | None
    sources: list[str]
    contributing_rows: int | None
    lat: float | None
    lng: float | None
    geocoded: bool | None
    permit_number: str | None
    permit_type: str | None
    work_class: str | None
    description: str | None
    valuation: float | None


class PermitLeadsResponse(BaseModel):
    results: list[PermitLead]
    total: int
    page: int
    page_size: int
    total_pages: int


class PermitLeadsClassCount(BaseModel):
    lead_class: str
    count: int


class PermitLeadsStats(BaseModel):
    total_leads: int
    by_class: list[PermitLeadsClassCount]
    counties_covered: int
    sources_covered: int
    with_coords: int
    without_coords: int
    geocoded_backfilled: int
    missing_owner_name: int
    latest_event_date: date | None


def _build_filter_sql(
    *,
    county: str | None,
    lead_class: str | None,
    source: str | None,
    from_date: date | None,
    to_date: date | None,
    has_coords: bool | None,
) -> tuple[str, dict[str, Any]]:
    """Return (where_sql, params). All user input is bound, never interpolated."""
    clauses: list[str] = ["TRUE"]
    params: dict[str, Any] = {}

    if county:
        clauses.append("bpl.county ILIKE :county")
        params["county"] = county
    if lead_class:
        if lead_class not in _LEAD_CLASSES:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid lead_class: {lead_class} (allowed: {', '.join(_LEAD_CLASSES)})",
            )
        clauses.append("bpl.lead_class = :lead_class")
        params["lead_class"] = lead_class
    if source:
        # Match either the richest-row source OR any contributing source.
        clauses.append("(bpl.primary_source = :source OR :source = ANY(bpl.sources))")
        params["source"] = source
    if from_date is not None:
        clauses.append("bpl.event_date >= :from_date")
        params["from_date"] = from_date
    if to_date is not None:
        clauses.append("bpl.event_date <= :to_date")
        params["to_date"] = to_date
    if has_coords is True:
        clauses.append("(bpl.lat IS NOT NULL AND bpl.lng IS NOT NULL)")
    elif has_coords is False:
        clauses.append("(bpl.lat IS NULL OR bpl.lng IS NULL)")

    return " AND ".join(clauses), params


@router.get(
    "/stats",
    response_model=PermitLeadsStats,
    dependencies=[Depends(require_demo_key)],
)
async def permit_leads_stats(
    county: str | None = Query(None, max_length=100),
    db: AsyncSession = Depends(get_read_db),
) -> PermitLeadsStats:
    """Headline KPIs for the Brazoria permit-lead feed: counts by lead_class +
    contact-coverage gaps (so Phase 4 skip-trace can be scoped).

    Empty/unpopulated MV returns a zeroed payload, never a 500.
    """
    where = "bpl.county ILIKE :county" if county else "TRUE"
    params: dict[str, Any] = {"county": county} if county else {}
    try:
        await db.execute(text(f"SET LOCAL statement_timeout = '{_STATEMENT_TIMEOUT}'"))
        rows = (await db.execute(text(f"""
            SELECT lead_class, count(*)::bigint AS n
              FROM brazoria_permit_leads bpl
             WHERE {where}
             GROUP BY lead_class
        """), params)).mappings().all()
        agg = (await db.execute(text(f"""
            SELECT
                count(*)::bigint                                          AS total,
                count(DISTINCT county)::bigint                            AS counties,
                count(DISTINCT primary_source)::bigint                    AS sources,
                count(*) FILTER (WHERE lat IS NOT NULL AND lng IS NOT NULL)::bigint AS with_coords,
                count(*) FILTER (WHERE lat IS NULL OR lng IS NULL)::bigint AS without_coords,
                count(*) FILTER (WHERE geocoded)::bigint                  AS geocoded,
                count(*) FILTER (WHERE owner_name IS NULL OR owner_name = '')::bigint AS no_owner,
                max(event_date)                                          AS latest
              FROM brazoria_permit_leads bpl
             WHERE {where}
        """), params)).first()
    except Exception as exc:  # noqa: BLE001
        try:
            await db.rollback()
        except Exception:  # noqa: BLE001
            pass
        if _is_mv_unpopulated(exc):
            logger.info("brazoria_permit_leads MV not yet populated; zeroed stats")
            return PermitLeadsStats(
                total_leads=0, by_class=[], counties_covered=0, sources_covered=0,
                with_coords=0, without_coords=0, geocoded_backfilled=0,
                missing_owner_name=0, latest_event_date=None,
            )
        logger.warning("permit-leads stats failed: %s", exc)
        raise HTTPException(
            status_code=503, detail="Permit leads stats temporarily unavailable."
        ) from exc

    by_class = [
        PermitLeadsClassCount(lead_class=r["lead_class"] or "unknown", count=int(r["n"]))
        for r in rows
    ]
    return PermitLeadsStats(
        total_leads=int(agg.total or 0) if agg else 0,
        by_class=by_class,
        counties_covered=int(agg.counties or 0) if agg else 0,
        sources_covered=int(agg.sources or 0) if agg else 0,
        with_coords=int(agg.with_coords or 0) if agg else 0,
        without_coords=int(agg.without_coords or 0) if agg else 0,
        geocoded_backfilled=int(agg.geocoded or 0) if agg else 0,
        missing_owner_name=int(agg.no_owner or 0) if agg else 0,
        latest_event_date=agg.latest if agg else None,
    )


_SELECT_COLS = """
    bpl.address               AS address,
    bpl.city                  AS city,
    bpl.zip                   AS zip,
    bpl.county                AS county,
    bpl.owner_name            AS owner_name,
    bpl.lead_class            AS lead_class,
    bpl.event_date            AS event_date,
    bpl.last_event_date       AS last_event_date,
    bpl.primary_source        AS primary_source,
    bpl.sources               AS sources,
    bpl.contributing_rows     AS contributing_rows,
    bpl.lat                   AS lat,
    bpl.lng                   AS lng,
    bpl.geocoded              AS geocoded,
    bpl.permit_number         AS permit_number,
    bpl.permit_type           AS permit_type,
    bpl.work_class            AS work_class,
    bpl.description           AS description,
    bpl.valuation             AS valuation
"""


@router.get(
    "/",
    response_model=PermitLeadsResponse,
    dependencies=[Depends(require_demo_key)],
)
async def permit_leads_list(
    county: str | None = Query(
        None, max_length=100,
        description="County name, case-insensitive (e.g. Brazoria). Omit for all.",
    ),
    lead_class: str | None = Query(
        None,
        description="new_construction | addition | remodel | other",
    ),
    source: str | None = Query(
        None, max_length=100,
        description="Filter to leads contributed by this hot_leads source.",
    ),
    from_date: date | None = Query(
        None, description="Only leads with event_date on/after this date.",
    ),
    to_date: date | None = Query(
        None, description="Only leads with event_date on/before this date.",
    ),
    has_coords: bool | None = Query(
        None, description="true = only geocoded leads, false = only missing coords.",
    ),
    page: int = Query(1, ge=1, le=10000),
    page_size: int = Query(50, ge=1, le=_PAGE_SIZE_CAP),
    db: AsyncSession = Depends(get_read_db),
) -> PermitLeadsResponse:
    """Paginated Brazoria permit leads, newest trigger first.

    The backing MV is created WITH NO DATA; until the nightly refresh runs this
    returns an empty list (never a 500).
    """
    where_sql, params = _build_filter_sql(
        county=county, lead_class=lead_class, source=source,
        from_date=from_date, to_date=to_date, has_coords=has_coords,
    )
    offset = (page - 1) * page_size
    page_params = {**params, "_limit": page_size, "_offset": offset}

    rows_sql = f"""
        SELECT {_SELECT_COLS}
          FROM brazoria_permit_leads bpl
         WHERE {where_sql}
         ORDER BY bpl.event_date DESC NULLS LAST, bpl.address_norm ASC
         LIMIT :_limit OFFSET :_offset
    """
    count_sql = f"SELECT count(*) FROM brazoria_permit_leads bpl WHERE {where_sql}"

    try:
        await db.execute(text(f"SET LOCAL statement_timeout = '{_STATEMENT_TIMEOUT}'"))
        total = int((await db.execute(text(count_sql), params)).scalar() or 0)
        result = await db.execute(text(rows_sql), page_params)
        rows = result.mappings().all()
    except Exception as exc:  # noqa: BLE001
        try:
            await db.rollback()
        except Exception:  # noqa: BLE001
            pass
        if _is_mv_unpopulated(exc):
            logger.info("brazoria_permit_leads MV not yet populated; empty list")
            return PermitLeadsResponse(
                results=[], total=0, page=page, page_size=page_size, total_pages=0
            )
        logger.warning("permit-leads list query failed: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="Permit leads temporarily unavailable — please retry.",
        ) from exc

    items = [
        PermitLead(
            address=r["address"],
            city=r["city"],
            zip=r["zip"],
            county=r["county"],
            owner_name=r["owner_name"],
            lead_class=r["lead_class"],
            event_date=r["event_date"],
            last_event_date=r["last_event_date"],
            primary_source=r["primary_source"],
            sources=list(r["sources"]) if r["sources"] is not None else [],
            contributing_rows=int(r["contributing_rows"]) if r["contributing_rows"] is not None else None,
            lat=float(r["lat"]) if r["lat"] is not None else None,
            lng=float(r["lng"]) if r["lng"] is not None else None,
            geocoded=bool(r["geocoded"]) if r["geocoded"] is not None else None,
            permit_number=r["permit_number"],
            permit_type=r["permit_type"],
            work_class=r["work_class"],
            description=r["description"],
            valuation=float(r["valuation"]) if r["valuation"] is not None else None,
        )
        for r in rows
    ]
    total_pages = (max(total, 0) + page_size - 1) // page_size
    return PermitLeadsResponse(
        results=items, total=total, page=page,
        page_size=page_size, total_pages=total_pages,
    )


_EXPORT_COLUMNS = [
    "Address", "City", "Zip", "County", "Owner Name", "Lead Class",
    "Event Date", "Last Event Date", "Primary Source", "Sources",
    "Lat", "Lng", "Geocoded", "Permit Number", "Permit Type",
    "Work Class", "Description", "Valuation",
]


@router.get(
    "/export.csv",
    dependencies=[Depends(require_demo_key)],
)
async def permit_leads_export_csv(
    county: str | None = Query(None, max_length=100),
    lead_class: str | None = Query(None),
    source: str | None = Query(None, max_length=100),
    from_date: date | None = Query(None),
    to_date: date | None = Query(None),
    has_coords: bool | None = Query(None),
    db: AsyncSession = Depends(get_read_db),
) -> StreamingResponse:
    """Export filtered Brazoria permit leads as CSV (cap 25,000).

    Empty MV (pre-refresh) yields a header-only CSV, never a 500.
    """
    where_sql, params = _build_filter_sql(
        county=county, lead_class=lead_class, source=source,
        from_date=from_date, to_date=to_date, has_coords=has_coords,
    )
    params["_limit"] = _EXPORT_MAX_ROWS
    rows_sql = f"""
        SELECT {_SELECT_COLS}
          FROM brazoria_permit_leads bpl
         WHERE {where_sql}
         ORDER BY bpl.event_date DESC NULLS LAST, bpl.address_norm ASC
         LIMIT :_limit
    """
    rows: list[Any] = []
    try:
        await db.execute(text(f"SET LOCAL statement_timeout = '{_STATEMENT_TIMEOUT}'"))
        result = await db.execute(text(rows_sql), params)
        rows = result.mappings().all()
    except Exception as exc:  # noqa: BLE001
        try:
            await db.rollback()
        except Exception:  # noqa: BLE001
            pass
        if _is_mv_unpopulated(exc):
            logger.info("brazoria_permit_leads MV not yet populated; empty CSV")
            rows = []
        else:
            logger.warning("permit-leads export failed: %s", exc)
            raise HTTPException(
                status_code=503,
                detail="Permit leads export temporarily unavailable — please retry.",
            ) from exc

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_EXPORT_COLUMNS)
    for r in rows:
        srcs = r["sources"]
        writer.writerow([
            r["address"] or "",
            r["city"] or "",
            r["zip"] or "",
            r["county"] or "",
            r["owner_name"] or "",
            r["lead_class"] or "",
            r["event_date"].isoformat() if r["event_date"] else "",
            r["last_event_date"].isoformat() if r["last_event_date"] else "",
            r["primary_source"] or "",
            ";".join(srcs) if isinstance(srcs, list) else "",
            r["lat"] if r["lat"] is not None else "",
            r["lng"] if r["lng"] is not None else "",
            "yes" if r["geocoded"] else "",
            r["permit_number"] or "",
            r["permit_type"] or "",
            r["work_class"] or "",
            r["description"] or "",
            r["valuation"] if r["valuation"] is not None else "",
        ])

    buf.seek(0)
    county_slug = (county or "brazoria").lower().replace(" ", "-")
    today = date.today().isoformat()
    filename = f"permit-leads-{county_slug}-{today}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
