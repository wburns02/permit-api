"""Hail Leads API — contractor-facing storm × permit lead flow.

Endpoints:
    GET    /v1/hail-leads/stats         — headline KPIs
    GET    /v1/hail-leads/              — paginated list with filters
    GET    /v1/hail-leads/export.csv    — CSV export of filtered leads (up to 10k)
    GET    /v1/hail-leads/{lead_id}     — full lead detail + enrichment
    POST   /v1/hail-leads/enrich        — admin-only BatchData skip-trace trigger

Auth: demo key via X-API-Key (settings.DEMO_API_KEY). /enrich additionally
requires X-Admin-Key (settings.DEMO_ADMIN_KEY).

Data sources (all on primary Postgres at 100.122.216.15:5432/permits):
    hail_leads               — materialized view (17.3M rows) joining storm_events × hot_leads
    hail_leads_categorized   — view adding lead_category (roof_replace/siding/gutter/solar)
    address_permit_history   — materialized view (828K rows) of per-address permit counts
    hail_leads_enriched      — cache table of BatchData skip-trace results
    tcad_year_built          — cache table of TCAD year-built / sqft / appraised value
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date, timedelta
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db, get_read_db
from app.schemas.hail_leads import (
    HailLeadAddressHistory,
    HailLeadDetail,
    HailLeadListItem,
    HailLeadListResponse,
    HailLeadOwner,
    HailLeadPermit,
    HailLeadPhone,
    HailLeadStorm,
    HailLeadsEnrichRequest,
    HailLeadsEnrichResponse,
    HailLeadsStats,
    LeadCategory,
    SortKey,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/hail-leads", tags=["Hail Leads"])


# ---------------------------------------------------------------------------
# Auth — simple header-based demo key; separate key gates /enrich admin ops.
# ---------------------------------------------------------------------------

def require_demo_key(request: Request) -> None:
    """Require X-API-Key header to equal settings.DEMO_API_KEY.

    If DEMO_API_KEY is unset (empty string), the endpoint is open — useful
    in local dev. In production always set a value.
    """
    expected = (settings.DEMO_API_KEY or "").strip()
    if not expected:
        return  # auth disabled
    provided = (request.headers.get("X-API-Key") or "").strip()
    if provided != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key.")


def require_admin_key(request: Request) -> None:
    """Require X-Admin-Key header to equal settings.DEMO_ADMIN_KEY."""
    expected = (settings.DEMO_ADMIN_KEY or "").strip()
    if not expected:
        # Admin key MUST be set — refuse to expose enrich without it.
        raise HTTPException(
            status_code=503,
            detail="Admin enrich endpoint is disabled (DEMO_ADMIN_KEY not configured).",
        )
    provided = (request.headers.get("X-Admin-Key") or "").strip()
    if provided != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Admin-Key.")


# ---------------------------------------------------------------------------
# Shared SQL fragments
# ---------------------------------------------------------------------------

# False-positive exclusion — descriptions that look like roof permits but aren't.
_FALSE_POSITIVE_REGEX = (
    r"(fence|privacy fence|iron fence|deck.*rail|stoop|patio cover|"
    r"ornamental|awning only|window only)"
)

# Keep in sync with HailLeadListItem.
_ALLOWED_CATEGORIES: tuple[str, ...] = ("roof_replace", "siding", "gutter", "solar")

_ADDRESS_NORM_SQL = (
    "UPPER(REGEXP_REPLACE(hl.address, '[^A-Za-z0-9 ]', ' ', 'g'))"
)


def _build_filter_sql(
    *,
    county: str | None,
    from_date: date | None,
    to_date: date | None,
    category: str | None,
    min_hail_inches: float | None,
    min_days_after: int | None,
    max_days_after: int | None,
) -> tuple[str, dict[str, Any]]:
    """Return (where_sql, params) for list/export endpoints."""
    params: dict[str, Any] = {}
    clauses: list[str] = [
        "hl.storm_type = 'Hail'",
        "hc.lead_category = ANY(:_allowed_categories)",
        "hl.address !~ '^[0-9]+$'",
        "hl.address IS NOT NULL",
        f"COALESCE(hl.description, '') !~* '{_FALSE_POSITIVE_REGEX}'",
    ]
    params["_allowed_categories"] = list(_ALLOWED_CATEGORIES)

    if county:
        clauses.append("hl.county ILIKE :county")
        params["county"] = county
    if from_date:
        clauses.append("hl.storm_date >= :from_date")
        params["from_date"] = from_date
    if to_date:
        clauses.append("hl.storm_date <= :to_date")
        params["to_date"] = to_date
    if category:
        if category not in _ALLOWED_CATEGORIES:
            raise HTTPException(status_code=400, detail=f"Invalid category: {category}")
        clauses.append("hc.lead_category = :category")
        params["category"] = category
    if min_hail_inches is not None:
        clauses.append("hl.storm_magnitude >= :min_hail_inches")
        params["min_hail_inches"] = min_hail_inches
    if min_days_after is not None:
        clauses.append("hl.days_after_storm >= :min_days_after")
        params["min_days_after"] = min_days_after
    if max_days_after is not None:
        clauses.append("hl.days_after_storm <= :max_days_after")
        params["max_days_after"] = max_days_after

    return " AND ".join(clauses), params


def _sort_expression(sort: str) -> str:
    """Translate sort key to SQL ORDER BY (outer query).

    Column names match the list SELECT aliases.
    """
    if sort == "storm_date_desc":
        return "storm_date DESC NULLS LAST, score DESC NULLS LAST"
    if sort == "issue_date_desc":
        return "permit_date DESC NULLS LAST, score DESC NULLS LAST"
    # default: score_desc
    return "score DESC NULLS LAST, storm_date DESC NULLS LAST"


# ---------------------------------------------------------------------------
# 1) GET /stats
# ---------------------------------------------------------------------------

@router.get(
    "/stats",
    response_model=HailLeadsStats,
    dependencies=[Depends(require_demo_key)],
)
async def hail_leads_stats(
    db: AsyncSession = Depends(get_read_db),
) -> HailLeadsStats:
    """Headline KPIs for the hail-leads dashboard header."""
    # Use pg_class reltuples for fast approximate count on the big MV.
    # reltuples is -1 after CREATE but refreshed by ANALYZE/REFRESH.
    total_leads_row = await db.execute(text(
        "SELECT GREATEST(reltuples, 0)::bigint AS n "
        "FROM pg_class WHERE relname = 'hail_leads'"
    ))
    total_leads = int(total_leads_row.scalar() or 0)

    # Unique addresses — approximate via address_permit_history count.
    uniq_row = await db.execute(text(
        "SELECT GREATEST(reltuples, 0)::bigint AS n "
        "FROM pg_class WHERE relname = 'address_permit_history'"
    ))
    unique_addresses = int(uniq_row.scalar() or 0)

    # Counties covered — distinct county count (small, fast).
    counties_row = await db.execute(text(
        "SELECT COUNT(DISTINCT county) FROM hail_leads WHERE county IS NOT NULL"
    ))
    counties_covered = int(counties_row.scalar() or 0)

    # Latest storm date — fast (indexed).
    latest_storm_row = await db.execute(text(
        "SELECT MAX(storm_date) FROM hail_leads"
    ))
    latest_storm_date = latest_storm_row.scalar()

    # Fresh leads this week — storms in last 7 days.
    fresh_row = await db.execute(text(
        "SELECT COUNT(*) FROM hail_leads "
        "WHERE storm_date >= CURRENT_DATE - INTERVAL '7 days'"
    ))
    fresh_leads_this_week = int(fresh_row.scalar() or 0)

    # Hail events in last year — distinct storm_event_id where type='Hail'.
    hail_events_row = await db.execute(text(
        "SELECT COUNT(DISTINCT storm_event_id) FROM hail_leads "
        "WHERE storm_type = 'Hail' "
        "AND storm_date >= CURRENT_DATE - INTERVAL '365 days'"
    ))
    hail_events_last_year = int(hail_events_row.scalar() or 0)

    return HailLeadsStats(
        total_leads=total_leads,
        unique_addresses=unique_addresses,
        counties_covered=counties_covered,
        latest_storm_date=latest_storm_date,
        fresh_leads_this_week=fresh_leads_this_week,
        hail_events_last_year=hail_events_last_year,
    )


# ---------------------------------------------------------------------------
# 2) GET / (list with filters)
# NOTE: must come BEFORE the /{lead_id} catch-all.
# ---------------------------------------------------------------------------

def _list_select_sql(order_by: str) -> str:
    """Build the DISTINCT ON (lead_id) SELECT used by list + export.

    Uses DISTINCT ON to keep highest-score row per lead_id, then re-sorts
    the collapsed set by the requested order.
    """
    return f"""
        WITH filtered AS (
            SELECT DISTINCT ON (hl.lead_id)
                hl.lead_id::text                                     AS lead_id,
                hl.address                                            AS address,
                hl.city                                               AS city,
                hl.zip                                                AS zip,
                hl.county                                             AS county,
                hl.storm_date                                         AS storm_date,
                hl.storm_type                                         AS storm_type,
                hl.storm_magnitude                                    AS hail_size_inches,
                hl.issue_date                                         AS permit_date,
                hl.days_after_storm                                   AS days_after_storm,
                hc.lead_category                                      AS lead_category,
                hl.description                                        AS permit_description,
                hl.contractor_company                                 AS competitor_contractor,
                hl.hail_lead_score                                    AS score,
                COALESCE(aph.roof_permit_count, 0)                    AS prior_roof_permits,
                aph.last_roof_permit_date                             AS last_roof_permit_date,
                (hle.lead_id IS NOT NULL)                             AS owner_enriched
            FROM hail_leads hl
            LEFT JOIN hail_leads_categorized hc
                   USING (lead_id, storm_event_id)
            LEFT JOIN address_permit_history aph
                   ON aph.address_norm = {_ADDRESS_NORM_SQL}
                  AND aph.zip = hl.zip
            LEFT JOIN hail_leads_enriched hle
                   ON hle.address_norm = aph.address_norm
                  AND hle.zip = aph.zip
            WHERE {{where_sql}}
            ORDER BY hl.lead_id, hl.hail_lead_score DESC NULLS LAST
        )
        SELECT * FROM filtered
        ORDER BY {order_by}
    """


@router.get(
    "/",
    response_model=HailLeadListResponse,
    dependencies=[Depends(require_demo_key)],
)
async def hail_leads_list(
    county: str | None = Query(None, max_length=100),
    from_date: date | None = Query(None),
    to_date: date | None = Query(None),
    category: LeadCategory | None = Query(None),
    min_hail_inches: float | None = Query(None, ge=0.0, le=10.0),
    min_days_after: int | None = Query(None, ge=0, le=365),
    max_days_after: int | None = Query(None, ge=0, le=365),
    page: int = Query(1, ge=1, le=10000),
    page_size: int = Query(50, ge=1, le=500),
    sort: SortKey = Query("score_desc"),
    db: AsyncSession = Depends(get_read_db),
) -> HailLeadListResponse:
    """Paginated list of hail leads with filters. See module docstring."""
    where_sql, params = _build_filter_sql(
        county=county,
        from_date=from_date,
        to_date=to_date,
        category=category,
        min_hail_inches=min_hail_inches,
        min_days_after=min_days_after,
        max_days_after=max_days_after,
    )

    base_select = _list_select_sql(_sort_expression(sort)).replace(
        "{where_sql}", where_sql
    )

    # Count (over collapsed distinct set)
    count_sql = (
        f"SELECT COUNT(*) FROM (SELECT DISTINCT hl.lead_id "
        f"FROM hail_leads hl "
        f"LEFT JOIN hail_leads_categorized hc USING (lead_id, storm_event_id) "
        f"LEFT JOIN address_permit_history aph "
        f"  ON aph.address_norm = {_ADDRESS_NORM_SQL} AND aph.zip = hl.zip "
        f"LEFT JOIN hail_leads_enriched hle "
        f"  ON hle.address_norm = aph.address_norm AND hle.zip = aph.zip "
        f"WHERE {where_sql}) _q"
    )

    # Cap count lookup so page requests remain snappy on huge filter sets.
    await db.execute(text("SET LOCAL statement_timeout = '20s'"))

    try:
        total_row = await db.execute(text(count_sql), params)
        total = int(total_row.scalar() or 0)
    except Exception as exc:  # noqa: BLE001
        logger.warning("hail-leads count failed, returning -1: %s", exc)
        total = -1  # signal "unknown"

    offset = (page - 1) * page_size
    page_params = {**params, "_limit": page_size, "_offset": offset}
    rows_sql = base_select + " LIMIT :_limit OFFSET :_offset"

    result = await db.execute(text(rows_sql), page_params)
    rows = result.mappings().all()

    items = [
        HailLeadListItem(
            lead_id=r["lead_id"],
            address=r["address"],
            city=r["city"],
            zip=r["zip"],
            county=r["county"],
            storm_date=r["storm_date"],
            storm_type=r["storm_type"],
            hail_size_inches=float(r["hail_size_inches"]) if r["hail_size_inches"] is not None else None,
            permit_date=r["permit_date"],
            days_after_storm=_days_int(r["days_after_storm"]),
            lead_category=r["lead_category"],
            permit_description=r["permit_description"],
            competitor_contractor=r["competitor_contractor"],
            score=float(r["score"]) if r["score"] is not None else None,
            prior_roof_permits=int(r["prior_roof_permits"]) if r["prior_roof_permits"] is not None else 0,
            last_roof_permit_date=r["last_roof_permit_date"],
            owner_enriched=bool(r["owner_enriched"]),
        )
        for r in rows
    ]

    total_pages = (
        (max(total, 0) + page_size - 1) // page_size if total >= 0 else -1
    )

    return HailLeadListResponse(
        results=items,
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
    )


def _days_int(v: Any) -> int | None:
    """Normalize days_after_storm which may be an interval or int."""
    if v is None:
        return None
    # asyncpg returns intervals as datetime.timedelta; plain ints pass through.
    if isinstance(v, timedelta):
        return int(v.days)
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 3) GET /export.csv  (static route MUST come BEFORE /{lead_id})
# ---------------------------------------------------------------------------

_EXPORT_MAX_ROWS = 10_000

_EXPORT_COLUMNS = [
    "Address", "City", "Zip", "County", "Storm Date", "Hail Size",
    "Permit Date", "Days After", "Category", "Description", "Competitor",
    "Year Built", "Prior Roofs", "Owner Name", "Phone 1", "Email 1",
]


@router.get(
    "/export.csv",
    dependencies=[Depends(require_demo_key)],
)
async def hail_leads_export_csv(
    county: str | None = Query(None, max_length=100),
    from_date: date | None = Query(None),
    to_date: date | None = Query(None),
    category: LeadCategory | None = Query(None),
    min_hail_inches: float | None = Query(None, ge=0.0, le=10.0),
    min_days_after: int | None = Query(None, ge=0, le=365),
    max_days_after: int | None = Query(None, ge=0, le=365),
    sort: SortKey = Query("score_desc"),
    db: AsyncSession = Depends(get_read_db),
) -> StreamingResponse:
    """Export filtered leads as CSV (cap 10,000 rows)."""
    where_sql, params = _build_filter_sql(
        county=county,
        from_date=from_date,
        to_date=to_date,
        category=category,
        min_hail_inches=min_hail_inches,
        min_days_after=min_days_after,
        max_days_after=max_days_after,
    )

    # Extend list query to also pull year_built and enriched owner/phone/email.
    rows_sql = f"""
        WITH filtered AS (
            SELECT DISTINCT ON (hl.lead_id)
                hl.lead_id::text                                     AS lead_id,
                hl.address                                            AS address,
                hl.city                                               AS city,
                hl.zip                                                AS zip,
                hl.county                                             AS county,
                hl.storm_date                                         AS storm_date,
                hl.storm_magnitude                                    AS hail_size_inches,
                hl.issue_date                                         AS permit_date,
                hl.days_after_storm                                   AS days_after_storm,
                hc.lead_category                                      AS lead_category,
                hl.description                                        AS description,
                hl.contractor_company                                 AS contractor,
                hl.hail_lead_score                                    AS score,
                COALESCE(aph.roof_permit_count, 0)                    AS prior_roof_permits,
                tyb.year_built                                        AS year_built,
                hle.owner_name                                        AS owner_name,
                hle.phones                                            AS phones,
                hle.emails                                            AS emails
            FROM hail_leads hl
            LEFT JOIN hail_leads_categorized hc
                   USING (lead_id, storm_event_id)
            LEFT JOIN address_permit_history aph
                   ON aph.address_norm = {_ADDRESS_NORM_SQL}
                  AND aph.zip = hl.zip
            LEFT JOIN hail_leads_enriched hle
                   ON hle.address_norm = aph.address_norm
                  AND hle.zip = aph.zip
            LEFT JOIN tcad_year_built tyb
                   ON tyb.address_norm = aph.address_norm
                  AND tyb.zip = aph.zip
            WHERE {where_sql}
            ORDER BY hl.lead_id, hl.hail_lead_score DESC NULLS LAST
        )
        SELECT * FROM filtered
        ORDER BY {_sort_expression(sort)}
        LIMIT :_limit
    """
    params["_limit"] = _EXPORT_MAX_ROWS

    result = await db.execute(text(rows_sql), params)
    rows = result.mappings().all()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(_EXPORT_COLUMNS)
    for r in rows:
        phone_1 = ""
        phones = r.get("phones")
        if isinstance(phones, list) and phones:
            first = phones[0] if isinstance(phones[0], dict) else None
            if first:
                phone_1 = str(first.get("number") or "")
        email_1 = ""
        emails = r.get("emails")
        if isinstance(emails, list) and emails:
            email_1 = str(emails[0]) if not isinstance(emails[0], dict) else str(emails[0].get("email") or "")

        writer.writerow([
            r["address"] or "",
            r["city"] or "",
            r["zip"] or "",
            r["county"] or "",
            r["storm_date"].isoformat() if r["storm_date"] else "",
            r["hail_size_inches"] if r["hail_size_inches"] is not None else "",
            r["permit_date"].isoformat() if r["permit_date"] else "",
            _days_int(r["days_after_storm"]) if r["days_after_storm"] is not None else "",
            r["lead_category"] or "",
            r["description"] or "",
            r["contractor"] or "",
            r["year_built"] if r["year_built"] is not None else "",
            r["prior_roof_permits"] if r["prior_roof_permits"] is not None else 0,
            r["owner_name"] or "",
            phone_1,
            email_1,
        ])

    buf.seek(0)
    county_slug = (county or "all").lower().replace(" ", "-")
    today = date.today().isoformat()
    filename = f"hail-leads-{county_slug}-{today}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# 4) POST /enrich  (admin-only — static route before /{lead_id})
# ---------------------------------------------------------------------------

_BATCHDATA_URL = "https://api.batchdata.com/api/v1/property/skip-trace"
_BATCHDATA_CHUNK = 50


async def _fetch_leads_for_enrich(
    db: AsyncSession, lead_ids: list[str]
) -> list[dict[str, Any]]:
    """Return [{lead_id, address, city, state, zip, address_norm}] for given IDs."""
    if not lead_ids:
        return []
    q = text(f"""
        SELECT DISTINCT ON (hl.lead_id)
            hl.lead_id::text    AS lead_id,
            hl.address          AS address,
            hl.city             AS city,
            hl.state            AS state,
            hl.zip              AS zip,
            {_ADDRESS_NORM_SQL} AS address_norm
        FROM hail_leads hl
        WHERE hl.lead_id::text = ANY(:lead_ids)
        ORDER BY hl.lead_id, hl.hail_lead_score DESC NULLS LAST
    """)
    result = await db.execute(q, {"lead_ids": lead_ids})
    return [dict(r) for r in result.mappings().all()]


async def _already_enriched(
    db: AsyncSession, rows: list[dict[str, Any]]
) -> set[tuple[str, str | None]]:
    """Return set of (address_norm, zip) already present in hail_leads_enriched."""
    if not rows:
        return set()
    norms = list({(r["address_norm"], r["zip"]) for r in rows if r.get("address_norm")})
    if not norms:
        return set()
    # Parameterized: pass two arrays.
    address_norms = [n[0] for n in norms]
    zips = [n[1] for n in norms]
    q = text("""
        SELECT address_norm, zip
          FROM hail_leads_enriched
         WHERE (address_norm, zip) IN (
            SELECT UNNEST(:norms), UNNEST(:zips)
         )
    """)
    result = await db.execute(q, {"norms": address_norms, "zips": zips})
    return {(r["address_norm"], r["zip"]) for r in result.mappings().all()}


async def _call_batchdata(
    client: httpx.AsyncClient, api_key: str, chunk: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """POST a chunk of addresses to BatchData skip-trace. Returns list[person]."""
    payload = {
        "requests": [
            {
                "propertyAddress": {
                    "street": row["address"] or "",
                    "city": row["city"] or "",
                    "state": row["state"] or "",
                    "zip": row["zip"] or "",
                }
            }
            for row in chunk
        ]
    }
    resp = await client.post(
        _BATCHDATA_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        json=payload,
        timeout=120.0,
    )
    resp.raise_for_status()
    body = resp.json()
    return (body.get("results") or {}).get("persons") or []


def _flatten_person(person: dict[str, Any]) -> dict[str, Any]:
    """Flatten a BatchData person into the columns stored in hail_leads_enriched."""
    name = person.get("name") or {}
    phones_raw = person.get("phoneNumbers") or []
    emails_raw = person.get("emails") or []
    mail = person.get("mailingAddress") or {}

    # Sort: highest score first, Mobile preferred on ties.
    phones_sorted = sorted(
        phones_raw,
        key=lambda p: (-int(p.get("score") or 0), 0 if p.get("type") == "Mobile" else 1),
    )
    phones = [
        {
            "number": p.get("number") or "",
            "type": p.get("type") or None,
            "dnc": p.get("dnc"),
            "score": p.get("score"),
        }
        for p in phones_sorted[:5]
    ]
    emails = [
        (e.get("email") if isinstance(e, dict) else e)
        for e in emails_raw
    ]
    emails = [e for e in emails if e]

    mailing_parts = [
        mail.get("street"), mail.get("city"),
        mail.get("state"), mail.get("zip"),
    ]
    mailing_address = ", ".join([p for p in mailing_parts if p]) or None

    return {
        "owner_name": (name.get("full") or
                       (f"{name.get('first','')} {name.get('last','')}".strip() or None)),
        "phones": phones,
        "emails": emails,
        "mailing_address": mailing_address,
        "raw": person,
    }


@router.post(
    "/enrich",
    response_model=HailLeadsEnrichResponse,
    dependencies=[Depends(require_admin_key)],
)
async def hail_leads_enrich(
    body: HailLeadsEnrichRequest,
    db: AsyncSession = Depends(get_db),
) -> HailLeadsEnrichResponse:
    """Admin-only: trigger BatchData skip-trace for the given lead IDs.

    - Looks up addresses from hail_leads.
    - Skips leads whose (address_norm, zip) is already in hail_leads_enriched
      unless force=True.
    - Calls BatchData in chunks of 50.
    - Writes results to hail_leads_enriched (UPSERT).
    """
    if not settings.BATCHDATA_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="BATCHDATA_API_KEY is not configured.",
        )

    rows = await _fetch_leads_for_enrich(db, body.lead_ids)
    if not rows:
        return HailLeadsEnrichResponse(enriched=0, skipped=0, failed=0)

    # Filter out unknowns.
    targetable = [r for r in rows if r.get("address") and r.get("zip")]
    missing = len(body.lead_ids) - len(targetable)

    skip_set: set[tuple[str, str | None]] = set()
    if not body.force:
        skip_set = await _already_enriched(db, targetable)

    to_enrich = [
        r for r in targetable
        if (r["address_norm"], r["zip"]) not in skip_set
    ]
    skipped = len(targetable) - len(to_enrich) + missing

    enriched_count = 0
    failed = 0
    errors: list[str] = []

    # Ensure cache table exists (lightweight idempotent guard).
    try:
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS hail_leads_enriched (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                lead_id UUID,
                address_norm TEXT NOT NULL,
                zip VARCHAR(10),
                owner_name TEXT,
                phones JSONB,
                emails JSONB,
                mailing_address TEXT,
                raw JSONB,
                enriched_at TIMESTAMPTZ DEFAULT NOW(),
                CONSTRAINT uq_hle_addr UNIQUE (address_norm, zip)
            )
        """))
        await db.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning("hail_leads_enriched table guard failed: %s", exc)
        await db.rollback()

    async with httpx.AsyncClient() as client:
        for i in range(0, len(to_enrich), _BATCHDATA_CHUNK):
            chunk = to_enrich[i : i + _BATCHDATA_CHUNK]
            try:
                persons = await _call_batchdata(
                    client, settings.BATCHDATA_API_KEY, chunk
                )
            except httpx.HTTPError as exc:
                failed += len(chunk)
                errors.append(f"chunk {i // _BATCHDATA_CHUNK}: {exc}")
                logger.warning("BatchData chunk failed: %s", exc)
                continue
            except Exception as exc:  # noqa: BLE001
                failed += len(chunk)
                errors.append(f"chunk {i // _BATCHDATA_CHUNK}: {exc}")
                logger.exception("BatchData unexpected error")
                continue

            for row, person in zip(chunk, persons):
                flat = _flatten_person(person or {})
                try:
                    await db.execute(
                        text("""
                            INSERT INTO hail_leads_enriched
                              (lead_id, address_norm, zip, owner_name,
                               phones, emails, mailing_address, raw, enriched_at)
                            VALUES
                              (CAST(:lead_id AS UUID), :address_norm, :zip,
                               :owner_name, CAST(:phones AS JSONB),
                               CAST(:emails AS JSONB), :mailing_address,
                               CAST(:raw AS JSONB), NOW())
                            ON CONFLICT (address_norm, zip) DO UPDATE SET
                              lead_id = EXCLUDED.lead_id,
                              owner_name = EXCLUDED.owner_name,
                              phones = EXCLUDED.phones,
                              emails = EXCLUDED.emails,
                              mailing_address = EXCLUDED.mailing_address,
                              raw = EXCLUDED.raw,
                              enriched_at = NOW()
                        """),
                        {
                            "lead_id": row["lead_id"],
                            "address_norm": row["address_norm"],
                            "zip": row["zip"],
                            "owner_name": flat["owner_name"],
                            "phones": _json_dumps(flat["phones"]),
                            "emails": _json_dumps(flat["emails"]),
                            "mailing_address": flat["mailing_address"],
                            "raw": _json_dumps(flat["raw"]),
                        },
                    )
                    enriched_count += 1
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    errors.append(f"lead {row['lead_id']}: {exc}")
                    logger.warning("insert hail_leads_enriched failed: %s", exc)
                    await db.rollback()
                    continue
        await db.commit()

    return HailLeadsEnrichResponse(
        enriched=enriched_count,
        skipped=skipped,
        failed=failed,
        errors=errors[:20],
    )


def _json_dumps(v: Any) -> str:
    """Safe JSON dump for JSONB insert params."""
    import json
    return json.dumps(v, default=str)


# ---------------------------------------------------------------------------
# 5) GET /{lead_id} — MUST come LAST (after all static routes).
# ---------------------------------------------------------------------------

@router.get(
    "/{lead_id}",
    response_model=HailLeadDetail,
    dependencies=[Depends(require_demo_key)],
)
async def hail_lead_detail(
    lead_id: str,
    db: AsyncSession = Depends(get_read_db),
) -> HailLeadDetail:
    """Single lead detail with enriched fields."""
    # Note: we pick the highest-score row for this lead across storm matches.
    q = text(f"""
        SELECT
            hl.lead_id::text                                         AS lead_id,
            hl.address                                                AS address,
            hl.city                                                   AS city,
            hl.zip                                                    AS zip,
            hl.county                                                 AS county,
            hl.lat                                                    AS lat,
            hl.lng                                                    AS lng,
            hl.storm_date                                             AS storm_date,
            hl.storm_type                                             AS storm_type,
            hl.storm_magnitude                                        AS hail_size_inches,
            hl.storm_event_id::text                                   AS storm_event_id,
            hl.storm_damage_report                                    AS damage_report,
            hl.issue_date                                             AS permit_date,
            hl.days_after_storm                                       AS days_after_storm,
            hl.permit_number                                          AS permit_number,
            hl.permit_type                                            AS permit_type,
            hl.work_class                                             AS work_class,
            hl.description                                            AS description,
            hl.valuation                                              AS valuation,
            hl.contractor_company                                     AS contractor,
            hc.lead_category                                          AS lead_category,
            aph.address_norm                                          AS address_norm,
            aph.permit_count                                          AS total_permits,
            aph.roof_permit_count                                     AS prior_roof_permits,
            aph.earliest_permit_date                                  AS earliest_permit_date,
            aph.latest_permit_date                                    AS latest_permit_date,
            aph.last_roof_permit_date                                 AS last_roof_permit_date,
            aph.total_roof_valuation                                  AS total_roof_valuation,
            tyb.year_built                                            AS year_built,
            tyb.living_area_sqft                                      AS living_area_sqft,
            tyb.appraised_value                                       AS appraised_value,
            hle.owner_name                                            AS owner_name,
            hle.phones                                                AS owner_phones,
            hle.emails                                                AS owner_emails,
            hle.mailing_address                                       AS owner_mailing_address,
            (hle.lead_id IS NOT NULL OR hle.address_norm IS NOT NULL) AS owner_enriched
        FROM hail_leads hl
        LEFT JOIN hail_leads_categorized hc USING (lead_id, storm_event_id)
        LEFT JOIN address_permit_history aph
               ON aph.address_norm = {_ADDRESS_NORM_SQL}
              AND aph.zip = hl.zip
        LEFT JOIN hail_leads_enriched hle
               ON hle.address_norm = aph.address_norm
              AND hle.zip = aph.zip
        LEFT JOIN tcad_year_built tyb
               ON tyb.address_norm = aph.address_norm
              AND tyb.zip = aph.zip
        WHERE hl.lead_id::text = :lead_id
        ORDER BY hl.hail_lead_score DESC NULLS LAST
        LIMIT 1
    """)

    result = await db.execute(q, {"lead_id": lead_id})
    row = result.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail=f"Lead not found: {lead_id}")

    storm = HailLeadStorm(
        storm_date=row["storm_date"],
        storm_type=row["storm_type"],
        hail_size_inches=float(row["hail_size_inches"]) if row["hail_size_inches"] is not None else None,
        storm_event_id=row["storm_event_id"],
        damage_report=row["damage_report"],
    )

    permit = HailLeadPermit(
        permit_date=row["permit_date"],
        days_after_storm=_days_int(row["days_after_storm"]),
        permit_number=row["permit_number"],
        permit_type=row["permit_type"],
        work_class=row["work_class"],
        description=row["description"],
        valuation=float(row["valuation"]) if row["valuation"] is not None else None,
        contractor=row["contractor"],
        lead_category=row["lead_category"],
    )

    address_history = HailLeadAddressHistory(
        total_permits=int(row["total_permits"] or 0),
        prior_roof_permits=int(row["prior_roof_permits"] or 0),
        earliest_permit_date=row["earliest_permit_date"],
        latest_permit_date=row["latest_permit_date"],
        last_roof_permit_date=row["last_roof_permit_date"],
        total_roof_valuation=(
            float(row["total_roof_valuation"])
            if row["total_roof_valuation"] is not None else None
        ),
    )

    owner: HailLeadOwner | None
    if row["owner_enriched"]:
        phones_raw = row["owner_phones"] or []
        phones: list[HailLeadPhone] = []
        for p in phones_raw:
            if not isinstance(p, dict):
                continue
            num = p.get("number")
            if not num:
                continue
            phones.append(HailLeadPhone(
                number=str(num),
                type=p.get("type"),
                dnc=p.get("dnc"),
                score=p.get("score"),
            ))
        emails_raw = row["owner_emails"] or []
        emails: list[str] = []
        for e in emails_raw:
            if isinstance(e, dict):
                v = e.get("email")
                if v:
                    emails.append(str(v))
            elif e:
                emails.append(str(e))
        owner = HailLeadOwner(
            enriched=True,
            owner_name=row["owner_name"],
            phones=phones,
            emails=emails,
            mailing_address=row["owner_mailing_address"],
        )
    else:
        owner = None

    return HailLeadDetail(
        lead_id=row["lead_id"],
        address=row["address"],
        city=row["city"],
        zip=row["zip"],
        county=row["county"],
        lat=float(row["lat"]) if row["lat"] is not None else None,
        lng=float(row["lng"]) if row["lng"] is not None else None,
        storm=storm,
        permit=permit,
        address_history=address_history,
        year_built=int(row["year_built"]) if row["year_built"] is not None else None,
        living_area_sqft=int(row["living_area_sqft"]) if row["living_area_sqft"] is not None else None,
        appraised_value=float(row["appraised_value"]) if row["appraised_value"] is not None else None,
        owner=owner,
    )
