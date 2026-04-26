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
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import (
    get_db,
    get_read_db,
    primary_session_maker,
    replica_session_maker,
)
from app.schemas.hail_leads import (
    CoverageStat,
    CronHeartbeat,
    FreshLeadsCounts,
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
    HailLeadsHealth,
    HailLeadsStats,
    LeadCategory,
    MaterializedViewFreshness,
    SortKey,
    StormSourceFreshness,
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
# Read session helper — replica-first with primary fallback (health helpers only)
# ---------------------------------------------------------------------------

# Cache replica-dead state so we don't re-probe (and re-wait for connection
# reset) on every subquery within a single /health invocation. TTL is short
# so a recovering replica is picked back up within a minute.
_REPLICA_DEAD_UNTIL: float = 0.0
_REPLICA_DEAD_TTL_SEC: float = 60.0


@asynccontextmanager
async def _read_session():
    """Yield a read session, preferring replica but falling back to primary."""
    global _REPLICA_DEAD_UNTIL
    import time

    now = time.monotonic()
    if now >= _REPLICA_DEAD_UNTIL:
        try:
            async with replica_session_maker() as session:
                await session.execute(text("SELECT 1"))
                yield session
                return
        except Exception as exc:  # noqa: BLE001
            logger.warning("Health: replica unreachable, using primary: %s", exc)
            _REPLICA_DEAD_UNTIL = now + _REPLICA_DEAD_TTL_SEC
    async with primary_session_maker() as session:
        yield session


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
# 1.5) GET /health  (system observability)
# Static route — placed BEFORE /{lead_id}.
# ---------------------------------------------------------------------------

# Cron-status thresholds (hours).
_CRON_OK_HOURS = 26.0
_CRON_STALE_HOURS = 50.0


async def _safe_scalar(
    sql: str,
    params: dict[str, Any] | None = None,
    *,
    label: str = "",
    default: Any = None,
) -> Any:
    """Run a single-scalar query in its own session with a 10s timeout.

    Each subquery gets its own session so a statement_timeout abort or a
    missing-table error does not poison the parent transaction (mirrors the
    pattern used by hail_leads_list for its count subquery).
    """
    try:
        async with _read_session() as session:
            try:
                await session.execute(text("SET LOCAL statement_timeout = '10s'"))
                row = await session.execute(text(sql), params or {})
                return row.scalar()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "hail-leads health subquery failed (%s): %s", label, exc
                )
                await session.rollback()
                return default
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "hail-leads health session open failed (%s): %s", label, exc
        )
        return default


def _hours_between(now: datetime, then: datetime | None) -> float | None:
    """Hours between `then` and `now`, or None if `then` is None."""
    if then is None:
        return None
    if then.tzinfo is None:
        then = then.replace(tzinfo=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return round((now - then).total_seconds() / 3600.0, 2)


def _to_datetime(v: Any) -> datetime | None:
    """Best-effort coerce a DB scalar to a UTC datetime."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day, tzinfo=timezone.utc)
    return None


def _to_date(v: Any) -> date | None:
    """Best-effort coerce a DB scalar to a date."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return None


def _cron_status(hours_since: float | None) -> str:
    """Map hours-since-last-seen to status enum."""
    if hours_since is None:
        return "missing"
    if hours_since < _CRON_OK_HOURS:
        return "ok"
    if hours_since < _CRON_STALE_HOURS:
        return "stale"
    return "missing"


async def _mv_freshness(
    name: str, *, data_col: str
) -> MaterializedViewFreshness:
    """Build the freshness record for a materialized view."""
    row_count_raw = await _safe_scalar(
        "SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE relname = :n",
        {"n": name},
        label=f"{name}.row_count",
        default=0,
    )
    row_count = int(row_count_raw or 0)

    last_data_raw = await _safe_scalar(
        f"SELECT MAX({data_col}) FROM {name}",
        label=f"{name}.last_data",
    )
    last_data_at = _to_datetime(last_data_raw)

    last_analyze_raw = await _safe_scalar(
        "SELECT GREATEST(last_analyze, last_vacuum) "
        "FROM pg_stat_user_tables WHERE relname = :n",
        {"n": name},
        label=f"{name}.last_analyze",
    )
    last_analyzed_at = _to_datetime(last_analyze_raw)

    now = datetime.now(timezone.utc)
    return MaterializedViewFreshness(
        name=name,
        row_count=row_count,
        last_data_at=last_data_at,
        hours_since_data=_hours_between(now, last_data_at),
        last_analyzed_at=last_analyzed_at,
        hours_since_analyze=_hours_between(now, last_analyzed_at),
    )


async def _detect_date_column(table: str, candidates: list[str]) -> str | None:
    """Return the first candidate column that exists on `table`."""
    for col in candidates:
        exists = await _safe_scalar(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = :t AND column_name = :c LIMIT 1",
            {"t": table, "c": col},
            label=f"detect.{table}.{col}",
        )
        if exists:
            return col
    return None


async def _table_exists(table: str) -> bool:
    """Return True iff `table` exists in the current database."""
    exists = await _safe_scalar(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_name = :t LIMIT 1",
        {"t": table},
        label=f"exists.{table}",
    )
    return bool(exists)


async def _storm_source_freshness(
    *, source: str, candidates: list[str]
) -> StormSourceFreshness:
    """Build a StormSourceFreshness record by detecting the date column."""
    if not await _table_exists(source):
        return StormSourceFreshness(
            source=source,
            latest_report_date=None,
            days_since=None,
            rows_last_7d=0,
            rows_last_30d=0,
        )

    date_col = await _detect_date_column(source, candidates)
    if not date_col:
        return StormSourceFreshness(
            source=source,
            latest_report_date=None,
            days_since=None,
            rows_last_7d=0,
            rows_last_30d=0,
        )

    latest_raw = await _safe_scalar(
        f"SELECT MAX({date_col})::date FROM {source}",
        label=f"{source}.latest",
    )
    latest = _to_date(latest_raw)
    days_since = (date.today() - latest).days if latest else None

    rows_7d_raw = await _safe_scalar(
        f"SELECT COUNT(*) FROM {source} "
        f"WHERE {date_col} >= CURRENT_DATE - INTERVAL '7 days'",
        label=f"{source}.rows_7d",
        default=0,
    )
    rows_30d_raw = await _safe_scalar(
        f"SELECT COUNT(*) FROM {source} "
        f"WHERE {date_col} >= CURRENT_DATE - INTERVAL '30 days'",
        label=f"{source}.rows_30d",
        default=0,
    )

    return StormSourceFreshness(
        source=source,
        latest_report_date=latest,
        days_since=days_since,
        rows_last_7d=int(rows_7d_raw or 0),
        rows_last_30d=int(rows_30d_raw or 0),
    )


async def _fresh_leads_counts() -> FreshLeadsCounts:
    """Hail-leads counts for several recency windows."""
    this_week = await _safe_scalar(
        "SELECT COUNT(*) FROM hail_leads "
        "WHERE storm_date >= CURRENT_DATE - INTERVAL '7 days'",
        label="fresh.this_week",
        default=0,
    )
    last_week = await _safe_scalar(
        "SELECT COUNT(*) FROM hail_leads "
        "WHERE storm_date >= CURRENT_DATE - INTERVAL '14 days' "
        "AND storm_date <  CURRENT_DATE - INTERVAL '7 days'",
        label="fresh.last_week",
        default=0,
    )
    last_30d = await _safe_scalar(
        "SELECT COUNT(*) FROM hail_leads "
        "WHERE storm_date >= CURRENT_DATE - INTERVAL '30 days'",
        label="fresh.last_30d",
        default=0,
    )
    return FreshLeadsCounts(
        this_week=int(this_week or 0),
        last_week=int(last_week or 0),
        last_30d=int(last_30d or 0),
    )


async def _coverage_stats() -> list[CoverageStat]:
    """Build TCAD + enrichment coverage rows.

    Denominator is reltuples on address_permit_history (fast approximation).
    """
    denom_raw = await _safe_scalar(
        "SELECT GREATEST(reltuples, 0)::bigint FROM pg_class "
        "WHERE relname = 'address_permit_history'",
        label="coverage.denom",
        default=0,
    )
    denom = int(denom_raw or 0)

    out: list[CoverageStat] = []

    for name in ("tcad_year_built", "hail_leads_enriched"):
        if not await _table_exists(name):
            out.append(CoverageStat(
                name=name,
                enriched_rows=0,
                total_addresses=denom,
                percent_covered=0.0,
            ))
            continue

        rows_raw = await _safe_scalar(
            f"SELECT COUNT(*) FROM {name}",
            label=f"coverage.{name}",
            default=0,
        )
        rows = int(rows_raw or 0)
        pct = round((rows / denom) * 100.0, 1) if denom > 0 else 0.0
        out.append(CoverageStat(
            name=name,
            enriched_rows=rows,
            total_addresses=denom,
            percent_covered=pct,
        ))

    return out


async def _cron_heartbeats() -> list[CronHeartbeat]:
    """Build cron heartbeat rows.

    First tries a `cron_heartbeat` table; if absent, infers from data:
        - spc_load                          → MAX(date) on spc_storm_reports
        - storm_events_load                 → MAX(date) on storm_events
        - mv_refresh_hail_leads             → pg_stat_user_tables.last_analyze
        - mv_refresh_address_permit_history → same for address_permit_history
        - tcad_scrape                       → MAX(scraped_at) on tcad_year_built (best-effort)
    """
    now = datetime.now(timezone.utc)
    out: list[CronHeartbeat] = []

    if await _table_exists("cron_heartbeat"):
        try:
            async with _read_session() as session:
                await session.execute(text("SET LOCAL statement_timeout = '10s'"))
                rows = (await session.execute(text(
                    "SELECT name, MAX(beat_at) AS last_seen "
                    "FROM cron_heartbeat GROUP BY name"
                ))).mappings().all()
                for r in rows:
                    last_seen = _to_datetime(r["last_seen"])
                    hrs = _hours_between(now, last_seen)
                    out.append(CronHeartbeat(
                        name=str(r["name"]),
                        last_seen_at=last_seen,
                        hours_since=hrs,
                        status=_cron_status(hrs),
                    ))
                if out:
                    return out
        except Exception as exc:  # noqa: BLE001
            logger.warning("cron_heartbeat read failed: %s", exc)

    # Inferred-from-data fallbacks ----------------------------------------

    # spc_load
    spc_col = await _detect_date_column(
        "spc_storm_reports",
        ["report_date", "event_date", "begin_date", "begin_date_time", "date"],
    ) if await _table_exists("spc_storm_reports") else None
    spc_last = None
    if spc_col:
        spc_last = _to_datetime(await _safe_scalar(
            f"SELECT MAX({spc_col}) FROM spc_storm_reports",
            label="cron.spc_load",
        ))
    spc_hrs = _hours_between(now, spc_last)
    out.append(CronHeartbeat(
        name="spc_load",
        last_seen_at=spc_last,
        hours_since=spc_hrs,
        status=_cron_status(spc_hrs),
    ))

    # storm_events_load
    se_col = await _detect_date_column(
        "storm_events",
        ["begin_date_time", "begin_date", "event_date", "report_date", "date"],
    ) if await _table_exists("storm_events") else None
    se_last = None
    if se_col:
        se_last = _to_datetime(await _safe_scalar(
            f"SELECT MAX({se_col}) FROM storm_events",
            label="cron.storm_events_load",
        ))
    se_hrs = _hours_between(now, se_last)
    out.append(CronHeartbeat(
        name="storm_events_load",
        last_seen_at=se_last,
        hours_since=se_hrs,
        status=_cron_status(se_hrs),
    ))

    # MV refresh heartbeats — use pg_stat_user_tables.last_analyze.
    for cron_name, relname in (
        ("mv_refresh_hail_leads", "hail_leads"),
        ("mv_refresh_address_permit_history", "address_permit_history"),
    ):
        ts_raw = await _safe_scalar(
            "SELECT GREATEST(last_analyze, last_vacuum) "
            "FROM pg_stat_user_tables WHERE relname = :n",
            {"n": relname},
            label=f"cron.{cron_name}",
        )
        ts = _to_datetime(ts_raw)
        hrs = _hours_between(now, ts)
        out.append(CronHeartbeat(
            name=cron_name,
            last_seen_at=ts,
            hours_since=hrs,
            status=_cron_status(hrs),
        ))

    # tcad_scrape — pick first available timestamp column.
    tcad_last = None
    if await _table_exists("tcad_year_built"):
        tcad_col = await _detect_date_column(
            "tcad_year_built",
            ["scraped_at", "updated_at", "created_at", "inserted_at"],
        )
        if tcad_col:
            tcad_last = _to_datetime(await _safe_scalar(
                f"SELECT MAX({tcad_col}) FROM tcad_year_built",
                label="cron.tcad_scrape",
            ))
    tcad_hrs = _hours_between(now, tcad_last)
    out.append(CronHeartbeat(
        name="tcad_scrape",
        last_seen_at=tcad_last,
        hours_since=tcad_hrs,
        status=_cron_status(tcad_hrs),
    ))

    return out


@router.get(
    "/health",
    response_model=HailLeadsHealth,
    dependencies=[Depends(require_demo_key)],
)
async def hail_leads_health() -> HailLeadsHealth:
    """System observability snapshot for the hail-leads pipeline.

    Returns:
      - materialized_views: row count + data age + last analyze for hail_leads
        and address_permit_history.
      - storm_sources: latest report date + recent row counts for the upstream
        storm-data tables (storm_events, spc_storm_reports).
      - fresh_leads: lead counts in the last 7d / prior 7d / last 30d.
      - coverage: enrichment coverage as a percent of address_permit_history.
      - crons: heartbeats for spc_load / storm_events_load / mv_refresh_* /
        tcad_scrape (read from cron_heartbeat if present, else inferred).

    Each subquery runs in its own short-timeout session and degrades to
    null/0 on failure so a single broken table can't 500 the whole endpoint.
    """
    mvs = [
        await _mv_freshness("hail_leads", data_col="storm_date"),
        await _mv_freshness(
            "address_permit_history", data_col="latest_permit_date"
        ),
    ]

    storm_sources = [
        await _storm_source_freshness(
            source="storm_events",
            candidates=[
                "begin_date_time",
                "begin_date",
                "event_date",
                "report_date",
                "date",
            ],
        ),
        await _storm_source_freshness(
            source="spc_storm_reports",
            candidates=[
                "report_date",
                "event_date",
                "begin_date",
                "begin_date_time",
                "date",
            ],
        ),
    ]

    fresh = await _fresh_leads_counts()
    coverage = await _coverage_stats()
    crons = await _cron_heartbeats()

    return HailLeadsHealth(
        generated_at=datetime.now(timezone.utc),
        materialized_views=mvs,
        storm_sources=storm_sources,
        fresh_leads=fresh,
        coverage=coverage,
        crons=crons,
    )


# ---------------------------------------------------------------------------
# Manual MV refresh trigger (admin-only, static — must come before /{lead_id})
# ---------------------------------------------------------------------------

@router.post(
    "/refresh-mvs",
    dependencies=[Depends(require_admin_key)],
)
async def hail_leads_refresh_mvs() -> dict[str, str]:
    """Trigger an immediate REFRESH of hail-leads materialized views.

    Runs in the background — returns 202-style payload immediately. The
    APScheduler job at 04:25 UTC handles the regular cadence; this endpoint
    is for one-off manual kicks (e.g., after a fix deploys but the boot-time
    refresh's staleness gate skipped it).
    """
    import asyncio as _asyncio

    from app.services.mv_refresh import refresh_hail_leads_mvs

    _asyncio.create_task(refresh_hail_leads_mvs())
    return {
        "status": "kicked off",
        "detail": (
            "REFRESH MATERIALIZED VIEW running in background; check "
            "/v1/hail-leads/health in ~5-15 min for updated cron heartbeats."
        ),
    }


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
                COALESCE(aph.prior_roof_permits, 0)                    AS prior_roof_permits,
                aph.last_roof_permit_date                             AS last_roof_permit_date,
                (hle.address_norm IS NOT NULL)                        AS owner_enriched
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

    # Run the count in a SEPARATE session so a statement_timeout abort here
    # does not poison the main (list) transaction. In Postgres a
    # QueryCanceledError taints the whole transaction, and subsequent queries
    # fail with InFailedSQLTransactionError until ROLLBACK.
    # If count fails/times out we return total=-1; existing frontend handles it.
    total = -1
    try:
        async with replica_session_maker() as count_session:
            try:
                await count_session.execute(
                    text("SET LOCAL statement_timeout = '30s'")
                )
                total_row = await count_session.execute(text(count_sql), params)
                total = int(total_row.scalar() or 0)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "hail-leads count failed, returning -1: %s", exc
                )
                await count_session.rollback()
                total = -1
    except Exception as exc:  # noqa: BLE001
        # Session-open failure — e.g., replica unreachable. Don't block list.
        logger.warning("hail-leads count session open failed: %s", exc)
        total = -1

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
                COALESCE(aph.prior_roof_permits, 0)                    AS prior_roof_permits,
                tyb.year_built                                        AS year_built,
                hle.owner_name                                        AS owner_name,
                COALESCE(
                  (CASE WHEN hle.phone_1 IS NOT NULL THEN jsonb_build_array(
                    jsonb_build_object('number', hle.phone_1, 'type', hle.phone_1_type, 'score', hle.phone_1_score, 'dnc', hle.phone_1_dnc)
                  ) ELSE '[]'::jsonb END)
                  || (CASE WHEN hle.phone_2 IS NOT NULL THEN jsonb_build_array(
                    jsonb_build_object('number', hle.phone_2, 'dnc', hle.phone_2_dnc)
                  ) ELSE '[]'::jsonb END)
                  || (CASE WHEN hle.phone_3 IS NOT NULL THEN jsonb_build_array(
                    jsonb_build_object('number', hle.phone_3, 'dnc', hle.phone_3_dnc)
                  ) ELSE '[]'::jsonb END),
                  '[]'::jsonb
                )                                                      AS phones,
                COALESCE(
                  (CASE WHEN hle.email_1 IS NOT NULL THEN jsonb_build_array(hle.email_1) ELSE '[]'::jsonb END)
                  || (CASE WHEN hle.email_2 IS NOT NULL THEN jsonb_build_array(hle.email_2) ELSE '[]'::jsonb END),
                  '[]'::jsonb
                )                                                      AS emails
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
            aph.prior_roof_permits                                     AS prior_roof_permits,
            aph.earliest_permit_date                                  AS earliest_permit_date,
            aph.latest_permit_date                                    AS latest_permit_date,
            aph.last_roof_permit_date                                 AS last_roof_permit_date,
            aph.total_roof_valuation                                  AS total_roof_valuation,
            tyb.year_built                                            AS year_built,
            tyb.living_area_sqft                                      AS living_area_sqft,
            tyb.appraised_value                                       AS appraised_value,
            hle.owner_name                                            AS owner_name,
            hle.phone_1                                               AS phone_1,
            hle.phone_1_type                                          AS phone_1_type,
            hle.phone_1_score                                         AS phone_1_score,
            hle.phone_1_dnc                                           AS phone_1_dnc,
            hle.phone_2                                               AS phone_2,
            hle.phone_2_dnc                                           AS phone_2_dnc,
            hle.phone_3                                               AS phone_3,
            hle.phone_3_dnc                                           AS phone_3_dnc,
            hle.email_1                                               AS email_1,
            hle.email_2                                               AS email_2,
            hle.mailing_street                                        AS mailing_street,
            hle.mailing_city                                          AS mailing_city,
            hle.mailing_state                                         AS mailing_state,
            hle.mailing_zip                                           AS mailing_zip,
            hle.age                                                   AS age,
            hle.deceased                                              AS deceased,
            (hle.address_norm IS NOT NULL)                            AS owner_enriched
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

    try:
        result = await db.execute(q, {"lead_id": lead_id})
        row = result.mappings().first()
    except Exception as exc:  # noqa: BLE001
        # Malformed lead_id (e.g., not a UUID) or driver-level error on the
        # parameter cast — treat as not-found rather than 500.
        logger.info("hail lead detail lookup failed for %r: %s", lead_id, exc)
        await db.rollback()
        raise HTTPException(
            status_code=404, detail=f"Lead not found: {lead_id}"
        ) from exc
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
        phones: list[HailLeadPhone] = []
        if row["phone_1"]:
            phones.append(HailLeadPhone(
                number=str(row["phone_1"]),
                type=row["phone_1_type"],
                dnc=row["phone_1_dnc"],
                score=(
                    int(row["phone_1_score"])
                    if row["phone_1_score"] is not None else None
                ),
            ))
        if row["phone_2"]:
            phones.append(HailLeadPhone(
                number=str(row["phone_2"]),
                dnc=row["phone_2_dnc"],
            ))
        if row["phone_3"]:
            phones.append(HailLeadPhone(
                number=str(row["phone_3"]),
                dnc=row["phone_3_dnc"],
            ))
        emails: list[str] = []
        if row["email_1"]:
            emails.append(str(row["email_1"]))
        if row["email_2"]:
            emails.append(str(row["email_2"]))

        mailing_parts = [
            row["mailing_street"], row["mailing_city"],
            row["mailing_state"], row["mailing_zip"],
        ]
        mailing_address = ", ".join([p for p in mailing_parts if p]) or None

        owner = HailLeadOwner(
            enriched=True,
            owner_name=row["owner_name"],
            phones=phones,
            emails=emails,
            mailing_address=mailing_address,
            age=int(row["age"]) if row["age"] is not None else None,
            deceased=bool(row["deceased"]) if row["deceased"] is not None else None,
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
