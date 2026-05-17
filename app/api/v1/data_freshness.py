"""Data refresh log → public status board for portfolio.ecbtx.com.

Reads from public.data_refresh_log on T430 primary DB. Returns one row per
configured source with humanized freshness flags.

No auth — public status board, safe to expose.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_read_db

router = APIRouter(prefix="/data-freshness", tags=["DataFreshness"])


class DataSourceFreshness(BaseModel):
    """Freshness state for a single data source."""

    source: str
    last_refresh_at: datetime | None = None
    status: str = Field(
        description="success | partial | failure | stale | overdue | unknown"
    )
    row_count_last: int | None = None
    rows_added_last: int | None = None
    hours_since_refresh: float | None = None
    expected_refresh_hours: float | None = None
    is_stale: bool = False
    notes: str | None = None
    error_text: str | None = None


class DataFreshnessResponse(BaseModel):
    """Top-level wrapper — list + summary."""

    generated_at: datetime
    source_count: int
    fresh_count: int
    stale_count: int
    failure_count: int
    sources: list[DataSourceFreshness]


@router.get("", response_model=DataFreshnessResponse)
@router.get("/", response_model=DataFreshnessResponse)
async def data_freshness(db: AsyncSession = Depends(get_read_db)) -> DataFreshnessResponse:
    """Return per-source freshness status from data_refresh_log.

    Each source's latest refreshed_at row wins. Derived fields:
      - hours_since_refresh = (now - refreshed_at) / 1h
      - expected_refresh_hours = (expected_next_refresh_at - refreshed_at) / 1h
      - is_stale = hours_since_refresh > 2 * expected_refresh_hours
      - status = stored status, escalated to 'stale' or 'overdue' when warranted
    """
    rows = (
        await db.execute(
            text(
                """
                SELECT DISTINCT ON (source)
                    source,
                    refreshed_at,
                    status,
                    rows_added,
                    rows_total_after,
                    expected_next_refresh_at,
                    notes,
                    error_text
                FROM data_refresh_log
                ORDER BY source, refreshed_at DESC
                """
            )
        )
    ).fetchall()

    now = datetime.now(timezone.utc)
    sources: list[DataSourceFreshness] = []
    fresh = 0
    stale = 0
    failed = 0

    for r in rows:
        (
            src,
            refreshed_at,
            status,
            rows_added,
            rows_total,
            expected_next,
            notes,
            error_text,
        ) = r

        hours_since = None
        expected_hours = None
        is_stale = False
        derived_status = status

        if refreshed_at is not None:
            hours_since = round(
                (now - refreshed_at).total_seconds() / 3600.0, 2
            )

        if expected_next is not None and refreshed_at is not None:
            expected_hours = round(
                (expected_next - refreshed_at).total_seconds() / 3600.0, 2
            )
            if hours_since is not None and expected_hours and expected_hours > 0:
                if hours_since > 2 * expected_hours:
                    is_stale = True
                    if derived_status == "success":
                        derived_status = "overdue"
                elif hours_since > expected_hours:
                    if derived_status == "success":
                        derived_status = "stale"

        if derived_status == "failure":
            failed += 1
        elif is_stale or derived_status in ("stale", "overdue"):
            stale += 1
        else:
            fresh += 1

        sources.append(
            DataSourceFreshness(
                source=src,
                last_refresh_at=refreshed_at,
                status=derived_status,
                row_count_last=rows_total,
                rows_added_last=rows_added,
                hours_since_refresh=hours_since,
                expected_refresh_hours=expected_hours,
                is_stale=is_stale,
                notes=notes,
                error_text=error_text,
            )
        )

    sources.sort(key=lambda s: s.source)

    return DataFreshnessResponse(
        generated_at=now,
        source_count=len(sources),
        fresh_count=fresh,
        stale_count=stale,
        failure_count=failed,
        sources=sources,
    )
