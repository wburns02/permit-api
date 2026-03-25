"""Coverage, stats, and data freshness endpoints (public, no auth required)."""

import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models.permit import Permit, Jurisdiction
from app.services.search_service import get_coverage
from app.services.fast_counts import fast_count, safe_query

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Coverage"])


@router.get("/coverage")
async def coverage(db: AsyncSession = Depends(get_db)):
    """
    Get list of supported jurisdictions with record counts.

    No authentication required — used on the landing page.
    """
    jurisdictions = await get_coverage(db)

    total_records = await fast_count(db, "permits")

    # State counts from jurisdictions table (much smaller than permits)
    from app.services.fast_counts import safe_query
    state_rows = await safe_query(db,
        select(Jurisdiction.state, func.sum(Jurisdiction.record_count).label("count"))
        .group_by(Jurisdiction.state)
        .order_by(func.sum(Jurisdiction.record_count).desc())
    )
    states = {row.state: row.count for row in state_rows if row.state}

    return {
        "total_records": total_records,
        "total_jurisdictions": len(jurisdictions),
        "total_states": len(states),
        "states": states,
        "jurisdictions": jurisdictions,
    }


@router.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Quick stats for the landing page hero section.

    Now connected directly to T430 via Tailscale — uses fast reltuples
    for permit count, and known constants for jurisdictions/states.
    """
    # Sum all data layer tables for total platform records
    tables = [
        "permits", "business_entities", "septic_systems",
        "property_valuations", "fema_flood_zones", "code_violations",
        "epa_facilities", "contractor_licenses", "census_demographics",
        "property_sales", "property_liens", "permit_predictions",
    ]
    total_records = 0
    for tbl in tables:
        total_records += await fast_count(db, tbl)

    # Jurisdiction and state counts are relatively stable
    total_jurisdictions = await fast_count(db, "jurisdictions")
    if total_jurisdictions == 0:
        total_jurisdictions = 3143  # fallback

    # States: 50 US + DC + 3 Canadian provinces
    total_states = 54

    return {
        "total_permits": total_records,
        "total_jurisdictions": total_jurisdictions,
        "total_states": total_states,
    }


# ---------------------------------------------------------------------------
# Data Freshness Dashboard
# ---------------------------------------------------------------------------

# Each layer: (display_name, table_name, date_column, expected_frequency_label, max_age_hours)
DATA_LAYERS = [
    ("Permits", "permits", "issue_date", "Daily", 48),
    ("Hot Leads", "hot_leads", "created_at", "Daily", 48),
    ("Contractor Licenses", "contractor_licenses", "last_updated", "Weekly", 168),
    ("Property Sales", "property_sales", "sale_date", "Weekly", 168),
    ("Property Liens", "property_liens", "filing_date", "Weekly", 168),
    ("Code Violations", "code_violations", "violation_date", "Weekly", 168),
    ("Septic Systems", "septic_systems", "last_inspection", "Monthly", 720),
    ("Property Valuations", "property_valuations", "period_end", "Monthly", 720),
    ("EPA Facilities", "epa_facilities", None, "Quarterly", 2160),
    ("FEMA Flood Zones", "fema_flood_zones", None, "Quarterly", 2160),
    ("Business Entities", "business_entities", "scraped_at", "Weekly", 168),
    ("Census Demographics", "census_demographics", None, "Annually", 8760),
    ("Permit Predictions", "permit_predictions", "scored_at", "Daily", 48),
]


@router.get("/freshness")
async def data_freshness(db: AsyncSession = Depends(get_db)):
    """Data freshness dashboard — shows when each data layer was last updated.

    Public endpoint, no auth required. Builds user trust by showing data recency.
    """
    layers = []
    total_records = 0
    latest_update = None

    for name, table, date_col, freq, max_age_hrs in DATA_LAYERS:
        record_count = await fast_count(db, table)
        total_records += record_count

        last_updated = None
        if date_col:
            try:
                rows = await safe_query(
                    db,
                    text(f"SELECT MAX({date_col}) AS last_dt FROM {table}"),
                    timeout_ms=5000,
                )
                if rows and rows[0].last_dt:
                    raw = rows[0].last_dt
                    if isinstance(raw, datetime):
                        last_updated = raw
                    else:
                        # It's a date, convert to datetime
                        last_updated = datetime.combine(raw, datetime.min.time(), tzinfo=timezone.utc)
            except Exception as e:
                logger.debug("Freshness query for %s.%s failed: %s", table, date_col, e)

        # Determine staleness status
        status = "unknown"
        if last_updated:
            age = datetime.now(timezone.utc) - (
                last_updated if last_updated.tzinfo else last_updated.replace(tzinfo=timezone.utc)
            )
            age_hours = age.total_seconds() / 3600
            if age_hours <= max_age_hrs:
                status = "fresh"
            elif age_hours <= max_age_hrs * 1.5:
                status = "warning"
            else:
                status = "stale"

            if latest_update is None or last_updated > latest_update:
                latest_update = last_updated

        layers.append({
            "name": name,
            "table": table,
            "records": record_count,
            "last_updated": last_updated.isoformat() if last_updated else None,
            "freshness": freq,
            "status": status,
        })

    return {
        "layers": layers,
        "total_records": total_records,
        "last_scraper_run": latest_update.isoformat() if latest_update else None,
    }
