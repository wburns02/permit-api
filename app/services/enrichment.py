"""Service layer for cross-product enrichment.

Currently:
  - summarize_broadband_for_geo / for_address: cheap broadband summary that's
    small enough to nest inside other product responses without bloating them.
  - bulk_broadband: same lookup applied to a batch of addresses (capped at 500).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.broadband import (
    BroadbandLookupResponse,
    CABLE_TECH_CODES,
    FIBER_TECH_CODES,
    SATELLITE_TECH_CODES,
)
from app.schemas.enrichment import (
    BroadbandEnrichmentSummary,
    BulkAddressInput,
    BulkBroadbandResultItem,
)
from app.services.broadband import lookup_broadband, resolve_address_to_geo

logger = logging.getLogger(__name__)


# Max parallel broadband lookups inside a bulk request. Keep small to avoid
# saturating the DB pool — each call may do several round-trips.
_BULK_CONCURRENCY = 8


def _summarize(bb: BroadbandLookupResponse) -> BroadbandEnrichmentSummary:
    """Compress a full BroadbandLookupResponse into the slim summary shape."""
    rural_signal = bb.only_satellite or (not bb.has_fiber and not bb.has_cable)
    return BroadbandEnrichmentSummary(
        isp_count=bb.isp_count,
        fiber_isp_count=bb.fiber_isp_count,
        cable_isp_count=bb.cable_isp_count,
        satellite_isp_count=bb.satellite_isp_count,
        has_fiber=bb.has_fiber,
        has_cable=bb.has_cable,
        only_satellite=bb.only_satellite,
        max_download_mbps=bb.max_download_mbps,
        max_upload_mbps=bb.max_upload_mbps,
        match_method=bb.match_method,
        rural_signal=rural_signal,
    )


# ---------------------------------------------------------------------------
# Single-row helpers (used by include_broadband=true on other endpoints)
# ---------------------------------------------------------------------------


async def summarize_broadband_for_address(
    db: AsyncSession,
    *,
    address: str | None,
    city: str | None,
    state: str,
    zip_code: str | None,
) -> BroadbandEnrichmentSummary | None:
    """Summary keyed by address. Returns None if state code is invalid."""
    state_up = (state or "").upper()
    if len(state_up) != 2 or not state_up.isalpha():
        return None
    try:
        bb = await lookup_broadband(
            db, address=address, city=city, state=state_up, zip_code=zip_code,
        )
    except Exception as e:
        logger.warning("broadband summary lookup failed for %s: %s", address, e)
        try:
            await db.rollback()
        except Exception:
            pass
        return None
    return _summarize(bb)


async def summarize_broadband_for_latlon(
    db: AsyncSession,
    *,
    lat: float,
    lon: float,
    state: str,
) -> BroadbandEnrichmentSummary | None:
    """Summary keyed by lat/lon. Resolves the tract via tiger_tracts first."""
    state_up = (state or "").upper()
    if len(state_up) != 2 or not state_up.isalpha():
        return None

    tract_geoid: str | None = None
    try:
        row = (await db.execute(
            text("""
                SELECT geoid
                FROM tiger_tracts
                WHERE intptlat BETWEEN :lat - 0.5 AND :lat + 0.5
                  AND intptlon BETWEEN :lon - 0.5 AND :lon + 0.5
                ORDER BY (intptlat - :lat) * (intptlat - :lat)
                       + (intptlon - :lon) * (intptlon - :lon)
                LIMIT 1
            """),
            {"lat": lat, "lon": lon},
        )).first()
        if row:
            tract_geoid = row[0]
    except Exception as e:
        logger.debug("tract lookup for latlon failed: %s", e)
        try:
            await db.rollback()
        except Exception:
            pass

    geo = {
        "lat": lat, "lon": lon,
        "tract_geoid": tract_geoid, "block_geoid": None,
        "match_method": "latlon",
    }
    try:
        bb = await lookup_broadband(
            db, address=None, city=None, state=state_up, zip_code=None, geo=geo,
        )
    except Exception as e:
        logger.warning("broadband lookup_broadband (latlon) failed: %s", e)
        try:
            await db.rollback()
        except Exception:
            pass
        return None
    return _summarize(bb)


# ---------------------------------------------------------------------------
# Bulk endpoint
# ---------------------------------------------------------------------------


async def bulk_broadband(
    db: AsyncSession,
    items: Iterable[BulkAddressInput],
) -> list[BulkBroadbandResultItem]:
    """Run broadband summary on a batch of addresses.

    Limited concurrency to keep DB pool happy. Each item gets its own try/except
    so one bad row doesn't fail the batch.

    NB: All lookups run against the same db session — that means they're
    serialized via session locking. For real throughput we'd want a session
    per task, but adding pool churn under burst load is a bigger risk than
    serial latency for a 500-row cap.
    """
    sem = asyncio.Semaphore(_BULK_CONCURRENCY)

    async def _one(it: BulkAddressInput) -> BulkBroadbandResultItem:
        async with sem:
            try:
                summary: BroadbandEnrichmentSummary | None
                if it.lat is not None and it.lon is not None and it.state:
                    summary = await summarize_broadband_for_latlon(
                        db, lat=it.lat, lon=it.lon, state=it.state,
                    )
                elif it.address and it.state:
                    summary = await summarize_broadband_for_address(
                        db, address=it.address, city=it.city,
                        state=it.state, zip_code=it.zip,
                    )
                else:
                    return BulkBroadbandResultItem(
                        id=it.id, input=it, broadband=None,
                        error="Need (address+state) or (lat+lon+state).",
                    )
                return BulkBroadbandResultItem(
                    id=it.id, input=it, broadband=summary,
                )
            except Exception as e:
                logger.warning("bulk broadband row failed: %s", e)
                try:
                    await db.rollback()
                except Exception:
                    pass
                return BulkBroadbandResultItem(
                    id=it.id, input=it, broadband=None, error=str(e)[:160],
                )

    return await asyncio.gather(*[_one(it) for it in items])
