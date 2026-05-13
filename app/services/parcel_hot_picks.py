"""Hot Picks service — bulk parcel ingest + state-law max-yield scoring.

Powers Ladder 1 of parcels.ecbtx.com: pre-compute the best-case CA state-law
unit yield for every parcel in a registered city, store the ranked candidates
in `parcel_hot_picks`, and expose them as a leaderboard.

Pipeline:
1. Paginate the city's Esri parcel FeatureServer (resultOffset + resultRecordCount).
2. For each parcel, run pure-python eligibility + yield rules against:
   - the base zone (by-right)
   - state-ADU stacking
   - SB-9 (urban lot split + duplex)
   - SB-684 (small-lot MF subdivision, 10-lot ceiling)
   - SB-1123 (vacant-SFR subdivision, same ceiling)
   - SB-1211 (up to 8 detached ADUs on existing MF)
   - AB-2011 / SB-6 (housing on commercial corridors)
   - density-bonus (+20 % top-up on best non-AB-130 path)
   - AB-130 (+1 retained existing improvement when stacked on SB-684/SB-1123)
3. Pick the highest-yield combo as `best_path`, set `score = max_units`.
4. Bulk-upsert into `parcel_hot_picks` in batches of 500.
5. Delete any APN not seen in this refresh so removals propagate.

Out of scope (Phase 2):
- 75 % perimeter urban-use test for SB-684 SFR-as-infill (we only flag MF zones today).
- CA statewide overlay exclusions (FHSZ, Alquist-Priolo, FMMP, OHP).
- LLM-driven nuance — Ladder 1 is intentionally rule-based for speed.
"""

import logging
import math
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.parcel_screen import (
    ParcelHotPick,
    ParcelJurisdiction,
    ParcelZoneDensity,
)
from app.services.parcel_screen_service import (  # noqa: F401 — private helpers reused intentionally
    _check_zone_commercial,
    _check_zone_mf,
    _check_zone_residential,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Esri pagination
# ---------------------------------------------------------------------------
def _parcels_where_for(jurisdiction: ParcelJurisdiction) -> str:
    """Pick the right WHERE clause for the city's parcel layer.

    The SB-County and Riverside-County parcel layers are shared across every
    city in the county. Pulling 870K rows per refresh is unnecessary and trips
    the 32K-parameter NOT-IN limit downstream. When we detect a known shared
    layer, scope the fetch by the city's display name.
    """
    parcels_url = (jurisdiction.parcels_url or "").lower()
    display = (jurisdiction.display_name or "").split(",")[0].strip()
    if "parcels_for_san_bernardino_county" in parcels_url:
        # SB County uses `Jurisdiction = 'City of Fontana'` etc.
        return f"Jurisdiction = 'City of {display}'"
    if "countyofriverside.us" in parcels_url:
        # Riverside CREST uses the bare uppercase city name in `CITY`.
        return f"CITY = '{display.upper()}'"
    return "1=1"


async def fetch_all_parcels_paginated(
    jurisdiction: ParcelJurisdiction,
    max_rows: int = 50000,
    page_size: int = 2000,
    timeout_s: float = 20.0,
) -> list[dict]:
    """Pull every parcel feature from the city's Esri FeatureServer.

    Uses `resultOffset` + `resultRecordCount` pagination and stops when the
    server stops returning features OR `exceededTransferLimit` is false on a
    short page. Caps at `max_rows` to keep refreshes bounded.

    Returns a list of dicts:
        {"attrs": {...raw attributes...}, "lat": float|None, "lng": float|None}

    Requests `outSR=4326` so geometry comes back as WGS84 lng/lat — easy
    centroid math, no client-side projection.
    """
    parcels_url = jurisdiction.parcels_url
    if not parcels_url:
        raise ValueError(f"jurisdiction {jurisdiction.state}/{jurisdiction.city_slug} has no parcels_url")

    out_rows: list[dict] = []
    offset = 0
    pages = 0

    where_clause = _parcels_where_for(jurisdiction)
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        while len(out_rows) < max_rows:
            params = {
                "where": where_clause,
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "4326",
                "f": "json",
                "resultOffset": str(offset),
                "resultRecordCount": str(page_size),
                "orderByFields": "objectid",
            }
            try:
                r = await client.get(f"{parcels_url}/query", params=params)
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                logger.error(f"parcel page request failed at offset {offset}: {e}")
                break

            features = data.get("features") or []
            if not features:
                break

            for feat in features:
                attrs = feat.get("attributes", {}) or {}
                geom = feat.get("geometry") or {}
                lat = None
                lng = None
                geometry_wgs84 = None
                rings = geom.get("rings") or []
                if rings and rings[0]:
                    outer = rings[0]
                    lng = sum(p[0] for p in outer) / len(outer)
                    lat = sum(p[1] for p in outer) / len(outer)
                    # Persist the polygon as GeoJSON so /map can serve it
                    # directly. Outer ring must be non-empty; Esri returns
                    # rings already in WGS84 lng/lat because we requested
                    # outSR=4326 above.
                    geometry_wgs84 = {
                        "type": "Polygon",
                        "coordinates": rings,
                    }
                out_rows.append({
                    "attrs": attrs,
                    "lat": lat,
                    "lng": lng,
                    "geometry_wgs84": geometry_wgs84,
                })

            pages += 1
            offset += len(features)

            exceeded = data.get("exceededTransferLimit") is True
            # Some Esri servers omit exceededTransferLimit when they return
            # a short page — short page on its own is also a "we're done"
            # signal. Combine both.
            if not exceeded and len(features) < page_size:
                break

            if len(out_rows) >= max_rows:
                logger.warning(f"hit max_rows cap of {max_rows}; truncating ingest")
                break

    logger.info(f"pulled {len(out_rows)} parcels across {pages} pages from {jurisdiction.city_slug}")
    return out_rows[:max_rows]


# ---------------------------------------------------------------------------
# Pure-python scoring
# ---------------------------------------------------------------------------
def _normalize_attrs(attrs: dict) -> dict:
    """Lower-case attribute keys and pull a small set of normalized values."""
    norm = {k.lower(): v for k, v in attrs.items()}

    parcel_acres = (
        norm.get("acres")
        or norm.get("acreage")
        or norm.get("acres_calc")
        or (norm.get("shape__area") / 43560 if norm.get("shape__area") else None)
    )
    try:
        acres = float(parcel_acres) if parcel_acres is not None else None
    except (TypeError, ValueError):
        acres = None

    # Coalesce on None, not on falsy — impr_value == 0 is meaningful (vacancy signal for SB-1123).
    # SB County → `ImprovementValue` (improvementvalue); Riverside CREST →
    # `STRUCTURES` (structures). Both may arrive as strings; coerce below.
    impr_raw = norm.get("impr_value")
    if impr_raw is None:
        impr_raw = norm.get("imprvalue")
    if impr_raw is None:
        impr_raw = norm.get("improvement_value")
    if impr_raw is None:
        impr_raw = norm.get("improvementvalue")
    if impr_raw is None:
        impr_raw = norm.get("structures")
    try:
        impr_value = (
            float(str(impr_raw).replace(",", "").strip())
            if impr_raw is not None and str(impr_raw).strip() != ""
            else None
        )
    except (TypeError, ValueError):
        impr_value = None

    # SB County puts the *city name* in `Zoning` (e.g. "CITY OF FONTANA").
    # Reject those placeholders here — for Hot Picks we just lose the by-right
    # path on those parcels (zone_code = None → falls through eligibility),
    # which is the right behavior until we wire per-city zoning spatial joins.
    zoning_raw = norm.get("zone_code") or norm.get("zone") or norm.get("zonecode") or norm.get("zoning")
    if isinstance(zoning_raw, str) and zoning_raw.upper().startswith("CITY OF"):
        zoning_raw = None

    return {
        "apn": (
            norm.get("apn")
            or norm.get("parcelno")
            or norm.get("parcel_no")
            or norm.get("parcelnumber")  # SB County
        ),
        "address": (
            norm.get("address")
            or norm.get("site_address")
            or norm.get("situs")
            or norm.get("situs_street")  # Riverside CREST
        ),
        "owner_name": norm.get("owner_name") or norm.get("owner") or norm.get("ownername"),
        "acres": acres,
        "zone_code": zoning_raw,
        "gp_code": norm.get("gp_code") or norm.get("genplan") or norm.get("gp_general"),
        "fire_zone": norm.get("fire_zonre") or norm.get("fire_zone") or norm.get("fhsz"),
        "impr_value": impr_value,
    }


def score_parcel(facts: dict, density_table: dict[str, ParcelZoneDensity]) -> dict:
    """Pure-python scoring against the CA state-law menu.

    Args:
        facts: normalized parcel facts (zone_code, acres, impr_value, ...).
        density_table: {zone_code: ParcelZoneDensity} for the city. Used for
            by-right du/ac and the explicit `is_residential` flag.

    Returns:
        {
          "max_units": int,
          "best_path": "sb684+ab130" | "sb1211" | "state-adu" | ...,
          "eligible_paths": ["by-right", "state-adu", "sb9", ...],
        }
    """
    zone = facts.get("zone_code")
    acres = facts.get("acres") or 0.0
    impr_value = facts.get("impr_value")
    zd = density_table.get(zone) if zone else None

    # Promote density-table hint into a local is_residential signal.
    is_residential_flag = None
    if zd:
        if zd.is_residential == "Y":
            is_residential_flag = True
        elif zd.is_residential == "N":
            is_residential_flag = False

    is_sfr = _check_zone_residential(zone)
    is_mf = _check_zone_mf(zone)
    is_any_res = is_sfr or is_mf or (is_residential_flag is True)
    is_commercial = _check_zone_commercial(zone)

    paths: dict[str, int] = {}

    # ---- by-right (always present, never empty) ----------------------------
    if zd and zd.du_per_ac is not None:
        try:
            du = float(zd.du_per_ac)
            by_right_units = max(1, math.floor(acres * du))
        except (TypeError, ValueError):
            by_right_units = 1
    else:
        by_right_units = 1
    paths["by-right"] = by_right_units

    # ---- state-ADU stacking -----------------------------------------------
    # SFR: +1 ADU + 1 JADU. MF gets its detached ADUs via SB-1211, so we only
    # add the +2 floor to non-MF residential here.
    if is_any_res and not is_mf:
        paths["state-adu"] = by_right_units + 2

    # ---- SB-9 (urban lot split + duplex on R-1) ---------------------------
    # Need SFR (or density-table flagged residential) AND ≥2400 sqft for two
    # post-split lots of 1200 sqft each.
    sqft = acres * 43560 if acres else 0
    if (is_sfr or is_residential_flag is True) and sqft >= 2400:
        paths["sb9"] = 4

    # ---- SB-684 (MF qualifying-infill, 10-lot ceiling) --------------------
    # Phase-2 will add the 75 % perimeter urban-use test for SFR-as-infill;
    # today we only light SB-684 up for MF zones (definitionally eligible).
    if is_mf:
        paths["sb684"] = 10

    # ---- SB-1123 (vacant SFR small-lot subdivision) -----------------------
    # Improvement value == 0 is the vacancy signal in Rob's playbook.
    if is_sfr and (impr_value is not None and impr_value == 0):
        paths["sb1123"] = 10

    # ---- SB-1211 (up to 8 detached ADUs on existing MF) -------------------
    if is_mf and (impr_value is not None and impr_value > 0):
        # Heuristic: assume 4 existing units (representative R-2/R-3 build),
        # then +8 detached ADUs ministerially.
        paths["sb1211"] = 4 + 8

    # ---- AB-2011 / SB-6 (housing on commercial corridors) -----------------
    if is_commercial and acres >= 0.25:
        # Suburban floor: 20 du/ac (Inland Empire baseline). Density-bonus
        # stacking handled below.
        paths["ab2011-sb6"] = int(math.floor(acres * 20))

    # ---- Pick the leader BEFORE bonuses -----------------------------------
    # Apply density bonus (+20 %) on top of the best non-AB-130 path. AB-130
    # is a remainder modifier — handled separately and only stacks on
    # SB-684 / SB-1123.
    base_best_path = max(paths, key=lambda k: paths[k])
    base_best_units = paths[base_best_path]

    # Density-bonus top-up
    bonus_best_units = base_best_units
    bonus_best_path = base_best_path
    if base_best_units > 0:
        bonus_units = int(math.floor(base_best_units * 1.20))
        if bonus_units > base_best_units:
            paths["density-bonus"] = bonus_units
            bonus_best_units = bonus_units
            bonus_best_path = f"{base_best_path}+density-bonus"

    # AB-130 stack — +1 retained existing structure on SB-684 / SB-1123
    final_best_path = bonus_best_path
    final_best_units = bonus_best_units
    if base_best_path in ("sb684", "sb1123") and impr_value is not None and impr_value > 0:
        ab130_units = base_best_units + 1
        # Compare against density-bonus alternative — keep whichever is taller.
        if ab130_units > final_best_units:
            paths["ab130"] = ab130_units
            final_best_units = ab130_units
            final_best_path = f"{base_best_path}+ab130"
        else:
            paths["ab130"] = ab130_units

    eligible_paths = sorted(paths.keys())
    return {
        "max_units": int(final_best_units),
        "best_path": final_best_path,
        "eligible_paths": eligible_paths,
    }


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
async def refresh_city(db: AsyncSession, jurisdiction: ParcelJurisdiction) -> dict[str, int | float]:
    """Pull every parcel for the city, score it, upsert into parcel_hot_picks.

    Deletes any APN not seen in this refresh so removals/changes propagate.
    Returns a stats dict: total / scored / skipped_no_zone / wall_clock_s.
    """
    started = time.monotonic()
    state = jurisdiction.state
    city_slug = jurisdiction.city_slug

    # Load density table once for cheap lookups during scoring.
    result = await db.execute(
        select(ParcelZoneDensity).where(
            ParcelZoneDensity.state == state,
            ParcelZoneDensity.city_slug == city_slug,
        )
    )
    density_rows = result.scalars().all()
    density_table: dict[str, ParcelZoneDensity] = {row.zone_code: row for row in density_rows}
    logger.info(f"loaded {len(density_table)} density rows for {state}/{city_slug}")

    # 1. Pull all parcels
    parcels = await fetch_all_parcels_paginated(jurisdiction)
    total = len(parcels)

    # 2. Score each
    now = datetime.now(timezone.utc)
    scored_rows: list[dict] = []
    seen_apns: set[str] = set()
    skipped_no_zone = 0

    for p in parcels:
        facts = _normalize_attrs(p["attrs"])
        apn = facts.get("apn")
        if not apn:
            continue
        apn = str(apn).strip()
        if not apn or apn in seen_apns:
            continue

        if not facts.get("zone_code"):
            # Parcel has no zone on the layer — happens for shared county
            # parcel layers (SB County / Riverside CREST) until we wire a
            # per-city zoning spatial join. Still record the row so the
            # parcel is discoverable in Hot Picks; max_units falls back to
            # 1 (by-right floor) via score_parcel below.
            skipped_no_zone += 1

        scoring = score_parcel(facts, density_table)
        max_units = scoring["max_units"]
        score_val = float(max_units)

        scored_rows.append({
            "state": state,
            "city_slug": city_slug,
            "apn": apn,
            "address": (facts.get("address") or "")[:500] or None,
            "owner_name": (facts.get("owner_name") or "")[:255] or None,
            "acres": facts.get("acres"),
            "zone_code": (facts.get("zone_code") or "")[:40] or None,
            "gp_code": (facts.get("gp_code") or "")[:40] or None,
            "fire_zone": (facts.get("fire_zone") or "")[:40] or None,
            "impr_value": facts.get("impr_value"),
            "lat": p.get("lat"),
            "lng": p.get("lng"),
            "geometry_wgs84": p.get("geometry_wgs84"),
            "max_units": max_units,
            "best_path": scoring["best_path"][:80],
            "eligible_paths": scoring["eligible_paths"],
            "score": score_val,
            "refreshed_at": now,
        })
        seen_apns.add(apn)

    scored = len(scored_rows)

    # 3. Bulk upsert in batches of 500
    BATCH = 500
    for i in range(0, len(scored_rows), BATCH):
        batch = scored_rows[i:i + BATCH]
        stmt = pg_insert(ParcelHotPick).values(batch)
        update_cols = {
            c: stmt.excluded[c]
            for c in (
                "address", "owner_name", "acres", "zone_code", "gp_code",
                "fire_zone", "impr_value", "lat", "lng", "geometry_wgs84",
                "max_units", "best_path", "eligible_paths", "score", "refreshed_at",
            )
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=["state", "city_slug", "apn"],
            set_=update_cols,
        )
        await db.execute(stmt)

    # 4. Delete stale rows — any APN not seen this run.
    # Strategy: write the seen set into a session-scoped temp table in chunks
    # (each INSERT carries at most 5K varchar params, well under asyncpg's
    # 32767 cap and the asyncpg/Postgres wire-protocol message limits), then
    # DELETE WHERE apn NOT IN (SELECT FROM temp_table).
    if seen_apns:
        from sqlalchemy import text as _sql_text
        # ON COMMIT DROP drops the temp table when the outer transaction
        # commits later in this function, so back-to-back refreshes don't
        # collide.
        await db.execute(_sql_text(
            "CREATE TEMP TABLE IF NOT EXISTS _hot_picks_seen "
            "(apn varchar(40) PRIMARY KEY) ON COMMIT DROP"
        ))
        await db.execute(_sql_text("TRUNCATE _hot_picks_seen"))
        seen_list = list(seen_apns)
        INSERT_BATCH = 5000
        for i in range(0, len(seen_list), INSERT_BATCH):
            chunk = seen_list[i:i + INSERT_BATCH]
            # `unnest(:apns)` makes the single array parameter expand into
            # rows without burning per-value placeholder slots.
            await db.execute(
                _sql_text(
                    "INSERT INTO _hot_picks_seen(apn) "
                    "SELECT DISTINCT unnest(CAST(:apns AS varchar[])) "
                    "ON CONFLICT DO NOTHING"
                ),
                {"apns": chunk},
            )
        await db.execute(
            _sql_text(
                "DELETE FROM parcel_hot_picks "
                "WHERE state = :s AND city_slug = :c "
                "AND apn NOT IN (SELECT apn FROM _hot_picks_seen)"
            ),
            {"s": state, "c": city_slug},
        )

    await db.commit()
    wall = time.monotonic() - started
    return {
        "total": total,
        "scored": scored,
        "skipped_no_zone": skipped_no_zone,
        "deleted_stale_unseen": True,
        "wall_clock_s": round(wall, 2),
    }
