"""Service layer for broadband, septic-score, and rural-leads queries.

All queries target the v2 tables on the permits database:
  - rural_septic_score_v2 (materialized view)
  - fcc_bdc_locations_<state> (partitioned per-state)
  - fcc_bdc_providers
  - tiger_tracts
  - zcta_pop_density_<state>
  - property_sales (for address-to-coord fallback)

Uses raw SQL via SQLAlchemy text() for these — they hit MVs/partitioned tables
that are not ORM-mapped, and we want full control over the query plan.
"""

from __future__ import annotations

import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.broadband import (
    BroadbandLookupResponse,
    BroadbandProvider,
    FCC_TECHNOLOGY_LABELS,
    FIBER_TECH_CODES,
    CABLE_TECH_CODES,
    SATELLITE_TECH_CODES,
    WIRELESS_TECH_CODES,
    RuralLead,
    SepticScoreComponents,
    SepticScoreResponse,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Address resolution
# ---------------------------------------------------------------------------

async def resolve_address_to_geo(
    db: AsyncSession,
    *,
    address: str | None,
    city: str | None,
    state: str,
    zip_code: str | None,
) -> dict:
    """Resolve an address to lat/lon + block/tract geoids.

    Strategy (cheapest first):
      1. Fuzzy address match in property_sales (returns lat/lng if present).
      2. If we have lat/lng, find nearest tract centroid in tiger_tracts.
      3. As a final fallback, snap to ZCTA centroid via zcta_pop_density_<state>.

    Returns a dict with: lat, lon, tract_geoid, block_geoid, match_method.
    Any field may be None if resolution failed.
    """
    state_up = (state or "").upper()
    result = {
        "lat": None,
        "lon": None,
        "tract_geoid": None,
        "block_geoid": None,
        "match_method": "none",
    }

    # 1. property_sales fuzzy match
    if address:
        try:
            params = {"addr": f"%{address[:80]}%", "state": state_up}
            sql = """
                SELECT lat, lng
                FROM property_sales
                WHERE state = :state
                  AND address ILIKE :addr
                  AND lat IS NOT NULL AND lng IS NOT NULL
                LIMIT 1
            """
            row = (await db.execute(text(sql), params)).first()
            if row:
                result["lat"] = float(row[0])
                result["lon"] = float(row[1])
                result["match_method"] = "property_sales"
        except Exception as e:
            logger.debug("property_sales lookup failed: %s", e)

    # 2. ZCTA centroid fallback (TX-only table for now)
    if result["lat"] is None and zip_code and state_up == "TX":
        try:
            row = (await db.execute(
                text("""
                    SELECT intptlat, intptlon, zcta_geoid
                    FROM zcta_pop_density_tx
                    WHERE zip5 = :zip
                    LIMIT 1
                """),
                {"zip": zip_code[:5]},
            )).first()
            if row:
                result["lat"] = float(row[0])
                result["lon"] = float(row[1])
                result["match_method"] = "zcta_centroid"
        except Exception as e:
            logger.debug("zcta lookup failed: %s", e)

    # 3. Resolve tract_geoid from lat/lon (nearest centroid, bounded)
    if result["lat"] is not None and result["lon"] is not None:
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
                {"lat": result["lat"], "lon": result["lon"]},
            )).first()
            if row:
                result["tract_geoid"] = row[0]
        except Exception as e:
            logger.debug("tract lookup failed: %s", e)

    return result


# ---------------------------------------------------------------------------
# Broadband lookup
# ---------------------------------------------------------------------------

async def lookup_broadband(
    db: AsyncSession,
    *,
    address: str | None,
    city: str | None,
    state: str,
    zip_code: str | None,
    geo: dict | None = None,
) -> BroadbandLookupResponse:
    """Single-address broadband lookup. Returns every ISP serving the address.

    Uses block_geoid when available, else falls back to county_geoid in the
    resolved tract. Empty providers list if nothing resolves.

    `geo` is an optional pre-resolved geo dict (saves a round-trip when called
    from the septic-score on-the-fly compute).
    """
    state_up = (state or "").upper()
    if geo is None:
        geo = await resolve_address_to_geo(
            db, address=address, city=city, state=state_up, zip_code=zip_code,
        )

    providers: list[BroadbandProvider] = []
    block_geoid: str | None = None

    # FCC BDC table is partitioned per state; build the table name dynamically.
    # We constrain state_up to [A-Z]{2} to make this safe from injection.
    if not (len(state_up) == 2 and state_up.isalpha()):
        return BroadbandLookupResponse(
            address=address,
            city=city,
            state=state_up,
            zip=zip_code,
            providers=[],
            has_fiber=False,
            has_cable=False,
            only_satellite=False,
            isp_count=0,
            fiber_isp_count=0,
            cable_isp_count=0,
            satellite_isp_count=0,
            wireless_isp_count=0,
            match_method=geo["match_method"],
        )

    bdc_table = f"fcc_bdc_locations_{state_up.lower()}"

    # If we have a tract_geoid, find every block in that tract via a range scan.
    # FCC BDC block_geoid is a 15-char census block id; the first 11 chars are tract.
    # A range scan (>= tract || '0000', < tract || '9999'+1) uses the btree index
    # cleanly — substring() does NOT, and would force a full partition scan
    # (30M+ rows = timeout).
    rows = []
    if geo.get("tract_geoid"):
        tract = geo["tract_geoid"]
        # 15-char block_geoid = 11-char tract + 4-char block. Range: tract||'0000' .. tract||'9999'+1
        lo = f"{tract}0000"
        hi = f"{tract}9999" + "\x00"  # one above the max
        try:
            # Belt-and-braces timeout (DB also has a global 20s cap).
            await db.execute(text("SET LOCAL statement_timeout = '10s'"))
            sql = f"""
                SELECT
                    p.provider_id,
                    p.brand_name,
                    p.holding_company_name,
                    bdc.technology,
                    MAX(bdc.max_advertised_download_speed) AS max_dl,
                    MAX(bdc.max_advertised_upload_speed)   AS max_ul,
                    bool_or(bdc.low_latency)               AS low_latency,
                    string_agg(DISTINCT bdc.business_residential_code, '/') AS br,
                    MIN(bdc.block_geoid)                   AS sample_block
                FROM {bdc_table} bdc
                JOIN fcc_bdc_providers p USING (provider_id)
                WHERE bdc.block_geoid >= :lo AND bdc.block_geoid < :hi
                GROUP BY p.provider_id, p.brand_name, p.holding_company_name, bdc.technology
                ORDER BY max_dl DESC NULLS LAST
                LIMIT 200
            """
            rows = (await db.execute(
                text(sql), {"lo": lo, "hi": hi}
            )).all()
        except Exception as e:
            logger.warning("BDC lookup (%s) by tract failed: %s", bdc_table, e)
            rows = []

    # De-duplicate to one row per (provider_id, technology), keeping the best speed.
    # (Already de-duped server-side, but keep this as a safety net.)
    best: dict[tuple[int, int], dict] = {}
    for r in rows:
        provider_id = r[0]
        tech_code = r[3]
        if not block_geoid and len(r) > 8 and r[8]:
            block_geoid = r[8]
        key = (provider_id, tech_code)
        candidate = {
            "provider_id": provider_id,
            "brand_name": r[1],
            "holding_company_name": r[2],
            "technology_code": tech_code,
            "technology": FCC_TECHNOLOGY_LABELS.get(tech_code, f"code-{tech_code}"),
            "max_download_mbps": int(r[4]) if r[4] is not None else None,
            "max_upload_mbps": int(r[5]) if r[5] is not None else None,
            "low_latency": r[6],
            "business_residential": r[7],
        }
        prior = best.get(key)
        if not prior or (candidate["max_download_mbps"] or 0) > (prior["max_download_mbps"] or 0):
            best[key] = candidate

    providers = [BroadbandProvider(**v) for v in best.values()]
    providers.sort(key=lambda p: (p.max_download_mbps or 0), reverse=True)

    # Aggregates
    fiber_count = sum(1 for p in providers if p.technology_code in FIBER_TECH_CODES)
    cable_count = sum(1 for p in providers if p.technology_code in CABLE_TECH_CODES)
    sat_count   = sum(1 for p in providers if p.technology_code in SATELLITE_TECH_CODES)
    wifi_count  = sum(1 for p in providers if p.technology_code in WIRELESS_TECH_CODES)
    max_dl = max((p.max_download_mbps or 0 for p in providers), default=0) or None
    max_ul = max((p.max_upload_mbps   or 0 for p in providers), default=0) or None

    has_fiber = fiber_count > 0
    has_cable = cable_count > 0
    only_satellite = (len(providers) > 0) and all(
        p.technology_code in SATELLITE_TECH_CODES for p in providers
    )

    return BroadbandLookupResponse(
        address=address,
        city=city,
        state=state_up,
        zip=zip_code,
        block_geoid=block_geoid,
        tract_geoid=geo.get("tract_geoid"),
        lat=geo.get("lat"),
        lon=geo.get("lon"),
        providers=providers,
        max_download_mbps=max_dl,
        max_upload_mbps=max_ul,
        has_fiber=has_fiber,
        has_cable=has_cable,
        only_satellite=only_satellite,
        isp_count=len(providers),
        fiber_isp_count=fiber_count,
        cable_isp_count=cable_count,
        satellite_isp_count=sat_count,
        wireless_isp_count=wifi_count,
        match_method=geo["match_method"],
    )


# ---------------------------------------------------------------------------
# Septic-score lookup (TX only for now)
# ---------------------------------------------------------------------------

def _tier_for_score(score: int) -> str:
    if score >= 85:
        return "high-rural"
    if score >= 70:
        return "rural"
    if score >= 40:
        return "suburban"
    return "urban"


def _interpretation_for(components: SepticScoreComponents, score: int) -> str:
    if components.in_urban_area:
        return "In a Census-defined urban area — unlikely to be on septic."
    if score >= 85:
        bits = []
        if components.only_satellite:
            bits.append("satellite-only broadband")
        if components.population_density is not None and components.population_density < 100:
            bits.append("very low population density")
        if components.fiber_available is False:
            bits.append("no fiber availability")
        why = ", ".join(bits) if bits else "multiple rural indicators present"
        return f"Strong rural-septic indicator: {why}."
    if score >= 70:
        return "Moderate rural-septic indicator — likely on septic, worth qualifying."
    if score >= 40:
        return "Suburban edge — could be on septic or municipal sewer."
    return "Low rural-septic indicator — almost certainly on municipal sewer."


async def lookup_septic_score(
    db: AsyncSession,
    *,
    address: str | None,
    city: str | None,
    state: str,
    zip_code: str | None,
) -> SepticScoreResponse | None:
    """Return the v2 rural_septic_score for a TX address.

    1. Try a direct match in rural_septic_score_v2 (fuzzy on address).
    2. If no match, compute on-the-fly using the same signals.
    """
    state_up = (state or "TX").upper()

    if state_up != "TX":
        return None  # v2 model is TX-only for now

    # ---- 1. Direct MV match ----
    if address:
        try:
            row = (await db.execute(
                text("""
                    SELECT
                        permit_id, address, city, zip_code, county_name,
                        rural_septic_score, in_urban_area, population_density,
                        median_household_income, has_fiber, has_cable, only_satellite,
                        isp_count, lot_acres
                    FROM rural_septic_score_v2
                    WHERE address ILIKE :addr
                      AND (:zip IS NULL OR zip_code = :zip)
                    ORDER BY rural_septic_score DESC
                    LIMIT 1
                """),
                {"addr": f"%{address[:80]}%", "zip": zip_code[:5] if zip_code else None},
            )).first()
            if row:
                components = SepticScoreComponents(
                    in_urban_area=row[6],
                    population_density=float(row[7]) if row[7] is not None else None,
                    median_household_income=float(row[8]) if row[8] is not None else None,
                    fiber_available=row[9],
                    cable_available=row[10],
                    only_satellite=row[11],
                    isp_count=int(row[12]) if row[12] is not None else None,
                    lot_acres=float(row[13]) if row[13] is not None else None,
                )
                score = int(row[5] or 0)
                return SepticScoreResponse(
                    address=row[1] or address,
                    city=row[2] or city,
                    state=state_up,
                    zip=row[3] or zip_code,
                    score=score,
                    tier=_tier_for_score(score),
                    components=components,
                    interpretation=_interpretation_for(components, score),
                    confidence="moderate (v2 model, 2.29x lift over baseline)",
                    source="materialized_view",
                    permit_id=int(row[0]) if row[0] is not None else None,
                    county_name=row[4],
                )
        except Exception as e:
            logger.warning("rural_septic_score_v2 lookup failed: %s", e)

    # ---- 2. Compute on the fly ----
    geo = await resolve_address_to_geo(
        db, address=address, city=city, state=state_up, zip_code=zip_code,
    )

    in_urban: bool | None = None
    if geo["lat"] is not None and geo["lon"] is not None:
        try:
            row = (await db.execute(
                text("SELECT is_in_urban_area(:lat, :lon)"),
                {"lat": geo["lat"], "lon": geo["lon"]},
            )).first()
            if row:
                in_urban = bool(row[0])
        except Exception as e:
            logger.debug("is_in_urban_area failed: %s", e)

    pop_density: float | None = None
    median_income: float | None = None
    if zip_code:
        try:
            row = (await db.execute(
                text("""
                    SELECT pop_density_per_sqmi, median_hh_income
                    FROM zcta_pop_density_tx
                    WHERE zip5 = :zip
                    LIMIT 1
                """),
                {"zip": zip_code[:5]},
            )).first()
            if row:
                pop_density = float(row[0]) if row[0] is not None else None
                median_income = float(row[1]) if row[1] is not None else None
        except Exception as e:
            logger.debug("zcta lookup failed: %s", e)

    # Pull broadband signal via the broadband resolver (reuses geo to avoid
    # a second property_sales/tract lookup).
    bb = await lookup_broadband(
        db, address=address, city=city, state=state_up, zip_code=zip_code, geo=geo,
    )

    components = SepticScoreComponents(
        in_urban_area=in_urban,
        population_density=pop_density,
        median_household_income=median_income,
        fiber_available=bb.has_fiber if bb.isp_count > 0 else None,
        cable_available=bb.has_cable if bb.isp_count > 0 else None,
        only_satellite=bb.only_satellite if bb.isp_count > 0 else None,
        isp_count=bb.isp_count,
        lot_acres=None,
    )

    # Recreate the v2 scoring formula (linearized for on-the-fly compute).
    # See sql/score_v2.sql for the canonical version on T430.
    score = 50
    if in_urban is True:
        score -= 35
    elif in_urban is False:
        score += 15
    if pop_density is not None:
        if pop_density < 100:
            score += 20
        elif pop_density < 500:
            score += 10
        elif pop_density > 2000:
            score -= 15
    if components.only_satellite:
        score += 20
    elif components.fiber_available is False and components.cable_available is False:
        score += 10
    elif components.fiber_available:
        score -= 10
    score = max(0, min(100, score))

    return SepticScoreResponse(
        address=address,
        city=city,
        state=state_up,
        zip=zip_code,
        score=score,
        tier=_tier_for_score(score),
        components=components,
        interpretation=_interpretation_for(components, score),
        confidence="low (on-the-fly compute; v2 MV had no direct match)",
        source="computed",
        permit_id=None,
        county_name=None,
    )


# ---------------------------------------------------------------------------
# Rural leads by county
# ---------------------------------------------------------------------------

async def list_rural_leads_by_county(
    db: AsyncSession,
    *,
    county: str,
    state: str,
    min_score: int,
    limit: int,
) -> list[RuralLead]:
    """Ranked rural-septic leads by county. TX-only for now (the MV is TX-only)."""
    state_up = (state or "TX").upper()
    if state_up != "TX":
        return []

    try:
        rows = (await db.execute(
            text("""
                SELECT
                    permit_id, permit_number, address, city, zip_code, county_name,
                    rural_septic_score, max_dl_mbps, fiber_isp_count, isp_count,
                    only_satellite, has_fiber, in_urban_area, population_density,
                    lot_acres, system_type, source
                FROM rural_septic_score_v2
                WHERE upper(county_name) = upper(:county)
                  AND rural_septic_score >= :min_score
                ORDER BY rural_septic_score DESC, permit_id
                LIMIT :lim
            """),
            {"county": county, "min_score": min_score, "lim": limit},
        )).all()
    except Exception as e:
        logger.warning("rural_septic_score_v2 county query failed: %s", e)
        return []

    leads: list[RuralLead] = []
    for r in rows:
        leads.append(RuralLead(
            permit_id=int(r[0]) if r[0] is not None else 0,
            permit_number=r[1],
            address=r[2],
            city=r[3],
            zip=r[4],
            county_name=r[5],
            rural_septic_score=int(r[6] or 0),
            max_dl_mbps=int(r[7]) if r[7] is not None else None,
            fiber_isp_count=int(r[8] or 0),
            isp_count=int(r[9] or 0),
            only_satellite=bool(r[10]),
            fiber_available=bool(r[11]),
            in_urban_area=r[12],
            population_density=float(r[13]) if r[13] is not None else None,
            lot_acres=float(r[14]) if r[14] is not None else None,
            system_type=r[15],
            source=r[16],
        ))
    return leads
