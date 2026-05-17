"""Service layer for real-time rural-score lookup.

Pipeline:
  1. Try property_sales fuzzy match for lat/lon (cheapest, indexed).
  2. Fall back to Census Geocoder API (free, ~5k/day rate limit) — cached in
     the `geocoded_addresses` table.
  3. Compute components:
       - in_urban_area via is_in_urban_area(lat, lon) PG function
       - pop_density via census_acs_2023_zcta (preferred) or zcta_pop_density_<state>
       - lot_acres via parcel_lookup_v5 (Hill Country + HCAD polygons)
       - broadband via lookup_broadband
  4. Apply the v5 scoring formula (mirror of rural_septic_score_v5 MV).

Deliverable D (Census geocoder cache table) is created here if not present so
both this agent (E) and the D agent can populate it without conflict.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.broadband import BroadbandLookupResponse
from app.schemas.rural_score import (
    RuralScoreComponents,
    RuralScoreLookupResponse,
)
from app.services.broadband import lookup_broadband, resolve_address_to_geo

logger = logging.getLogger(__name__)


_CENSUS_GEOCODER_URL = (
    "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
)


# ---------------------------------------------------------------------------
# Geocoded-addresses cache (shared with agent D's deliverable)
# ---------------------------------------------------------------------------


async def ensure_geocoded_addresses_table(db: AsyncSession) -> None:
    """Create the geocoded_addresses cache table if missing.

    Coordinated with agent D so both can populate without schema conflicts.
    IF NOT EXISTS is idempotent.
    """
    try:
        await db.execute(text("""
            CREATE TABLE IF NOT EXISTS geocoded_addresses (
                address_norm TEXT PRIMARY KEY,
                lat NUMERIC(9,6),
                lon NUMERIC(9,6),
                match_type TEXT,
                source TEXT,
                geocoded_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        await db.commit()
    except Exception as e:
        logger.debug("ensure_geocoded_addresses_table failed (likely ok): %s", e)
        try:
            await db.rollback()
        except Exception:
            pass


def _normalize_address(address: str, city: str | None, state: str, zip_code: str | None) -> str:
    parts = [
        (address or "").strip().upper(),
        (city or "").strip().upper() if city else "",
        (state or "").strip().upper(),
        (zip_code or "")[:5].strip(),
    ]
    return " ".join(p for p in parts if p)


async def _read_geocode_cache(
    db: AsyncSession, address_norm: str
) -> tuple[float, float, str] | None:
    """Return (lat, lon, match_type) from cache, or None."""
    try:
        row = (await db.execute(
            text("SELECT lat, lon, match_type FROM geocoded_addresses WHERE address_norm = :a"),
            {"a": address_norm},
        )).first()
        if row and row[0] is not None and row[1] is not None:
            return (float(row[0]), float(row[1]), row[2] or "cached")
    except Exception as e:
        logger.debug("geocode cache read failed (table may not exist yet): %s", e)
        try:
            await db.rollback()
        except Exception:
            pass
    return None


async def _write_geocode_cache(
    db: AsyncSession,
    address_norm: str,
    lat: float | None,
    lon: float | None,
    match_type: str,
    source: str,
) -> None:
    """Upsert into the cache. Best-effort — never blocks the request."""
    try:
        await db.execute(
            text("""
                INSERT INTO geocoded_addresses
                  (address_norm, lat, lon, match_type, source, geocoded_at)
                VALUES (:a, :lat, :lon, :mt, :src, NOW())
                ON CONFLICT (address_norm) DO UPDATE SET
                  lat = EXCLUDED.lat,
                  lon = EXCLUDED.lon,
                  match_type = EXCLUDED.match_type,
                  source = EXCLUDED.source,
                  geocoded_at = NOW()
            """),
            {"a": address_norm, "lat": lat, "lon": lon, "mt": match_type, "src": source},
        )
        await db.commit()
    except Exception as e:
        logger.debug("geocode cache write failed: %s", e)
        try:
            await db.rollback()
        except Exception:
            pass


async def _call_census_geocoder(
    address: str, city: str | None, state: str, zip_code: str | None
) -> tuple[float, float, str] | None:
    """Call the free Census Geocoder. Returns (lat, lon, match_type) or None."""
    parts = [address.strip()]
    if city:
        parts.append(city.strip())
    if state:
        parts.append(state.strip())
    if zip_code:
        parts.append(zip_code[:5].strip())
    one_line = ", ".join(p for p in parts if p)

    params = {
        "address": one_line,
        "benchmark": "Public_AR_Current",
        "format": "json",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(_CENSUS_GEOCODER_URL, params=params)
            resp.raise_for_status()
            body: dict[str, Any] = resp.json()
    except Exception as e:
        logger.warning("Census geocoder call failed: %s", e)
        return None

    matches = (body.get("result") or {}).get("addressMatches") or []
    if not matches:
        return None
    coords = matches[0].get("coordinates") or {}
    lat = coords.get("y")
    lon = coords.get("x")
    if lat is None or lon is None:
        return None
    return (float(lat), float(lon), matches[0].get("matchedAddress") or "match")


# ---------------------------------------------------------------------------
# Address resolution (property_sales → cache → Census geocoder)
# ---------------------------------------------------------------------------


async def resolve_address(
    db: AsyncSession,
    *,
    address: str,
    city: str | None,
    state: str,
    zip_code: str | None,
) -> dict:
    """Return {lat, lon, source, confidence}."""
    state_up = (state or "").upper()
    out: dict[str, Any] = {
        "lat": None, "lon": None, "source": "none", "confidence": "low",
    }

    # 1. property_sales (existing resolver).
    geo = await resolve_address_to_geo(
        db, address=address, city=city, state=state_up, zip_code=zip_code,
    )
    if geo.get("lat") is not None and geo.get("lon") is not None:
        if geo["match_method"] == "property_sales":
            out.update(lat=geo["lat"], lon=geo["lon"], source="property_sales", confidence="high")
            return out
        if geo["match_method"] == "zcta_centroid":
            # Keep as a fallback but try census first for better accuracy.
            zcta_fallback = (geo["lat"], geo["lon"])
        else:
            zcta_fallback = None
    else:
        zcta_fallback = None

    # 2. geocoded_addresses cache.
    addr_norm = _normalize_address(address, city, state_up, zip_code)
    cached = await _read_geocode_cache(db, addr_norm)
    if cached:
        lat, lon, _ = cached
        out.update(lat=lat, lon=lon, source="cache", confidence="medium")
        return out

    # 3. Census geocoder (live).
    await ensure_geocoded_addresses_table(db)
    census = await _call_census_geocoder(address, city, state_up, zip_code)
    if census:
        lat, lon, mt = census
        await _write_geocode_cache(db, addr_norm, lat, lon, mt, "census")
        out.update(lat=lat, lon=lon, source="census", confidence="medium")
        return out

    # 4. ZCTA centroid (worst case — still beats nothing).
    if zcta_fallback:
        out.update(lat=zcta_fallback[0], lon=zcta_fallback[1], source="zcta_centroid", confidence="low")
        return out

    return out


# ---------------------------------------------------------------------------
# Component lookups
# ---------------------------------------------------------------------------


async def _is_in_urban_area(db: AsyncSession, lat: float, lon: float) -> bool | None:
    try:
        row = (await db.execute(
            text("SELECT is_in_urban_area(:lat, :lon)"),
            {"lat": lat, "lon": lon},
        )).first()
        if row:
            return bool(row[0])
    except Exception as e:
        logger.debug("is_in_urban_area failed: %s", e)
        try:
            await db.rollback()
        except Exception:
            pass
    return None


async def _pop_density(
    db: AsyncSession, *, zip_code: str | None, state: str
) -> float | None:
    """Try census_acs_2023_zcta first; fall back to per-state zcta_pop_density_<state>."""
    if not zip_code:
        return None
    zip5 = zip_code[:5]

    # Preferred: nationwide census_acs_2023_zcta if it exists.
    try:
        row = (await db.execute(
            text("""
                SELECT population_density_per_sqmi
                FROM census_acs_2023_zcta
                WHERE zcta = :z
                LIMIT 1
            """),
            {"z": zip5},
        )).first()
        if row and row[0] is not None:
            return float(row[0])
    except Exception as e:
        logger.debug("census_acs_2023_zcta lookup failed: %s", e)
        try:
            await db.rollback()
        except Exception:
            pass

    # Fallback: per-state legacy table.
    state_up = (state or "").upper()
    if len(state_up) != 2 or not state_up.isalpha():
        return None
    table = f"zcta_pop_density_{state_up.lower()}"
    try:
        row = (await db.execute(
            text(f"SELECT pop_density_per_sqmi FROM {table} WHERE zip5 = :z LIMIT 1"),
            {"z": zip5},
        )).first()
        if row and row[0] is not None:
            return float(row[0])
    except Exception as e:
        logger.debug("%s lookup failed: %s", table, e)
        try:
            await db.rollback()
        except Exception:
            pass
    return None


async def _lot_acres(db: AsyncSession, lat: float, lon: float) -> float | None:
    """Look up parcel-derived lot size via parcel_lookup_v5 (Hill Country / HCAD)."""
    try:
        row = (await db.execute(
            text("""
                SELECT effective_lot_acres::float8
                FROM parcel_lookup_v5
                WHERE lat BETWEEN :lat - 0.01 AND :lat + 0.01
                  AND lon BETWEEN :lon - 0.01 AND :lon + 0.01
                ORDER BY (lat - :lat) * (lat - :lat) + (lon - :lon) * (lon - :lon)
                LIMIT 1
            """),
            {"lat": lat, "lon": lon},
        )).first()
        if row and row[0] is not None:
            return float(row[0])
    except Exception as e:
        logger.debug("parcel_lookup_v5 lookup failed: %s", e)
        try:
            await db.rollback()
        except Exception:
            pass
    return None


# ---------------------------------------------------------------------------
# Scoring (mirror of rural_septic_score_v5 MV formula)
# ---------------------------------------------------------------------------


def _tier_for_score(score: int) -> str:
    if score >= 85:
        return "high-rural"
    if score >= 70:
        return "rural"
    if score >= 40:
        return "suburban"
    return "urban"


def _interpretation(c: RuralScoreComponents, score: int) -> str:
    if c.in_urban_area is True:
        return "In a Census-defined urban area — unlikely to be on septic."
    if score >= 85:
        bits = []
        if c.only_satellite:
            bits.append("satellite-only broadband")
        if c.lot_acres is not None and c.lot_acres >= 2.0:
            bits.append(f"{c.lot_acres:.1f}-acre lot")
        if c.population_density is not None and c.population_density < 100:
            bits.append("very low population density")
        if c.fiber_available is False:
            bits.append("no fiber availability")
        why = ", ".join(bits) if bits else "multiple rural indicators present"
        return f"Strong rural-septic indicator: {why}."
    if score >= 70:
        return "Moderate rural-septic indicator — likely on septic, worth qualifying."
    if score >= 40:
        return "Suburban edge — could be on septic or municipal sewer."
    return "Low rural-septic indicator — almost certainly on municipal sewer."


def _compute_v5_score(c: RuralScoreComponents) -> int:
    """V5 scoring formula — mirrors sql/score_v5.sql."""
    score = 50

    # Urban-area gate
    if c.in_urban_area is True:
        score -= 35
    elif c.in_urban_area is False:
        score += 15

    # Lot acres
    if c.lot_acres is not None:
        if c.lot_acres >= 5.0:
            score += 25
        elif c.lot_acres >= 2.0:
            score += 15
        elif c.lot_acres >= 1.0:
            score += 8
        elif c.lot_acres < 0.25:
            score -= 10

    # Population density (per sq mi)
    if c.population_density is not None:
        if c.population_density < 50:
            score += 20
        elif c.population_density < 200:
            score += 12
        elif c.population_density < 500:
            score += 5
        elif c.population_density > 2000:
            score -= 15

    # Broadband
    if c.only_satellite:
        score += 20
    elif c.fiber_available is False and c.cable_available is False:
        score += 10
    elif c.fiber_available:
        score -= 10

    return max(0, min(100, score))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def real_time_rural_score(
    db: AsyncSession,
    *,
    address: str,
    city: str | None,
    state: str,
    zip_code: str | None,
) -> RuralScoreLookupResponse:
    """On-the-fly rural_septic_score for ANY address."""
    state_up = (state or "").upper()
    resolved = await resolve_address(
        db, address=address, city=city, state=state_up, zip_code=zip_code,
    )
    lat = resolved.get("lat")
    lon = resolved.get("lon")

    in_urban: bool | None = None
    pop_density: float | None = None
    lot_acres: float | None = None
    bb: BroadbandLookupResponse | None = None

    if lat is not None and lon is not None:
        in_urban = await _is_in_urban_area(db, lat, lon)
        lot_acres = await _lot_acres(db, lat, lon)

    pop_density = await _pop_density(db, zip_code=zip_code, state=state_up)

    # Broadband: reuse the existing service with the resolved geo.
    if lat is not None and lon is not None:
        # Snap tract for the broadband resolver.
        try:
            tract_row = (await db.execute(
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
            tract_geoid = tract_row[0] if tract_row else None
        except Exception:
            try:
                await db.rollback()
            except Exception:
                pass
            tract_geoid = None

        geo = {
            "lat": lat, "lon": lon,
            "tract_geoid": tract_geoid, "block_geoid": None,
            "match_method": resolved.get("source") or "real_time",
        }
        try:
            bb = await lookup_broadband(
                db, address=address, city=city, state=state_up, zip_code=zip_code, geo=geo,
            )
        except Exception as e:
            logger.warning("real-time rural-score broadband lookup failed: %s", e)
            try:
                await db.rollback()
            except Exception:
                pass
            bb = None

    components = RuralScoreComponents(
        in_urban_area=in_urban,
        population_density=pop_density,
        lot_acres=lot_acres,
        fiber_available=(bb.has_fiber if bb and bb.isp_count > 0 else None),
        cable_available=(bb.has_cable if bb and bb.isp_count > 0 else None),
        only_satellite=(bb.only_satellite if bb and bb.isp_count > 0 else None),
        fiber_isp_count=(bb.fiber_isp_count if bb else 0),
        isp_count=(bb.isp_count if bb else 0),
    )
    score = _compute_v5_score(components)

    return RuralScoreLookupResponse(
        address=address,
        city=city,
        state=state_up,
        zip=zip_code,
        lat=lat,
        lon=lon,
        rural_septic_score=score,
        tier=_tier_for_score(score),
        components=components,
        geocode_source=resolved.get("source") or "none",
        confidence=resolved.get("confidence") or "low",
        interpretation=_interpretation(components, score),
    )
