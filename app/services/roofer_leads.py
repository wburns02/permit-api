"""Service layer for roofer-leads (storm-strike dispatch) queries.

Strategy:
  1. Look up a storm event in noaa_storm_events_details (event_id PK).
  2. For an N-mile radius around (begin_lat, begin_lon), find properties in
     property_sales (uses ix_property_sales_lat/lng if present, else falls
     back to a bounding-box scan).
  3. Score each property with weighted sub-scores:
       - storm_severity      = hail_inches × 10, capped at 30
       - home_age_score      = clamp((current_year - year_built) / 2, 0, 25)
       - mortgage_score      = 20 if active HMDA mortgage match else 0
       - roof_permit_penalty = -20 if a roof permit issued in last 5 years
  4. Cap composite at 100 and sort DESC.

Materialized view `roofer_strike_candidates_v1` is NOT precomputed here — the
straight join is cheap enough at radius=20mi for a single event. If the
`/recent` endpoint times out under load, we will add that MV in a follow-up.

All queries wrap try/except with rollback to avoid poisoning the request
transaction (matches the pattern in app/services/broadband.py).
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.roofer_leads import (
    RooferLeadComponents,
    RooferLeadItem,
    StormEventSummary,
)

logger = logging.getLogger(__name__)


_CURRENT_YEAR = datetime.now(timezone.utc).year

# Reasonable per-call ceiling — keeps query plans index-bound.
_MAX_PROPERTIES_SCANNED = 5000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in miles. Used only for in-Python re-ranking."""
    r = 3958.7613  # earth radius (miles)
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def _score_storm_severity(magnitude: float | None) -> float:
    if magnitude is None:
        return 0.0
    # hail magnitude is inches; cap at 3" for the score (above which it caps the 0-30 range)
    return max(0.0, min(30.0, float(magnitude) * 10.0))


def _score_home_age(year_built: int | None) -> float:
    if year_built is None or year_built < 1850 or year_built > _CURRENT_YEAR:
        return 0.0
    age = _CURRENT_YEAR - int(year_built)
    return max(0.0, min(25.0, age / 2.0))


def _score_mortgage(has_mortgage: bool) -> float:
    return 20.0 if has_mortgage else 0.0


def _score_roof_recency(last_roof_permit_date: datetime | None) -> float:
    """Negative score: a recent roof permit is bad for our lead quality."""
    if last_roof_permit_date is None:
        return 0.0
    delta_days = (datetime.now(timezone.utc) - last_roof_permit_date).days
    if delta_days < 365 * 5:
        return -20.0
    if delta_days < 365 * 10:
        return -10.0
    return 0.0


# ---------------------------------------------------------------------------
# Storm event lookup
# ---------------------------------------------------------------------------


async def fetch_storm_event(
    db: AsyncSession, *, event_id: int
) -> StormEventSummary | None:
    """Look up a single storm event by event_id."""
    try:
        row = (await db.execute(
            text("""
                SELECT
                    event_id, state, event_type, cz_name,
                    begin_datetime, magnitude, magnitude_type,
                    begin_lat, begin_lon, damage_property
                FROM noaa_storm_events_details
                WHERE event_id = :eid
                LIMIT 1
            """),
            {"eid": event_id},
        )).first()
    except Exception as e:
        logger.warning("noaa_storm_events_details lookup failed: %s", e)
        try:
            await db.rollback()
        except Exception:
            pass
        # fall back to legacy `storm_events` table if `_details` is unavailable
        try:
            row = (await db.execute(
                text("""
                    SELECT
                        event_id, state, event_type, cz_name,
                        begin_datetime, magnitude, magnitude_type,
                        begin_lat, begin_lon, damage_property
                    FROM storm_events
                    WHERE event_id = :eid
                    LIMIT 1
                """),
                {"eid": event_id},
            )).first()
        except Exception as e2:
            logger.warning("storm_events fallback lookup failed: %s", e2)
            try:
                await db.rollback()
            except Exception:
                pass
            return None

    if not row:
        return None

    return StormEventSummary(
        event_id=int(row[0]),
        state=row[1],
        event_type=row[2],
        cz_name=row[3],
        begin_datetime=row[4],
        magnitude=float(row[5]) if row[5] is not None else None,
        magnitude_type=row[6],
        begin_lat=float(row[7]) if row[7] is not None else None,
        begin_lon=float(row[8]) if row[8] is not None else None,
        damage_property=row[9],
    )


async def list_recent_hail_events(
    db: AsyncSession,
    *,
    state: str,
    days_back: int,
    min_magnitude: float,
    limit: int = 200,
) -> list[StormEventSummary]:
    """Return all hail events in `state` within the last `days_back` days."""
    state_up = (state or "").upper()
    if len(state_up) != 2 or not state_up.isalpha():
        return []

    try:
        rows = (await db.execute(
            text("""
                SELECT
                    event_id, state, event_type, cz_name,
                    begin_datetime, magnitude, magnitude_type,
                    begin_lat, begin_lon, damage_property
                FROM noaa_storm_events_details
                WHERE state = :st
                  AND event_type ILIKE 'Hail'
                  AND magnitude >= :mag
                  AND begin_datetime >= NOW() - (:dback || ' days')::interval
                  AND begin_lat IS NOT NULL AND begin_lon IS NOT NULL
                ORDER BY begin_datetime DESC
                LIMIT :lim
            """),
            {"st": state_up, "mag": min_magnitude, "dback": days_back, "lim": limit},
        )).all()
    except Exception as e:
        logger.warning("recent hail events query failed: %s", e)
        try:
            await db.rollback()
        except Exception:
            pass
        return []

    out: list[StormEventSummary] = []
    for r in rows:
        out.append(StormEventSummary(
            event_id=int(r[0]),
            state=r[1],
            event_type=r[2],
            cz_name=r[3],
            begin_datetime=r[4],
            magnitude=float(r[5]) if r[5] is not None else None,
            magnitude_type=r[6],
            begin_lat=float(r[7]) if r[7] is not None else None,
            begin_lon=float(r[8]) if r[8] is not None else None,
            damage_property=r[9],
        ))
    return out


# ---------------------------------------------------------------------------
# Core scoring query — given event, return ranked properties
# ---------------------------------------------------------------------------


async def score_properties_for_event(
    db: AsyncSession,
    *,
    event: StormEventSummary,
    days_after: int,
    radius_miles: float,
    limit: int,
    min_score: float = 0.0,
) -> list[RooferLeadItem]:
    """Score properties within a storm event's footprint.

    Single SQL round-trip that joins:
      - property_sales (bounding-box scan around storm centroid)
      - hmda_lar_2020_2024 (active mortgages by address)
      - permits_<state> (recent roof permits) — only if state partition exists
    """
    if event.begin_lat is None or event.begin_lon is None:
        return []

    # Bounding box from centroid + radius (degrees).
    # 1 degree latitude ≈ 69 miles; 1 degree longitude ≈ 69 × cos(lat) miles.
    lat = float(event.begin_lat)
    lon = float(event.begin_lon)
    lat_pad = radius_miles / 69.0
    lon_pad = radius_miles / max(0.1, 69.0 * math.cos(math.radians(lat)))

    state_up = (event.state or "").upper()
    permits_table_clause = ""
    permits_join = ""
    has_permits_table = False

    if len(state_up) == 2 and state_up.isalpha():
        permits_table = f"permits_{state_up.lower()}"
        # Test if partition exists (cheap pg_class probe).
        try:
            exists = (await db.execute(
                text("SELECT 1 FROM pg_class WHERE relname = :n LIMIT 1"),
                {"n": permits_table},
            )).first()
            has_permits_table = bool(exists)
        except Exception:
            try:
                await db.rollback()
            except Exception:
                pass
            has_permits_table = False

        if has_permits_table:
            permits_join = f"""
                LEFT JOIN LATERAL (
                    SELECT MAX(issue_date) AS last_roof_permit_date
                    FROM {permits_table} p
                    WHERE COALESCE(p.address, '') ILIKE ps.norm_addr || '%'
                      AND (p.permit_type ILIKE '%roof%'
                           OR COALESCE(p.description, '') ILIKE '%roof%')
                      AND p.issue_date >= NOW() - INTERVAL '10 years'
                    LIMIT 1
                ) rp ON TRUE
            """
            permits_table_clause = ", rp.last_roof_permit_date"

    # We attempt the full join. If something fails (e.g. column name drift on
    # property_sales), fall back to a property-only query.
    sql = f"""
        SELECT
            ps.norm_addr        AS address,
            ps.city             AS city,
            ps.state            AS state,
            ps.zip              AS zip,
            ps.county           AS county,
            ps.lat              AS lat,
            ps.lng              AS lon,
            ps.year_built       AS year_built,
            (h.address_norm IS NOT NULL) AS has_active_mortgage
            {permits_table_clause}
        FROM property_sales ps
        LEFT JOIN LATERAL (
            SELECT address_norm
            FROM hmda_lar_2020_2024 hl
            WHERE hl.property_state = ps.state
              AND hl.action_taken = 1
              AND hl.loan_purpose IN (1, 31, 32)
              AND hl.address_norm = UPPER(REGEXP_REPLACE(ps.norm_addr, '[^A-Za-z0-9 ]', ' ', 'g'))
            LIMIT 1
        ) h ON TRUE
        {permits_join}
        WHERE ps.lat BETWEEN :lat_lo AND :lat_hi
          AND ps.lng BETWEEN :lon_lo AND :lon_hi
          AND ps.state = :st
        LIMIT :scan_lim
    """

    params = {
        "lat_lo": lat - lat_pad,
        "lat_hi": lat + lat_pad,
        "lon_lo": lon - lon_pad,
        "lon_hi": lon + lon_pad,
        "st": state_up,
        "scan_lim": _MAX_PROPERTIES_SCANNED,
    }

    rows = []
    try:
        rows = (await db.execute(text(sql), params)).all()
    except Exception as e:
        logger.warning("roofer-leads full join failed (%s) — retrying without HMDA/permits", e)
        try:
            await db.rollback()
        except Exception:
            pass
        # Minimal fallback: just property_sales bounding-box.
        try:
            rows = (await db.execute(
                text("""
                    SELECT
                        ps.norm_addr, ps.city, ps.state, ps.zip, ps.county,
                        ps.lat, ps.lng, ps.year_built,
                        FALSE AS has_active_mortgage
                    FROM property_sales ps
                    WHERE ps.lat BETWEEN :lat_lo AND :lat_hi
                      AND ps.lng BETWEEN :lon_lo AND :lon_hi
                      AND ps.state = :st
                    LIMIT :scan_lim
                """),
                params,
            )).all()
            has_permits_table = False
        except Exception as e2:
            logger.warning("roofer-leads fallback also failed: %s", e2)
            try:
                await db.rollback()
            except Exception:
                pass
            return []

    storm_severity = _score_storm_severity(event.magnitude)
    storm_started = event.begin_datetime
    if storm_started is not None and storm_started.tzinfo is None:
        storm_started = storm_started.replace(tzinfo=timezone.utc)

    items: list[RooferLeadItem] = []
    for r in rows:
        prop_lat = float(r[5]) if r[5] is not None else None
        prop_lon = float(r[6]) if r[6] is not None else None
        if prop_lat is None or prop_lon is None:
            continue

        dist = _haversine_miles(lat, lon, prop_lat, prop_lon)
        if dist > radius_miles:
            continue  # bounding box admits more than the radius circle

        year_built = int(r[7]) if r[7] is not None else None
        has_mortgage = bool(r[8])
        last_roof = r[9] if has_permits_table and len(r) > 9 else None
        if last_roof is not None and not isinstance(last_roof, datetime):
            # column may come back as date
            try:
                last_roof = datetime(last_roof.year, last_roof.month, last_roof.day, tzinfo=timezone.utc)
            except Exception:
                last_roof = None

        components = RooferLeadComponents(
            storm_severity=storm_severity,
            home_age_score=_score_home_age(year_built),
            mortgage_score=_score_mortgage(has_mortgage),
            roof_permit_recency_penalty=_score_roof_recency(last_roof),
            distance_miles=round(dist, 2),
        )
        composite = (
            components.storm_severity
            + components.home_age_score
            + components.mortgage_score
            + components.roof_permit_recency_penalty
        )
        # Distance falloff: linear, 0% at radius, 100% at 0
        if radius_miles > 0:
            composite *= max(0.0, 1.0 - (dist / radius_miles) * 0.3)
        composite = max(0.0, min(100.0, composite))

        if composite < min_score:
            continue

        days_after_storm: int | None = None
        if storm_started is not None:
            days_after_storm = max(0, (datetime.now(timezone.utc) - storm_started).days)

        items.append(RooferLeadItem(
            address=r[0],
            city=r[1],
            state=r[2],
            zip=r[3],
            county=r[4],
            lat=prop_lat,
            lon=prop_lon,
            year_built=year_built,
            has_active_mortgage=has_mortgage,
            recent_roof_permit_date=(
                last_roof.date() if isinstance(last_roof, datetime) else None
            ),
            storm_event_id=event.event_id,
            storm_date=storm_started,
            days_after_storm=days_after_storm,
            storm_magnitude=event.magnitude,
            composite_score=round(composite, 2),
            components=components,
        ))

    items.sort(key=lambda it: it.composite_score, reverse=True)
    return items[:limit]


async def list_roofer_leads_recent(
    db: AsyncSession,
    *,
    state: str,
    days_back: int,
    min_score: int,
    limit: int,
    min_magnitude: float = 1.0,
    radius_miles: float = 20.0,
) -> tuple[int, list[RooferLeadItem]]:
    """Multi-event roll-up: score all properties hit by hail in last N days."""
    events = await list_recent_hail_events(
        db, state=state, days_back=days_back,
        min_magnitude=min_magnitude, limit=50,
    )
    if not events:
        return (0, [])

    # Per-event budget so a single mega-event doesn't starve others.
    per_event_budget = max(20, limit // max(1, len(events)) * 3)

    all_items: list[RooferLeadItem] = []
    for ev in events:
        items = await score_properties_for_event(
            db, event=ev,
            days_after=days_back, radius_miles=radius_miles,
            limit=per_event_budget, min_score=float(min_score),
        )
        all_items.extend(items)

    # Dedup by address (keep highest score)
    seen: dict[str, RooferLeadItem] = {}
    for it in all_items:
        key = f"{(it.address or '').upper().strip()}|{it.zip or ''}"
        if not key.strip("|"):
            continue
        prior = seen.get(key)
        if prior is None or it.composite_score > prior.composite_score:
            seen[key] = it

    deduped = sorted(seen.values(), key=lambda it: it.composite_score, reverse=True)
    return (len(events), deduped[:limit])
