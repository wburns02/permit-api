"""NOAA Storm Events loader — per-year CSVs from NCEI.

Source: https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/

Filters: state IN {TEXAS, LOUISIANA}. Keeps all event types — hail,
thunderstorm wind, high/strong wind, tropical storm, hurricane, tornado,
flood, drought, etc. Roof/siding/fence/foundation lead funnels all map to
these. LOUISIANA was added for the East Baton Rouge Parish (FIPS 22033)
WIND/tropical storm-lead product (EBR gets little hail; the roof-damage
peril there is wind + tropical systems). The cz_name/cz_fips/state join
machinery downstream is already state-agnostic, so adding a state here is
all that's needed to feed LA `storm_events`.

This module is the only place the NOAA load lives now — the previous
T430 cron + CLI script (`scripts/backfill_noaa_storm_events.py` in the
permit-api-live checkout) is deprecated.

Each successful run writes a row into `cron_heartbeat` so the
`/v1/hail-leads/health` endpoint can report freshness without inferring
it from `pg_stat_user_tables`.
"""

from __future__ import annotations

import csv
import gzip
import io
import logging
import re
import time
from datetime import date, datetime
from typing import Any

import httpx
from sqlalchemy import text

from app.database import primary_session_maker

logger = logging.getLogger(__name__)


INDEX_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
# States we ingest. NCEI's STATE column is uppercase full names. TEXAS drives
# the hail-lead product; LOUISIANA drives the East Baton Rouge (FIPS 22033)
# wind/tropical storm-lead arm.
STATE_FILTER: frozenset[str] = frozenset({"TEXAS", "LOUISIANA"})
HEARTBEAT_NAME = "storm_events_load"

_INDEX_RX = re.compile(
    r'href="(StormEvents_details-ftp_v1\.0_d(\d{4})_c(\d{8})\.csv\.gz)"'
)

_COLS = [
    "event_id", "episode_id", "state", "state_fips", "year", "event_type",
    "cz_type", "cz_fips", "cz_name", "wfo",
    "begin_datetime", "end_datetime", "cz_timezone",
    "injuries_direct", "injuries_indirect", "deaths_direct", "deaths_indirect",
    "damage_property", "damage_crops", "source",
    "magnitude", "magnitude_type", "flood_cause", "tor_f_scale",
    "begin_location", "end_location", "begin_lat", "begin_lon", "end_lat", "end_lon",
    "episode_narrative", "event_narrative", "scraped_at",
]

_UPSERT_SQL = text(
    """
    INSERT INTO storm_events (
        event_id, episode_id, state, state_fips, year, event_type,
        cz_type, cz_fips, cz_name, wfo,
        begin_datetime, end_datetime, cz_timezone,
        injuries_direct, injuries_indirect, deaths_direct, deaths_indirect,
        damage_property, damage_crops, source,
        magnitude, magnitude_type, flood_cause, tor_f_scale,
        begin_location, end_location, begin_lat, begin_lon, end_lat, end_lon,
        episode_narrative, event_narrative, scraped_at
    ) VALUES (
        :event_id, :episode_id, :state, :state_fips, :year, :event_type,
        :cz_type, :cz_fips, :cz_name, :wfo,
        :begin_datetime, :end_datetime, :cz_timezone,
        :injuries_direct, :injuries_indirect, :deaths_direct, :deaths_indirect,
        :damage_property, :damage_crops, :source,
        :magnitude, :magnitude_type, :flood_cause, :tor_f_scale,
        :begin_location, :end_location, :begin_lat, :begin_lon, :end_lat, :end_lon,
        :episode_narrative, :event_narrative, :scraped_at
    )
    ON CONFLICT (event_id) DO UPDATE SET
        damage_property = COALESCE(EXCLUDED.damage_property, storm_events.damage_property),
        magnitude       = COALESCE(EXCLUDED.magnitude, storm_events.magnitude),
        event_narrative = COALESCE(EXCLUDED.event_narrative, storm_events.event_narrative),
        scraped_at      = EXCLUDED.scraped_at
    """
)


def _i(s: Any) -> int | None:
    if s is None or s == "":
        return None
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def _f(s: Any) -> float | None:
    if s is None or s == "":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _ts(date_str: str | None) -> datetime | None:
    """Parse 'DD-MON-YY HH:MM:SS' (e.g. '15-APR-24 17:23:00'); fall back to ISO."""
    if not date_str:
        return None
    s = date_str.strip()
    try:
        return datetime.strptime(s, "%d-%b-%y %H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None


async def _list_year_urls(client: httpx.AsyncClient) -> dict[int, str]:
    """Return {year: url}, picking the latest c-date variant per year."""
    r = await client.get(INDEX_URL, timeout=30)
    r.raise_for_status()
    best: dict[int, tuple[str, str]] = {}
    for m in _INDEX_RX.finditer(r.text):
        fname, yr_s, cdate = m.group(1), m.group(2), m.group(3)
        yr = int(yr_s)
        prev = best.get(yr)
        if prev is None or cdate > prev[1]:
            best[yr] = (fname, cdate)
    return {yr: INDEX_URL + t[0] for yr, t in best.items()}


async def _fetch_and_filter(client: httpx.AsyncClient, url: str) -> list[dict[str, Any]]:
    """GET the per-year .csv.gz, gunzip, filter to STATE_FILTER rows."""
    r = await client.get(url, timeout=120)
    r.raise_for_status()
    raw = gzip.decompress(r.content).decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(raw))
    today = date.today()
    out: list[dict[str, Any]] = []
    for row in reader:
        if row.get("STATE") not in STATE_FILTER:
            continue
        eid = _i(row.get("EVENT_ID"))
        if eid is None:
            continue
        out.append({
            "event_id":          eid,
            "episode_id":        _i(row.get("EPISODE_ID")),
            "state":             row.get("STATE"),
            "state_fips":        _i(row.get("STATE_FIPS")),
            "year":              _i(row.get("YEAR")),
            "event_type":        row.get("EVENT_TYPE"),
            "cz_type":           row.get("CZ_TYPE"),
            "cz_fips":           _i(row.get("CZ_FIPS")),
            "cz_name":           row.get("CZ_NAME"),
            "wfo":               row.get("WFO"),
            "begin_datetime":    _ts(row.get("BEGIN_DATE_TIME")),
            "end_datetime":      _ts(row.get("END_DATE_TIME")),
            "cz_timezone":       row.get("CZ_TIMEZONE"),
            "injuries_direct":   _i(row.get("INJURIES_DIRECT")),
            "injuries_indirect": _i(row.get("INJURIES_INDIRECT")),
            "deaths_direct":     _i(row.get("DEATHS_DIRECT")),
            "deaths_indirect":   _i(row.get("DEATHS_INDIRECT")),
            "damage_property":   row.get("DAMAGE_PROPERTY"),
            "damage_crops":      row.get("DAMAGE_CROPS"),
            "source":            row.get("SOURCE"),
            "magnitude":         _f(row.get("MAGNITUDE")),
            "magnitude_type":    row.get("MAGNITUDE_TYPE"),
            "flood_cause":       row.get("FLOOD_CAUSE"),
            "tor_f_scale":       row.get("TOR_F_SCALE"),
            "begin_location":    row.get("BEGIN_LOCATION"),
            "end_location":      row.get("END_LOCATION"),
            "begin_lat":         _f(row.get("BEGIN_LAT")),
            "begin_lon":         _f(row.get("BEGIN_LON")),
            "end_lat":           _f(row.get("END_LAT")),
            "end_lon":           _f(row.get("END_LON")),
            "episode_narrative": (row.get("EPISODE_NARRATIVE") or "")[:2000] or None,
            "event_narrative":   (row.get("EVENT_NARRATIVE") or "")[:2000] or None,
            "scraped_at":        today,
        })
    return out


async def _upsert_records(records: list[dict[str, Any]]) -> tuple[int, int]:
    """INSERT ... ON CONFLICT in chunks. Returns (inserted_or_updated, errors).

    SQLAlchemy's executemany over text() with ON CONFLICT does the right
    thing on asyncpg; we batch in chunks of 500 to keep statement payload
    bounded.
    """
    if not records:
        return 0, 0

    affected = 0
    errors = 0
    chunk_size = 500

    async with primary_session_maker() as db:
        # Disable the 20s connection-level statement_timeout for this
        # session — 500-row INSERT...ON CONFLICT chunks against an asyncpg
        # pool under load can blow past 20s, killing the whole load.
        try:
            await db.execute(text("SET statement_timeout = 0"))
            await db.commit()
        except Exception as exc:  # noqa: BLE001
            await db.rollback()
            logger.warning("noaa upsert: failed to disable timeout (%s)", exc)

        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            try:
                result = await db.execute(_UPSERT_SQL, chunk)
                # asyncpg/psycopg report rowcount per statement; in
                # executemany mode this can be -1, so we fall back to
                # len(chunk) which is the upper bound.
                rc = result.rowcount if result.rowcount and result.rowcount > 0 else len(chunk)
                affected += rc
                await db.commit()
            except Exception as exc:  # noqa: BLE001
                errors += 1
                await db.rollback()
                logger.exception("storm_events upsert chunk failed (size=%d): %s", len(chunk), exc)

    return affected, errors


async def _write_heartbeat(duration: float, row_count: int, last_error: str | None) -> None:
    """Mirror the upsert pattern in app.services.mv_refresh."""
    try:
        async with primary_session_maker() as db:
            await db.execute(
                text(
                    """
                    INSERT INTO cron_heartbeat
                        (name, beat_at, duration_seconds, row_count, last_error)
                    VALUES
                        (:name, NOW(), :dur, :rows, :err)
                    ON CONFLICT (name) DO UPDATE SET
                        beat_at = EXCLUDED.beat_at,
                        duration_seconds = EXCLUDED.duration_seconds,
                        row_count = EXCLUDED.row_count,
                        last_error = EXCLUDED.last_error
                    """
                ),
                {
                    "name": HEARTBEAT_NAME,
                    "dur": round(duration, 2),
                    "rows": row_count,
                    "err": last_error,
                },
            )
            await db.commit()
    except Exception:  # noqa: BLE001
        logger.exception("Failed to write heartbeat for %s", HEARTBEAT_NAME)


async def load_noaa_storm_events(years: int = 1) -> dict[str, Any]:
    """Pull the last `years` years of TEXAS storm events from NCEI.

    Idempotent. Safe to call repeatedly. Each year is fetched + upserted
    in its own try/except so a single bad year doesn't kill the rest.

    Args:
        years: How many trailing calendar years to pull (default 1 =
            the current year only). `years=2` pulls current + previous
            year, etc.

    Returns:
        {
            "downloaded_files": int,
            "rows_inserted": int,         # rowcount sum from upserts
            "rows_updated":  int,         # placeholder; see note below
            "duration_seconds": float,
            "errors": list[str],
        }

        Note: Postgres' ON CONFLICT DO UPDATE doesn't distinguish
        inserts from updates in rowcount. We report the combined number
        in `rows_inserted` and leave `rows_updated` at 0 for forward
        compatibility — callers should treat `rows_inserted` as
        "rows touched".
    """
    started = time.monotonic()
    current_year = datetime.utcnow().year
    # years=1 -> [current], years=2 -> [current-1, current], etc.
    target_years = list(range(current_year - years + 1, current_year + 1))

    logger.info(
        "NOAA storm_events load starting: states=%s years=%s",
        sorted(STATE_FILTER), target_years,
    )

    downloaded_files = 0
    rows_touched = 0
    errors: list[str] = []

    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            urls = await _list_year_urls(client)
            logger.info("NCEI index: %d yearly files available", len(urls))
        except Exception as exc:  # noqa: BLE001
            msg = f"index fetch failed: {type(exc).__name__}: {exc}"
            logger.exception(msg)
            errors.append(msg)
            urls = {}

        for yr in target_years:
            url = urls.get(yr)
            if not url:
                msg = f"year {yr}: not in NCEI index, skipping"
                logger.warning(msg)
                errors.append(msg)
                continue

            try:
                records = await _fetch_and_filter(client, url)
                downloaded_files += 1
            except Exception as exc:  # noqa: BLE001
                msg = f"year {yr}: fetch error {type(exc).__name__}: {exc}"
                logger.exception(msg)
                errors.append(msg)
                continue

            try:
                affected, chunk_errors = await _upsert_records(records)
            except Exception as exc:  # noqa: BLE001
                msg = f"year {yr}: upsert error {type(exc).__name__}: {exc}"
                logger.exception(msg)
                errors.append(msg)
                continue

            rows_touched += affected
            logger.info(
                "year %d: %d events fetched (states=%s), %d rows touched, %d chunk errors",
                yr, len(records), sorted(STATE_FILTER), affected, chunk_errors,
            )
            if chunk_errors:
                errors.append(f"year {yr}: {chunk_errors} chunk(s) failed")

    duration = time.monotonic() - started
    last_error = errors[-1] if errors else None
    await _write_heartbeat(duration, rows_touched, last_error)

    logger.info(
        "NOAA storm_events load done: files=%d rows_touched=%d errors=%d duration=%.1fs",
        downloaded_files, rows_touched, len(errors), duration,
    )

    return {
        "downloaded_files": downloaded_files,
        "rows_inserted": rows_touched,
        "rows_updated": 0,
        "duration_seconds": round(duration, 2),
        "errors": errors,
    }
