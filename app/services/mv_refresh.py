"""Materialized-view refresh job for the hail-leads pipeline.

`hail_leads` (NOAA storm_events × hot_leads, ~17M rows), `hail_leads_spc`
(SPC storm_reports × hot_leads, ~2.4M rows), and `address_permit_history`
(per-address permit aggregate, ~848K rows) are refreshed nightly. This module
is the only place those refreshes live now — the previous T430 cron is
deprecated.

Each successful refresh writes a row into `cron_heartbeat` so the
`/v1/hail-leads/health` endpoint can report freshness without inferring it
from `pg_stat_user_tables`.
"""

import asyncio
import logging
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from app.config import settings
from app.database import primary_session_maker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dedicated maintenance engine for long-running MV REFRESHes.
#
# The shared primary engine pins asyncpg `command_timeout=10` (see
# app/database.py) — a CLIENT-side cap that kills any statement after 10s.
# That's correct for request paths, but fatal for `REFRESH MATERIALIZED VIEW`
# on `hail_leads` (~17M rows, 40+ min) or any CONCURRENTLY refresh. Clearing
# the Postgres `statement_timeout` (as the old code did) does nothing about
# asyncpg's command_timeout — so every nightly refresh died at ~10s with a
# bare `TimeoutError`, the heartbeat recorded the failure, and the MVs silently
# froze (hail_leads stuck at storm_date 2026-02-20 for months).
#
# This engine disables BOTH the client command_timeout and the server-side
# statement/lock/idle timeouts, and uses NullPool so a multi-hour refresh never
# occupies a pooled request connection.
# ---------------------------------------------------------------------------
_maint_session_maker: async_sessionmaker[AsyncSession] | None = None


def _maintenance_session_maker() -> async_sessionmaker[AsyncSession]:
    global _maint_session_maker
    if _maint_session_maker is None:
        engine = create_async_engine(
            settings.DATABASE_URL,
            poolclass=NullPool,
            connect_args={
                "timeout": 10,            # connect timeout — fail fast if tunnel dead
                "command_timeout": None,  # NO client-side cap; REFRESH runs 40+ min
                "server_settings": {
                    "statement_timeout": "0",
                    "lock_timeout": "0",
                    "idle_in_transaction_session_timeout": "0",
                },
            },
        )
        _maint_session_maker = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
    return _maint_session_maker


_MVS = (
    ("mv_refresh_hail_leads", "hail_leads"),
    ("mv_refresh_hail_leads_spc", "hail_leads_spc"),
    ("mv_refresh_address_permit_history", "address_permit_history"),
    # Derives from hail_leads_unified (= hail_leads + hail_leads_spc), so it
    # MUST refresh last — after the base MVs above are current. Has a unique
    # index on lead_id, so REFRESH ... CONCURRENTLY works.
    ("mv_refresh_hail_leads_list", "hail_leads_list"),
    # Parcel-level un-serviced storm leads: storms (last 18 months) ×
    # <county>_parcel_geometries × tx_cad_parcels, minus parcels that pulled a
    # roof permit post-storm. TX hail arms: Tarrant (TAD) + Dallas (DCAD) +
    # Hays (HaysCAD) + Comal (CCAD) + Bexar (BCAD) + Travis (TCAD) +
    # Harris (HCAD) + Smith (SMITHCAD, Tyler/Lindale), SPC hail driven. Plus the
    # EAST BATON ROUGE + Ascension, LA arms: NOAA storm_events WIND/tropical (+
    # hail secondary) × <parish>_parcel_geometries, minus parcels with a Re-Roof
    # permit issued after the storm. Refresh runs
    # on the dedicated maintenance engine (no client/server timeout cap), so the
    # multi-county full/CONCURRENTLY refresh has no statement-timeout ceiling.
    # Has a unique index on (parcel_id, county_source), so REFRESH ...
    # CONCURRENTLY works.
    ("mv_refresh_unserviced_hail_leads", "unserviced_hail_leads"),
    # Brazoria TX permit-lead feed (Phase 3). Deduplicated/classified/geocoded
    # one-row-per-address lead view over the Brazoria hot_leads sources. Reads
    # geocoded_addresses (populated out-of-band by
    # scripts/geocode_brazoria_leads.py), so ideally run AFTER that cron. Has a
    # unique index on address_norm, so REFRESH ... CONCURRENTLY works.
    ("mv_refresh_brazoria_permit_leads", "brazoria_permit_leads"),
)


async def _refresh_one(cron_name: str, relname: str) -> None:
    """Refresh a single MV and write a heartbeat row.

    Tries CONCURRENTLY first (zero downtime, requires a UNIQUE index on the
    MV); falls back to a plain REFRESH on failure. Both paths record the
    result in cron_heartbeat — including failures, with last_error set.

    Runs on the dedicated maintenance engine (`_maintenance_session_maker`),
    which has the asyncpg client `command_timeout` AND the server-side
    statement/lock/idle timeouts disabled — full MV REFRESH on `hail_leads`
    takes 40+ min and REFRESH CONCURRENTLY can take hours. The old code only
    cleared the Postgres `statement_timeout`, which did nothing about asyncpg's
    10s command_timeout, so every refresh died at ~10s and the MV silently
    rotted.
    """
    started = time.monotonic()
    last_error: str | None = None
    used_concurrent = True

    async with _maintenance_session_maker()() as db:
        try:
            await db.execute(
                text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {relname}")
            )
            await db.commit()
        except Exception as exc:  # noqa: BLE001
            await db.rollback()
            used_concurrent = False
            logger.warning(
                "MV %s: CONCURRENTLY refresh failed (%s) — falling back to plain REFRESH",
                relname,
                exc,
            )
            try:
                await db.execute(text(f"REFRESH MATERIALIZED VIEW {relname}"))
                await db.commit()
            except Exception as exc2:  # noqa: BLE001
                await db.rollback()
                last_error = f"{type(exc2).__name__}: {exc2}"
                logger.exception("MV %s: plain REFRESH also failed", relname)

        # ANALYZE after a successful REFRESH so pg_stat_user_tables.last_analyze
        # advances — the /health endpoint's mv freshness KPI reads from there.
        if last_error is None:
            try:
                await db.execute(text(f"ANALYZE {relname}"))
                await db.commit()
            except Exception as exc:  # noqa: BLE001
                await db.rollback()
                logger.warning("ANALYZE %s failed (non-fatal): %s", relname, exc)

        duration = round(time.monotonic() - started, 2)

        row_count: int | None = None
        if last_error is None:
            try:
                rc = await db.execute(
                    text(
                        "SELECT GREATEST(reltuples, 0)::bigint "
                        "FROM pg_class WHERE relname = :n"
                    ),
                    {"n": relname},
                )
                row_count = int(rc.scalar() or 0)
            except Exception:  # noqa: BLE001
                pass

        try:
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
                    "name": cron_name,
                    "dur": duration,
                    "rows": row_count,
                    "err": last_error,
                },
            )
            await db.commit()
        except Exception:  # noqa: BLE001
            await db.rollback()
            logger.exception("Failed to write heartbeat for %s", cron_name)

    if last_error:
        logger.error(
            "MV %s refresh FAILED in %.2fs: %s", relname, duration, last_error
        )
    else:
        mode = "CONCURRENTLY" if used_concurrent else "plain"
        logger.info(
            "MV %s refreshed (%s) in %.2fs (rows=%s)",
            relname,
            mode,
            duration,
            row_count,
        )


async def refresh_hail_leads_mvs() -> None:
    """Refresh both hail-leads MVs sequentially. Failures are isolated."""
    for cron_name, relname in _MVS:
        try:
            await _refresh_one(cron_name, relname)
        except Exception:  # noqa: BLE001
            logger.exception("MV refresh job: %s raised", relname)


async def _is_stale(name: str, threshold_hours: float) -> bool:
    """Return True if the named cron has not beat within `threshold_hours`.

    On any error (table missing, DB down) returns True so we err toward
    refreshing rather than silently skipping forever.
    """
    try:
        async with primary_session_maker() as db:
            row = await db.execute(
                text(
                    """
                    SELECT beat_at
                      FROM cron_heartbeat
                     WHERE name = :n
                       AND beat_at > NOW() - (:hours || ' hours')::interval
                     LIMIT 1
                    """
                ),
                {"n": name, "hours": str(threshold_hours)},
            )
            return row.scalar() is None
    except Exception as exc:  # noqa: BLE001
        logger.warning("MV stale-check failed (%s): %s — assuming stale", name, exc)
        return True


async def refresh_if_stale(threshold_hours: float = 6.0) -> None:
    """Refresh both MVs only if their last heartbeat is older than threshold.

    Used at app startup so a restart loop doesn't trigger redundant refreshes
    of 17M-row materialized views. The scheduled daily job calls
    `refresh_hail_leads_mvs()` directly without the gate.
    """
    needed = []
    for cron_name, relname in _MVS:
        if await _is_stale(cron_name, threshold_hours):
            needed.append((cron_name, relname))

    if not needed:
        logger.info(
            "MV boot-refresh skipped — both heartbeats fresh within %sh; "
            "running ANALYZE only so pg_stat freshness still advances",
            threshold_hours,
        )
        for _, relname in _MVS:
            try:
                async with _maintenance_session_maker()() as db:
                    await db.execute(text(f"ANALYZE {relname}"))
                    await db.commit()
                logger.info("ANALYZE %s ok", relname)
            except Exception as exc:  # noqa: BLE001
                logger.warning("ANALYZE %s failed: %s", relname, exc)
        return

    logger.info(
        "MV boot-refresh kicking off for %d MV(s): %s",
        len(needed),
        [n for n, _ in needed],
    )
    for cron_name, relname in needed:
        try:
            await _refresh_one(cron_name, relname)
        except Exception:  # noqa: BLE001
            logger.exception("Boot MV refresh: %s raised", relname)


async def refresh_unpopulated() -> None:
    """Populate any MV in `_MVS` that is currently UNPOPULATED.

    Every MV in `_MVS` is created `WITH NO DATA` by the startup migration in
    `app/main.py`, and several self-heal by DROP+CREATE (also `WITH NO DATA`)
    when their definition drifts. After such a (re)build the MV is empty until
    the nightly `refresh_hail_leads_mvs` cron runs at 04:25 UTC — so endpoints
    like `/v1/permit-leads` (brazoria_permit_leads) and the live hail product
    (`unserviced_hail_leads`) serve EMPTY for up to a day after every deploy.

    This runs right after the startup migrations and force-refreshes only the
    MVs that pg reports as unpopulated. It is deliberately NOT gated on
    `cron_heartbeat` staleness: a self-heal rebuild leaves the heartbeat row
    fresh while the MV is empty, so the staleness gate would wrongly skip it.
    `_refresh_one` updates the heartbeat after each populate. Empty MVs whose
    base tables aren't loaded yet (fresh DB) refresh to zero rows cheaply.

    Detection uses `pg_matviews.ispopulated` — the authoritative "has this MV
    ever been refreshed since its last (re)build" flag — so a populated MV is
    never needlessly re-walked on a steady-state redeploy.
    """
    try:
        async with primary_session_maker() as db:
            rows = await db.execute(
                text(
                    "SELECT matviewname FROM pg_matviews "
                    "WHERE schemaname = 'public' "
                    "  AND matviewname = ANY(:names) "
                    "  AND ispopulated = false"
                ),
                {"names": [relname for _, relname in _MVS]},
            )
            unpopulated = {r[0] for r in rows.fetchall()}
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "MV populate-check failed (%s) — skipping startup populate", exc
        )
        return

    if not unpopulated:
        logger.info("MV startup-populate: all MVs already populated")
        return

    # Preserve _MVS order so dependency-ordered MVs (e.g. hail_leads_list,
    # which derives from the base MVs) refresh after their inputs.
    to_refresh = [(c, r) for c, r in _MVS if r in unpopulated]
    logger.info(
        "MV startup-populate: %d unpopulated MV(s): %s",
        len(to_refresh),
        [r for _, r in to_refresh],
    )
    for cron_name, relname in to_refresh:
        try:
            await _refresh_one(cron_name, relname)
        except Exception:  # noqa: BLE001
            logger.exception("Startup MV populate: %s raised", relname)


async def refresh_in_background() -> None:
    """Wrapper used at app startup so REFRESH never blocks /health.

    Gated on staleness — see `refresh_if_stale`.
    """
    try:
        await refresh_if_stale()
    except Exception:  # noqa: BLE001
        logger.exception("Background MV refresh failed")


def schedule_in_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Kick off a one-shot background refresh on the running loop."""
    loop.create_task(refresh_in_background())
