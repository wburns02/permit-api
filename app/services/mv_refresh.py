"""Materialized-view refresh job for the hail-leads pipeline.

`hail_leads` (storm × permit join, ~17M rows) and `address_permit_history`
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

from app.database import primary_session_maker

logger = logging.getLogger(__name__)


_MVS = (
    ("mv_refresh_hail_leads", "hail_leads"),
    ("mv_refresh_address_permit_history", "address_permit_history"),
)


async def _refresh_one(cron_name: str, relname: str) -> None:
    """Refresh a single MV and write a heartbeat row.

    Tries CONCURRENTLY first (zero downtime, requires a UNIQUE index on the
    MV); falls back to a plain REFRESH on failure. Both paths record the
    result in cron_heartbeat — including failures, with last_error set.
    """
    started = time.monotonic()
    last_error: str | None = None
    used_concurrent = True

    async with primary_session_maker() as db:
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
                async with primary_session_maker() as db:
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
