"""APScheduler setup for alert execution batches."""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from app.models.alert import AlertFrequency
from app.services.alert_engine import run_frequency_batch
from app.services.mv_refresh import refresh_hail_leads_mvs
from app.services.noaa_loader import load_noaa_storm_events

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


def start_scheduler():
    """Configure and start the alert scheduler."""
    # INSTANT alerts: every 5 minutes
    scheduler.add_job(
        run_frequency_batch,
        trigger=IntervalTrigger(minutes=5),
        args=[AlertFrequency.INSTANT],
        id="alert_instant",
        name="Instant alert batch",
        replace_existing=True,
    )

    # DAILY alerts: 6:00 AM UTC
    scheduler.add_job(
        run_frequency_batch,
        trigger=CronTrigger(hour=6, minute=0),
        args=[AlertFrequency.DAILY],
        id="alert_daily",
        name="Daily alert batch",
        replace_existing=True,
    )

    # WEEKLY alerts: Monday 6:00 AM UTC
    scheduler.add_job(
        run_frequency_batch,
        trigger=CronTrigger(day_of_week="mon", hour=6, minute=0),
        args=[AlertFrequency.WEEKLY],
        id="alert_weekly",
        name="Weekly alert batch",
        replace_existing=True,
    )

    # Hail-leads MV refresh: 04:25 UTC daily (matches retired T430 cron slot).
    # Three MVs refreshed sequentially in one job (hail_leads, hail_leads_spc,
    # address_permit_history); each refresh writes its own cron_heartbeat row.
    scheduler.add_job(
        refresh_hail_leads_mvs,
        trigger=CronTrigger(hour=4, minute=25),
        id="hail_leads_mv_refresh",
        name="Hail-leads MV refresh",
        replace_existing=True,
    )

    # NOAA storm_events daily load: 05:00 UTC every day. Pulls the
    # current calendar year's CSV from NCEI, filters to TEXAS, and
    # upserts. Idempotent (ON CONFLICT (event_id) DO UPDATE).
    #
    # Bumped from Mon-only to daily 2026-05-12 so a single missed run
    # doesn't cost a full week of storm coverage in the hail_leads MV.
    # The full TX year is ~5K rows, the upsert is cheap.
    scheduler.add_job(
        load_noaa_storm_events,
        trigger=CronTrigger(hour=5, minute=0),
        id="noaa_storm_events_load",
        name="NOAA Storm Events daily load",
        replace_existing=True,
    )

    scheduler.start()
    logger.info(
        "Scheduler started (alerts: instant=5min, daily=6am UTC, weekly=Mon 6am UTC; "
        "hail-leads MV refresh=4:25am UTC; NOAA storm_events load=daily 5:00am UTC)"
    )


def stop_scheduler():
    """Gracefully shut down the scheduler."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Alert scheduler stopped")
