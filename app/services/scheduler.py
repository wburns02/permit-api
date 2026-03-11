"""APScheduler setup for alert execution batches."""

import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from app.models.alert import AlertFrequency
from app.services.alert_engine import run_frequency_batch

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

    scheduler.start()
    logger.info("Alert scheduler started (instant=5min, daily=6am UTC, weekly=Mon 6am UTC)")


def stop_scheduler():
    """Gracefully shut down the scheduler."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Alert scheduler stopped")
