"""Alert execution engine — matches alerts against new permits and delivers notifications."""

import logging
from datetime import datetime, timezone
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import primary_session_maker as async_session_maker
from app.models.alert import PermitAlert, AlertFrequency
from app.models.alert_history import AlertExecutionHistory
from app.models.api_key import ApiUser
from app.services.search_service import build_filter_conditions, PERMIT_COLUMNS, row_to_dict
from app.services.email_service import send_alert_email
from app.services.webhook_service import deliver_webhook
from app.models.permit import Permit

logger = logging.getLogger(__name__)

MATCH_LIMIT = 100


async def match_alert(alert: PermitAlert, db: AsyncSession) -> list[dict]:
    """Find new permits matching alert filters since last check."""
    conditions = build_filter_conditions(alert.filters)

    # Temporal: only new permits since last check
    if alert.last_checked_at:
        conditions.append(Permit.issue_date > alert.last_checked_at.date())

    if not conditions:
        return []

    query = (
        select(*PERMIT_COLUMNS)
        .where(and_(*conditions))
        .order_by(Permit.issue_date.desc().nullslast())
        .limit(MATCH_LIMIT)
    )

    result = await db.execute(query)
    return [row_to_dict(r) for r in result.all()]


async def execute_alert(alert: PermitAlert, db: AsyncSession) -> int:
    """Execute a single alert: match, deliver, log history."""
    now = datetime.now(timezone.utc)
    matches = await match_alert(alert, db)

    # Determine delivery method
    methods = []
    if alert.email_notify:
        methods.append("email")
    if alert.webhook_url:
        methods.append("webhook")
    delivery_method = "+".join(methods) if methods else "none"

    if not matches:
        # Update last_checked_at even with no matches
        alert.last_checked_at = now
        alert.consecutive_failures = 0
        await db.commit()
        return 0

    # Deliver
    email_ok = True
    webhook_ok = True
    errors = []

    if alert.email_notify:
        # Load user email
        user = await db.get(ApiUser, alert.user_id)
        if user:
            email_ok = await send_alert_email(user.email, alert.name, matches)
            if not email_ok:
                errors.append("email delivery failed")
        else:
            email_ok = False
            errors.append("user not found")

    if alert.webhook_url:
        payload = {
            "alert_id": str(alert.id),
            "alert_name": alert.name,
            "match_count": len(matches),
            "matches": matches,
            "run_at": now.isoformat(),
        }
        webhook_ok = await deliver_webhook(alert.webhook_url, payload)
        if not webhook_ok:
            errors.append("webhook delivery failed")

    # Determine status
    all_ok = email_ok and webhook_ok
    if all_ok:
        status = "success"
    elif email_ok or webhook_ok:
        status = "partial"
    else:
        status = "failed"

    # Log history
    history = AlertExecutionHistory(
        alert_id=alert.id,
        run_at=now,
        match_count=len(matches),
        delivery_method=delivery_method,
        delivery_status=status,
        error="; ".join(errors) if errors else None,
        matches_sample=matches[:5],
    )
    db.add(history)

    # Update alert stats
    alert.last_checked_at = now
    alert.last_match_count = len(matches)
    alert.total_matches = (alert.total_matches or 0) + len(matches)
    if all_ok:
        alert.consecutive_failures = 0
        alert.last_error = None
    else:
        alert.consecutive_failures = (alert.consecutive_failures or 0) + 1
        alert.last_error = "; ".join(errors)

    await db.commit()
    logger.info("Alert '%s' executed: %d matches, status=%s", alert.name, len(matches), status)
    return len(matches)


async def run_frequency_batch(frequency: AlertFrequency):
    """Run all active alerts of a given frequency."""
    logger.info("Starting %s alert batch", frequency.value)
    async with async_session_maker() as db:
        result = await db.execute(
            select(PermitAlert).where(
                PermitAlert.is_active.is_(True),
                PermitAlert.frequency == frequency,
                PermitAlert.consecutive_failures < 10,  # Skip repeatedly failing alerts
            )
        )
        alerts = result.scalars().all()
        logger.info("Found %d active %s alerts", len(alerts), frequency.value)

        for alert in alerts:
            try:
                await execute_alert(alert, db)
            except Exception as e:
                logger.error("Alert %s failed: %s", alert.id, e)
                alert.consecutive_failures = (alert.consecutive_failures or 0) + 1
                alert.last_error = str(e)
                await db.commit()

    logger.info("Completed %s alert batch", frequency.value)
