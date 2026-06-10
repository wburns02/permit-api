"""Alert execution engine — matches alerts against new permits and delivers notifications."""

import asyncio
import logging
from datetime import datetime, timezone
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import primary_session_maker as async_session_maker
from app.models.alert import PermitAlert, AlertFrequency
from app.models.alert_history import AlertExecutionHistory
from app.models.api_key import ApiUser
from app.services.search_service import build_filter_conditions, PERMIT_COLUMNS, row_to_dict
from app.services.email_service import send_alert_email
from app.services.webhook_service import deliver_webhook
from app.models.permit import Permit
from app.models.oil_gas import WellPermit

logger = logging.getLogger(__name__)

MATCH_LIMIT = 100

# Allowed filter keys for W-1 drilling permit watchlists. Adding a key here
# (and a condition below) is the whole change needed to watch a new field —
# criteria live in JSONB, so no schema migration.
W1_FILTER_KEYS = {
    "state", "county", "operator", "lease", "district",
    "wellbore_profile", "filing_purpose", "min_depth",
}


def build_w1_conditions(filters: dict) -> list:
    """SQLAlchemy conditions for a W-1 drilling permit watchlist."""
    conditions = [
        WellPermit.state == (filters.get("state") or "TX").upper(),
        WellPermit.current_status == "approved",
    ]
    if filters.get("county"):
        conditions.append(func.upper(WellPermit.county) == filters["county"].upper())
    if filters.get("operator"):
        conditions.append(WellPermit.operator_name_raw.ilike(f"%{filters['operator']}%"))
    if filters.get("lease"):
        conditions.append(WellPermit.lease_name.ilike(f"%{filters['lease']}%"))
    if filters.get("district"):
        conditions.append(WellPermit.district == str(filters["district"]).upper())
    if filters.get("wellbore_profile"):
        conditions.append(WellPermit.wellbore_profile == filters["wellbore_profile"].lower())
    if filters.get("filing_purpose"):
        conditions.append(WellPermit.filing_purpose.ilike(f"%{filters['filing_purpose']}%"))
    if filters.get("min_depth"):
        conditions.append(WellPermit.total_depth >= float(filters["min_depth"]))
    return conditions


def w1_row_to_dict(p: WellPermit) -> dict:
    return {
        "permit_number": p.permit_number,
        "county": p.county,
        "district": p.district,
        "operator": p.operator_name_raw,
        "operator_number": p.operator_number,
        "lease_name": p.lease_name,
        "well_number": p.well_number,
        "wellbore_profile": p.wellbore_profile,
        "filing_purpose": p.filing_purpose,
        "total_depth": float(p.total_depth) if p.total_depth is not None else None,
        "submitted_date": p.submitted_date.isoformat() if p.submitted_date else None,
        "approved_date": p.approved_date.isoformat() if p.approved_date else None,
        "lat": p.lat,
        "lng": p.lng,
    }


async def match_alert(alert: PermitAlert, db: AsyncSession) -> list[dict]:
    """Find new records matching alert filters since last check."""
    source_type = getattr(alert, "source_type", None) or "permits"

    if source_type == "well_permits":
        conditions = build_w1_conditions(alert.filters or {})
        # Temporal: only newly approved W-1s since last check. No backfill:
        # the first run after activation sets the cursor without matching.
        if alert.last_checked_at:
            conditions.append(WellPermit.approved_date > alert.last_checked_at.date())
        else:
            return []
        query = (
            select(WellPermit)
            .where(and_(*conditions))
            .order_by(WellPermit.county.asc().nullslast(),
                      WellPermit.approved_date.desc().nullslast())
            .limit(MATCH_LIMIT)
        )
        result = await db.execute(query)
        return [w1_row_to_dict(p) for p in result.scalars().all()]

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
            email_ok = await send_alert_email(
                user.email, alert.name, matches,
                source_type=getattr(alert, "source_type", None) or "permits",
            )
            if not email_ok:
                errors.append("email delivery failed")
        else:
            email_ok = False
            errors.append("user not found")

    if alert.webhook_url:
        payload = {
            "alert_id": str(alert.id),
            "alert_name": alert.name,
            "source_type": getattr(alert, "source_type", None) or "permits",
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

    # 1. Fetch alert IDs in one short-lived session.
    async with async_session_maker() as db:
        result = await db.execute(
            select(PermitAlert.id).where(
                PermitAlert.is_active.is_(True),
                PermitAlert.frequency == frequency,
                PermitAlert.consecutive_failures < 10,
            )
        )
        alert_ids = [row[0] for row in result.all()]

    logger.info("Found %d active %s alerts", len(alert_ids), frequency.value)

    # 2. Process each alert in its own session with a 30s wall-clock cap.
    for alert_id in alert_ids:
        try:
            async with async_session_maker() as db:
                alert = await db.get(PermitAlert, alert_id)
                if alert is None:
                    continue
                try:
                    await asyncio.wait_for(execute_alert(alert, db), timeout=30.0)
                except asyncio.TimeoutError:
                    await db.rollback()
                    logger.error("Alert %s timed out after 30s", alert_id)
                    alert = await db.get(PermitAlert, alert_id)  # re-fetch after rollback
                    if alert:
                        alert.consecutive_failures = (alert.consecutive_failures or 0) + 1
                        alert.last_error = "execute_alert timed out (30s)"
                        await db.commit()
                except Exception as e:
                    await db.rollback()
                    logger.error("Alert %s failed: %s", alert_id, e)
                    alert = await db.get(PermitAlert, alert_id)
                    if alert:
                        alert.consecutive_failures = (alert.consecutive_failures or 0) + 1
                        alert.last_error = str(e)
                        await db.commit()
        except Exception as e:
            logger.error("Failed to open session for alert %s: %s", alert_id, e)

    logger.info("Completed %s alert batch", frequency.value)
