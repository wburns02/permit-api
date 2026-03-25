"""Email outreach service — sends campaign batches via SendGrid.

Handles:
- Unsubscribe token generation/verification (HMAC-SHA256)
- Batch sending with rate limiting
- Campaign creation from prospect_contacts table
- SendGrid event processing (opens, clicks, bounces)
"""

import hmac
import hashlib
import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select, update, text, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.email_campaign import EmailCampaign, EmailRecipient, EmailUnsubscribe

logger = logging.getLogger(__name__)

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import (
        Mail, Content, TrackingSettings, OpenTracking, ClickTracking,
    )
    HAS_SENDGRID = True
except ImportError:
    HAS_SENDGRID = False
    logger.warning("sendgrid not installed — email sending will be disabled")


# ── Unsubscribe Token ─────────────────────────────────────────────────────────

def generate_unsubscribe_token(email: str, secret_key: str | None = None) -> str:
    """Generate an HMAC token for unsubscribe link verification."""
    key = (secret_key or settings.SECRET_KEY).encode()
    return hmac.new(key, email.lower().strip().encode(), hashlib.sha256).hexdigest()[:16]


def verify_unsubscribe_token(email: str, token: str, secret_key: str | None = None) -> bool:
    """Verify an unsubscribe token is valid for the given email."""
    expected = generate_unsubscribe_token(email, secret_key)
    return hmac.compare_digest(expected, token)


# ── Template Selection ────────────────────────────────────────────────────────

def _get_template_fn(template_key: str, audience: str):
    """Get the template function based on key or audience fallback."""
    from app.services.email_templates import TEMPLATES
    if template_key in TEMPLATES:
        return TEMPLATES[template_key]
    # Fallback: map audience to template
    audience_map = {
        "insurance": "insurance",
        "realtor": "realtor",
        "real_estate": "realtor",
        "contractor": "contractor",
        "all": "contractor",
    }
    key = audience_map.get(audience.lower(), "contractor")
    return TEMPLATES.get(key)


# ── Batch Sending ─────────────────────────────────────────────────────────────

async def send_campaign_batch(
    campaign_id: str | uuid.UUID,
    batch_size: int,
    db: AsyncSession,
    dry_run: bool = False,
) -> dict:
    """Send next batch of pending emails for a campaign.

    Returns dict with keys: sent, skipped, errors, done
    """
    # 1. Get campaign
    result = await db.execute(
        select(EmailCampaign).where(EmailCampaign.id == campaign_id)
    )
    campaign = result.scalar_one_or_none()
    if not campaign:
        return {"sent": 0, "skipped": 0, "errors": 1, "done": True, "error": "Campaign not found"}
    if campaign.status != "active":
        return {"sent": 0, "skipped": 0, "errors": 0, "done": True, "error": f"Campaign status is {campaign.status}"}

    # 2. Get unsubscribed emails
    unsub_result = await db.execute(select(EmailUnsubscribe.email))
    unsub_emails = {row[0].lower() for row in unsub_result.all()}

    # 3. Get pending recipients not in unsubscribe list
    recip_result = await db.execute(
        select(EmailRecipient)
        .where(
            EmailRecipient.campaign_id == campaign_id,
            EmailRecipient.status == "pending",
        )
        .limit(batch_size)
    )
    recipients = recip_result.scalars().all()

    if not recipients:
        # No more pending — mark complete
        campaign.status = "completed"
        campaign.completed_at = datetime.now(timezone.utc)
        await db.commit()
        return {"sent": 0, "skipped": 0, "errors": 0, "done": True}

    # Get template function
    template_fn = _get_template_fn(campaign.target_audience or "contractor", campaign.target_audience or "contractor")
    if not template_fn:
        return {"sent": 0, "skipped": 0, "errors": 1, "done": False, "error": "No template found"}

    sent = 0
    skipped = 0
    errors = 0
    now = datetime.now(timezone.utc)

    sg = None
    if not dry_run and HAS_SENDGRID and settings.SENDGRID_API_KEY:
        sg = SendGridAPIClient(settings.SENDGRID_API_KEY)

    for recip in recipients:
        # Check unsubscribe
        if recip.email.lower() in unsub_emails:
            recip.status = "unsubscribed"
            recip.unsubscribed_at = now
            skipped += 1
            continue

        # Generate personalized email
        token = generate_unsubscribe_token(recip.email)
        try:
            from app.services.email_templates import contractor_pitch, followup_pitch
            # Use appropriate template with right args
            if campaign.target_audience == "contractor":
                trade = recip.license_type or "construction"
                subject, html, text_body = template_fn(
                    recip.name or "", recip.state or "", recip.email, token, trade
                )
            elif campaign.target_audience in ("followup",):
                subject, html, text_body = template_fn(
                    recip.name or "", recip.email, token, recip.state or ""
                )
            else:
                subject, html, text_body = template_fn(
                    recip.name or "", recip.state or "", recip.email, token
                )
        except Exception as e:
            logger.error("Template error for %s: %s", recip.email, e)
            errors += 1
            continue

        if dry_run:
            recip.status = "sent"
            recip.sent_at = now
            sent += 1
            continue

        if not sg:
            logger.warning("SendGrid not configured, skipping send to %s", recip.email)
            skipped += 1
            continue

        # Send via SendGrid
        try:
            message = Mail(
                from_email="Will at PermitLookup <outreach@permitlookup.com>",
                to_emails=recip.email,
                subject=subject,
            )
            message.content = [
                Content("text/plain", text_body),
                Content("text/html", html),
            ]

            # Enable tracking
            tracking = TrackingSettings()
            tracking.open_tracking = OpenTracking(True)
            tracking.click_tracking = ClickTracking(True, True)
            message.tracking_settings = tracking

            # Add custom headers for webhook correlation
            message.header = {
                "X-Campaign-Id": str(campaign.id),
                "X-Recipient-Id": str(recip.id),
            }

            response = sg.send(message)
            if response.status_code in (200, 201, 202):
                recip.status = "sent"
                recip.sent_at = now
                sent += 1
            else:
                logger.warning("SendGrid returned %s for %s", response.status_code, recip.email)
                errors += 1
        except Exception as e:
            logger.error("Failed to send to %s: %s", recip.email, e)
            errors += 1

    # Update campaign sent_count
    campaign.sent_count = (campaign.sent_count or 0) + sent
    await db.commit()

    # Check if campaign is now complete
    remaining = await db.execute(
        select(func.count(EmailRecipient.id))
        .where(
            EmailRecipient.campaign_id == campaign_id,
            EmailRecipient.status == "pending",
        )
    )
    pending_count = remaining.scalar() or 0
    done = pending_count == 0

    if done:
        campaign.status = "completed"
        campaign.completed_at = datetime.now(timezone.utc)
        await db.commit()

    return {"sent": sent, "skipped": skipped, "errors": errors, "done": done, "pending": pending_count}


# ── Campaign Creation ─────────────────────────────────────────────────────────

async def create_campaign_from_prospects(
    db: AsyncSession,
    name: str,
    audience: str,
    state: str | None,
    template_key: str,
    send_rate: int = 200,
) -> tuple[uuid.UUID, int]:
    """Create a campaign and populate recipients from prospect_contacts table.

    Returns (campaign_id, recipient_count).
    """
    # Build the audience filter for license_type
    audience_filters = {
        "insurance": ["insurance", "adjuster", "claims", "underwriter", "actuary"],
        "realtor": ["real estate", "realtor", "broker", "appraiser", "inspector"],
        "contractor": [
            "contractor", "electrician", "plumber", "plumbing", "hvac",
            "roofing", "roofer", "general", "builder", "mechanical",
            "electrical", "construction",
        ],
        "all": [],  # No filter — everyone
    }

    # Generate subject/template preview
    template_fn = _get_template_fn(template_key, audience)
    if not template_fn:
        raise ValueError(f"Unknown template: {template_key}")

    # Create a preview to store subject
    preview_token = generate_unsubscribe_token("preview@example.com")
    try:
        if template_key == "contractor":
            subj, html_preview, text_preview = template_fn("Preview", state or "TX", "preview@example.com", preview_token, "construction")
        elif template_key == "followup":
            subj, html_preview, text_preview = template_fn("Preview", "preview@example.com", preview_token, state or "")
        else:
            subj, html_preview, text_preview = template_fn("Preview", state or "TX", "preview@example.com", preview_token)
    except Exception:
        subj = f"PermitLookup — {name}"
        html_preview = ""
        text_preview = ""

    # 1. Create campaign record
    campaign = EmailCampaign(
        id=uuid.uuid4(),
        name=name,
        subject=subj,
        body_html=html_preview,
        body_text=text_preview,
        target_audience=audience,
        target_state=state,
        status="draft",
        send_rate=send_rate,
    )
    db.add(campaign)
    await db.flush()

    # 2. Query prospect_contacts for matching recipients
    like_filters = audience_filters.get(audience.lower(), [])

    # Build WHERE clause
    where_parts = [
        "email IS NOT NULL",
        "email != ''",
        "email NOT IN (SELECT email FROM email_unsubscribes)",
    ]
    params: dict = {}

    if state:
        where_parts.append("state = :state")
        params["state"] = state.upper()

    if like_filters:
        like_clauses = " OR ".join(
            f"LOWER(license_type) LIKE :lt{i}" for i in range(len(like_filters))
        )
        where_parts.append(f"({like_clauses})")
        for i, lt in enumerate(like_filters):
            params[f"lt{i}"] = f"%{lt}%"

    where_sql = " AND ".join(where_parts)

    query = text(f"""
        SELECT DISTINCT ON (LOWER(email))
            email, name, company, state, license_type
        FROM prospect_contacts
        WHERE {where_sql}
        ORDER BY LOWER(email), name
    """)

    result = await db.execute(query, params)
    rows = result.all()

    # 3. Bulk insert recipients
    count = 0
    batch = []
    for row in rows:
        batch.append(EmailRecipient(
            id=uuid.uuid4(),
            campaign_id=campaign.id,
            email=row[0].strip(),
            name=row[1],
            company=row[2],
            state=row[3],
            license_type=row[4],
            status="pending",
        ))
        count += 1

        if len(batch) >= 5000:
            db.add_all(batch)
            await db.flush()
            batch = []

    if batch:
        db.add_all(batch)
        await db.flush()

    await db.commit()
    return campaign.id, count


# ── SendGrid Event Processing ─────────────────────────────────────────────────

async def process_sendgrid_events(events: list[dict], db: AsyncSession) -> dict:
    """Process SendGrid event webhook payload.

    Events: open, click, bounce, dropped, spamreport, unsubscribe
    Returns processing stats.
    """
    processed = 0
    errors = 0
    now = datetime.now(timezone.utc)

    for event in events:
        try:
            email = event.get("email", "").lower().strip()
            event_type = event.get("event", "")

            if not email or not event_type:
                continue

            # Find the most recent recipient with this email
            result = await db.execute(
                select(EmailRecipient)
                .where(EmailRecipient.email == email)
                .order_by(EmailRecipient.sent_at.desc())
                .limit(1)
            )
            recip = result.scalar_one_or_none()
            if not recip:
                continue

            # Get the campaign
            camp_result = await db.execute(
                select(EmailCampaign).where(EmailCampaign.id == recip.campaign_id)
            )
            campaign = camp_result.scalar_one_or_none()

            if event_type == "open":
                if recip.status in ("sent", "pending"):
                    recip.status = "opened"
                    recip.opened_at = now
                    if campaign:
                        campaign.open_count = (campaign.open_count or 0) + 1

            elif event_type == "click":
                recip.status = "clicked"
                recip.clicked_at = now
                if campaign:
                    campaign.click_count = (campaign.click_count or 0) + 1

            elif event_type in ("bounce", "dropped"):
                recip.status = "bounced"
                if campaign:
                    campaign.bounce_count = (campaign.bounce_count or 0) + 1

            elif event_type in ("spamreport", "unsubscribe"):
                recip.status = "unsubscribed"
                recip.unsubscribed_at = now
                if campaign:
                    campaign.unsubscribe_count = (campaign.unsubscribe_count or 0) + 1
                # Also add to global unsubscribe list
                existing = await db.execute(
                    select(EmailUnsubscribe).where(EmailUnsubscribe.email == email)
                )
                if not existing.scalar_one_or_none():
                    db.add(EmailUnsubscribe(
                        email=email,
                        reason=f"sendgrid_{event_type}",
                    ))

            processed += 1
        except Exception as e:
            logger.error("Error processing event: %s", e)
            errors += 1

    await db.commit()
    return {"processed": processed, "errors": errors}
