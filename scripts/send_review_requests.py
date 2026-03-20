#!/usr/bin/env python3
"""
Review Request Sender — emails customers 7 days after a deal is won.

Finds deals in "won" stage that closed 7+ days ago, where:
  - review_requested_at is NULL
  - contact has an email address

Sends a friendly review request email and stamps review_requested_at.

Usage:
    python3 send_review_requests.py --db-host 100.122.216.15
    python3 send_review_requests.py --db-host 100.122.216.15 --dry-run

Cron (daily 10 AM):
    0 10 * * * cd /home/will/permit-api && source backend_venv/bin/activate && python3 scripts/send_review_requests.py --db-host 100.122.216.15 >> /tmp/review_requests.log 2>&1
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("review_requests")


def _build_review_email_html(contact_name: str, company_name: str, frontend_url: str) -> str:
    """Build the review request email HTML."""
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#0a0a0f;font-family:-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a0f;padding:20px 0">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%">

<tr><td style="padding:32px;background:#12121a;border:1px solid #2a2a3a;border-radius:12px">
  <div style="text-align:center;margin-bottom:24px">
    <div style="width:48px;height:48px;background:linear-gradient(135deg,#6366f1,#a855f7);border-radius:12px;display:inline-block;text-align:center;line-height:48px;font-size:24px;color:#fff;font-weight:700">P</div>
  </div>

  <h2 style="text-align:center;color:#e8e8f0;font-size:22px;margin:0 0 16px">How did we do?</h2>

  <p style="color:#a0a0b8;font-size:15px;line-height:1.6;text-align:center;margin:0 0 8px">
    Hi {contact_name},
  </p>
  <p style="color:#a0a0b8;font-size:15px;line-height:1.6;text-align:center;margin:0 0 24px">
    Thank you for choosing <strong style="color:#e8e8f0">{company_name}</strong>! We hope everything went smoothly.
    We'd really appreciate your feedback — it helps us improve and helps others find great service.
  </p>

  <div style="text-align:center;margin-bottom:24px">
    <div style="display:inline-flex;gap:12px;font-size:36px">
      <span style="cursor:pointer" title="Great">&#11088;</span>
      <span style="cursor:pointer" title="Great">&#11088;</span>
      <span style="cursor:pointer" title="Great">&#11088;</span>
      <span style="cursor:pointer" title="Great">&#11088;</span>
      <span style="cursor:pointer" title="Great">&#11088;</span>
    </div>
  </div>

  <p style="color:#6a6a80;font-size:13px;line-height:1.5;text-align:center;margin:0 0 16px">
    Simply reply to this email with your thoughts, or leave us a review online.
    Your feedback means the world to us.
  </p>

  <div style="border-top:1px solid #2a2a3a;margin-top:24px;padding-top:16px;text-align:center">
    <div style="color:#6a6a80;font-size:11px">
      {company_name} &mdash; Powered by <a href="{frontend_url}" style="color:#818cf8;text-decoration:none">PermitLookup</a>
    </div>
  </div>
</td></tr>

</table>
</td></tr>
</table>
</body></html>"""


async def main():
    parser = argparse.ArgumentParser(description="Send review request emails")
    parser.add_argument("--db-host", default="100.122.216.15", help="Database host")
    parser.add_argument("--db-port", type=int, default=5432, help="Database port")
    parser.add_argument("--db-name", default="permit_api", help="Database name")
    parser.add_argument("--db-user", default="will", help="Database user")
    parser.add_argument("--db-pass", default="", help="Database password")
    parser.add_argument("--dry-run", action="store_true", help="Print eligible deals without sending")
    parser.add_argument("--days", type=int, default=7, help="Days after won to send request")
    args = parser.parse_args()

    password_part = f":{args.db_pass}" if args.db_pass else ""
    db_url = f"postgresql+asyncpg://{args.db_user}{password_part}@{args.db_host}:{args.db_port}/{args.db_name}"
    os.environ["DATABASE_URL"] = db_url

    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
    from sqlalchemy import select, text

    engine = create_async_engine(db_url, pool_size=3, pool_pre_ping=True)
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    from app.config import settings

    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail, Content
        has_sendgrid = True
    except ImportError:
        has_sendgrid = False
        logger.warning("sendgrid not installed, emails will not send")

    sent = 0
    skipped = 0
    failed = 0
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=args.days)

    async with session_maker() as db:
        # Find won deals older than N days with no review request sent
        result = await db.execute(text("""
            SELECT d.id as deal_id, d.title, d.value, d.actual_close_date, d.user_id,
                   c.id as contact_id, c.name as contact_name, c.email as contact_email, c.company as contact_company,
                   u.company_name as user_company, u.email as user_email
            FROM deals d
            JOIN contacts c ON c.id = d.contact_id
            JOIN api_users u ON u.id = d.user_id
            WHERE d.stage = 'won'
              AND d.review_requested_at IS NULL
              AND c.email IS NOT NULL
              AND c.email != ''
              AND d.updated_at <= :cutoff
            ORDER BY d.updated_at ASC
            LIMIT 100
        """), {"cutoff": cutoff})

        deals = result.mappings().all()
        logger.info("Found %d deals eligible for review request", len(deals))

        for deal in deals:
            contact_name = deal["contact_name"] or "Customer"
            contact_email = deal["contact_email"]
            company = deal["user_company"] or deal["user_email"] or "Our Team"
            deal_title = deal["title"] or "your project"

            logger.info("  Deal '%s' (contact: %s <%s>)", deal_title, contact_name, contact_email)

            if args.dry_run:
                sent += 1
                continue

            if not has_sendgrid or not settings.SENDGRID_API_KEY:
                logger.warning("  SendGrid not configured, skipping")
                skipped += 1
                continue

            # Send email
            try:
                frontend_url = settings.FRONTEND_URL
                html = _build_review_email_html(contact_name, company, frontend_url)
                message = Mail(
                    from_email=settings.SENDGRID_FROM_EMAIL,
                    to_emails=contact_email,
                    subject=f"How was your experience with {company}?",
                )
                message.content = [Content("text/html", html)]
                sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
                response = sg.send(message)

                if response.status_code in (200, 201, 202):
                    # Update review_requested_at
                    await db.execute(text("""
                        UPDATE deals SET review_requested_at = :now WHERE id = :deal_id
                    """), {"now": now, "deal_id": deal["deal_id"]})
                    await db.commit()
                    sent += 1
                    logger.info("  Review request sent to %s", contact_email)
                else:
                    failed += 1
                    logger.warning("  SendGrid returned status %s", response.status_code)
            except Exception as e:
                failed += 1
                logger.error("  Error sending to %s: %s", contact_email, e)

    await engine.dispose()

    prefix = "[DRY RUN] " if args.dry_run else ""
    logger.info("%sDone: %d sent, %d failed, %d skipped", prefix, sent, failed, skipped)


if __name__ == "__main__":
    asyncio.run(main())
