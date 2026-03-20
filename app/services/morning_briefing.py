"""Morning Briefing Email — daily AI-powered sales brief for each user.

Queries user stats (new permits, callbacks, pipeline, yesterday's activity),
generates an AI one-liner with Claude Haiku, and sends a styled HTML email via SendGrid.
"""

import logging
from datetime import datetime, timezone, timedelta

from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.api_key import ApiUser, PlanTier, resolve_plan
from app.models.crm import Contact, Deal, Commission
from app.models.dialer import CallLog

logger = logging.getLogger(__name__)

try:
    from anthropic import Anthropic
except ImportError:
    Anthropic = None

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Content
except ImportError:
    SendGridAPIClient = None


async def _gather_user_stats(user: ApiUser, db: AsyncSession) -> dict:
    """Gather yesterday's stats and today's agenda for a user."""
    now = datetime.now(timezone.utc)
    yesterday_start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = yesterday_start + timedelta(days=1)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)

    # New permits in user's saved search areas since yesterday
    new_permits = 0
    try:
        r = await db.execute(text("""
            SELECT COUNT(*) FROM hot_leads
            WHERE created_at >= :since
        """), {"since": yesterday_start})
        new_permits = r.scalar() or 0
    except Exception:
        pass

    # Top ZIP by permit velocity (new permits in last 7 days vs prior 7 days)
    top_zip = None
    zip_velocity = 0
    try:
        week_ago = now - timedelta(days=7)
        two_weeks_ago = now - timedelta(days=14)
        r = await db.execute(text("""
            WITH recent AS (
                SELECT zip, COUNT(*) as cnt FROM hot_leads
                WHERE created_at >= :week_ago AND zip IS NOT NULL
                GROUP BY zip ORDER BY cnt DESC LIMIT 10
            ),
            prior AS (
                SELECT zip, COUNT(*) as cnt FROM hot_leads
                WHERE created_at >= :two_weeks_ago AND created_at < :week_ago AND zip IS NOT NULL
                GROUP BY zip
            )
            SELECT r.zip, r.cnt as recent_cnt, COALESCE(p.cnt, 0) as prior_cnt,
                   CASE WHEN COALESCE(p.cnt, 0) > 0
                        THEN ROUND((r.cnt::numeric - p.cnt::numeric) / p.cnt::numeric * 100)
                        ELSE 100 END as velocity_pct
            FROM recent r LEFT JOIN prior p ON r.zip = p.zip
            ORDER BY velocity_pct DESC LIMIT 1
        """), {"week_ago": week_ago, "two_weeks_ago": two_weeks_ago})
        row = r.mappings().first()
        if row:
            top_zip = row["zip"]
            zip_velocity = int(row["velocity_pct"])
    except Exception:
        pass

    # Callbacks due today
    callbacks_today = 0
    try:
        r = await db.execute(
            select(func.count()).select_from(CallLog).where(
                CallLog.user_id == user.id,
                CallLog.callback_date >= today_start,
                CallLog.callback_date < today_end,
            )
        )
        callbacks_today = r.scalar() or 0
    except Exception:
        pass

    # Pipeline value (active deals)
    pipeline_value = 0.0
    try:
        r = await db.execute(
            select(func.coalesce(func.sum(Deal.value), 0)).where(
                Deal.user_id == user.id,
                Deal.stage.notin_(["won", "lost"]),
            )
        )
        pipeline_value = float(r.scalar() or 0)
    except Exception:
        pass

    # Yesterday's calls
    yesterday_calls = 0
    try:
        r = await db.execute(
            select(func.count()).select_from(CallLog).where(
                CallLog.user_id == user.id,
                CallLog.created_at >= yesterday_start,
                CallLog.created_at < yesterday_end,
            )
        )
        yesterday_calls = r.scalar() or 0
    except Exception:
        pass

    # Yesterday's new contacts
    yesterday_contacts = 0
    try:
        r = await db.execute(
            select(func.count()).select_from(Contact).where(
                Contact.user_id == user.id,
                Contact.created_at >= yesterday_start,
                Contact.created_at < yesterday_end,
            )
        )
        yesterday_contacts = r.scalar() or 0
    except Exception:
        pass

    # Yesterday's deals won
    yesterday_deals_won = 0
    yesterday_revenue = 0.0
    try:
        r = await db.execute(
            select(func.count(), func.coalesce(func.sum(Deal.value), 0)).where(
                Deal.user_id == user.id,
                Deal.stage == "won",
                Deal.updated_at >= yesterday_start,
                Deal.updated_at < yesterday_end,
            )
        )
        row = r.one()
        yesterday_deals_won = row[0] or 0
        yesterday_revenue = float(row[1] or 0)
    except Exception:
        pass

    return {
        "new_permits": new_permits,
        "top_zip": top_zip,
        "zip_velocity": zip_velocity,
        "callbacks_today": callbacks_today,
        "pipeline_value": pipeline_value,
        "yesterday_calls": yesterday_calls,
        "yesterday_contacts": yesterday_contacts,
        "yesterday_deals_won": yesterday_deals_won,
        "yesterday_revenue": yesterday_revenue,
    }


def _generate_ai_insight(stats: dict) -> str:
    """Generate a one-liner AI insight using Claude Haiku."""
    if not Anthropic or not settings.ANTHROPIC_API_KEY:
        # Fallback if no API key
        if stats["top_zip"] and stats["zip_velocity"] > 0:
            return f"Focus on ZIP {stats['top_zip']} today — permit velocity is up {stats['zip_velocity']}%."
        if stats["callbacks_today"] > 0:
            return f"You have {stats['callbacks_today']} callback{'s' if stats['callbacks_today'] != 1 else ''} scheduled — start there for quick wins."
        return "Hit the phones early — morning calls have 28% higher connect rates."

    try:
        client = Anthropic(api_key=settings.ANTHROPIC_API_KEY)
        prompt = f"""Based on this salesperson's daily stats, write ONE concise, actionable sentence (max 120 chars) to guide their day. Be specific and data-driven. Do not use emojis.

Stats:
- New permits in area: {stats['new_permits']}
- Top ZIP by velocity: {stats['top_zip']} (up {stats['zip_velocity']}%)
- Callbacks due today: {stats['callbacks_today']}
- Pipeline value: ${stats['pipeline_value']:,.0f}
- Yesterday: {stats['yesterday_calls']} calls, {stats['yesterday_contacts']} contacts, {stats['yesterday_deals_won']} deals won (${stats['yesterday_revenue']:,.0f})

Reply with ONLY the one-liner, no quotes, no explanation."""

        response = client.messages.create(
            model="claude-3-5-haiku-latest",
            max_tokens=100,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        logger.warning("AI insight generation failed: %s", e)
        if stats["top_zip"]:
            return f"Focus on ZIP {stats['top_zip']} today — permit velocity is up {stats['zip_velocity']}%."
        return "Start with your callbacks — reconnecting converts 3x better than cold calls."


def _build_briefing_html(user: ApiUser, stats: dict, ai_insight: str) -> str:
    """Build the morning briefing HTML email."""
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%A, %B %d, %Y")
    frontend_url = settings.FRONTEND_URL

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#0a0a0f;font-family:-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a0f;padding:20px 0">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%">

<!-- Header -->
<tr><td style="padding:24px 32px;background:linear-gradient(135deg,#12121a,#1a1a25);border-radius:12px 12px 0 0;border:1px solid #2a2a3a;border-bottom:none">
  <div style="display:flex;align-items:center;gap:10px">
    <div style="width:32px;height:32px;background:linear-gradient(135deg,#6366f1,#a855f7);border-radius:8px;display:inline-block;text-align:center;line-height:32px;font-size:16px;color:#fff;font-weight:700">P</div>
    <span style="font-size:20px;font-weight:700;color:#e8e8f0">Morning Briefing</span>
  </div>
  <div style="font-size:13px;color:#6a6a80;margin-top:8px">{date_str}</div>
  <div style="font-size:13px;color:#a0a0b8;margin-top:2px">{user.company_name or user.email}</div>
</td></tr>

<!-- AI Insight Banner -->
<tr><td style="padding:20px 32px;background:linear-gradient(135deg,rgba(99,102,241,.15),rgba(168,85,247,.1));border-left:1px solid #2a2a3a;border-right:1px solid #2a2a3a">
  <div style="font-size:15px;font-weight:600;color:#818cf8;line-height:1.5">{ai_insight}</div>
</td></tr>

<!-- Stat Cards -->
<tr><td style="padding:20px 32px;background:#12121a;border-left:1px solid #2a2a3a;border-right:1px solid #2a2a3a">
  <div style="font-size:11px;font-weight:700;color:#6a6a80;text-transform:uppercase;letter-spacing:1px;margin-bottom:12px">Today's Numbers</div>
  <table width="100%" cellpadding="0" cellspacing="0">
  <tr>
    <td width="33%" style="padding:12px;background:#1a1a25;border:1px solid #2a2a3a;border-radius:8px;text-align:center">
      <div style="font-size:28px;font-weight:800;color:#22c55e;font-variant-numeric:tabular-nums">{stats['new_permits']:,}</div>
      <div style="font-size:10px;color:#6a6a80;text-transform:uppercase;letter-spacing:.5px;margin-top:4px">New Permits</div>
    </td>
    <td width="4"></td>
    <td width="33%" style="padding:12px;background:#1a1a25;border:1px solid #2a2a3a;border-radius:8px;text-align:center">
      <div style="font-size:28px;font-weight:800;color:#f59e0b;font-variant-numeric:tabular-nums">{stats['callbacks_today']}</div>
      <div style="font-size:10px;color:#6a6a80;text-transform:uppercase;letter-spacing:.5px;margin-top:4px">Callbacks Due</div>
    </td>
    <td width="4"></td>
    <td width="33%" style="padding:12px;background:#1a1a25;border:1px solid #2a2a3a;border-radius:8px;text-align:center">
      <div style="font-size:28px;font-weight:800;color:#818cf8;font-variant-numeric:tabular-nums">${stats['pipeline_value']:,.0f}</div>
      <div style="font-size:10px;color:#6a6a80;text-transform:uppercase;letter-spacing:.5px;margin-top:4px">Pipeline</div>
    </td>
  </tr>
  </table>
</td></tr>

<!-- Yesterday's Activity -->
<tr><td style="padding:20px 32px;background:#12121a;border-left:1px solid #2a2a3a;border-right:1px solid #2a2a3a">
  <div style="font-size:11px;font-weight:700;color:#6a6a80;text-transform:uppercase;letter-spacing:1px;margin-bottom:12px">Yesterday's Activity</div>
  <table width="100%" cellpadding="0" cellspacing="0">
  <tr>
    <td width="25%" style="padding:10px;background:#1a1a25;border:1px solid #2a2a3a;border-radius:8px;text-align:center">
      <div style="font-size:22px;font-weight:800;color:#e8e8f0">{stats['yesterday_calls']}</div>
      <div style="font-size:10px;color:#6a6a80;text-transform:uppercase;margin-top:2px">Calls</div>
    </td>
    <td width="4"></td>
    <td width="25%" style="padding:10px;background:#1a1a25;border:1px solid #2a2a3a;border-radius:8px;text-align:center">
      <div style="font-size:22px;font-weight:800;color:#e8e8f0">{stats['yesterday_contacts']}</div>
      <div style="font-size:10px;color:#6a6a80;text-transform:uppercase;margin-top:2px">Contacts</div>
    </td>
    <td width="4"></td>
    <td width="25%" style="padding:10px;background:#1a1a25;border:1px solid #2a2a3a;border-radius:8px;text-align:center">
      <div style="font-size:22px;font-weight:800;color:#22c55e">{stats['yesterday_deals_won']}</div>
      <div style="font-size:10px;color:#6a6a80;text-transform:uppercase;margin-top:2px">Deals Won</div>
    </td>
    <td width="4"></td>
    <td width="25%" style="padding:10px;background:#1a1a25;border:1px solid #2a2a3a;border-radius:8px;text-align:center">
      <div style="font-size:22px;font-weight:800;color:#22c55e">${stats['yesterday_revenue']:,.0f}</div>
      <div style="font-size:10px;color:#6a6a80;text-transform:uppercase;margin-top:2px">Revenue</div>
    </td>
  </tr>
  </table>
</td></tr>

<!-- CTA Button -->
<tr><td style="padding:24px 32px;background:#12121a;border-left:1px solid #2a2a3a;border-right:1px solid #2a2a3a;text-align:center">
  <a href="{frontend_url}#dialer" style="display:inline-block;padding:16px 48px;background:linear-gradient(135deg,#6366f1,#818cf8);color:#fff;font-size:16px;font-weight:700;border-radius:10px;text-decoration:none;letter-spacing:.5px">Open Dialer</a>
</td></tr>

<!-- Top ZIP -->
{"" if not stats['top_zip'] else f'''<tr><td style="padding:16px 32px;background:#12121a;border-left:1px solid #2a2a3a;border-right:1px solid #2a2a3a">
  <div style="padding:14px 18px;background:rgba(99,102,241,.08);border:1px solid rgba(99,102,241,.2);border-radius:8px;display:flex;align-items:center;gap:12px">
    <div style="font-size:13px;color:#a0a0b8">Hottest ZIP: <strong style="color:#818cf8">{stats['top_zip']}</strong> — permit velocity up <strong style="color:#22c55e">{stats['zip_velocity']}%</strong> this week</div>
  </div>
</td></tr>'''}

<!-- Footer -->
<tr><td style="padding:20px 32px;background:#12121a;border:1px solid #2a2a3a;border-top:none;border-radius:0 0 12px 12px;text-align:center">
  <div style="font-size:11px;color:#6a6a80;line-height:1.6">
    <a href="{frontend_url}#crm" style="color:#818cf8;text-decoration:none">CRM</a> &nbsp;&middot;&nbsp;
    <a href="{frontend_url}#dialer" style="color:#818cf8;text-decoration:none">Dialer</a> &nbsp;&middot;&nbsp;
    <a href="{frontend_url}#search" style="color:#818cf8;text-decoration:none">Search Permits</a>
    <br>PermitLookup &mdash; Building permit intelligence for sales teams
  </div>
</td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


async def send_morning_briefing(user_id, db: AsyncSession) -> bool:
    """Send the morning briefing email to a single user."""
    # Fetch user
    result = await db.execute(
        select(ApiUser).where(ApiUser.id == user_id, ApiUser.is_active == True)
    )
    user = result.scalar_one_or_none()
    if not user:
        logger.warning("User %s not found or inactive, skipping briefing", user_id)
        return False

    # Only send to paid plans
    plan = resolve_plan(user.plan)
    if plan == PlanTier.FREE:
        logger.info("User %s is on free plan, skipping briefing", user.email)
        return False

    if not user.email:
        logger.warning("User %s has no email, skipping briefing", user_id)
        return False

    # Gather stats
    stats = await _gather_user_stats(user, db)
    logger.info("Stats for %s: %s", user.email, stats)

    # Generate AI insight
    ai_insight = _generate_ai_insight(stats)

    # Build HTML
    html = _build_briefing_html(user, stats, ai_insight)

    # Send via SendGrid
    if not settings.SENDGRID_API_KEY or not SendGridAPIClient:
        logger.warning("SendGrid not configured, skipping email to %s", user.email)
        return False

    try:
        now = datetime.now(timezone.utc)
        message = Mail(
            from_email=settings.SENDGRID_FROM_EMAIL,
            to_emails=user.email,
            subject=f"Your Morning Briefing — {now.strftime('%b %d')}: {stats['new_permits']:,} new permits, {stats['callbacks_today']} callbacks",
        )
        message.content = [Content("text/html", html)]

        sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
        response = sg.send(message)
        logger.info("Briefing sent to %s (status %s)", user.email, response.status_code)
        return response.status_code in (200, 201, 202)
    except Exception as e:
        logger.error("Failed to send briefing to %s: %s", user.email, e)
        return False
