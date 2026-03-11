"""SendGrid email service for alert notifications."""

import logging
from app.config import settings

logger = logging.getLogger(__name__)

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Content
except ImportError:
    SendGridAPIClient = None


def _build_matches_html(matches: list[dict]) -> str:
    """Build an HTML table of matched permits."""
    if not matches:
        return "<p>No new matches.</p>"

    rows = ""
    for m in matches[:20]:
        rows += f"""<tr>
            <td style="padding:6px;border:1px solid #ddd">{m.get('permit_number','')}</td>
            <td style="padding:6px;border:1px solid #ddd">{m.get('address','')}</td>
            <td style="padding:6px;border:1px solid #ddd">{m.get('city','')}, {m.get('state','')}</td>
            <td style="padding:6px;border:1px solid #ddd">{m.get('permit_type','')}</td>
            <td style="padding:6px;border:1px solid #ddd">{m.get('issue_date','')}</td>
            <td style="padding:6px;border:1px solid #ddd">{m.get('contractor_name','') or m.get('contractor_company','')}</td>
        </tr>"""

    return f"""<table style="border-collapse:collapse;width:100%;font-size:14px">
    <thead><tr style="background:#f4f4f4">
        <th style="padding:8px;border:1px solid #ddd;text-align:left">Permit #</th>
        <th style="padding:8px;border:1px solid #ddd;text-align:left">Address</th>
        <th style="padding:8px;border:1px solid #ddd;text-align:left">Location</th>
        <th style="padding:8px;border:1px solid #ddd;text-align:left">Type</th>
        <th style="padding:8px;border:1px solid #ddd;text-align:left">Issue Date</th>
        <th style="padding:8px;border:1px solid #ddd;text-align:left">Contractor</th>
    </tr></thead>
    <tbody>{rows}</tbody>
    </table>"""


async def send_alert_email(to: str, alert_name: str, matches: list[dict]) -> bool:
    """Send alert notification email via SendGrid."""
    if not settings.SENDGRID_API_KEY or not SendGridAPIClient:
        logger.warning("SendGrid not configured, skipping email to %s", to)
        return False

    count = len(matches)
    html = f"""<div style="font-family:Arial,sans-serif;max-width:800px;margin:0 auto">
    <h2 style="color:#1a56db">PermitLookup Alert: {alert_name}</h2>
    <p>{count} new permit{'s' if count != 1 else ''} matched your alert criteria.</p>
    {_build_matches_html(matches)}
    {"<p><em>Showing first 20 of " + str(count) + " matches.</em></p>" if count > 20 else ""}
    <hr style="margin:24px 0;border:none;border-top:1px solid #eee">
    <p style="color:#666;font-size:12px">
        You're receiving this because you have an active alert on PermitLookup.
        Manage your alerts at <a href="{settings.FRONTEND_URL}/alerts">your dashboard</a>.
    </p>
    </div>"""

    message = Mail(
        from_email=settings.SENDGRID_FROM_EMAIL,
        to_emails=to,
        subject=f"PermitLookup Alert: {count} new match{'es' if count != 1 else ''} — {alert_name}",
    )
    message.content = [Content("text/html", html)]

    try:
        sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
        response = sg.send(message)
        logger.info("Alert email sent to %s (status %s)", to, response.status_code)
        return response.status_code in (200, 201, 202)
    except Exception as e:
        logger.error("Failed to send alert email to %s: %s", to, e)
        return False
