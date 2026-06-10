"""SendGrid email service for alert notifications."""

import html as _html
import logging
from itertools import groupby

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
            <td style="padding:6px;border:1px solid #ddd">{m.get('applicant_name','')}</td>
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


def _e(v) -> str:
    """Escape scraped free-text before HTML interpolation."""
    return _html.escape(str(v)) if v is not None else ""


def _build_w1_matches_html(matches: list[dict]) -> str:
    """W-1 drilling permit digest: matches grouped by county."""
    if not matches:
        return "<p>No new W-1 filings.</p>"

    sections = ""
    by_county = groupby(
        sorted(matches, key=lambda m: (m.get("county") or "", m.get("approved_date") or "")),
        key=lambda m: m.get("county") or "UNKNOWN",
    )
    for county, group in by_county:
        items = list(group)
        rows = ""
        for m in items[:25]:
            depth = m.get("total_depth")
            rows += f"""<tr>
                <td style="padding:6px;border:1px solid #ddd">{_e(m.get('permit_number'))}</td>
                <td style="padding:6px;border:1px solid #ddd">{_e(m.get('operator'))}</td>
                <td style="padding:6px;border:1px solid #ddd">{_e(m.get('lease_name'))} #{_e(m.get('well_number'))}</td>
                <td style="padding:6px;border:1px solid #ddd">{_e(m.get('wellbore_profile'))}</td>
                <td style="padding:6px;border:1px solid #ddd">{f"{depth:,.0f} ft" if depth else ""}</td>
                <td style="padding:6px;border:1px solid #ddd">{_e(m.get('approved_date'))}</td>
            </tr>"""
        sections += f"""
        <h3 style="margin:18px 0 6px;color:#0f172a">{_e(county)} County
            <span style="font-weight:400;color:#666">({len(items)} permit{'s' if len(items) != 1 else ''})</span></h3>
        <table style="border-collapse:collapse;width:100%;font-size:13.5px">
        <thead><tr style="background:#f4f4f4">
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Permit #</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Operator</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Lease / Well</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Profile</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Depth</th>
            <th style="padding:8px;border:1px solid #ddd;text-align:left">Approved</th>
        </tr></thead>
        <tbody>{rows}</tbody>
        </table>"""
    return sections


async def send_alert_email(to: str, alert_name: str, matches: list[dict],
                           source_type: str = "permits") -> bool:
    """Send alert notification email via SendGrid."""
    if not settings.SENDGRID_API_KEY or not SendGridAPIClient:
        logger.warning("SendGrid not configured, skipping email to %s", to)
        return False

    count = len(matches)
    if source_type == "well_permits":
        noun = f"new W-1 drilling permit{'s' if count != 1 else ''}"
        body = _build_w1_matches_html(matches)
    else:
        noun = f"new permit{'s' if count != 1 else ''}"
        body = _build_matches_html(matches)

    html = f"""<div style="font-family:Arial,sans-serif;max-width:800px;margin:0 auto">
    <h2 style="color:#1a56db">PermitLookup Alert: {_e(alert_name)}</h2>
    <p>{count} {noun} matched your watchlist criteria.</p>
    {body}
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
        subject=f"PermitLookup Alert: {count} {noun} — {alert_name}",
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
