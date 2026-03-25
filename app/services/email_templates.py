"""Email outreach templates for PermitLookup campaigns.

4 professional HTML templates targeting insurance agents, realtors, contractors,
and a generic follow-up. All include CAN-SPAM compliance (unsubscribe link,
physical address, reason for contact).
"""

from typing import Tuple

BASE_URL = "https://permits.ecbtx.com"
FROM_EMAIL = "Will at PermitLookup <outreach@permitlookup.com>"
PHYSICAL_ADDRESS = "PermitLookup | San Marcos, TX 78666"

# ── State full names ──────────────────────────────────────────────────────────
STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}


def _state_name(state: str) -> str:
    return STATE_NAMES.get(state.upper(), state) if state else "your state"


def _wrap_html(body_content: str, email: str, token: str, state: str) -> str:
    """Wrap body content in the standard dark-theme email layout."""
    unsub_url = f"{BASE_URL}/unsubscribe?email={email}&token={token}"
    state_name = _state_name(state)
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background-color:#0f172a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#0f172a;">
<tr><td align="center" style="padding:40px 20px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;">

<!-- Logo -->
<tr><td style="padding:0 0 24px 0;">
<span style="font-size:24px;font-weight:800;color:#e2e8f0;letter-spacing:-0.5px;">Permit<span style="color:#6366f1;">Lookup</span></span>
</td></tr>

<!-- Main Card -->
<tr><td style="background-color:#1e293b;border-radius:12px;border:1px solid #334155;padding:32px;">
{body_content}
</td></tr>

<!-- Footer -->
<tr><td style="padding:24px 0 0 0;text-align:center;">
<p style="margin:0 0 8px;font-size:12px;color:#64748b;line-height:1.5;">
You're receiving this because you hold a professional license in {state_name}.
</p>
<p style="margin:0 0 8px;font-size:12px;color:#64748b;">
<a href="{unsub_url}" style="color:#6366f1;text-decoration:underline;">Unsubscribe</a> from future emails
</p>
<p style="margin:0;font-size:11px;color:#475569;">{PHYSICAL_ADDRESS}</p>
</td></tr>

</table>
</td></tr>
</table>
</body>
</html>"""


def _cta_button(text: str, url: str) -> str:
    return f"""<table role="presentation" cellpadding="0" cellspacing="0" style="margin:24px 0 8px;">
<tr><td style="background-color:#6366f1;border-radius:8px;">
<a href="{url}" style="display:inline-block;padding:14px 32px;color:#ffffff;font-size:16px;font-weight:700;text-decoration:none;letter-spacing:0.3px;">{text}</a>
</td></tr>
</table>"""


def _feature_row(icon: str, title: str, desc: str) -> str:
    return f"""<tr>
<td style="padding:8px 12px 8px 0;vertical-align:top;font-size:20px;width:36px;">{icon}</td>
<td style="padding:8px 0;">
<p style="margin:0;font-size:14px;font-weight:700;color:#e2e8f0;">{title}</p>
<p style="margin:2px 0 0;font-size:13px;color:#94a3b8;line-height:1.4;">{desc}</p>
</td>
</tr>"""


# ── Template 1: Insurance Agents ─────────────────────────────────────────────

def insurance_pitch(name: str, state: str, email: str, token: str) -> Tuple[str, str, str]:
    """Generate insurance agent outreach email."""
    state_name = _state_name(state)
    first_name = name.split()[0] if name else "there"
    subject = f"Property risk data that {state_name} insurance agents are using to write better policies"

    cta_url = f"{BASE_URL}/?ref=email_insurance"
    body = f"""
<p style="margin:0 0 16px;font-size:16px;color:#e2e8f0;line-height:1.6;">Hi {first_name},</p>
<p style="margin:0 0 20px;font-size:15px;color:#cbd5e1;line-height:1.6;">
Insurance agents in {state_name} are using PermitLookup to underwrite smarter. We aggregate
<strong style="color:#e2e8f0;">923M+ property records</strong> across 50+ data layers so you can
assess risk in seconds, not hours.
</p>

<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin:20px 0;">
{_feature_row("&#127754;", "FEMA Flood Zone Data", "Instant flood zone lookup for any US address — SFHA, Zone X, V, A, and more")}
{_feature_row("&#9888;&#65039;", "Code Violations & Liens", "Active violations, tax liens, and compliance history on every property")}
{_feature_row("&#127968;", "Full Property Reports", "Valuations, sales history, building permits, and environmental risk scores")}
{_feature_row("&#128202;", "EPA & Environmental Risk", "TRI facilities, Superfund proximity, environmental compliance flags")}
</table>

<p style="margin:16px 0 0;font-size:14px;color:#94a3b8;line-height:1.5;">
Used by adjusters, underwriters, and agency owners across {state_name} to reduce loss ratios and
close better-priced policies.
</p>

{_cta_button("Start Your Free Trial &rarr;", cta_url)}
"""

    text_body = f"""Hi {first_name},

Insurance agents in {state_name} are using PermitLookup to underwrite smarter. We aggregate 923M+ property records across 50+ data layers.

- FEMA Flood Zone Data: Instant flood zone lookup for any US address
- Code Violations & Liens: Active violations, tax liens, compliance history
- Full Property Reports: Valuations, sales history, building permits
- EPA & Environmental Risk: TRI facilities, Superfund proximity

Start your free trial: {cta_url}

---
{PHYSICAL_ADDRESS}
Unsubscribe: {BASE_URL}/unsubscribe?email={email}&token={token}
"""

    html = _wrap_html(body, email, token, state)
    return subject, html, text_body


# ── Template 2: Real Estate Agents ───────────────────────────────────────────

def realtor_pitch(name: str, state: str, email: str, token: str) -> Tuple[str, str, str]:
    """Generate real estate agent outreach email."""
    state_name = _state_name(state)
    first_name = name.split()[0] if name else "there"
    subject = f"Every property transaction in {state_name} -- before your competition sees it"

    cta_url = f"{BASE_URL}/?ref=email_realtor"
    body = f"""
<p style="margin:0 0 16px;font-size:16px;color:#e2e8f0;line-height:1.6;">Hi {first_name},</p>
<p style="margin:0 0 20px;font-size:15px;color:#cbd5e1;line-height:1.6;">
Top-producing agents in {state_name} are using PermitLookup to find deals before they hit the MLS.
We track <strong style="color:#e2e8f0;">every building permit, property sale, and market signal</strong>
across the entire state.
</p>

<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin:20px 0;">
{_feature_row("&#128200;", "Property Sales History", "See every recorded sale, price, buyer/seller — going back decades")}
{_feature_row("&#128640;", "Permit-to-Sale Pipeline", "Properties with recent permits sell 40% faster — find them first")}
{_feature_row("&#127919;", "Market Trends & Analytics", "Zip-level pricing trends, days-on-market, inventory signals")}
{_feature_row("&#129302;", "AI-Powered Analyst", "Ask questions in plain English — our AI searches 923M+ records for you")}
</table>

<p style="margin:16px 0 0;font-size:14px;color:#94a3b8;line-height:1.5;">
When a homeowner pulls a renovation permit, they're 3x more likely to list within 18 months.
You'll know about it the day the permit is filed.
</p>

{_cta_button("Get Free Access &rarr;", cta_url)}
"""

    text_body = f"""Hi {first_name},

Top-producing agents in {state_name} are using PermitLookup to find deals before they hit the MLS.

- Property Sales History: Every recorded sale, price, buyer/seller
- Permit-to-Sale Pipeline: Properties with recent permits sell 40% faster
- Market Trends & Analytics: Zip-level pricing, days-on-market, inventory
- AI-Powered Analyst: Ask questions in plain English across 923M+ records

Get free access: {cta_url}

---
{PHYSICAL_ADDRESS}
Unsubscribe: {BASE_URL}/unsubscribe?email={email}&token={token}
"""

    html = _wrap_html(body, email, token, state)
    return subject, html, text_body


# ── Template 3: Contractors ──────────────────────────────────────────────────

def contractor_pitch(name: str, state: str, email: str, token: str, trade: str = "construction") -> Tuple[str, str, str]:
    """Generate contractor outreach email."""
    state_name = _state_name(state)
    first_name = name.split()[0] if name else "there"

    # Normalize trade name for subject line
    trade_display = trade.title() if trade else "Construction"
    subject = f"{trade_display} permits filed in {state_name} this week -- want the leads?"

    cta_url = f"{BASE_URL}/?ref=email_contractor"
    body = f"""
<p style="margin:0 0 16px;font-size:16px;color:#e2e8f0;line-height:1.6;">Hi {first_name},</p>
<p style="margin:0 0 20px;font-size:15px;color:#cbd5e1;line-height:1.6;">
Every week, thousands of {trade_display.lower()} permits are filed in {state_name}.
Each one is a potential customer. PermitLookup delivers them to you
<strong style="color:#e2e8f0;">fresh daily</strong>, with contact info and project details.
</p>

<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin:20px 0;">
{_feature_row("&#128293;", "Fresh Daily Permits", f"New {trade_display.lower()} permits in your area, delivered every morning")}
{_feature_row("&#128222;", "Click-to-Call Dialer", "Built-in sales dialer with AI call summaries and follow-up reminders")}
{_feature_row("&#128188;", "Full CRM System", "Track leads, deals, quotes, and commissions in one place")}
{_feature_row("&#127919;", "Competitor Intelligence", "See which contractors are pulling permits in your territory")}
</table>

<p style="margin:16px 0 0;font-size:14px;color:#94a3b8;line-height:1.5;">
The contractors who call first win the job. PermitLookup gives you a 2-3 day head start
over anyone watching the public records manually.
</p>

{_cta_button("See Fresh Permits &rarr;", cta_url)}
"""

    text_body = f"""Hi {first_name},

Every week, thousands of {trade_display.lower()} permits are filed in {state_name}. Each one is a potential customer.

- Fresh Daily Permits: New {trade_display.lower()} permits in your area, delivered every morning
- Click-to-Call Dialer: Built-in sales dialer with AI call summaries
- Full CRM System: Track leads, deals, quotes, and commissions
- Competitor Intelligence: See which contractors are pulling permits in your territory

See fresh permits: {cta_url}

---
{PHYSICAL_ADDRESS}
Unsubscribe: {BASE_URL}/unsubscribe?email={email}&token={token}
"""

    html = _wrap_html(body, email, token, state)
    return subject, html, text_body


# ── Template 4: Follow-up ────────────────────────────────────────────────────

def followup_pitch(name: str, email: str, token: str, state: str = "") -> Tuple[str, str, str]:
    """Generate follow-up email for recipients who didn't open the first email."""
    first_name = name.split()[0] if name else "there"
    subject = "Quick follow-up -- did you see this?"

    cta_url = f"{BASE_URL}/?ref=email_followup"
    body = f"""
<p style="margin:0 0 16px;font-size:16px;color:#e2e8f0;line-height:1.6;">Hi {first_name},</p>
<p style="margin:0 0 20px;font-size:15px;color:#cbd5e1;line-height:1.6;">
I wanted to make sure you saw my note from last week. PermitLookup is the largest
property intelligence platform in the US, and I think it could be a game-changer for your work.
</p>

<div style="background:#0f172a;border-radius:8px;padding:20px;margin:20px 0;border-left:4px solid #6366f1;">
<p style="margin:0 0 12px;font-size:22px;font-weight:800;color:#e2e8f0;">923M+ records</p>
<p style="margin:0 0 8px;font-size:14px;color:#94a3b8;">Building permits, property sales, flood zones, code violations, contractor licenses, EPA data, and more.</p>
<p style="margin:0 0 8px;font-size:14px;color:#94a3b8;">50+ data layers across every US state.</p>
<p style="margin:0;font-size:14px;color:#94a3b8;">AI-powered analyst, sales dialer, CRM, and real-time alerts included.</p>
</div>

<p style="margin:0 0 20px;font-size:15px;color:#cbd5e1;line-height:1.6;">
Used daily by contractors, insurers, real estate professionals, and investors nationwide.
Free trial gets you instant access.
</p>

{_cta_button("Try It Free &rarr;", cta_url)}
"""

    text_body = f"""Hi {first_name},

I wanted to make sure you saw my note from last week.

PermitLookup is the largest property intelligence platform in the US:
- 923M+ records across 50+ data layers
- Building permits, property sales, flood zones, code violations, and more
- AI-powered analyst, sales dialer, CRM, and real-time alerts
- Used by contractors, insurers, RE agents, and investors nationwide

Try it free: {cta_url}

---
{PHYSICAL_ADDRESS}
Unsubscribe: {BASE_URL}/unsubscribe?email={email}&token={token}
"""

    html = _wrap_html(body, email, token, state)
    return subject, html, text_body


# ── Template Registry ─────────────────────────────────────────────────────────

TEMPLATES = {
    "insurance": insurance_pitch,
    "realtor": realtor_pitch,
    "contractor": contractor_pitch,
    "followup": followup_pitch,
}


def get_template(key: str):
    """Get a template function by key."""
    return TEMPLATES.get(key)


def list_templates() -> list[str]:
    """List available template keys."""
    return list(TEMPLATES.keys())
