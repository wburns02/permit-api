"""Email campaign endpoints — create, manage, and monitor outreach campaigns.

Admin endpoints require API key authentication + admin email whitelist.
Public endpoints: unsubscribe and SendGrid webhook.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db, async_session_maker
from app.middleware.api_key_auth import get_current_user
from app.models.api_key import ApiUser
from app.models.email_campaign import EmailCampaign, EmailRecipient, EmailUnsubscribe
from app.services.email_outreach import (
    create_campaign_from_prospects,
    send_campaign_batch,
    process_sendgrid_events,
    generate_unsubscribe_token,
    verify_unsubscribe_token,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/campaigns", tags=["Email Campaigns"])

ADMIN_EMAILS = ["will@ecbtx.com", "admin@ecbtx.com", "willwalterburns@gmail.com"]


# ── Auth ──────────────────────────────────────────────────────────────────────

async def require_admin(user: ApiUser = Depends(get_current_user)) -> ApiUser:
    """Ensure the authenticated user is an admin."""
    if not user or user.email not in ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="Admin access required.")
    return user


# ── Pydantic Schemas ──────────────────────────────────────────────────────────

class CreateCampaignRequest(BaseModel):
    name: str
    audience: str = "contractor"  # insurance / realtor / contractor / all
    state: Optional[str] = None
    template: str = "contractor"  # insurance / realtor / contractor / followup
    send_rate: int = 200  # emails per hour


class CampaignResponse(BaseModel):
    id: str
    name: str
    subject: str
    target_audience: str | None
    target_state: str | None
    status: str
    sent_count: int
    open_count: int
    click_count: int
    unsubscribe_count: int
    signup_count: int
    bounce_count: int
    send_rate: int
    created_at: str
    started_at: str | None
    completed_at: str | None
    open_rate: float
    click_rate: float
    bounce_rate: float

    @classmethod
    def from_campaign(cls, c: EmailCampaign) -> "CampaignResponse":
        sent = c.sent_count or 0
        return cls(
            id=str(c.id),
            name=c.name,
            subject=c.subject or "",
            target_audience=c.target_audience,
            target_state=c.target_state,
            status=c.status,
            sent_count=sent,
            open_count=c.open_count or 0,
            click_count=c.click_count or 0,
            unsubscribe_count=c.unsubscribe_count or 0,
            signup_count=c.signup_count or 0,
            bounce_count=c.bounce_count or 0,
            send_rate=c.send_rate or 200,
            created_at=c.created_at.isoformat() if c.created_at else "",
            started_at=c.started_at.isoformat() if c.started_at else None,
            completed_at=c.completed_at.isoformat() if c.completed_at else None,
            open_rate=round((c.open_count or 0) / sent * 100, 1) if sent > 0 else 0,
            click_rate=round((c.click_count or 0) / sent * 100, 1) if sent > 0 else 0,
            bounce_rate=round((c.bounce_count or 0) / sent * 100, 1) if sent > 0 else 0,
        )


class UnsubscribeRequest(BaseModel):
    email: str
    token: str
    reason: Optional[str] = None


# ── Admin Endpoints ───────────────────────────────────────────────────────────

@router.post("", status_code=201)
async def create_campaign(
    body: CreateCampaignRequest,
    admin: ApiUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Create a new email campaign from prospect_contacts."""
    try:
        campaign_id, recipient_count = await create_campaign_from_prospects(
            db=db,
            name=body.name,
            audience=body.audience,
            state=body.state,
            template_key=body.template,
            send_rate=body.send_rate,
        )
        return {
            "campaign_id": str(campaign_id),
            "recipient_count": recipient_count,
            "status": "draft",
            "message": f"Campaign created with {recipient_count:,} recipients",
        }
    except Exception as e:
        logger.error("Failed to create campaign: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("")
async def list_campaigns(
    admin: ApiUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """List all campaigns with stats."""
    result = await db.execute(
        select(EmailCampaign).order_by(EmailCampaign.created_at.desc())
    )
    campaigns = result.scalars().all()

    # Get recipient counts per campaign
    counts_result = await db.execute(
        select(
            EmailRecipient.campaign_id,
            func.count(EmailRecipient.id).label("total"),
            func.count(EmailRecipient.id).filter(EmailRecipient.status == "pending").label("pending"),
        )
        .group_by(EmailRecipient.campaign_id)
    )
    counts_map = {str(row[0]): {"total": row[1], "pending": row[2]} for row in counts_result.all()}

    return {
        "campaigns": [
            {
                **CampaignResponse.from_campaign(c).model_dump(),
                "total_recipients": counts_map.get(str(c.id), {}).get("total", 0),
                "pending_recipients": counts_map.get(str(c.id), {}).get("pending", 0),
            }
            for c in campaigns
        ],
        "total": len(campaigns),
    }


@router.post("/{campaign_id}/start")
async def start_campaign(
    campaign_id: str,
    admin: ApiUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Activate a campaign to start sending."""
    result = await db.execute(
        select(EmailCampaign).where(EmailCampaign.id == campaign_id)
    )
    campaign = result.scalar_one_or_none()
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if campaign.status not in ("draft", "paused"):
        raise HTTPException(status_code=400, detail=f"Cannot start campaign with status '{campaign.status}'")

    campaign.status = "active"
    campaign.started_at = datetime.now(timezone.utc)
    await db.commit()

    return {"status": "active", "message": "Campaign started", "campaign_id": str(campaign.id)}


@router.post("/{campaign_id}/pause")
async def pause_campaign(
    campaign_id: str,
    admin: ApiUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Pause an active campaign."""
    result = await db.execute(
        select(EmailCampaign).where(EmailCampaign.id == campaign_id)
    )
    campaign = result.scalar_one_or_none()
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    if campaign.status != "active":
        raise HTTPException(status_code=400, detail=f"Cannot pause campaign with status '{campaign.status}'")

    campaign.status = "paused"
    await db.commit()

    return {"status": "paused", "message": "Campaign paused", "campaign_id": str(campaign.id)}


@router.get("/{campaign_id}/stats")
async def campaign_stats(
    campaign_id: str,
    admin: ApiUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get detailed stats for a campaign including conversion funnel."""
    result = await db.execute(
        select(EmailCampaign).where(EmailCampaign.id == campaign_id)
    )
    campaign = result.scalar_one_or_none()
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    # Status breakdown
    status_result = await db.execute(
        select(
            EmailRecipient.status,
            func.count(EmailRecipient.id),
        )
        .where(EmailRecipient.campaign_id == campaign_id)
        .group_by(EmailRecipient.status)
    )
    status_breakdown = {row[0]: row[1] for row in status_result.all()}

    # State breakdown
    state_result = await db.execute(
        select(
            EmailRecipient.state,
            func.count(EmailRecipient.id),
        )
        .where(EmailRecipient.campaign_id == campaign_id)
        .group_by(EmailRecipient.state)
        .order_by(func.count(EmailRecipient.id).desc())
        .limit(20)
    )
    state_breakdown = {row[0] or "Unknown": row[1] for row in state_result.all()}

    total = sum(status_breakdown.values())
    sent = campaign.sent_count or 0
    opened = campaign.open_count or 0
    clicked = campaign.click_count or 0
    signups = campaign.signup_count or 0

    return {
        "campaign": CampaignResponse.from_campaign(campaign).model_dump(),
        "funnel": {
            "total_recipients": total,
            "sent": sent,
            "opened": opened,
            "clicked": clicked,
            "signups": signups,
            "open_rate": round(opened / sent * 100, 1) if sent else 0,
            "click_rate": round(clicked / sent * 100, 1) if sent else 0,
            "click_to_open_rate": round(clicked / opened * 100, 1) if opened else 0,
            "signup_rate": round(signups / clicked * 100, 1) if clicked else 0,
        },
        "status_breakdown": status_breakdown,
        "state_breakdown": state_breakdown,
    }


@router.get("/{campaign_id}/recipients")
async def campaign_recipients(
    campaign_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: Optional[str] = None,
    admin: ApiUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get paginated recipient list for a campaign."""
    query = select(EmailRecipient).where(EmailRecipient.campaign_id == campaign_id)
    count_query = select(func.count(EmailRecipient.id)).where(EmailRecipient.campaign_id == campaign_id)

    if status:
        query = query.where(EmailRecipient.status == status)
        count_query = count_query.where(EmailRecipient.status == status)

    # Total count
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Paginated results
    query = query.order_by(EmailRecipient.sent_at.desc().nullslast()).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(query)
    recipients = result.scalars().all()

    return {
        "recipients": [
            {
                "id": str(r.id),
                "email": r.email,
                "name": r.name,
                "company": r.company,
                "state": r.state,
                "license_type": r.license_type,
                "status": r.status,
                "sent_at": r.sent_at.isoformat() if r.sent_at else None,
                "opened_at": r.opened_at.isoformat() if r.opened_at else None,
                "clicked_at": r.clicked_at.isoformat() if r.clicked_at else None,
            }
            for r in recipients
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
        "pages": (total + page_size - 1) // page_size,
    }


@router.post("/{campaign_id}/send-batch")
async def send_batch(
    campaign_id: str,
    batch_size: int = Query(50, ge=1, le=500),
    dry_run: bool = Query(False),
    admin: ApiUser = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Manually trigger sending a batch of emails for a campaign."""
    result = await send_campaign_batch(campaign_id, batch_size, db, dry_run=dry_run)
    return result


# ── Public Endpoints ──────────────────────────────────────────────────────────

@router.get("/unsubscribe", response_class=HTMLResponse)
async def unsubscribe_page(
    email: str = Query(...),
    token: str = Query(...),
):
    """Render the unsubscribe confirmation page."""
    valid = verify_unsubscribe_token(email, token)

    if not valid:
        return HTMLResponse(content="""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Invalid Link - PermitLookup</title>
<style>body{margin:0;padding:0;background:#0f172a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;display:flex;justify-content:center;align-items:center;min-height:100vh;}
.card{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:40px;max-width:480px;text-align:center;}
h1{color:#e2e8f0;font-size:24px;margin:0 0 12px;}
p{color:#94a3b8;font-size:15px;line-height:1.6;}
</style></head>
<body><div class="card">
<h1>Invalid Unsubscribe Link</h1>
<p>This unsubscribe link is invalid or has expired. If you'd like to unsubscribe, please contact us at support@permitlookup.com.</p>
</div></body></html>""", status_code=400)

    return HTMLResponse(content=f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Unsubscribe - PermitLookup</title>
<style>body{{margin:0;padding:0;background:#0f172a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;display:flex;justify-content:center;align-items:center;min-height:100vh;}}
.card{{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:40px;max-width:480px;text-align:center;}}
h1{{color:#e2e8f0;font-size:24px;margin:0 0 12px;}}
p{{color:#94a3b8;font-size:15px;line-height:1.6;}}
.logo{{font-size:28px;font-weight:800;color:#e2e8f0;margin-bottom:20px;}}
.logo span{{color:#6366f1;}}
.btn{{display:inline-block;margin-top:20px;padding:12px 28px;background:#6366f1;color:#fff;border:none;border-radius:8px;font-size:15px;font-weight:600;cursor:pointer;text-decoration:none;}}
.btn:hover{{background:#4f46e5;}}
.subtle{{color:#64748b;font-size:12px;margin-top:16px;}}
</style></head>
<body><div class="card">
<div class="logo">Permit<span>Lookup</span></div>
<h1>Unsubscribe</h1>
<p>Click the button below to unsubscribe <strong style="color:#e2e8f0;">{email}</strong> from PermitLookup emails.</p>
<form method="POST" action="/v1/campaigns/unsubscribe">
<input type="hidden" name="email" value="{email}">
<input type="hidden" name="token" value="{token}">
<button type="submit" class="btn">Unsubscribe Me</button>
</form>
<p class="subtle">PermitLookup | San Marcos, TX 78666</p>
</div></body></html>""")


@router.post("/unsubscribe")
async def process_unsubscribe(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Process an unsubscribe request (form POST or JSON)."""
    # Support both form data and JSON
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        data = await request.json()
        email = data.get("email", "")
        token = data.get("token", "")
        reason = data.get("reason", "")
    else:
        form = await request.form()
        email = form.get("email", "")
        token = form.get("token", "")
        reason = form.get("reason", "")

    if not email or not token:
        raise HTTPException(status_code=400, detail="Email and token required")

    if not verify_unsubscribe_token(email, token):
        raise HTTPException(status_code=400, detail="Invalid unsubscribe token")

    # Add to unsubscribe list
    existing = await db.execute(
        select(EmailUnsubscribe).where(EmailUnsubscribe.email == email.lower().strip())
    )
    if not existing.scalar_one_or_none():
        db.add(EmailUnsubscribe(
            email=email.lower().strip(),
            reason=reason or "user_unsubscribe",
        ))

    # Update all recipient records for this email
    await db.execute(
        text("""
            UPDATE email_recipients
            SET status = 'unsubscribed', unsubscribed_at = NOW()
            WHERE LOWER(email) = :email AND status != 'unsubscribed'
        """),
        {"email": email.lower().strip()},
    )

    # Increment unsubscribe count on affected campaigns
    await db.execute(
        text("""
            UPDATE email_campaigns
            SET unsubscribe_count = unsubscribe_count + 1
            WHERE id IN (
                SELECT DISTINCT campaign_id FROM email_recipients
                WHERE LOWER(email) = :email
            )
        """),
        {"email": email.lower().strip()},
    )

    await db.commit()

    # Return HTML confirmation
    return HTMLResponse(content="""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Unsubscribed - PermitLookup</title>
<style>body{margin:0;padding:0;background:#0f172a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;display:flex;justify-content:center;align-items:center;min-height:100vh;}
.card{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:40px;max-width:480px;text-align:center;}
h1{color:#e2e8f0;font-size:24px;margin:0 0 12px;}
p{color:#94a3b8;font-size:15px;line-height:1.6;}
.logo{font-size:28px;font-weight:800;color:#e2e8f0;margin-bottom:20px;}
.logo span{color:#6366f1;}
.check{font-size:48px;margin-bottom:16px;}
.subtle{color:#64748b;font-size:12px;margin-top:16px;}
</style></head>
<body><div class="card">
<div class="logo">Permit<span>Lookup</span></div>
<div class="check">&#10003;</div>
<h1>You've Been Unsubscribed</h1>
<p>You won't receive any more marketing emails from PermitLookup. If this was a mistake, you can always sign up again at <a href="https://permits.ecbtx.com" style="color:#6366f1;">permits.ecbtx.com</a>.</p>
<p class="subtle">PermitLookup | San Marcos, TX 78666</p>
</div></body></html>""")


@router.post("/sendgrid-events")
async def sendgrid_webhook(request: Request):
    """Process SendGrid event webhook (opens, clicks, bounces)."""
    try:
        events = await request.json()
        if not isinstance(events, list):
            events = [events]
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    async with async_session_maker() as db:
        result = await process_sendgrid_events(events, db)

    return result
