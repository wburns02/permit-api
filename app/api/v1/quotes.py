"""Quote/Estimate Builder endpoints — create, manage, and send quotes."""

import uuid
from datetime import datetime, timezone, date

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.quote import Quote
from app.models.crm import Contact
from app.config import settings

router = APIRouter(prefix="/quotes", tags=["Quotes"])


# ---------------------------------------------------------------------------
# Plan gating
# ---------------------------------------------------------------------------

def _require_paid(user: ApiUser):
    plan = resolve_plan(user.plan)
    if plan == PlanTier.FREE:
        raise HTTPException(
            status_code=403,
            detail="Quotes require Explorer plan or higher. Upgrade at /pricing",
        )


def _log_usage(user: ApiUser, request: Request, endpoint: str) -> UsageLog:
    return UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint=endpoint,
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class QuoteItem(BaseModel):
    description: str = Field(..., min_length=1, max_length=500)
    quantity: float = Field(..., gt=0)
    unit_price: float = Field(..., ge=0)
    total: float | None = None  # auto-calculated if omitted


class QuoteCreate(BaseModel):
    contact_id: uuid.UUID | None = None
    deal_id: uuid.UUID | None = None
    items: list[QuoteItem] = Field(..., min_length=1, max_length=50)
    notes: str | None = None
    terms: str | None = None
    tax_rate: float = Field(0.0, ge=0, le=1)
    valid_until: date | None = None
    company_name: str | None = Field(None, max_length=200)
    company_phone: str | None = Field(None, max_length=20)
    company_email: str | None = Field(None, max_length=200)


class QuoteUpdate(BaseModel):
    items: list[QuoteItem] | None = None
    status: str | None = Field(None, max_length=20)
    notes: str | None = None
    terms: str | None = None
    tax_rate: float | None = None
    valid_until: date | None = None
    company_name: str | None = None
    company_phone: str | None = None
    company_email: str | None = None


VALID_STATUSES = {"draft", "sent", "accepted", "declined"}


def _calc_totals(items: list[dict], tax_rate: float) -> tuple[list[dict], float, float, float]:
    """Calculate line totals, subtotal, tax, and grand total."""
    processed = []
    subtotal = 0.0
    for item in items:
        line_total = round(item["quantity"] * item["unit_price"], 2)
        processed.append({
            "description": item["description"],
            "quantity": item["quantity"],
            "unit_price": item["unit_price"],
            "total": line_total,
        })
        subtotal += line_total
    subtotal = round(subtotal, 2)
    tax_amount = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax_amount, 2)
    return processed, subtotal, tax_amount, total


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("")
async def create_quote(
    body: QuoteCreate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new quote/estimate."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    items_raw = [item.model_dump() for item in body.items]
    processed_items, subtotal, tax_amount, total = _calc_totals(items_raw, body.tax_rate)

    quote = Quote(
        user_id=user.id,
        contact_id=body.contact_id,
        deal_id=body.deal_id,
        items=processed_items,
        subtotal=subtotal,
        tax_rate=body.tax_rate,
        tax_amount=tax_amount,
        total=total,
        status="draft",
        valid_until=body.valid_until,
        notes=body.notes,
        terms=body.terms,
        company_name=body.company_name or user.company_name,
        company_phone=body.company_phone,
        company_email=body.company_email or user.email,
    )
    db.add(quote)
    db.add(_log_usage(user, request, "/v1/quotes"))
    await db.commit()
    await db.refresh(quote)

    return _quote_dict(quote)


@router.get("")
async def list_quotes(
    request: Request,
    status: str | None = Query(None, max_length=20),
    page: int = Query(1, ge=1, le=500),
    page_size: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List user's quotes."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    query = select(Quote).where(Quote.user_id == user.id)
    if status:
        if status not in VALID_STATUSES:
            raise HTTPException(status_code=400, detail=f"Invalid status. Must be: {', '.join(sorted(VALID_STATUSES))}")
        query = query.where(Quote.status == status)

    count_q = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_q)).scalar() or 0

    query = query.order_by(Quote.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(query)
    quotes = result.scalars().all()

    # Get contact names
    contact_ids = [q.contact_id for q in quotes if q.contact_id]
    contact_names = {}
    if contact_ids:
        cn_q = select(Contact.id, Contact.name, Contact.company).where(Contact.id.in_(contact_ids))
        cn_result = await db.execute(cn_q)
        contact_names = {r.id: {"name": r.name, "company": r.company} for r in cn_result.all()}

    items = []
    for q in quotes:
        d = _quote_dict(q)
        d["contact_name"] = contact_names.get(q.contact_id, {}).get("name") if q.contact_id else None
        d["contact_company"] = contact_names.get(q.contact_id, {}).get("company") if q.contact_id else None
        items.append(d)

    db.add(_log_usage(user, request, "/v1/quotes"))
    await db.commit()

    return {"results": items, "total": total, "page": page, "page_size": page_size}


@router.get("/{quote_id}")
async def get_quote(
    quote_id: uuid.UUID,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get quote detail."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    result = await db.execute(
        select(Quote).where(Quote.id == quote_id, Quote.user_id == user.id)
    )
    quote = result.scalar_one_or_none()
    if not quote:
        raise HTTPException(status_code=404, detail="Quote not found")

    # Get contact info
    contact = None
    if quote.contact_id:
        cr = await db.execute(select(Contact).where(Contact.id == quote.contact_id))
        contact = cr.scalar_one_or_none()

    d = _quote_dict(quote)
    if contact:
        d["contact_name"] = contact.name
        d["contact_company"] = contact.company
        d["contact_email"] = contact.email
        d["contact_phone"] = contact.phone

    db.add(_log_usage(user, request, f"/v1/quotes/{quote_id}"))
    await db.commit()

    return d


@router.put("/{quote_id}")
async def update_quote(
    quote_id: uuid.UUID,
    body: QuoteUpdate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a quote's items, status, or metadata."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    result = await db.execute(
        select(Quote).where(Quote.id == quote_id, Quote.user_id == user.id)
    )
    quote = result.scalar_one_or_none()
    if not quote:
        raise HTTPException(status_code=404, detail="Quote not found")

    update_data = body.model_dump(exclude_unset=True)

    if "status" in update_data:
        if update_data["status"] not in VALID_STATUSES:
            raise HTTPException(status_code=400, detail=f"Invalid status. Must be: {', '.join(sorted(VALID_STATUSES))}")
        if update_data["status"] == "accepted":
            quote.accepted_at = datetime.now(timezone.utc)

    if "items" in update_data and update_data["items"] is not None:
        items_raw = [item.model_dump() if hasattr(item, 'model_dump') else item for item in update_data["items"]]
        tax_rate = update_data.get("tax_rate", quote.tax_rate or 0.0)
        processed_items, subtotal, tax_amount, total = _calc_totals(items_raw, tax_rate)
        quote.items = processed_items
        quote.subtotal = subtotal
        quote.tax_amount = tax_amount
        quote.total = total
        # Remove items from update_data since we handled it
        del update_data["items"]
    elif "tax_rate" in update_data and update_data["tax_rate"] is not None:
        # Recalculate with new tax rate but existing items
        if quote.items:
            _, subtotal, tax_amount, total = _calc_totals(quote.items, update_data["tax_rate"])
            quote.subtotal = subtotal
            quote.tax_amount = tax_amount
            quote.total = total

    for field, value in update_data.items():
        if field != "items":
            setattr(quote, field, value)
    quote.updated_at = datetime.now(timezone.utc)

    db.add(_log_usage(user, request, f"/v1/quotes/{quote_id}"))
    await db.commit()
    await db.refresh(quote)

    return _quote_dict(quote)


@router.post("/{quote_id}/send")
async def send_quote(
    quote_id: uuid.UUID,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Email the quote to the associated contact."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    result = await db.execute(
        select(Quote).where(Quote.id == quote_id, Quote.user_id == user.id)
    )
    quote = result.scalar_one_or_none()
    if not quote:
        raise HTTPException(status_code=404, detail="Quote not found")

    if not quote.contact_id:
        raise HTTPException(status_code=400, detail="Quote has no contact assigned")

    # Get contact email
    cr = await db.execute(select(Contact).where(Contact.id == quote.contact_id))
    contact = cr.scalar_one_or_none()
    if not contact or not contact.email:
        raise HTTPException(status_code=400, detail="Contact has no email address")

    # Build and send email
    html = _build_quote_email_html(quote, contact)
    success = await _send_quote_email(contact.email, quote, html)

    if success:
        quote.status = "sent"
        quote.sent_at = datetime.now(timezone.utc)
        quote.updated_at = datetime.now(timezone.utc)
        await db.commit()
        await db.refresh(quote)

    db.add(_log_usage(user, request, f"/v1/quotes/{quote_id}/send"))
    await db.commit()

    return {
        "sent": success,
        "to": contact.email,
        "quote_id": str(quote.id),
        "status": quote.status,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _quote_dict(q: Quote) -> dict:
    return {
        "id": str(q.id),
        "user_id": str(q.user_id),
        "contact_id": str(q.contact_id) if q.contact_id else None,
        "deal_id": str(q.deal_id) if q.deal_id else None,
        "items": q.items or [],
        "subtotal": q.subtotal,
        "tax_rate": q.tax_rate,
        "tax_amount": q.tax_amount,
        "total": q.total,
        "status": q.status,
        "valid_until": q.valid_until.isoformat() if q.valid_until else None,
        "sent_at": q.sent_at.isoformat() if q.sent_at else None,
        "accepted_at": q.accepted_at.isoformat() if q.accepted_at else None,
        "notes": q.notes,
        "terms": q.terms,
        "company_name": q.company_name,
        "company_phone": q.company_phone,
        "company_email": q.company_email,
        "created_at": q.created_at.isoformat() if q.created_at else None,
        "updated_at": q.updated_at.isoformat() if q.updated_at else None,
    }


def _build_quote_email_html(quote: Quote, contact: Contact) -> str:
    """Build a simple HTML email for the quote."""
    items_rows = ""
    for item in (quote.items or []):
        items_rows += f"""<tr>
            <td style="padding:8px 12px;border-bottom:1px solid #2a2a3a;color:#e8e8f0">{item.get('description','')}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #2a2a3a;color:#a0a0b8;text-align:center">{item.get('quantity','')}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #2a2a3a;color:#a0a0b8;text-align:right">${item.get('unit_price',0):,.2f}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #2a2a3a;color:#e8e8f0;text-align:right;font-weight:600">${item.get('total',0):,.2f}</td>
        </tr>"""

    valid_str = f"Valid until: {quote.valid_until.strftime('%B %d, %Y')}" if quote.valid_until else ""

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#0a0a0f;font-family:-apple-system,BlinkMacSystemFont,'Inter',sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#0a0a0f;padding:20px 0">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%">

<tr><td style="padding:24px 32px;background:#12121a;border:1px solid #2a2a3a;border-radius:12px 12px 0 0">
  <h2 style="margin:0;color:#e8e8f0;font-size:22px">Quote / Estimate</h2>
  <div style="color:#6a6a80;font-size:13px;margin-top:4px">From: {quote.company_name or 'PermitLookup User'}</div>
  {f'<div style="color:#6a6a80;font-size:13px">{quote.company_phone or ""}</div>' if quote.company_phone else ''}
  {f'<div style="color:#6a6a80;font-size:13px">{quote.company_email or ""}</div>' if quote.company_email else ''}
</td></tr>

<tr><td style="padding:16px 32px;background:#12121a;border-left:1px solid #2a2a3a;border-right:1px solid #2a2a3a">
  <div style="color:#a0a0b8;font-size:13px">To: <strong style="color:#e8e8f0">{contact.name}</strong></div>
  {f'<div style="color:#a0a0b8;font-size:13px">{contact.company}</div>' if contact.company else ''}
</td></tr>

<tr><td style="padding:0 32px 16px;background:#12121a;border-left:1px solid #2a2a3a;border-right:1px solid #2a2a3a">
  <table width="100%" cellpadding="0" cellspacing="0" style="font-size:13px">
    <thead>
      <tr style="background:#1a1a25">
        <th style="padding:10px 12px;text-align:left;color:#6a6a80;font-size:11px;text-transform:uppercase;letter-spacing:.5px">Description</th>
        <th style="padding:10px 12px;text-align:center;color:#6a6a80;font-size:11px;text-transform:uppercase">Qty</th>
        <th style="padding:10px 12px;text-align:right;color:#6a6a80;font-size:11px;text-transform:uppercase">Unit Price</th>
        <th style="padding:10px 12px;text-align:right;color:#6a6a80;font-size:11px;text-transform:uppercase">Total</th>
      </tr>
    </thead>
    <tbody>{items_rows}</tbody>
  </table>

  <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:12px;font-size:14px">
    <tr><td style="padding:4px 12px;text-align:right;color:#a0a0b8">Subtotal:</td><td style="padding:4px 12px;text-align:right;color:#e8e8f0;width:100px">${quote.subtotal or 0:,.2f}</td></tr>
    {f'<tr><td style="padding:4px 12px;text-align:right;color:#a0a0b8">Tax ({quote.tax_rate*100:.1f}%):</td><td style="padding:4px 12px;text-align:right;color:#e8e8f0;width:100px">${quote.tax_amount or 0:,.2f}</td></tr>' if quote.tax_rate else ''}
    <tr><td style="padding:8px 12px;text-align:right;color:#e8e8f0;font-weight:700;font-size:18px;border-top:2px solid #2a2a3a">Total:</td><td style="padding:8px 12px;text-align:right;color:#22c55e;font-weight:800;font-size:18px;width:100px;border-top:2px solid #2a2a3a">${quote.total or 0:,.2f}</td></tr>
  </table>
</td></tr>

{f'<tr><td style="padding:12px 32px;background:#12121a;border-left:1px solid #2a2a3a;border-right:1px solid #2a2a3a"><div style="color:#a0a0b8;font-size:13px;line-height:1.5"><strong>Notes:</strong> {quote.notes}</div></td></tr>' if quote.notes else ''}
{f'<tr><td style="padding:12px 32px;background:#12121a;border-left:1px solid #2a2a3a;border-right:1px solid #2a2a3a"><div style="color:#6a6a80;font-size:12px;line-height:1.5"><strong>Terms:</strong> {quote.terms}</div></td></tr>' if quote.terms else ''}

<tr><td style="padding:16px 32px;background:#12121a;border:1px solid #2a2a3a;border-top:none;border-radius:0 0 12px 12px;text-align:center">
  <div style="color:#6a6a80;font-size:12px">{valid_str}</div>
  <div style="color:#6a6a80;font-size:11px;margin-top:8px">Sent via PermitLookup</div>
</td></tr>

</table>
</td></tr>
</table>
</body></html>"""


async def _send_quote_email(to: str, quote: Quote, html: str) -> bool:
    """Send quote email via SendGrid."""
    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Mail, Content
    except ImportError:
        return False

    if not settings.SENDGRID_API_KEY:
        return False

    try:
        company = quote.company_name or "PermitLookup"
        message = Mail(
            from_email=settings.SENDGRID_FROM_EMAIL,
            to_emails=to,
            subject=f"Quote from {company} — ${quote.total or 0:,.2f}",
        )
        message.content = [Content("text/html", html)]
        sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
        response = sg.send(message)
        return response.status_code in (200, 201, 202)
    except Exception:
        return False
