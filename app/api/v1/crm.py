"""CRM endpoints — contacts, deals, pipeline, commissions, leaderboard, teams, webhooks."""

import uuid
from datetime import datetime, timezone, timedelta, date

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select, func, text, or_, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.crm import Contact, Deal, Note, Commission, Activity, Webhook
from app.models.dialer import CallLog, LeadStatus
from app.models.team import Team, TeamMember
from app.services.webhook_delivery import deliver_webhook

router = APIRouter(prefix="/crm", tags=["CRM"])


# ---------------------------------------------------------------------------
# Plan gating — Explorer+ can use CRM
# ---------------------------------------------------------------------------

def _require_paid(user: ApiUser):
    plan = resolve_plan(user.plan)
    if plan == PlanTier.FREE:
        raise HTTPException(
            status_code=403,
            detail="CRM requires Explorer plan or higher. Upgrade at /pricing",
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
# Pydantic Schemas
# ---------------------------------------------------------------------------

class ContactCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=500)
    company: str | None = None
    phone: str | None = Field(None, max_length=20)
    email: str | None = Field(None, max_length=255)
    address: str | None = None
    city: str | None = Field(None, max_length=100)
    state: str | None = Field(None, max_length=2)
    zip: str | None = Field(None, max_length=10)
    lead_source: str = Field("permit", max_length=50)
    lead_id: uuid.UUID | None = None

class ContactUpdate(BaseModel):
    name: str | None = None
    company: str | None = None
    phone: str | None = None
    email: str | None = None
    address: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None
    lead_source: str | None = None
    tags: list | None = None

class FromLeadRequest(BaseModel):
    lead_id: uuid.UUID

class DealCreate(BaseModel):
    contact_id: uuid.UUID
    title: str | None = None
    stage: str = Field("new", max_length=50)
    value: float | None = None
    permit_number: str | None = Field(None, max_length=100)
    permit_type: str | None = Field(None, max_length=50)
    expected_close_date: date | None = None
    notes: str | None = None

class DealUpdate(BaseModel):
    title: str | None = None
    stage: str | None = None
    value: float | None = None
    expected_close_date: date | None = None
    actual_close_date: date | None = None
    lost_reason: str | None = None
    notes: str | None = None
    permit_number: str | None = None
    permit_type: str | None = None

class NoteCreate(BaseModel):
    contact_id: uuid.UUID | None = None
    deal_id: uuid.UUID | None = None
    content: str = Field(..., min_length=1)
    note_type: str = Field("note", max_length=20)


VALID_STAGES = {"new", "contacted", "quoted", "negotiating", "won", "lost"}
VALID_NOTE_TYPES = {"call", "email", "meeting", "task", "note", "system"}


# ---------------------------------------------------------------------------
# Contacts
# ---------------------------------------------------------------------------

@router.get("/contacts")
async def list_contacts(
    request: Request,
    q: str | None = Query(None, max_length=200),
    page: int = Query(1, ge=1, le=500),
    page_size: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List contacts with optional search across name/company/phone/email."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    query = select(Contact).where(Contact.user_id == user.id)

    if q:
        search = f"%{q}%"
        query = query.where(
            or_(
                Contact.name.ilike(search),
                Contact.company.ilike(search),
                Contact.phone.ilike(search),
                Contact.email.ilike(search),
            )
        )

    # Count
    count_q = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_q)).scalar() or 0

    # Fetch page
    query = query.order_by(Contact.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(query)
    contacts = result.scalars().all()

    # Get deal counts per contact
    contact_ids = [c.id for c in contacts]
    deal_counts = {}
    if contact_ids:
        dc_q = (
            select(Deal.contact_id, func.count().label("cnt"))
            .where(Deal.contact_id.in_(contact_ids), Deal.user_id == user.id)
            .group_by(Deal.contact_id)
        )
        dc_result = await db.execute(dc_q)
        deal_counts = {r.contact_id: r.cnt for r in dc_result.all()}

    items = [
        {
            "id": str(c.id),
            "name": c.name,
            "company": c.company,
            "phone": c.phone,
            "email": c.email,
            "address": c.address,
            "city": c.city,
            "state": c.state,
            "zip": c.zip,
            "lead_source": c.lead_source,
            "lead_id": str(c.lead_id) if c.lead_id else None,
            "tags": c.tags,
            "deals": deal_counts.get(c.id, 0),
            "created_at": c.created_at.isoformat() if c.created_at else None,
            "updated_at": c.updated_at.isoformat() if c.updated_at else None,
        }
        for c in contacts
    ]

    db.add(_log_usage(user, request, "/v1/crm/contacts"))
    await db.commit()

    return {"results": items, "total": total, "page": page, "page_size": page_size}


@router.post("/contacts")
async def create_contact(
    body: ContactCreate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new contact."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    contact = Contact(
        user_id=user.id,
        name=body.name,
        company=body.company,
        phone=body.phone,
        email=body.email,
        address=body.address,
        city=body.city,
        state=body.state,
        zip=body.zip,
        lead_source=body.lead_source,
        lead_id=body.lead_id,
    )
    db.add(contact)
    db.add(_log_usage(user, request, "/v1/crm/contacts"))
    await db.flush()

    # Auto-log activity
    team_id = await _get_user_team_id(db, user.id)
    await log_activity(
        db, user.id, team_id,
        "contact_created",
        f"Created contact: {body.name}" + (f" ({body.company})" if body.company else ""),
        entity_type="contact",
        entity_id=contact.id,
    )

    await db.commit()
    await db.refresh(contact)

    return {
        "id": str(contact.id),
        "name": contact.name,
        "company": contact.company,
        "phone": contact.phone,
        "email": contact.email,
        "lead_source": contact.lead_source,
        "created_at": contact.created_at.isoformat() if contact.created_at else None,
    }


@router.get("/contacts/{contact_id}")
async def get_contact(
    contact_id: uuid.UUID,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get contact detail with related deals, notes, and call history."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    result = await db.execute(
        select(Contact).where(Contact.id == contact_id, Contact.user_id == user.id)
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    # Get deals
    deals_q = select(Deal).where(Deal.contact_id == contact_id, Deal.user_id == user.id).order_by(Deal.created_at.desc())
    deals_result = await db.execute(deals_q)
    deals = [
        {
            "id": str(d.id),
            "title": d.title,
            "stage": d.stage,
            "value": d.value,
            "permit_number": d.permit_number,
            "permit_type": d.permit_type,
            "expected_close_date": d.expected_close_date.isoformat() if d.expected_close_date else None,
            "actual_close_date": d.actual_close_date.isoformat() if d.actual_close_date else None,
            "created_at": d.created_at.isoformat() if d.created_at else None,
        }
        for d in deals_result.scalars().all()
    ]

    # Get notes
    notes_q = select(Note).where(Note.contact_id == contact_id, Note.user_id == user.id).order_by(Note.created_at.desc())
    notes_result = await db.execute(notes_q)
    notes = [
        {
            "id": str(n.id),
            "content": n.content,
            "note_type": n.note_type,
            "deal_id": str(n.deal_id) if n.deal_id else None,
            "created_at": n.created_at.isoformat() if n.created_at else None,
        }
        for n in notes_result.scalars().all()
    ]

    # Get call history from call_logs if contact has a lead_id
    call_history = []
    if contact.lead_id:
        calls_q = text("""
            SELECT id, phone_number, duration_seconds, disposition, notes, ai_summary, created_at
            FROM call_logs
            WHERE user_id = :user_id AND lead_id = :lead_id
            ORDER BY created_at DESC
            LIMIT 20
        """)
        calls_result = await db.execute(calls_q, {"user_id": user.id, "lead_id": contact.lead_id})
        call_history = [
            {
                "id": str(r["id"]),
                "phone_number": r["phone_number"],
                "duration_seconds": r["duration_seconds"],
                "disposition": r["disposition"],
                "notes": r["notes"],
                "ai_summary": r["ai_summary"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in calls_result.mappings().all()
        ]

    db.add(_log_usage(user, request, "/v1/crm/contacts/" + str(contact_id)))
    await db.commit()

    return {
        "id": str(contact.id),
        "name": contact.name,
        "company": contact.company,
        "phone": contact.phone,
        "email": contact.email,
        "address": contact.address,
        "city": contact.city,
        "state": contact.state,
        "zip": contact.zip,
        "lead_source": contact.lead_source,
        "lead_id": str(contact.lead_id) if contact.lead_id else None,
        "tags": contact.tags,
        "created_at": contact.created_at.isoformat() if contact.created_at else None,
        "updated_at": contact.updated_at.isoformat() if contact.updated_at else None,
        "deals": deals,
        "notes": notes,
        "call_history": call_history,
    }


@router.put("/contacts/{contact_id}")
async def update_contact(
    contact_id: uuid.UUID,
    body: ContactUpdate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a contact's fields."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    result = await db.execute(
        select(Contact).where(Contact.id == contact_id, Contact.user_id == user.id)
    )
    contact = result.scalar_one_or_none()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    update_data = body.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(contact, field, value)
    contact.updated_at = datetime.now(timezone.utc)

    db.add(_log_usage(user, request, "/v1/crm/contacts/" + str(contact_id)))
    await db.commit()
    await db.refresh(contact)

    return {
        "id": str(contact.id),
        "name": contact.name,
        "company": contact.company,
        "phone": contact.phone,
        "email": contact.email,
        "updated_at": contact.updated_at.isoformat() if contact.updated_at else None,
    }


@router.post("/contacts/from-lead")
async def create_contact_from_lead(
    body: FromLeadRequest,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a contact from a hot_lead record. Auto-fills fields from lead data."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    # Check if contact already exists for this lead
    existing = await db.execute(
        select(Contact).where(Contact.user_id == user.id, Contact.lead_id == body.lead_id)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Contact already exists for this lead")

    # Fetch lead data from hot_leads
    lead_q = text("""
        SELECT id, contractor_company, contractor_name, contractor_phone,
               applicant_name, applicant_phone, owner_name,
               address, city, state, zip, permit_number, permit_type
        FROM hot_leads WHERE id = :lead_id
    """)
    result = await db.execute(lead_q, {"lead_id": body.lead_id})
    lead = result.mappings().one_or_none()
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")

    # Auto-fill: prefer contractor_company, fall back to applicant_name
    name = lead["contractor_company"] or lead["applicant_name"] or lead["owner_name"] or "Unknown"
    phone = lead["contractor_phone"] or lead["applicant_phone"]

    contact = Contact(
        user_id=user.id,
        name=name,
        company=lead["contractor_company"],
        phone=phone,
        address=lead["address"],
        city=lead["city"],
        state=lead["state"],
        zip=lead["zip"],
        lead_source="permit",
        lead_id=body.lead_id,
    )
    db.add(contact)
    db.add(_log_usage(user, request, "/v1/crm/contacts/from-lead"))
    await db.commit()
    await db.refresh(contact)

    return {
        "id": str(contact.id),
        "name": contact.name,
        "company": contact.company,
        "phone": contact.phone,
        "address": contact.address,
        "city": contact.city,
        "state": contact.state,
        "zip": contact.zip,
        "lead_source": contact.lead_source,
        "lead_id": str(contact.lead_id),
        "created_at": contact.created_at.isoformat() if contact.created_at else None,
    }


# ---------------------------------------------------------------------------
# Deals
# ---------------------------------------------------------------------------

@router.get("/deals")
async def list_deals(
    request: Request,
    stage: str | None = Query(None, max_length=50),
    page: int = Query(1, ge=1, le=500),
    page_size: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List deals with optional stage filter, sorted by value DESC."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    query = select(Deal).where(Deal.user_id == user.id)
    if stage:
        if stage not in VALID_STAGES:
            raise HTTPException(status_code=400, detail=f"Invalid stage. Must be one of: {', '.join(sorted(VALID_STAGES))}")
        query = query.where(Deal.stage == stage)

    count_q = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_q)).scalar() or 0

    query = query.order_by(Deal.value.desc().nullslast()).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(query)
    deals = result.scalars().all()

    # Fetch contact names for these deals
    contact_ids = [d.contact_id for d in deals if d.contact_id]
    contact_names = {}
    if contact_ids:
        cn_q = select(Contact.id, Contact.name, Contact.company).where(Contact.id.in_(contact_ids))
        cn_result = await db.execute(cn_q)
        contact_names = {r.id: {"name": r.name, "company": r.company} for r in cn_result.all()}

    items = [
        {
            "id": str(d.id),
            "contact_id": str(d.contact_id) if d.contact_id else None,
            "contact_name": contact_names.get(d.contact_id, {}).get("name") if d.contact_id else None,
            "contact_company": contact_names.get(d.contact_id, {}).get("company") if d.contact_id else None,
            "title": d.title,
            "stage": d.stage,
            "value": d.value,
            "permit_number": d.permit_number,
            "permit_type": d.permit_type,
            "expected_close_date": d.expected_close_date.isoformat() if d.expected_close_date else None,
            "actual_close_date": d.actual_close_date.isoformat() if d.actual_close_date else None,
            "lost_reason": d.lost_reason,
            "notes": d.notes,
            "created_at": d.created_at.isoformat() if d.created_at else None,
        }
        for d in deals
    ]

    db.add(_log_usage(user, request, "/v1/crm/deals"))
    await db.commit()

    return {"results": items, "total": total, "page": page, "page_size": page_size}


@router.post("/deals")
async def create_deal(
    body: DealCreate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new deal linked to a contact."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    if body.stage not in VALID_STAGES:
        raise HTTPException(status_code=400, detail=f"Invalid stage. Must be one of: {', '.join(sorted(VALID_STAGES))}")

    # Verify contact exists and belongs to user
    contact_result = await db.execute(
        select(Contact).where(Contact.id == body.contact_id, Contact.user_id == user.id)
    )
    if not contact_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Contact not found")

    deal = Deal(
        user_id=user.id,
        contact_id=body.contact_id,
        title=body.title,
        stage=body.stage,
        value=body.value,
        permit_number=body.permit_number,
        permit_type=body.permit_type,
        expected_close_date=body.expected_close_date,
        notes=body.notes,
    )
    db.add(deal)

    # If stage is "won" at creation, auto-create commission
    if body.stage == "won" and body.value and body.value > 0:
        deal.actual_close_date = date.today()
        commission = Commission(
            user_id=user.id,
            deal_id=deal.id,
            amount=round(body.value * 0.10, 2),
            rate=0.10,
            status="pending",
        )
        db.add(commission)

    db.add(_log_usage(user, request, "/v1/crm/deals"))
    await db.flush()

    # Auto-log activity
    team_id = await _get_user_team_id(db, user.id)
    val_str = f" (${body.value:,.0f})" if body.value else ""
    await log_activity(
        db, user.id, team_id,
        "deal_created",
        f"Created deal: {body.title or 'Untitled'}{val_str} at stage {body.stage}",
        entity_type="deal",
        entity_id=deal.id,
    )

    await db.commit()
    await db.refresh(deal)

    return {
        "id": str(deal.id),
        "contact_id": str(deal.contact_id),
        "title": deal.title,
        "stage": deal.stage,
        "value": deal.value,
        "created_at": deal.created_at.isoformat() if deal.created_at else None,
    }


@router.put("/deals/{deal_id}")
async def update_deal(
    deal_id: uuid.UUID,
    body: DealUpdate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a deal. When stage changes to 'won', auto-creates a commission."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    result = await db.execute(
        select(Deal).where(Deal.id == deal_id, Deal.user_id == user.id)
    )
    deal = result.scalar_one_or_none()
    if not deal:
        raise HTTPException(status_code=404, detail="Deal not found")

    old_stage = deal.stage
    update_data = body.model_dump(exclude_unset=True)

    if "stage" in update_data and update_data["stage"] not in VALID_STAGES:
        raise HTTPException(status_code=400, detail=f"Invalid stage. Must be one of: {', '.join(sorted(VALID_STAGES))}")

    for field, value in update_data.items():
        setattr(deal, field, value)
    deal.updated_at = datetime.now(timezone.utc)

    # Auto-create commission and schedule review request when stage changes to "won"
    new_stage = update_data.get("stage")
    if new_stage == "won" and old_stage != "won":
        deal.actual_close_date = deal.actual_close_date or date.today()
        if deal.value and deal.value > 0:
            # Check if commission already exists
            existing_comm = await db.execute(
                select(Commission).where(Commission.deal_id == deal_id, Commission.user_id == user.id)
            )
            if not existing_comm.scalar_one_or_none():
                commission = Commission(
                    user_id=user.id,
                    deal_id=deal.id,
                    amount=round(deal.value * 0.10, 2),
                    rate=0.10,
                    status="pending",
                )
                db.add(commission)
        # Add system note about scheduled review request (7 days from now)
        review_date = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%b %d, %Y")
        review_note = Note(
            user_id=user.id,
            deal_id=deal.id,
            contact_id=deal.contact_id,
            content=f"Review request email scheduled for {review_date} (7 days after close).",
            note_type="system",
        )
        db.add(review_note)

    db.add(_log_usage(user, request, "/v1/crm/deals/" + str(deal_id)))

    # Auto-log activity for stage changes
    new_stage = update_data.get("stage")
    if new_stage and new_stage != old_stage:
        team_id = await _get_user_team_id(db, user.id)
        val_str = f" (${deal.value:,.0f})" if deal.value else ""
        await log_activity(
            db, user.id, team_id,
            "deal_stage_changed",
            f"Moved '{deal.title or 'Untitled'}' from {old_stage} to {new_stage}{val_str}",
            entity_type="deal",
            entity_id=deal.id,
        )

    await db.commit()
    await db.refresh(deal)

    return {
        "id": str(deal.id),
        "title": deal.title,
        "stage": deal.stage,
        "value": deal.value,
        "actual_close_date": deal.actual_close_date.isoformat() if deal.actual_close_date else None,
        "updated_at": deal.updated_at.isoformat() if deal.updated_at else None,
    }


# ---------------------------------------------------------------------------
# Notes
# ---------------------------------------------------------------------------

@router.post("/notes")
async def create_note(
    body: NoteCreate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a note linked to a contact and/or deal."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    if not body.contact_id and not body.deal_id:
        raise HTTPException(status_code=400, detail="Either contact_id or deal_id is required")

    if body.note_type not in VALID_NOTE_TYPES:
        raise HTTPException(status_code=400, detail=f"Invalid note_type. Must be one of: {', '.join(sorted(VALID_NOTE_TYPES))}")

    note = Note(
        user_id=user.id,
        contact_id=body.contact_id,
        deal_id=body.deal_id,
        content=body.content,
        note_type=body.note_type,
    )
    db.add(note)
    db.add(_log_usage(user, request, "/v1/crm/notes"))
    await db.flush()

    # Auto-log activity
    team_id = await _get_user_team_id(db, user.id)
    entity_type = "contact" if body.contact_id else "deal"
    entity_id = body.contact_id or body.deal_id
    preview = body.content[:80] + "..." if len(body.content) > 80 else body.content
    await log_activity(
        db, user.id, team_id,
        "note_added",
        f"Added {body.note_type} note: {preview}",
        entity_type=entity_type,
        entity_id=entity_id,
    )

    await db.commit()
    await db.refresh(note)

    return {
        "id": str(note.id),
        "contact_id": str(note.contact_id) if note.contact_id else None,
        "deal_id": str(note.deal_id) if note.deal_id else None,
        "content": note.content,
        "note_type": note.note_type,
        "created_at": note.created_at.isoformat() if note.created_at else None,
    }


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

@router.get("/pipeline")
async def get_pipeline(
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Pipeline summary: count and total value per stage."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    q = (
        select(
            Deal.stage,
            func.count().label("count"),
            func.coalesce(func.sum(Deal.value), 0).label("total_value"),
        )
        .where(Deal.user_id == user.id)
        .group_by(Deal.stage)
    )
    result = await db.execute(q)
    stages_data = {r.stage: {"count": r.count, "total_value": float(r.total_value)} for r in result.all()}

    # Ensure all stages are present
    stages = []
    for s in ["new", "contacted", "quoted", "negotiating", "won", "lost"]:
        data = stages_data.get(s, {"count": 0, "total_value": 0.0})
        stages.append({"stage": s, "count": data["count"], "total_value": data["total_value"]})

    db.add(_log_usage(user, request, "/v1/crm/pipeline"))
    await db.commit()

    return {"stages": stages}


@router.get("/dashboard")
async def get_dashboard(
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Dashboard: pipeline summary, this week's activity, and top deals."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    now = datetime.now(timezone.utc)
    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)

    # Pipeline summary
    pipeline_q = (
        select(
            Deal.stage,
            func.count().label("count"),
            func.coalesce(func.sum(Deal.value), 0).label("total_value"),
        )
        .where(Deal.user_id == user.id)
        .group_by(Deal.stage)
    )
    pipeline_result = await db.execute(pipeline_q)
    stages_data = {r.stage: {"count": r.count, "total_value": float(r.total_value)} for r in pipeline_result.all()}

    pipeline = []
    for s in ["new", "contacted", "quoted", "negotiating", "won", "lost"]:
        data = stages_data.get(s, {"count": 0, "total_value": 0.0})
        pipeline.append({"stage": s, "count": data["count"], "total_value": data["total_value"]})

    # This week stats
    new_contacts_q = select(func.count()).select_from(Contact).where(
        Contact.user_id == user.id, Contact.created_at >= week_start
    )
    new_contacts = (await db.execute(new_contacts_q)).scalar() or 0

    calls_made_q = select(func.count()).select_from(CallLog).where(
        CallLog.user_id == user.id, CallLog.created_at >= week_start
    )
    calls_made = (await db.execute(calls_made_q)).scalar() or 0

    deals_won_q = select(func.count()).select_from(Deal).where(
        Deal.user_id == user.id, Deal.stage == "won", Deal.updated_at >= week_start
    )
    deals_won = (await db.execute(deals_won_q)).scalar() or 0

    revenue_q = select(func.coalesce(func.sum(Deal.value), 0)).where(
        Deal.user_id == user.id, Deal.stage == "won", Deal.updated_at >= week_start
    )
    revenue = float((await db.execute(revenue_q)).scalar() or 0)

    # Top deals (active, by value)
    top_deals_q = (
        select(Deal)
        .where(Deal.user_id == user.id, Deal.stage.notin_(["won", "lost"]))
        .order_by(Deal.value.desc().nullslast())
        .limit(5)
    )
    top_result = await db.execute(top_deals_q)
    top_deals_raw = top_result.scalars().all()

    # Get contact names for top deals
    td_contact_ids = [d.contact_id for d in top_deals_raw if d.contact_id]
    td_contact_names = {}
    if td_contact_ids:
        cn_q = select(Contact.id, Contact.name).where(Contact.id.in_(td_contact_ids))
        cn_result = await db.execute(cn_q)
        td_contact_names = {r.id: r.name for r in cn_result.all()}

    top_deals = [
        {
            "id": str(d.id),
            "title": d.title,
            "stage": d.stage,
            "value": d.value,
            "contact_name": td_contact_names.get(d.contact_id) if d.contact_id else None,
            "created_at": d.created_at.isoformat() if d.created_at else None,
        }
        for d in top_deals_raw
    ]

    # Overall stats for stat cards
    total_contacts_q = select(func.count()).select_from(Contact).where(Contact.user_id == user.id)
    total_contacts = (await db.execute(total_contacts_q)).scalar() or 0

    active_deals_q = select(func.count()).select_from(Deal).where(
        Deal.user_id == user.id, Deal.stage.notin_(["won", "lost"])
    )
    active_deals = (await db.execute(active_deals_q)).scalar() or 0

    pipeline_value_q = select(func.coalesce(func.sum(Deal.value), 0)).where(
        Deal.user_id == user.id, Deal.stage.notin_(["won", "lost"])
    )
    pipeline_value = float((await db.execute(pipeline_value_q)).scalar() or 0)

    total_won_q = select(func.count()).select_from(Deal).where(
        Deal.user_id == user.id, Deal.stage == "won"
    )
    total_won = (await db.execute(total_won_q)).scalar() or 0

    total_revenue_q = select(func.coalesce(func.sum(Deal.value), 0)).where(
        Deal.user_id == user.id, Deal.stage == "won"
    )
    total_revenue = float((await db.execute(total_revenue_q)).scalar() or 0)

    total_deals_q = select(func.count()).select_from(Deal).where(Deal.user_id == user.id)
    total_deals = (await db.execute(total_deals_q)).scalar() or 0
    conversion_rate = round((total_won / total_deals * 100), 1) if total_deals > 0 else 0.0

    db.add(_log_usage(user, request, "/v1/crm/dashboard"))
    await db.commit()

    return {
        "pipeline": pipeline,
        "this_week": {
            "new_contacts": new_contacts,
            "calls_made": calls_made,
            "deals_won": deals_won,
            "revenue": revenue,
        },
        "top_deals": top_deals,
        "stats": {
            "total_contacts": total_contacts,
            "active_deals": active_deals,
            "pipeline_value": pipeline_value,
            "total_won": total_won,
            "total_revenue": total_revenue,
            "conversion_rate": conversion_rate,
        },
    }


@router.get("/leaderboard")
async def get_leaderboard(
    request: Request,
    period: str = Query("week", description="week or month"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Leaderboard: rank users by calls, deals, and revenue for the given period."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    now = datetime.now(timezone.utc)
    if period == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)

    # Calls per user
    calls_q = text("""
        SELECT u.id as user_id, u.email, u.company_name,
               COUNT(cl.id) as calls
        FROM api_users u
        LEFT JOIN call_logs cl ON cl.user_id = u.id AND cl.created_at >= :start
        GROUP BY u.id, u.email, u.company_name
        HAVING COUNT(cl.id) > 0
        ORDER BY calls DESC
        LIMIT 20
    """)
    calls_result = await db.execute(calls_q, {"start": start})
    calls_data = {str(r["user_id"]): {"email": r["email"], "company": r["company_name"], "calls": r["calls"]} for r in calls_result.mappings().all()}

    # Deals won + revenue per user
    deals_q = text("""
        SELECT u.id as user_id, u.email, u.company_name,
               COUNT(d.id) as deals_won,
               COALESCE(SUM(d.value), 0) as revenue
        FROM api_users u
        LEFT JOIN deals d ON d.user_id = u.id AND d.stage = 'won' AND d.updated_at >= :start
        GROUP BY u.id, u.email, u.company_name
        HAVING COUNT(d.id) > 0
        ORDER BY revenue DESC
        LIMIT 20
    """)
    deals_result = await db.execute(deals_q, {"start": start})
    deals_data = {str(r["user_id"]): {"email": r["email"], "company": r["company_name"], "deals_won": r["deals_won"], "revenue": float(r["revenue"])} for r in deals_result.mappings().all()}

    # Merge
    all_user_ids = set(list(calls_data.keys()) + list(deals_data.keys()))
    leaderboard = []
    for uid in all_user_ids:
        c = calls_data.get(uid, {})
        d = deals_data.get(uid, {})
        leaderboard.append({
            "user_id": uid,
            "email": c.get("email") or d.get("email"),
            "company": c.get("company") or d.get("company"),
            "calls": c.get("calls", 0),
            "deals_won": d.get("deals_won", 0),
            "revenue": d.get("revenue", 0.0),
        })

    leaderboard.sort(key=lambda x: (x["revenue"], x["calls"]), reverse=True)

    db.add(_log_usage(user, request, "/v1/crm/leaderboard"))
    await db.commit()

    return {"period": period, "leaderboard": leaderboard[:20]}


# ---------------------------------------------------------------------------
# Commissions
# ---------------------------------------------------------------------------

@router.get("/commissions")
async def list_commissions(
    request: Request,
    page: int = Query(1, ge=1, le=500),
    page_size: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List user's commissions."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    query = select(Commission).where(Commission.user_id == user.id).order_by(Commission.created_at.desc())

    count_q = select(func.count()).select_from(query.subquery())
    total = (await db.execute(count_q)).scalar() or 0

    query = query.offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(query)
    commissions = result.scalars().all()

    # Get deal info
    deal_ids = [c.deal_id for c in commissions if c.deal_id]
    deal_info = {}
    if deal_ids:
        di_q = select(Deal.id, Deal.title, Deal.value, Deal.stage).where(Deal.id.in_(deal_ids))
        di_result = await db.execute(di_q)
        deal_info = {r.id: {"title": r.title, "value": r.value, "stage": r.stage} for r in di_result.all()}

    items = [
        {
            "id": str(c.id),
            "deal_id": str(c.deal_id) if c.deal_id else None,
            "deal_title": deal_info.get(c.deal_id, {}).get("title") if c.deal_id else None,
            "deal_value": deal_info.get(c.deal_id, {}).get("value") if c.deal_id else None,
            "amount": c.amount,
            "rate": c.rate,
            "status": c.status,
            "created_at": c.created_at.isoformat() if c.created_at else None,
        }
        for c in commissions
    ]

    db.add(_log_usage(user, request, "/v1/crm/commissions"))
    await db.commit()

    return {"results": items, "total": total, "page": page, "page_size": page_size}


@router.get("/commissions/summary")
async def commission_summary(
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Commission summary: total earned, pending, this month."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    # Total earned (paid)
    total_earned_q = select(func.coalesce(func.sum(Commission.amount), 0)).where(
        Commission.user_id == user.id, Commission.status == "paid"
    )
    total_earned = float((await db.execute(total_earned_q)).scalar() or 0)

    # Total pending
    total_pending_q = select(func.coalesce(func.sum(Commission.amount), 0)).where(
        Commission.user_id == user.id, Commission.status == "pending"
    )
    total_pending = float((await db.execute(total_pending_q)).scalar() or 0)

    # This month
    now = datetime.now(timezone.utc)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    this_month_q = select(func.coalesce(func.sum(Commission.amount), 0)).where(
        Commission.user_id == user.id, Commission.created_at >= month_start
    )
    this_month = float((await db.execute(this_month_q)).scalar() or 0)

    db.add(_log_usage(user, request, "/v1/crm/commissions/summary"))
    await db.commit()

    return {
        "total_earned": total_earned,
        "total_pending": total_pending,
        "this_month": this_month,
    }


# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------

class TeamCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)


class TeamMemberAdd(BaseModel):
    email: str = Field(..., max_length=255)
    role: str = Field("member", max_length=20)


class TeamMemberUpdate(BaseModel):
    role: str | None = None
    territories: list[str] | None = None


VALID_TEAM_ROLES = {"owner", "manager", "member"}


@router.post("/teams")
async def create_team(
    body: TeamCreate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new team. The creator becomes the owner."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    team = Team(name=body.name, owner_id=user.id)
    db.add(team)
    await db.flush()

    # Add creator as owner member
    member = TeamMember(team_id=team.id, user_id=user.id, role="owner")
    db.add(member)
    db.add(_log_usage(user, request, "/v1/crm/teams"))
    await db.commit()
    await db.refresh(team)

    return {
        "id": str(team.id),
        "name": team.name,
        "owner_id": str(team.owner_id),
        "created_at": team.created_at.isoformat() if team.created_at else None,
    }


@router.get("/teams")
async def list_teams(
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List teams the user belongs to."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    # Find teams where user is a member
    query = (
        select(Team, TeamMember.role)
        .join(TeamMember, TeamMember.team_id == Team.id)
        .where(TeamMember.user_id == user.id)
        .order_by(Team.created_at.desc())
    )
    result = await db.execute(query)
    teams = result.all()

    # Get member counts
    team_ids = [t[0].id for t in teams]
    member_counts = {}
    if team_ids:
        mc_q = (
            select(TeamMember.team_id, func.count().label("cnt"))
            .where(TeamMember.team_id.in_(team_ids))
            .group_by(TeamMember.team_id)
        )
        mc_result = await db.execute(mc_q)
        member_counts = {r.team_id: r.cnt for r in mc_result.all()}

    items = [
        {
            "id": str(t.id),
            "name": t.name,
            "owner_id": str(t.owner_id),
            "your_role": role,
            "member_count": member_counts.get(t.id, 0),
            "created_at": t.created_at.isoformat() if t.created_at else None,
        }
        for t, role in teams
    ]

    db.add(_log_usage(user, request, "/v1/crm/teams"))
    await db.commit()

    return {"results": items}


@router.post("/teams/{team_id}/members")
async def add_team_member(
    team_id: uuid.UUID,
    body: TeamMemberAdd,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Add a member to a team by email. Only owner/manager can add."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    # Verify team exists and user has permission
    team_q = await db.execute(select(Team).where(Team.id == team_id))
    team = team_q.scalar_one_or_none()
    if not team:
        raise HTTPException(status_code=404, detail="Team not found")

    # Check caller's role
    caller_member = await db.execute(
        select(TeamMember).where(TeamMember.team_id == team_id, TeamMember.user_id == user.id)
    )
    caller = caller_member.scalar_one_or_none()
    if not caller or caller.role not in ("owner", "manager"):
        raise HTTPException(status_code=403, detail="Only team owners and managers can add members")

    if body.role not in VALID_TEAM_ROLES:
        raise HTTPException(status_code=400, detail=f"Invalid role. Must be: {', '.join(sorted(VALID_TEAM_ROLES))}")

    # Find user by email
    target_q = await db.execute(select(ApiUser).where(ApiUser.email == body.email))
    target_user = target_q.scalar_one_or_none()
    if not target_user:
        raise HTTPException(status_code=404, detail="No user found with that email")

    # Check if already a member
    existing_q = await db.execute(
        select(TeamMember).where(TeamMember.team_id == team_id, TeamMember.user_id == target_user.id)
    )
    if existing_q.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="User is already a team member")

    member = TeamMember(team_id=team_id, user_id=target_user.id, role=body.role)
    db.add(member)
    db.add(_log_usage(user, request, f"/v1/crm/teams/{team_id}/members"))
    await db.commit()
    await db.refresh(member)

    return {
        "id": str(member.id),
        "team_id": str(member.team_id),
        "user_id": str(member.user_id),
        "email": target_user.email,
        "role": member.role,
        "territories": member.territories,
    }


@router.get("/teams/{team_id}/members")
async def list_team_members(
    team_id: uuid.UUID,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List members of a team."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    # Verify user is a member of this team
    check_q = await db.execute(
        select(TeamMember).where(TeamMember.team_id == team_id, TeamMember.user_id == user.id)
    )
    if not check_q.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Not a member of this team")

    query = (
        select(TeamMember, ApiUser.email, ApiUser.company_name)
        .join(ApiUser, ApiUser.id == TeamMember.user_id)
        .where(TeamMember.team_id == team_id)
        .order_by(TeamMember.role)
    )
    result = await db.execute(query)
    members = result.all()

    items = [
        {
            "id": str(m.id),
            "user_id": str(m.user_id),
            "email": email,
            "company_name": company_name,
            "role": m.role,
            "territories": m.territories or [],
        }
        for m, email, company_name in members
    ]

    db.add(_log_usage(user, request, f"/v1/crm/teams/{team_id}/members"))
    await db.commit()

    return {"results": items}


@router.put("/teams/{team_id}/members/{member_id}")
async def update_team_member(
    team_id: uuid.UUID,
    member_id: uuid.UUID,
    body: TeamMemberUpdate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a team member's role or territories. Only owner/manager can update."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    # Verify caller's role
    caller_q = await db.execute(
        select(TeamMember).where(TeamMember.team_id == team_id, TeamMember.user_id == user.id)
    )
    caller = caller_q.scalar_one_or_none()
    if not caller or caller.role not in ("owner", "manager"):
        raise HTTPException(status_code=403, detail="Only team owners and managers can update members")

    # Find the target member
    target_q = await db.execute(
        select(TeamMember).where(TeamMember.id == member_id, TeamMember.team_id == team_id)
    )
    member = target_q.scalar_one_or_none()
    if not member:
        raise HTTPException(status_code=404, detail="Team member not found")

    update_data = body.model_dump(exclude_unset=True)
    if "role" in update_data:
        if update_data["role"] not in VALID_TEAM_ROLES:
            raise HTTPException(status_code=400, detail=f"Invalid role. Must be: {', '.join(sorted(VALID_TEAM_ROLES))}")
        member.role = update_data["role"]
    if "territories" in update_data:
        member.territories = update_data["territories"]

    db.add(_log_usage(user, request, f"/v1/crm/teams/{team_id}/members/{member_id}"))
    await db.commit()
    await db.refresh(member)

    return {
        "id": str(member.id),
        "user_id": str(member.user_id),
        "role": member.role,
        "territories": member.territories or [],
    }


@router.get("/teams/{team_id}/dashboard")
async def team_dashboard(
    team_id: uuid.UUID,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Team aggregate dashboard — combined stats for all members."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    # Verify user is a member
    check_q = await db.execute(
        select(TeamMember).where(TeamMember.team_id == team_id, TeamMember.user_id == user.id)
    )
    if not check_q.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Not a member of this team")

    # Get all member user_ids
    members_q = await db.execute(
        select(TeamMember.user_id).where(TeamMember.team_id == team_id)
    )
    member_ids = [r[0] for r in members_q.all()]

    now = datetime.now(timezone.utc)
    week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)

    # Team pipeline
    pipeline_q = (
        select(
            Deal.stage,
            func.count().label("count"),
            func.coalesce(func.sum(Deal.value), 0).label("total_value"),
        )
        .where(Deal.user_id.in_(member_ids))
        .group_by(Deal.stage)
    )
    pipeline_result = await db.execute(pipeline_q)
    stages_data = {r.stage: {"count": r.count, "total_value": float(r.total_value)} for r in pipeline_result.all()}

    pipeline = []
    for s in ["new", "contacted", "quoted", "negotiating", "won", "lost"]:
        data = stages_data.get(s, {"count": 0, "total_value": 0.0})
        pipeline.append({"stage": s, "count": data["count"], "total_value": data["total_value"]})

    # This week team stats
    team_calls_q = select(func.count()).select_from(CallLog).where(
        CallLog.user_id.in_(member_ids), CallLog.created_at >= week_start
    )
    team_calls = (await db.execute(team_calls_q)).scalar() or 0

    team_contacts_q = select(func.count()).select_from(Contact).where(
        Contact.user_id.in_(member_ids), Contact.created_at >= week_start
    )
    team_contacts = (await db.execute(team_contacts_q)).scalar() or 0

    team_won_q = select(func.count(), func.coalesce(func.sum(Deal.value), 0)).where(
        Deal.user_id.in_(member_ids), Deal.stage == "won", Deal.updated_at >= week_start
    )
    team_won_result = (await db.execute(team_won_q)).one()
    team_deals_won = team_won_result[0] or 0
    team_revenue = float(team_won_result[1] or 0)

    # Per-member breakdown
    member_stats_q = text("""
        SELECT u.id as user_id, u.email, u.company_name,
               (SELECT COUNT(*) FROM call_logs cl WHERE cl.user_id = u.id AND cl.created_at >= :week_start) as calls,
               (SELECT COUNT(*) FROM deals d WHERE d.user_id = u.id AND d.stage = 'won' AND d.updated_at >= :week_start) as deals_won,
               (SELECT COALESCE(SUM(d.value), 0) FROM deals d WHERE d.user_id = u.id AND d.stage = 'won' AND d.updated_at >= :week_start) as revenue
        FROM api_users u
        WHERE u.id = ANY(:member_ids)
        ORDER BY revenue DESC
    """)
    member_stats_result = await db.execute(member_stats_q, {"week_start": week_start, "member_ids": member_ids})
    member_breakdown = [
        {
            "user_id": str(r["user_id"]),
            "email": r["email"],
            "company_name": r["company_name"],
            "calls": r["calls"],
            "deals_won": r["deals_won"],
            "revenue": float(r["revenue"]),
        }
        for r in member_stats_result.mappings().all()
    ]

    db.add(_log_usage(user, request, f"/v1/crm/teams/{team_id}/dashboard"))
    await db.commit()

    return {
        "team_id": str(team_id),
        "member_count": len(member_ids),
        "pipeline": pipeline,
        "this_week": {
            "calls": team_calls,
            "new_contacts": team_contacts,
            "deals_won": team_deals_won,
            "revenue": team_revenue,
        },
        "members": member_breakdown,
    }


@router.get("/territories")
async def get_my_territories(
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get the current user's assigned territories (ZIPs/states) across all teams."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    result = await db.execute(
        select(TeamMember.territories, Team.name)
        .join(Team, Team.id == TeamMember.team_id)
        .where(TeamMember.user_id == user.id, TeamMember.territories.isnot(None))
    )
    rows = result.all()

    # Merge all territories
    all_territories = []
    for territories, team_name in rows:
        if territories:
            for t in territories:
                all_territories.append({"code": t, "team": team_name})

    db.add(_log_usage(user, request, "/v1/crm/territories"))
    await db.commit()

    return {"territories": all_territories}


# ---------------------------------------------------------------------------
# Activity Logging Helper
# ---------------------------------------------------------------------------

async def log_activity(
    db: AsyncSession,
    user_id,
    team_id,
    activity_type: str,
    description: str,
    entity_type: str | None = None,
    entity_id=None,
):
    """Create an Activity record. Called after successful CRM operations."""
    activity = Activity(
        user_id=user_id,
        team_id=team_id,
        activity_type=activity_type,
        description=description,
        entity_type=entity_type,
        entity_id=entity_id,
    )
    db.add(activity)


async def _get_user_team_id(db: AsyncSession, user_id) -> uuid.UUID | None:
    """Get the first team_id for a user (for activity logging)."""
    result = await db.execute(
        select(TeamMember.team_id).where(TeamMember.user_id == user_id).limit(1)
    )
    row = result.scalar_one_or_none()
    return row if row else None


# ---------------------------------------------------------------------------
# Collaboration: Activity Feed
# ---------------------------------------------------------------------------

@router.get("/activity-feed")
async def activity_feed(
    request: Request,
    limit: int = Query(50, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Team activity feed — last N actions across all team members.
    Shows: calls logged, deals created/updated, contacts created, leads assigned, quotes sent.
    """
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    # Get all team IDs the user belongs to
    teams_q = await db.execute(
        select(TeamMember.team_id).where(TeamMember.user_id == user.id)
    )
    team_ids = [r[0] for r in teams_q.all()]

    if team_ids:
        # Show activities from all teams the user is in
        query = (
            select(Activity)
            .where(Activity.team_id.in_(team_ids))
            .order_by(Activity.created_at.desc())
            .limit(limit)
        )
    else:
        # Solo user — show own activities
        query = (
            select(Activity)
            .where(Activity.user_id == user.id)
            .order_by(Activity.created_at.desc())
            .limit(limit)
        )

    result = await db.execute(query)
    activities = result.scalars().all()

    # Get user emails for display
    user_ids = list(set(a.user_id for a in activities))
    user_names = {}
    if user_ids:
        un_q = await db.execute(
            select(ApiUser.id, ApiUser.email, ApiUser.company_name)
            .where(ApiUser.id.in_(user_ids))
        )
        user_names = {
            r.id: {"email": r.email, "company": r.company_name}
            for r in un_q.all()
        }

    items = [
        {
            "id": str(a.id),
            "user_id": str(a.user_id),
            "user_email": user_names.get(a.user_id, {}).get("email"),
            "user_company": user_names.get(a.user_id, {}).get("company"),
            "team_id": str(a.team_id) if a.team_id else None,
            "activity_type": a.activity_type,
            "description": a.description,
            "entity_type": a.entity_type,
            "entity_id": str(a.entity_id) if a.entity_id else None,
            "created_at": a.created_at.isoformat() if a.created_at else None,
        }
        for a in activities
    ]

    # Also pull recent call_logs, deal changes, new contacts for a richer feed
    # if no logged activities exist yet
    if not items:
        now = datetime.now(timezone.utc)
        recent_start = now - timedelta(days=7)

        # Recent calls
        if team_ids:
            member_ids_q = await db.execute(
                select(TeamMember.user_id).where(TeamMember.team_id.in_(team_ids))
            )
            member_ids = [r[0] for r in member_ids_q.all()]
        else:
            member_ids = [user.id]

        calls_q = text("""
            SELECT cl.id, cl.user_id, u.email, cl.disposition, cl.duration_seconds,
                   cl.created_at, h.contractor_company, h.address
            FROM call_logs cl
            JOIN api_users u ON u.id = cl.user_id
            LEFT JOIN hot_leads h ON h.id = cl.lead_id
            WHERE cl.user_id = ANY(:member_ids)
              AND cl.created_at >= :start
            ORDER BY cl.created_at DESC
            LIMIT :lim
        """)
        calls_result = await db.execute(calls_q, {
            "member_ids": member_ids,
            "start": recent_start,
            "lim": limit,
        })
        for r in calls_result.mappings().all():
            target = r["contractor_company"] or r["address"] or "a lead"
            items.append({
                "id": str(r["id"]),
                "user_id": str(r["user_id"]),
                "user_email": r["email"],
                "activity_type": "call_logged",
                "description": f"Logged a {r['disposition'] or 'call'} with {target} ({r['duration_seconds'] or 0}s)",
                "entity_type": "lead",
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            })

        # Recent deal stage changes
        deals_q = text("""
            SELECT d.id, d.user_id, u.email, d.title, d.stage, d.value, d.updated_at
            FROM deals d
            JOIN api_users u ON u.id = d.user_id
            WHERE d.user_id = ANY(:member_ids)
              AND d.updated_at >= :start
            ORDER BY d.updated_at DESC
            LIMIT :lim
        """)
        deals_result = await db.execute(deals_q, {
            "member_ids": member_ids,
            "start": recent_start,
            "lim": 20,
        })
        for r in deals_result.mappings().all():
            val_str = f" (${r['value']:,.0f})" if r["value"] else ""
            items.append({
                "id": str(r["id"]),
                "user_id": str(r["user_id"]),
                "user_email": r["email"],
                "activity_type": "deal_stage_changed",
                "description": f"Deal '{r['title'] or 'Untitled'}' moved to {r['stage']}{val_str}",
                "entity_type": "deal",
                "entity_id": str(r["id"]),
                "created_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            })

        # Sort by date
        items.sort(key=lambda x: x.get("created_at") or "", reverse=True)
        items = items[:limit]

    db.add(_log_usage(user, request, "/v1/crm/activity-feed"))
    await db.commit()

    return {"results": items, "count": len(items)}


# ---------------------------------------------------------------------------
# Collaboration: Lead Assignment
# ---------------------------------------------------------------------------

class LeadAssignRequest(BaseModel):
    lead_ids: list[uuid.UUID] = Field(..., min_length=1, max_length=100)
    member_id: uuid.UUID


@router.post("/leads/assign")
async def assign_leads(
    body: LeadAssignRequest,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Assign leads to a team member. Creates lead_statuses with status='assigned'
    for the target user. Only team owners/managers can assign.
    """
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    # Verify the caller is an owner/manager of a shared team with the target
    caller_teams_q = await db.execute(
        select(TeamMember.team_id).where(
            TeamMember.user_id == user.id,
            TeamMember.role.in_(["owner", "manager"]),
        )
    )
    caller_team_ids = [r[0] for r in caller_teams_q.all()]

    if not caller_team_ids:
        raise HTTPException(
            status_code=403,
            detail="Only team owners and managers can assign leads",
        )

    # Verify target member is on one of the caller's teams
    target_member_q = await db.execute(
        select(TeamMember).where(
            TeamMember.user_id == body.member_id,
            TeamMember.team_id.in_(caller_team_ids),
        )
    )
    target_member = target_member_q.scalar_one_or_none()
    if not target_member:
        raise HTTPException(
            status_code=404,
            detail="Target user is not a member of your team",
        )

    # Get target user info for activity description
    target_user_q = await db.execute(
        select(ApiUser.email).where(ApiUser.id == body.member_id)
    )
    target_email = target_user_q.scalar_one_or_none() or "team member"

    # Create/update lead_statuses for the target user
    assigned_count = 0
    for lead_id in body.lead_ids:
        existing_q = await db.execute(
            select(LeadStatus).where(
                LeadStatus.user_id == body.member_id,
                LeadStatus.lead_id == lead_id,
            )
        )
        existing = existing_q.scalar_one_or_none()

        if existing:
            existing.status = "assigned"
            existing.updated_at = datetime.now(timezone.utc)
        else:
            db.add(LeadStatus(
                user_id=body.member_id,
                lead_id=lead_id,
                status="assigned",
            ))
        assigned_count += 1

    # Log activity
    team_id = await _get_user_team_id(db, user.id)
    await log_activity(
        db, user.id, team_id,
        "lead_assigned",
        f"Assigned {assigned_count} leads to {target_email}",
        entity_type="lead",
    )

    db.add(_log_usage(user, request, "/v1/crm/leads/assign"))
    await db.commit()

    return {
        "assigned": assigned_count,
        "member_id": str(body.member_id),
        "member_email": target_email,
    }


@router.get("/leads/assigned")
async def get_assigned_leads(
    request: Request,
    page: int = Query(1, ge=1, le=500),
    page_size: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get leads assigned to the current user. Returns hot_leads where
    lead_statuses.user_id = current user AND status = 'assigned'.
    """
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    # Count total assigned
    count_sql = text("""
        SELECT count(*)
        FROM lead_statuses ls
        WHERE ls.user_id = :user_id AND ls.status = 'assigned'
    """)
    total = (await db.execute(count_sql, {"user_id": user.id})).scalar() or 0

    # Fetch assigned leads with hot_leads data
    query = text("""
        SELECT
            h.id, h.permit_number, h.permit_type, h.work_class, h.description,
            h.address, h.city, h.state, h.zip, h.county,
            h.lat, h.lng, h.issue_date, h.valuation, h.sqft,
            h.contractor_company, h.contractor_name, h.contractor_phone,
            h.contractor_trade, h.applicant_name, h.applicant_phone,
            h.owner_name, h.jurisdiction, h.source,
            ls.updated_at as assigned_at
        FROM lead_statuses ls
        JOIN hot_leads h ON h.id = ls.lead_id
        WHERE ls.user_id = :user_id AND ls.status = 'assigned'
        ORDER BY ls.updated_at DESC
        OFFSET :offset LIMIT :limit
    """)

    result = await db.execute(query, {
        "user_id": user.id,
        "offset": (page - 1) * page_size,
        "limit": page_size,
    })
    rows = result.mappings().all()

    leads = [
        {
            "id": str(r["id"]),
            "permit_number": r["permit_number"],
            "permit_type": r["permit_type"],
            "work_class": r["work_class"],
            "description": r["description"],
            "address": r["address"],
            "city": r["city"],
            "state": r["state"],
            "zip": r["zip"],
            "county": r["county"],
            "lat": r["lat"],
            "lng": r["lng"],
            "issue_date": r["issue_date"].isoformat() if r["issue_date"] else None,
            "valuation": r["valuation"],
            "sqft": r["sqft"],
            "contractor_company": r["contractor_company"],
            "contractor_name": r["contractor_name"],
            "contractor_phone": r["contractor_phone"],
            "contractor_trade": r["contractor_trade"],
            "applicant_name": r["applicant_name"],
            "applicant_phone": r["applicant_phone"],
            "owner_name": r["owner_name"],
            "jurisdiction": r["jurisdiction"],
            "source": r["source"],
            "assigned_at": r["assigned_at"].isoformat() if r["assigned_at"] else None,
        }
        for r in rows
    ]

    db.add(_log_usage(user, request, "/v1/crm/leads/assigned"))
    await db.commit()

    return {
        "results": leads,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# ---------------------------------------------------------------------------
# Webhook Management
# ---------------------------------------------------------------------------

VALID_EVENT_TYPES = {"new_permit", "new_violation", "price_change"}


class WebhookCreate(BaseModel):
    name: str | None = Field(None, max_length=200)
    url: str = Field(..., min_length=8, max_length=2000)
    event_types: list[str] = Field(default_factory=lambda: ["new_permit"])
    filters: dict | None = None


class WebhookUpdate(BaseModel):
    name: str | None = None
    url: str | None = None
    event_types: list[str] | None = None
    filters: dict | None = None
    is_active: bool | None = None


def _webhook_to_dict(w: Webhook) -> dict:
    return {
        "id": str(w.id),
        "name": w.name,
        "url": w.url,
        "event_types": w.event_types or [],
        "filters": w.filters or {},
        "is_active": w.is_active,
        "secret": w.secret,
        "last_triggered": w.last_triggered.isoformat() if w.last_triggered else None,
        "failure_count": w.failure_count,
        "created_at": w.created_at.isoformat() if w.created_at else None,
    }


@router.post("/webhooks")
async def create_webhook(
    body: WebhookCreate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new webhook. Requires Explorer+."""
    _require_paid(user)

    # Validate event types
    invalid = set(body.event_types) - VALID_EVENT_TYPES
    if invalid:
        raise HTTPException(status_code=400, detail=f"Invalid event types: {invalid}. Valid: {VALID_EVENT_TYPES}")

    # Max 20 webhooks per user
    count = await db.scalar(select(func.count()).where(Webhook.user_id == user.id))
    if count >= 20:
        raise HTTPException(status_code=400, detail="Maximum 20 webhooks per account.")

    webhook = Webhook(
        user_id=user.id,
        name=body.name or "Untitled webhook",
        url=body.url,
        event_types=body.event_types,
        filters=body.filters or {},
    )
    db.add(webhook)
    db.add(_log_usage(user, request, "/v1/crm/webhooks"))
    await db.commit()
    await db.refresh(webhook)

    return _webhook_to_dict(webhook)


@router.get("/webhooks")
async def list_webhooks(
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List user's webhooks."""
    _require_paid(user)
    result = await db.execute(
        select(Webhook)
        .where(Webhook.user_id == user.id)
        .order_by(Webhook.created_at.desc())
    )
    webhooks = result.scalars().all()
    return {"webhooks": [_webhook_to_dict(w) for w in webhooks], "total": len(webhooks)}


@router.put("/webhooks/{webhook_id}")
async def update_webhook(
    webhook_id: uuid.UUID,
    body: WebhookUpdate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a webhook."""
    _require_paid(user)
    webhook = await db.get(Webhook, webhook_id)
    if not webhook or webhook.user_id != user.id:
        raise HTTPException(status_code=404, detail="Webhook not found.")

    if body.event_types is not None:
        invalid = set(body.event_types) - VALID_EVENT_TYPES
        if invalid:
            raise HTTPException(status_code=400, detail=f"Invalid event types: {invalid}")
        webhook.event_types = body.event_types
    if body.name is not None:
        webhook.name = body.name
    if body.url is not None:
        webhook.url = body.url
    if body.filters is not None:
        webhook.filters = body.filters
    if body.is_active is not None:
        webhook.is_active = body.is_active
        if body.is_active:
            webhook.failure_count = 0  # reset on re-enable

    db.add(_log_usage(user, request, f"/v1/crm/webhooks/{webhook_id}"))
    await db.commit()
    await db.refresh(webhook)

    return _webhook_to_dict(webhook)


@router.delete("/webhooks/{webhook_id}")
async def delete_webhook(
    webhook_id: uuid.UUID,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a webhook."""
    _require_paid(user)
    webhook = await db.get(Webhook, webhook_id)
    if not webhook or webhook.user_id != user.id:
        raise HTTPException(status_code=404, detail="Webhook not found.")

    await db.delete(webhook)
    db.add(_log_usage(user, request, f"/v1/crm/webhooks/{webhook_id}"))
    await db.commit()

    return {"status": "deleted", "id": str(webhook_id)}


@router.post("/webhooks/{webhook_id}/test")
async def test_webhook(
    webhook_id: uuid.UUID,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Send a test payload to a webhook."""
    _require_paid(user)
    webhook = await db.get(Webhook, webhook_id)
    if not webhook or webhook.user_id != user.id:
        raise HTTPException(status_code=404, detail="Webhook not found.")

    test_payload = {
        "event_type": "test",
        "message": "This is a test webhook from PermitLookup.",
        "webhook_id": str(webhook.id),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sample_data": {
            "permit_number": "TEST-2026-001",
            "address": "123 Main St",
            "city": "Austin",
            "state": "TX",
            "zip": "78701",
            "permit_type": "roofing",
            "contractor_company": "ABC Roofing LLC",
            "valuation": 25000,
            "issue_date": "2026-03-25",
        },
    }

    success = await deliver_webhook(webhook.url, test_payload, secret=webhook.secret)

    return {
        "status": "delivered" if success else "failed",
        "webhook_id": str(webhook.id),
        "url": webhook.url,
    }
