"""Sales Dialer endpoints — click-to-call queue powered by fresh permit data."""

import uuid
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.dialer import CallLog, LeadStatus
from app.models.crm import Activity
from app.models.team import TeamMember

router = APIRouter(prefix="/dialer", tags=["Sales Dialer"])


# ---------------------------------------------------------------------------
# Plan gating — any paid plan (Explorer+) can use the dialer
# ---------------------------------------------------------------------------

def _require_paid(user: ApiUser):
    plan = resolve_plan(user.plan)
    if plan == PlanTier.FREE:
        raise HTTPException(
            status_code=403,
            detail="Sales Dialer requires Explorer plan or higher. Upgrade at /pricing",
        )


# ---------------------------------------------------------------------------
# Trade-to-permit filter mapping
# ---------------------------------------------------------------------------

TRADE_FILTERS = {
    "roofing": (
        "(permit_type IN ('BP', 'RP', 'Building') AND (work_class ILIKE '%roof%' OR description ILIKE '%roof%'))"
    ),
    "hvac": (
        "(permit_type IN ('MP', 'Mechanical') OR description ILIKE '%hvac%' OR description ILIKE '%air condition%')"
    ),
    "plumbing": (
        "(permit_type IN ('PP', 'Plumbing') OR description ILIKE '%plumb%')"
    ),
    "electrical": (
        "(permit_type IN ('EP', 'Electrical') OR description ILIKE '%electric%')"
    ),
    "solar": (
        "(description ILIKE '%solar%' OR description ILIKE '%photovoltaic%')"
    ),
    "general": None,  # No filter — all permits
}

VALID_TRADES = list(TRADE_FILTERS.keys())
VALID_DISPOSITIONS = {"connected", "voicemail", "no_answer", "wrong_number", "callback", "sold"}
VALID_STATUSES = {"contacted", "callback", "qualified", "won", "lost", "skipped"}


# ---------------------------------------------------------------------------
# Request/Response schemas
# ---------------------------------------------------------------------------

class CallLogRequest(BaseModel):
    lead_id: uuid.UUID
    phone_number: str = Field(..., max_length=20)
    duration_seconds: int = Field(0, ge=0)
    disposition: str = Field(..., max_length=50)
    notes: str | None = None

class DispositionRequest(BaseModel):
    lead_id: uuid.UUID
    status: str = Field(..., max_length=50)
    callback_date: datetime | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log_usage(user: ApiUser, request: Request, endpoint: str) -> UsageLog:
    return UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint=endpoint,
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )


# ---------------------------------------------------------------------------
# GET /v1/dialer/queue — fresh leads the user hasn't contacted
# ---------------------------------------------------------------------------

@router.get("/queue")
async def get_call_queue(
    request: Request,
    trade: str = Query("general", description="Trade filter: roofing, hvac, plumbing, electrical, solar, general"),
    state: str | None = Query(None, max_length=2),
    city: str | None = Query(None, max_length=100),
    zip: str | None = Query(None, max_length=10),
    limit: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Return a queue of fresh leads from hot_leads that the user hasn't contacted
    or skipped yet. Sorted by issue_date DESC (freshest first).
    """
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    trade = trade.lower().strip()
    if trade not in VALID_TRADES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid trade. Must be one of: {', '.join(VALID_TRADES)}",
        )

    # Build the query using raw SQL against hot_leads (not an ORM model)
    where_clauses = ["1=1"]
    params: dict = {"user_id": user.id, "limit": limit}

    # Trade filter
    trade_sql = TRADE_FILTERS.get(trade)
    if trade_sql:
        where_clauses.append(trade_sql)

    # Location filters
    if state:
        where_clauses.append("h.state = :state")
        params["state"] = state.upper()
    if city:
        where_clauses.append("h.city ILIKE :city")
        params["city"] = f"%{city}%"
    if zip:
        where_clauses.append("h.zip = :zip")
        params["zip"] = zip

    # Exclude leads this user has already contacted or skipped
    exclude_sub = (
        "h.id NOT IN ("
        "  SELECT ls.lead_id FROM lead_statuses ls"
        "  WHERE ls.user_id = :user_id"
        ")"
    )
    where_clauses.append(exclude_sub)

    where_sql = " AND ".join(where_clauses)

    query = text(f"""
        SELECT
            h.id, h.permit_number, h.permit_type, h.work_class, h.description,
            h.address, h.city, h.state, h.zip, h.county,
            h.lat, h.lng, h.issue_date, h.valuation, h.sqft,
            h.contractor_company, h.contractor_name, h.contractor_phone,
            h.contractor_trade, h.applicant_name, h.applicant_phone,
            h.owner_name, h.jurisdiction, h.source
        FROM hot_leads h
        WHERE {where_sql}
        ORDER BY h.issue_date DESC NULLS LAST
        LIMIT :limit
    """)

    result = await db.execute(query, params)
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
        }
        for r in rows
    ]

    db.add(_log_usage(user, request, "/v1/dialer/queue"))
    await db.commit()

    return {
        "trade": trade,
        "count": len(leads),
        "leads": leads,
    }


# ---------------------------------------------------------------------------
# POST /v1/dialer/log — record a call
# ---------------------------------------------------------------------------

@router.post("/log")
async def log_call(
    body: CallLogRequest,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Record a call attempt and update the lead's status."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    if body.disposition not in VALID_DISPOSITIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid disposition. Must be one of: {', '.join(sorted(VALID_DISPOSITIONS))}",
        )

    # AI-generate summary + action items from notes
    ai_summary = None
    action_items = None
    if body.notes and len(body.notes.strip()) > 10:
        try:
            from app.services.call_intelligence import summarize_call
            ai_result = await summarize_call(body.notes, {
                "lead_id": str(body.lead_id),
                "phone": body.phone_number,
            })
            ai_summary = ai_result.get("summary")
            action_items = ai_result.get("action_items")
        except Exception:
            pass  # AI is optional — don't block the call log

    # Create call log
    call_log = CallLog(
        user_id=user.id,
        lead_id=body.lead_id,
        phone_number=body.phone_number,
        duration_seconds=body.duration_seconds,
        disposition=body.disposition,
        notes=body.notes,
        ai_summary=ai_summary,
        action_items=action_items,
    )
    db.add(call_log)

    # Map disposition to a lead status
    status_map = {
        "connected": "contacted",
        "voicemail": "contacted",
        "no_answer": "contacted",
        "wrong_number": "contacted",
        "callback": "callback",
        "sold": "won",
    }
    mapped_status = status_map.get(body.disposition, "contacted")

    # Upsert lead status
    existing = await db.execute(
        select(LeadStatus).where(
            LeadStatus.user_id == user.id,
            LeadStatus.lead_id == body.lead_id,
        )
    )
    lead_status = existing.scalar_one_or_none()

    if lead_status:
        lead_status.status = mapped_status
        lead_status.updated_at = datetime.now(timezone.utc)
    else:
        lead_status = LeadStatus(
            user_id=user.id,
            lead_id=body.lead_id,
            status=mapped_status,
        )
        db.add(lead_status)

    db.add(_log_usage(user, request, "/v1/dialer/log"))

    # Auto-log activity for team feed
    try:
        team_q = await db.execute(
            select(TeamMember.team_id).where(TeamMember.user_id == user.id).limit(1)
        )
        team_id = team_q.scalar_one_or_none()
        dur_str = f" ({body.duration_seconds}s)" if body.duration_seconds else ""
        activity = Activity(
            user_id=user.id,
            team_id=team_id,
            activity_type="call_logged",
            description=f"Logged a {body.disposition} call to {body.phone_number}{dur_str}",
            entity_type="lead",
            entity_id=body.lead_id,
        )
        db.add(activity)
    except Exception:
        pass  # Activity logging is non-critical

    await db.commit()

    return {
        "call_log_id": str(call_log.id),
        "lead_id": str(body.lead_id),
        "disposition": body.disposition,
        "status": mapped_status,
        "ai_summary": ai_summary,
        "action_items": action_items,
    }


# ---------------------------------------------------------------------------
# POST /v1/dialer/analyze-transcription — AI analysis of call recording
# ---------------------------------------------------------------------------

class TranscriptionRequest(BaseModel):
    call_log_id: str
    transcription: str


@router.post("/analyze-transcription")
async def analyze_transcription_endpoint(
    body: TranscriptionRequest,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Analyze a call transcription with AI — extracts summary, action items, objections, buying signals."""
    _require_paid(user)

    from app.services.call_intelligence import analyze_transcription

    # Get lead context from the call log
    call_log = await db.execute(
        select(CallLog).where(CallLog.id == body.call_log_id, CallLog.user_id == user.id)
    )
    log_record = call_log.scalar_one_or_none()
    if not log_record:
        raise HTTPException(status_code=404, detail="Call log not found")

    lead_context = None
    if log_record.lead_id:
        lead_row = await db.execute(
            text("SELECT address, permit_type, valuation, contractor_company FROM hot_leads WHERE id = :lid"),
            {"lid": str(log_record.lead_id)},
        )
        lead = lead_row.one_or_none()
        if lead:
            lead_context = {
                "address": lead.address,
                "permit_type": lead.permit_type,
                "valuation": lead.valuation,
                "contractor_company": lead.contractor_company,
            }

    result = await analyze_transcription(body.transcription, lead_context)

    # Update the call log with AI analysis
    log_record.ai_summary = result.get("summary")
    log_record.action_items = result.get("action_items")
    await db.commit()

    return result


# ---------------------------------------------------------------------------
# POST /v1/dialer/disposition — update lead status directly
# ---------------------------------------------------------------------------

@router.post("/disposition")
async def update_disposition(
    body: DispositionRequest,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update or create a lead status. If 'callback', include callback_date."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    if body.status not in VALID_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid status. Must be one of: {', '.join(sorted(VALID_STATUSES))}",
        )

    if body.status == "callback" and not body.callback_date:
        raise HTTPException(
            status_code=400,
            detail="callback_date is required when status is 'callback'.",
        )

    # Upsert lead status
    existing = await db.execute(
        select(LeadStatus).where(
            LeadStatus.user_id == user.id,
            LeadStatus.lead_id == body.lead_id,
        )
    )
    lead_status = existing.scalar_one_or_none()

    if lead_status:
        lead_status.status = body.status
        lead_status.updated_at = datetime.now(timezone.utc)
    else:
        lead_status = LeadStatus(
            user_id=user.id,
            lead_id=body.lead_id,
            status=body.status,
        )
        db.add(lead_status)

    # If callback, also update the most recent call log's callback_date
    if body.status == "callback" and body.callback_date:
        latest_call = await db.execute(
            select(CallLog)
            .where(CallLog.user_id == user.id, CallLog.lead_id == body.lead_id)
            .order_by(CallLog.created_at.desc())
            .limit(1)
        )
        call = latest_call.scalar_one_or_none()
        if call:
            call.callback_date = body.callback_date

    db.add(_log_usage(user, request, "/v1/dialer/disposition"))
    await db.commit()

    return {
        "lead_id": str(body.lead_id),
        "status": body.status,
        "callback_date": body.callback_date.isoformat() if body.callback_date else None,
    }


# ---------------------------------------------------------------------------
# GET /v1/dialer/callbacks — leads due for callback
# ---------------------------------------------------------------------------

@router.get("/callbacks")
async def get_callbacks(
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Return leads where the user has a callback_date <= now.
    These should be shown first in the dialer queue.
    """
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    now = datetime.now(timezone.utc)

    query = text("""
        SELECT
            h.id, h.permit_number, h.permit_type, h.work_class, h.description,
            h.address, h.city, h.state, h.zip, h.county,
            h.lat, h.lng, h.issue_date, h.valuation, h.sqft,
            h.contractor_company, h.contractor_name, h.contractor_phone,
            h.contractor_trade, h.applicant_name, h.applicant_phone,
            h.owner_name, h.jurisdiction, h.source,
            cl.callback_date, cl.notes as last_notes, cl.disposition as last_disposition
        FROM call_logs cl
        JOIN hot_leads h ON h.id = cl.lead_id
        WHERE cl.user_id = :user_id
          AND cl.callback_date <= :now
          AND cl.callback_date IS NOT NULL
          AND cl.id = (
              SELECT cl2.id FROM call_logs cl2
              WHERE cl2.user_id = :user_id AND cl2.lead_id = cl.lead_id
              ORDER BY cl2.created_at DESC LIMIT 1
          )
        ORDER BY cl.callback_date ASC
    """)

    result = await db.execute(query, {"user_id": user.id, "now": now})
    rows = result.mappings().all()

    callbacks = [
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
            "callback_date": r["callback_date"].isoformat() if r["callback_date"] else None,
            "last_notes": r["last_notes"],
            "last_disposition": r["last_disposition"],
        }
        for r in rows
    ]

    db.add(_log_usage(user, request, "/v1/dialer/callbacks"))
    await db.commit()

    return {
        "count": len(callbacks),
        "callbacks": callbacks,
    }


# ---------------------------------------------------------------------------
# GET /v1/dialer/stats — user's dialing stats
# ---------------------------------------------------------------------------

@router.get("/stats")
async def get_stats(
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the user's call activity stats."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=today_start.weekday())  # Monday

    # Calls today
    calls_today_q = select(func.count()).select_from(CallLog).where(
        CallLog.user_id == user.id,
        CallLog.created_at >= today_start,
    )
    calls_today = (await db.execute(calls_today_q)).scalar() or 0

    # Calls this week
    calls_week_q = select(func.count()).select_from(CallLog).where(
        CallLog.user_id == user.id,
        CallLog.created_at >= week_start,
    )
    calls_this_week = (await db.execute(calls_week_q)).scalar() or 0

    # Leads contacted (distinct leads with any status)
    leads_contacted_q = select(func.count()).select_from(LeadStatus).where(
        LeadStatus.user_id == user.id,
    )
    leads_contacted = (await db.execute(leads_contacted_q)).scalar() or 0

    # Leads qualified
    leads_qualified_q = select(func.count()).select_from(LeadStatus).where(
        LeadStatus.user_id == user.id,
        LeadStatus.status == "qualified",
    )
    leads_qualified = (await db.execute(leads_qualified_q)).scalar() or 0

    # Leads won
    leads_won_q = select(func.count()).select_from(LeadStatus).where(
        LeadStatus.user_id == user.id,
        LeadStatus.status == "won",
    )
    leads_won = (await db.execute(leads_won_q)).scalar() or 0

    # Conversion rate (won / total contacted)
    conversion_rate = round((leads_won / leads_contacted * 100), 1) if leads_contacted > 0 else 0.0

    # Average call duration (only calls with duration > 0)
    avg_dur_q = select(func.avg(CallLog.duration_seconds)).where(
        CallLog.user_id == user.id,
        CallLog.duration_seconds > 0,
    )
    avg_duration = (await db.execute(avg_dur_q)).scalar()
    avg_call_duration = round(float(avg_duration)) if avg_duration else 0

    # Disposition breakdown (all time)
    disp_q = (
        select(CallLog.disposition, func.count().label("cnt"))
        .where(CallLog.user_id == user.id)
        .group_by(CallLog.disposition)
    )
    disp_result = await db.execute(disp_q)
    disposition_breakdown = {r.disposition: r.cnt for r in disp_result.all()}

    db.add(_log_usage(user, request, "/v1/dialer/stats"))
    await db.commit()

    return {
        "calls_today": calls_today,
        "calls_this_week": calls_this_week,
        "leads_contacted": leads_contacted,
        "leads_qualified": leads_qualified,
        "leads_won": leads_won,
        "conversion_rate": conversion_rate,
        "avg_call_duration": avg_call_duration,
        "disposition_breakdown": disposition_breakdown,
    }


# ---------------------------------------------------------------------------
# GET /v1/dialer/history — paginated call history
# ---------------------------------------------------------------------------

@router.get("/history")
async def get_history(
    request: Request,
    page: int = Query(1, ge=1, le=100),
    page_size: int = Query(25, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the user's call history with notes and dispositions."""
    _require_paid(user)
    await check_rate_limit(request, lookup_count=1)

    # Join call_logs with hot_leads for lead context
    query = text("""
        SELECT
            cl.id as call_id, cl.lead_id, cl.phone_number, cl.duration_seconds,
            cl.disposition, cl.notes, cl.callback_date, cl.created_at,
            h.permit_number, h.address, h.city, h.state, h.zip,
            h.contractor_company, h.contractor_name, h.permit_type,
            h.description, h.valuation
        FROM call_logs cl
        LEFT JOIN hot_leads h ON h.id = cl.lead_id
        WHERE cl.user_id = :user_id
        ORDER BY cl.created_at DESC
        OFFSET :offset
        LIMIT :limit
    """)

    result = await db.execute(query, {
        "user_id": user.id,
        "offset": (page - 1) * page_size,
        "limit": page_size,
    })
    rows = result.mappings().all()

    # Total count
    count_q = select(func.count()).select_from(CallLog).where(CallLog.user_id == user.id)
    total = (await db.execute(count_q)).scalar() or 0

    history = [
        {
            "call_id": str(r["call_id"]),
            "lead_id": str(r["lead_id"]) if r["lead_id"] else None,
            "phone_number": r["phone_number"],
            "duration_seconds": r["duration_seconds"],
            "disposition": r["disposition"],
            "notes": r["notes"],
            "callback_date": r["callback_date"].isoformat() if r["callback_date"] else None,
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "permit_number": r["permit_number"],
            "address": r["address"],
            "city": r["city"],
            "state": r["state"],
            "zip": r["zip"],
            "contractor_company": r["contractor_company"],
            "contractor_name": r["contractor_name"],
            "permit_type": r["permit_type"],
            "description": r["description"],
            "valuation": r["valuation"],
        }
        for r in rows
    ]

    db.add(_log_usage(user, request, "/v1/dialer/history"))
    await db.commit()

    return {
        "results": history,
        "total": total,
        "page": page,
        "page_size": page_size,
    }
