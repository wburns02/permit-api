"""Permit alert CRUD endpoints with test and history."""

from uuid import UUID
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.models.api_key import ApiUser, resolve_plan
from app.models.alert import PermitAlert, AlertFrequency
from app.models.alert_history import AlertExecutionHistory
from app.services.stripe_service import get_alert_limit

router = APIRouter(prefix="/alerts", tags=["Alerts"])


class AlertCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    filters: dict = Field(..., description="Filter criteria: state, city, permit_type, contractor, address, keyword")
    frequency: AlertFrequency = AlertFrequency.DAILY
    webhook_url: str | None = None
    email_notify: bool = True


class AlertUpdate(BaseModel):
    name: str | None = None
    filters: dict | None = None
    frequency: AlertFrequency | None = None
    webhook_url: str | None = None
    email_notify: bool | None = None
    is_active: bool | None = None


def _alert_to_dict(a: PermitAlert) -> dict:
    return {
        "id": str(a.id),
        "name": a.name,
        "filters": a.filters,
        "frequency": a.frequency.value,
        "webhook_url": a.webhook_url,
        "email_notify": a.email_notify,
        "is_active": a.is_active,
        "last_checked_at": a.last_checked_at.isoformat() if a.last_checked_at else None,
        "last_match_count": a.last_match_count,
        "total_matches": a.total_matches,
        "last_error": a.last_error,
        "consecutive_failures": a.consecutive_failures,
        "created_at": a.created_at.isoformat() if a.created_at else None,
    }


@router.get("")
async def list_alerts(
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List all alerts for the current user."""
    result = await db.execute(
        select(PermitAlert)
        .where(PermitAlert.user_id == user.id)
        .order_by(PermitAlert.created_at.desc())
    )
    alerts = result.scalars().all()
    return {
        "alerts": [_alert_to_dict(a) for a in alerts],
        "total": len(alerts),
    }


@router.post("")
async def create_alert(
    body: AlertCreate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new permit alert."""
    count_result = await db.execute(
        select(func.count()).select_from(PermitAlert).where(PermitAlert.user_id == user.id)
    )
    count = count_result.scalar() or 0
    plan = resolve_plan(user.plan)
    limit = get_alert_limit(plan)
    if count >= limit:
        raise HTTPException(status_code=403, detail=f"Alert limit reached ({limit}). Upgrade your plan for more alerts.")

    alert = PermitAlert(
        user_id=user.id,
        name=body.name,
        filters=body.filters,
        frequency=body.frequency,
        webhook_url=body.webhook_url,
        email_notify=body.email_notify,
    )
    db.add(alert)
    await db.commit()
    await db.refresh(alert)

    return _alert_to_dict(alert)


@router.post("/{alert_id}/test")
async def test_alert(
    alert_id: UUID,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Dry-run an alert — returns matching permits without delivering notifications."""
    result = await db.execute(
        select(PermitAlert).where(PermitAlert.id == alert_id, PermitAlert.user_id == user.id)
    )
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found.")

    from app.services.alert_engine import match_alert
    matches = await match_alert(alert, db)

    return {
        "alert_id": str(alert.id),
        "alert_name": alert.name,
        "filters": alert.filters,
        "match_count": len(matches),
        "matches": matches,
        "note": "Dry run — no notifications sent.",
    }


@router.get("/{alert_id}/history")
async def alert_history(
    alert_id: UUID,
    request: Request,
    page: int = Query(1, ge=1, le=20),
    page_size: int = Query(20, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get execution history for an alert."""
    # Verify ownership
    alert_result = await db.execute(
        select(PermitAlert).where(PermitAlert.id == alert_id, PermitAlert.user_id == user.id)
    )
    if not alert_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Alert not found.")

    result = await db.execute(
        select(AlertExecutionHistory)
        .where(AlertExecutionHistory.alert_id == alert_id)
        .order_by(AlertExecutionHistory.run_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    entries = result.scalars().all()

    count_result = await db.execute(
        select(func.count()).select_from(AlertExecutionHistory)
        .where(AlertExecutionHistory.alert_id == alert_id)
    )
    total = count_result.scalar() or 0

    return {
        "history": [
            {
                "id": str(h.id),
                "run_at": h.run_at.isoformat() if h.run_at else None,
                "match_count": h.match_count,
                "delivery_method": h.delivery_method,
                "delivery_status": h.delivery_status,
                "error": h.error,
                "matches_sample": h.matches_sample,
            }
            for h in entries
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.put("/{alert_id}")
async def update_alert(
    alert_id: UUID,
    body: AlertUpdate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update an existing alert."""
    result = await db.execute(
        select(PermitAlert).where(PermitAlert.id == alert_id, PermitAlert.user_id == user.id)
    )
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found.")

    updates = body.model_dump(exclude_unset=True)
    if updates:
        for k, v in updates.items():
            setattr(alert, k, v)
        await db.commit()
        await db.refresh(alert)

    return _alert_to_dict(alert)


@router.delete("/{alert_id}")
async def delete_alert(
    alert_id: UUID,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete an alert."""
    result = await db.execute(
        select(PermitAlert).where(PermitAlert.id == alert_id, PermitAlert.user_id == user.id)
    )
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found.")

    await db.delete(alert)
    await db.commit()
    return {"deleted": True}
