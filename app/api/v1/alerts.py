"""Permit alert CRUD endpoints."""

from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select, update, delete, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.models.api_key import ApiUser
from app.models.alert import PermitAlert, AlertFrequency

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
        "alerts": [
            {
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
                "created_at": a.created_at.isoformat() if a.created_at else None,
            }
            for a in alerts
        ],
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
    # Limit alerts per user
    count_result = await db.execute(
        select(func.count()).select_from(PermitAlert).where(PermitAlert.user_id == user.id)
    )
    count = count_result.scalar() or 0
    max_alerts = {"free": 2, "starter": 10, "pro": 50, "enterprise": 200}
    limit = max_alerts.get(user.plan.value, 2)
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

    return {
        "id": str(alert.id),
        "name": alert.name,
        "filters": alert.filters,
        "frequency": alert.frequency.value,
        "webhook_url": alert.webhook_url,
        "email_notify": alert.email_notify,
        "is_active": alert.is_active,
        "created_at": alert.created_at.isoformat() if alert.created_at else None,
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

    return {
        "id": str(alert.id),
        "name": alert.name,
        "filters": alert.filters,
        "frequency": alert.frequency.value,
        "webhook_url": alert.webhook_url,
        "email_notify": alert.email_notify,
        "is_active": alert.is_active,
    }


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
