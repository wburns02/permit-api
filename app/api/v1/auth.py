"""API key management and signup endpoints."""

import hashlib
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, EmailStr

from app.database import get_db
from app.middleware.api_key_auth import get_current_user, hash_api_key
from app.models.api_key import ApiUser, ApiKey, PlanTier

router = APIRouter(tags=["Auth"])


class SignupRequest(BaseModel):
    email: EmailStr
    company_name: str | None = None


class SignupResponse(BaseModel):
    api_key: str
    user_id: str
    email: str
    plan: str
    message: str


@router.post("/signup", response_model=SignupResponse)
async def signup(body: SignupRequest, db: AsyncSession = Depends(get_db)):
    """
    Create a free account and get an API key.

    No credit card required. Free tier: 100 lookups/day.
    """
    # Normalize email: lowercase, strip whitespace
    email = body.email.strip().lower()

    # Check if email already registered
    existing = await db.execute(
        select(ApiUser).where(func.lower(ApiUser.email) == email)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Email already registered.")

    # Create user
    user = ApiUser(
        email=email,
        company_name=body.company_name,
        plan=PlanTier.FREE,
    )
    db.add(user)
    await db.flush()

    # Generate API key
    raw_key = ApiKey.generate_key()
    api_key = ApiKey(
        user_id=user.id,
        key_hash=hash_api_key(raw_key),
        key_prefix=raw_key[:12],
        name="Default",
    )
    db.add(api_key)
    await db.commit()

    return SignupResponse(
        api_key=raw_key,
        user_id=str(user.id),
        email=user.email,
        plan=user.plan.value,
        message="Save your API key — it won't be shown again. Include it as X-API-Key header.",
    )


@router.get("/api-keys")
async def list_api_keys(
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List your API keys (shows prefix only, not full key)."""
    result = await db.execute(
        select(ApiKey).where(ApiKey.user_id == user.id)
    )
    keys = result.scalars().all()

    return [
        {
            "id": str(k.id),
            "key_prefix": k.key_prefix + "...",
            "name": k.name,
            "is_active": k.is_active,
            "created_at": k.created_at.isoformat() if k.created_at else None,
            "last_used_at": k.last_used_at.isoformat() if k.last_used_at else None,
        }
        for k in keys
    ]


@router.post("/api-keys")
async def create_api_key(
    request: Request,
    name: str = "New Key",
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Generate a new API key for your account."""
    # Limit to 5 keys per user
    count = await db.execute(
        select(func.count()).select_from(ApiKey).where(
            ApiKey.user_id == user.id, ApiKey.is_active.is_(True)
        )
    )
    if count.scalar() >= 5:
        raise HTTPException(status_code=400, detail="Maximum 5 active API keys per account.")

    raw_key = ApiKey.generate_key()
    api_key = ApiKey(
        user_id=user.id,
        key_hash=hash_api_key(raw_key),
        key_prefix=raw_key[:12],
        name=name,
    )
    db.add(api_key)
    await db.commit()

    return {
        "api_key": raw_key,
        "name": name,
        "message": "Save this key — it won't be shown again.",
    }


@router.delete("/api-keys/{key_id}")
async def revoke_api_key(
    key_id: str,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Revoke an API key."""
    result = await db.execute(
        select(ApiKey).where(ApiKey.id == key_id, ApiKey.user_id == user.id)
    )
    key = result.scalar_one_or_none()
    if not key:
        raise HTTPException(status_code=404, detail="API key not found.")

    key.is_active = False
    await db.commit()
    return {"message": "API key revoked."}
