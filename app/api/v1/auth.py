"""API key management and signup endpoints."""

import hashlib
from datetime import date, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, EmailStr

from app.database import get_db
from app.middleware.api_key_auth import get_current_user, hash_api_key
from app.models.api_key import ApiUser, ApiKey, PlanTier, resolve_plan
from app.services.stripe_service import get_freshness_limit, get_daily_limit, get_alert_limit

router = APIRouter(tags=["Auth"])


class LoginRequest(BaseModel):
    email: EmailStr


class LoginResponse(BaseModel):
    api_key: str
    user_id: str
    email: str
    company_name: str | None
    plan: str
    message: str


class SignupRequest(BaseModel):
    email: EmailStr
    company_name: str | None = None


class SignupResponse(BaseModel):
    api_key: str
    user_id: str
    email: str
    plan: str
    freshness_limit_days: int
    daily_limit: int
    message: str


def _build_freshness_info(plan: PlanTier) -> dict:
    """Build freshness tier info dict for a resolved plan."""
    limit = get_freshness_limit(plan)
    return {
        "plan": plan.value,
        "freshness_limit_days": limit,
        "daily_limit": get_daily_limit(plan),
        "alert_limit": get_alert_limit(plan),
        "can_access_hot": limit == 0,
        "can_access_warm": limit <= 30,
        "can_access_mild": limit <= 90,
        "can_access_cold": True,
        "oldest_accessible_date": (date.today() - timedelta(days=limit)).isoformat() if limit > 0 else None,
    }


class ContactRequest(BaseModel):
    name: str
    email: EmailStr
    company: str | None = None
    plan: str | None = None
    message: str | None = None


@router.post("/contact")
async def contact_sales(body: ContactRequest):
    """Submit a sales inquiry. Sends email notification."""
    import logging
    logger = logging.getLogger(__name__)
    logger.info(
        "SALES INQUIRY: name=%s email=%s company=%s plan=%s message=%s",
        body.name, body.email, body.company, body.plan, body.message,
    )

    # Try SendGrid if configured
    try:
        from app.config import settings
        if settings.SENDGRID_API_KEY:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail
            msg = Mail(
                from_email=settings.SENDGRID_FROM_EMAIL,
                to_emails="willwalterburns@gmail.com",
                subject=f"PermitLookup {body.plan or 'Sales'} Inquiry — {body.name}",
                plain_text_content=(
                    f"Name: {body.name}\n"
                    f"Email: {body.email}\n"
                    f"Company: {body.company or 'N/A'}\n"
                    f"Plan: {body.plan or 'N/A'}\n\n"
                    f"Message:\n{body.message or 'N/A'}"
                ),
            )
            sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
            sg.send(msg)
    except Exception as e:
        logger.warning("SendGrid failed: %s — inquiry logged above", e)

    return {"status": "received", "message": "Thanks! We'll be in touch within 24 hours."}


@router.post("/login", response_model=LoginResponse)
async def login(body: LoginRequest, db: AsyncSession = Depends(get_db)):
    """
    Login with email. Generates a fresh API key for the account.

    Since API keys are hashed, we can't return an existing key.
    Instead, we generate a new key for the user's account.
    """
    email = body.email.strip().lower()

    # Look up user by email
    result = await db.execute(
        select(ApiUser).where(
            func.lower(ApiUser.email) == email,
            ApiUser.is_active.is_(True),
        )
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(
            status_code=404,
            detail="No active account found with that email. Check your email or sign up for a free account.",
        )

    # Generate a fresh API key for this login session
    raw_key = ApiKey.generate_key()
    api_key = ApiKey(
        user_id=user.id,
        key_hash=hash_api_key(raw_key),
        key_prefix=raw_key[:12],
        name=f"Login {date.today().isoformat()}",
    )
    db.add(api_key)
    await db.commit()

    plan = resolve_plan(user.plan)
    return LoginResponse(
        api_key=raw_key,
        user_id=str(user.id),
        email=user.email,
        company_name=user.company_name,
        plan=plan.value,
        message="Logged in successfully. A new API key has been generated for this session.",
    )


@router.post("/signup", response_model=SignupResponse)
async def signup(body: SignupRequest, db: AsyncSession = Depends(get_db)):
    """
    Create a free account and get an API key.

    No credit card required. Free tier: 100 lookups/day, COLD data only (180+ days old).
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

    plan = resolve_plan(user.plan)
    return SignupResponse(
        api_key=raw_key,
        user_id=str(user.id),
        email=user.email,
        plan=plan.value,
        freshness_limit_days=get_freshness_limit(plan),
        daily_limit=get_daily_limit(plan),
        message="Save your API key — it won't be shown again. Include it as X-API-Key header. "
                "Free tier: COLD data only (permits 180+ days old). Upgrade for fresher data.",
    )


@router.post("/demo")
async def demo_login(db: AsyncSession = Depends(get_db)):
    """
    Get a demo API key. Creates a demo account if it doesn't exist,
    or generates a fresh key for the existing demo account.
    Intended for investors, prospects, and quick exploration.
    """
    demo_email = "demo@permitlookup.com"

    # Check if demo user exists
    result = await db.execute(
        select(ApiUser).where(func.lower(ApiUser.email) == demo_email)
    )
    user = result.scalar_one_or_none()

    if not user:
        # Create demo user with Enterprise plan for full access demo
        user = ApiUser(
            email=demo_email,
            company_name="PermitLookup Demo",
            plan=PlanTier.ENTERPRISE,
        )
        db.add(user)
        await db.flush()

    # Generate a fresh key for this demo session
    raw_key = ApiKey.generate_key()
    api_key = ApiKey(
        user_id=user.id,
        key_hash=hash_api_key(raw_key),
        key_prefix=raw_key[:12],
        name=f"Demo {date.today().isoformat()}",
    )
    db.add(api_key)
    await db.commit()

    plan = resolve_plan(user.plan)
    return {
        "api_key": raw_key,
        "user_id": str(user.id),
        "email": user.email,
        "company_name": user.company_name,
        "plan": plan.value,
        "message": "Demo account ready. Full Enterprise access for exploration.",
    }


@router.get("/me")
async def get_profile(
    request: Request,
    user: ApiUser = Depends(get_current_user),
):
    """Get current user profile with plan details and freshness tier info."""
    plan = resolve_plan(user.plan)
    return {
        "user_id": str(user.id),
        "email": user.email,
        "company_name": user.company_name,
        "is_active": user.is_active,
        "created_at": user.created_at.isoformat() if user.created_at else None,
        **_build_freshness_info(plan),
    }


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
