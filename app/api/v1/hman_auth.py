"""H-Man Electrical CRM — JWT authentication endpoints.

Separate from the existing API-key auth system.
Uses email + password to issue a 30-day JWT token.
"""

import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from pydantic import BaseModel, EmailStr
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models.api_key import ApiUser, PlanTier

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/hman", tags=["H-Man Auth"])

# 30-day token expiry
JWT_EXPIRE_DAYS = 30

http_bearer = HTTPBearer(auto_error=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hash_password(password: str) -> str:
    """SHA-256 hash of password (single-user system, not high-security)."""
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _create_jwt(user_id: str, email: str) -> str:
    """Create a signed JWT token valid for JWT_EXPIRE_DAYS days."""
    payload = {
        "sub": user_id,
        "email": email,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS),
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def _decode_jwt(token: str) -> dict:
    """Decode and validate a JWT token. Raises HTTPException on failure."""
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        return payload
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class SetupRequest(BaseModel):
    email: EmailStr
    password: str
    name: str | None = None


class SetupResponse(BaseModel):
    token: str
    user_id: str
    email: str
    name: str | None
    plan: str
    message: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class LoginResponse(BaseModel):
    token: str
    user_id: str
    email: str
    name: str | None
    plan: str


class MeResponse(BaseModel):
    user_id: str
    email: str
    name: str | None
    plan: str
    is_active: bool


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/setup", response_model=SetupResponse, status_code=201)
async def hman_setup(body: SetupRequest, db: AsyncSession = Depends(get_db)):
    """
    One-time account creation for H-Man CRM.

    Creates an ApiUser with PRO_LEADS plan and stores password hash.
    Returns a JWT for immediate use.
    """
    email = body.email.strip().lower()

    # Check if user already exists
    result = await db.execute(
        select(ApiUser).where(func.lower(ApiUser.email) == email)
    )
    existing = result.scalar_one_or_none()
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An account with that email already exists. Use /hman/login instead.",
        )

    # Create user with password hash
    password_hash = _hash_password(body.password)
    user = ApiUser(
        email=email,
        company_name=body.name or "H-Man Electrical",
        plan=PlanTier.PRO_LEADS,
        is_active=True,
    )
    db.add(user)
    await db.flush()  # Get the UUID assigned

    # Store password_hash via raw SQL update (column added via auto-migration)
    from sqlalchemy import text, update
    await db.execute(
        text("UPDATE api_users SET password_hash = :ph WHERE id = :uid"),
        {"ph": password_hash, "uid": str(user.id)},
    )
    await db.commit()

    token = _create_jwt(str(user.id), user.email)

    logger.info("HMAN SETUP: email=%s", email)

    return SetupResponse(
        token=token,
        user_id=str(user.id),
        email=user.email,
        name=user.company_name,
        plan=PlanTier.PRO_LEADS.value,
        message="Account created. Save your token — it expires in 30 days.",
    )


@router.post("/login", response_model=LoginResponse)
async def hman_login(body: LoginRequest, db: AsyncSession = Depends(get_db)):
    """
    Login for H-Man CRM. Validates email + password, returns JWT.
    """
    email = body.email.strip().lower()

    result = await db.execute(
        select(ApiUser).where(
            func.lower(ApiUser.email) == email,
            ApiUser.is_active.is_(True),
        )
    )
    user = result.scalar_one_or_none()

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    # Fetch password_hash via raw SQL (nullable column)
    from sqlalchemy import text
    row = await db.execute(
        text("SELECT password_hash FROM api_users WHERE id = :uid"),
        {"uid": str(user.id)},
    )
    stored_hash = row.scalar_one_or_none()

    if stored_hash is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Password not set for this account. Use /hman/setup first.",
        )

    if stored_hash != _hash_password(body.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    token = _create_jwt(str(user.id), user.email)

    logger.info("HMAN LOGIN: email=%s", email)

    return LoginResponse(
        token=token,
        user_id=str(user.id),
        email=user.email,
        name=user.company_name,
        plan=user.plan.value if user.plan else PlanTier.PRO_LEADS.value,
    )


@router.get("/me", response_model=MeResponse)
async def hman_me(
    credentials: HTTPAuthorizationCredentials = Depends(http_bearer),
    db: AsyncSession = Depends(get_db),
):
    """
    Validate JWT and return current user info.

    Requires: Authorization: Bearer <token>
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    payload = _decode_jwt(credentials.credentials)
    user_id = payload.get("sub")

    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token payload",
        )

    result = await db.execute(
        select(ApiUser).where(ApiUser.id == user_id)
    )
    user = result.scalar_one_or_none()

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    return MeResponse(
        user_id=str(user.id),
        email=user.email,
        name=user.company_name,
        plan=user.plan.value if user.plan else PlanTier.PRO_LEADS.value,
        is_active=user.is_active,
    )
