"""Parcel Screen API — California state-law qualification + yield calculator.

Auth model: standard X-API-Key + allowlist gate via PARCEL_SCREEN_ALLOWED_USERS
env var (comma-separated user UUIDs). Phase 1 access is restricted to Will + Rob.

Origin: Rob's `.claude/skills/parcel-screen/` Claude Code skill, productized
into parcels.ecbtx.com.
"""

import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.models.api_key import ApiUser
from app.models.parcel_screen import (
    ParcelJurisdiction,
    ParcelScreen,
    ParcelStateLaw,
)
from app.services.parcel_screen_service import run_parcel_screen

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/parcel-screen", tags=["parcel-screen"])


# ---------------------------------------------------------------------------
# Allowlist gate
# ---------------------------------------------------------------------------
def _allowed_users() -> set[str]:
    raw = os.environ.get("PARCEL_SCREEN_ALLOWED_USERS", "").strip()
    if not raw:
        return set()
    return {u.strip() for u in raw.split(",") if u.strip()}


def _require_allowlist(user: ApiUser) -> None:
    allow = _allowed_users()
    if not allow:
        # Closed by default — empty allowlist means feature disabled
        raise HTTPException(
            status_code=403,
            detail="Parcel Screen is restricted. Admin: set PARCEL_SCREEN_ALLOWED_USERS env var.",
        )
    if str(user.id) not in allow:
        raise HTTPException(
            status_code=403,
            detail="Your account is not enabled for Parcel Screen. Contact admin.",
        )


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
class ScreenRequest(BaseModel):
    state: str = Field(..., min_length=2, max_length=2, description="2-letter state code")
    city_slug: str = Field(..., min_length=1, max_length=80)
    address: str | None = None
    apn: str | None = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@router.get("/health")
async def health(user: ApiUser = Depends(get_current_user)):
    """Auth + allowlist check probe. Returns OK only if caller is allowlisted."""
    _require_allowlist(user)
    return {"status": "ok", "user_id": str(user.id), "email": user.email}


@router.get("/jurisdictions")
async def list_jurisdictions(
    state: str | None = Query(None, min_length=2, max_length=2),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List registered jurisdictions (cities/counties with cached GIS endpoints)."""
    _require_allowlist(user)

    stmt = select(ParcelJurisdiction).order_by(
        ParcelJurisdiction.state, ParcelJurisdiction.city_slug
    )
    if state:
        stmt = stmt.where(ParcelJurisdiction.state == state.upper())

    result = await db.execute(stmt)
    rows = result.scalars().all()
    return {
        "jurisdictions": [
            {
                "state": j.state,
                "city_slug": j.city_slug,
                "display_name": j.display_name,
                "gis_viewer_url": j.gis_viewer_url,
                "apn_field": j.apn_field,
                "address_field": j.address_field,
                "last_verified": j.last_verified.isoformat() if j.last_verified else None,
                "notes": j.notes,
            }
            for j in rows
        ],
        "total": len(rows),
    }


@router.get("/state-laws")
async def list_state_laws(
    state: str = Query("CA", min_length=2, max_length=2),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List state laws on file for a state, with staleness flags."""
    _require_allowlist(user)

    stmt = (
        select(ParcelStateLaw)
        .where(ParcelStateLaw.state == state.upper())
        .order_by(ParcelStateLaw.display_order)
    )
    result = await db.execute(stmt)
    rows = result.scalars().all()
    return {
        "laws": [
            {
                "law_id": law.law_id,
                "name": law.name,
                "code_section": law.code_section,
                "summary": law.summary,
                "leginfo_url": law.leginfo_url,
                "last_verified": law.last_verified.isoformat() if law.last_verified else None,
                "stale_warning": law.last_verified is None,
            }
            for law in rows
        ],
        "total": len(rows),
    }


@router.post("")
async def run_screen(
    body: ScreenRequest,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Main parcel-screen endpoint. Returns the full memo as JSON."""
    _require_allowlist(user)

    if not body.address and not body.apn:
        raise HTTPException(status_code=400, detail="Must provide address or apn")

    try:
        result = await run_parcel_screen(
            db=db,
            state=body.state.upper(),
            city_slug=body.city_slug.lower(),
            address=body.address,
            apn=body.apn,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("parcel screen failed")
        raise HTTPException(status_code=502, detail=f"Screen failed: {e}")

    # Save to audit log
    if result.get("status") == "ok":
        screen_row = ParcelScreen(
            user_id=user.id,
            state=body.state.upper(),
            city_slug=body.city_slug.lower(),
            address=body.address,
            apn=body.apn or (result.get("parcel") or {}).get("apn"),
            result=result,
        )
        db.add(screen_row)
        await db.commit()
        result["screen_id"] = str(screen_row.id)

    return result


@router.get("/screens")
async def list_my_screens(
    limit: int = Query(50, ge=1, le=200),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List the current user's saved screens (newest first)."""
    _require_allowlist(user)

    stmt = (
        select(ParcelScreen)
        .where(ParcelScreen.user_id == user.id)
        .order_by(ParcelScreen.created_at.desc())
        .limit(limit)
    )
    result = await db.execute(stmt)
    rows = result.scalars().all()
    return {
        "screens": [
            {
                "id": str(s.id),
                "state": s.state,
                "city_slug": s.city_slug,
                "address": s.address,
                "apn": s.apn,
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "summary": {
                    "max_yield": _summarize_max_yield(s.result),
                    "parcel_acres": (s.result or {}).get("parcel", {}).get("acres"),
                    "zone_code": (s.result or {}).get("parcel", {}).get("zone_code"),
                },
            }
            for s in rows
        ],
        "total": len(rows),
    }


@router.get("/screens/{screen_id}")
async def get_screen(
    screen_id: str,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch a saved screen by ID (only the owner can read it)."""
    _require_allowlist(user)

    import uuid as _uuid
    try:
        sid = _uuid.UUID(screen_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid screen id")

    stmt = select(ParcelScreen).where(
        ParcelScreen.id == sid, ParcelScreen.user_id == user.id
    )
    result = await db.execute(stmt)
    screen = result.scalar_one_or_none()
    if not screen:
        raise HTTPException(status_code=404, detail="screen not found")
    return screen.result


def _summarize_max_yield(result: dict | None) -> int | None:
    """Pull the highest max_units across eligible laws for the history list."""
    if not result or not isinstance(result, dict):
        return None
    laws = result.get("laws") or []
    yields = []
    for law in laws:
        elig = law.get("eligibility", {})
        yld = law.get("yield", {})
        if elig.get("auto_eligible") and isinstance(yld.get("max_units"), int):
            yields.append(yld["max_units"])
    return max(yields) if yields else None
