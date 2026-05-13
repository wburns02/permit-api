"""Parcel Screen API — California state-law qualification + yield calculator.

Auth model: standard X-API-Key + allowlist gate via PARCEL_SCREEN_ALLOWED_USERS
env var (comma-separated user UUIDs). Phase 1 access is restricted to Will + Rob.

Origin: Rob's `.claude/skills/parcel-screen/` Claude Code skill, productized
into parcels.ecbtx.com.
"""

import asyncio
import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session_maker, get_db
from app.middleware.api_key_auth import get_current_user
from app.models.api_key import ApiUser
from app.models.parcel_screen import (
    ParcelHotPick,
    ParcelJurisdiction,
    ParcelScreen,
    ParcelStateLaw,
)
from app.services.parcel_hot_picks import refresh_city
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


# ---------------------------------------------------------------------------
# Hot Picks (Ladder 1) — bulk-scored leaderboard
# ---------------------------------------------------------------------------
class HotPicksRefreshRequest(BaseModel):
    state: str = Field("CA", min_length=2, max_length=2)
    city_slug: str = Field(..., min_length=1, max_length=80)


@router.get("/hot-picks")
async def list_hot_picks(
    state: str = Query("CA", min_length=2, max_length=2),
    city: str = Query(..., min_length=1, max_length=80),
    path: str | None = Query(None, description="Filter by best_path (substring match)"),
    min_yield: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Top-N candidate parcels for a city, ranked by score (= max_units desc).

    Returns the pre-computed Ladder 1 leaderboard. Refresh via
    `scripts/refresh_hot_picks.py` (preferred) or `POST /hot-picks/refresh`.
    """
    _require_allowlist(user)

    stmt = (
        select(ParcelHotPick)
        .where(
            ParcelHotPick.state == state.upper(),
            ParcelHotPick.city_slug == city.lower(),
            ParcelHotPick.max_units >= min_yield,
        )
        .order_by(ParcelHotPick.score.desc(), ParcelHotPick.acres.desc().nullslast())
        .limit(limit)
    )
    if path:
        stmt = stmt.where(ParcelHotPick.best_path.ilike(f"%{path}%"))

    result = await db.execute(stmt)
    rows = result.scalars().all()
    return {
        "state": state.upper(),
        "city_slug": city.lower(),
        "total": len(rows),
        "picks": [
            {
                "apn": r.apn,
                "address": r.address,
                "owner_name": r.owner_name,
                "acres": float(r.acres) if r.acres is not None else None,
                "zone_code": r.zone_code,
                "gp_code": r.gp_code,
                "fire_zone": r.fire_zone,
                "impr_value": float(r.impr_value) if r.impr_value is not None else None,
                "lat": float(r.lat) if r.lat is not None else None,
                "lng": float(r.lng) if r.lng is not None else None,
                "max_units": r.max_units,
                "best_path": r.best_path,
                "eligible_paths": r.eligible_paths or [],
                "score": float(r.score) if r.score is not None else 0.0,
                "refreshed_at": r.refreshed_at.isoformat() if r.refreshed_at else None,
            }
            for r in rows
        ],
    }


@router.get("/hot-picks/stats")
async def hot_picks_stats(
    state: str = Query("CA", min_length=2, max_length=2),
    city: str = Query(..., min_length=1, max_length=80),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Counts by yield tier + last refresh timestamp for a city."""
    _require_allowlist(user)

    s = state.upper()
    c = city.lower()

    total_q = await db.execute(
        select(func.count())
        .select_from(ParcelHotPick)
        .where(ParcelHotPick.state == s, ParcelHotPick.city_slug == c)
    )
    total = total_q.scalar_one()

    # Yield tiers — buckets we care about for the leaderboard UI
    tier_q = await db.execute(
        select(
            func.count().filter(ParcelHotPick.max_units >= 10).label("ge_10"),
            func.count().filter(ParcelHotPick.max_units >= 5).label("ge_5"),
            func.count().filter(ParcelHotPick.max_units >= 4).label("ge_4"),
            func.count().filter(ParcelHotPick.max_units >= 3).label("ge_3"),
            func.count().filter(ParcelHotPick.max_units >= 2).label("ge_2"),
        ).where(ParcelHotPick.state == s, ParcelHotPick.city_slug == c)
    )
    tier_row = tier_q.one()

    last_q = await db.execute(
        select(func.max(ParcelHotPick.refreshed_at))
        .where(ParcelHotPick.state == s, ParcelHotPick.city_slug == c)
    )
    last_refresh = last_q.scalar_one()

    top_path_q = await db.execute(
        select(ParcelHotPick.best_path, func.count().label("n"))
        .where(ParcelHotPick.state == s, ParcelHotPick.city_slug == c)
        .group_by(ParcelHotPick.best_path)
        .order_by(func.count().desc())
        .limit(10)
    )
    by_path = [{"best_path": row[0], "count": row[1]} for row in top_path_q.all()]

    return {
        "state": s,
        "city_slug": c,
        "total": total,
        "tiers": {
            "ge_10": tier_row.ge_10,
            "ge_5": tier_row.ge_5,
            "ge_4": tier_row.ge_4,
            "ge_3": tier_row.ge_3,
            "ge_2": tier_row.ge_2,
        },
        "by_path": by_path,
        "last_refreshed_at": last_refresh.isoformat() if last_refresh else None,
    }


@router.post("/hot-picks/refresh")
async def refresh_hot_picks(
    body: HotPicksRefreshRequest,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Kick off a refresh for one city. Returns immediately; runs in background.

    The CLI (`scripts/refresh_hot_picks.py`) is the preferred path for full
    refreshes — this endpoint is a convenience for ad-hoc admin use that
    avoids hitting Railway's edge timeout on long pulls.
    """
    _require_allowlist(user)

    state = body.state.upper()
    city_slug = body.city_slug.lower()

    # Resolve jurisdiction up front so we can fail fast with a 404.
    result = await db.execute(
        select(ParcelJurisdiction).where(
            ParcelJurisdiction.state == state,
            ParcelJurisdiction.city_slug == city_slug,
        )
    )
    jurisdiction = result.scalar_one_or_none()
    if not jurisdiction:
        raise HTTPException(status_code=404, detail=f"jurisdiction not registered: {state}/{city_slug}")

    # The injected db session is tied to this request's lifecycle and will be
    # closed when this handler returns — so spawn the background task with a
    # fresh session. We also need to re-fetch the jurisdiction in that session
    # since SQLAlchemy 2.0 objects are bound to their original session.
    async def _bg(state_: str, city_slug_: str) -> None:
        try:
            async with async_session_maker() as bg_db:
                bg_result = await bg_db.execute(
                    select(ParcelJurisdiction).where(
                        ParcelJurisdiction.state == state_,
                        ParcelJurisdiction.city_slug == city_slug_,
                    )
                )
                bg_juris = bg_result.scalar_one_or_none()
                if not bg_juris:
                    logger.error(f"refresh_city: jurisdiction vanished mid-task: {state_}/{city_slug_}")
                    return
                stats = await refresh_city(bg_db, bg_juris)
                logger.info(f"refresh_city {state_}/{city_slug_} done: {stats}")
        except Exception:
            logger.exception(f"refresh_city {state_}/{city_slug_} failed")

    asyncio.create_task(_bg(state, city_slug))

    return {
        "status": "started",
        "state": state,
        "city_slug": city_slug,
        "note": "Running asynchronously; poll /hot-picks/stats for progress.",
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
