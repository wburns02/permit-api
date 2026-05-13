"""Parcel Screen API — California state-law qualification + yield calculator.

Auth model: standard X-API-Key + allowlist gate via PARCEL_SCREEN_ALLOWED_USERS
env var (comma-separated user UUIDs). Phase 1 access is restricted to Will + Rob.

Origin: Rob's `.claude/skills/parcel-screen/` Claude Code skill, productized
into parcels.ecbtx.com.
"""

import asyncio
import json
import logging
import os
import time

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
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

try:
    from anthropic import AsyncAnthropic
except ImportError:  # pragma: no cover — anthropic is in requirements.txt
    AsyncAnthropic = None

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


# ---------------------------------------------------------------------------
# Ladder 2 — Natural-language Hunt
# ---------------------------------------------------------------------------
# Anthropic translation prompt: returns a JSON filter spec with whitelisted
# keys ONLY. The engine drops anything outside this list, so an LLM-induced
# typo or hallucinated key can't reach SQL.
_HUNT_FILTER_KEYS: dict[str, str] = {
    # numeric thresholds
    "max_units_gte": "int",
    "acres_gte": "float",
    "acres_lte": "float",
    "impr_value_lte": "float",
    "impr_value_gte": "float",
    # boolean / equality
    "impr_value_eq_zero": "bool",
    # str-list "starts with any of"
    "zone_starts_with": "str_list",
    "gp_starts_with": "str_list",
    # str-list "best_path contains any of"
    "best_path_contains": "str_list",
    # plain substring matches
    "address_contains": "str",
    "owner_contains": "str",
}

_HUNT_SYSTEM_PROMPT = """You translate a real-estate developer's English question into a JSON filter spec for a parcel-search engine. The engine ranks pre-scored parcels by max_units desc and returns the top N.

Return ONLY a JSON object — no prose, no markdown fences. The object's keys MUST be a subset of:

  max_units_gte         : integer (e.g. {"max_units_gte": 4})
  acres_gte             : number  (acres floor)
  acres_lte             : number  (acres ceiling)
  impr_value_lte        : number  (improvement value $ ceiling)
  impr_value_gte        : number  (improvement value $ floor)
  impr_value_eq_zero    : true    (use ONLY for "vacant" / "no improvement")
  zone_starts_with      : ["R", "RH", "C-2"]   (list of zone-code prefixes)
  gp_starts_with        : ["RH", "MU"]         (list of general-plan-code prefixes)
  best_path_contains    : ["sb1123", "sb684"]  (list of state-law slugs to match)
  address_contains      : "elm"   (plain substring of the address)
  owner_contains        : "smith" (plain substring of owner name)

Notes:
- "vacant" / "no improvements" => impr_value_eq_zero=true
- "SB-1123" / "SB1123" => best_path_contains=["sb1123"]
- "SB-684" => ["sb684"]; "ADU" => ["state-adu","sb1211"]; "SB-9" => ["sb9"]
- "AB-2011" / "AB2011" => ["ab2011-sb6"]
- "SFR" / "single family" => zone_starts_with=["R-1","R1","SFR"]
- "multifamily" / "MF" => zone_starts_with=["R-2","R-3","R2","R3","RM","RH"]
- "commercial" => zone_starts_with=["C","CG","CC","CN"]
- "with high yield" / "lots of units" => max_units_gte=4
- "big" / "large lots" => acres_gte=0.5

Omit any key the user didn't ask for. Do NOT invent keys. Do NOT add explanations.
"""


def _coerce_filter_spec(raw: dict) -> dict:
    """Drop unknown keys and coerce known keys to their typed shape.

    LLM output is untrusted — only keys present in _HUNT_FILTER_KEYS survive,
    and each one is coerced (or dropped on coercion failure) before reaching
    SQL. This is the only barrier between LLM output and the query builder.
    """
    spec: dict = {}
    if not isinstance(raw, dict):
        return spec
    for key, typ in _HUNT_FILTER_KEYS.items():
        if key not in raw:
            continue
        val = raw[key]
        try:
            if typ == "int":
                spec[key] = int(val)
            elif typ == "float":
                spec[key] = float(val)
            elif typ == "bool":
                # Only honor a literal true — anything else drops the key
                if val is True:
                    spec[key] = True
            elif typ == "str_list":
                if isinstance(val, list):
                    cleaned = [str(v)[:40].strip() for v in val if str(v).strip()]
                    if cleaned:
                        spec[key] = cleaned[:20]   # cap list length
            elif typ == "str":
                s = str(val).strip()[:80]
                if s:
                    spec[key] = s
        except (TypeError, ValueError):
            continue
    return spec


def _build_hunt_query(
    state: str,
    city_slug: str | None,
    spec: dict,
    limit: int,
) -> tuple[str, dict]:
    """Build a parameter-bound SELECT against parcel_hot_picks from a clean spec.

    All user-supplied values are bound parameters — never string-interpolated.
    Returns (sql, params).
    """
    where_clauses: list[str] = ["state = :state"]
    params: dict = {"state": state}

    if city_slug:
        where_clauses.append("city_slug = :city_slug")
        params["city_slug"] = city_slug

    if "max_units_gte" in spec:
        where_clauses.append("max_units >= :max_units_gte")
        params["max_units_gte"] = spec["max_units_gte"]
    if "acres_gte" in spec:
        where_clauses.append("acres >= :acres_gte")
        params["acres_gte"] = spec["acres_gte"]
    if "acres_lte" in spec:
        where_clauses.append("acres <= :acres_lte")
        params["acres_lte"] = spec["acres_lte"]
    if "impr_value_lte" in spec:
        where_clauses.append("impr_value <= :impr_value_lte")
        params["impr_value_lte"] = spec["impr_value_lte"]
    if "impr_value_gte" in spec:
        where_clauses.append("impr_value >= :impr_value_gte")
        params["impr_value_gte"] = spec["impr_value_gte"]
    if spec.get("impr_value_eq_zero") is True:
        where_clauses.append("impr_value = 0")

    if "zone_starts_with" in spec:
        # ANY(:zone_prefixes) against zone_code with LIKE via array unnest.
        # We build a clause: zone_code ILIKE ANY(ARRAY[:zone_p0, :zone_p1, ...])
        names = []
        for i, prefix in enumerate(spec["zone_starts_with"]):
            key = f"zone_p{i}"
            params[key] = f"{prefix}%"
            names.append(f":{key}")
        where_clauses.append(f"zone_code ILIKE ANY(ARRAY[{', '.join(names)}])")
    if "gp_starts_with" in spec:
        names = []
        for i, prefix in enumerate(spec["gp_starts_with"]):
            key = f"gp_p{i}"
            params[key] = f"{prefix}%"
            names.append(f":{key}")
        where_clauses.append(f"gp_code ILIKE ANY(ARRAY[{', '.join(names)}])")
    if "best_path_contains" in spec:
        names = []
        for i, frag in enumerate(spec["best_path_contains"]):
            key = f"bp_{i}"
            params[key] = f"%{frag}%"
            names.append(f":{key}")
        where_clauses.append(f"best_path ILIKE ANY(ARRAY[{', '.join(names)}])")
    if "address_contains" in spec:
        where_clauses.append("address ILIKE :addr_contains")
        params["addr_contains"] = f"%{spec['address_contains']}%"
    if "owner_contains" in spec:
        where_clauses.append("owner_name ILIKE :owner_contains")
        params["owner_contains"] = f"%{spec['owner_contains']}%"

    params["limit"] = limit

    sql = f"""
        SELECT
            state, city_slug, apn, address, owner_name, acres, zone_code, gp_code,
            fire_zone, impr_value, lat, lng, max_units, best_path, eligible_paths,
            score, refreshed_at
        FROM parcel_hot_picks
        WHERE {' AND '.join(where_clauses)}
        ORDER BY score DESC, acres DESC NULLS LAST
        LIMIT :limit
    """
    return sql, params


def _hunt_get_client():
    if not AsyncAnthropic:
        return None
    key = getattr(settings, "ANTHROPIC_API_KEY", None) or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return None
    return AsyncAnthropic(api_key=key, timeout=18.0)


async def _hunt_translate(client, question: str) -> dict:
    """Call Haiku to translate the question to a JSON filter spec. Returns {} on bad output."""
    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=400,
        messages=[{
            "role": "user",
            "content": f"{_HUNT_SYSTEM_PROMPT}\n\nQuestion: {question}",
        }],
    )
    raw = resp.content[0].text.strip()
    # Strip markdown fences if Haiku wrapped it
    if raw.startswith("```"):
        lines = [ln for ln in raw.split("\n") if not ln.strip().startswith("```")]
        raw = "\n".join(lines).strip()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("[hunt] translator returned non-JSON: %r", raw[:200])
        return {}
    return _coerce_filter_spec(parsed)


async def _hunt_rationales(
    client, question: str, spec: dict, picks: list[dict]
) -> list[str]:
    """Generate a one-sentence rationale per pick. Returns [] on failure (graceful)."""
    if not picks:
        return []
    # Send a compact representation per pick to keep tokens small
    summary_picks = [
        {
            "i": i,
            "apn": p.get("apn"),
            "address": p.get("address"),
            "acres": p.get("acres"),
            "zone": p.get("zone_code"),
            "gp": p.get("gp_code"),
            "max_units": p.get("max_units"),
            "best_path": p.get("best_path"),
            "impr_value": p.get("impr_value"),
        }
        for i, p in enumerate(picks)
    ]
    prompt = (
        f"User question: {question}\n"
        f"Filter spec used: {json.dumps(spec, default=str)}\n"
        f"Picks (top {len(picks)} by score):\n{json.dumps(summary_picks, default=str)}\n\n"
        "For EACH pick, write one short sentence (<= 20 words) explaining why it matches the question — "
        "cite the concrete fact (zone, acres, vacancy, best_path) that earned it the spot. "
        "Return ONLY a JSON array of strings in the same order as `i`. No markdown, no prose."
    )
    resp = await client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=800,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = resp.content[0].text.strip()
    if raw.startswith("```"):
        lines = [ln for ln in raw.split("\n") if not ln.strip().startswith("```")]
        raw = "\n".join(lines).strip()
    try:
        arr = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("[hunt] rationale returned non-JSON: %r", raw[:200])
        return []
    if not isinstance(arr, list):
        return []
    # Trim/pad to length(picks); clamp each to 280 chars
    out = []
    for i in range(len(picks)):
        if i < len(arr) and isinstance(arr[i], str):
            out.append(arr[i].strip()[:280])
        else:
            out.append("")
    return out


class HuntRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=500)
    state: str = Field(default="CA", min_length=2, max_length=2)
    city_slug: str | None = Field(default=None, max_length=80)
    limit: int = Field(default=20, ge=1, le=50)


@router.post("/hunt")
async def parcel_hunt(
    body: HuntRequest,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Ladder 2 — natural-language parcel hunt over the pre-scored Hot Picks set.

    Haiku translates the question to a whitelisted JSON filter spec. The engine
    builds a parameter-bound SELECT and returns the top N picks. A second
    Haiku call attaches a one-sentence rationale per pick (graceful-degrades to
    blank rationales on failure). Hard-capped under 25s end-to-end.
    """
    _require_allowlist(user)

    client = _hunt_get_client()
    if not client:
        raise HTTPException(
            status_code=503,
            detail="Hunt is temporarily unavailable. ANTHROPIC_API_KEY not configured.",
        )

    started = time.monotonic()
    state = body.state.upper()
    city_slug = body.city_slug.lower() if body.city_slug else None

    # ── Step 1: Anthropic translation (bounded) ──────────────────────────
    try:
        spec = await asyncio.wait_for(_hunt_translate(client, body.question), timeout=20.0)
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail="Translator timed out. Try a shorter or more specific question.",
        )
    except Exception as e:
        logger.exception("[hunt] translation failed")
        raise HTTPException(
            status_code=422,
            detail={"error": f"Could not translate question: {e}", "question": body.question},
        )

    if not spec:
        # Empty spec is allowed (it just falls back to top-N by score) — but warn.
        logger.info("[hunt] empty filter spec for question=%r", body.question)

    # ── Step 2: Build + execute parameter-bound SQL ─────────────────────
    sql, params = _build_hunt_query(state, city_slug, spec, body.limit)
    try:
        await db.execute(text("SET LOCAL statement_timeout = '8000'"))
        result = await db.execute(text(sql), params)
        columns = list(result.keys())
        rows = [dict(zip(columns, row)) for row in result.fetchall()]
    except Exception as e:
        logger.exception("[hunt] SQL execution failed")
        raise HTTPException(status_code=502, detail=f"Query failed: {e}")

    # Serialize for JSON (numeric → float, datetime → iso)
    picks: list[dict] = []
    for r in rows:
        picks.append({
            "state": r.get("state"),
            "city_slug": r.get("city_slug"),
            "apn": r.get("apn"),
            "address": r.get("address"),
            "owner_name": r.get("owner_name"),
            "acres": float(r["acres"]) if r.get("acres") is not None else None,
            "zone_code": r.get("zone_code"),
            "gp_code": r.get("gp_code"),
            "fire_zone": r.get("fire_zone"),
            "impr_value": float(r["impr_value"]) if r.get("impr_value") is not None else None,
            "lat": float(r["lat"]) if r.get("lat") is not None else None,
            "lng": float(r["lng"]) if r.get("lng") is not None else None,
            "max_units": int(r["max_units"]) if r.get("max_units") is not None else 0,
            "best_path": r.get("best_path"),
            "eligible_paths": r.get("eligible_paths") or [],
            "score": float(r["score"]) if r.get("score") is not None else 0.0,
            "refreshed_at": r["refreshed_at"].isoformat() if r.get("refreshed_at") else None,
        })

    # ── Step 3: Rationale pass (graceful — never break the response) ────
    rationales: list[str] = []
    elapsed = time.monotonic() - started
    remaining = max(0.0, 22.0 - elapsed)   # leave headroom for serialization
    if picks and remaining > 4.0:
        try:
            rationales = await asyncio.wait_for(
                _hunt_rationales(client, body.question, spec, picks),
                timeout=min(remaining, 14.0),
            )
        except Exception as e:
            logger.warning("[hunt] rationale generation failed (graceful): %s", e)
            rationales = []

    # Stitch rationales into picks (empty string when missing)
    for i, p in enumerate(picks):
        p["rationale"] = rationales[i] if i < len(rationales) else ""

    wall = round(time.monotonic() - started, 3)

    ai_summary = (
        f"Found {len(picks)} matching parcels"
        f" in {city_slug.upper() if city_slug else state}"
        f" using filters: {', '.join(sorted(spec.keys())) or 'none — top by score'}."
    )

    return {
        "question": body.question,
        "state": state,
        "city_slug": city_slug,
        "filter_spec": spec,
        "ai_summary": ai_summary,
        "picks": picks,
        "total": len(picks),
        "wall_clock_s": wall,
    }


# ---------------------------------------------------------------------------
# Ladder 3 — Map FeatureCollection
# ---------------------------------------------------------------------------
@router.get("/map")
async def parcel_map(
    state: str = Query("CA", min_length=2, max_length=2),
    city: str = Query(..., min_length=1, max_length=80, description="city_slug"),
    min_yield: int = Query(2, ge=0),
    limit: int = Query(500, ge=10, le=2000),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Ladder 3 — GeoJSON FeatureCollection of top-N scored parcels with polygons.

    Returns a single FeatureCollection that the frontend can pipe directly into
    a MapLibre source. Top-level metadata (`total_in_city`, `total_with_geometry`,
    `returned`, `last_refreshed_at`) is included alongside the standard GeoJSON
    keys for UI status badges.
    """
    _require_allowlist(user)

    s = state.upper()
    c = city.lower()

    # Counts first — cheap, helpful for the UI's "X of Y" display.
    total_q = await db.execute(
        select(func.count())
        .select_from(ParcelHotPick)
        .where(ParcelHotPick.state == s, ParcelHotPick.city_slug == c)
    )
    total_in_city = int(total_q.scalar_one() or 0)

    geom_q = await db.execute(
        select(func.count())
        .select_from(ParcelHotPick)
        .where(
            ParcelHotPick.state == s,
            ParcelHotPick.city_slug == c,
            ParcelHotPick.geometry_wgs84.isnot(None),
        )
    )
    total_with_geometry = int(geom_q.scalar_one() or 0)

    last_q = await db.execute(
        select(func.max(ParcelHotPick.refreshed_at))
        .where(ParcelHotPick.state == s, ParcelHotPick.city_slug == c)
    )
    last_refresh = last_q.scalar_one()

    # Main query — ranked picks with polygon present.
    stmt = (
        select(ParcelHotPick)
        .where(
            ParcelHotPick.state == s,
            ParcelHotPick.city_slug == c,
            ParcelHotPick.max_units >= min_yield,
            ParcelHotPick.geometry_wgs84.isnot(None),
        )
        .order_by(
            ParcelHotPick.score.desc(),
            ParcelHotPick.acres.desc().nullslast(),
        )
        .limit(limit)
    )
    result = await db.execute(stmt)
    rows = result.scalars().all()

    features = []
    for r in rows:
        features.append({
            "type": "Feature",
            "geometry": r.geometry_wgs84,
            "properties": {
                "apn": r.apn,
                "address": r.address,
                "owner_name": r.owner_name,
                "acres": float(r.acres) if r.acres is not None else None,
                "zone_code": r.zone_code,
                "gp_code": r.gp_code,
                "max_units": r.max_units,
                "best_path": r.best_path,
                "score": float(r.score) if r.score is not None else 0.0,
                "lat": float(r.lat) if r.lat is not None else None,
                "lng": float(r.lng) if r.lng is not None else None,
            },
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "total_in_city": total_in_city,
        "total_with_geometry": total_with_geometry,
        "returned": len(features),
        "last_refreshed_at": last_refresh.isoformat() if last_refresh else None,
        "state": s,
        "city_slug": c,
    }
