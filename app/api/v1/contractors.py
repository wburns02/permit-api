"""Contractor search and profile endpoints."""

from datetime import date

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_, or_, case, extract
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.permit import Permit
from app.models.data_layers import ContractorLicense

router = APIRouter(prefix="/contractors", tags=["Contractors"])


def _escape_like(s: str) -> str:
    """Escape SQL ILIKE wildcards in user input."""
    return s.replace("%", r"\%").replace("_", r"\_")


@router.get("/search")
async def search_contractors(
    request: Request,
    name: str = Query(..., min_length=2, description="Contractor name or company"),
    state: str | None = Query(None, max_length=2),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Search contractors by name or company. Returns aggregated contractor profiles
    with permit counts, active jurisdictions, and specialties.
    """
    await check_rate_limit(request, lookup_count=1)

    conditions = [
        or_(
            Permit.contractor_name.ilike(f"%{name}%"),
            Permit.contractor_company.ilike(f"%{name}%"),
        ),
        or_(
            Permit.contractor_name.is_not(None),
            Permit.contractor_company.is_not(None),
        ),
    ]
    if state:
        conditions.append(Permit.state == state.upper())

    where = and_(*conditions)

    # Aggregate by contractor_company (primary) or contractor_name
    contractor_key = func.coalesce(Permit.contractor_company, Permit.contractor_name)

    query = (
        select(
            contractor_key.label("contractor"),
            func.count().label("total_permits"),
            func.count(func.distinct(Permit.jurisdiction)).label("jurisdictions"),
            func.count(func.distinct(Permit.state)).label("states"),
            func.min(Permit.issue_date).label("first_permit"),
            func.max(Permit.issue_date).label("last_permit"),
            func.array_agg(func.distinct(Permit.permit_type)).label("permit_types"),
            func.array_agg(func.distinct(Permit.state)).label("active_states"),
            func.avg(Permit.valuation).label("avg_valuation"),
        )
        .where(where)
        .group_by(contractor_key)
        .order_by(func.count().desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )

    result = await db.execute(query)
    rows = result.all()

    # Count total
    count_q = (
        select(func.count(func.distinct(contractor_key)))
        .where(where)
    )
    total = (await db.execute(count_q)).scalar() or 0

    # Log usage
    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/contractors/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "results": [
            {
                "contractor": r.contractor,
                "total_permits": r.total_permits,
                "jurisdictions": r.jurisdictions,
                "states": r.states,
                "first_active": r.first_permit.isoformat() if r.first_permit else None,
                "last_active": r.last_permit.isoformat() if r.last_permit else None,
                "permit_types": [t for t in (r.permit_types or []) if t],
                "active_states": [s for s in (r.active_states or []) if s],
                "avg_valuation": round(r.avg_valuation, 2) if r.avg_valuation else None,
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/{contractor_name}/permits")
async def contractor_permits(
    contractor_name: str,
    request: Request,
    state: str | None = Query(None, max_length=2),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=100),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get all permits for a specific contractor. Shows their complete work history.
    """
    await check_rate_limit(request, lookup_count=1)

    from app.services.search_service import PERMIT_COLUMNS, row_to_dict

    conditions = [
        or_(
            Permit.contractor_name.ilike(f"%{contractor_name}%"),
            Permit.contractor_company.ilike(f"%{contractor_name}%"),
        ),
    ]
    if state:
        conditions.append(Permit.state == state.upper())

    where = and_(*conditions)

    query = (
        select(*PERMIT_COLUMNS)
        .where(where)
        .order_by(Permit.issue_date.desc().nullslast())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )

    result = await db.execute(query)
    rows = result.all()

    total = 0
    if rows:
        if len(rows) < page_size:
            total = (page - 1) * page_size + len(rows)
        else:
            count_q = select(func.count()).select_from(Permit).where(where)
            total = (await db.execute(count_q)).scalar()

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/contractors/permits",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "contractor": contractor_name,
        "results": [row_to_dict(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# ---------------------------------------------------------------------------
# Risk scoring helpers
# ---------------------------------------------------------------------------

def _permit_volume_score(count: int) -> int:
    """0 permits = 100 risk, 50+ = 0 risk.  Linear interpolation between."""
    if count >= 50:
        return 0
    if count <= 0:
        return 100
    return round(100 - (count / 50) * 100)


def _license_status_score(statuses: list[str | None]) -> tuple[int, str]:
    """
    Score based on best license found.
    CLEAR/Active = 0, Expired = 50, Suspended/Revoked = 100, No license = 75.
    Returns (score, description).
    """
    if not statuses:
        return 75, "No license found"

    normalized = [s.upper().strip() if s else "" for s in statuses]

    # Check for best-case first
    active_terms = {"CLEAR", "ACTIVE", "CURRENT", "VALID"}
    if any(term in st for st in normalized for term in active_terms):
        return 0, "Active/Clear license on file"

    expired_terms = {"EXPIRED", "INACTIVE", "LAPSED"}
    if any(term in st for st in normalized for term in expired_terms):
        return 50, "License expired or inactive"

    bad_terms = {"SUSPENDED", "REVOKED", "CANCELLED", "DENIED"}
    if any(term in st for st in normalized for term in bad_terms):
        return 100, "License suspended or revoked"

    # Unknown status — treat with moderate caution
    return 60, f"License status unclear ({statuses[0]})"


def _activity_recency_score(last_permit_date: date | None) -> tuple[int, str]:
    """Score based on how recently the contractor pulled a permit."""
    if last_permit_date is None:
        return 100, "No permit history"

    today = date.today()
    days_ago = (today - last_permit_date).days

    if days_ago <= 90:
        return 0, f"Active — last permit {days_ago} days ago"
    if days_ago <= 365:
        return 25, f"Recent — last permit {days_ago} days ago"
    if days_ago <= 730:
        return 50, f"Moderately stale — last permit {days_ago} days ago"
    if days_ago <= 1095:
        return 75, f"Stale — last permit {days_ago} days ago"
    return 100, f"Very stale — last permit {days_ago} days ago"


def _jurisdiction_spread_score(count: int) -> tuple[int, str]:
    """More jurisdictions = more established.  1 = 50, 3+ = 25, 5+ = 0."""
    if count >= 5:
        return 0, f"Operating in {count} jurisdictions — well established"
    if count >= 3:
        return 25, f"Operating in {count} jurisdictions"
    if count >= 1:
        return 50, f"Operating in {count} jurisdiction(s)"
    return 75, "No jurisdiction data"


def _valuation_consistency_score(
    avg_val: float | None,
    stddev_val: float | None,
    min_val: float | None,
    max_val: float | None,
    count: int,
) -> tuple[int, str]:
    """
    High coefficient of variation in project valuations = higher risk.
    With fewer than 3 permits or no valuation data, score is neutral.
    """
    if count < 3 or avg_val is None or avg_val <= 0:
        return 50, "Insufficient valuation data"

    if stddev_val is None or stddev_val <= 0:
        return 0, "Consistent project valuations"

    cv = stddev_val / avg_val  # coefficient of variation

    if cv <= 0.3:
        score = 0
        desc = "Very consistent valuations"
    elif cv <= 0.6:
        score = 20
        desc = "Fairly consistent valuations"
    elif cv <= 1.0:
        score = 40
        desc = "Moderate valuation variance"
    elif cv <= 2.0:
        score = 65
        desc = "High valuation variance"
    else:
        score = 85
        desc = "Extremely high valuation variance"

    if min_val is not None and max_val is not None and min_val > 0:
        desc += f" (range: ${min_val:,.0f}–${max_val:,.0f})"

    return score, desc


def _risk_level(composite: int) -> str:
    if composite <= 33:
        return "Low"
    if composite <= 66:
        return "Moderate"
    return "High"


# ---------------------------------------------------------------------------
# Risk score endpoint
# ---------------------------------------------------------------------------

@router.get("/{contractor_name}/risk-score")
async def contractor_risk_score(
    contractor_name: str,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Compute a composite risk score (0-100) for a contractor based on permit
    history, license status, activity recency, jurisdiction spread, and
    valuation consistency.

    Requires Pro Leads plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Contractor risk scoring requires Pro Leads plan or higher.",
        )

    await check_rate_limit(request, lookup_count=1)

    # ----- Query permit data -----
    safe_name = _escape_like(contractor_name)
    name_filter = or_(
        Permit.contractor_name.ilike(f"%{safe_name}%"),
        Permit.contractor_company.ilike(f"%{safe_name}%"),
    )

    permit_agg_q = select(
        func.count().label("total_permits"),
        func.count(func.distinct(Permit.jurisdiction)).label("jurisdiction_count"),
        func.min(Permit.issue_date).label("first_permit"),
        func.max(Permit.issue_date).label("last_permit"),
        func.avg(Permit.valuation).label("avg_valuation"),
        func.stddev(Permit.valuation).label("stddev_valuation"),
        func.min(Permit.valuation).label("min_valuation"),
        func.max(Permit.valuation).label("max_valuation"),
        func.array_agg(func.distinct(Permit.jurisdiction)).label("jurisdictions"),
        func.array_agg(func.distinct(Permit.state)).label("active_states"),
        func.array_agg(func.distinct(Permit.permit_type)).label("permit_types"),
    ).where(name_filter)

    permit_result = await db.execute(permit_agg_q)
    pdata = permit_result.one()

    total_permits = pdata.total_permits or 0

    # ----- Query license data (before 404 check so we have both) -----
    license_q = select(ContractorLicense.status, ContractorLicense.license_number).where(
        or_(
            ContractorLicense.business_name.ilike(f"%{safe_name}%"),
            ContractorLicense.full_business_name.ilike(f"%{safe_name}%"),
        )
    )
    license_result = await db.execute(license_q)
    license_rows = license_result.all()
    license_statuses = [r.status for r in license_rows]
    license_numbers = [r.license_number for r in license_rows]

    # ----- 404 if no data at all -----
    if total_permits == 0 and len(license_rows) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No permit or license data found for contractor '{contractor_name}'.",
        )

    # ----- Compute sub-scores -----
    vol_score = _permit_volume_score(total_permits)

    lic_score, lic_desc = _license_status_score(license_statuses)

    recency_score, recency_desc = _activity_recency_score(pdata.last_permit)

    jurisd_score, jurisd_desc = _jurisdiction_spread_score(pdata.jurisdiction_count or 0)

    val_score, val_desc = _valuation_consistency_score(
        avg_val=float(pdata.avg_valuation) if pdata.avg_valuation else None,
        stddev_val=float(pdata.stddev_valuation) if pdata.stddev_valuation else None,
        min_val=float(pdata.min_valuation) if pdata.min_valuation else None,
        max_val=float(pdata.max_valuation) if pdata.max_valuation else None,
        count=total_permits,
    )

    # ----- Weighted composite -----
    weights = {
        "permit_volume": 0.25,
        "license_status": 0.25,
        "activity_recency": 0.20,
        "jurisdiction_spread": 0.15,
        "valuation_consistency": 0.15,
    }
    composite = round(
        vol_score * weights["permit_volume"]
        + lic_score * weights["license_status"]
        + recency_score * weights["activity_recency"]
        + jurisd_score * weights["jurisdiction_spread"]
        + val_score * weights["valuation_consistency"]
    )
    composite = max(0, min(100, composite))  # clamp

    level = _risk_level(composite)

    # ----- Contributing factors (human-readable) -----
    contributing_factors = []
    if vol_score >= 50:
        contributing_factors.append(
            f"Low permit volume ({total_permits} permits)"
        )
    if lic_score >= 50:
        contributing_factors.append(lic_desc)
    if recency_score >= 50:
        contributing_factors.append(recency_desc)
    if jurisd_score >= 50:
        contributing_factors.append(jurisd_desc)
    if val_score >= 50:
        contributing_factors.append(val_desc)

    if not contributing_factors:
        contributing_factors.append("No significant risk factors identified")

    # ----- Log usage -----
    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/contractors/risk-score",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "contractor": contractor_name,
        "composite_score": composite,
        "risk_level": level,
        "breakdown": {
            "permit_volume": {
                "score": vol_score,
                "weight": weights["permit_volume"],
                "detail": f"{total_permits} permits on record",
            },
            "license_status": {
                "score": lic_score,
                "weight": weights["license_status"],
                "detail": lic_desc,
                "licenses_found": len(license_rows),
                "license_numbers": license_numbers[:5],  # cap to avoid huge payloads
            },
            "activity_recency": {
                "score": recency_score,
                "weight": weights["activity_recency"],
                "detail": recency_desc,
                "first_permit": pdata.first_permit.isoformat() if pdata.first_permit else None,
                "last_permit": pdata.last_permit.isoformat() if pdata.last_permit else None,
            },
            "jurisdiction_spread": {
                "score": jurisd_score,
                "weight": weights["jurisdiction_spread"],
                "detail": jurisd_desc,
                "jurisdictions": [j for j in (pdata.jurisdictions or []) if j][:10],
                "active_states": [s for s in (pdata.active_states or []) if s],
            },
            "valuation_consistency": {
                "score": val_score,
                "weight": weights["valuation_consistency"],
                "detail": val_desc,
                "avg_valuation": round(float(pdata.avg_valuation), 2) if pdata.avg_valuation else None,
                "permit_types": [t for t in (pdata.permit_types or []) if t],
            },
        },
        "contributing_factors": contributing_factors,
    }
