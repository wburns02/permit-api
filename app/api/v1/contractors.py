"""Contractor search and profile endpoints.

Searches contractor_licenses (503K records — FL, CA) and prospect_contacts
(16M records — TX, IL, IA, CO, etc.) instead of permits.applicant_name which
is NULL for all 835M rows.
"""

from datetime import date

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_, or_, case, extract, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_read_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, resolve_plan
from app.services.usage_logger import log_usage
from app.models.permit import Permit
from app.models.data_layers import ContractorLicense
from app.services.response_guard import guard_response

router = APIRouter(prefix="/contractors", tags=["Contractors"])

# States covered by the contractor_licenses table
_LICENSE_STATES = {"FL", "CA"}


def _escape_like(s: str) -> str:
    """Escape SQL ILIKE wildcards in user input."""
    return s.replace("%", r"\%").replace("_", r"\_")


@router.get("/search")
async def search_contractors(
    request: Request,
    name: str | None = Query(None, min_length=2, description="Contractor name or company"),
    state: str | None = Query(None, max_length=2),
    city: str | None = Query(None, description="City filter"),
    page: int = Query(1, ge=1, le=20),
    page_size: int = Query(20, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Search contractors by name, state, or city.

    Queries contractor_licenses (FL, CA) and prospect_contacts (TX, IL, IA,
    CO, etc.) — the two tables that actually contain contractor data.
    """
    await check_rate_limit(request, lookup_count=1)

    # At least one search parameter required
    if not name and not state and not city:
        raise HTTPException(
            status_code=400,
            detail="At least one search parameter required (name, state, or city).",
        )

    norm_state = state.upper() if state else None
    name_pattern = f"%{_escape_like(name)}%" if name else None
    city_pattern = f"%{_escape_like(city)}%" if city else None
    offset = (page - 1) * page_size

    results_list: list[dict] = []
    total = 0

    # Set query timeout to prevent runaway scans
    await db.execute(text("SET LOCAL statement_timeout = '10s'"))

    # ------------------------------------------------------------------
    # 1. Query contractor_licenses (FL, CA)
    # ------------------------------------------------------------------
    search_licenses = (norm_state is None or norm_state in _LICENSE_STATES)

    if search_licenses:
        cl_conditions = []
        if norm_state:
            cl_conditions.append(ContractorLicense.state == norm_state)
        else:
            # When no state filter, include license states
            pass
        if name:
            safe = _escape_like(name)
            cl_conditions.append(
                or_(
                    ContractorLicense.business_name.ilike(f"%{safe}%"),
                    ContractorLicense.full_business_name.ilike(f"%{safe}%"),
                )
            )
        if city:
            cl_conditions.append(ContractorLicense.city.ilike(city_pattern))

        cl_where = and_(*cl_conditions) if cl_conditions else True

        # Count
        count_q = select(func.count()).select_from(ContractorLicense).where(cl_where)
        cl_total = (await db.execute(count_q)).scalar() or 0

        # Data
        cl_query = (
            select(
                ContractorLicense.business_name,
                ContractorLicense.license_number,
                ContractorLicense.classifications,
                ContractorLicense.status,
                ContractorLicense.state,
                ContractorLicense.city,
                ContractorLicense.phone,
                ContractorLicense.source,
            )
            .where(cl_where)
            .order_by(ContractorLicense.business_name)
            .limit(page_size)
            .offset(offset)
        )
        cl_rows = (await db.execute(cl_query)).all()

        for r in cl_rows:
            results_list.append({
                "name": r.business_name,
                "license_number": r.license_number,
                "license_type": r.classifications,
                "status": r.status,
                "state": r.state,
                "city": r.city,
                "phone": r.phone,
                "email": None,
                "source": r.source,
            })
        total += cl_total

    # ------------------------------------------------------------------
    # 2. Query prospect_contacts for states NOT in contractor_licenses
    #    (or when no state filter is given)
    # ------------------------------------------------------------------
    search_prospects = (norm_state is None or norm_state not in _LICENSE_STATES)

    if search_prospects:
        # How many slots remain on this page after license results
        remaining = page_size - len(results_list)
        # Adjust offset: if licenses consumed some total, shift prospect offset
        if search_licenses and norm_state is None:
            prospect_offset = max(0, offset - cl_total)
        else:
            prospect_offset = offset

        if remaining > 0 or not search_licenses:
            if not search_licenses:
                remaining = page_size
                prospect_offset = offset

            # Build WHERE clause dynamically to avoid asyncpg ambiguous param errors
            pc_conditions = ["1=1"]
            params: dict = {"limit": remaining, "offset": prospect_offset}

            if norm_state:
                pc_conditions.append("state = :state")
                params["state"] = norm_state
            if name_pattern:
                pc_conditions.append("name ILIKE :name_pattern")
                params["name_pattern"] = name_pattern
            if city_pattern:
                pc_conditions.append("city ILIKE :city_pattern")
                params["city_pattern"] = city_pattern

            pc_where = " AND ".join(pc_conditions)

            pc_count_sql = text(f"SELECT count(*) FROM prospect_contacts WHERE {pc_where}")
            pc_total = (await db.execute(pc_count_sql, params)).scalar() or 0

            pc_query = text(f"""
                SELECT name, license_number, license_type, status,
                       state, city, phone, email, source
                FROM prospect_contacts
                WHERE {pc_where}
                ORDER BY name
                LIMIT :limit OFFSET :offset
            """)
            pc_rows = (await db.execute(pc_query, params)).all()

            for r in pc_rows:
                results_list.append({
                    "name": r.name,
                    "license_number": r.license_number,
                    "license_type": r.license_type,
                    "status": r.status,
                    "state": r.state,
                    "city": r.city,
                    "phone": r.phone,
                    "email": r.email,
                    "source": r.source,
                })
            total += pc_total

    # Log usage
    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/contractors/search",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    # Apply security layers
    guarded_results, sec_meta = await guard_response(request, results_list, page=page)

    return {
        "results": guarded_results,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/{contractor_name}/details")
async def contractor_details(
    contractor_name: str,
    request: Request,
    state: str | None = Query(None, max_length=2),
    page: int = Query(1, ge=1, le=20),
    page_size: int = Query(25, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Get license details for a specific contractor across both
    contractor_licenses and prospect_contacts tables.
    """
    await check_rate_limit(request, lookup_count=1)

    await db.execute(text("SET LOCAL statement_timeout = '10s'"))

    safe_name = _escape_like(contractor_name)
    norm_state = state.upper() if state else None
    offset = (page - 1) * page_size
    results: list[dict] = []
    total = 0

    # Search contractor_licenses
    cl_conditions = [
        or_(
            ContractorLicense.business_name.ilike(f"%{safe_name}%"),
            ContractorLicense.full_business_name.ilike(f"%{safe_name}%"),
        )
    ]
    if norm_state:
        cl_conditions.append(ContractorLicense.state == norm_state)

    cl_where = and_(*cl_conditions)
    cl_count = (await db.execute(
        select(func.count()).select_from(ContractorLicense).where(cl_where)
    )).scalar() or 0

    cl_rows = (await db.execute(
        select(
            ContractorLicense.business_name,
            ContractorLicense.license_number,
            ContractorLicense.classifications,
            ContractorLicense.status,
            ContractorLicense.state,
            ContractorLicense.city,
            ContractorLicense.phone,
            ContractorLicense.address,
            ContractorLicense.issue_date,
            ContractorLicense.expiration_date,
            ContractorLicense.source,
        )
        .where(cl_where)
        .order_by(ContractorLicense.business_name)
        .limit(page_size)
        .offset(offset)
    )).all()

    for r in cl_rows:
        results.append({
            "name": r.business_name,
            "license_number": r.license_number,
            "license_type": r.classifications,
            "status": r.status,
            "state": r.state,
            "city": r.city,
            "phone": r.phone,
            "email": None,
            "address": r.address,
            "issue_date": r.issue_date.isoformat() if r.issue_date else None,
            "expiration_date": r.expiration_date.isoformat() if r.expiration_date else None,
            "source": r.source,
        })
    total += cl_count

    # Search prospect_contacts
    remaining = page_size - len(results)
    if remaining > 0:
        pc_offset = max(0, offset - cl_count)
        pc_conditions = ["name ILIKE :name_pattern"]
        pc_params: dict = {
            "name_pattern": f"%{safe_name}%",
            "limit": remaining,
            "offset": pc_offset,
        }
        if norm_state:
            pc_conditions.append("state = :state")
            pc_params["state"] = norm_state

        pc_where = " AND ".join(pc_conditions)

        pc_count = (await db.execute(
            text(f"SELECT count(*) FROM prospect_contacts WHERE {pc_where}"),
            pc_params,
        )).scalar() or 0

        pc_rows = (await db.execute(text(f"""
            SELECT name, license_number, license_type, status,
                   state, city, phone, email, address, source
            FROM prospect_contacts
            WHERE {pc_where}
            ORDER BY name
            LIMIT :limit OFFSET :offset
        """), pc_params)).all()

        for r in pc_rows:
            results.append({
                "name": r.name,
                "license_number": r.license_number,
                "license_type": r.license_type,
                "status": r.status,
                "state": r.state,
                "city": r.city,
                "phone": r.phone,
                "email": r.email,
                "address": r.address,
                "issue_date": None,
                "expiration_date": None,
                "source": r.source,
            })
        total += pc_count

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/contractors/details",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

    return {
        "contractor": contractor_name,
        "results": results,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/{contractor_name}/permits")
async def contractor_permits(
    contractor_name: str,
    request: Request,
    state: str | None = Query(None, max_length=2),
    page: int = Query(1, ge=1, le=20),
    page_size: int = Query(25, ge=1, le=50),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Get all permits for a specific contractor. Shows their complete work history.
    Note: This queries permits.applicant_name which may be sparse.
    """
    await check_rate_limit(request, lookup_count=1)
    await db.execute(text("SET LOCAL statement_timeout = '10s'"))

    from app.services.search_service import PERMIT_COLUMNS, row_to_dict

    safe_name = _escape_like(contractor_name)
    conditions = [
        Permit.applicant_name.ilike(f"%{safe_name}%"),
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

    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/contractors/permits",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

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


def _county_spread_score(count: int) -> tuple[int, str]:
    """More counties = more established.  1 = 50, 3+ = 25, 5+ = 0."""
    if count >= 5:
        return 0, f"Operating in {count} counties — well established"
    if count >= 3:
        return 25, f"Operating in {count} counties"
    if count >= 1:
        return 50, f"Operating in {count} county/counties"
    return 75, "No county data"


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
    db: AsyncSession = Depends(get_read_db),
):
    """
    Compute a composite risk score (0-100) for a contractor based on permit
    history, license status, activity recency, and county spread.

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
    name_filter = Permit.applicant_name.ilike(f"%{safe_name}%")

    permit_agg_q = select(
        func.count().label("total_permits"),
        func.count(func.distinct(Permit.county)).label("county_count"),
        func.min(Permit.issue_date).label("first_permit"),
        func.max(Permit.issue_date).label("last_permit"),
        func.array_agg(func.distinct(Permit.county)).label("counties"),
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

    county_score, county_desc = _county_spread_score(pdata.county_count or 0)

    # ----- Weighted composite (rebalanced without valuation) -----
    weights = {
        "permit_volume": 0.30,
        "license_status": 0.30,
        "activity_recency": 0.25,
        "county_spread": 0.15,
    }
    composite = round(
        vol_score * weights["permit_volume"]
        + lic_score * weights["license_status"]
        + recency_score * weights["activity_recency"]
        + county_score * weights["county_spread"]
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
    if county_score >= 50:
        contributing_factors.append(county_desc)

    if not contributing_factors:
        contributing_factors.append("No significant risk factors identified")

    # ----- Log usage -----
    log_usage(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/contractors/risk-score",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )

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
            "county_spread": {
                "score": county_score,
                "weight": weights["county_spread"],
                "detail": county_desc,
                "counties": [c for c in (pdata.counties or []) if c][:10],
                "active_states": [s for s in (pdata.active_states or []) if s],
            },
        },
        "contributing_factors": contributing_factors,
        "permit_types": [t for t in (pdata.permit_types or []) if t],
    }
