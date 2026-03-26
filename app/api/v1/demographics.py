"""Census demographics endpoints — market intelligence overlay."""

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy import select, func, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_read_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, PlanTier, UsageLog, resolve_plan
from app.models.data_layers import CensusDemographics
from app.services.fast_counts import fast_count

router = APIRouter(prefix="/demographics", tags=["Demographics"])

# State FIPS to abbreviation mapping
FIPS_TO_STATE = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "72": "PR",
}

STATE_TO_FIPS = {v: k for k, v in FIPS_TO_STATE.items()}


@router.get("/county")
async def county_demographics(
    request: Request,
    state: str = Query(..., max_length=2, description="State abbreviation"),
    county_fips: str = Query(..., max_length=3, description="County FIPS code (3 digits)"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Get aggregated demographics for a county — median income, home values,
    population, homeownership rates.

    Requires Explorer plan or higher.
    """
    plan = resolve_plan(user.plan)
    if plan == PlanTier.FREE:
        raise HTTPException(
            status_code=403,
            detail="Demographics data requires Explorer plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    state_fips = STATE_TO_FIPS.get(state.upper())
    if not state_fips:
        raise HTTPException(status_code=400, detail=f"Unknown state: {state}")

    query = (
        select(
            func.sum(CensusDemographics.population).label("total_population"),
            func.avg(CensusDemographics.median_income).label("avg_median_income"),
            func.avg(CensusDemographics.median_home_value).label("avg_median_home_value"),
            func.avg(CensusDemographics.homeownership_rate).label("avg_homeownership_rate"),
            func.avg(CensusDemographics.median_year_built).label("avg_median_year_built"),
            func.sum(CensusDemographics.total_housing_units).label("total_housing_units"),
            func.avg(CensusDemographics.vacancy_rate).label("avg_vacancy_rate"),
            func.count().label("tract_count"),
        )
        .where(
            and_(
                CensusDemographics.state_fips == state_fips,
                CensusDemographics.county_fips == county_fips,
            )
        )
    )
    result = (await db.execute(query)).one_or_none()

    if not result or not result.total_population:
        raise HTTPException(status_code=404, detail="No census data found for this county.")

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/demographics/county",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "state": state.upper(),
        "county_fips": county_fips,
        "total_population": result.total_population,
        "median_income": round(result.avg_median_income) if result.avg_median_income else None,
        "median_home_value": round(result.avg_median_home_value) if result.avg_median_home_value else None,
        "homeownership_rate": round(result.avg_homeownership_rate, 1) if result.avg_homeownership_rate else None,
        "median_year_built": round(result.avg_median_year_built) if result.avg_median_year_built else None,
        "total_housing_units": result.total_housing_units,
        "vacancy_rate": round(result.avg_vacancy_rate, 1) if result.avg_vacancy_rate else None,
        "census_tracts": result.tract_count,
    }


@router.get("/state")
async def state_demographics(
    request: Request,
    state: str = Query(..., max_length=2, description="State abbreviation"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """Get aggregated demographics for an entire state."""
    plan = resolve_plan(user.plan)
    if plan == PlanTier.FREE:
        raise HTTPException(
            status_code=403,
            detail="Demographics data requires Explorer plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    state_fips = STATE_TO_FIPS.get(state.upper())
    if not state_fips:
        raise HTTPException(status_code=400, detail=f"Unknown state: {state}")

    # Per-county breakdown
    county_q = (
        select(
            CensusDemographics.county_fips,
            func.sum(CensusDemographics.population).label("population"),
            func.avg(CensusDemographics.median_income).label("median_income"),
            func.avg(CensusDemographics.median_home_value).label("median_home_value"),
            func.avg(CensusDemographics.homeownership_rate).label("homeownership_rate"),
            func.sum(CensusDemographics.total_housing_units).label("housing_units"),
        )
        .where(CensusDemographics.state_fips == state_fips)
        .group_by(CensusDemographics.county_fips)
        .order_by(func.sum(CensusDemographics.population).desc().nullslast())
    )
    counties = (await db.execute(county_q)).all()

    if not counties:
        raise HTTPException(status_code=404, detail="No census data found for this state.")

    # State totals
    total_pop = sum(c.population or 0 for c in counties)
    total_units = sum(c.housing_units or 0 for c in counties)
    incomes = [c.median_income for c in counties if c.median_income]
    values = [c.median_home_value for c in counties if c.median_home_value]

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/demographics/state",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "state": state.upper(),
        "total_population": total_pop,
        "median_income": round(sum(incomes) / len(incomes)) if incomes else None,
        "median_home_value": round(sum(values) / len(values)) if values else None,
        "total_housing_units": total_units,
        "county_count": len(counties),
        "counties": [
            {
                "county_fips": c.county_fips,
                "population": c.population,
                "median_income": round(c.median_income) if c.median_income else None,
                "median_home_value": round(c.median_home_value) if c.median_home_value else None,
                "homeownership_rate": round(c.homeownership_rate, 1) if c.homeownership_rate else None,
                "housing_units": c.housing_units,
            }
            for c in counties[:50]
        ],
    }


@router.get("/tract")
async def tract_demographics(
    request: Request,
    state: str = Query(..., max_length=2),
    county_fips: str = Query(..., max_length=3),
    tract: str = Query(..., max_length=6),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """Get block-group level demographics for a specific census tract."""
    plan = resolve_plan(user.plan)
    if plan in (PlanTier.FREE, PlanTier.EXPLORER, PlanTier.STARTER):
        raise HTTPException(
            status_code=403,
            detail="Tract-level demographics requires Pro Leads plan or higher."
        )

    await check_rate_limit(request, lookup_count=1)

    state_fips = STATE_TO_FIPS.get(state.upper())
    if not state_fips:
        raise HTTPException(status_code=400, detail=f"Unknown state: {state}")

    query = (
        select(CensusDemographics)
        .where(
            and_(
                CensusDemographics.state_fips == state_fips,
                CensusDemographics.county_fips == county_fips,
                CensusDemographics.tract == tract,
            )
        )
        .order_by(CensusDemographics.block_group)
    )
    result = await db.execute(query)
    block_groups = result.scalars().all()

    if not block_groups:
        raise HTTPException(status_code=404, detail="No census data found for this tract.")

    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/demographics/tract",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "state": state.upper(),
        "county_fips": county_fips,
        "tract": tract,
        "block_groups": [
            {
                "block_group": bg.block_group,
                "name": bg.name,
                "population": bg.population,
                "median_income": bg.median_income,
                "median_home_value": bg.median_home_value,
                "homeownership_rate": bg.homeownership_rate,
                "median_year_built": bg.median_year_built,
                "total_housing_units": bg.total_housing_units,
                "vacancy_rate": bg.vacancy_rate,
            }
            for bg in block_groups
        ],
    }


@router.get("/stats")
async def demographics_stats(
    request: Request,
    db: AsyncSession = Depends(get_read_db),
):
    """Public endpoint — census demographics database statistics."""
    total = await fast_count(db, "census_demographics")

    states = (await db.execute(
        select(
            CensusDemographics.state_fips,
            func.count().label("count"),
            func.sum(CensusDemographics.population).label("population"),
        )
        .group_by(CensusDemographics.state_fips)
        .order_by(func.sum(CensusDemographics.population).desc().nullslast())
        .limit(20)
    )).all()

    return {
        "total_block_groups": total,
        "states_covered": len(states),
        "top_states": [
            {
                "state": FIPS_TO_STATE.get(r.state_fips, r.state_fips),
                "block_groups": r.count,
                "population": r.population,
            }
            for r in states
        ],
    }
