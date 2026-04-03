"""Address normalization and permit search logic."""

import re
from datetime import date, timedelta
from sqlalchemy import select, func, text, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.permit import Permit, Jurisdiction

# Columns to select for search results (avoid loading search_vector/raw_data)
PERMIT_COLUMNS = [
    Permit.id, Permit.permit_number, Permit.address, Permit.city, Permit.state,
    Permit.zip, Permit.county, Permit.lat, Permit.lng, Permit.permit_type,
    Permit.work_type, Permit.trade, Permit.category, Permit.status,
    Permit.description, Permit.issue_date,
    Permit.owner_name, Permit.applicant_name, Permit.source,
]


# Standard street suffix abbreviations (USPS Publication 28)
STREET_ABBREVS = {
    "avenue": "ave", "boulevard": "blvd", "circle": "cir", "court": "ct",
    "drive": "dr", "expressway": "expy", "freeway": "fwy", "highway": "hwy",
    "lane": "ln", "parkway": "pkwy", "place": "pl", "road": "rd",
    "square": "sq", "street": "st", "terrace": "ter", "trail": "trl",
    "way": "way",
}

DIRECTION_ABBREVS = {
    "north": "n", "south": "s", "east": "e", "west": "w",
    "northeast": "ne", "northwest": "nw", "southeast": "se", "southwest": "sw",
}


def normalize_address(address: str) -> str:
    """Normalize an address for consistent matching."""
    if not address:
        return ""

    addr = address.strip().upper()

    # Remove unit/suite/apt designators for base matching
    addr = re.sub(r'\b(SUITE|STE|UNIT|APT|#)\s*\S+', '', addr)

    # Standardize directions
    for full, abbr in DIRECTION_ABBREVS.items():
        addr = re.sub(rf'\b{full.upper()}\b', abbr.upper(), addr)

    # Standardize street suffixes
    for full, abbr in STREET_ABBREVS.items():
        addr = re.sub(rf'\b{full.upper()}\b', abbr.upper(), addr)

    # Remove extra whitespace and punctuation
    addr = re.sub(r'[.,#]', '', addr)
    addr = re.sub(r'\s+', ' ', addr).strip()

    return addr


def build_filter_conditions(filters: dict) -> list:
    """Build SQLAlchemy filter conditions from a filters dict.

    Shared by search_permits() and alert_engine.match_alert().
    Supported keys: address, city, state, zip_code/zip, permit_type, status,
    contractor, keyword, date_from, date_to.
    """
    conditions = []
    address = filters.get("address")
    city = filters.get("city")
    state = filters.get("state")
    zip_code = filters.get("zip_code") or filters.get("zip")
    permit_type = filters.get("permit_type")
    status = filters.get("status")
    contractor = filters.get("contractor")
    keyword = filters.get("keyword")
    date_from = filters.get("date_from")
    date_to = filters.get("date_to")

    if address:
        # Use ILIKE on address column (address_normalized doesn't exist on T430)
        normalized = normalize_address(address)
        conditions.append(Permit.address.ilike(f"%{normalized}%"))
    if city:
        conditions.append(func.upper(Permit.city) == city.upper())
    if state:
        # Use direct equality — data is uppercase, and this enables partition pruning
        conditions.append(Permit.state == state.upper())
    if zip_code:
        conditions.append(Permit.zip == zip_code)
    if permit_type:
        conditions.append(func.upper(Permit.permit_type) == permit_type.upper())
    if status:
        conditions.append(func.upper(Permit.status) == status.upper())
    if contractor:
        # T430 has applicant_name instead of contractor_name/contractor_company
        conditions.append(Permit.applicant_name.ilike(f"%{contractor}%"))
    if keyword:
        conditions.append(
            or_(
                Permit.description.ilike(f"%{keyword}%"),
                Permit.address.ilike(f"%{keyword}%"),
                Permit.owner_name.ilike(f"%{keyword}%"),
            )
        )
    if date_from:
        conditions.append(Permit.issue_date >= date_from)
    if date_to:
        conditions.append(Permit.issue_date <= date_to)

    # Data freshness enforcement — restrict how recent the data can be
    freshness_limit_days = filters.get("freshness_limit_days")
    if freshness_limit_days is not None and freshness_limit_days > 0:
        cutoff = date.today() - timedelta(days=freshness_limit_days)
        # User can only see permits with issue_date AT OR BEFORE the cutoff
        conditions.append(Permit.issue_date <= cutoff)

    return conditions


async def search_permits(
    db: AsyncSession,
    address: str | None = None,
    city: str | None = None,
    state: str | None = None,
    zip_code: str | None = None,
    permit_type: str | None = None,
    status: str | None = None,
    jurisdiction: str | None = None,
    contractor: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    page: int = 1,
    page_size: int = 25,
    freshness_limit_days: int | None = None,
) -> dict:
    """Search permits with text matching.

    Args:
        freshness_limit_days: If >0, only return permits with issue_date
            at least this many days old. 0 or None = no restriction.
    """
    conditions = build_filter_conditions({
        "address": address, "city": city, "state": state, "zip_code": zip_code,
        "permit_type": permit_type, "status": status,
        "contractor": contractor, "date_from": date_from, "date_to": date_to,
        "freshness_limit_days": freshness_limit_days,
    })

    # jurisdiction filter: no longer a column on permits, ignore silently
    # (jurisdictions table still works independently)

    if not conditions:
        return {"results": [], "total": 0, "page": page, "page_size": page_size}

    where_clause = and_(*conditions)

    # Set a statement timeout to avoid blocking on slow queries
    await db.execute(text("SET LOCAL statement_timeout = '15s'"))

    query = (
        select(*PERMIT_COLUMNS)
        .where(where_clause)
        .distinct(Permit.permit_number, Permit.address)
        .order_by(Permit.permit_number, Permit.address, Permit.issue_date.desc().nullslast())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )

    try:
        result = await db.execute(query)
        rows = result.all()
    except Exception as e:
        if "statement timeout" in str(e) or "cancel" in str(e).lower():
            # Retry without ORDER BY — much faster on unindexed tables
            await db.rollback()
            await db.execute(text("SET LOCAL statement_timeout = '15s'"))
            query_fast = (
                select(*PERMIT_COLUMNS)
                .where(where_clause)
                .distinct(Permit.permit_number, Permit.address)
                .limit(page_size)
            )
            result = await db.execute(query_fast)
            rows = result.all()
        else:
            raise

    # Fast total: skip expensive COUNT(*) on 800M+ rows
    # Exact counts on millions of rows take 60-120s with no index
    total = 0
    if rows:
        if len(rows) < page_size:
            total = (page - 1) * page_size + len(rows)
        else:
            # Estimate: we know there are many results, give a reasonable cap
            # Real count would block the API for minutes on unindexed 833M rows
            total = page_size * 1000  # "25,000+ results" is informative enough

    return {
        "results": [row_to_dict(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


async def geo_search_permits(
    db: AsyncSession,
    lat: float,
    lng: float,
    radius_miles: float = 0.5,
    permit_type: str | None = None,
    page: int = 1,
    page_size: int = 25,
    freshness_limit_days: int | None = None,
) -> dict:
    """Search permits within a radius of lat/lng using Haversine approximation.

    Args:
        freshness_limit_days: If >0, only return permits with issue_date
            at least this many days old. 0 or None = no restriction.
    """
    deg_per_mile = 0.0145
    lat_range = radius_miles * deg_per_mile
    lng_range = radius_miles * deg_per_mile * 1.2

    conditions = [
        Permit.lat.is_not(None),
        Permit.lng.is_not(None),
        Permit.lat.between(lat - lat_range, lat + lat_range),
        Permit.lng.between(lng - lng_range, lng + lng_range),
    ]

    if permit_type:
        conditions.append(func.upper(Permit.permit_type) == permit_type.upper())

    # Data freshness enforcement
    if freshness_limit_days is not None and freshness_limit_days > 0:
        cutoff = date.today() - timedelta(days=freshness_limit_days)
        conditions.append(Permit.issue_date <= cutoff)

    where_clause = and_(*conditions)

    count_q = select(func.count()).select_from(Permit).where(where_clause)
    total = (await db.execute(count_q)).scalar()

    query = (
        select(*PERMIT_COLUMNS)
        .where(where_clause)
        .order_by(Permit.issue_date.desc().nullslast())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )

    result = await db.execute(query)
    rows = result.all()

    return {
        "results": [row_to_dict(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


async def get_coverage(db: AsyncSession) -> list[dict]:
    """Get list of supported jurisdictions with record counts."""
    cols = [
        Jurisdiction.name,
        Jurisdiction.state,
        Jurisdiction.record_count,
        Jurisdiction.source,
        Jurisdiction.last_updated,
    ]
    q = select(*cols).order_by(Jurisdiction.record_count.desc())
    result = await db.execute(q)

    return [
        {
            "name": j.name,
            "state": j.state,
            "record_count": j.record_count,
            "source": j.source,
            "last_updated": j.last_updated.isoformat() if j.last_updated else None,
        }
        for j in result.all()
    ]


def row_to_dict(r) -> dict:
    """Convert a Row tuple (from column-based select) to API response dict."""
    return {
        "id": str(r.id),
        "permit_number": r.permit_number,
        "address": r.address,
        "city": r.city,
        "state": r.state,
        "zip": r.zip,
        "county": r.county,
        "lat": r.lat,
        "lng": r.lng,
        "permit_type": r.permit_type,
        "work_type": r.work_type,
        "trade": r.trade,
        "category": r.category,
        "status": r.status,
        "description": r.description,
        "issue_date": r.issue_date.isoformat() if r.issue_date else None,
        "owner_name": r.owner_name,
        "applicant_name": r.applicant_name,
        "source": r.source,
    }
