"""Address normalization and permit search logic."""

import re
from sqlalchemy import select, func, text, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.permit import Permit, Jurisdiction

# Columns to select for search results (avoid loading search_vector)
PERMIT_COLUMNS = [
    Permit.id, Permit.permit_number, Permit.address, Permit.city, Permit.state,
    Permit.zip, Permit.lat, Permit.lng, Permit.permit_type, Permit.work_type,
    Permit.trade, Permit.status, Permit.description, Permit.valuation,
    Permit.issue_date, Permit.created_date, Permit.completed_date,
    Permit.owner_name, Permit.contractor_name, Permit.contractor_company,
    Permit.jurisdiction, Permit.source,
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
) -> dict:
    """Search permits with full-text and trigram matching."""
    conditions = []

    if address:
        normalized = normalize_address(address)
        conditions.append(
            text("similarity(address_normalized, :addr) > 0.3").bindparams(addr=normalized)
        )

    if city:
        conditions.append(Permit.city.ilike(city))
    if state:
        conditions.append(Permit.state.ilike(state))
    if zip_code:
        conditions.append(Permit.zip == zip_code)
    if permit_type:
        conditions.append(Permit.permit_type.ilike(permit_type))
    if status:
        conditions.append(Permit.status.ilike(status))
    if jurisdiction:
        conditions.append(Permit.jurisdiction.ilike(f"%{jurisdiction}%"))
    if contractor:
        conditions.append(
            or_(
                Permit.contractor_name.ilike(f"%{contractor}%"),
                Permit.contractor_company.ilike(f"%{contractor}%"),
            )
        )
    if date_from:
        conditions.append(Permit.issue_date >= date_from)
    if date_to:
        conditions.append(Permit.issue_date <= date_to)

    if not conditions:
        return {"results": [], "total": 0, "page": page, "page_size": page_size}

    where_clause = and_(*conditions)

    if address:
        normalized = normalize_address(address)
        query = (
            select(*PERMIT_COLUMNS)
            .where(where_clause)
            .order_by(
                text("similarity(address_normalized, :addr) DESC").bindparams(addr=normalized),
                Permit.issue_date.desc().nullslast(),
            )
            .offset((page - 1) * page_size)
            .limit(page_size)
        )
    else:
        query = (
            select(*PERMIT_COLUMNS)
            .where(where_clause)
            .order_by(Permit.issue_date.desc().nullslast())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )

    result = await db.execute(query)
    rows = result.all()

    # Only run COUNT if results exist and page is full
    total = 0
    if rows:
        if len(rows) < page_size:
            total = (page - 1) * page_size + len(rows)
        else:
            count_q = select(func.count()).select_from(Permit).where(where_clause)
            total = (await db.execute(count_q)).scalar()

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
) -> dict:
    """Search permits within a radius of lat/lng using Haversine approximation."""
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
    cols = [Jurisdiction.name, Jurisdiction.state, Jurisdiction.record_count,
            Jurisdiction.source, Jurisdiction.last_updated]
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
        "lat": r.lat,
        "lng": r.lng,
        "permit_type": r.permit_type,
        "work_type": r.work_type,
        "trade": r.trade,
        "status": r.status,
        "description": r.description,
        "valuation": r.valuation,
        "issue_date": r.issue_date.isoformat() if r.issue_date else None,
        "created_date": r.created_date.isoformat() if r.created_date else None,
        "completed_date": r.completed_date.isoformat() if r.completed_date else None,
        "owner_name": r.owner_name,
        "contractor_name": r.contractor_name,
        "contractor_company": r.contractor_company,
        "jurisdiction": r.jurisdiction,
        "source": r.source,
    }
