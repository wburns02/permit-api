"""Saved search CRUD and execution endpoints."""

from uuid import UUID
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.middleware.api_key_auth import get_current_user
from app.middleware.rate_limit import check_rate_limit
from app.models.api_key import ApiUser, UsageLog
from app.models.saved_search import SavedSearch
from app.services.search_service import build_filter_conditions, PERMIT_COLUMNS, row_to_dict
from app.models.permit import Permit
from sqlalchemy import and_

router = APIRouter(prefix="/saved-searches", tags=["Saved Searches"])


class SavedSearchCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    filters: dict = Field(..., description="Filter criteria")


class SavedSearchUpdate(BaseModel):
    name: str | None = None
    filters: dict | None = None


@router.get("")
async def list_saved_searches(
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """List all saved searches for the current user."""
    result = await db.execute(
        select(SavedSearch)
        .where(SavedSearch.user_id == user.id)
        .order_by(SavedSearch.created_at.desc())
    )
    searches = result.scalars().all()
    return {
        "saved_searches": [
            {
                "id": str(s.id),
                "name": s.name,
                "filters": s.filters,
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "last_run_at": s.last_run_at.isoformat() if s.last_run_at else None,
            }
            for s in searches
        ],
        "total": len(searches),
    }


@router.post("")
async def create_saved_search(
    body: SavedSearchCreate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new saved search."""
    # Limit to 50 saved searches
    count_result = await db.execute(
        select(func.count()).select_from(SavedSearch).where(SavedSearch.user_id == user.id)
    )
    if (count_result.scalar() or 0) >= 50:
        raise HTTPException(status_code=403, detail="Saved search limit reached (50).")

    search = SavedSearch(user_id=user.id, name=body.name, filters=body.filters)
    db.add(search)
    await db.commit()
    await db.refresh(search)

    return {
        "id": str(search.id),
        "name": search.name,
        "filters": search.filters,
        "created_at": search.created_at.isoformat() if search.created_at else None,
    }


@router.put("/{search_id}")
async def update_saved_search(
    search_id: UUID,
    body: SavedSearchUpdate,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Update a saved search."""
    result = await db.execute(
        select(SavedSearch).where(SavedSearch.id == search_id, SavedSearch.user_id == user.id)
    )
    search = result.scalar_one_or_none()
    if not search:
        raise HTTPException(status_code=404, detail="Saved search not found.")

    updates = body.model_dump(exclude_unset=True)
    for k, v in updates.items():
        setattr(search, k, v)
    await db.commit()
    await db.refresh(search)

    return {
        "id": str(search.id),
        "name": search.name,
        "filters": search.filters,
    }


@router.delete("/{search_id}")
async def delete_saved_search(
    search_id: UUID,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a saved search."""
    result = await db.execute(
        select(SavedSearch).where(SavedSearch.id == search_id, SavedSearch.user_id == user.id)
    )
    search = result.scalar_one_or_none()
    if not search:
        raise HTTPException(status_code=404, detail="Saved search not found.")

    await db.delete(search)
    await db.commit()
    return {"deleted": True}


@router.post("/{search_id}/run")
async def run_saved_search(
    search_id: UUID,
    request: Request,
    page: int = 1,
    page_size: int = 25,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Execute a saved search and return results."""
    result = await db.execute(
        select(SavedSearch).where(SavedSearch.id == search_id, SavedSearch.user_id == user.id)
    )
    search = result.scalar_one_or_none()
    if not search:
        raise HTTPException(status_code=404, detail="Saved search not found.")

    await check_rate_limit(request, lookup_count=1)

    conditions = build_filter_conditions(search.filters)
    if not conditions:
        return {"results": [], "total": 0, "page": page, "page_size": page_size}

    where = and_(*conditions)
    query = (
        select(*PERMIT_COLUMNS)
        .where(where)
        .order_by(Permit.issue_date.desc().nullslast())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    rows = (await db.execute(query)).all()

    total = 0
    if rows:
        if len(rows) < page_size:
            total = (page - 1) * page_size + len(rows)
        else:
            count_q = select(func.count()).select_from(Permit).where(where)
            total = (await db.execute(count_q)).scalar()

    # Update last_run_at
    search.last_run_at = datetime.now(timezone.utc)
    log = UsageLog(
        user_id=user.id,
        api_key_id=request.state.api_key.id,
        endpoint="/v1/saved-searches/run",
        lookup_count=1,
        ip_address=request.client.host if request.client else None,
    )
    db.add(log)
    await db.commit()

    return {
        "results": [row_to_dict(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total else 0,
    }
