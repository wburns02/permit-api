"""Batch Address Lookup — async job system for enterprise customers.

POST /v1/batch/submit  — submit up to 500 addresses, returns job_id
GET  /v1/batch/{job_id} — poll job status + results
GET  /v1/batch/history   — list user's past batch jobs
"""

import uuid
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from app.database import get_db, get_read_db
from app.config import settings
from app.middleware.api_key_auth import get_current_user
from app.models.api_key import ApiUser, PlanTier, resolve_plan
from app.models.crm import BatchJob

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/batch", tags=["Batch Lookup"])

MAX_BATCH_SIZE = 500


# ---------------------------------------------------------------------------
# Plan gating — Pro Leads+ only
# ---------------------------------------------------------------------------

def _require_pro_leads(user: ApiUser):
    plan = resolve_plan(user.plan)
    allowed = {PlanTier.PRO_LEADS, PlanTier.REALTIME, PlanTier.ENTERPRISE}
    if plan not in allowed:
        raise HTTPException(
            status_code=403,
            detail="Batch Address Lookup requires Pro Leads plan or higher. Upgrade at /pricing",
        )


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class BatchSubmitRequest(BaseModel):
    addresses: list[str] = Field(..., min_length=1, max_length=MAX_BATCH_SIZE)


class BatchSubmitResponse(BaseModel):
    job_id: str
    status: str
    total_addresses: int
    message: str


class BatchStatusResponse(BaseModel):
    job_id: str
    status: str
    total_addresses: int
    processed: int
    results: list | None = None
    error: str | None = None
    created_at: str
    completed_at: str | None = None


class BatchHistoryItem(BaseModel):
    job_id: str
    status: str
    total_addresses: int
    processed: int
    created_at: str
    completed_at: str | None = None


# ---------------------------------------------------------------------------
# Background processor
# ---------------------------------------------------------------------------

async def process_batch(job_id: uuid.UUID, addresses: list[str], db_url: str):
    """Process batch addresses in the background against T430 data."""
    engine = create_async_engine(db_url, pool_size=2, max_overflow=1, pool_recycle=1800)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    results = []
    processed = 0

    try:
        async with async_session() as db:
            # Mark as processing
            await db.execute(
                update(BatchJob)
                .where(BatchJob.id == job_id)
                .values(status="processing")
            )
            await db.commit()

            for addr in addresses:
                result = {"address": addr, "permits_found": 0, "violations": 0,
                          "sales": 0, "septic": 0, "risk_signals": []}
                try:
                    # Sanitize address for ILIKE
                    safe_addr = addr.replace("'", "''").replace("%", "\\%").strip()
                    if not safe_addr:
                        result["risk_signals"].append("Empty address")
                        results.append(result)
                        processed += 1
                        continue

                    like_pattern = f"%{safe_addr}%"

                    # 1. Query hot_leads
                    try:
                        await db.execute(text("SET LOCAL statement_timeout = '5000'"))
                        hl_result = await db.execute(
                            text("SELECT COUNT(*) FROM hot_leads WHERE address ILIKE :addr LIMIT 10"),
                            {"addr": like_pattern},
                        )
                        hl_count = hl_result.scalar() or 0
                        if hl_count > 0:
                            result["risk_signals"].append(f"Found in hot_leads ({hl_count} matches)")
                    except Exception:
                        await db.rollback()

                    # 2. Count permits
                    try:
                        await db.execute(text("SET LOCAL statement_timeout = '5000'"))
                        p_result = await db.execute(
                            text("SELECT COUNT(*) FROM permits WHERE address ILIKE :addr"),
                            {"addr": like_pattern},
                        )
                        result["permits_found"] = p_result.scalar() or 0
                    except Exception:
                        await db.rollback()

                    # 3. Count violations
                    try:
                        await db.execute(text("SET LOCAL statement_timeout = '5000'"))
                        v_result = await db.execute(
                            text("SELECT COUNT(*) FROM code_violations WHERE address ILIKE :addr"),
                            {"addr": like_pattern},
                        )
                        result["violations"] = v_result.scalar() or 0
                        if result["violations"] > 0:
                            result["risk_signals"].append(f"{result['violations']} code violation(s)")
                    except Exception:
                        await db.rollback()

                    # 4. Count sales
                    try:
                        await db.execute(text("SET LOCAL statement_timeout = '5000'"))
                        s_result = await db.execute(
                            text("SELECT COUNT(*) FROM property_sales WHERE address ILIKE :addr"),
                            {"addr": like_pattern},
                        )
                        result["sales"] = s_result.scalar() or 0
                    except Exception:
                        await db.rollback()

                    # 5. Count septic
                    try:
                        await db.execute(text("SET LOCAL statement_timeout = '5000'"))
                        sep_result = await db.execute(
                            text("SELECT COUNT(*) FROM septic_systems WHERE address ILIKE :addr"),
                            {"addr": like_pattern},
                        )
                        result["septic"] = sep_result.scalar() or 0
                        if result["septic"] > 0:
                            result["risk_signals"].append("Septic system on record")
                    except Exception:
                        await db.rollback()

                    # Generate risk signals
                    if result["permits_found"] == 0:
                        result["risk_signals"].append("No permits on record")

                except Exception as e:
                    result["risk_signals"].append(f"Lookup error: {str(e)[:100]}")

                results.append(result)
                processed += 1

                # Update progress every 25 addresses
                if processed % 25 == 0:
                    try:
                        await db.execute(
                            update(BatchJob)
                            .where(BatchJob.id == job_id)
                            .values(processed=processed)
                        )
                        await db.commit()
                    except Exception:
                        await db.rollback()

            # Final update — mark complete
            await db.execute(
                update(BatchJob)
                .where(BatchJob.id == job_id)
                .values(
                    status="complete",
                    processed=processed,
                    results=results,
                    completed_at=datetime.now(timezone.utc),
                )
            )
            await db.commit()

    except Exception as e:
        logger.error("Batch job %s failed: %s", job_id, e)
        try:
            async with async_session() as db:
                await db.execute(
                    update(BatchJob)
                    .where(BatchJob.id == job_id)
                    .values(status="failed", error=str(e)[:500])
                )
                await db.commit()
        except Exception:
            pass
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/submit", response_model=BatchSubmitResponse)
async def batch_submit(
    body: BatchSubmitRequest,
    background_tasks: BackgroundTasks,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Submit a batch of addresses for async lookup. Max 500 per batch. Requires Pro Leads+."""
    _require_pro_leads(user)

    # Deduplicate and clean addresses
    addresses = list(dict.fromkeys(a.strip() for a in body.addresses if a.strip()))
    if len(addresses) > MAX_BATCH_SIZE:
        raise HTTPException(status_code=400, detail=f"Maximum {MAX_BATCH_SIZE} addresses per batch")
    if not addresses:
        raise HTTPException(status_code=400, detail="No valid addresses provided")

    # Create job record
    job = BatchJob(
        user_id=user.id,
        status="pending",
        total_addresses=len(addresses),
        processed=0,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    # Kick off background processing
    background_tasks.add_task(process_batch, job.id, addresses, str(settings.DATABASE_URL))

    return BatchSubmitResponse(
        job_id=str(job.id),
        status="pending",
        total_addresses=len(addresses),
        message=f"Batch job created. Processing {len(addresses)} addresses. Poll GET /v1/batch/{job.id} for results.",
    )


@router.get("/history", response_model=list[BatchHistoryItem])
async def batch_history(
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """List user's past batch jobs."""
    _require_pro_leads(user)

    result = await db.execute(
        select(BatchJob)
        .where(BatchJob.user_id == user.id)
        .order_by(BatchJob.created_at.desc())
        .limit(50)
    )
    jobs = result.scalars().all()

    return [
        BatchHistoryItem(
            job_id=str(j.id),
            status=j.status,
            total_addresses=j.total_addresses,
            processed=j.processed,
            created_at=j.created_at.isoformat() if j.created_at else "",
            completed_at=j.completed_at.isoformat() if j.completed_at else None,
        )
        for j in jobs
    ]


@router.get("/{job_id}", response_model=BatchStatusResponse)
async def batch_status(
    job_id: uuid.UUID,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """Get batch job status and results when complete."""
    _require_pro_leads(user)

    result = await db.execute(
        select(BatchJob).where(BatchJob.id == job_id, BatchJob.user_id == user.id)
    )
    job = result.scalar_one_or_none()

    if not job:
        raise HTTPException(status_code=404, detail="Batch job not found")

    return BatchStatusResponse(
        job_id=str(job.id),
        status=job.status,
        total_addresses=job.total_addresses,
        processed=job.processed,
        results=job.results if job.status == "complete" else None,
        error=job.error,
        created_at=job.created_at.isoformat() if job.created_at else "",
        completed_at=job.completed_at.isoformat() if job.completed_at else None,
    )
