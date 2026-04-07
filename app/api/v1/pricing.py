"""Pricing benchmarks — market rate data for electrician quote generation."""

import logging
import uuid

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db, get_read_db
from app.models.pricing import PricingBenchmark

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pricing", tags=["Pricing"])

# ---------------------------------------------------------------------------
# Hardcoded defaults (used when no benchmarks found for a job_type)
# ---------------------------------------------------------------------------

_DEFAULTS: dict[str, tuple[float, float, float]] = {
    "panel_upgrade":  (1500,  2800,  4500),
    "rewire":         (3000,  6000, 12000),
    "outlet_install": ( 150,   250,   400),
    "ceiling_fan":    ( 150,   300,   500),
    "ev_charger":     ( 800,  1500,  2500),
    "rough_in":       (3000,  5000,  8000),
    "service_call":   ( 100,   200,   350),
}


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class BenchmarkOut(BaseModel):
    id: str
    job_type: str
    metro: str
    low: float
    mid: float
    high: float
    source: str
    scraped_at: str | None


class QuoteRequest(BaseModel):
    lead_id: str
    job_type: str
    description: str
    valuation: float | None = None


class QuoteResponse(BaseModel):
    recommended: float
    low: float
    high: float
    reasoning: str
    based_on: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/benchmarks", response_model=list[BenchmarkOut])
async def get_benchmarks(
    job_type: str | None = Query(None, max_length=50),
    metro: str | None = Query(None, max_length=100),
    db: AsyncSession = Depends(get_read_db),
):
    """
    Return pricing benchmarks, optionally filtered by job_type and/or metro.

    Source data scraped from HomeAdvisor/Angi/Thumbtack by job type and metro area.
    """
    q = select(PricingBenchmark)
    if job_type:
        q = q.where(PricingBenchmark.job_type == job_type)
    if metro:
        q = q.where(PricingBenchmark.metro == metro)
    q = q.order_by(PricingBenchmark.job_type, PricingBenchmark.metro)

    result = await db.execute(q)
    rows = result.scalars().all()

    return [
        BenchmarkOut(
            id=str(r.id),
            job_type=r.job_type,
            metro=r.metro,
            low=float(r.low),
            mid=float(r.mid),
            high=float(r.high),
            source=r.source,
            scraped_at=r.scraped_at.isoformat() if r.scraped_at else None,
        )
        for r in rows
    ]


@router.post("/generate-quote", response_model=QuoteResponse)
async def generate_quote(
    body: QuoteRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    AI-powered quote suggestion for a given job type.

    Looks up benchmarks for the job_type.  If a permit valuation is provided,
    estimates the electrical portion as 10% of total and blends it with the
    benchmark mid price.  Falls back to hardcoded defaults when no benchmarks
    exist for the requested job type.
    """
    # Fetch benchmarks for this job_type
    result = await db.execute(
        select(PricingBenchmark).where(PricingBenchmark.job_type == body.job_type)
    )
    rows = result.scalars().all()

    if rows:
        avg_low  = sum(float(r.low)  for r in rows) / len(rows)
        avg_mid  = sum(float(r.mid)  for r in rows) / len(rows)
        avg_high = sum(float(r.high) for r in rows) / len(rows)
        based_on = f"{len(rows)} benchmark record(s) for job type '{body.job_type}'"
    else:
        defaults = _DEFAULTS.get(body.job_type)
        if defaults:
            avg_low, avg_mid, avg_high = defaults
            based_on = f"hardcoded defaults for job type '{body.job_type}'"
        else:
            # Unknown job type — use a generic service-call fallback
            avg_low, avg_mid, avg_high = (200.0, 500.0, 1500.0)
            based_on = "generic fallback (no benchmarks or defaults found)"

    # Blend with permit valuation if provided (electrical = ~10% of total job cost)
    if body.valuation and body.valuation > 0:
        electrical_estimate = body.valuation * 0.10
        recommended = round((avg_mid + electrical_estimate) / 2, 2)
        reasoning = (
            f"Blended benchmark mid (${avg_mid:,.0f}) with electrical estimate from "
            f"permit valuation ${body.valuation:,.0f} × 10% = ${electrical_estimate:,.0f}. "
            f"Recommended: ${recommended:,.2f}."
        )
    else:
        recommended = round(avg_mid, 2)
        reasoning = (
            f"No permit valuation provided. Recommended price is the benchmark "
            f"mid of ${avg_mid:,.0f} for job type '{body.job_type}'."
        )

    return QuoteResponse(
        recommended=recommended,
        low=round(avg_low, 2),
        high=round(avg_high, 2),
        reasoning=reasoning,
        based_on=based_on,
    )
