"""Pydantic schemas for the real-time rural-score lookup endpoint.

Deliverable E: GET /v1/rural-score/lookup?address=... returns a v5-style
rural_septic_score for ANY address — not limited to addresses already in a
materialized view.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class RuralScoreComponents(BaseModel):
    in_urban_area: bool | None = None
    population_density: float | None = None
    lot_acres: float | None = None
    fiber_available: bool | None = None
    cable_available: bool | None = None
    only_satellite: bool | None = None
    fiber_isp_count: int = 0
    isp_count: int = 0


class RuralScoreLookupResponse(BaseModel):
    address: str | None = None
    city: str | None = None
    state: str
    zip: str | None = None
    lat: float | None = None
    lon: float | None = None
    rural_septic_score: int = Field(..., ge=0, le=100)
    tier: str = Field(..., description="urban / suburban / rural / high-rural")
    components: RuralScoreComponents
    geocode_source: str = Field(
        ..., description="property_sales | census | none"
    )
    confidence: str = Field(..., description="high | medium | low")
    interpretation: str
