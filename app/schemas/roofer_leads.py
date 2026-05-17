"""Pydantic schemas for roofer-leads (storm-strike dispatch) endpoints.

Deliverable B: After a NOAA hail event, return ranked rooftop addresses within N
days of the storm, ordered by composite score (storm severity × home age ×
mortgage-presence × roof-permit recency).

Data sources on T430:
  - noaa_storm_events_details   (771K rows: event_id, magnitude, lat/lon, dates)
  - property_sales              (97M rows: address, lat/lng, year_built signal)
  - hmda_lar_2020_2024          (35M rows: mortgages keyed by property)
  - permits_<state>             (roof permits — penalty if recent)
  - code_violations             (297M rows: distressed flag)
"""

from __future__ import annotations

from datetime import date, datetime
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Storm event summary
# ---------------------------------------------------------------------------


class StormEventSummary(BaseModel):
    """Compact representation of a storm event."""

    event_id: int
    state: str | None = None
    event_type: str | None = None
    cz_name: str | None = None
    begin_datetime: datetime | None = None
    magnitude: float | None = Field(
        None, description="Hail size in inches (or wind mph for non-hail)"
    )
    magnitude_type: str | None = None
    begin_lat: float | None = None
    begin_lon: float | None = None
    damage_property: str | None = None


# ---------------------------------------------------------------------------
# Score components
# ---------------------------------------------------------------------------


class RooferLeadComponents(BaseModel):
    """Sub-scores that combine into the composite ranking."""

    storm_severity: float = Field(
        0.0, description="0-30; normalized from hail size in inches"
    )
    home_age_score: float = Field(
        0.0, description="0-25; older homes score higher (more likely to need replacement)"
    )
    mortgage_score: float = Field(
        0.0, description="0-20; bonus if active mortgage present (insurance + escrow)"
    )
    roof_permit_recency_penalty: float = Field(
        0.0, description="0..-20; recent roof permit reduces score"
    )
    distance_miles: float | None = Field(
        None, description="Miles from storm centroid to property"
    )


class RooferLeadItem(BaseModel):
    """One ranked property within a storm's footprint."""

    address: str | None = None
    city: str | None = None
    state: str | None = None
    zip: str | None = None
    county: str | None = None
    lat: float | None = None
    lon: float | None = None
    year_built: int | None = None
    has_active_mortgage: bool = False
    recent_roof_permit_date: date | None = None
    storm_event_id: int
    storm_date: datetime | None = None
    days_after_storm: int | None = None
    storm_magnitude: float | None = None
    composite_score: float = Field(..., description="0-100 ranking score")
    components: RooferLeadComponents


# ---------------------------------------------------------------------------
# Endpoint responses
# ---------------------------------------------------------------------------


class RooferLeadsByEventResponse(BaseModel):
    event: StormEventSummary
    days_after: int
    min_magnitude: float
    radius_miles: float
    count: int
    leads: list[RooferLeadItem]


class RooferLeadsRecentResponse(BaseModel):
    state: str
    days_back: int
    min_score: int
    event_count: int
    count: int
    leads: list[RooferLeadItem]
