"""Pydantic schemas for the Hail Leads / PermitLookup hail-leads API."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

class HailLeadsStats(BaseModel):
    """Headline KPIs for the hail-leads dashboard header."""
    total_leads: int
    unique_addresses: int
    counties_covered: int
    latest_storm_date: date | None
    fresh_leads_this_week: int
    hail_events_last_year: int


# ---------------------------------------------------------------------------
# List / search
# ---------------------------------------------------------------------------

LeadCategory = Literal["roof_replace", "siding", "gutter", "solar"]
SortKey = Literal["score_desc", "storm_date_desc", "issue_date_desc"]


class HailLeadListItem(BaseModel):
    """Single lead row on the list endpoint."""
    lead_id: str
    address: str | None
    city: str | None
    zip: str | None
    county: str | None
    storm_date: date | None
    storm_type: str | None
    hail_size_inches: float | None
    permit_date: date | None
    days_after_storm: int | None
    lead_category: str | None
    permit_description: str | None
    competitor_contractor: str | None
    score: float | None
    prior_roof_permits: int | None
    last_roof_permit_date: date | None
    owner_enriched: bool


class HailLeadListResponse(BaseModel):
    """Paginated list of hail leads."""
    results: list[HailLeadListItem]
    total: int
    page: int
    page_size: int
    total_pages: int


# ---------------------------------------------------------------------------
# Detail
# ---------------------------------------------------------------------------

class HailLeadStorm(BaseModel):
    storm_date: date | None
    storm_type: str | None
    hail_size_inches: float | None
    storm_event_id: str | None
    damage_report: str | None


class HailLeadPermit(BaseModel):
    permit_date: date | None
    days_after_storm: int | None
    permit_number: str | None
    permit_type: str | None
    work_class: str | None
    description: str | None
    valuation: float | None
    contractor: str | None
    lead_category: str | None


class HailLeadAddressHistory(BaseModel):
    total_permits: int
    prior_roof_permits: int
    earliest_permit_date: date | None
    latest_permit_date: date | None
    last_roof_permit_date: date | None
    total_roof_valuation: float | None


class HailLeadPhone(BaseModel):
    number: str
    type: str | None = None
    dnc: bool | None = None
    score: int | None = None


class HailLeadOwner(BaseModel):
    enriched: bool
    owner_name: str | None = None
    phones: list[HailLeadPhone] = Field(default_factory=list)
    emails: list[str] = Field(default_factory=list)
    mailing_address: str | None = None
    age: int | None = None
    deceased: bool | None = None


class HailLeadDetail(BaseModel):
    """Full hail lead detail with enrichment."""
    lead_id: str
    address: str | None
    city: str | None
    zip: str | None
    county: str | None
    lat: float | None
    lng: float | None
    storm: HailLeadStorm
    permit: HailLeadPermit
    address_history: HailLeadAddressHistory
    year_built: int | None
    living_area_sqft: int | None
    appraised_value: float | None
    owner: HailLeadOwner | None


# ---------------------------------------------------------------------------
# Enrich
# ---------------------------------------------------------------------------

class HailLeadsEnrichRequest(BaseModel):
    lead_ids: list[str] = Field(..., min_length=1, max_length=5000)
    force: bool = False


class HailLeadsEnrichResponse(BaseModel):
    enriched: int
    skipped: int
    failed: int
    errors: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Health (system observability)
# ---------------------------------------------------------------------------

CronStatus = Literal["ok", "stale", "missing"]


class MaterializedViewFreshness(BaseModel):
    """Freshness of a materialized view (row count + data age + last analyze)."""
    name: str
    row_count: int
    last_data_at: datetime | None
    hours_since_data: float | None
    last_analyzed_at: datetime | None
    hours_since_analyze: float | None


class StormSourceFreshness(BaseModel):
    """Freshness of an upstream storm-data source table."""
    source: str
    latest_report_date: date | None
    days_since: int | None
    rows_last_7d: int
    rows_last_30d: int


class FreshLeadsCounts(BaseModel):
    """Hail-leads counts by recency window."""
    this_week: int
    last_week: int
    last_30d: int


class CoverageStat(BaseModel):
    """Coverage of an enrichment cache vs total addresses."""
    name: str
    enriched_rows: int
    total_addresses: int
    percent_covered: float


class CronHeartbeat(BaseModel):
    """Last-seen heartbeat for a recurring backend job."""
    name: str
    last_seen_at: datetime | None
    hours_since: float | None
    status: CronStatus


class HailLeadsHealth(BaseModel):
    """Overall health snapshot for the hail-leads pipeline."""
    generated_at: datetime
    materialized_views: list[MaterializedViewFreshness]
    storm_sources: list[StormSourceFreshness]
    fresh_leads: FreshLeadsCounts
    coverage: list[CoverageStat]
    crons: list[CronHeartbeat]
