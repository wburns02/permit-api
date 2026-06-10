"""Pydantic schemas for broadband, septic-score, and rural-leads endpoints.

These power the v2 rural-septic intelligence + raw FCC BDC broadband lookup
endpoints introduced for the broader portfolio (permits.ecbtx.com).
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Broadband lookup
# ---------------------------------------------------------------------------

# FCC BDC technology codes — used to label provider tech in responses.
# https://help.bdc.fcc.gov/hc/en-us/articles/9999931803547
FCC_TECHNOLOGY_LABELS: dict[int, str] = {
    0: "other",
    10: "copper",
    40: "coax",
    50: "cable",
    60: "fiber",
    61: "fiber",
    70: "licensed-fixed-wireless",
    71: "licensed-unlicensed-fixed-wireless",
    72: "unlicensed-fixed-wireless",
    90: "satellite",
}

FIBER_TECH_CODES = {60, 61}
CABLE_TECH_CODES = {40, 50}
SATELLITE_TECH_CODES = {90}
WIRELESS_TECH_CODES = {70, 71, 72}
COPPER_TECH_CODES = {10}


class BroadbandProvider(BaseModel):
    """A single ISP serving an address (one row per provider+tech combination)."""

    provider_id: int
    brand_name: str | None = None
    holding_company_name: str | None = None
    technology: str
    technology_code: int
    max_download_mbps: int | None = None
    max_upload_mbps: int | None = None
    low_latency: bool | None = None
    business_residential: str | None = Field(
        None, description="'R' residential, 'B' business, 'X' both"
    )


class BroadbandLookupResponse(BaseModel):
    """Aggregated broadband availability at an address."""

    address: str | None = None
    city: str | None = None
    state: str
    zip: str | None = None
    block_geoid: str | None = None
    tract_geoid: str | None = None
    lat: float | None = None
    lon: float | None = None
    providers: list[BroadbandProvider]
    max_download_mbps: int | None = None
    max_upload_mbps: int | None = None
    has_fiber: bool
    has_cable: bool
    only_satellite: bool
    isp_count: int
    fiber_isp_count: int
    cable_isp_count: int
    satellite_isp_count: int
    wireless_isp_count: int
    source: str = "fcc_bdc"
    match_method: str = Field(
        ..., description="How the address was resolved: 'property_sales', 'zcta_centroid', 'tract_centroid', 'block_geoid'"
    )


# ---------------------------------------------------------------------------
# Rural-septic score
# ---------------------------------------------------------------------------

class SepticScoreComponents(BaseModel):
    """Signals that feed the rural_septic_score v2 model."""

    in_urban_area: bool | None = None
    population_density: float | None = Field(
        None, description="Population per square mile (ZCTA)"
    )
    median_household_income: float | None = None
    fiber_available: bool | None = None
    cable_available: bool | None = None
    only_satellite: bool | None = None
    isp_count: int | None = None
    lot_acres: float | None = None


class SepticScoreResponse(BaseModel):
    """v2 rural-septic score for a single address."""

    address: str | None = None
    city: str | None = None
    state: str
    zip: str | None = None
    score: int = Field(..., ge=0, le=100, description="0-100; higher = stronger rural-septic signal")
    tier: str = Field(..., description="urban / suburban / rural / high-rural")
    components: SepticScoreComponents
    interpretation: str
    confidence: str
    source: str = Field(
        ..., description="'materialized_view' (direct hit) or 'computed' (on-the-fly)"
    )
    permit_id: int | None = None
    county_name: str | None = None


# ---------------------------------------------------------------------------
# Rural-leads by county
# ---------------------------------------------------------------------------

class RuralLead(BaseModel):
    """One rural-septic lead row."""

    permit_id: int
    permit_number: str | None = None
    address: str | None = None
    city: str | None = None
    zip: str | None = None
    county_name: str | None = None
    rural_septic_score: int
    max_dl_mbps: int | None = None
    fiber_isp_count: int = 0
    isp_count: int = 0
    only_satellite: bool = False
    fiber_available: bool = False
    in_urban_area: bool | None = None
    population_density: float | None = None
    lot_acres: float | None = None
    system_type: str | None = None
    source: str | None = None


class RuralLeadsResponse(BaseModel):
    county: str
    state: str
    min_score: int
    count: int
    leads: list[RuralLead]
