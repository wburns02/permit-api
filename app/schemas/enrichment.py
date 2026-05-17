"""Pydantic schemas for cross-product enrichment endpoints.

Deliverable C: bolt broadband signal onto existing endpoints + a bulk enrichment
endpoint for CRM imports.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class BroadbandEnrichmentSummary(BaseModel):
    """Compact broadband signal — for nesting inside other product responses.

    NOT the full `BroadbandLookupResponse` — that's heavier (per-provider rows).
    """

    isp_count: int = 0
    fiber_isp_count: int = 0
    cable_isp_count: int = 0
    satellite_isp_count: int = 0
    has_fiber: bool = False
    has_cable: bool = False
    only_satellite: bool = False
    max_download_mbps: int | None = None
    max_upload_mbps: int | None = None
    match_method: str | None = None
    rural_signal: bool = Field(
        False,
        description="True if only-satellite OR (no fiber AND no cable) — useful for rural-product filtering",
    )


# ---------------------------------------------------------------------------
# Bulk endpoint
# ---------------------------------------------------------------------------


class BulkAddressInput(BaseModel):
    """One row in a bulk-enrichment request.

    Either provide (address+state) or (lat+lon). If both are present, lat/lon wins.
    """

    id: str | None = Field(
        None,
        description="Caller-supplied row id — echoed back so clients can re-join",
    )
    address: str | None = None
    city: str | None = None
    state: str | None = Field(None, min_length=2, max_length=2)
    zip: str | None = None
    lat: float | None = None
    lon: float | None = None


class BulkBroadbandRequest(BaseModel):
    items: list[BulkAddressInput] = Field(..., max_length=500)


class BulkBroadbandResultItem(BaseModel):
    id: str | None = None
    input: BulkAddressInput
    broadband: BroadbandEnrichmentSummary | None = None
    error: str | None = None


class BulkBroadbandResponse(BaseModel):
    count: int
    succeeded: int
    failed: int
    results: list[BulkBroadbandResultItem]
