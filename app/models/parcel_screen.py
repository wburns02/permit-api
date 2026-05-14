"""Parcel-screen feature models.

Powers the California state-law qualification + unit-yield calculator at
parcels.ecbtx.com. Schema is state-agnostic so we can extend beyond CA later
(TX, CO, WA, etc.).

Origin: Rob's `.claude/skills/parcel-screen/` Claude Code skill, productized.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, Column, String, Integer, Numeric, Date, DateTime, Text, ForeignKey, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB

from app.database import Base


class ParcelJurisdiction(Base):
    """Cached GIS REST endpoints per city/county.

    Populated via Rob's discovery technique (Chrome MCP performance API trick),
    then keyed by (state, city_slug) for fast lookup on subsequent parcel screens.
    """
    __tablename__ = "parcel_jurisdictions"

    state = Column(String(2), primary_key=True)
    city_slug = Column(String(80), primary_key=True)
    display_name = Column(String(200), nullable=False)
    gis_viewer_url = Column(Text)
    parcels_url = Column(Text)
    zoning_url = Column(Text)
    general_plan_url = Column(Text)
    specific_plan_url = Column(Text)
    fire_hazard_url = Column(Text)
    focus_areas_url = Column(Text)
    apn_field = Column(String(80), default="APN")
    address_field = Column(String(80))
    spatial_reference_wkid = Column(Integer)
    notes = Column(Text)
    last_verified = Column(Date)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


class ParcelZoneDensity(Base):
    """Cached zoning code → density/dimensional standards per city.

    Populated by scraping the city's Municode/CodePublishing page on first
    encounter with a new zone. Refreshed by re-running the scrape when
    last_verified gets stale.
    """
    __tablename__ = "parcel_zone_density"

    state = Column(String(2), primary_key=True)
    city_slug = Column(String(80), primary_key=True)
    zone_code = Column(String(40), primary_key=True)
    zone_desc = Column(String(200))
    du_per_ac = Column(Numeric(10, 4))
    min_lot_sqft = Column(Integer)
    min_lot_width_ft = Column(Integer)
    max_height_ft = Column(Integer)
    front_setback_ft = Column(Integer)
    side_setback_ft = Column(Integer)
    rear_setback_ft = Column(Integer)
    max_lot_coverage_pct = Column(Numeric(5, 2))
    is_residential = Column(String(1), default="?")  # Y/N/? (? = check GP)
    gp_designations = Column(JSONB)  # GP code → du/ac mapping, denormalized
    source_url = Column(Text)
    notes = Column(Text)
    last_verified = Column(Date)


class ParcelStateLaw(Base):
    """Per-state housing-program reference cards.

    Each row is one law (e.g., CA SB-9, CA SB-684) with its eligibility checklist
    and yield formula. The eligibility engine in the router queries this table at
    runtime so we can add/update laws without redeploying.

    Origin: Rob's `state-law/*.md` files, JSON-converted.
    """
    __tablename__ = "parcel_state_laws"

    state = Column(String(2), primary_key=True)
    law_id = Column(String(80), primary_key=True)  # e.g., "sb9", "sb684"
    display_order = Column(Integer, default=999)
    name = Column(String(200), nullable=False)
    code_section = Column(String(200))
    effective_date = Column(Date)
    leginfo_url = Column(Text)
    summary = Column(Text)
    eligibility_checklist = Column(JSONB, nullable=False, default=list)
    # Schema for eligibility_checklist:
    #   [{"id": "zone_sfr", "label": "Zone is R-1 / SFR", "category": "gis", "check": "zone_code IN ['R-1','R1','SFR']"},
    #    {"id": "no_demo", "label": "No housing demo in past 10 yrs", "category": "verify"},
    #    ...]
    # `category` is "gis" (auto-checkable) or "verify" (user must confirm).
    # `check` is a small DSL string the engine evaluates against parcel facts.
    yield_formula = Column(JSONB, nullable=False, default=dict)
    # Schema: {"max_units": "min(10, floor(acres*30))", "min_lot_sqft": 600, ...}
    caveats_md = Column(Text)
    last_verified = Column(Date)


class ParcelScreen(Base):
    """Audit log of every parcel screen run.

    Stores the final memo so the user can re-open historical screens, plus
    enables future batch mode and analytics.
    """
    __tablename__ = "parcel_screens"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False)
    state = Column(String(2), nullable=False)
    city_slug = Column(String(80), nullable=False)
    address = Column(String(500))
    apn = Column(String(80))
    result = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_parcel_screens_user_created", "user_id", "created_at"),
        Index("ix_parcel_screens_apn", "apn"),
    )


class ParcelHotPick(Base):
    """Ladder 1 Hot Picks leaderboard — bulk-scored candidate parcels.

    Pre-computed max-possible CA state-law unit yield for every parcel in a
    registered city. Sorted by score (= max_units, tie-break acres). Refreshed
    periodically via scripts/refresh_hot_picks.py and POST /parcel-screen/hot-picks/refresh.

    Origin: Phase-2 productization of Rob's parcel-screen — `.claude/skills/parcel-screen/`.
    """
    __tablename__ = "parcel_hot_picks"

    state = Column(String(2), primary_key=True)
    city_slug = Column(String(80), primary_key=True)
    apn = Column(String(80), primary_key=True)
    address = Column(String(500))
    owner_name = Column(String(255))
    acres = Column(Numeric(10, 4))
    zone_code = Column(String(40))
    gp_code = Column(String(40))
    fire_zone = Column(String(40))
    impr_value = Column(Numeric(14, 2))
    lat = Column(Numeric(11, 7))
    lng = Column(Numeric(11, 7))
    geometry_wgs84 = Column(JSONB)   # GeoJSON Polygon: {"type":"Polygon","coordinates":[[[lng,lat],...]]}
    max_units = Column(Integer, nullable=False, default=0)
    best_path = Column(String(80))          # e.g. "sb684+ab130" or "state-adu"
    eligible_paths = Column(JSONB, default=list)  # ["by-right","state-adu","ab130", ...]
    score = Column(Numeric(10, 4), nullable=False, default=0)  # max_units × small bonuses
    refreshed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_parcel_hot_picks_city_score", "state", "city_slug", "score"),
    )


class ParcelOwnerEnrichment(Base):
    """Cached BatchData skip-trace results per parcel.

    Key: (state, city_slug, apn). One row per parcel — re-enriching overwrites
    the row (we never want stale phone numbers — BatchData refreshes its data
    monthly so a 90-day TTL is the right cache window).
    """
    __tablename__ = "parcel_owner_enrichment"

    state         = Column(String(2),   primary_key=True)
    city_slug     = Column(String(80),  primary_key=True)
    apn           = Column(String(80),  primary_key=True)
    owner_name    = Column(String(200))           # what we sent (parcel.owner_name)
    property_addr = Column(JSONB)                 # the {street, city, state, zip} we asked about
    persons       = Column(JSONB, nullable=False, default=list)
    # persons schema (subset, from BatchData):
    #   [{
    #     "name": {"first","last","full"},
    #     "phones": [{"number","type","score","dnc"}],
    #     "emails": ["a@b.com", ...],
    #     "mailing_address": {"street","city","state","zip"},
    #     "demographics": {"age","deceased"}
    #   }, ...]
    raw_response  = Column(JSONB)                 # full BatchData response for debugging
    hit           = Column(Boolean, default=False)  # did BatchData return any persons
    cost_cents    = Column(Integer, default=25)   # accounting rough estimate
    fetched_at    = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    fetched_by_user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=True)

    __table_args__ = (
        Index("ix_parcel_owner_enrich_user_fetched", "fetched_by_user_id", "fetched_at"),
    )
