"""Data layer models — contractor licenses, EPA, FEMA, census, septic, valuations, business entities, code violations, permit predictions, property sales."""

import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Text, Float, Date, DateTime, Integer, Boolean, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.database import Base


class ContractorLicense(Base):
    __tablename__ = "contractor_licenses"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    license_number = Column(String(100), nullable=False, index=True)
    business_name = Column(String(500), nullable=False)
    full_business_name = Column(String(500))
    address = Column(String(500))
    city = Column(String(100), index=True)
    state = Column(String(2), nullable=False, index=True)
    zip = Column(String(10), index=True)
    county = Column(String(100))
    phone = Column(String(20))
    business_type = Column(String(50))  # Sole Owner, Partnership, Corporation, etc.
    issue_date = Column(Date)
    expiration_date = Column(Date)
    status = Column(String(50), index=True)  # CLEAR, SUSPENDED, REVOKED, etc.
    secondary_status = Column(String(100))
    classifications = Column(Text)  # License classifications (e.g., B, C-10, HAZ)
    workers_comp_type = Column(String(100))  # Exempt, Workers' Compensation Insurance, etc.
    workers_comp_company = Column(String(255))
    surety_company = Column(String(255))
    surety_amount = Column(Float)
    source = Column(String(50), nullable=False)  # california_cslb, florida_dbpr, etc.
    last_updated = Column(Date)

    __table_args__ = (
        Index("ix_contractor_licenses_name", "business_name"),
        Index("ix_contractor_licenses_state_status", "state", "status"),
        Index("ix_contractor_licenses_license_state", "license_number", "state"),
    )


class EpaFacility(Base):
    __tablename__ = "epa_facilities"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    registry_id = Column(String(50), nullable=False, unique=True, index=True)
    name = Column(String(500), nullable=False)
    address = Column(String(500))
    city = Column(String(100), index=True)
    state = Column(String(2), nullable=False, index=True)
    zip = Column(String(10), index=True)
    county = Column(String(100))
    epa_region = Column(String(5))
    lat = Column(Float, index=True)
    lng = Column(Float, index=True)
    source = Column(String(50), default="epa_frs")  # epa_frs, epa_echo, epa_npdes, etc.

    __table_args__ = (
        Index("ix_epa_facilities_geo", "lat", "lng"),
        Index("ix_epa_facilities_state_city", "state", "city"),
    )


class FemaFloodZone(Base):
    __tablename__ = "fema_flood_zones"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dfirm_id = Column(String(20), nullable=False, index=True)
    fld_zone = Column(String(20), nullable=False, index=True)  # A, AE, AO, VE, X, etc.
    zone_subtype = Column(String(100))  # FLOODWAY, 0.2 PCT, AREA OF MINIMAL FLOOD HAZARD
    sfha_tf = Column(String(1))  # T = in SFHA (Special Flood Hazard Area), F = not
    static_bfe = Column(Float)  # Base flood elevation
    state_fips = Column(String(2), nullable=False, index=True)
    state_abbrev = Column(String(2), nullable=False, index=True)
    county_fips = Column(String(5))  # Extracted from dfirm_id

    __table_args__ = (
        Index("ix_fema_flood_state_zone", "state_abbrev", "fld_zone"),
        Index("ix_fema_flood_county", "county_fips"),
    )


class CensusDemographics(Base):
    __tablename__ = "census_demographics"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    state_fips = Column(String(2), nullable=False, index=True)
    county_fips = Column(String(3), nullable=False, index=True)
    tract = Column(String(6), nullable=False)
    block_group = Column(String(1))
    name = Column(String(500))  # Human-readable name
    population = Column(Integer)
    median_income = Column(Integer)
    median_home_value = Column(Integer)
    homeownership_rate = Column(Float)  # Percentage
    median_year_built = Column(Integer)
    total_housing_units = Column(Integer)
    occupied_units = Column(Integer)
    vacancy_rate = Column(Float)

    __table_args__ = (
        Index("ix_census_geo", "state_fips", "county_fips", "tract", "block_group",
              unique=True),
        Index("ix_census_state_county", "state_fips", "county_fips"),
    )


class SepticSystem(Base):
    __tablename__ = "septic_systems"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    address = Column(String(500))
    city = Column(String(100), index=True)
    state = Column(String(2), nullable=False, index=True)
    zip = Column(String(10), index=True)
    county = Column(String(100))
    parcel_id = Column(String(200), index=True)
    lat = Column(Float)
    lng = Column(Float)
    system_type = Column(String(100))  # OSTDS, Septic, LikelySeptic, LikelySewer, etc.
    wastewater_source = Column(String(200))  # Source name / utility
    install_date = Column(Date)
    last_inspection = Column(Date)
    land_use = Column(String(50))  # RES, COM, IND
    status = Column(String(50))  # Approved, Pending, Failed
    source = Column(String(50), nullable=False)  # fl_doh, co_cdphe, nc_dhhs, etc.

    __table_args__ = (
        Index("ix_septic_geo", "lat", "lng"),
        Index("ix_septic_state_city", "state", "city"),
        Index("ix_septic_address", "address"),
    )


class PropertyValuation(Base):
    __tablename__ = "property_valuations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    zip = Column(String(10), nullable=False, index=True)
    state = Column(String(2), index=True)
    state_code = Column(String(2))
    city = Column(String(100))
    region = Column(String(200))
    property_type = Column(String(50))  # Single Family Residential, All Residential, etc.
    period_begin = Column(Date, nullable=False)
    period_end = Column(Date, nullable=False)
    median_sale_price = Column(Float)
    median_list_price = Column(Float)
    median_ppsf = Column(Float)  # Price per square foot
    median_list_ppsf = Column(Float)
    homes_sold = Column(Integer)
    pending_sales = Column(Integer)
    new_listings = Column(Integer)
    inventory = Column(Integer)
    months_of_supply = Column(Float)
    median_dom = Column(Integer)  # Days on market
    avg_sale_to_list = Column(Float)
    sold_above_list = Column(Float)  # Percentage
    price_drops = Column(Float)
    parent_metro = Column(String(200))

    __table_args__ = (
        Index("ix_valuations_zip_period", "zip", "period_end"),
        Index("ix_valuations_state_zip", "state", "zip"),
    )


class BusinessEntity(Base):
    __tablename__ = "business_entities"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_name = Column(String(500), nullable=False, index=True)
    entity_type = Column(String(50), index=True)  # LLC, Corporation, LP, LLP, etc.
    state = Column(String(2), nullable=False, index=True)
    filing_number = Column(String(100), index=True)
    status = Column(String(50), index=True)  # Active, Inactive, Dissolved, etc.
    formation_date = Column(Date)
    dissolution_date = Column(Date)
    registered_agent_name = Column(String(500))
    registered_agent_address = Column(String(500))
    principal_address = Column(String(500))
    mailing_address = Column(String(500))
    officers = Column(JSONB)  # [{name, title, address}, ...]
    source = Column(String(50), nullable=False)  # fl_sunbiz, tx_sos, ca_bizfile, etc.
    scraped_at = Column(Date)

    __table_args__ = (
        Index("ix_entity_name_state", "entity_name", "state"),
        Index("ix_entity_filing", "filing_number", "state"),
        Index("ix_entity_state_type", "state", "entity_type"),
        Index("ix_entity_state_status", "state", "status"),
    )


class CodeViolation(Base):
    __tablename__ = "code_violations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    violation_id = Column(String(100), index=True)  # Source system ID
    address = Column(String(500), index=True)
    city = Column(String(100), index=True)
    state = Column(String(2), nullable=False, index=True)
    zip = Column(String(10), index=True)
    violation_type = Column(String(200))  # Class A/B/C, category
    violation_code = Column(String(100))
    description = Column(Text)
    status = Column(String(50), index=True)  # Open, Closed, Pending
    violation_date = Column(Date, index=True)
    inspection_date = Column(Date)
    resolution_date = Column(Date)
    fine_amount = Column(Float)
    lat = Column(Float)
    lng = Column(Float)
    source = Column(String(50), nullable=False)  # nyc_hpd, chicago_bldg, etc.

    __table_args__ = (
        Index("ix_violations_geo", "lat", "lng"),
        Index("ix_violations_state_city", "state", "city"),
        Index("ix_violations_address", "address"),
        Index("ix_violations_source_vid", "source", "violation_id"),
        Index("ix_violations_date_status", "violation_date", "status"),
    )


class PermitPrediction(Base):
    __tablename__ = "permit_predictions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    zip = Column(String(10), nullable=False, index=True)
    state = Column(String(2))
    prediction_score = Column(Float)  # 0-100, probability of 5+ permits in next 90 days
    predicted_permits = Column(Integer)  # Expected permit count next 90 days
    confidence = Column(Float)  # Model confidence 0-1
    features = Column(JSONB)  # Feature values used for this prediction
    risk_factors = Column(JSONB)  # Human-readable factors driving the prediction
    model_version = Column(String(50))
    scored_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_predictions_state_score", "state", prediction_score.desc()),
        Index("ix_predictions_scored_at", "scored_at"),
    )


class PropertySale(Base):
    __tablename__ = "property_sales"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id = Column(String(100), index=True)  # Source document ID
    address = Column(String(500), index=True)
    city = Column(String(100), index=True)
    state = Column(String(2), nullable=False, index=True)
    zip = Column(String(10), index=True)
    borough = Column(String(50))  # NYC specific
    sale_price = Column(Float)
    sale_date = Column(Date, index=True)
    recorded_date = Column(Date)
    doc_type = Column(String(50))  # DEED, TRANSFER, etc.
    grantor = Column(String(500))  # Seller
    grantee = Column(String(500))  # Buyer
    property_type = Column(String(100))  # Residential, Commercial, etc.
    building_class = Column(String(50))
    residential_units = Column(Integer)
    land_sqft = Column(Float)
    gross_sqft = Column(Float)
    lat = Column(Float)
    lng = Column(Float)
    source = Column(String(50), nullable=False)

    __table_args__ = (
        Index("ix_sales_address", "address"),
        Index("ix_sales_state_city", "state", "city"),
        Index("ix_sales_zip_date", "zip", "sale_date"),
        Index("ix_sales_sale_date", "sale_date"),
        Index("ix_sales_grantor", "grantor"),
        Index("ix_sales_grantee", "grantee"),
    )


class PropertyLien(Base):
    __tablename__ = "property_liens"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    document_id = Column(String(100), index=True)
    lien_type = Column(String(100), index=True)  # Tax Lien, UCC, Mechanic's Lien, Judgment, etc.
    filing_number = Column(String(100), index=True)
    address = Column(String(500), index=True)
    city = Column(String(100))
    state = Column(String(2), nullable=False, index=True)
    zip = Column(String(10), index=True)
    borough = Column(String(50))  # NYC specific
    amount = Column(Float)
    filing_date = Column(Date, index=True)
    lapse_date = Column(Date)
    status = Column(String(50))  # Active, Satisfied, Terminated, etc.
    debtor_name = Column(String(500), index=True)
    creditor_name = Column(String(500))
    description = Column(Text)
    source = Column(String(50), nullable=False)

    __table_args__ = (
        Index("ix_liens_address", "address"),
        Index("ix_liens_state_type", "state", "lien_type"),
        Index("ix_liens_filing_date", "filing_date"),
        Index("ix_liens_debtor", "debtor_name"),
        Index("ix_liens_filing_state", "filing_number", "state"),
    )
