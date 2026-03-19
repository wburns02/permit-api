"""Data layer models — contractor licenses, EPA, FEMA, census, septic, valuations, business entities."""

import uuid
from sqlalchemy import (
    Column, String, Text, Float, Date, Integer, Boolean, Index,
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
