"""Permit model — normalized from 56M+ scraped government records."""

import uuid
from sqlalchemy import (
    Column, String, Text, Float, Date, Integer, Index,
    func,
)
from sqlalchemy.dialects.postgresql import UUID, TSVECTOR
from app.database import Base


class Permit(Base):
    __tablename__ = "permits"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    permit_number = Column(String(100), nullable=False, index=True)
    original_id = Column(String(255))

    # Location
    address = Column(String(500), nullable=False)
    address_normalized = Column(String(500), index=True)
    city = Column(String(100), index=True)
    state = Column(String(2), nullable=False, index=True)
    zip = Column(String(10), index=True)
    lat = Column(Float)
    lng = Column(Float)
    parcel_id = Column(String(200), index=True)

    # Permit details
    permit_type = Column(String(100), index=True)  # building, electrical, plumbing, mechanical, demolition
    work_type = Column(String(255))
    trade = Column(String(50), index=True)
    status = Column(String(50), index=True)
    description = Column(Text)
    valuation = Column(Float)

    # Dates
    issue_date = Column(Date, index=True)
    created_date = Column(Date)
    expired_date = Column(Date)
    completed_date = Column(Date)

    # People
    owner_name = Column(String(255))
    contractor_name = Column(String(255), index=True)
    contractor_company = Column(String(255))
    applicant_name = Column(String(255))

    # Source tracking
    jurisdiction = Column(String(200), nullable=False, index=True)
    source = Column(String(50))  # energov, mgo, arcgis, socrata, opengov
    scraped_at = Column(Date)

    # Full-text search vector
    search_vector = Column(TSVECTOR)

    __table_args__ = (
        Index("ix_permits_search_vector", "search_vector", postgresql_using="gin"),
        Index("ix_permits_address_trgm", "address_normalized", postgresql_using="gin",
              postgresql_ops={"address_normalized": "gin_trgm_ops"}),
        Index("ix_permits_state_city", "state", "city"),
        Index("ix_permits_jurisdiction_type", "jurisdiction", "permit_type"),
        Index("ix_permits_geo", "lat", "lng"),
    )


class Jurisdiction(Base):
    __tablename__ = "jurisdictions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    state = Column(String(2), nullable=False)
    record_count = Column(Integer, default=0)
    source = Column(String(50))
    last_updated = Column(Date)

    __table_args__ = (
        Index("ix_jurisdictions_name_state", "name", "state", unique=True),
    )
