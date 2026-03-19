"""Permit model — aligned with T430 PostgreSQL schema (744M+ records)."""

from sqlalchemy import Column, String, Text, Float, DateTime, BigInteger, Integer, Date, Index
from sqlalchemy.dialects.postgresql import JSONB, TSVECTOR
from app.database import Base


class Permit(Base):
    __tablename__ = "permits"

    id = Column("id", BigInteger, primary_key=True)
    permit_number = Column("permit_number", Text, index=True)

    # Location
    address = Column("address", Text)
    city = Column("city", Text, index=True)
    state = Column("state_code", String(2), nullable=False, index=True)
    zip = Column("zip_code", Text, index=True)
    county = Column("county", Text)
    lat = Column("lat", Float)
    lng = Column("lng", Float)
    parcel_id = Column("parcel_number", Text)

    # Permit details
    permit_type = Column("project_type", Text, index=True)
    work_type = Column("work_type", Text)
    trade = Column("trade", Text)
    category = Column("category", Text)
    project_name = Column("project_name", Text)
    status = Column("status", Text, index=True)
    description = Column("description", Text)

    # Dates
    issue_date = Column("date_created", DateTime)

    # People
    owner_name = Column("owner_name", Text)
    applicant_name = Column("applicant_name", Text)

    # Source tracking
    source = Column("source", Text)
    source_file = Column("source_file", Text)

    # T430-specific fields
    ossf_details = Column("ossf_details", Text)
    system_type = Column("system_type", Text)
    subdivision = Column("subdivision", Text)
    raw_data = Column("raw_data", JSONB)

    # Full-text search vector
    search_vector = Column("search_vector", TSVECTOR)

    __table_args__ = (
        Index("ix_permits_search_vector", "search_vector", postgresql_using="gin"),
        Index("ix_permits_state_city", "state_code", "city"),
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
