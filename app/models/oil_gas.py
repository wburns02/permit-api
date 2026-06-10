"""Oil & gas models — canonical schema on the permits warehouse.

Tables are managed by alembic_warehouse (NOT the app's alembic); these
mappings are read-only views of canonical.wells / well_permits / operators /
disposal_wells. The geometry column is intentionally unmapped (no geoalchemy2
dependency); geo filters use raw SQL against geom, payloads carry lat/lng.
"""
import uuid
from sqlalchemy import Column, String, Text, Float, Date, DateTime, Boolean, Numeric
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.database import Base


class Operator(Base):
    __tablename__ = "operators"
    __table_args__ = {"schema": "canonical"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id = Column(UUID(as_uuid=True))
    state = Column(String(2), nullable=False)
    operator_number = Column(Text)
    name = Column(Text, nullable=False)
    p5_status = Column(Text)
    p5_renewal_date = Column(Date)
    organization_kind = Column(Text)
    lineage = Column(JSONB)
    freshness_at = Column(DateTime(timezone=True))


class Well(Base):
    __tablename__ = "wells"
    __table_args__ = {"schema": "canonical"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    state = Column(String(2), nullable=False)
    api14 = Column(Text)
    api10 = Column(Text)
    well_name = Column(Text)
    well_number = Column(Text)
    operator_id = Column(UUID(as_uuid=True))
    operator_name_raw = Column(Text)
    lease_name = Column(Text)
    lease_number = Column(Text)
    district = Column(Text)
    county = Column(Text)
    field_name = Column(Text)
    field_number = Column(Text)
    well_type = Column(Text)
    status = Column(Text)
    wellbore_profile = Column(Text)
    spud_date = Column(Date)
    completion_date = Column(Date)
    plug_date = Column(Date)
    total_depth = Column(Numeric)
    lat = Column(Float)
    lng = Column(Float)
    source = Column(Text, nullable=False)
    lineage = Column(JSONB)
    freshness_at = Column(DateTime(timezone=True))


class WellPermit(Base):
    __tablename__ = "well_permits"
    __table_args__ = {"schema": "canonical"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    state = Column(String(2), nullable=False)
    permit_number = Column(Text, nullable=False)
    api10 = Column(Text)
    operator_id = Column(UUID(as_uuid=True))
    operator_number = Column(Text)
    operator_name_raw = Column(Text)
    lease_name = Column(Text)
    well_number = Column(Text)
    district = Column(Text)
    county = Column(Text)
    field_name = Column(Text)
    wellbore_profile = Column(Text)
    filing_purpose = Column(Text)
    amended = Column(Boolean)
    total_depth = Column(Numeric)
    current_status = Column(Text)
    status_date = Column(Date)
    submitted_date = Column(Date)
    approved_date = Column(Date)
    spud_date = Column(Date)
    lat = Column(Float)
    lng = Column(Float)
    source = Column(Text, nullable=False)
    source_file = Column(Text)
    lineage = Column(JSONB)
    freshness_at = Column(DateTime(timezone=True))


class DisposalWell(Base):
    __tablename__ = "disposal_wells"
    __table_args__ = {"schema": "canonical"}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    state = Column(String(2), nullable=False)
    uic_number = Column(Text)
    permit_number = Column(Text)
    api10 = Column(Text)
    operator_id = Column(UUID(as_uuid=True))
    operator_name_raw = Column(Text)
    district = Column(Text)
    county = Column(Text)
    well_kind = Column(Text)
    status = Column(Text)
    formation = Column(Text)
    depth_interval = Column(Text)
    max_injection_pressure = Column(Numeric)
    max_injection_bpd = Column(Numeric)
    lat = Column(Float)
    lng = Column(Float)
    source = Column(Text, nullable=False)
    lineage = Column(JSONB)
    freshness_at = Column(DateTime(timezone=True))
