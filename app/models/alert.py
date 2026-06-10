"""Permit alert models for monitoring and notifications."""

import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Boolean, Integer, DateTime, ForeignKey, Text,
    Enum as PGEnum, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.database import Base
import enum


class AlertFrequency(str, enum.Enum):
    INSTANT = "instant"
    DAILY = "daily"
    WEEKLY = "weekly"


class PermitAlert(Base):
    __tablename__ = "permit_alerts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False)
    name = Column(String(200), nullable=False)
    # What the alert watches: 'permits' (building permits, default) or
    # 'well_permits' (RRC W-1 drilling permits). Criteria live in the same
    # JSONB filters column so new watchable fields never need migrations.
    source_type = Column(String(30), nullable=False, default="permits", server_default="permits")
    # Filter criteria stored as JSON
    filters = Column(JSONB, nullable=False, default=dict)
    # permits: {"state": "TX", "city": "Austin", "permit_type": "building", "contractor": "Smith", "address": "Main St"}
    # well_permits: {"state": "TX", "county": "MIDLAND", "operator": "PIONEER", "lease": "BUCHANAN", "district": "08", "wellbore_profile": "horizontal", "min_depth": 8000}
    frequency = Column(PGEnum(AlertFrequency, name="alert_frequency"), default=AlertFrequency.DAILY, nullable=False)
    webhook_url = Column(Text)  # Optional webhook endpoint
    email_notify = Column(Boolean, default=True)
    is_active = Column(Boolean, default=True)
    last_checked_at = Column(DateTime(timezone=True))
    last_match_count = Column(Integer, default=0)
    total_matches = Column(Integer, default=0)
    last_error = Column(Text)
    consecutive_failures = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_alerts_user", "user_id"),
        Index("ix_alerts_active", "is_active", "frequency"),
    )
