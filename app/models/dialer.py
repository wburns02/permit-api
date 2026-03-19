"""Sales Dialer models — call logs and lead status tracking."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Column, String, Text, Integer, DateTime, ForeignKey, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.database import Base


class CallLog(Base):
    __tablename__ = "call_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False, index=True)
    lead_id = Column(UUID(as_uuid=True), index=True)  # References hot_leads.id
    phone_number = Column(String(20))
    duration_seconds = Column(Integer)
    disposition = Column(String(50))  # connected, voicemail, no_answer, wrong_number, callback, sold
    notes = Column(Text)
    ai_summary = Column(Text)
    action_items = Column(JSONB)  # [{task, due_date, status}]
    callback_date = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class LeadStatus(Base):
    __tablename__ = "lead_statuses"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False)
    lead_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    status = Column(String(50), default="new")  # new, contacted, callback, qualified, won, lost, skipped
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_lead_status_user_lead", "user_id", "lead_id", unique=True),
    )
