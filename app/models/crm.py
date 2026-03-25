"""CRM models — contacts, deals, notes, commissions, activities, and webhooks."""

import uuid
import secrets
from datetime import datetime, timezone

from sqlalchemy import (
    Column, String, Text, Float, Integer, Boolean, Date, DateTime, ForeignKey, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.database import Base


class Contact(Base):
    __tablename__ = "contacts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False, index=True)
    name = Column(Text, nullable=False)
    company = Column(Text)
    phone = Column(String(20))
    email = Column(String(255))
    address = Column(Text)
    city = Column(String(100))
    state = Column(String(2))
    zip = Column(String(10))
    lead_source = Column(String(50), default="permit")
    lead_id = Column(UUID(as_uuid=True), nullable=True)  # links to hot_leads
    tags = Column(JSONB)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_contacts_user_phone", "user_id", "phone"),
        Index("ix_contacts_user_email", "user_id", "email"),
    )


class Deal(Base):
    __tablename__ = "deals"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False, index=True)
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id"), index=True)
    title = Column(Text)
    stage = Column(String(50), default="new")  # new/contacted/quoted/negotiating/won/lost
    value = Column(Float)
    expected_close_date = Column(Date)
    actual_close_date = Column(Date)
    lost_reason = Column(Text)
    notes = Column(Text)
    permit_number = Column(String(100))
    permit_type = Column(String(50))
    review_requested_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_deals_user_stage", "user_id", "stage"),
        Index("ix_deals_contact", "contact_id"),
    )


class Note(Base):
    __tablename__ = "crm_notes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"))
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id"), nullable=True)
    deal_id = Column(UUID(as_uuid=True), ForeignKey("deals.id"), nullable=True)
    content = Column(Text, nullable=False)
    note_type = Column(String(20), default="note")  # call/email/meeting/task/note/system
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class Commission(Base):
    __tablename__ = "commissions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"))
    deal_id = Column(UUID(as_uuid=True), ForeignKey("deals.id"))
    amount = Column(Float)
    rate = Column(Float, default=0.10)
    status = Column(String(20), default="pending")  # pending/paid
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class Activity(Base):
    """Team activity feed — auto-logged for calls, deals, contacts, quotes, lead assignments."""
    __tablename__ = "activities"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    team_id = Column(UUID(as_uuid=True), ForeignKey("teams.id", ondelete="SET NULL"), nullable=True, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False, index=True)
    activity_type = Column(String(50), nullable=False)
    # Types: call_logged, deal_created, deal_stage_changed, contact_created,
    #        note_added, lead_assigned, quote_sent
    description = Column(Text)
    entity_type = Column(String(20))  # contact, deal, lead, quote
    entity_id = Column(UUID(as_uuid=True))
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_activities_team_created", "team_id", "created_at"),
        Index("ix_activities_user_created", "user_id", "created_at"),
    )


class BatchJob(Base):
    """Async batch address lookup job."""
    __tablename__ = "batch_jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False, index=True)
    status = Column(String(20), default="pending")  # pending/processing/complete/failed
    total_addresses = Column(Integer, default=0)
    processed = Column(Integer, default=0)
    results = Column(JSONB, nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_batch_jobs_user_created", "user_id", "created_at"),
    )


class Webhook(Base):
    """User-configured webhook for event notifications (new permits, violations, price changes)."""
    __tablename__ = "webhooks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False, index=True)
    name = Column(String(200))
    url = Column(Text, nullable=False)
    event_types = Column(JSONB, default=list)  # ["new_permit", "new_violation", "price_change"]
    filters = Column(JSONB, default=dict)  # {state: "TX", zip: "78666", permit_type: "roofing"}
    is_active = Column(Boolean, default=True)
    secret = Column(String(100), default=lambda: secrets.token_hex(32))
    last_triggered = Column(DateTime(timezone=True), nullable=True)
    failure_count = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_webhooks_user", "user_id"),
        Index("ix_webhooks_active", "user_id", "is_active"),
    )
