"""CRM models — contacts, deals, notes, and commissions."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Column, String, Text, Float, Date, DateTime, ForeignKey, Index,
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
