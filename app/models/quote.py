"""Quote/Estimate models — quote builder for deals."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Column, String, Text, Float, Date, DateTime, ForeignKey, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.database import Base


class Quote(Base):
    __tablename__ = "quotes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False, index=True)
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id"), nullable=True)
    deal_id = Column(UUID(as_uuid=True), ForeignKey("deals.id"), nullable=True)
    items = Column(JSONB)  # [{description, quantity, unit_price, total}]
    subtotal = Column(Float, default=0.0)
    tax_rate = Column(Float, default=0.0)
    tax_amount = Column(Float, default=0.0)
    total = Column(Float, default=0.0)
    status = Column(String(20), default="draft")  # draft/sent/accepted/declined
    valid_until = Column(Date)
    sent_at = Column(DateTime(timezone=True))
    accepted_at = Column(DateTime(timezone=True))
    notes = Column(Text)
    terms = Column(Text)
    company_name = Column(String(200))
    company_phone = Column(String(20))
    company_email = Column(String(200))
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_quotes_user", "user_id"),
        Index("ix_quotes_contact", "contact_id"),
        Index("ix_quotes_deal", "deal_id"),
        Index("ix_quotes_status", "user_id", "status"),
    )
