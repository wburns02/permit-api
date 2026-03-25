"""Email campaign models — outreach campaigns, recipients, and unsubscribes."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Column, String, Text, Integer, Float, DateTime, ForeignKey, Index,
)
from sqlalchemy.dialects.postgresql import UUID
from app.database import Base


class EmailCampaign(Base):
    __tablename__ = "email_campaigns"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(200), nullable=False)
    subject = Column(String(500), nullable=False)
    body_html = Column(Text)
    body_text = Column(Text)
    target_audience = Column(String(100))  # insurance / realtor / contractor / all
    target_state = Column(String(2), nullable=True)
    status = Column(String(20), default="draft")  # draft/active/paused/completed
    sent_count = Column(Integer, default=0)
    open_count = Column(Integer, default=0)
    click_count = Column(Integer, default=0)
    unsubscribe_count = Column(Integer, default=0)
    signup_count = Column(Integer, default=0)
    bounce_count = Column(Integer, default=0)
    send_rate = Column(Integer, default=200)  # emails per hour
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_email_campaigns_status", "status"),
        Index("ix_email_campaigns_audience", "target_audience"),
    )


class EmailRecipient(Base):
    __tablename__ = "email_recipients"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    campaign_id = Column(UUID(as_uuid=True), ForeignKey("email_campaigns.id", ondelete="CASCADE"), nullable=False, index=True)
    email = Column(String(255), nullable=False)
    name = Column(String(500))
    company = Column(String(500))
    state = Column(String(2))
    license_type = Column(String(100))
    status = Column(String(20), default="pending")  # pending/sent/opened/clicked/unsubscribed/bounced
    sent_at = Column(DateTime(timezone=True), nullable=True)
    opened_at = Column(DateTime(timezone=True), nullable=True)
    clicked_at = Column(DateTime(timezone=True), nullable=True)
    unsubscribed_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_email_recipients_campaign_status", "campaign_id", "status"),
        Index("ix_email_recipients_email", "email"),
    )


class EmailUnsubscribe(Base):
    __tablename__ = "email_unsubscribes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), nullable=False, unique=True)
    reason = Column(Text)
    unsubscribed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_email_unsubscribes_email", "email", unique=True),
    )
