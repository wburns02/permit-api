"""Saved search model for on-demand re-runnable queries."""

import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, DateTime, ForeignKey, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.database import Base


class SavedSearch(Base):
    __tablename__ = "saved_searches"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False)
    name = Column(String(200), nullable=False)
    filters = Column(JSONB, nullable=False, default=dict)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_run_at = Column(DateTime(timezone=True))

    __table_args__ = (
        Index("ix_saved_searches_user", "user_id"),
    )
