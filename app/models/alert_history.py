"""Alert execution history model."""

import uuid
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Integer, DateTime, ForeignKey, Text, Index,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from app.database import Base


class AlertExecutionHistory(Base):
    __tablename__ = "alert_execution_history"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    alert_id = Column(UUID(as_uuid=True), ForeignKey("permit_alerts.id", ondelete="CASCADE"), nullable=False)
    run_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    match_count = Column(Integer, default=0)
    delivery_method = Column(String(20))  # email, webhook, both
    delivery_status = Column(String(20))  # success, failed, partial
    error = Column(Text)
    matches_sample = Column(JSONB)  # first 5 matches for debugging

    __table_args__ = (
        Index("ix_alert_history_alert_run", "alert_id", "run_at"),
    )
