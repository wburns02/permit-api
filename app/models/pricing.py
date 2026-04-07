"""Pricing benchmark model for electrician quote generation."""
import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, Numeric, DateTime
from sqlalchemy.dialects.postgresql import UUID

from app.database import Base


class PricingBenchmark(Base):
    __tablename__ = "pricing_benchmarks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_type = Column(String(50), nullable=False, index=True)
    metro = Column(String(100), nullable=False, index=True)
    low = Column(Numeric(10, 2), nullable=False)
    mid = Column(Numeric(10, 2), nullable=False)
    high = Column(Numeric(10, 2), nullable=False)
    source = Column(String(50), nullable=False)
    scraped_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
