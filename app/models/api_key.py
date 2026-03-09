"""API key and user models for authentication and billing."""

import uuid
import secrets
from datetime import datetime, timezone
from sqlalchemy import (
    Column, String, Boolean, Integer, DateTime, ForeignKey,
    Enum as PGEnum, Index,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from app.database import Base
import enum


class PlanTier(str, enum.Enum):
    FREE = "free"
    STARTER = "starter"
    PRO = "pro"
    ENTERPRISE = "enterprise"


class ApiUser(Base):
    __tablename__ = "api_users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False, index=True)
    company_name = Column(String(255))
    plan = Column(PGEnum(PlanTier, name="plan_tier"), default=PlanTier.FREE, nullable=False)
    stripe_customer_id = Column(String(255), unique=True)
    stripe_subscription_id = Column(String(255))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    api_keys = relationship("ApiKey", back_populates="user", cascade="all, delete-orphan")


class ApiKey(Base):
    __tablename__ = "api_keys"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False)
    key_hash = Column(String(128), unique=True, nullable=False, index=True)
    key_prefix = Column(String(12), nullable=False)  # First 8 chars for identification
    name = Column(String(100), default="Default")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    last_used_at = Column(DateTime(timezone=True))

    user = relationship("ApiUser", back_populates="api_keys")

    @staticmethod
    def generate_key() -> str:
        """Generate a new API key: pl_live_<random>."""
        return f"pl_live_{secrets.token_urlsafe(32)}"


class UsageLog(Base):
    __tablename__ = "usage_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("api_users.id"), nullable=False, index=True)
    api_key_id = Column(UUID(as_uuid=True), ForeignKey("api_keys.id"))
    endpoint = Column(String(100), nullable=False)
    lookup_count = Column(Integer, default=1)  # bulk searches count multiple
    ip_address = Column(String(45))
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_usage_user_date", "user_id", "created_at"),
    )
