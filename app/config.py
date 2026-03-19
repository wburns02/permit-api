"""PermitLookup API Configuration."""

from pydantic_settings import BaseSettings
from pydantic import field_validator, model_validator
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database
    DATABASE_URL: str = "postgresql+asyncpg://localhost:5432/permit_api"

    @field_validator("DATABASE_URL", mode="before")
    @classmethod
    def convert_database_url(cls, v: str) -> str:
        if v and v.startswith("postgresql://"):
            return v.replace("postgresql://", "postgresql+asyncpg://", 1)
        return v

    # Auth
    SECRET_KEY: str = "development-secret-key-change-in-production"
    ALGORITHM: str = "HS256"

    # CORS
    FRONTEND_URL: str = "http://localhost:5173"

    # Stripe
    STRIPE_SECRET_KEY: str | None = None
    STRIPE_PUBLISHABLE_KEY: str | None = None
    STRIPE_WEBHOOK_SECRET: str | None = None
    STRIPE_PRICE_STARTER: str | None = None  # legacy alias for Explorer
    STRIPE_PRICE_PRO: str | None = None  # legacy alias for Pro Leads
    STRIPE_PRICE_EXPLORER: str | None = None  # Stripe Price ID for $79/mo
    STRIPE_PRICE_PRO_LEADS: str | None = None  # Stripe Price ID for $249/mo
    STRIPE_PRICE_REALTIME: str | None = None  # Stripe Price ID for $599/mo
    STRIPE_PRICE_ENTERPRISE: str | None = None  # Stripe Price ID for $1,499/mo

    # Redis (rate limiting + caching)
    REDIS_URL: str | None = None

    # Rate limits (lookups per day) — tiered by data freshness plan
    RATE_LIMIT_FREE: int = 25
    RATE_LIMIT_STARTER: int = 100       # legacy, mapped to Explorer
    RATE_LIMIT_PRO: int = 250           # legacy, mapped to Pro Leads
    RATE_LIMIT_EXPLORER: int = 100
    RATE_LIMIT_PRO_LEADS: int = 250
    RATE_LIMIT_REALTIME: int = 1000
    RATE_LIMIT_ENTERPRISE: int = 10000
    OVERAGE_COST_CENTS: int = 5  # $0.05 per lookup over limit

    # Daily result caps (total records returned per day)
    RESULT_CAP_FREE: int = 500
    RESULT_CAP_EXPLORER: int = 2000
    RESULT_CAP_PRO_LEADS: int = 10000
    RESULT_CAP_REALTIME: int = 25000
    RESULT_CAP_ENTERPRISE: int = 50000

    # Data freshness limits (days) — how old permits must be for each plan
    DATA_FRESHNESS_FREE: int = 180          # 6 months old
    DATA_FRESHNESS_EXPLORER: int = 90       # 3 months old
    DATA_FRESHNESS_PRO_LEADS: int = 30      # 1 month old
    DATA_FRESHNESS_REALTIME: int = 0        # All data, no restriction
    DATA_FRESHNESS_ENTERPRISE: int = 0      # All data

    # Per-lead enrichment costs (cents)
    ENRICHMENT_PHONE_CENTS: int = 200       # $2
    ENRICHMENT_EMAIL_CENTS: int = 100       # $1
    ENRICHMENT_PROPERTY_CENTS: int = 300    # $3
    ENRICHMENT_FULL_CENTS: int = 500        # $5

    # Environment
    ENVIRONMENT: str = "development"
    DEBUG: bool = True
    VERSION: str = "0.1.0"

    # SendGrid (alert emails)
    SENDGRID_API_KEY: str | None = None
    SENDGRID_FROM_EMAIL: str = "alerts@permitlookup.com"
    ALERT_BATCH_SIZE: int = 50

    # Sentry
    SENTRY_DSN: str | None = None

    @model_validator(mode="after")
    def validate_production(self) -> "Settings":
        if self.ENVIRONMENT.lower() in ("production", "prod"):
            if len(self.SECRET_KEY) < 32 or self.SECRET_KEY.startswith("development"):
                raise ValueError("Set a strong SECRET_KEY for production")
            if self.DEBUG:
                object.__setattr__(self, "DEBUG", False)
        return self

    @property
    def is_production(self) -> bool:
        return self.ENVIRONMENT.lower() in ("production", "prod")

    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
