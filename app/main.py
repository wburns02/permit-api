"""PermitLookup API — Building permit data for contractors, investors, and insurers."""

import logging
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.database import init_db

# Import models to register with SQLAlchemy
from app.models.permit import Permit, Jurisdiction  # noqa: F401
from app.models.api_key import ApiUser, ApiKey, UsageLog  # noqa: F401
from app.models.alert import PermitAlert  # noqa: F401
from app.models.alert_history import AlertExecutionHistory  # noqa: F401
from app.models.saved_search import SavedSearch  # noqa: F401
from app.models.data_layers import (  # noqa: F401
    ContractorLicense, EpaFacility, FemaFloodZone,
    CensusDemographics, SepticSystem, PropertyValuation,
    BusinessEntity, CodeViolation, PermitPrediction,
    PropertySale, PropertyLien,
)
from app.models.dialer import CallLog, LeadStatus  # noqa: F401
from app.models.crm import Contact, Deal, Note, Commission, Activity, Webhook, BatchJob  # noqa: F401
from app.models.quote import Quote  # noqa: F401
from app.models.team import Team, TeamMember  # noqa: F401
from app.models.email_campaign import EmailCampaign, EmailRecipient, EmailUnsubscribe  # noqa: F401
from app.models.pricing import PricingBenchmark  # noqa: F401

# Import routers
from app.api.v1.permits import router as permits_router
from app.api.v1.auth import router as auth_router
from app.api.v1.billing import router as billing_router
from app.api.v1.coverage import router as coverage_router
from app.api.v1.contractors import router as contractors_router
from app.api.v1.alerts import router as alerts_router
from app.api.v1.properties import router as properties_router
from app.api.v1.market import router as market_router
from app.api.v1.saved_searches import router as saved_searches_router
from app.api.v1.admin import router as admin_router
from app.api.v1.licenses import router as licenses_router
from app.api.v1.environmental import router as environmental_router
from app.api.v1.septic import router as septic_router
from app.api.v1.demographics import router as demographics_router
from app.api.v1.valuations import router as valuations_router
from app.api.v1.entities import router as entities_router
from app.api.v1.pipeline import router as pipeline_router
from app.api.v1.violations import router as violations_router
from app.api.v1.predictions import router as predictions_router
from app.api.v1.sales import router as sales_router
from app.api.v1.liens import router as liens_router
from app.api.v1.dialer import router as dialer_router
from app.api.v1.crm import router as crm_router
from app.api.v1.quotes import router as quotes_router
from app.api.v1.analyst import router as analyst_router
from app.api.v1.trends import router as trends_router
from app.api.v1.batch import router as batch_router
from app.api.v1.campaigns import router as campaigns_router
from app.api.v1.dialer_ws import router as dialer_ws_router
from app.api.v1.freshness import router as freshness_router
from app.api.v1.hman_auth import router as hman_auth_router
from app.api.v1.pricing import router as pricing_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info("Starting PermitLookup API v%s", settings.VERSION)
    try:
        await init_db()
        logger.info("Database initialized")
    except Exception as e:
        logger.warning("Database not available at startup: %s", e)
        logger.warning("API will start but database endpoints will fail until DB is connected")

    # Auto-migrate: add webhook_url column if it doesn't exist
    try:
        from sqlalchemy import text as _text
        from app.database import primary_engine
        async with primary_engine.begin() as conn:
            await conn.execute(_text(
                "ALTER TABLE api_users ADD COLUMN IF NOT EXISTS webhook_url VARCHAR(500)"
            ))
    except Exception as e:
        logger.warning("Could not apply webhook_url migration: %s", e)

    # Auto-migrate: add password_hash column for H-Man CRM JWT auth
    try:
        from app.database import primary_engine
        async with primary_engine.begin() as conn:
            await conn.execute(_text(
                "ALTER TABLE api_users ADD COLUMN IF NOT EXISTS password_hash VARCHAR(64)"
            ))
    except Exception as e:
        logger.warning("Could not apply password_hash migration: %s", e)

    # Auto-migrate: add softphone columns to call_logs
    try:
        async with primary_engine.begin() as conn:
            for col, typ in [
                ("twilio_call_sid", "VARCHAR(64)"),
                ("recording_url", "TEXT"),
                ("recording_duration", "INTEGER"),
                ("transcript", "TEXT"),
            ]:
                await conn.execute(_text(
                    f"ALTER TABLE call_logs ADD COLUMN IF NOT EXISTS {col} {typ}"
                ))
    except Exception as e:
        logger.warning("Could not apply softphone migration: %s", e)

    # Start alert scheduler
    from app.services.scheduler import start_scheduler, stop_scheduler
    try:
        start_scheduler()
    except Exception as e:
        logger.warning("Failed to start alert scheduler: %s", e)

    # Start DB health watchdog — kills process if DB unreachable 3x in a row
    # Railway auto-restarts crashed containers, which resets the Tailscale tunnel
    import asyncio
    import os
    import signal

    async def _db_watchdog():
        from app.database import primary_session_maker
        from sqlalchemy import text
        consecutive_failures = 0
        # Grace period: Tailscale needs time to establish routes on fresh deploy
        await asyncio.sleep(120)
        while True:
            await asyncio.sleep(30)
            try:
                async with primary_session_maker() as db:
                    await asyncio.wait_for(
                        db.execute(text("SELECT 1")),
                        timeout=10.0,
                    )
                consecutive_failures = 0
            except Exception as e:
                consecutive_failures += 1
                logger.warning("DB watchdog: failure %d/%d — %s", consecutive_failures, 5, e)
                if consecutive_failures >= 5:
                    logger.error("DB watchdog: 5 consecutive failures, killing process for Railway restart")
                    os.kill(os.getpid(), signal.SIGTERM)

    watchdog_task = asyncio.create_task(_db_watchdog())

    yield

    watchdog_task.cancel()
    try:
        stop_scheduler()
    except Exception:
        pass
    logger.info("Shutting down PermitLookup API")


app = FastAPI(
    title="PermitLookup API",
    description="Search 1B+ property and permit records from 180+ jurisdictions across 50+ states. "
    "Includes building permits, contractor licenses, EPA environmental risk, FEMA flood zones, "
    "septic systems, census demographics, and property valuations. "
    "Address lookup, bulk search, geo search, and filtering by permit type, date, status, and more.",
    version=settings.VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "X-API-Key", "Authorization"],
    max_age=3600,
)

app.add_middleware(GZipMiddleware, minimum_size=1000)

# Mount v1 routers
app.include_router(permits_router, prefix="/v1")
app.include_router(auth_router, prefix="/v1")
app.include_router(billing_router, prefix="/v1")
app.include_router(coverage_router, prefix="/v1")
app.include_router(contractors_router, prefix="/v1")
app.include_router(alerts_router, prefix="/v1")
app.include_router(properties_router, prefix="/v1")
app.include_router(market_router, prefix="/v1")
app.include_router(saved_searches_router, prefix="/v1")
app.include_router(admin_router, prefix="/v1")
app.include_router(licenses_router, prefix="/v1")
app.include_router(environmental_router, prefix="/v1")
app.include_router(septic_router, prefix="/v1")
app.include_router(demographics_router, prefix="/v1")
app.include_router(valuations_router, prefix="/v1")
app.include_router(entities_router, prefix="/v1")
app.include_router(pipeline_router, prefix="/v1")
app.include_router(violations_router, prefix="/v1")
app.include_router(predictions_router, prefix="/v1")
app.include_router(sales_router, prefix="/v1")
app.include_router(liens_router, prefix="/v1")
app.include_router(dialer_router, prefix="/v1")
app.include_router(crm_router, prefix="/v1")
app.include_router(quotes_router, prefix="/v1")
app.include_router(analyst_router, prefix="/v1")
app.include_router(trends_router, prefix="/v1")
app.include_router(batch_router, prefix="/v1")
app.include_router(campaigns_router, prefix="/v1")
app.include_router(dialer_ws_router)  # WebSocket routes at root (no /v1 prefix)
app.include_router(freshness_router, prefix="/v1")
app.include_router(hman_auth_router, prefix="/v1")
app.include_router(pricing_router, prefix="/v1")


@app.get("/health")
async def health():
    """Health check — returns 503 only if PRIMARY DB is unreachable. Replica failure is non-fatal."""
    import asyncio
    from app.database import primary_session_maker, replica_session_maker, _replica_is_separate
    from sqlalchemy import text

    primary_ok = False
    try:
        async with primary_session_maker() as db:
            await asyncio.wait_for(db.execute(text("SELECT 1")), timeout=5.0)
        primary_ok = True
    except Exception:
        pass

    replica_ok = False
    if _replica_is_separate:
        try:
            async with replica_session_maker() as db:
                await asyncio.wait_for(db.execute(text("SELECT 1")), timeout=5.0)
            replica_ok = True
        except Exception:
            pass
    else:
        replica_ok = primary_ok

    # App is healthy if primary works (replica failure is degraded, not down)
    status_code = 200 if primary_ok else 503
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "healthy" if primary_ok else "unhealthy",
            "database": "connected" if primary_ok else "unreachable",
            "replica": "connected" if replica_ok else "down (using primary fallback)",
            "version": settings.VERSION,
            "environment": settings.ENVIRONMENT,
        },
    )


@app.post("/health/db/migrate-expansion")
async def migrate_expansion():
    """Add new columns and tables for industry expansion."""
    from app.database import primary_session_maker as async_session_maker
    from sqlalchemy import text
    migrations = []
    async with async_session_maker() as db:
        # Add columns to permit_alerts
        for col, typ, default in [
            ("last_error", "TEXT", None),
            ("consecutive_failures", "INTEGER", "0"),
        ]:
            try:
                defstr = f" DEFAULT {default}" if default else ""
                await db.execute(text(f"ALTER TABLE permit_alerts ADD COLUMN {col} {typ}{defstr}"))
                migrations.append(f"permit_alerts.{col} added")
            except Exception:
                migrations.append(f"permit_alerts.{col} already exists")
                await db.rollback()

        # Create alert_execution_history table
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS alert_execution_history (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    alert_id UUID REFERENCES permit_alerts(id) ON DELETE CASCADE,
                    run_at TIMESTAMPTZ DEFAULT NOW(),
                    match_count INTEGER DEFAULT 0,
                    delivery_method VARCHAR(20),
                    delivery_status VARCHAR(20),
                    error TEXT,
                    matches_sample JSONB
                )
            """))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_alert_history_alert_run ON alert_execution_history (alert_id, run_at)"))
            migrations.append("alert_execution_history table created")
        except Exception as e:
            migrations.append(f"alert_execution_history: {e}")
            await db.rollback()

        # Create saved_searches table
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS saved_searches (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID REFERENCES api_users(id),
                    name VARCHAR(200) NOT NULL,
                    filters JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    last_run_at TIMESTAMPTZ
                )
            """))
            await db.execute(text("CREATE INDEX IF NOT EXISTS ix_saved_searches_user ON saved_searches (user_id)"))
            migrations.append("saved_searches table created")
        except Exception as e:
            migrations.append(f"saved_searches: {e}")
            await db.rollback()

        # ---- UsageLog new columns (security services) ----
        for col, typ in [
            ("result_count", "INTEGER"),
            ("response_bytes", "INTEGER"),
            ("query_hash", "VARCHAR(64)"),
            ("abuse_score", "INTEGER"),
        ]:
            try:
                await db.execute(text(f"ALTER TABLE usage_logs ADD COLUMN {col} {typ}"))
                migrations.append(f"usage_logs.{col} added")
            except Exception:
                migrations.append(f"usage_logs.{col} already exists")
                await db.rollback()

        # ---- Data expansion tables (Phase 1) ----
        new_tables = {
            "contractor_licenses": """
                CREATE TABLE IF NOT EXISTS contractor_licenses (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    license_number VARCHAR(100) NOT NULL,
                    business_name VARCHAR(500) NOT NULL,
                    full_business_name VARCHAR(500),
                    address VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(2) NOT NULL,
                    zip VARCHAR(10),
                    county VARCHAR(100),
                    phone VARCHAR(20),
                    business_type VARCHAR(50),
                    issue_date DATE,
                    expiration_date DATE,
                    status VARCHAR(50),
                    secondary_status VARCHAR(100),
                    classifications TEXT,
                    workers_comp_type VARCHAR(100),
                    workers_comp_company VARCHAR(255),
                    surety_company VARCHAR(255),
                    surety_amount FLOAT,
                    source VARCHAR(50) NOT NULL,
                    last_updated DATE
                )
            """,
            "epa_facilities": """
                CREATE TABLE IF NOT EXISTS epa_facilities (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    registry_id VARCHAR(50) NOT NULL UNIQUE,
                    name VARCHAR(500) NOT NULL,
                    address VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(2) NOT NULL,
                    zip VARCHAR(10),
                    county VARCHAR(100),
                    epa_region VARCHAR(5),
                    lat FLOAT,
                    lng FLOAT,
                    source VARCHAR(50) DEFAULT 'epa_frs'
                )
            """,
            "fema_flood_zones": """
                CREATE TABLE IF NOT EXISTS fema_flood_zones (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    dfirm_id VARCHAR(20) NOT NULL,
                    fld_zone VARCHAR(20) NOT NULL,
                    zone_subtype VARCHAR(100),
                    sfha_tf VARCHAR(1),
                    static_bfe FLOAT,
                    state_fips VARCHAR(2) NOT NULL,
                    state_abbrev VARCHAR(2) NOT NULL,
                    county_fips VARCHAR(5)
                )
            """,
            "census_demographics": """
                CREATE TABLE IF NOT EXISTS census_demographics (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    state_fips VARCHAR(2) NOT NULL,
                    county_fips VARCHAR(3) NOT NULL,
                    tract VARCHAR(6) NOT NULL,
                    block_group VARCHAR(1),
                    name VARCHAR(500),
                    population INTEGER,
                    median_income INTEGER,
                    median_home_value INTEGER,
                    homeownership_rate FLOAT,
                    median_year_built INTEGER,
                    total_housing_units INTEGER,
                    occupied_units INTEGER,
                    vacancy_rate FLOAT
                )
            """,
            "septic_systems": """
                CREATE TABLE IF NOT EXISTS septic_systems (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    address VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(2) NOT NULL,
                    zip VARCHAR(10),
                    county VARCHAR(100),
                    parcel_id VARCHAR(200),
                    lat FLOAT,
                    lng FLOAT,
                    system_type VARCHAR(100),
                    wastewater_source VARCHAR(200),
                    install_date DATE,
                    last_inspection DATE,
                    land_use VARCHAR(50),
                    status VARCHAR(50),
                    source VARCHAR(50) NOT NULL
                )
            """,
            "property_valuations": """
                CREATE TABLE IF NOT EXISTS property_valuations (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    zip VARCHAR(10) NOT NULL,
                    state VARCHAR(2),
                    state_code VARCHAR(2),
                    city VARCHAR(100),
                    region VARCHAR(200),
                    property_type VARCHAR(50),
                    period_begin DATE NOT NULL,
                    period_end DATE NOT NULL,
                    median_sale_price FLOAT,
                    median_list_price FLOAT,
                    median_ppsf FLOAT,
                    median_list_ppsf FLOAT,
                    homes_sold INTEGER,
                    pending_sales INTEGER,
                    new_listings INTEGER,
                    inventory INTEGER,
                    months_of_supply FLOAT,
                    median_dom INTEGER,
                    avg_sale_to_list FLOAT,
                    sold_above_list FLOAT,
                    price_drops FLOAT,
                    parent_metro VARCHAR(200)
                )
            """,
            "business_entities": """
                CREATE TABLE IF NOT EXISTS business_entities (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    entity_name VARCHAR(500) NOT NULL,
                    entity_type VARCHAR(50),
                    state VARCHAR(2) NOT NULL,
                    filing_number VARCHAR(100),
                    status VARCHAR(50),
                    formation_date DATE,
                    dissolution_date DATE,
                    registered_agent_name VARCHAR(500),
                    registered_agent_address VARCHAR(500),
                    principal_address VARCHAR(500),
                    mailing_address VARCHAR(500),
                    officers JSONB,
                    source VARCHAR(50) NOT NULL,
                    scraped_at DATE
                )
            """,
            "code_violations": """
                CREATE TABLE IF NOT EXISTS code_violations (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    violation_id VARCHAR(100),
                    address VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(2) NOT NULL,
                    zip VARCHAR(10),
                    violation_type VARCHAR(200),
                    violation_code VARCHAR(100),
                    description TEXT,
                    status VARCHAR(50),
                    violation_date DATE,
                    inspection_date DATE,
                    resolution_date DATE,
                    fine_amount FLOAT,
                    lat FLOAT,
                    lng FLOAT,
                    source VARCHAR(50) NOT NULL
                )
            """,
            "permit_predictions": """
                CREATE TABLE IF NOT EXISTS permit_predictions (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    zip VARCHAR(10) NOT NULL,
                    state VARCHAR(2),
                    prediction_score FLOAT,
                    predicted_permits INTEGER,
                    confidence FLOAT,
                    features JSONB,
                    risk_factors JSONB,
                    model_version VARCHAR(50),
                    scored_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
            "property_sales": """
                CREATE TABLE IF NOT EXISTS property_sales (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    document_id VARCHAR(100),
                    address VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(2) NOT NULL,
                    zip VARCHAR(10),
                    borough VARCHAR(50),
                    sale_price FLOAT,
                    sale_date DATE,
                    recorded_date DATE,
                    doc_type VARCHAR(50),
                    grantor VARCHAR(500),
                    grantee VARCHAR(500),
                    property_type VARCHAR(100),
                    building_class VARCHAR(50),
                    residential_units INTEGER,
                    land_sqft FLOAT,
                    gross_sqft FLOAT,
                    lat FLOAT,
                    lng FLOAT,
                    source VARCHAR(50) NOT NULL
                )
            """,
            "property_liens": """
                CREATE TABLE IF NOT EXISTS property_liens (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    document_id VARCHAR(100),
                    lien_type VARCHAR(100),
                    filing_number VARCHAR(100),
                    address VARCHAR(500),
                    city VARCHAR(100),
                    state VARCHAR(2) NOT NULL,
                    zip VARCHAR(10),
                    borough VARCHAR(50),
                    amount FLOAT,
                    filing_date DATE,
                    lapse_date DATE,
                    status VARCHAR(50),
                    debtor_name VARCHAR(500),
                    creditor_name VARCHAR(500),
                    description TEXT,
                    source VARCHAR(50) NOT NULL
                )
            """,
        }

        for table_name, ddl in new_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # ---- Sales Dialer tables ----
        dialer_tables = {
            "call_logs": """
                CREATE TABLE IF NOT EXISTS call_logs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    lead_id UUID,
                    phone_number VARCHAR(20),
                    duration_seconds INTEGER,
                    disposition VARCHAR(50),
                    notes TEXT,
                    ai_summary TEXT,
                    action_items JSONB,
                    callback_date TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
            "lead_statuses": """
                CREATE TABLE IF NOT EXISTS lead_statuses (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    lead_id UUID NOT NULL,
                    status VARCHAR(50) DEFAULT 'new',
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
        }
        for table_name, ddl in dialer_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # ---- CRM tables ----
        crm_tables = {
            "contacts": """
                CREATE TABLE IF NOT EXISTS contacts (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    name TEXT NOT NULL,
                    company TEXT,
                    phone VARCHAR(20),
                    email VARCHAR(255),
                    address TEXT,
                    city VARCHAR(100),
                    state VARCHAR(2),
                    zip VARCHAR(10),
                    lead_source VARCHAR(50) DEFAULT 'permit',
                    lead_id UUID,
                    tags JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
            "deals": """
                CREATE TABLE IF NOT EXISTS deals (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    contact_id UUID REFERENCES contacts(id),
                    title TEXT,
                    stage VARCHAR(50) DEFAULT 'new',
                    value FLOAT,
                    expected_close_date DATE,
                    actual_close_date DATE,
                    lost_reason TEXT,
                    notes TEXT,
                    permit_number VARCHAR(100),
                    permit_type VARCHAR(50),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
            "crm_notes": """
                CREATE TABLE IF NOT EXISTS crm_notes (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID REFERENCES api_users(id),
                    contact_id UUID REFERENCES contacts(id),
                    deal_id UUID REFERENCES deals(id),
                    content TEXT NOT NULL,
                    note_type VARCHAR(20) DEFAULT 'note',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
            "commissions": """
                CREATE TABLE IF NOT EXISTS commissions (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID REFERENCES api_users(id),
                    deal_id UUID REFERENCES deals(id),
                    amount FLOAT,
                    rate FLOAT DEFAULT 0.10,
                    status VARCHAR(20) DEFAULT 'pending',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
        }
        for table_name, ddl in crm_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # ---- Add review_requested_at to deals ----
        try:
            await db.execute(text("ALTER TABLE deals ADD COLUMN review_requested_at TIMESTAMPTZ"))
            migrations.append("deals.review_requested_at added")
        except Exception:
            migrations.append("deals.review_requested_at already exists")
            await db.rollback()

        # ---- Quote/Estimate tables ----
        quote_tables = {
            "quotes": """
                CREATE TABLE IF NOT EXISTS quotes (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    contact_id UUID REFERENCES contacts(id),
                    deal_id UUID REFERENCES deals(id),
                    items JSONB,
                    subtotal FLOAT DEFAULT 0.0,
                    tax_rate FLOAT DEFAULT 0.0,
                    tax_amount FLOAT DEFAULT 0.0,
                    total FLOAT DEFAULT 0.0,
                    status VARCHAR(20) DEFAULT 'draft',
                    valid_until DATE,
                    sent_at TIMESTAMPTZ,
                    accepted_at TIMESTAMPTZ,
                    notes TEXT,
                    terms TEXT,
                    company_name VARCHAR(200),
                    company_phone VARCHAR(20),
                    company_email VARCHAR(200),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
        }
        for table_name, ddl in quote_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # ---- Team Management tables ----
        team_tables = {
            "teams": """
                CREATE TABLE IF NOT EXISTS teams (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name VARCHAR(200) NOT NULL,
                    owner_id UUID NOT NULL REFERENCES api_users(id),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
            "team_members": """
                CREATE TABLE IF NOT EXISTS team_members (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    team_id UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    role VARCHAR(20) DEFAULT 'member',
                    territories JSONB
                )
            """,
        }
        for table_name, ddl in team_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # ---- Activities table (collaboration feed) ----
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS activities (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    team_id UUID REFERENCES teams(id) ON DELETE SET NULL,
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    activity_type VARCHAR(50) NOT NULL,
                    description TEXT,
                    entity_type VARCHAR(20),
                    entity_id UUID,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            migrations.append("activities table created")
        except Exception as e:
            migrations.append(f"activities: {e}")
            await db.rollback()

        # ---- Webhooks table ----
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS webhooks (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    name VARCHAR(200),
                    url TEXT NOT NULL,
                    event_types JSONB DEFAULT '[]'::jsonb,
                    filters JSONB DEFAULT '{}'::jsonb,
                    is_active BOOLEAN DEFAULT TRUE,
                    secret VARCHAR(100),
                    last_triggered TIMESTAMPTZ,
                    failure_count INTEGER DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            migrations.append("webhooks table created")
        except Exception as e:
            migrations.append(f"webhooks: {e}")
            await db.rollback()

        # ---- Batch Jobs table ----
        try:
            await db.execute(text("""
                CREATE TABLE IF NOT EXISTS batch_jobs (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id UUID NOT NULL REFERENCES api_users(id),
                    status VARCHAR(20) DEFAULT 'pending',
                    total_addresses INTEGER DEFAULT 0,
                    processed INTEGER DEFAULT 0,
                    results JSONB,
                    error TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    completed_at TIMESTAMPTZ
                )
            """))
            migrations.append("batch_jobs table created")
        except Exception as e:
            migrations.append(f"batch_jobs: {e}")
            await db.rollback()

        # ---- Email Campaign tables ----
        email_tables = {
            "email_campaigns": """
                CREATE TABLE IF NOT EXISTS email_campaigns (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name VARCHAR(200) NOT NULL,
                    subject VARCHAR(500) NOT NULL,
                    body_html TEXT,
                    body_text TEXT,
                    target_audience VARCHAR(100),
                    target_state VARCHAR(2),
                    status VARCHAR(20) DEFAULT 'draft',
                    sent_count INTEGER DEFAULT 0,
                    open_count INTEGER DEFAULT 0,
                    click_count INTEGER DEFAULT 0,
                    unsubscribe_count INTEGER DEFAULT 0,
                    signup_count INTEGER DEFAULT 0,
                    bounce_count INTEGER DEFAULT 0,
                    send_rate INTEGER DEFAULT 200,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    started_at TIMESTAMPTZ,
                    completed_at TIMESTAMPTZ
                )
            """,
            "email_recipients": """
                CREATE TABLE IF NOT EXISTS email_recipients (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    campaign_id UUID REFERENCES email_campaigns(id) ON DELETE CASCADE,
                    email VARCHAR(255) NOT NULL,
                    name VARCHAR(500),
                    company VARCHAR(500),
                    state VARCHAR(2),
                    license_type VARCHAR(100),
                    status VARCHAR(20) DEFAULT 'pending',
                    sent_at TIMESTAMPTZ,
                    opened_at TIMESTAMPTZ,
                    clicked_at TIMESTAMPTZ,
                    unsubscribed_at TIMESTAMPTZ
                )
            """,
            "email_unsubscribes": """
                CREATE TABLE IF NOT EXISTS email_unsubscribes (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    email VARCHAR(255) NOT NULL UNIQUE,
                    reason TEXT,
                    unsubscribed_at TIMESTAMPTZ DEFAULT NOW()
                )
            """,
        }
        for table_name, ddl in email_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # Indexes for new tables
        indexes = [
            # Email campaign indexes
            "CREATE INDEX IF NOT EXISTS ix_ec_status ON email_campaigns (status)",
            "CREATE INDEX IF NOT EXISTS ix_ec_audience ON email_campaigns (target_audience)",
            "CREATE INDEX IF NOT EXISTS ix_er_campaign_status ON email_recipients (campaign_id, status)",
            "CREATE INDEX IF NOT EXISTS ix_er_email ON email_recipients (email)",
            "CREATE INDEX IF NOT EXISTS ix_er_sent_at ON email_recipients (sent_at)",
            "CREATE INDEX IF NOT EXISTS ix_eu_email ON email_unsubscribes (email)",
            "CREATE INDEX IF NOT EXISTS ix_cl_license ON contractor_licenses (license_number)",
            "CREATE INDEX IF NOT EXISTS ix_cl_name ON contractor_licenses (business_name)",
            "CREATE INDEX IF NOT EXISTS ix_cl_state ON contractor_licenses (state)",
            "CREATE INDEX IF NOT EXISTS ix_cl_status ON contractor_licenses (state, status)",
            "CREATE INDEX IF NOT EXISTS ix_epa_registry ON epa_facilities (registry_id)",
            "CREATE INDEX IF NOT EXISTS ix_epa_geo ON epa_facilities (lat, lng)",
            "CREATE INDEX IF NOT EXISTS ix_epa_state ON epa_facilities (state, city)",
            "CREATE INDEX IF NOT EXISTS ix_fema_state ON fema_flood_zones (state_abbrev, fld_zone)",
            "CREATE INDEX IF NOT EXISTS ix_fema_dfirm ON fema_flood_zones (dfirm_id)",
            "CREATE INDEX IF NOT EXISTS ix_census_geo ON census_demographics (state_fips, county_fips, tract, block_group)",
            "CREATE INDEX IF NOT EXISTS ix_census_state ON census_demographics (state_fips, county_fips)",
            "CREATE INDEX IF NOT EXISTS ix_septic_state ON septic_systems (state, city)",
            "CREATE INDEX IF NOT EXISTS ix_septic_geo ON septic_systems (lat, lng)",
            "CREATE INDEX IF NOT EXISTS ix_septic_addr ON septic_systems (address)",
            "CREATE INDEX IF NOT EXISTS ix_val_zip ON property_valuations (zip, period_end)",
            "CREATE INDEX IF NOT EXISTS ix_val_state ON property_valuations (state, zip)",
            "CREATE INDEX IF NOT EXISTS ix_entity_name ON business_entities (entity_name)",
            "CREATE INDEX IF NOT EXISTS ix_entity_filing ON business_entities (filing_number, state)",
            "CREATE INDEX IF NOT EXISTS ix_entity_state ON business_entities (state, entity_type)",
            "CREATE INDEX IF NOT EXISTS ix_entity_agent ON business_entities (registered_agent_name)",
            "CREATE INDEX IF NOT EXISTS ix_violations_vid ON code_violations (violation_id)",
            "CREATE INDEX IF NOT EXISTS ix_violations_addr ON code_violations (address)",
            "CREATE INDEX IF NOT EXISTS ix_violations_city ON code_violations (city)",
            "CREATE INDEX IF NOT EXISTS ix_violations_state ON code_violations (state)",
            "CREATE INDEX IF NOT EXISTS ix_violations_status ON code_violations (status)",
            "CREATE INDEX IF NOT EXISTS ix_violations_date ON code_violations (violation_date)",
            "CREATE INDEX IF NOT EXISTS ix_violations_geo ON code_violations (lat, lng)",
            "CREATE INDEX IF NOT EXISTS ix_violations_source_vid ON code_violations (source, violation_id)",
            "CREATE INDEX IF NOT EXISTS ix_predictions_zip ON permit_predictions (zip)",
            "CREATE INDEX IF NOT EXISTS ix_predictions_state_score ON permit_predictions (state, prediction_score DESC)",
            "CREATE INDEX IF NOT EXISTS ix_predictions_scored_at ON permit_predictions (scored_at)",
            # property_sales indexes
            "CREATE INDEX IF NOT EXISTS ix_sales_doc_id ON property_sales (document_id)",
            "CREATE INDEX IF NOT EXISTS ix_sales_address ON property_sales (address)",
            "CREATE INDEX IF NOT EXISTS ix_sales_city ON property_sales (city)",
            "CREATE INDEX IF NOT EXISTS ix_sales_state ON property_sales (state)",
            "CREATE INDEX IF NOT EXISTS ix_sales_zip ON property_sales (zip)",
            "CREATE INDEX IF NOT EXISTS ix_sales_state_city ON property_sales (state, city)",
            "CREATE INDEX IF NOT EXISTS ix_sales_zip_date ON property_sales (zip, sale_date)",
            "CREATE INDEX IF NOT EXISTS ix_sales_sale_date ON property_sales (sale_date)",
            "CREATE INDEX IF NOT EXISTS ix_sales_grantor ON property_sales (grantor)",
            "CREATE INDEX IF NOT EXISTS ix_sales_grantee ON property_sales (grantee)",
            # property_liens indexes
            "CREATE INDEX IF NOT EXISTS ix_liens_doc_id ON property_liens (document_id)",
            "CREATE INDEX IF NOT EXISTS ix_liens_address ON property_liens (address)",
            "CREATE INDEX IF NOT EXISTS ix_liens_lien_type ON property_liens (lien_type)",
            "CREATE INDEX IF NOT EXISTS ix_liens_filing_number ON property_liens (filing_number)",
            "CREATE INDEX IF NOT EXISTS ix_liens_state ON property_liens (state)",
            "CREATE INDEX IF NOT EXISTS ix_liens_state_type ON property_liens (state, lien_type)",
            "CREATE INDEX IF NOT EXISTS ix_liens_filing_date ON property_liens (filing_date)",
            "CREATE INDEX IF NOT EXISTS ix_liens_debtor ON property_liens (debtor_name)",
            "CREATE INDEX IF NOT EXISTS ix_liens_filing_state ON property_liens (filing_number, state)",
            "CREATE INDEX IF NOT EXISTS ix_liens_zip ON property_liens (zip)",
            # call_logs indexes
            "CREATE INDEX IF NOT EXISTS ix_call_logs_user ON call_logs (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_call_logs_lead ON call_logs (lead_id)",
            "CREATE INDEX IF NOT EXISTS ix_call_logs_user_date ON call_logs (user_id, created_at)",
            "CREATE INDEX IF NOT EXISTS ix_call_logs_callback ON call_logs (user_id, callback_date) WHERE callback_date IS NOT NULL",
            # lead_statuses indexes
            "CREATE INDEX IF NOT EXISTS ix_lead_statuses_lead ON lead_statuses (lead_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_lead_status_user_lead ON lead_statuses (user_id, lead_id)",
            # CRM indexes
            "CREATE INDEX IF NOT EXISTS ix_contacts_user ON contacts (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_contacts_user_phone ON contacts (user_id, phone)",
            "CREATE INDEX IF NOT EXISTS ix_contacts_user_email ON contacts (user_id, email)",
            "CREATE INDEX IF NOT EXISTS ix_deals_user ON deals (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_deals_user_stage ON deals (user_id, stage)",
            "CREATE INDEX IF NOT EXISTS ix_deals_contact ON deals (contact_id)",
            "CREATE INDEX IF NOT EXISTS ix_crm_notes_contact ON crm_notes (contact_id)",
            "CREATE INDEX IF NOT EXISTS ix_crm_notes_deal ON crm_notes (deal_id)",
            "CREATE INDEX IF NOT EXISTS ix_commissions_user ON commissions (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_commissions_deal ON commissions (deal_id)",
            # quotes indexes
            "CREATE INDEX IF NOT EXISTS ix_quotes_user ON quotes (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_quotes_contact ON quotes (contact_id)",
            "CREATE INDEX IF NOT EXISTS ix_quotes_deal ON quotes (deal_id)",
            "CREATE INDEX IF NOT EXISTS ix_quotes_user_status ON quotes (user_id, status)",
            # teams indexes
            "CREATE INDEX IF NOT EXISTS ix_teams_owner ON teams (owner_id)",
            "CREATE INDEX IF NOT EXISTS ix_team_members_team ON team_members (team_id)",
            "CREATE INDEX IF NOT EXISTS ix_team_members_user ON team_members (user_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_team_members_team_user ON team_members (team_id, user_id)",
            # activities indexes
            "CREATE INDEX IF NOT EXISTS ix_activities_team ON activities (team_id)",
            "CREATE INDEX IF NOT EXISTS ix_activities_user ON activities (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_activities_team_created ON activities (team_id, created_at)",
            "CREATE INDEX IF NOT EXISTS ix_activities_user_created ON activities (user_id, created_at)",
            # webhooks indexes
            "CREATE INDEX IF NOT EXISTS ix_webhooks_user ON webhooks (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_webhooks_active ON webhooks (user_id, is_active)",
            # batch_jobs indexes
            "CREATE INDEX IF NOT EXISTS ix_batch_jobs_user ON batch_jobs (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_batch_jobs_user_created ON batch_jobs (user_id, created_at)",
        ]
        for idx_sql in indexes:
            try:
                await db.execute(text(idx_sql))
            except Exception:
                pass

        await db.commit()
    return {"migrations": migrations}


@app.get("/health/db")
async def health_db():
    """Test database connectivity for both primary and replica."""
    import time
    from app.database import primary_session_maker, replica_session_maker, _replica_is_separate
    from sqlalchemy import text
    result = {}

    # Test primary (T430)
    t0 = time.time()
    try:
        async with primary_session_maker() as db:
            r = await db.execute(text("SELECT reltuples::bigint FROM pg_class WHERE relname = 'permits'"))
            count = r.scalar()
        result["primary"] = {"status": "ok", "permits": count, "latency_ms": round((time.time() - t0) * 1000)}
    except Exception as e:
        result["primary"] = {"status": "error", "error": str(e), "latency_ms": round((time.time() - t0) * 1000)}

    # Test replica (R730-2)
    if _replica_is_separate:
        t0 = time.time()
        try:
            async with replica_session_maker() as db:
                r = await db.execute(text("SELECT reltuples::bigint FROM pg_class WHERE relname = 'permits'"))
                count = r.scalar()
            result["replica"] = {"status": "ok", "permits": count, "latency_ms": round((time.time() - t0) * 1000)}
        except Exception as e:
            result["replica"] = {"status": "error", "error": str(e), "latency_ms": round((time.time() - t0) * 1000)}
    else:
        result["replica"] = {"status": "not_configured", "note": "Using primary for all queries"}

    overall = "ok" if result["primary"]["status"] == "ok" else "degraded"
    return {"status": overall, **result}


STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# PWA: service worker must be served from root scope
@app.get("/sw.js", include_in_schema=False)
async def service_worker():
    return FileResponse(
        STATIC_DIR / "sw.js",
        media_type="application/javascript",
        headers={"Cache-Control": "no-cache", "Service-Worker-Allowed": "/"},
    )


_INDEX_HTML_CACHE: str | None = None

def _get_index_html() -> str:
    """Read index.html and patch the dead Twilio SDK URL at runtime."""
    global _INDEX_HTML_CACHE
    if _INDEX_HTML_CACHE is None:
        raw = (STATIC_DIR / "index.html").read_text()
        # Old SDK 1.14.3 is 403'd — replace with Voice SDK 2.x
        raw = raw.replace(
            'https://sdk.twilio.com/js/client/releases/1.14.3/twilio.js',
            'https://cdn.jsdelivr.net/npm/@twilio/voice-sdk@2/dist/twilio.min.js',
        )
        _INDEX_HTML_CACHE = raw
    return _INDEX_HTML_CACHE


@app.get("/")
async def root():
    from fastapi.responses import HTMLResponse
    return HTMLResponse(_get_index_html(), headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


# SPA catch-all routes — serve index.html for frontend pages
async def _spa_page():
    from fastapi.responses import HTMLResponse
    return HTMLResponse(_get_index_html(), headers={"Cache-Control": "no-cache, no-store, must-revalidate"})

for _path in ("/search", "/coverage", "/pricing", "/dashboard", "/contractors", "/alerts", "/properties", "/market", "/saved-searches", "/admin", "/dialer", "/crm", "/quotes", "/analyst", "/trends", "/batch", "/campaigns", "/unsubscribe"):
    app.get(_path, include_in_schema=False)(_spa_page)


@app.get("/api")
async def api_info():
    return {
        "name": "PermitLookup API",
        "version": settings.VERSION,
        "docs": "/docs",
        "description": "Building permit data API — search ~1B records from 180+ jurisdictions",
        "endpoints": {
            "search": "GET /v1/permits/search?address=...",
            "bulk": "POST /v1/permits/bulk",
            "coverage": "GET /v1/coverage",
            "usage": "GET /v1/usage",
            "signup": "POST /v1/signup",
            "alerts": "GET /v1/alerts",
            "properties": "GET /v1/properties/history?address=...",
            "market": "GET /v1/market/activity?zip=78701&months=6",
            "saved_searches": "GET /v1/saved-searches",
            "licenses": "GET /v1/licenses/verify?name=...&state=CA",
            "environmental": "GET /v1/environmental/risk?lat=...&lng=...&state=TX",
            "septic": "GET /v1/septic/lookup?address=...&state=FL",
            "demographics": "GET /v1/demographics/county?state=TX&county_fips=201",
            "valuations": "GET /v1/valuations/zip?zip=78701",
            "entities": "GET /v1/entities/search?name=Sunrise+Holdings&state=TX",
            "pipeline": "GET /v1/pipeline/permit-to-sale?zip=78701&months=12",
            "hot_zips": "GET /v1/pipeline/hot-zips?state=TX&limit=25",
            "violations_search": "GET /v1/violations/search?address=...&city=...&state=NY",
            "violations_property": "GET /v1/violations/property?address=123+Main+St&state=NY",
            "violations_stats": "GET /v1/violations/stats",
            "predictions_zip": "GET /v1/predictions/zip?zip=78701",
            "predictions_hotspots": "GET /v1/predictions/hotspots?state=TX&limit=50",
            "predictions_stats": "GET /v1/predictions/stats",
            "sales_search": "GET /v1/sales/search?address=...&state=NY",
            "sales_property": "GET /v1/sales/property?address=123+Main+St&state=NY",
            "sales_stats": "GET /v1/sales/stats",
            "liens_search": "GET /v1/liens/search?debtor=...&state=NY&lien_type=Tax+Lien",
            "liens_property": "GET /v1/liens/property?address=123+Main+St&state=NY",
            "liens_stats": "GET /v1/liens/stats",
            "dialer_queue": "GET /v1/dialer/queue?trade=roofing&state=TX&limit=25",
            "dialer_log": "POST /v1/dialer/log",
            "dialer_disposition": "POST /v1/dialer/disposition",
            "dialer_callbacks": "GET /v1/dialer/callbacks",
            "dialer_stats": "GET /v1/dialer/stats",
            "dialer_history": "GET /v1/dialer/history?page=1&page_size=25",
            "crm_contacts": "GET /v1/crm/contacts?q=...&page=1",
            "crm_contact_create": "POST /v1/crm/contacts",
            "crm_contact_detail": "GET /v1/crm/contacts/{id}",
            "crm_contact_from_lead": "POST /v1/crm/contacts/from-lead",
            "crm_deals": "GET /v1/crm/deals?stage=new",
            "crm_deal_create": "POST /v1/crm/deals",
            "crm_notes": "POST /v1/crm/notes",
            "crm_pipeline": "GET /v1/crm/pipeline",
            "crm_dashboard": "GET /v1/crm/dashboard",
            "crm_leaderboard": "GET /v1/crm/leaderboard?period=week",
            "crm_commissions": "GET /v1/crm/commissions",
            "crm_commissions_summary": "GET /v1/crm/commissions/summary",
            "crm_teams": "GET /v1/crm/teams",
            "crm_team_create": "POST /v1/crm/teams",
            "crm_team_members": "GET /v1/crm/teams/{id}/members",
            "crm_team_add_member": "POST /v1/crm/teams/{id}/members",
            "crm_team_update_member": "PUT /v1/crm/teams/{id}/members/{member_id}",
            "crm_team_dashboard": "GET /v1/crm/teams/{id}/dashboard",
            "crm_territories": "GET /v1/crm/territories",
            "quotes_list": "GET /v1/quotes",
            "quotes_create": "POST /v1/quotes",
            "quotes_detail": "GET /v1/quotes/{id}",
            "quotes_update": "PUT /v1/quotes/{id}",
            "quotes_send": "POST /v1/quotes/{id}/send",
            "analyst_query": "POST /v1/analyst/query",
            "analyst_suggestions": "GET /v1/analyst/suggestions",
            "analyst_report": "GET /v1/analyst/report?address=123+Main+St&city=Austin&state=TX",
            "trends_zip": "GET /v1/trends/zip?zip=78701&months=12",
            "trends_contractor": "GET /v1/trends/contractor?name=ABC+Builders&months=24",
            "trends_market": "GET /v1/trends/market?state=TX&months=12",
            "trends_entity": "GET /v1/trends/entity?name=Sunrise+Holdings+LLC",
            "trends_stats": "GET /v1/trends/stats",
            "crm_activity_feed": "GET /v1/crm/activity-feed",
            "crm_leads_assign": "POST /v1/crm/leads/assign",
            "crm_leads_assigned": "GET /v1/crm/leads/assigned",
            "webhooks_list": "GET /v1/crm/webhooks",
            "webhooks_create": "POST /v1/crm/webhooks",
            "webhooks_update": "PUT /v1/crm/webhooks/{id}",
            "webhooks_delete": "DELETE /v1/crm/webhooks/{id}",
            "webhooks_test": "POST /v1/crm/webhooks/{id}/test",
            "permits_export_csv": "GET /v1/permits/export?address=...&state=TX",
            "batch_submit": "POST /v1/batch/submit",
            "batch_status": "GET /v1/batch/{job_id}",
            "batch_history": "GET /v1/batch/history",
            "data_freshness": "GET /v1/freshness",
        },
    }
