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
    BusinessEntity, CodeViolation,
)

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

    # Start alert scheduler
    from app.services.scheduler import start_scheduler, stop_scheduler
    try:
        start_scheduler()
    except Exception as e:
        logger.warning("Failed to start alert scheduler: %s", e)

    yield

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
    allow_origins=[
        settings.FRONTEND_URL,
        "https://permits.ecbtx.com",
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_credentials=True,
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


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "version": settings.VERSION,
        "environment": settings.ENVIRONMENT,
    }


@app.post("/health/db/migrate-expansion")
async def migrate_expansion():
    """Add new columns and tables for industry expansion."""
    from app.database import async_session_maker
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
        }

        for table_name, ddl in new_tables.items():
            try:
                await db.execute(text(ddl))
                migrations.append(f"{table_name} table created")
            except Exception as e:
                migrations.append(f"{table_name}: {e}")
                await db.rollback()

        # Indexes for new tables
        indexes = [
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
    """Test database connectivity."""
    import time
    from app.database import async_session_maker
    from sqlalchemy import text
    t0 = time.time()
    try:
        async with async_session_maker() as db:
            r = await db.execute(text("SELECT COUNT(*) FROM permits"))
            count = r.scalar()
        return {"status": "ok", "permits": count, "latency_ms": round((time.time() - t0) * 1000)}
    except Exception as e:
        return {"status": "error", "error": str(e), "latency_ms": round((time.time() - t0) * 1000)}


STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def root():
    return FileResponse(STATIC_DIR / "index.html")


# SPA catch-all routes — serve index.html for frontend pages
async def _spa_page():
    return FileResponse(STATIC_DIR / "index.html")

for _path in ("/search", "/coverage", "/pricing", "/dashboard", "/contractors", "/alerts", "/properties", "/market", "/saved-searches", "/admin"):
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
        },
    }
