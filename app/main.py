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
    description="Search 1B+ property and permit records from 180+ jurisdictions across 17+ states. "
    "Includes building permits, EPA records, septic/OWTS, property/parcel data, and construction permits. "
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

for _path in ("/search", "/coverage", "/pricing", "/dashboard", "/contractors", "/alerts", "/properties", "/market", "/saved-searches"):
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
        },
    }
