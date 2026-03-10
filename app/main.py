"""PermitLookup API — Building permit data for contractors, investors, and insurers."""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.config import settings
from app.database import init_db

# Import models to register with SQLAlchemy
from app.models.permit import Permit, Jurisdiction  # noqa: F401
from app.models.api_key import ApiUser, ApiKey, UsageLog  # noqa: F401

# Import routers
from app.api.v1.permits import router as permits_router
from app.api.v1.auth import router as auth_router
from app.api.v1.billing import router as billing_router
from app.api.v1.coverage import router as coverage_router

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
    yield
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


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "version": settings.VERSION,
        "environment": settings.ENVIRONMENT,
    }


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


@app.get("/health/redis")
async def health_redis():
    """Test Redis connectivity."""
    import time
    t0 = time.time()
    try:
        from app.middleware.rate_limit import get_redis
        r = await get_redis()
        if r:
            await r.close()
            return {"status": "ok", "connected": True, "latency_ms": round((time.time() - t0) * 1000)}
        return {"status": "ok", "connected": False, "message": "Redis unavailable, using in-memory fallback", "latency_ms": round((time.time() - t0) * 1000)}
    except Exception as e:
        return {"status": "error", "error": str(e), "latency_ms": round((time.time() - t0) * 1000)}


@app.get("/health/tunnel-test")
async def tunnel_test(limit: int = 10):
    """Test tunnel with simple generate_series query."""
    import time
    from app.database import async_session_maker
    from sqlalchemy import text
    t0 = time.time()
    try:
        async with async_session_maker() as db:
            r = await db.execute(text(f"SELECT generate_series(1, {int(limit)})"))
            rows = [row[0] for row in r.fetchall()]
        return {"count": len(rows), "latency_ms": round((time.time() - t0) * 1000)}
    except Exception as e:
        return {"status": "error", "error": str(e), "latency_ms": round((time.time() - t0) * 1000)}


@app.get("/health/search-test")
async def search_test(limit: int = 3):
    """Test a raw search query with configurable LIMIT."""
    import time
    from app.database import async_session_maker
    from sqlalchemy import text
    t0 = time.time()
    try:
        async with async_session_maker() as db:
            r = await db.execute(text(
                "SELECT id, permit_number, address, city, state, zip, "
                "permit_type, status, description, issue_date, jurisdiction, source "
                "FROM permits "
                "WHERE LOWER(city) = 'campbell' AND UPPER(state) = 'CA' "
                f"ORDER BY issue_date DESC NULLS LAST LIMIT {int(limit)}"
            ))
            rows = [
                {"id": str(row[0]), "permit_number": row[1], "address": row[2],
                 "city": row[3], "state": row[4], "zip": row[5],
                 "permit_type": row[6], "status": row[7],
                 "description": str(row[8])[:100] if row[8] else None,
                 "issue_date": str(row[9]) if row[9] else None,
                 "jurisdiction": row[10], "source": row[11]}
                for row in r.fetchall()
            ]
        return {"count": len(rows), "results": rows, "latency_ms": round((time.time() - t0) * 1000)}
    except Exception as e:
        return {"status": "error", "error": str(e), "latency_ms": round((time.time() - t0) * 1000)}


@app.get("/")
async def root():
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
        },
    }
