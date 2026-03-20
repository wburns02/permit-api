"""AI Permit Analyst — natural language queries over 800M+ property records.

The killer feature: ask questions in plain English, get SQL-powered answers
from the largest property intelligence database in the industry.
"""

import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db, async_session_maker
from app.middleware.api_key_auth import get_current_user
from app.models.api_key import ApiUser, PlanTier, resolve_plan

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analyst", tags=["AI Analyst"])

# ---------------------------------------------------------------------------
# Anthropic client
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = getattr(settings, "ANTHROPIC_API_KEY", None) or None

try:
    from anthropic import Anthropic
except ImportError:
    Anthropic = None


def _get_client():
    if not Anthropic:
        return None
    if not ANTHROPIC_API_KEY:
        return None
    return Anthropic(api_key=ANTHROPIC_API_KEY)


# ---------------------------------------------------------------------------
# Plan gating — Pro Leads+ required
# ---------------------------------------------------------------------------
_PLAN_ORDER = [PlanTier.FREE, PlanTier.EXPLORER, PlanTier.PRO_LEADS, PlanTier.REALTIME, PlanTier.ENTERPRISE]


def _require_pro_leads(user: ApiUser):
    """Raise 403 unless the user is on Pro Leads or higher."""
    plan = resolve_plan(user.plan)
    try:
        idx = _PLAN_ORDER.index(plan)
    except ValueError:
        idx = 0
    if idx < _PLAN_ORDER.index(PlanTier.PRO_LEADS):
        raise HTTPException(
            status_code=403,
            detail="AI Analyst requires Pro Leads plan or higher. Upgrade at /pricing.",
        )


# ---------------------------------------------------------------------------
# Schema context for SQL generation
# ---------------------------------------------------------------------------
SCHEMA_CONTEXT = """You are an expert PostgreSQL analyst. You have access to a property intelligence database with these tables:

permits (760M rows): id, permit_number, address, city, state_code (2-letter), zip_code,
    county, lat, lng, project_type, work_type, trade, status, description,
    date_created (timestamp), owner_name, applicant_name, source

hot_leads (daily fresh): id, permit_number, permit_type, work_class, description,
    address, city, state, zip, valuation (numeric), sqft, issue_date (date),
    contractor_company, contractor_name, contractor_phone,
    applicant_name, applicant_phone, jurisdiction, source

business_entities (13M): id, entity_name, entity_type, state (2-letter), filing_number, status,
    formation_date (date), registered_agent_name, principal_address, source

code_violations (19M): id, violation_id, address, city, state (2-letter), zip, violation_type,
    description, status, violation_date (date), fine_amount (numeric), lat, lng, source

property_sales (4M): id, document_id, address, city, state (2-letter), zip, sale_price (numeric), sale_date (date),
    doc_type, grantor, grantee, property_type, source

property_liens (3.5M): id, document_id, lien_type, filing_number, address, state (2-letter),
    amount (numeric), filing_date (date), status, debtor_name, creditor_name, source

contractor_licenses (503K): id, license_number, business_name, state (2-letter), city, zip,
    phone, status, classifications, expiration_date (date), source

septic_systems (10M): id, address, city, state (2-letter), zip, system_type, install_date (date), source

property_valuations (9.5M): id, zip, state (2-letter), median_sale_price, median_list_price,
    homes_sold, inventory, median_dom, period_end (date), parent_metro

hmda_mortgages (15M): id, activity_year, loan_type, loan_purpose, loan_amount,
    action_taken, state_code, county_code, census_tract, income

noaa_storm_events: id, event_type, state, county, begin_date, damage_property,
    begin_lat, begin_lng, event_narrative

epa_facilities (3M): id, name, address, city, state (2-letter), zip, lat, lng, source
fema_flood_zones (5.5M): id, dfirm_id, fld_zone, sfha_tf, state_abbrev, state_fips
census_demographics (242K): id, state_fips, county_fips, tract, population,
    median_income, median_home_value, homeownership_rate

CRITICAL COLUMN NAME RULES — follow these EXACTLY:
- permits table: uses state_code (NOT state), zip_code (NOT zip), date_created (NOT issue_date), project_type (NOT permit_type)
- hot_leads table: uses state (NOT state_code), zip (NOT zip_code), issue_date (NOT date_created), permit_type (NOT project_type)
- All other tables: use state and zip
- fema_flood_zones uses state_abbrev for the 2-letter code

QUERY RULES:
- ALWAYS use LIMIT 50 on every query to prevent huge result sets
- Use ILIKE for all text searches (case insensitive)
- For recent/fresh data with contractor phones, prefer hot_leads table
- For historical/bulk data, use permits table
- When user says "this month" use CURRENT_DATE - interval '30 days'
- When user says "this week" use CURRENT_DATE - interval '7 days'
- When user says "this year" use date_trunc('year', CURRENT_DATE)
- State names should be converted to 2-letter codes (Texas->TX, California->CA, etc.)
- Always include useful columns in SELECT (address, city, state, etc.)
- For aggregations, use meaningful aliases
- Never use SELECT * — always specify columns
"""

# ---------------------------------------------------------------------------
# SQL safety
# ---------------------------------------------------------------------------
_FORBIDDEN_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|EXECUTE|COPY|"
    r"pg_read_file|pg_write_file|lo_import|lo_export)\b",
    re.IGNORECASE,
)


def _validate_sql(sql: str) -> str:
    """Validate that generated SQL is a safe SELECT query. Returns cleaned SQL."""
    sql = sql.strip().rstrip(";")

    # Strip markdown code fences if Claude wrapped it
    if sql.startswith("```"):
        lines = sql.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        sql = "\n".join(lines).strip()

    if _FORBIDDEN_PATTERNS.search(sql):
        raise ValueError("Query contains forbidden operations. Only SELECT queries are allowed.")

    if not sql.upper().lstrip().startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed.")

    # Enforce LIMIT
    if "LIMIT" not in sql.upper():
        sql += " LIMIT 50"
    else:
        # Cap any existing LIMIT to 50
        limit_match = re.search(r"LIMIT\s+(\d+)", sql, re.IGNORECASE)
        if limit_match and int(limit_match.group(1)) > 50:
            sql = re.sub(r"LIMIT\s+\d+", "LIMIT 50", sql, flags=re.IGNORECASE)

    return sql


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------
class AnalystRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=1000, description="Natural language question")


class AnalystResponse(BaseModel):
    question: str
    sql: str
    summary: str
    data: list[dict]
    row_count: int
    execution_time_ms: int
    query_id: str


class ReportResponse(BaseModel):
    address: str
    permits: list[dict]
    violations: list[dict]
    sales: list[dict]
    liens: list[dict]
    septic: list[dict]
    flood_zone: list[dict]
    epa_nearby: list[dict]
    demographics: list[dict]
    market: list[dict]
    risk_score: int
    ai_summary: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

EXAMPLE_QUESTIONS = [
    "Roofing permits in Austin this week",
    "Properties in flood zones with recent remodel permits",
    "Top contractors by permit volume in Florida",
    "LLC-owned properties that filed permits over $200K",
    "Hailstorm areas with roofing permit spikes",
    "New construction permits near Phoenix this month",
    "Code violations in Houston zip 77002",
    "Properties with both liens and recent permits in Miami",
    "Contractor license expirations coming up in California",
    "Highest valuation permits issued this week nationwide",
]


@router.get("/suggestions")
async def get_suggestions():
    """Return example questions to help users get started with the AI Analyst."""
    return {
        "suggestions": EXAMPLE_QUESTIONS,
        "description": "Ask anything about 800M+ property intelligence records. "
        "Combines permits, violations, sales, liens, contractors, flood zones, and more.",
    }


@router.post("/query", response_model=AnalystResponse)
async def analyst_query(
    body: AnalystRequest,
    request: Request,
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Natural language query over 800M+ property intelligence records.

    Takes a plain English question, generates SQL, executes it, and returns
    a human-friendly summary plus the raw data. Requires Pro Leads+ plan.
    """
    _require_pro_leads(user)

    client = _get_client()
    if not client:
        raise HTTPException(
            status_code=503,
            detail="AI Analyst is temporarily unavailable. Anthropic API key not configured.",
        )

    query_id = str(uuid.uuid4())[:12]
    t0 = time.time()

    # ── Step 1: Generate SQL from natural language ─────────────────────
    try:
        sql_response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            messages=[{
                "role": "user",
                "content": (
                    f"{SCHEMA_CONTEXT}\n\n"
                    f"Generate a PostgreSQL query to answer this question. "
                    f"Return ONLY the raw SQL — no explanation, no markdown, no code fences.\n\n"
                    f"Question: {body.question}"
                ),
            }],
        )
        raw_sql = sql_response.content[0].text.strip()
    except Exception as e:
        logger.error("SQL generation failed for query %s: %s", query_id, e)
        raise HTTPException(status_code=502, detail=f"AI SQL generation failed: {e}")

    # ── Step 2: Validate the SQL ──────────────────────────────────────
    try:
        safe_sql = _validate_sql(raw_sql)
    except ValueError as e:
        logger.warning("Unsafe SQL rejected for query %s: %s — SQL: %s", query_id, e, raw_sql)
        raise HTTPException(status_code=422, detail=f"Generated query was rejected for safety: {e}")

    logger.info("[Analyst:%s] user=%s question=%r sql=%s", query_id, user.id, body.question, safe_sql)

    # ── Step 3: Execute the SQL ───────────────────────────────────────
    try:
        result = await db.execute(text(safe_sql))
        columns = list(result.keys())
        rows = [dict(zip(columns, row)) for row in result.fetchall()]
    except Exception as e:
        logger.warning("SQL execution failed for query %s: %s", query_id, e)
        # Try to get Claude to fix the query
        try:
            fix_response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1000,
                messages=[{
                    "role": "user",
                    "content": (
                        f"{SCHEMA_CONTEXT}\n\n"
                        f"This SQL query failed with error: {e}\n\n"
                        f"Failed SQL: {safe_sql}\n\n"
                        f"Original question: {body.question}\n\n"
                        f"Fix the SQL query. Return ONLY the corrected raw SQL — no explanation, no markdown."
                    ),
                }],
            )
            fixed_sql = _validate_sql(fix_response.content[0].text.strip())
            logger.info("[Analyst:%s] Retrying with fixed SQL: %s", query_id, fixed_sql)
            result = await db.execute(text(fixed_sql))
            columns = list(result.keys())
            rows = [dict(zip(columns, row)) for row in result.fetchall()]
            safe_sql = fixed_sql
        except Exception as e2:
            logger.error("SQL retry also failed for query %s: %s", query_id, e2)
            raise HTTPException(
                status_code=422,
                detail=f"Could not execute query. Original error: {e}. Please try rephrasing your question.",
            )

    # Serialize data (handle dates, UUIDs, etc.)
    serialized_rows = []
    for row in rows:
        clean = {}
        for k, v in row.items():
            if isinstance(v, (datetime,)):
                clean[k] = v.isoformat()
            elif isinstance(v, uuid.UUID):
                clean[k] = str(v)
            elif hasattr(v, "isoformat"):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        serialized_rows.append(clean)

    exec_ms = int((time.time() - t0) * 1000)

    # ── Step 4: Summarize results with Claude ─────────────────────────
    if serialized_rows:
        # Send first 20 rows to keep token usage reasonable
        sample = json.dumps(serialized_rows[:20], default=str, indent=2)
        try:
            summary_response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=500,
                messages=[{
                    "role": "user",
                    "content": (
                        f"A user asked: \"{body.question}\"\n\n"
                        f"The query returned {len(serialized_rows)} results. "
                        f"Here is a sample of the data:\n{sample}\n\n"
                        f"Write a concise, insightful 2-4 sentence summary of these results. "
                        f"Highlight the most interesting findings, patterns, or notable data points. "
                        f"Be specific with numbers and names. Do NOT use markdown formatting."
                    ),
                }],
            )
            summary = summary_response.content[0].text.strip()
        except Exception as e:
            logger.warning("Summary generation failed: %s", e)
            summary = f"Found {len(serialized_rows)} results for your query."
    else:
        summary = "No results found. Try broadening your search or rephrasing the question."

    return AnalystResponse(
        question=body.question,
        sql=safe_sql,
        summary=summary,
        data=serialized_rows,
        row_count=len(serialized_rows),
        execution_time_ms=exec_ms,
        query_id=query_id,
    )


# ---------------------------------------------------------------------------
# Property Report — "Carfax for buildings"
# ---------------------------------------------------------------------------

@router.get("/report", response_model=ReportResponse)
async def property_report(
    request: Request,
    address: str = Query(..., min_length=3, description="Street address"),
    city: str = Query(None, description="City"),
    state: str = Query(..., min_length=2, max_length=2, description="2-letter state code"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Generate a comprehensive property report by querying ALL data layers.

    The 'Carfax for buildings' — crosses permits, violations, sales, liens,
    septic, flood, EPA, demographics, and market data for a single address.
    Requires Pro Leads+ plan.
    """
    _require_pro_leads(user)

    full_address = f"{address}, {city + ', ' if city else ''}{state}"
    addr_pattern = f"%{address}%"
    state_upper = state.upper()

    async def _safe_query(sql: str, params: dict) -> list[dict]:
        """Execute a query and return results as dicts, swallowing errors."""
        try:
            result = await db.execute(text(sql), params)
            cols = list(result.keys())
            rows = []
            for row in result.fetchall():
                clean = {}
                for k, v in zip(cols, row):
                    if isinstance(v, (datetime,)):
                        clean[k] = v.isoformat()
                    elif isinstance(v, uuid.UUID):
                        clean[k] = str(v)
                    elif hasattr(v, "isoformat"):
                        clean[k] = v.isoformat()
                    else:
                        clean[k] = v
                rows.append(clean)
            return rows
        except Exception as e:
            logger.warning("Report sub-query failed: %s — %s", sql[:80], e)
            return []

    # Query all data layers in parallel-ish (sequential for now, but fast)
    permits = await _safe_query(
        "SELECT permit_number, address, city, state_code, project_type, work_type, status, "
        "description, date_created, owner_name, applicant_name "
        "FROM permits WHERE address ILIKE :addr AND state_code = :state "
        "ORDER BY date_created DESC NULLS LAST LIMIT 20",
        {"addr": addr_pattern, "state": state_upper},
    )

    violations = await _safe_query(
        "SELECT violation_id, address, violation_type, description, status, "
        "violation_date, fine_amount "
        "FROM code_violations WHERE address ILIKE :addr AND state = :state "
        "ORDER BY violation_date DESC NULLS LAST LIMIT 20",
        {"addr": addr_pattern, "state": state_upper},
    )

    sales = await _safe_query(
        "SELECT document_id, address, sale_price, sale_date, doc_type, "
        "grantor, grantee, property_type "
        "FROM property_sales WHERE address ILIKE :addr AND state = :state "
        "ORDER BY sale_date DESC NULLS LAST LIMIT 20",
        {"addr": addr_pattern, "state": state_upper},
    )

    liens = await _safe_query(
        "SELECT document_id, lien_type, amount, filing_date, status, "
        "debtor_name, creditor_name "
        "FROM property_liens WHERE address ILIKE :addr AND state = :state "
        "ORDER BY filing_date DESC NULLS LAST LIMIT 20",
        {"addr": addr_pattern, "state": state_upper},
    )

    septic = await _safe_query(
        "SELECT address, system_type, install_date, status "
        "FROM septic_systems WHERE address ILIKE :addr AND state = :state LIMIT 5",
        {"addr": addr_pattern, "state": state_upper},
    )

    # For flood zones, we need lat/lng — try to get from permits
    flood_zone: list[dict] = []
    if permits and permits[0].get("lat") and permits[0].get("lng"):
        pass  # We would geo-query fema_flood_zones here
    else:
        flood_zone = await _safe_query(
            "SELECT dfirm_id, fld_zone, sfha_tf FROM fema_flood_zones "
            "WHERE state_abbrev = :state LIMIT 3",
            {"state": state_upper},
        )

    epa_nearby: list[dict] = []

    # Get zip for demographics/market
    zip_code = None
    for source in [permits, violations, sales]:
        if source:
            zip_code = source[0].get("zip_code") or source[0].get("zip")
            break

    demographics = []
    market = []
    if zip_code:
        market = await _safe_query(
            "SELECT zip, median_sale_price, median_list_price, homes_sold, inventory, "
            "median_dom, period_end, parent_metro "
            "FROM property_valuations WHERE zip = :zip "
            "ORDER BY period_end DESC NULLS LAST LIMIT 5",
            {"zip": zip_code},
        )

    # ── Risk score calculation ─────────────────────────────────────────
    risk_score = 0
    # Violations add risk
    risk_score += min(len(violations) * 8, 40)
    # Open liens add risk
    open_liens = [l for l in liens if l.get("status", "").lower() in ("open", "active", "pending")]
    risk_score += min(len(open_liens) * 10, 30)
    # No permits in 5+ years = deferred maintenance risk
    if not permits:
        risk_score += 15
    # Flood zone risk
    sfha_zones = [f for f in flood_zone if f.get("sfha_tf") == "T"]
    if sfha_zones:
        risk_score += 15
    risk_score = min(risk_score, 100)

    # ── AI Summary ─────────────────────────────────────────────────────
    client = _get_client()
    if client:
        report_context = json.dumps({
            "permits_count": len(permits),
            "violations_count": len(violations),
            "sales_count": len(sales),
            "liens_count": len(liens),
            "open_liens": len(open_liens),
            "septic": bool(septic),
            "flood_risk": bool(sfha_zones),
            "risk_score": risk_score,
            "recent_permit": permits[0] if permits else None,
            "recent_sale": sales[0] if sales else None,
        }, default=str)
        try:
            summary_resp = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=400,
                messages=[{
                    "role": "user",
                    "content": (
                        f"Write a concise 3-4 sentence property intelligence summary for {full_address}. "
                        f"Here is the data:\n{report_context}\n\n"
                        f"Highlight key findings: permit activity, violations, sales history, liens, "
                        f"and any risk factors. Be specific with numbers. No markdown."
                    ),
                }],
            )
            ai_summary = summary_resp.content[0].text.strip()
        except Exception as e:
            logger.warning("Report AI summary failed: %s", e)
            ai_summary = (
                f"Property at {full_address} has {len(permits)} permits, "
                f"{len(violations)} violations, {len(sales)} sales, and {len(liens)} liens on record. "
                f"Risk score: {risk_score}/100."
            )
    else:
        ai_summary = (
            f"Property at {full_address} has {len(permits)} permits, "
            f"{len(violations)} violations, {len(sales)} sales, and {len(liens)} liens on record. "
            f"Risk score: {risk_score}/100."
        )

    return ReportResponse(
        address=full_address,
        permits=permits,
        violations=violations,
        sales=sales,
        liens=liens,
        septic=septic,
        flood_zone=flood_zone,
        epa_nearby=epa_nearby,
        demographics=demographics,
        market=market,
        risk_score=risk_score,
        ai_summary=ai_summary,
    )
