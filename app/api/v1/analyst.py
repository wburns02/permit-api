"""AI Permit Analyst — natural language queries over 800M+ property records.

The killer feature: ask questions in plain English, get SQL-powered answers
from the largest property intelligence database in the industry.
"""

import asyncio
import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_read_db
from app.middleware.api_key_auth import get_current_user
from app.models.api_key import ApiUser, PlanTier, resolve_plan

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analyst", tags=["AI Analyst"])

# ---------------------------------------------------------------------------
# Anthropic client
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = getattr(settings, "ANTHROPIC_API_KEY", None) or None

try:
    from app.services.llm_client import AsyncLocalAnthropic as AsyncAnthropic
except ImportError:
    AsyncAnthropic = None


def _get_client():
    if not AsyncAnthropic:
        return None
    if not ANTHROPIC_API_KEY:
        return None
    return AsyncAnthropic(api_key=ANTHROPIC_API_KEY, timeout=8.0)


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
    - CRITICAL: project_type is a free-form municipal classification CODE column (values like "RRPL", "CALT", "A2", "02 - Automobile") — it is NOT a normalized English category label. DO NOT filter by ILIKE on words like 'residential', 'commercial', 'new construction', 'remodel' against project_type — that will return zero rows for most cities. For category-style filters use description, work_type, or trade columns instead. For project_type, only use it for equality match against known codes.

hot_leads (daily fresh): id, permit_number, permit_type, work_class, description,
    address, city, state, zip, sqft, issue_date (date),
    contractor_company, contractor_name, contractor_phone,
    applicant_name, applicant_phone, jurisdiction, source
    - valuation (double precision): permit job value in dollars. Has a btree index — use
      `valuation > X` directly for "over $X" / "above $X" / "more than $X" filters.
      Do NOT cast or wrap with COALESCE; it's already numeric. NULL means unknown value,
      so `valuation > X` will exclude unknowns (which is what users want).

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
fema_flood_zones (5.5M): id, dfirm_id, fld_zone, sfha_tf, state_abbrev, state_fips — WARNING: has NO address, city, or zip columns. Cannot be joined to permits by address. Only useful for state-level flood zone statistics.
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
- TABLE SELECTION (CRITICAL — follow exactly):
  * "Austin", "Houston", "Dallas", "Waco", "San Antonio", "Texas", "TX", "roofing", "plumbing", "electrical", "recent", "this week", "this month", "new", "phone numbers" → USE hot_leads TABLE. NEVER use permits for these.
  * "top contractors", "rankings", "license", "Florida", "California", "FL", "CA" → USE contractor_licenses TABLE. 503K rows, fast. Status column is often NULL — do NOT filter on status. Do NOT join to other tables.
  * "historical", "all time", "2024", "2023", specific non-TX states → USE permits TABLE. MUST include narrow WHERE + LIMIT.
  * IMPORTANT: The word "permits" in a user's question does NOT mean use the permits table. "Show me roofing permits in Austin" → use hot_leads. The permits table (760M rows) has an 8-second timeout and will fail for most queries.
- RELATIVE DATE SEMANTICS (use date_trunc — produces tighter, index-friendly windows):
  * "this month" -> `issue_date >= date_trunc('month', CURRENT_DATE)`
  * "this week" -> `issue_date >= date_trunc('week', CURRENT_DATE)`
  * "this year" -> `issue_date >= date_trunc('year', CURRENT_DATE)`
  * "last month" -> `issue_date >= date_trunc('month', CURRENT_DATE - interval '1 month') AND issue_date < date_trunc('month', CURRENT_DATE)`
  * "last week" -> `issue_date >= date_trunc('week', CURRENT_DATE - interval '1 week') AND issue_date < date_trunc('week', CURRENT_DATE)`
  * "last year" -> `issue_date >= date_trunc('year', CURRENT_DATE - interval '1 year') AND issue_date < date_trunc('year', CURRENT_DATE)`
  * "recent" / "new" / "latest" (no explicit window) -> `issue_date >= CURRENT_DATE - interval '14 days'`
  * "last N days" -> `issue_date >= CURRENT_DATE - interval 'N days'`
- State names should be converted to 2-letter codes (Texas->TX, California->CA, etc.)
- CRITICAL: City values in hot_leads are stored inconsistently — Austin may be "AUSTIN", "Austin", or "Austin, TX". ALWAYS use `city ILIKE '%austin%'` (NOT `city = 'Austin'`). Same for any city filter.
- CRITICAL: When a city is mentioned AND its state is known, ALSO add a `state = 'XX'` predicate. The state column is a cheap btree filter and slices the candidate set ~30x before the trigram pass on city/description. NEVER emit a city-only predicate when the state is inferable. City -> state mapping:
  * TX: Austin, Houston, Dallas, San Antonio, Fort Worth, El Paso, Arlington, Waco, Plano, Corpus Christi, Lubbock, Garland, Irving, Frisco, Mckinney, Round Rock, Pearland, Sugar Land, Grand Prairie, Brownsville, Killeen, Pasadena, Mesquite, Carrollton, Midland, Denton, Abilene, Beaumont, Odessa, Tyler, Laredo
  * FL: Miami, Jacksonville, Tampa, Orlando, St Petersburg, Hialeah, Tallahassee, Fort Lauderdale, Cape Coral, Pembroke Pines, Hollywood, Gainesville, Coral Springs, Clearwater, Miramar, Palm Bay, West Palm Beach, Lakeland
  * CA: Los Angeles, San Diego, San Jose, San Francisco, Fresno, Sacramento, Long Beach, Oakland, Bakersfield, Anaheim, Santa Ana, Riverside, Stockton, Irvine, Fremont, San Bernardino, Modesto, Oxnard, Fontana, Glendale
  * AZ: Phoenix, Tucson, Mesa, Chandler, Scottsdale, Glendale, Gilbert, Tempe, Peoria, Surprise
  * NY: New York, Buffalo, Rochester, Yonkers, Syracuse, Albany
  * IL: Chicago, Aurora, Naperville, Joliet, Rockford, Springfield
  * NC: Charlotte, Raleigh, Greensboro, Durham, Winston-Salem, Fayetteville, Cary
  * GA: Atlanta, Augusta, Columbus, Savannah, Athens, Sandy Springs
  * WA: Seattle, Spokane, Tacoma, Vancouver, Bellevue, Kent
  * CO: Denver, Colorado Springs, Aurora, Fort Collins, Lakewood, Thornton
  * MA: Boston, Worcester, Springfield, Cambridge, Lowell
  * TN: Nashville, Memphis, Knoxville, Chattanooga, Clarksville
  * NV: Las Vegas, Henderson, Reno, North Las Vegas
  * OR: Portland, Eugene, Salem, Gresham, Hillsboro
  * OH: Columbus, Cleveland, Cincinnati, Toledo, Akron, Dayton
  * MI: Detroit, Grand Rapids, Warren, Sterling Heights, Ann Arbor
  * Example: "Roofing permits in Austin this month" -> `WHERE state = 'TX' AND city ILIKE '%austin%' AND ...`
- Always include useful columns in SELECT (address, city, state, etc.)
- For aggregations, use meaningful aliases
- Never use SELECT * — always specify columns
- CRITICAL: When searching for trade/work types (roofing, plumbing, electrical, etc.), ALWAYS search the description column with ILIKE, NOT permit_type. permit_type contains codes like 'BP', 'PP', 'EP', NOT trade names. Example: WHERE description ILIKE '%roof%' (correct) vs WHERE permit_type ILIKE '%roof%' (WRONG — will return 0 results)
- For broader trade matching, also check work_class: (description ILIKE '%roof%' OR work_class ILIKE '%roof%')
"""

# ---------------------------------------------------------------------------
# Date-filter stripper for no-results fallback
# ---------------------------------------------------------------------------
# Matches WHERE-clause predicates that filter on a date/timestamp column,
# whether they reference CURRENT_DATE, INTERVAL math, or literal dates.
# Used when the LLM-generated SQL returned 0 rows — we strip the date
# filter, re-run, and tell the user how stale the freshest record is.
_DATE_COLS = (
    "issue_date", "date_created", "sale_date", "violation_date",
    "filing_date", "install_date", "expiration_date", "begin_date",
    "period_end", "formation_date", "scored_at", "last_updated",
    "scraped_at", "last_inspection",
)
_DATE_PREDICATE_RE = re.compile(
    (
        r"(?:AND\s+|OR\s+)?"                                        # leading conjunction (optional)
        r"\(?\s*"                                                    # optional opening paren
        r"(?:[a-zA-Z_][a-zA-Z0-9_]*\.)?"                             # optional table alias
        r"(?:" + "|".join(_DATE_COLS) + r")\b"
        r"\s*(?:>=|<=|>|<|=|BETWEEN|IS\s+NOT\s+NULL|IS\s+NULL)"
        r"[^)]*?"                                                    # match up to a closing paren / next clause
        r"(?:CURRENT_DATE|INTERVAL\s+'[^']+'|'\d{4}-\d{2}-\d{2}'|\d+)"
        r"(?:\s*(?:\+|-|AND)\s*(?:INTERVAL\s+'[^']+'|CURRENT_DATE|'\d{4}-\d{2}-\d{2}'|\d+))*"
        r"\s*\)?"                                                    # optional closing paren
    ),
    re.IGNORECASE,
)


def _strip_date_filters(sql: str) -> str:
    """Remove date predicates from a SELECT's WHERE clause so we can find
    the freshest matching record regardless of date. Best-effort regex —
    leaves all non-date filters (city, state, ILIKE, etc.) intact.
    Returns the cleaned SQL, or the original if nothing matched.
    """
    cleaned = _DATE_PREDICATE_RE.sub(" ", sql)
    # Tidy up dangling AND/OR/WHERE artefacts
    cleaned = re.sub(r"\bWHERE\s+(AND|OR)\s+", "WHERE ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bAND\s+AND\b", "AND", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bOR\s+OR\b", "OR", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bWHERE\s+(ORDER|GROUP|LIMIT|HAVING)\b",
                     r"\1", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bAND\s+(ORDER|GROUP|LIMIT|HAVING)\b",
                     r"\1", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    # If nothing meaningful changed, return original
    return cleaned if cleaned != sql.strip() else sql


def _detect_date_column(sql: str) -> str | None:
    """Return the first known date column referenced in the SQL, or None."""
    low = sql.lower()
    for col in _DATE_COLS:
        if col in low:
            return col
    return None


# ---------------------------------------------------------------------------
# SQL safety
# ---------------------------------------------------------------------------
_FORBIDDEN_PATTERNS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|EXECUTE|COPY|"
    r"MERGE|CALL|VACUUM|REINDEX|LOCK|"
    r"pg_read_file|pg_write_file|lo_import|lo_export)\b",
    re.IGNORECASE,
)

# Strip single-quoted string literals (including '' escapes), $$-quoted strings,
# -- line comments, and /* */ block comments. Tokens like ALTER/DROP/UPDATE that
# only appear inside literals or comments must not trip the forbidden check.
_SQL_LITERAL_OR_COMMENT = re.compile(
    r"'(?:''|[^'])*'"            # 'single quoted', supports '' escape
    r"|\$\$.*?\$\$"              # $$dollar quoted$$
    r"|--[^\n]*"                 # -- line comment
    r"|/\*.*?\*/",               # /* block comment */
    re.DOTALL,
)


def _strip_sql_literals_and_comments(sql: str) -> str:
    """Replace string literals and comments with spaces so keyword checks see only code."""
    return _SQL_LITERAL_OR_COMMENT.sub(" ", sql)


def _validate_sql(sql: str) -> str:
    """Validate that generated SQL is a safe SELECT query. Returns cleaned SQL.

    The forbidden-keyword check is token-aware: string literals (e.g.
    ``ILIKE '%alteration%'``) and comments are stripped before scanning so they
    can't trip the safety check. Only real SQL keywords (DDL/DML) are rejected.
    Also enforces that the statement begins with SELECT or WITH ... SELECT (CTE).
    """
    sql = sql.strip().rstrip(";")

    # Strip markdown code fences if Claude wrapped it
    if sql.startswith("```"):
        lines = sql.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        sql = "\n".join(lines).strip()

    # Scan for forbidden keywords against a copy with string literals & comments removed
    scrubbed = _strip_sql_literals_and_comments(sql)

    if _FORBIDDEN_PATTERNS.search(scrubbed):
        raise ValueError("Query contains forbidden operations. Only SELECT queries are allowed.")

    # Reject multi-statement payloads (anything after a `;` in the scrubbed code).
    # Trailing whitespace/comments-only after `;` is fine — those were already scrubbed.
    if ";" in scrubbed.strip().rstrip(";"):
        raise ValueError("Multiple statements are not allowed. Only a single SELECT is permitted.")

    head = scrubbed.lstrip().upper()
    if not (head.startswith("SELECT") or head.startswith("WITH")):
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
    fallback: dict | None = None  # populated when we recovered from a 0-result query
    # When the original LLM-generated SQL returned 0 rows and a relaxed/upgraded
    # query produced the rows actually returned. UI should warn the user that
    # their filter did not match — these are closest related results, not matches.
    filter_relaxed: bool = False
    original_filter_matched: int | None = None


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
# Analyst-specific SQL execution
# ---------------------------------------------------------------------------
# The engine-wide asyncpg `command_timeout=10` (set in app/database.py) is
# correct for most read endpoints — long queries elsewhere are bugs. But
# analyst LLM-generated SQL legitimately spans wider scans and needs more
# headroom. We do NOT raise the engine-wide value; instead we override both
# layers here, per-query:
#
#   1. Postgres `statement_timeout` via `SET LOCAL` (server-side budget).
#      Requires an open transaction so the LOCAL scope is meaningful.
#   2. asyncpg per-execute `timeout=...` kwarg (client-side budget).
#      Without this, asyncpg's 10s client-side timer fires first and the
#      SET LOCAL is useless. We reach the raw asyncpg connection through
#      SQLAlchemy's `get_raw_connection().driver_connection`.
#
# Touching the engine config or any other endpoint is intentionally out of
# scope — those should keep failing fast at 10s.
ANALYST_SQL_TIMEOUT_S = 30.0


async def _analyst_fetch(db: AsyncSession, sql: str, timeout_s: float = ANALYST_SQL_TIMEOUT_S) -> tuple[list[str], list[tuple]]:
    """Execute analyst SQL with a per-query timeout that survives both layers.

    Opens an explicit transaction so SET LOCAL statement_timeout sticks, then
    uses the raw asyncpg connection's fetch(..., timeout=...) so the asyncpg
    client-side timer matches the Postgres-side budget.

    Returns (columns, rows-as-tuples). Caller serializes.
    """
    ms = int(timeout_s * 1000)
    async with db.begin():
        sa_conn = await db.connection()
        raw = await sa_conn.get_raw_connection()
        asyncpg_conn = raw.driver_connection  # asyncpg.Connection
        # Server-side budget. `SET LOCAL` is scoped to the current tx.
        await asyncpg_conn.execute(f"SET LOCAL statement_timeout = '{ms}ms'", timeout=5.0)
        # Client-side budget. Overrides the engine-wide command_timeout=10.
        records = await asyncpg_conn.fetch(sql, timeout=timeout_s)
    if not records:
        return [], []
    columns = list(records[0].keys())
    rows = [tuple(r.values()) for r in records]
    return columns, rows


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
    db: AsyncSession = Depends(get_read_db),
):
    """Natural language query over 800M+ property intelligence records.

    Takes a plain English question, generates SQL, executes it, and returns
    a human-friendly summary plus the raw data. Requires Pro Leads+ plan.
    """
    _require_pro_leads(user)

    # Shared state so the outer 40s timeout catch can log the generated SQL +
    # query_id even though they're set deep inside _run_analyst_query.
    debug_state: dict = {"query_id": None, "sql": None}

    try:
        return await asyncio.wait_for(
            _run_analyst_query(body, request, user, db, debug_state=debug_state),
            timeout=40.0,
        )
    except asyncio.TimeoutError:
        logger.error(
            "[Analyst] handler exceeded 40s reason=timeout question=%r query_id=%s sql=%s",
            body.question,
            debug_state.get("query_id"),
            debug_state.get("sql"),
        )
        raise HTTPException(
            status_code=504,
            detail="Query took longer than 40 seconds. Try a more specific question (add a city, state, or date filter).",
        )


async def _run_analyst_query(
    body: AnalystRequest,
    request: Request,
    user: ApiUser,
    db: AsyncSession,
    debug_state: dict | None = None,
) -> AnalystResponse:
    """Core handler logic for /v1/analyst/query, wrapped in asyncio.wait_for by the route.

    `debug_state` is a shared dict the outer 40s wrapper uses to log the generated
    SQL and query_id when it fires. We mutate it as soon as those values are
    available so the timeout catch isn't logging None.
    """
    if debug_state is None:
        debug_state = {}
    client = _get_client()
    if not client:
        raise HTTPException(
            status_code=503,
            detail="AI Analyst is temporarily unavailable. Anthropic API key not configured.",
        )

    query_id = str(uuid.uuid4())[:12]
    debug_state["query_id"] = query_id
    t0 = time.time()
    logger.info("[Analyst:%s] START question=%r", query_id, body.question)

    # ── Step 1: Generate SQL from natural language ─────────────────────
    # Use Haiku for SQL generation — fast (sub-second) and accurate for structured tasks
    # Fail fast on 529 (overloaded): one attempt + one retry, 1s sleep
    try:
        t_sql = time.time()
        raw_sql = None
        for _attempt in range(2):
            try:
                sql_response = await client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=300,
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
                break
            except Exception as retry_err:
                if "529" in str(retry_err) or "overloaded" in str(retry_err).lower():
                    logger.warning("[Analyst:%s] Anthropic overloaded, retry %d/2", query_id, _attempt + 1)
                    await asyncio.sleep(1)
                    continue
                raise
        if not raw_sql:
            raise Exception("Anthropic API overloaded after 2 retries")
        logger.info("[Analyst:%s] SQL generated in %.1fs", query_id, time.time() - t_sql)
    except Exception as e:
        logger.error("SQL generation failed for query %s: %s", query_id, e)
        raise HTTPException(status_code=502, detail=f"AI SQL generation failed: {e}")

    # ── Step 2: Validate the SQL ──────────────────────────────────────
    try:
        safe_sql = _validate_sql(raw_sql)
    except ValueError as e:
        logger.warning("Unsafe SQL rejected for query %s: %s — SQL: %s", query_id, e, raw_sql)
        raise HTTPException(status_code=422, detail=f"Generated query was rejected for safety: {e}")

    debug_state["sql"] = safe_sql
    logger.info("[Analyst:%s] user=%s question=%r sql=%s", query_id, user.id, body.question, safe_sql)

    # ── Step 3: Execute the SQL (30s budget — see _analyst_fetch docstring) ──
    try:
        columns, raw_rows = await _analyst_fetch(db, safe_sql)
        rows = [dict(zip(columns, row)) for row in raw_rows]
    except Exception as e:
        logger.warning("SQL execution failed for query %s: %s", query_id, e)
        # Try to get Claude to fix the query
        try:
            fix_response = await client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=300,
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
            columns, raw_rows = await _analyst_fetch(db, fixed_sql)
            rows = [dict(zip(columns, row)) for row in raw_rows]
            safe_sql = fixed_sql
            debug_state["sql"] = safe_sql
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
    logger.info("[Analyst:%s] DB done in %.1fs, %d rows", query_id, time.time() - t0, len(serialized_rows))

    # ── Step 3b: Sonnet fallback — if Haiku returned 0, retry with smarter model ──
    upgraded = False
    if not serialized_rows and time.time() - t0 > 32:
        raise HTTPException(
            status_code=504,
            detail="Query took longer than 40 seconds. Try a more specific question.",
        )
    if not serialized_rows and (time.time() - t0) < 6.0:
        logger.info("[Analyst:%s] 0 results from Haiku — upgrading to Sonnet", query_id)
        try:
            sonnet_response = await client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=400,
                messages=[{
                    "role": "user",
                    "content": (
                        f"{SCHEMA_CONTEXT}\n\n"
                        f"A previous attempt to answer this question returned 0 results. "
                        f"The failed SQL was: {safe_sql}\n\n"
                        f"Generate a BETTER PostgreSQL query that will actually return results. "
                        f"Try a different table or relax the filters.\n\n"
                        f"RELAXATION PRIORITY ORDER (apply in this order, stopping as soon as you have any results):\n"
                        f"1. Widen the date window (e.g. \"last 30 days\" -> \"last 90 days\" -> \"last 180 days\" -> \"this year\")\n"
                        f"2. Drop the dollar/valuation threshold\n"
                        f"3. Loosen industry keyword (use broader synonyms or partial matches)\n"
                        f"4. ONLY AS A LAST RESORT drop the city filter — and when you do, state in the SQL comment \"RELAXED: city filter dropped\"\n\n"
                        f"NEVER drop the city filter while keeping a tight keyword filter. NEVER drop both the city and the keyword filter in the same retry.\n\n"
                        f"Return ONLY the raw SQL — no explanation.\n\n"
                        f"Question: {body.question}"
                    ),
                }],
            )
            upgraded_sql = _validate_sql(sonnet_response.content[0].text.strip())
            logger.info("[Analyst:%s] Sonnet SQL: %s", query_id, upgraded_sql)
            columns2, raw_rows2 = await _analyst_fetch(db, upgraded_sql)
            rows2 = [dict(zip(columns2, row)) for row in raw_rows2]
            if rows2:
                serialized_rows = []
                for row in rows2:
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
                safe_sql = upgraded_sql
                upgraded = True
                logger.info("[Analyst:%s] Sonnet found %d rows", query_id, len(serialized_rows))
        except Exception as e:
            logger.warning("[Analyst:%s] Sonnet fallback failed: %s", query_id, e)

    exec_ms = int((time.time() - t0) * 1000)

    # ── Step 3c: Graceful no-results fallback ─────────────────────────
    # If we still have 0 rows after Haiku + Sonnet, strip the date
    # filters from the SQL and re-run. This lets us either:
    #   (a) show "most recent matching records regardless of date" when
    #       the geography/type has stale data, or
    #   (b) tell the user honestly that the freshest record is N days old.
    fallback_info: dict | None = None
    if not serialized_rows and safe_sql:
        # The current `db` session may be in a poisoned state from the
        # previous failed/timeout SQL — open a fresh session for the probe.
        from app.database import replica_session_maker

        stripped_sql = _strip_date_filters(safe_sql)
        if stripped_sql and stripped_sql != safe_sql.strip():
            date_col = _detect_date_column(safe_sql)
            # Ensure ORDER BY <date_col> DESC so we get the freshest first
            probe_sql = stripped_sql
            if date_col and "order by" not in probe_sql.lower():
                probe_sql = re.sub(
                    r"\s+LIMIT\s+\d+\s*$",
                    f" ORDER BY {date_col} DESC NULLS LAST LIMIT 10",
                    probe_sql,
                    flags=re.IGNORECASE,
                )
            elif "limit" not in probe_sql.lower():
                probe_sql += " LIMIT 10"

            logger.info("[Analyst:%s] No-results fallback — stripped date filter, probing: %s",
                        query_id, probe_sql)
            try:
                async with replica_session_maker() as probe_db:
                    probe_cols, probe_raw = await _analyst_fetch(probe_db, probe_sql)
                    probe_rows = [dict(zip(probe_cols, row)) for row in probe_raw]
            except Exception as e:
                logger.warning("[Analyst:%s] No-results fallback probe failed: %s", query_id, e)
                probe_rows = []

            if probe_rows:
                # Serialize
                serialized_probe = []
                latest_date = None
                for row in probe_rows:
                    clean = {}
                    for k, v in row.items():
                        if isinstance(v, datetime):
                            clean[k] = v.isoformat()
                            if date_col and k == date_col and (latest_date is None or v > latest_date):
                                latest_date = v
                        elif isinstance(v, uuid.UUID):
                            clean[k] = str(v)
                        elif hasattr(v, "isoformat"):
                            clean[k] = v.isoformat()
                            if date_col and k == date_col:
                                # Date-like (date, not datetime)
                                try:
                                    as_dt = datetime.combine(v, datetime.min.time(), tzinfo=timezone.utc) if not isinstance(v, datetime) else v
                                    if latest_date is None or as_dt > latest_date:
                                        latest_date = as_dt
                                except Exception:
                                    pass
                        else:
                            clean[k] = v
                    serialized_probe.append(clean)

                serialized_rows = serialized_probe
                safe_sql = probe_sql

                # Build fallback message
                if latest_date:
                    anchor = latest_date if latest_date.tzinfo else latest_date.replace(tzinfo=timezone.utc)
                    age_days = (datetime.now(timezone.utc) - anchor).days
                    latest_str = anchor.date().isoformat()
                    if age_days > 30:
                        reason = "data_stale"
                        msg = (
                            f"No results in your time window. The most recent matching record is from "
                            f"{latest_str} ({age_days} days ago) — data may be stale for this geography "
                            f"or permit type. Showing the {len(serialized_rows)} most recent records "
                            f"regardless of date."
                        )
                    else:
                        reason = "no_results_in_window"
                        msg = (
                            f"No results in your requested window. Broadened the search and found "
                            f"{len(serialized_rows)} recent records (latest: {latest_str}, "
                            f"{age_days} days ago)."
                        )
                else:
                    reason = "no_results_in_window"
                    latest_str = None
                    age_days = None
                    msg = (
                        f"No results in your requested window. Broadened the search and found "
                        f"{len(serialized_rows)} matching records."
                    )

                fallback_info = {
                    "applied": True,
                    "reason": reason,
                    "latest_record_date": latest_str,
                    "latest_record_age_days": age_days,
                    "user_message": msg,
                }
                logger.info("[Analyst:%s] Fallback succeeded — %d rows, latest=%s, age=%s",
                            query_id, len(serialized_rows), latest_str, age_days)

    exec_ms = int((time.time() - t0) * 1000)

    # ── Step 4: Summarize results with Claude ─────────────────────────
    # Skip summary if we've already used >8s (prevents timeout on summary call)
    elapsed = time.time() - t0
    if elapsed > 8.0 and serialized_rows:
        logger.warning("[Analyst:%s] Skipping summary — already at %.1fs", query_id, elapsed)
        summary = f"Found {len(serialized_rows)} results for your query."
    elif serialized_rows:
        # Send first 5 rows compact — keeps prompt small for speed
        sample = json.dumps(serialized_rows[:5], default=str, separators=(',', ':'))

        # Sparse results get funnel explanation + broadening suggestions
        if len(serialized_rows) < 5:
            summary_prompt = (
                f"A user asked: \"{body.question}\"\n\n"
                f"The query returned only {len(serialized_rows)} result(s). "
                f"Here is the data:\n{sample}\n\n"
                f"Write a concise summary of these results. "
                f"IMPORTANT: The query returned very few results. "
                f"First, explain the filter funnel — describe how each filter in the user's question "
                f"likely narrowed the results. Use approximate counts based on your knowledge of typical "
                f"permit volumes (e.g., 'A city like Austin likely has hundreds of permits per week, "
                f"but filtering to roofing narrows it significantly, and the $10K minimum cuts it further'). "
                f"Be specific and helpful.\n\n"
                f"Then suggest 2-3 broader searches the user could try to find more leads. "
                f"Format each suggestion on its own line starting with >> like:\n"
                f">> Roofing permits in Austin this week with phone numbers\n"
                f">> All permits in Austin this week over $10K with phone numbers\n\n"
                f"Make suggestions that remove one filter at a time so the user can see which filter to relax. "
                f"Do NOT use markdown formatting."
            )
        else:
            summary_prompt = (
                f"Question: \"{body.question}\" — {len(serialized_rows)} results. Sample:\n{sample}\n\n"
                f"2-3 sentence summary. Be specific with numbers/names. No markdown."
            )

        try:
            summary_response = await client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=250,
                messages=[{"role": "user", "content": summary_prompt}],
            )
            summary = summary_response.content[0].text.strip()
        except Exception as e:
            logger.warning("Summary generation failed: %s", e)
            summary = f"Found {len(serialized_rows)} results for your query."
    else:
        summary = "No results found. Try broadening your search or rephrasing the question."

    # Honest fallback signaling: rows came from either the Sonnet retry or the
    # date-strip probe \u2014 in both cases the user's original SQL returned 0 rows
    # and the rows we're about to return are "closest related," not matches.
    # Set filter_relaxed so the client UI can warn, and prepend a blunt notice
    # to the summary so even clients that ignore the flag see what happened.
    used_fallback = bool(serialized_rows) and (upgraded or fallback_info is not None)

    if upgraded and serialized_rows:
        summary = "\u2728 Upgraded AI found results. " + summary

    # If we recovered via the no-results fallback, prepend the user message
    # so the chat bubble explains what happened before any LLM summary.
    if fallback_info:
        summary = fallback_info["user_message"] + ("\n\n" + summary if summary else "")

    if used_fallback:
        summary = (
            "NO RESULTS MATCHED YOUR ORIGINAL FILTER. Showing closest related results:\n\n"
            + (summary or "")
        )

    return AnalystResponse(
        question=body.question,
        sql=safe_sql,
        summary=summary,
        data=serialized_rows,
        row_count=len(serialized_rows),
        execution_time_ms=exec_ms,
        query_id=query_id,
        fallback=fallback_info,
        filter_relaxed=used_fallback,
        original_filter_matched=0 if used_fallback else None,
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
    db: AsyncSession = Depends(get_read_db),
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
            summary_resp = await client.messages.create(
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


# ---------------------------------------------------------------------------
# HTML Property Report — "Carfax for Buildings" (printable)
# ---------------------------------------------------------------------------

def _html_table(rows: list[dict], columns: list[str], labels: dict | None = None) -> str:
    """Render a list of dicts as an HTML table. Only includes specified columns."""
    if not rows:
        return '<p style="color:#6b7280;font-size:13px;padding:12px 0">No data available.</p>'
    labels = labels or {}
    ths = "".join(f'<th>{labels.get(c, c.replace("_", " ").title())}</th>' for c in columns)
    body = ""
    for row in rows:
        tds = ""
        for c in columns:
            val = row.get(c, "")
            if val is None:
                val = "--"
            elif isinstance(val, float):
                if c in ("sale_price", "amount", "fine_amount", "valuation",
                         "median_sale_price", "median_list_price"):
                    val = f"${val:,.0f}"
                else:
                    val = f"{val:,.2f}"
            else:
                val = str(val)
            tds += f"<td>{val}</td>"
        body += f"<tr>{tds}</tr>"
    return f"<table><thead><tr>{ths}</tr></thead><tbody>{body}</tbody></table>"


def _risk_color(score: int) -> str:
    if score <= 25:
        return "#22c55e"
    elif score <= 50:
        return "#84cc16"
    elif score <= 75:
        return "#f59e0b"
    else:
        return "#ef4444"


def _risk_label(score: int) -> str:
    if score <= 25:
        return "Low Risk"
    elif score <= 50:
        return "Moderate"
    elif score <= 75:
        return "Elevated"
    else:
        return "High Risk"


def _build_html_report(report: ReportResponse) -> str:
    """Build a self-contained, printable HTML property report."""

    report_date = datetime.now(timezone.utc).strftime("%B %d, %Y")
    risk_color = _risk_color(report.risk_score)
    risk_label = _risk_label(report.risk_score)

    permits_table = _html_table(
        report.permits,
        ["permit_number", "project_type", "work_type", "status", "date_created", "owner_name"],
        {"date_created": "Date", "project_type": "Type", "work_type": "Work", "owner_name": "Owner"},
    )

    violations_table = _html_table(
        report.violations,
        ["violation_id", "violation_type", "description", "status", "violation_date", "fine_amount"],
        {"violation_date": "Date", "fine_amount": "Fine"},
    )

    sales_table = _html_table(
        report.sales,
        ["sale_date", "sale_price", "doc_type", "grantor", "grantee", "property_type"],
        {"sale_date": "Date", "sale_price": "Price"},
    )

    liens_table = _html_table(
        report.liens,
        ["lien_type", "amount", "filing_date", "status", "debtor_name", "creditor_name"],
        {"filing_date": "Filed"},
    )

    septic_table = _html_table(
        report.septic,
        ["system_type", "install_date", "status"],
        {"install_date": "Installed"},
    )

    epa_section = _html_table(
        report.epa_nearby,
        ["name", "address", "city", "state"],
    ) if report.epa_nearby else '<p style="color:#6b7280;font-size:13px;padding:12px 0">No EPA facilities found nearby.</p>'

    flood_section = _html_table(
        report.flood_zone,
        ["dfirm_id", "fld_zone", "sfha_tf"],
        {"dfirm_id": "DFIRM ID", "fld_zone": "Zone", "sfha_tf": "SFHA"},
    ) if report.flood_zone else '<p style="color:#6b7280;font-size:13px;padding:12px 0">No flood zone data available.</p>'

    market_table = _html_table(
        report.market,
        ["zip", "median_sale_price", "median_list_price", "homes_sold", "inventory", "median_dom", "period_end", "parent_metro"],
        {"median_sale_price": "Median Sale", "median_list_price": "Median List",
         "homes_sold": "Sold", "median_dom": "DOM", "period_end": "Period", "parent_metro": "Metro"},
    )

    demographics_section = _html_table(
        report.demographics,
        list(report.demographics[0].keys()) if report.demographics else [],
    ) if report.demographics else '<p style="color:#6b7280;font-size:13px;padding:12px 0">No demographics data for this location.</p>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Property Report — {report.address} | PermitLookup</title>
<style>
@media print {{
  body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
  .no-print {{ display: none !important; }}
  @page {{ margin: 0.5in; }}
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', sans-serif;
  background: #0a0a0f;
  color: #e8e8f0;
  line-height: 1.6;
  font-size: 13px;
}}
.report-wrap {{
  max-width: 900px;
  margin: 0 auto;
  padding: 40px 32px;
}}
.header {{
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  padding-bottom: 24px;
  border-bottom: 2px solid #6366f1;
  margin-bottom: 32px;
}}
.header .brand {{
  display: flex;
  align-items: center;
  gap: 10px;
}}
.header .logo-box {{
  width: 36px; height: 36px;
  background: linear-gradient(135deg, #6366f1, #a855f7);
  border-radius: 8px;
  display: flex; align-items: center; justify-content: center;
  font-size: 18px; font-weight: 800; color: #fff;
}}
.header .brand-text {{
  font-size: 20px; font-weight: 800; color: #e8e8f0;
}}
.header .meta {{
  text-align: right;
  font-size: 12px;
  color: #a0a0b8;
}}
h1 {{
  font-size: 28px;
  font-weight: 800;
  margin-bottom: 8px;
  color: #fff;
  line-height: 1.2;
}}
.summary-box {{
  background: #12121a;
  border: 1px solid #2a2a3a;
  border-radius: 12px;
  padding: 20px 24px;
  margin-bottom: 28px;
  font-size: 14px;
  line-height: 1.7;
  color: #c8c8d8;
}}
.risk-badge {{
  display: inline-flex;
  align-items: center;
  gap: 10px;
  background: #12121a;
  border: 2px solid {risk_color};
  border-radius: 12px;
  padding: 12px 20px;
  margin-bottom: 28px;
}}
.risk-circle {{
  width: 48px; height: 48px;
  border-radius: 50%;
  background: {risk_color}22;
  border: 3px solid {risk_color};
  display: flex; align-items: center; justify-content: center;
  font-size: 20px; font-weight: 800; color: {risk_color};
}}
.risk-info {{
  font-size: 12px; color: #a0a0b8;
}}
.risk-info strong {{
  display: block; font-size: 16px; color: {risk_color}; font-weight: 700;
}}
.section {{
  margin-bottom: 28px;
}}
.section h2 {{
  font-size: 16px;
  font-weight: 700;
  color: #818cf8;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  padding-bottom: 8px;
  border-bottom: 1px solid #2a2a3a;
  margin-bottom: 12px;
}}
table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}}
th {{
  text-align: left;
  padding: 8px 10px;
  background: #1a1a25;
  color: #a0a0b8;
  font-weight: 700;
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  border-bottom: 1px solid #2a2a3a;
}}
td {{
  padding: 7px 10px;
  border-bottom: 1px solid #1a1a25;
  color: #c8c8d8;
  max-width: 200px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}}
tr:hover td {{
  background: #12121a;
}}
.footer {{
  margin-top: 40px;
  padding-top: 20px;
  border-top: 1px solid #2a2a3a;
  text-align: center;
  font-size: 12px;
  color: #6a6a80;
}}
.footer a {{ color: #818cf8; text-decoration: none; }}
.print-btn {{
  position: fixed;
  bottom: 24px;
  right: 24px;
  padding: 12px 24px;
  background: linear-gradient(135deg, #6366f1, #8b5cf6);
  color: #fff;
  border: none;
  border-radius: 10px;
  font-size: 14px;
  font-weight: 700;
  cursor: pointer;
  box-shadow: 0 4px 20px rgba(99,102,241,.4);
  z-index: 100;
}}
.print-btn:hover {{ opacity: 0.9; }}
.stats-row {{
  display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px;
}}
.stat-card {{
  flex: 1; min-width: 120px;
  background: #12121a;
  border: 1px solid #2a2a3a;
  border-radius: 10px;
  padding: 14px 16px;
  text-align: center;
}}
.stat-card .num {{
  font-size: 24px; font-weight: 800; color: #fff;
}}
.stat-card .lbl {{
  font-size: 11px; color: #6a6a80; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 2px;
}}
</style>
</head>
<body>
<button class="print-btn no-print" onclick="window.print()">Print / Save PDF</button>
<div class="report-wrap">
  <div class="header">
    <div class="brand">
      <div class="logo-box">P</div>
      <span class="brand-text">PermitLookup</span>
    </div>
    <div class="meta">
      Property Intelligence Report<br>
      Generated {report_date}
    </div>
  </div>

  <h1>{report.address}</h1>

  <div class="stats-row">
    <div class="stat-card"><div class="num">{len(report.permits)}</div><div class="lbl">Permits</div></div>
    <div class="stat-card"><div class="num">{len(report.violations)}</div><div class="lbl">Violations</div></div>
    <div class="stat-card"><div class="num">{len(report.sales)}</div><div class="lbl">Sales</div></div>
    <div class="stat-card"><div class="num">{len(report.liens)}</div><div class="lbl">Liens</div></div>
  </div>

  <div class="risk-badge">
    <div class="risk-circle">{report.risk_score}</div>
    <div class="risk-info">
      <strong>{risk_label}</strong>
      Risk Score (0-100)
    </div>
  </div>

  <div class="summary-box">{report.ai_summary}</div>

  <div class="section">
    <h2>Permit History</h2>
    {permits_table}
  </div>

  <div class="section">
    <h2>Code Violations</h2>
    {violations_table}
  </div>

  <div class="section">
    <h2>Sales History</h2>
    {sales_table}
  </div>

  <div class="section">
    <h2>Liens</h2>
    {liens_table}
  </div>

  <div class="section">
    <h2>Septic Status</h2>
    {septic_table}
  </div>

  <div class="section">
    <h2>Environmental Risk</h2>
    <h3 style="font-size:13px;color:#a0a0b8;margin-bottom:8px;font-weight:600">EPA Facilities Nearby</h3>
    {epa_section}
    <h3 style="font-size:13px;color:#a0a0b8;margin:16px 0 8px;font-weight:600">FEMA Flood Zone</h3>
    {flood_section}
  </div>

  <div class="section">
    <h2>Market Context</h2>
    {market_table}
  </div>

  <div class="section">
    <h2>Demographics</h2>
    {demographics_section}
  </div>

  <div class="footer">
    Generated by <a href="https://permits.ecbtx.com">PermitLookup</a> &mdash; permits.ecbtx.com<br>
    Data sourced from public records across 3,000+ jurisdictions. Report is informational only.
  </div>
</div>
</body>
</html>"""


@router.get("/report/html", response_class=HTMLResponse)
async def property_report_html(
    request: Request,
    address: str = Query(..., min_length=3, description="Street address"),
    city: str = Query(None, description="City"),
    state: str = Query(..., min_length=2, max_length=2, description="2-letter state code"),
    user: ApiUser = Depends(get_current_user),
    db: AsyncSession = Depends(get_read_db),
):
    """Generate a printable HTML property intelligence report.

    Same data as /report but rendered as a self-contained HTML page with
    PermitLookup branding. Users can print to PDF via Ctrl+P.
    Requires Pro Leads+ plan.
    """
    # Re-use the JSON report endpoint logic
    report = await property_report(
        request=request,
        address=address,
        city=city,
        state=state,
        user=user,
        db=db,
    )
    html = _build_html_report(report)
    return HTMLResponse(content=html)


# ---------------------------------------------------------------------------
# Webhook Configuration & Delivery
# ---------------------------------------------------------------------------

class WebhookConfigRequest(BaseModel):
    webhook_url: str | None = Field(None, max_length=500)


class WebhookSendRequest(BaseModel):
    rows: list[dict]
    source_query: str = ""


@router.put("/webhook/config")
async def configure_webhook(
    body: WebhookConfigRequest,
    user: ApiUser = Depends(get_current_user),
):
    """Save or clear the user's CRM webhook URL."""
    _require_pro_leads(user)

    from app.database import primary_session_maker
    from sqlalchemy import update
    from app.models.api_key import ApiUser as ApiUserModel

    async with primary_session_maker() as db:
        await db.execute(
            update(ApiUserModel)
            .where(ApiUserModel.id == user.id)
            .values(webhook_url=body.webhook_url)
        )
        await db.commit()

    return {"status": "ok", "webhook_url": body.webhook_url}


@router.get("/webhook/config")
async def get_webhook_config(
    user: ApiUser = Depends(get_current_user),
):
    """Get the user's current webhook URL."""
    _require_pro_leads(user)

    from app.database import replica_session_maker
    from sqlalchemy import select
    from app.models.api_key import ApiUser as ApiUserModel

    async with replica_session_maker() as db:
        result = await db.execute(
            select(ApiUserModel.webhook_url).where(ApiUserModel.id == user.id)
        )
        url = result.scalar_one_or_none()

    return {"webhook_url": url}


@router.post("/webhook/test")
async def test_webhook(
    user: ApiUser = Depends(get_current_user),
):
    """Send a test payload to the user's configured webhook URL."""
    _require_pro_leads(user)

    from app.database import replica_session_maker
    from sqlalchemy import select
    from app.models.api_key import ApiUser as ApiUserModel
    from app.services.webhook_delivery import deliver_webhook

    async with replica_session_maker() as db:
        result = await db.execute(
            select(ApiUserModel.webhook_url).where(ApiUserModel.id == user.id)
        )
        url = result.scalar_one_or_none()

    if not url:
        raise HTTPException(status_code=400, detail="No webhook URL configured. Set one first via PUT /analyst/webhook/config.")

    test_payload = {
        "source": "PermitLookup AI Analyst",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": "test",
        "count": 1,
        "leads": [{
            "permit_number": "TEST-001",
            "address": "123 Test Street",
            "city": "Austin",
            "state": "TX",
            "zip": "78701",
            "description": "Test webhook payload from PermitLookup",
            "date": "2026-03-27",
            "valuation": 50000,
            "contact_name": "Test Contact",
            "phone": "555-000-0000",
            "contractor": "Test Contractor LLC",
            "source_query": "Webhook test",
        }],
    }

    success = await deliver_webhook(url, test_payload)
    if not success:
        raise HTTPException(status_code=502, detail="Webhook delivery failed. Check that the URL accepts POST requests with JSON body.")

    return {"status": "ok", "message": "Test payload delivered successfully."}


@router.post("/webhook/send")
async def send_to_webhook(
    body: WebhookSendRequest,
    user: ApiUser = Depends(get_current_user),
):
    """Proxy selected analyst rows to the user's configured webhook URL."""
    _require_pro_leads(user)

    if not body.rows:
        raise HTTPException(status_code=400, detail="No rows to send.")
    if len(body.rows) > 50:
        raise HTTPException(status_code=400, detail="Maximum 50 rows per webhook send.")

    from app.database import replica_session_maker
    from sqlalchemy import select
    from app.models.api_key import ApiUser as ApiUserModel
    from app.services.webhook_delivery import deliver_webhook

    async with replica_session_maker() as db:
        result = await db.execute(
            select(ApiUserModel.webhook_url).where(ApiUserModel.id == user.id)
        )
        url = result.scalar_one_or_none()

    if not url:
        raise HTTPException(status_code=400, detail="No webhook URL configured.")

    payload = {
        "source": "PermitLookup AI Analyst",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "count": len(body.rows),
        "leads": body.rows,
        "source_query": body.source_query,
    }

    success = await deliver_webhook(url, payload)
    if not success:
        raise HTTPException(status_code=502, detail="Webhook delivery failed after retries.")

    return {"status": "ok", "delivered": len(body.rows)}
