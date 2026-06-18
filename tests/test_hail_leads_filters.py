"""Tests for hail-leads has_permit filter (Phase 4 gap close-out).

All tests are hermetic — no live database connection required.

Strategy:
    - Unit-test _build_filter_sql directly to verify the SQL clause is correct.
    - Smoke-test both list and export endpoints via httpx.ASGITransport to verify
      the parameter is accepted without 422 and that the route auth flow is intact.
      (DB calls are expected to fail in CI; we only assert on HTTP status codes that
      are produced before the query runs, i.e. 401 when DEMO_API_KEY is unset and
      auth is effectively disabled -> 200/422/5xx are all acceptable for route
      reachability without a DB.)
"""

import pytest

# ---------------------------------------------------------------------------
# Unit tests: _build_filter_sql
# ---------------------------------------------------------------------------


def test_build_filter_sql_has_permit_none():
    """has_permit=None must not add any clause (backwards-compatible default)."""
    from app.api.v1.hail_leads import _build_filter_sql

    where_sql, params = _build_filter_sql(
        county=None,
        from_date=None,
        to_date=None,
        category=None,
        min_hail_inches=None,
        min_days_after=None,
        max_days_after=None,
        has_permit=None,
    )
    assert "issue_date" not in where_sql


def test_build_filter_sql_has_permit_true():
    """has_permit=True must add 'issue_date IS NOT NULL' to the WHERE clause."""
    from app.api.v1.hail_leads import _build_filter_sql

    where_sql, params = _build_filter_sql(
        county=None,
        from_date=None,
        to_date=None,
        category=None,
        min_hail_inches=None,
        min_days_after=None,
        max_days_after=None,
        has_permit=True,
    )
    assert "hl.issue_date IS NOT NULL" in where_sql


def test_build_filter_sql_has_permit_false():
    """has_permit=False must add 'issue_date IS NULL' to the WHERE clause."""
    from app.api.v1.hail_leads import _build_filter_sql

    where_sql, params = _build_filter_sql(
        county=None,
        from_date=None,
        to_date=None,
        category=None,
        min_hail_inches=None,
        min_days_after=None,
        max_days_after=None,
        has_permit=False,
    )
    assert "hl.issue_date IS NULL" in where_sql


def test_build_filter_sql_has_permit_combined_with_other_filters():
    """has_permit stacks correctly with other filters — both clauses present."""
    from app.api.v1.hail_leads import _build_filter_sql

    where_sql, params = _build_filter_sql(
        county="Travis",
        from_date=None,
        to_date=None,
        category=None,
        min_hail_inches=1.0,
        min_days_after=None,
        max_days_after=None,
        has_permit=False,
    )
    assert "hl.county ILIKE :county" in where_sql
    assert "hl.storm_magnitude >= :min_hail_inches" in where_sql
    assert "hl.issue_date IS NULL" in where_sql
    # Params must be parameterized — no raw value injection.
    assert params.get("county") == "Travis"
    assert params.get("min_hail_inches") == 1.0


def test_build_filter_sql_no_injection():
    """SQL clause for has_permit must not interpolate any user-supplied value."""
    from app.api.v1.hail_leads import _build_filter_sql

    for flag in (True, False, None):
        where_sql, params = _build_filter_sql(
            county=None,
            from_date=None,
            to_date=None,
            category=None,
            min_hail_inches=None,
            min_days_after=None,
            max_days_after=None,
            has_permit=flag,
        )
        # No new params should be introduced — has_permit uses literal SQL keywords only.
        # Base filter params (_allowed_categories) may be present; that's fine.
        assert "has_permit" not in params


def test_build_filter_sql_default_omits_has_permit_arg():
    """Calling _build_filter_sql without has_permit= uses None default."""
    from app.api.v1.hail_leads import _build_filter_sql

    where_sql, params = _build_filter_sql(
        county=None,
        from_date=None,
        to_date=None,
        category=None,
        min_hail_inches=None,
        min_days_after=None,
        max_days_after=None,
        # has_permit intentionally omitted — tests default=None
    )
    assert "issue_date" not in where_sql


# ---------------------------------------------------------------------------
# Smoke tests: endpoint accepts has_permit param without 422
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_endpoint_accepts_has_permit_false():
    """GET /v1/hail-leads/?has_permit=false must not return 422 (param is recognised)."""
    import httpx
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        resp = await client.get("/v1/hail-leads/", params={"has_permit": "false"})
    # 422 = FastAPI rejected the parameter — the one outcome we must not see.
    # 401 (unset DEMO_API_KEY in CI), 200, 422, or 5xx (DB absent) are all fine
    # except 422 which signals a type-validation rejection of the new param.
    assert resp.status_code != 422, (
        f"List endpoint rejected has_permit=false with 422: {resp.text}"
    )


@pytest.mark.asyncio
async def test_list_endpoint_accepts_has_permit_true():
    """GET /v1/hail-leads/?has_permit=true must not return 422."""
    import httpx
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        resp = await client.get("/v1/hail-leads/", params={"has_permit": "true"})
    assert resp.status_code != 422, (
        f"List endpoint rejected has_permit=true with 422: {resp.text}"
    )


@pytest.mark.asyncio
async def test_export_endpoint_accepts_has_permit_false():
    """GET /v1/hail-leads/export.csv?has_permit=false must not fail at param parsing.

    The export endpoint always reaches the DB (no graceful degradation on the
    streaming path), so in CI the request either returns an HTTP response or
    raises a DB connection error.  Either outcome proves the parameter was
    recognised (a 422 would be raised BEFORE the DB call).
    """
    import httpx
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            resp = await client.get("/v1/hail-leads/export.csv", params={"has_permit": "false"})
        # If we got a response, it must not be 422 (param-type rejection).
        assert resp.status_code != 422, (
            f"Export endpoint rejected has_permit=false with 422: {resp.text}"
        )
    except Exception as exc:
        # A DB connection error (asyncpg.InvalidPasswordError, sqlalchemy
        # OperationalError, etc.) is acceptable in CI — it means FastAPI
        # accepted the parameter and forwarded the request to the handler.
        # A 422 would have been returned as an HTTP response, not raised.
        exc_type = type(exc).__name__
        assert "422" not in str(exc), (
            f"Export endpoint raised an exception containing '422': {exc}"
        )
        # Known acceptable DB errors — anything else should propagate.
        known_db = ("InvalidPasswordError", "OperationalError", "EndOfStream",
                    "WouldBlock", "ClosedResourceError")
        if not any(k in exc_type for k in known_db):
            raise


@pytest.mark.asyncio
async def test_export_endpoint_accepts_has_permit_true():
    """GET /v1/hail-leads/export.csv?has_permit=true must not fail at param parsing."""
    import httpx
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            resp = await client.get("/v1/hail-leads/export.csv", params={"has_permit": "true"})
        assert resp.status_code != 422, (
            f"Export endpoint rejected has_permit=true with 422: {resp.text}"
        )
    except Exception as exc:
        exc_type = type(exc).__name__
        assert "422" not in str(exc), (
            f"Export endpoint raised an exception containing '422': {exc}"
        )
        known_db = ("InvalidPasswordError", "OperationalError", "EndOfStream",
                    "WouldBlock", "ClosedResourceError")
        if not any(k in exc_type for k in known_db):
            raise


@pytest.mark.asyncio
async def test_list_endpoint_openapi_includes_has_permit():
    """OpenAPI spec must include has_permit in the list endpoint's query parameters."""
    import httpx
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    spec = resp.json()

    list_params = [
        p["name"]
        for p in spec.get("paths", {}).get("/v1/hail-leads/", {}).get("get", {}).get("parameters", [])
    ]
    assert "has_permit" in list_params, (
        f"has_permit not found in list endpoint OpenAPI params. Found: {list_params}"
    )

    export_params = [
        p["name"]
        for p in spec.get("paths", {}).get("/v1/hail-leads/export.csv", {}).get("get", {}).get("parameters", [])
    ]
    assert "has_permit" in export_params, (
        f"has_permit not found in export endpoint OpenAPI params. Found: {export_params}"
    )
