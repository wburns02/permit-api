"""Tests for the un-serviced hail-leads endpoints (roofer canvass list).

Hermetic — no live DB. Uses httpx.ASGITransport against the FastAPI app and
asserts on auth behaviour, parameter acceptance, and OpenAPI registration.
Mirrors the pattern in tests/test_security_sweep.py.

The endpoints under test:
    GET /v1/hail-leads/unserviced
    GET /v1/hail-leads/unserviced/export.csv

Both are gated by require_demo_key. In this test process DEMO_API_KEY is set so
the dependency enforces auth (401 without the header) BEFORE any DB access —
so these assertions never need a live database.
"""
import os

import httpx
import pytest

# Force the demo key ON so require_demo_key enforces (it fails open only when
# DEMO_API_KEY is empty). Must be set before app/config import.
os.environ.setdefault("DEMO_API_KEY", "test-demo-key-unserviced")

UNSERVICED_LIST = "/v1/hail-leads/unserviced"
UNSERVICED_CSV = "/v1/hail-leads/unserviced/export.csv"


def _client() -> httpx.AsyncClient:
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


@pytest.mark.asyncio
async def test_unserviced_list_requires_auth():
    """No X-API-Key → 401 before any DB access."""
    async with _client() as client:
        resp = await client.get(UNSERVICED_LIST)
    assert resp.status_code == 401, resp.text


@pytest.mark.asyncio
async def test_unserviced_csv_requires_auth():
    """No X-API-Key → 401 before any DB access."""
    async with _client() as client:
        resp = await client.get(UNSERVICED_CSV)
    assert resp.status_code == 401, resp.text


@pytest.mark.asyncio
async def test_unserviced_wrong_key_rejected():
    """A wrong X-API-Key → 401."""
    async with _client() as client:
        resp = await client.get(UNSERVICED_LIST, headers={"X-API-Key": "wrong"})
    assert resp.status_code == 401, resp.text


@pytest.mark.asyncio
async def test_unserviced_list_accepts_all_filter_params():
    """All filter params validate (no 422) once authenticated.

    We cannot reach a 200 without a live DB, but a malformed param would 422
    BEFORE the DB call. Anything that is not a validation error (401/422) means
    the params were accepted and the request progressed to the DB layer.
    """
    key = os.environ["DEMO_API_KEY"]
    params = {
        "county": "Tarrant",
        "min_hail_inches": 1.0,
        "from_date": "2024-01-01",
        "to_date": "2025-12-31",
        "max_days_since": 365,
        "page": 1,
        "page_size": 50,
    }
    async with _client() as client:
        resp = await client.get(
            UNSERVICED_LIST, params=params, headers={"X-API-Key": key}
        )
    # Auth passed and params accepted: NOT 401, NOT 422.
    assert resp.status_code not in (401, 422), resp.text


@pytest.mark.asyncio
async def test_unserviced_csv_accepts_all_filter_params():
    """Export endpoint accepts the same filter params (no 422)."""
    key = os.environ["DEMO_API_KEY"]
    params = {
        "county": "Dallas",
        "min_hail_inches": 0.75,
        "from_date": "2024-06-01",
        "to_date": "2026-06-01",
        "max_days_since": 730,
    }
    async with _client() as client:
        resp = await client.get(
            UNSERVICED_CSV, params=params, headers={"X-API-Key": key}
        )
    assert resp.status_code not in (401, 422), resp.text


@pytest.mark.asyncio
async def test_unserviced_list_rejects_out_of_range_params():
    """Validation bounds enforced (e.g. page_size cap, negative hail)."""
    key = os.environ["DEMO_API_KEY"]
    async with _client() as client:
        # page_size above the 200 cap → 422
        resp = await client.get(
            UNSERVICED_LIST,
            params={"page_size": 5000},
            headers={"X-API-Key": key},
        )
    assert resp.status_code == 422, resp.text


@pytest.mark.asyncio
async def test_unserviced_routes_in_openapi_spec():
    """Both routes are registered and appear in the OpenAPI spec."""
    async with _client() as client:
        resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    paths = resp.json().get("paths", {})
    assert UNSERVICED_LIST in paths, "list route missing from OpenAPI spec"
    assert UNSERVICED_CSV in paths, "export.csv route missing from OpenAPI spec"
    # The list endpoint should advertise the filter params it accepts.
    get_op = paths[UNSERVICED_LIST].get("get", {})
    param_names = {p["name"] for p in get_op.get("parameters", [])}
    for expected in {
        "county",
        "min_hail_inches",
        "from_date",
        "to_date",
        "max_days_since",
        "page",
        "page_size",
    }:
        assert expected in param_names, f"param {expected!r} not in OpenAPI spec"


@pytest.mark.asyncio
async def test_unserviced_accepts_nueces_county_filter():
    """The Nueces (Corpus Christi) WIND arm is filterable via ?county=nueces.

    Cannot reach 200 without a live DB, but a county filter that progresses past
    validation (not 401/422) confirms the param is wired for the new arm.
    """
    key = os.environ["DEMO_API_KEY"]
    async with _client() as client:
        resp = await client.get(
            UNSERVICED_LIST,
            params={"county": "nueces", "page_size": 10},
            headers={"X-API-Key": key},
        )
    assert resp.status_code not in (401, 422), resp.text


def test_main_mv_sql_has_nueces_wind_arm():
    """Static guard: the unserviced_hail_leads MV SQL in app/main.py must wire
    the Nueces (Corpus Christi) WIND arm — its geometry CTE, the NUECESCAD CAD
    join, the wind storm filter (TEXAS cz_fips=355), and the UNION ALL branch.
    Mirrors the EBR/Ascension wind-arm shape. No DB needed.
    """
    import pathlib

    src = (
        pathlib.Path(__file__).resolve().parents[1] / "app" / "main.py"
    ).read_text()
    assert "nueces_parcel_geometries" in src, "Nueces geometry CTE missing"
    assert "cad_source = 'NUECESCAD'" in src, "NUECESCAD join missing"
    assert "cz_fips = 355" in src, "Nueces (TX 48355) wind storm filter missing"
    assert "FROM nueces_rows" in src, "Nueces UNION ALL branch missing"
    assert "'Nueces'::text" in src, "Nueces county_source label missing"
