"""Tests for the Brazoria permit-lead feed (Phase 3).

Two layers, both hermetic (no live DB):

1. Classifier unit tests — app/services/permit_lead_classify.classify_permit on
   a corpus of REAL Brazoria descriptions sampled from hot_leads. Asserts the
   normalized lead_class and 911-source precedence.

2. Endpoint tests — httpx.ASGITransport against the FastAPI app. Asserts auth
   (require_demo_key), param validation, and OpenAPI registration. Mirrors
   tests/test_unserviced_hail.py.
"""

import os

import httpx
import pytest

# Force the demo key ON so require_demo_key enforces. Must be set before
# app/config import (mirrors test_unserviced_hail.py).
os.environ.setdefault("DEMO_API_KEY", "test-demo-key-permit-leads")

from app.services.permit_lead_classify import (  # noqa: E402
    ADDRESS_TRIGGER_SOURCES,
    BRAZORIA_SOURCES,
    brazoria_sources_sql,
    classify_permit,
    trigger_sources_sql,
)

LIST_URL = "/v1/permit-leads/"
STATS_URL = "/v1/permit-leads/stats"
CSV_URL = "/v1/permit-leads/export.csv"


# ---------------------------------------------------------------------------
# 1) Classifier unit tests — real Brazoria descriptions.
# ---------------------------------------------------------------------------

# (description, expected_class) — sampled from real mgo_angleton rows.
_CASES = [
    ("NEW HOME BUILD", "new_construction"),
    ("NH PERMIT", "new_construction"),
    ("PLUMBING FOR NEW CONSTRUCTION SINGLE HOME, INCLUDES GAS.", "new_construction"),
    ("new residential electrical", "new_construction"),
    ("Install electrical wiring, outlets, switches and fixtures in new construction homes.", "new_construction"),
    ("NEW HVAC INSTALL 4.0 TON SYSTEM FOR NEW HOME BUILD", "new_construction"),
    ("CERTIFICATE OF OCCUPANCY", "new_construction"),
    ("New Home Whole System", "new_construction"),
    # additions
    ("Phase 1 adding 21x30' slab extension", "addition"),
    ("CARPORT", "addition"),
    ("Residential Accessory Structures/Install a shed that is 10 x 14 feet", "addition"),
    ("Wooden gazebo with metal roof in back yard", "addition"),
    ("12'x12'x16' metal building", "addition"),
    ("Pour a 600 sq ft (20'x 30') concrete slab", "addition"),
    # remodels
    ("RE-ROOF HOUSE", "remodel"),
    ("Full Roof Replacement", "remodel"),
    ("REPLACE 12.5 TON HVAC UNIT ON ROOF TOP", "remodel"),
    ("whole house re-pipe of potable lines and replace 30' water service line in yard", "remodel"),
    ("REPAIR FOUNDATION", "remodel"),
    ("Replacing damaged siding", "remodel"),
    ("INSTALL 18 SOLAR PANELS ON ROOF OF HOUSE", "remodel"),
    ("Electrical Service Upgrade", "remodel"),
    # other (portal noise)
    ("GARAGE SALE - 6/6/2026", "other"),
    ("GS", "other"),
    ("HEALTH PERMIT", "other"),
    ("Food Service Establishment", "other"),
    ("Annual Gas Test", "other"),
    ("INSTALL IRRIGATION SYSTEM", "other"),
    ("Install 310 ft of 6'6\" wood privacy fence and gates", "other"),
    ("repair of parking lot.", "other"),
    ("RETAIL FOOD RENEWAL", "other"),
]


@pytest.mark.parametrize("desc,expected", _CASES)
def test_classify_real_descriptions(desc, expected):
    got = classify_permit("mgo_angleton", description=desc)
    assert got == expected, f"{desc!r} -> {got!r}, expected {expected!r}"


def test_911_source_is_always_new_construction():
    """A 911/NENA address-trigger source is new_construction regardless of text."""
    for src in ADDRESS_TRIGGER_SOURCES:
        assert classify_permit(src, description="garage sale") == "new_construction"
        assert classify_permit(src) == "new_construction"


def test_empty_blob_is_other():
    assert classify_permit("mgo_angleton", description=None) == "other"
    assert classify_permit("mgo_angleton", description="   ") == "other"


def test_returns_only_valid_classes():
    valid = {"new_construction", "addition", "remodel", "other"}
    for desc, _ in _CASES:
        assert classify_permit("mgo_angleton", description=desc) in valid


def test_source_registry_shape():
    """Every registry entry maps to (county:str, trigger:bool)."""
    assert "mgo_angleton" in BRAZORIA_SOURCES
    assert "brazoria_co_911_addresses" in BRAZORIA_SOURCES
    for _src, (county, trig) in BRAZORIA_SOURCES.items():
        assert isinstance(county, str) and county
        assert isinstance(trig, bool)
    # 911 source is a trigger; mgo is not.
    assert "brazoria_co_911_addresses" in ADDRESS_TRIGGER_SOURCES
    assert "mgo_angleton" not in ADDRESS_TRIGGER_SOURCES


def test_sql_list_helpers_are_well_formed():
    s = brazoria_sources_sql()
    assert s.startswith("(") and s.endswith(")")
    assert "'mgo_angleton'" in s
    t = trigger_sources_sql()
    assert "'brazoria_co_911_addresses'" in t


# ---------------------------------------------------------------------------
# 2) Endpoint tests — hermetic, auth + params + OpenAPI only.
# ---------------------------------------------------------------------------

def _client() -> httpx.AsyncClient:
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


@pytest.mark.asyncio
@pytest.mark.parametrize("url", [LIST_URL, STATS_URL, CSV_URL])
async def test_requires_auth(url):
    async with _client() as client:
        resp = await client.get(url)
    assert resp.status_code == 401, resp.text


@pytest.mark.asyncio
async def test_wrong_key_rejected():
    async with _client() as client:
        resp = await client.get(LIST_URL, headers={"X-API-Key": "wrong"})
    assert resp.status_code == 401, resp.text


@pytest.mark.asyncio
async def test_list_accepts_all_filter_params():
    key = os.environ["DEMO_API_KEY"]
    params = {
        "county": "Brazoria",
        "lead_class": "new_construction",
        "source": "mgo_angleton",
        "from_date": "2025-01-01",
        "to_date": "2026-12-31",
        "has_coords": "true",
        "page": 1,
        "page_size": 50,
    }
    async with _client() as client:
        resp = await client.get(LIST_URL, params=params, headers={"X-API-Key": key})
    # Auth passed + params accepted: NOT 401, NOT 422.
    assert resp.status_code not in (401, 422), resp.text


@pytest.mark.asyncio
async def test_list_rejects_bad_lead_class():
    key = os.environ["DEMO_API_KEY"]
    async with _client() as client:
        resp = await client.get(
            LIST_URL, params={"lead_class": "bogus"}, headers={"X-API-Key": key}
        )
    assert resp.status_code == 400, resp.text


@pytest.mark.asyncio
async def test_list_rejects_oversized_page():
    key = os.environ["DEMO_API_KEY"]
    async with _client() as client:
        resp = await client.get(
            LIST_URL, params={"page_size": 5000}, headers={"X-API-Key": key}
        )
    assert resp.status_code == 422, resp.text


@pytest.mark.asyncio
async def test_routes_in_openapi_spec():
    async with _client() as client:
        resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    paths = resp.json().get("paths", {})
    assert LIST_URL in paths or LIST_URL.rstrip("/") in paths
    assert STATS_URL in paths
    assert CSV_URL in paths
