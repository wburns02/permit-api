"""Unit tests for the ArcGIS plat-record adapter (scripts/scrape_arcgis_plats.py).

Covers the pure, DB-free functions: date parsing (DateOnly string AND epoch-ms),
polygon centroid, the Manvel registry entry, and normalize_plat field mapping,
plus the BRAZORIA_SOURCES wiring that lands manvel_plats in /v1/permit-leads.

The live fetch + DB round-trip is verified out-of-band (see
docs/tx-permit-leads-plan.md Phase 1b verdicts: 48 manvel_plats rows proven).
"""

import importlib.util
import os

import pytest

_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "scripts", "scrape_arcgis_plats.py",
)


def _load_adapter():
    """Import the adapter script. It hard-depends on httpx + psycopg2 and calls
    sys.exit(1) at import time if either is missing (same pattern as the sibling
    scrapers), so we skip the script-backed tests when the deps are absent
    (e.g. CI's test job does not install them into the script runtime)."""
    for dep in ("httpx", "psycopg2"):
        if importlib.util.find_spec(dep) is None:
            pytest.skip(f"{dep} not installed; skipping script-backed plat tests")
    spec = importlib.util.spec_from_file_location("scrape_arcgis_plats", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_parse_plat_date_dateonly_string():
    plats = _load_adapter()
    # Manvel's PDZ layer types Date_ as esriFieldTypeDateOnly -> 'YYYY-MM-DD' str
    d = plats.parse_plat_date("2026-06-08")
    assert d is not None and (d.year, d.month, d.day) == (2026, 6, 8)


def test_parse_plat_date_epoch_ms_fallback():
    plats = _load_adapter()
    # classic esriFieldTypeDate (epoch ms) still parses, for other cities
    d = plats.parse_plat_date(1717804800000)  # 2024-06-08 UTC
    assert d is not None and (d.year, d.month) == (2024, 6)


def test_parse_plat_date_none_and_blank():
    plats = _load_adapter()
    assert plats.parse_plat_date(None) is None
    assert plats.parse_plat_date("") is None
    assert plats.parse_plat_date("not-a-date") is None


def test_polygon_centroid_rings():
    plats = _load_adapter()
    geom = {"rings": [[[-95.0, 29.0], [-95.2, 29.0], [-95.2, 29.2],
                       [-95.0, 29.2], [-95.0, 29.0]]]}
    lat, lng = plats.polygon_centroid(geom)
    assert -95.2 <= lng <= -95.0 and 29.0 <= lat <= 29.2


def test_polygon_centroid_point_passthrough():
    plats = _load_adapter()
    assert plats.polygon_centroid({"x": -95.3, "y": 29.5}) == (29.5, -95.3)


def test_polygon_centroid_empty():
    plats = _load_adapter()
    assert plats.polygon_centroid(None) == (None, None)
    assert plats.polygon_centroid({}) == (None, None)


def test_manvel_registry_entry():
    plats = _load_adapter()
    cfg = plats.CITIES["manvel"]
    assert cfg["source"] == "manvel_plats"
    assert cfg["county"] == "Brazoria" and cfg["state"] == "TX"
    assert "services7.arcgis.com" in cfg["url"]
    assert cfg["url"].rstrip("/").split("/")[-1].isdigit()  # ends in a layer id


def test_normalize_plat_full_row():
    plats = _load_adapter()
    cfg = plats.CITIES["manvel"]
    fm = plats.fields_for(cfg)
    feat = {
        "attributes": {
            "Name": "Meridiana Section 9 - Final Plat",
            "Status": "Approved",
            "PlatType": "Residential",
            "Date_": "2026-06-08",
            "PDFLink": "https://manveltx.portal.civicclerk.com/event/1/files/1",
        },
        "geometry": {"rings": [[[-95.37, 29.45], [-95.36, 29.45],
                                [-95.36, 29.44], [-95.37, 29.45]]]},
    }
    row = plats.normalize_plat(feat, cfg, fm)
    # permit_number is left NULL by the adapter (the DB trigger fills NOPN-…)
    assert row["permit_number"] is None
    assert row["permit_type"] == "NEW SUBDIVISION (PLAT)"
    assert row["work_class"] == "PLAT TRIGGER"
    # the project name is the lead locator (no street address on a plat)
    assert row["address"] == "Meridiana Section 9 - Final Plat"
    assert row["city"] == "Manvel" and row["county"] == "Brazoria"
    assert row["status"] == "Approved"
    assert (row["issue_date"].year, row["issue_date"].month) == (2026, 6)
    # centroid lands in Manvel (~29.4N, -95.3W)
    assert 29.4 <= row["lat"] <= 29.5 and -95.4 <= row["lng"] <= -95.3
    assert "PlatType" in row["description"] and "Packet" in row["description"]
    assert row["owner_name"] is None  # plats carry no owner


def test_normalize_plat_rejects_missing_name():
    plats = _load_adapter()
    cfg = plats.CITIES["manvel"]
    fm = plats.fields_for(cfg)
    assert plats.normalize_plat({"attributes": {"Name": None}, "geometry": {}},
                                cfg, fm) is None


def test_manvel_plats_registered_as_brazoria_trigger_source():
    # No script import needed — pure app-side wiring, so this runs in CI even
    # without the scraper's httpx/psycopg2 deps. The adapter source MUST be
    # wired into the Brazoria lead feed as a trigger so /v1/permit-leads
    # classifies plats as new_construction.
    from app.services.permit_lead_classify import (
        ADDRESS_TRIGGER_SOURCES,
        BRAZORIA_SOURCES,
        brazoria_sources_sql,
        classify_permit,
    )
    assert "manvel_plats" in BRAZORIA_SOURCES
    assert BRAZORIA_SOURCES["manvel_plats"] == ("Brazoria", True)
    assert "manvel_plats" in ADDRESS_TRIGGER_SOURCES
    assert "manvel_plats" in brazoria_sources_sql()
    assert classify_permit("manvel_plats", "NEW SUBDIVISION (PLAT)",
                           "PLAT TRIGGER", None) == "new_construction"
