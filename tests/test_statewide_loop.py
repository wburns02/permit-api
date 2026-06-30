"""DB-free tests for the statewide permit loop's deterministic pieces.

CI cannot reach the T430 permits DB, so these tests stub `harness.statewide.db`
and exercise the VERIFIER's backpressure logic plus the harness's JSON parsing
and registry state machine. The verifier-vs-live-DB proof lives in
harness/statewide/test_verifier.py (run manually against the real DB).
"""
from __future__ import annotations

import importlib
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

HARNESS = Path(__file__).resolve().parents[1] / "harness" / "statewide"
sys.path.insert(0, str(HARNESS))


@pytest.fixture()
def verify_mod(monkeypatch):
    """Import verify.py with a stubbed db module so no network is touched."""
    import types

    fake_db = types.ModuleType("db")
    state = {"rows": [], "ping": True, "url_live": True}

    def scalar(sql):
        # bounded count subquery
        return str(len(state["rows"]))

    def query(sql):
        cols = ["permit_number", "address", "permit_type", "issue_date", "city", "state"]
        return [[r.get(c, "") or "" for c in cols] for r in state["rows"]]

    fake_db.scalar = scalar
    fake_db.query = query
    fake_db.ping = lambda: state["ping"]
    sys.modules["db"] = fake_db

    verify = importlib.import_module("verify")
    importlib.reload(verify)
    monkeypatch.setattr(verify, "_live_feed", lambda url: state["url_live"])
    verify._state = state  # type: ignore[attr-defined]
    return verify


def _real_rows(n):
    rows = []
    base = date.today() - timedelta(days=10)
    for i in range(n):
        rows.append({
            "permit_number": f"BP-{i}",
            "address": f"{100 + i} ELM ST",
            "permit_type": "Building Permit",
            "issue_date": (base + timedelta(days=i)).isoformat(),
            "city": "SOMEWHERE",
            "state": "TX",
        })
    return rows


def test_verifier_passes_real_data(verify_mod):
    verify_mod._state["rows"] = _real_rows(10)
    res = verify_mod.verify("statewide_loop:tx_city_test", "http://x")
    assert res.passed, res.reason


def test_verifier_fails_too_few_rows(verify_mod):
    verify_mod._state["rows"] = _real_rows(2)  # < MIN_ROWS
    verify_mod._state["url_live"] = True
    res = verify_mod.verify("t", "http://x")
    assert not res.passed
    assert "too few rows" in res.reason


def test_verifier_fails_placeholder_addresses(verify_mod):
    rows = [{
        "permit_number": f"X{i}", "address": "123 TEST ST",
        "permit_type": None, "issue_date": None, "city": "C", "state": "TX",
    } for i in range(8)]
    verify_mod._state["rows"] = rows
    res = verify_mod.verify("t", None)
    assert not res.passed


def test_verifier_fails_zero_address_variety(verify_mod):
    rows = [{
        "permit_number": f"X{i}", "address": "500 REAL AVE",
        "permit_type": "Roof", "issue_date": "2026-01-01", "city": "C", "state": "TX",
    } for i in range(8)]
    verify_mod._state["rows"] = rows
    res = verify_mod.verify("t", "http://x")
    assert not res.passed
    assert "variety" in res.reason


def test_verifier_fails_implausible_dates(verify_mod):
    rows = []
    for i in range(8):
        rows.append({
            "permit_number": f"X{i}", "address": f"{i} DATE LN",
            "permit_type": "BP", "issue_date": "1700-01-01", "city": "C", "state": "TX",
        })
    verify_mod._state["rows"] = rows
    res = verify_mod.verify("t", "http://x")
    assert not res.passed


def test_verifier_db_unreachable(verify_mod):
    verify_mod._state["ping"] = False
    res = verify_mod.verify("t", None)
    assert not res.passed
    assert "unreachable" in res.reason


# ── registry state machine ────────────────────────────────────────────────
def test_registry_seed_and_terminal(tmp_path):
    import registry
    conn = registry.connect(tmp_path / "reg.db")
    rows = [
        {"name": "Austin", "jtype": "city", "vendor": "socrata"},
        {"name": "Harris County", "jtype": "county", "vendor": "county"},
    ]
    assert registry.seed(conn, rows) == 2
    # idempotent
    assert registry.seed(conn, rows) == 0
    assert registry.counts(conn) == {"pending": 2}
    pend = registry.next_pending(conn, 5)
    registry.update(conn, pend[0]["id"], state="verified", rows_loaded=12)
    assert registry.counts(conn)["verified"] == 1
    assert "verified" in registry.TERMINAL and "walled" in registry.TERMINAL


def test_source_tag_namespaced():
    import registry
    tag = registry.make_source_tag("New Braunfels", "city")
    assert tag == "statewide_loop:tx_city_new_braunfels"


# ── harness JSON extraction ───────────────────────────────────────────────
def test_extract_agent_json_picks_last_valid():
    import run_loop
    text = (
        'noise {"source_tag":"a","status":"walled"} more\n'
        'final {"jurisdiction":"X","source_tag":"statewide_loop:tx_city_x",'
        '"vendor":"socrata","rows_loaded":40,"status":"built",'
        '"has_reroof":true,"barrier_if_walled":null}'
    )
    obj = run_loop.extract_agent_json(text)
    assert obj["rows_loaded"] == 40
    assert obj["status"] == "built"


def test_extract_agent_json_none_on_garbage():
    import run_loop
    assert run_loop.extract_agent_json("no json here at all") is None
