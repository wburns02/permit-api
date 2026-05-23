"""Tests for the Burns Industries Layer 4 emitter vendored in this repo.

Covers:
  - flag-off no-op (cheapest path)
  - schema validation success / rejection
  - ULID shape (26-char Crockford base32, no padding)
  - event_log INSERT-only path when Hatchet env is missing (warns once)
  - happy path (both event_log + Hatchet push)
  - caller never crashes when the DB or Hatchet client raise
  - the bridge_hot_leads_to_permits emit helper classifies trade correctly
  - the bridge emit helper passes a well-formed envelope through to emit_permit_detected
  - the bridge emit helper is itself fault-tolerant
"""
from __future__ import annotations

import importlib
import logging
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(autouse=True)
def _clean_burns_env(monkeypatch):
    """Strip all BURNS_/HATCHET_ env between tests and reset the singleton."""
    for var in (
        "BURNS_L4_EMIT_ENABLED",
        "BURNS_EVENTS_DSN",
        "HATCHET_CLIENT_TOKEN",
        "HATCHET_CLIENT_HOST_PORT",
        "HATCHET_CLIENT_TLS_STRATEGY",
    ):
        monkeypatch.delenv(var, raising=False)
    # Force a fresh import so module-level warning flags reset.
    if "burns_events" in sys.modules:
        del sys.modules["burns_events"]
    if "burns_events.emitter" in sys.modules:
        del sys.modules["burns_events.emitter"]
    import burns_events  # noqa: F401
    burns_events.reset_emitter()
    yield
    burns_events.reset_emitter()


def _enable(monkeypatch):
    monkeypatch.setenv("BURNS_L4_EMIT_ENABLED", "true")


def _valid_kwargs(**overrides):
    base = dict(
        permit_id="permit:tx-travis-test-001",
        address="100 Test Lane, Austin, TX 78701",
        trade="septic",
        county="Travis",
        state="TX",
        owner_name_raw="Test Owner",
    )
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# 1. Flag-off no-op
# ---------------------------------------------------------------------------

def test_flag_off_returns_none_and_does_no_io():
    """The cheapest path: no env, no envelope, no DB, no Hatchet."""
    from burns_events import emit_permit_detected, is_enabled
    assert is_enabled() is False
    result = emit_permit_detected(**_valid_kwargs())
    assert result is None


def test_flag_false_value_treated_as_off(monkeypatch):
    monkeypatch.setenv("BURNS_L4_EMIT_ENABLED", "false")
    from burns_events import emit_permit_detected, is_enabled
    assert is_enabled() is False
    assert emit_permit_detected(**_valid_kwargs()) is None


# ---------------------------------------------------------------------------
# 2. Schema validation
# ---------------------------------------------------------------------------

def test_envelope_passes_jsonschema_validation(monkeypatch):
    """Flag-on + injected DB writer captures envelope; validation passes."""
    _enable(monkeypatch)
    from burns_events import BurnsEmitter, reset_emitter

    captured = []
    em = BurnsEmitter(
        db_writer=lambda env: captured.append(env),
        hatchet_client=MagicMock(),  # not None so lazy init is skipped
    )
    em._hatchet.event.push.return_value = MagicMock(event_id="hatchet-evt-1")
    reset_emitter(em)

    from burns_events import emit_permit_detected
    result = emit_permit_detected(**_valid_kwargs())
    assert result is not None
    assert result.logged is True
    assert result.pushed is True
    assert result.emitted is True
    assert len(captured) == 1
    env = captured[0]
    assert env["specversion"] == "1.0"
    assert env["type"] == "permitlookup.permit.detected"
    assert env["data"]["trade"] == "septic"
    assert env["links"]["permit_id"].startswith("permit:")


def test_invalid_trade_coerced_to_other(monkeypatch):
    _enable(monkeypatch)
    from burns_events import BurnsEmitter, reset_emitter

    captured = []
    em = BurnsEmitter(
        db_writer=lambda env: captured.append(env),
        hatchet_client=MagicMock(),
    )
    em._hatchet.event.push.return_value = MagicMock(event_id="x")
    reset_emitter(em)

    from burns_events import emit_permit_detected
    result = emit_permit_detected(**_valid_kwargs(trade="garbage-trade-string"))
    assert result.emitted is True
    assert captured[0]["data"]["trade"] == "other"


def test_validation_rejects_bad_envelope(monkeypatch, caplog):
    """Force a schema violation by passing through emit() directly."""
    _enable(monkeypatch)
    caplog.set_level(logging.ERROR, logger="burns.l4.enrichment_worker")
    from burns_events import BurnsEmitter, reset_emitter

    em = BurnsEmitter(
        db_writer=lambda env: None,
        hatchet_client=MagicMock(),
    )
    reset_emitter(em)
    # Missing the required "owner_name_raw" field in data → validation failure.
    result = em.emit(
        event_type="permitlookup.permit.detected",
        source="permitlookup.enrichment_worker",
        subject="permit:tx-test-x",
        data={
            "permit_id": "permit:tx-test-x",
            "address": "1 X",
            "trade": "septic",
            "county": "Travis",
            "state": "TX",
            # owner_name_raw deliberately missing
        },
        links={"property_id": None, "permit_id": "permit:tx-test-x", "person_id": None},
    )
    assert result.emitted is False
    assert "validation_failed" in (result.reason or "")


# ---------------------------------------------------------------------------
# 3. ULID shape
# ---------------------------------------------------------------------------

def test_ulid_shape_is_26_crockford_chars():
    from burns_events.emitter import _new_event_id, _CROCKFORD
    ids = [_new_event_id() for _ in range(50)]
    assert all(len(i) == 26 for i in ids)
    valid = set(_CROCKFORD)
    for i in ids:
        assert set(i).issubset(valid), f"non-Crockford char in {i!r}"
    # Monotonicity is not guaranteed (no monotonic counter) but uniqueness is.
    assert len(set(ids)) == 50


# ---------------------------------------------------------------------------
# 4. Hatchet missing → event_log only, warn once
# ---------------------------------------------------------------------------

def test_missing_hatchet_env_writes_event_log_warns_once(monkeypatch, caplog):
    _enable(monkeypatch)
    caplog.set_level(logging.WARNING, logger="burns.l4.enrichment_worker")
    from burns_events import BurnsEmitter, reset_emitter

    captured = []
    em = BurnsEmitter(db_writer=lambda env: captured.append(env))
    # Force lazy hatchet path. No HATCHET_CLIENT_TOKEN set.
    reset_emitter(em)

    from burns_events import emit_permit_detected
    r1 = emit_permit_detected(**_valid_kwargs())
    r2 = emit_permit_detected(**_valid_kwargs(permit_id="permit:tx-test-002"))

    assert r1.logged is True and r1.pushed is False
    assert r2.logged is True and r2.pushed is False
    assert r1.reason == "hatchet_unavailable_event_logged_only"

    warnings = [r for r in caplog.records if "Hatchet env missing" in r.message]
    # One-shot warning across two calls.
    assert len(warnings) == 1


# ---------------------------------------------------------------------------
# 5. Caller never crashes
# ---------------------------------------------------------------------------

def test_caller_never_crashes_when_db_writer_raises(monkeypatch):
    _enable(monkeypatch)
    from burns_events import BurnsEmitter, reset_emitter

    def boom(_env):
        raise RuntimeError("db down")

    em = BurnsEmitter(db_writer=boom, hatchet_client=MagicMock())
    em._hatchet.event.push.return_value = MagicMock(event_id="ok")
    reset_emitter(em)

    from burns_events import emit_permit_detected
    # The injected db_writer raises raw (the BLE001 inside _write_event_log
    # only protects the real psycopg path). The exception is caught by the
    # outer try/except inside emit_permit_detected. The caller gets a
    # non-None EmitResult and NO exception propagates.
    result = emit_permit_detected(**_valid_kwargs())
    assert result is not None  # no exception propagated to the caller
    assert result.emitted is False
    assert result.reason is not None and "db down" in result.reason


def test_caller_never_crashes_when_hatchet_raises(monkeypatch):
    _enable(monkeypatch)
    from burns_events import BurnsEmitter, reset_emitter

    captured = []
    hatchet = MagicMock()
    hatchet.event.push.side_effect = RuntimeError("hatchet exploded")

    em = BurnsEmitter(
        db_writer=lambda env: captured.append(env),
        hatchet_client=hatchet,
    )
    reset_emitter(em)

    from burns_events import emit_permit_detected
    result = emit_permit_detected(**_valid_kwargs())
    assert result is not None
    assert result.logged is True
    assert result.pushed is False  # Hatchet push failed but no crash


# ---------------------------------------------------------------------------
# 6. Bridge helper — classify + emit shape
# ---------------------------------------------------------------------------

def test_bridge_trade_classifier_recognises_keywords():
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    if "bridge_hot_leads_to_permits" in sys.modules:
        del sys.modules["bridge_hot_leads_to_permits"]
    mod = importlib.import_module("bridge_hot_leads_to_permits")
    assert mod._burns_l4_classify_trade("OSSF Permit", "New install") == "septic"
    assert mod._burns_l4_classify_trade("Electrical", "Service upgrade") == "electrical"
    assert mod._burns_l4_classify_trade("Plumbing", "Repair") == "plumbing"
    assert mod._burns_l4_classify_trade("Roofing", "Re-roof") == "roofing"
    assert mod._burns_l4_classify_trade("HVAC", "Replace unit") == "hvac"
    assert mod._burns_l4_classify_trade("Foundation", "Remodel") == "other"
    assert mod._burns_l4_classify_trade(None, None) == "other"


def test_bridge_county_slug():
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    if "bridge_hot_leads_to_permits" in sys.modules:
        del sys.modules["bridge_hot_leads_to_permits"]
    mod = importlib.import_module("bridge_hot_leads_to_permits")
    assert mod._burns_l4_county_slug("Travis County") == "travis"
    assert mod._burns_l4_county_slug("St. Tammany Parish") == "st_tammany"
    assert mod._burns_l4_county_slug("O'Brien") == "obrien"
    assert mod._burns_l4_county_slug(None) == "unknown"


def test_bridge_emit_row_builds_proper_envelope(monkeypatch):
    _enable(monkeypatch)
    from burns_events import BurnsEmitter, reset_emitter

    captured = []
    em = BurnsEmitter(
        db_writer=lambda env: captured.append(env),
        hatchet_client=MagicMock(),
    )
    em._hatchet.event.push.return_value = MagicMock(event_id="ok")
    reset_emitter(em)

    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    if "bridge_hot_leads_to_permits" in sys.modules:
        del sys.modules["bridge_hot_leads_to_permits"]
    mod = importlib.import_module("bridge_hot_leads_to_permits")

    row = (
        "BP-2026-0001",  # permit_number
        "742 Evergreen Terrace, Austin, TX 78704",  # address
        "Austin",  # city
        "TX",  # state_code
        "Travis County",  # county
        "Bart Simpson",  # owner_name
        "OSSF",  # project_type
        "New Install",  # work_type
    )
    mod._burns_l4_emit_row(row, "TX")
    assert len(captured) == 1
    env = captured[0]
    assert env["data"]["permit_id"] == "permit:tx-travis-BP-2026-0001"
    assert env["data"]["trade"] == "septic"
    assert env["data"]["county"] == "Travis County"
    assert env["data"]["state"] == "TX"
    assert env["data"]["owner_name_raw"] == "Bart Simpson"
    assert env["data"]["permit_number"] == "BP-2026-0001"
    assert env["links"]["permit_id"] == "permit:tx-travis-BP-2026-0001"


def test_bridge_emit_row_is_fault_tolerant(monkeypatch, caplog):
    _enable(monkeypatch)
    caplog.set_level(logging.WARNING, logger="burns.l4.bridge")

    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    if "bridge_hot_leads_to_permits" in sys.modules:
        del sys.modules["bridge_hot_leads_to_permits"]
    mod = importlib.import_module("bridge_hot_leads_to_permits")

    # Wrong arity tuple → the helper's try/except should catch it.
    mod._burns_l4_emit_row(("only", "two"), "TX")
    # No crash. May or may not log a warning depending on where the unpack
    # failure lands.


def test_bridge_emit_row_skips_when_flag_off():
    # Flag is OFF in this test (autouse fixture clears the env).
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    if "bridge_hot_leads_to_permits" in sys.modules:
        del sys.modules["bridge_hot_leads_to_permits"]
    mod = importlib.import_module("bridge_hot_leads_to_permits")
    row = ("X", "1 main", "Austin", "TX", "Travis", "Owner", "OSSF", "New")
    # Must not raise and must not need any env.
    mod._burns_l4_emit_row(row, "TX")
