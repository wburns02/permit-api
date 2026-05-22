"""Unit tests for app/burns_events — Burns Layer 4 emitter.

These tests run without a real Hatchet server, real Postgres, or any
external dep. We mock the Hatchet client via BurnsEmitter(hatchet_client=...).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.burns_events import (
    BurnsEmitter,
    EmitResult,
    get_emitter,
    is_enabled,
    reset_emitter,
)
from app.burns_events.permits import emit_permit_detected


@pytest.fixture(autouse=True)
def _reset_emitter_singleton():
    """Clear singleton + warning flag between tests."""
    reset_emitter(None)
    yield
    reset_emitter(None)


def _set_env(monkeypatch, **kwargs) -> None:
    for key, value in kwargs.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)


def _required_env_on(monkeypatch) -> None:
    _set_env(
        monkeypatch,
        BURNS_L4_EMIT_ENABLED="true",
        HATCHET_CLIENT_HOST_PORT="localhost:7077",
        HATCHET_CLIENT_TOKEN="t.dummy.token",
        BURNS_EVENTS_DSN="postgresql://x:y@localhost:5432/burns_events",
    )


# --- is_enabled() gating ---------------------------------------------------

def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv("BURNS_L4_EMIT_ENABLED", raising=False)
    assert is_enabled() is False


def test_enabled_when_flag_true_and_env_set(monkeypatch):
    _required_env_on(monkeypatch)
    assert is_enabled() is True


def test_disabled_when_flag_true_but_env_missing(monkeypatch):
    monkeypatch.setenv("BURNS_L4_EMIT_ENABLED", "true")
    monkeypatch.delenv("HATCHET_CLIENT_TOKEN", raising=False)
    monkeypatch.delenv("HATCHET_CLIENT_HOST_PORT", raising=False)
    monkeypatch.delenv("BURNS_EVENTS_DSN", raising=False)
    assert is_enabled() is False


def test_flag_off_means_emit_returns_no_op(monkeypatch):
    monkeypatch.delenv("BURNS_L4_EMIT_ENABLED", raising=False)

    fake_client = MagicMock()
    emitter = BurnsEmitter(hatchet_client=fake_client)

    result = emitter.emit(
        event_type="permitlookup.permit.detected",
        source="permitlookup.test",
        subject="permit:test",
        data={"permit_id": "permit:test"},
        links={"permit_id": "permit:test", "property_id": None, "person_id": None},
    )

    assert isinstance(result, EmitResult)
    assert result.emitted is False
    assert result.reason == "burns_l4_disabled"
    fake_client.event.push.assert_not_called()


# --- emit() happy path -----------------------------------------------------

def test_emit_calls_hatchet_when_enabled(monkeypatch):
    _required_env_on(monkeypatch)

    fake_pushed = MagicMock()
    fake_pushed.event_id = "hatchet-event-123"

    fake_client = MagicMock()
    fake_client.event.push.return_value = fake_pushed

    emitter = BurnsEmitter(hatchet_client=fake_client)

    result = emitter.emit(
        event_type="permitlookup.permit.detected",
        source="permitlookup.enrichment_worker",
        subject="permit:tx-travis-2026-0438211",
        data={
            "permit_id": "permit:tx-travis-2026-0438211",
            "address": "123 Test Rd, Austin, TX 78704",
            "trade": "septic",
            "county": "Travis",
            "state": "TX",
            "owner_name_raw": "ALAN OWNER",
        },
        links={
            "permit_id": "permit:tx-travis-2026-0438211",
            "property_id": None,
            "person_id": None,
        },
    )

    assert result.emitted is True
    assert result.reason is None
    assert result.hatchet_event_id == "hatchet-event-123"
    assert result.envelope is not None
    assert result.envelope["specversion"] == "1.0"
    assert result.envelope["type"] == "permitlookup.permit.detected"
    assert result.envelope["data"]["address"] == "123 Test Rd, Austin, TX 78704"

    fake_client.event.push.assert_called_once()
    call_kwargs = fake_client.event.push.call_args.kwargs
    assert call_kwargs["event_key"] == "permitlookup.permit.detected"
    assert call_kwargs["scope"] == "permit:tx-travis-2026-0438211"
    assert "burns_event_id" in call_kwargs["additional_metadata"]


def test_emit_rejects_invalid_payload(monkeypatch):
    """Missing required fields should fail validation, not silently push."""
    _required_env_on(monkeypatch)

    fake_client = MagicMock()
    emitter = BurnsEmitter(hatchet_client=fake_client)

    result = emitter.emit(
        event_type="permitlookup.permit.detected",
        source="permitlookup.test",
        subject="permit:test",
        data={"permit_id": "permit:test"},  # missing address/trade/etc
        links={"permit_id": "permit:test", "property_id": None, "person_id": None},
    )
    assert result.emitted is False
    assert result.reason is not None
    assert "validation_failed" in result.reason
    fake_client.event.push.assert_not_called()


# --- emit_permit_detected() helper -----------------------------------------

def test_helper_no_op_when_flag_off(monkeypatch):
    monkeypatch.delenv("BURNS_L4_EMIT_ENABLED", raising=False)

    fake_client = MagicMock()
    reset_emitter(BurnsEmitter(hatchet_client=fake_client))

    result = emit_permit_detected(
        permit_id="permit:test",
        address="123 X",
        trade="septic",
        county="Travis",
        state="TX",
        owner_name_raw="J DOE",
    )
    assert result.emitted is False
    assert result.reason == "burns_l4_disabled"
    fake_client.event.push.assert_not_called()


def test_helper_emits_when_flag_on(monkeypatch):
    _required_env_on(monkeypatch)

    fake_pushed = MagicMock()
    fake_pushed.event_id = "hatchet-helper-456"
    fake_client = MagicMock()
    fake_client.event.push.return_value = fake_pushed

    reset_emitter(BurnsEmitter(hatchet_client=fake_client))

    result = emit_permit_detected(
        permit_id="permit:tx-travis-2026-0438211",
        address="123 Test Rd, Austin, TX 78704",
        trade="septic",
        county="Travis",
        state="TX",
        owner_name_raw="ALAN OWNER",
        permit_number="2026-0438211",
        permit_date="2026-05-20",
        property_apn="0123-4567",
        property_id="parcel:travis-0123-4567",
        person_id=None,
    )

    assert result.emitted is True
    assert result.hatchet_event_id == "hatchet-helper-456"
    env = result.envelope
    assert env["data"]["permit_number"] == "2026-0438211"
    assert env["data"]["property_apn"] == "0123-4567"
    assert env["links"]["property_id"] == "parcel:travis-0123-4567"


def test_helper_coerces_unknown_trade(monkeypatch):
    _required_env_on(monkeypatch)

    fake_client = MagicMock()
    fake_client.event.push.return_value = MagicMock(event_id="x")
    reset_emitter(BurnsEmitter(hatchet_client=fake_client))

    result = emit_permit_detected(
        permit_id="permit:test",
        address="123 X",
        trade="something_weird",  # not in enum
        county="Travis",
        state="TX",
        owner_name_raw="J DOE",
    )
    assert result.emitted is True
    assert result.envelope["data"]["trade"] == "other"


# --- singleton lifecycle ---------------------------------------------------

def test_singleton_is_reused(monkeypatch):
    monkeypatch.delenv("BURNS_L4_EMIT_ENABLED", raising=False)
    a = get_emitter()
    b = get_emitter()
    assert a is b


# --- vendored schema sanity check -----------------------------------------

def test_schema_file_present_and_loadable():
    path = (
        Path(__file__).resolve().parent.parent
        / "app"
        / "burns_events"
        / "schemas"
        / "permitlookup.permit.detected"
        / "v1.json"
    )
    assert path.exists(), f"schema vendored at unexpected path: {path}"
    with path.open("r", encoding="utf-8") as fh:
        schema = json.load(fh)
    assert schema["title"].startswith("permitlookup.permit.detected")
