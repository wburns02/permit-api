"""Integration tests for W-1 drilling permit watchlist alerts.

Runs against the live warehouse (T430) using throwaway fixture rows
(source='test_w1_alert_fixture', permit numbers in the 9900xxx test range)
and a local webhook catcher, exercising the REAL match + delivery pipeline
(alert_engine.execute_alert), not mocks. Email delivery is off in fixtures;
webhook delivery is asserted end to end.

Run: python3 -m pytest tests/test_w1_alerts.py -v
"""

import asyncio
import json
import os
import uuid
from datetime import date, datetime, timedelta, timezone

os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://will@100.122.216.15:5432/permits"
)

import pytest
from sqlalchemy import text

from app.database import primary_session_maker
from app.models.alert import PermitAlert, AlertFrequency
from app.services.alert_engine import execute_alert, match_alert

TEST_SOURCE = "test_w1_alert_fixture"
TEST_OPERATOR = "TESTALERT OPERATING LLC"
TEST_EMAIL = "w1-alert-test@ecbtx.com"
WEBHOOK_PORT = 18765

FIXTURES = [
    # (permit_number, county, lease, profile, depth, approved offset days)
    ("9900001", "MIDLAND", "TEST RANCH A", "horizontal", 11000, 0),
    ("9900002", "MIDLAND", "TEST RANCH B", "horizontal", 12000, 0),
    ("9900003", "LOVING", "TEST STATE 1", "vertical", 5000, 0),
]


class WebhookCatcher:
    """Minimal local HTTP server capturing JSON POST bodies."""

    def __init__(self):
        self.payloads = []
        self.server = None

    async def _handle(self, reader, writer):
        data = await reader.read(1 << 20)
        try:
            body = data.split(b"\r\n\r\n", 1)[1]
            self.payloads.append(json.loads(body))
        except Exception:
            pass
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
        await writer.drain()
        writer.close()

    async def __aenter__(self):
        self.server = await asyncio.start_server(
            self._handle, "127.0.0.1", WEBHOOK_PORT)
        return self

    async def __aexit__(self, *exc):
        self.server.close()
        await self.server.wait_closed()


async def seed():
    async with primary_session_maker() as db:
        # fixture user (deactivated; engine matching doesn't gate on it)
        await db.execute(text("""
            INSERT INTO api_users (id, email, plan, is_active)
            VALUES (gen_random_uuid(), :email, 'ENTERPRISE', false)
            ON CONFLICT (email) DO NOTHING
        """), {"email": TEST_EMAIL})
        for num, county, lease, profile, depth, off in FIXTURES:
            await db.execute(text("""
                INSERT INTO canonical.well_permits
                    (state, permit_number, county, operator_name_raw,
                     lease_name, well_number, district, wellbore_profile,
                     filing_purpose, total_depth, current_status,
                     submitted_date, approved_date, source)
                VALUES ('TX', :num, :county, :op, :lease, '1H', '08',
                        :profile, 'new drill', :depth, 'approved',
                        :appr, :appr, :src)
                ON CONFLICT (state, permit_number) DO NOTHING
            """), {"num": num, "county": county, "op": TEST_OPERATOR,
                   "lease": lease, "profile": profile, "depth": depth,
                   "appr": date.today() - timedelta(days=off),
                   "src": TEST_SOURCE})
        await db.commit()


async def cleanup():
    async with primary_session_maker() as db:
        await db.execute(text(
            "DELETE FROM canonical.well_permits WHERE source = :src"
        ), {"src": TEST_SOURCE})
        await db.execute(text("""
            DELETE FROM alert_execution_history WHERE alert_id IN
              (SELECT id FROM permit_alerts WHERE user_id =
                (SELECT id FROM api_users WHERE email = :email))
        """), {"email": TEST_EMAIL})
        await db.execute(text("""
            DELETE FROM permit_alerts WHERE user_id =
              (SELECT id FROM api_users WHERE email = :email)
        """), {"email": TEST_EMAIL})
        await db.commit()


async def make_alert(db, filters, frequency=AlertFrequency.WEEKLY,
                     webhook=None, cursor_days_ago=1):
    uid = (await db.execute(text(
        "SELECT id FROM api_users WHERE email = :email"
    ), {"email": TEST_EMAIL})).scalar()
    alert = PermitAlert(
        user_id=uid,
        name="w1 integration test",
        source_type="well_permits",
        filters=filters,
        frequency=frequency,
        webhook_url=webhook,
        email_notify=False,
        # "advance the clock": cursor in the past so today's fixtures match
        last_checked_at=datetime.now(timezone.utc) - timedelta(days=cursor_days_ago),
    )
    db.add(alert)
    await db.commit()
    await db.refresh(alert)
    return alert


@pytest.mark.asyncio(loop_scope="module")
async def test_w1_digest_fires_with_correct_contents():
    await seed()
    try:
        async with WebhookCatcher() as catcher:
            async with primary_session_maker() as db:
                alert = await make_alert(
                    db,
                    {"county": "MIDLAND", "operator": "TESTALERT"},
                    webhook=f"http://127.0.0.1:{WEBHOOK_PORT}/hook",
                )
                n = await execute_alert(alert, db)
                assert n == 2, f"expected 2 MIDLAND matches, got {n}"

            assert len(catcher.payloads) == 1, "webhook should fire exactly once"
            payload = catcher.payloads[0]
            assert payload["source_type"] == "well_permits"
            assert payload["match_count"] == 2
            nums = {m["permit_number"] for m in payload["matches"]}
            assert nums == {"9900001", "9900002"}
            for m in payload["matches"]:
                assert m["county"] == "MIDLAND"
                assert m["operator"] == TEST_OPERATOR
                assert m["wellbore_profile"] == "horizontal"
                assert m["approved_date"] == date.today().isoformat()
    finally:
        await cleanup()


@pytest.mark.asyncio(loop_scope="module")
async def test_w1_no_match_no_webhook():
    await seed()
    try:
        async with WebhookCatcher() as catcher:
            async with primary_session_maker() as db:
                alert = await make_alert(
                    db,
                    {"county": "REAGAN", "operator": "TESTALERT"},
                    webhook=f"http://127.0.0.1:{WEBHOOK_PORT}/hook",
                )
                n = await execute_alert(alert, db)
                assert n == 0
                # cursor must still advance on zero matches
                await db.refresh(alert)
                assert alert.last_checked_at is not None
                assert (datetime.now(timezone.utc) - alert.last_checked_at).seconds < 60
            assert catcher.payloads == []
    finally:
        await cleanup()


@pytest.mark.asyncio(loop_scope="module")
async def test_w1_lease_pattern_and_depth():
    await seed()
    try:
        async with primary_session_maker() as db:
            alert = await make_alert(
                db, {"operator": "TESTALERT", "lease": "TEST RANCH",
                     "min_depth": 11500})
            matches = await match_alert(alert, db)
            assert [m["permit_number"] for m in matches] == ["9900002"]
    finally:
        await cleanup()


@pytest.mark.asyncio(loop_scope="module")
async def test_w1_no_backfill_on_activation():
    """A watchlist with no cursor (just activated) must not match history."""
    await seed()
    try:
        async with primary_session_maker() as db:
            alert = await make_alert(db, {"county": "MIDLAND"})
            alert.last_checked_at = None
            await db.commit()
            matches = await match_alert(alert, db)
            assert matches == []
    finally:
        await cleanup()


def test_w1_filter_validation():
    from fastapi import HTTPException
    from app.api.v1.alerts import _validate_source_filters

    _validate_source_filters("well_permits", {"county": "MIDLAND"})
    with pytest.raises(HTTPException):
        _validate_source_filters("well_permits", {"frack_score": 9000})
    with pytest.raises(HTTPException):
        _validate_source_filters("well_permits", {"min_depth": 5000})  # no geo/operator key
    with pytest.raises(HTTPException):
        _validate_source_filters("drilling", {"county": "MIDLAND"})
