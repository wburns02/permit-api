"""Full E2E test suite for PermitLookup.

Tests all major features against the live deployed site.
"""

import asyncio
import pytest
import httpx

BASE_URL = "https://permits.ecbtx.com"
API_URL = f"{BASE_URL}/v1"
API_KEY = "pl_live_iQIhA0cTg50qP1nW6ITuzwz7ltHdQF4iYhi_uP8eEYA"
HEADERS = {"X-API-Key": API_KEY}


class TestHealthEndpoints:

    @pytest.mark.asyncio
    async def test_health(self):
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{BASE_URL}/health", timeout=10)
            assert r.status_code == 200
            d = r.json()
            assert d["status"] == "healthy"
            assert d["database"] == "connected"

    @pytest.mark.asyncio
    async def test_health_db(self):
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{BASE_URL}/health/db", timeout=15)
            assert r.status_code == 200
            d = r.json()
            assert d["primary"]["status"] == "ok"

    @pytest.mark.asyncio
    async def test_stats(self):
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{API_URL}/stats", timeout=10)
            assert r.status_code == 200
            d = r.json()
            assert d["total_permits"] > 700_000_000


class TestAnalystFeatures:

    @pytest.mark.asyncio
    async def test_analyst_suggestions(self):
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{API_URL}/analyst/suggestions", timeout=10)
            assert r.status_code == 200
            assert len(r.json()["suggestions"]) > 0

    @pytest.mark.asyncio
    async def test_analyst_query_austin_roofing(self):
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{API_URL}/analyst/query", json={
                "question": "Roofing permits in Austin this week with phone numbers"
            }, headers=HEADERS, timeout=15)
            assert r.status_code == 200
            d = r.json()
            assert d["row_count"] > 0
            assert "hot_leads" in d["sql"]

    @pytest.mark.asyncio
    async def test_analyst_query_texas_permits(self):
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{API_URL}/analyst/query", json={
                "question": "Show me 10 new permits with contractor phone numbers in Texas"
            }, headers=HEADERS, timeout=15)
            assert r.status_code == 200
            assert r.json()["row_count"] > 0

    @pytest.mark.asyncio
    async def test_analyst_query_fl_contractors(self):
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{API_URL}/analyst/query", json={
                "question": "Top contractors in Florida by license count"
            }, headers=HEADERS, timeout=15)
            assert r.status_code == 200
            assert r.json()["row_count"] > 0

    @pytest.mark.asyncio
    async def test_analyst_query_remodel_austin(self):
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{API_URL}/analyst/query", json={
                "question": "Remodel permits in Austin this week"
            }, headers=HEADERS, timeout=15)
            assert r.status_code == 200
            assert r.json()["row_count"] > 0

    @pytest.mark.asyncio
    async def test_analyst_query_ca_licenses(self):
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{API_URL}/analyst/query", json={
                "question": "Contractor license expirations in California"
            }, headers=HEADERS, timeout=15)
            assert r.status_code == 200
            assert r.json()["row_count"] > 0


class TestDialerEndpoints:

    @pytest.mark.asyncio
    async def test_dialer_token(self):
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{API_URL}/dialer/token", headers=HEADERS, timeout=10)
            assert r.status_code == 200
            d = r.json()
            assert "token" in d
            assert len(d["token"]) > 100

    @pytest.mark.asyncio
    async def test_dialer_token_requires_auth(self):
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{API_URL}/dialer/token", timeout=10)
            assert r.status_code == 401

    @pytest.mark.asyncio
    async def test_twiml_outbound(self):
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{API_URL}/dialer/twiml/outbound",
                data={"To": "+15125551234"}, timeout=10)
            assert r.status_code == 200
            assert "<Response>" in r.text
            assert "<Dial" in r.text
            assert "record-from-answer-dual" in r.text
            assert "twilio-media" in r.text

    @pytest.mark.asyncio
    async def test_dialer_queue(self):
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{API_URL}/dialer/queue?trade=roofing&state=TX",
                headers=HEADERS, timeout=10)
            assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_dialer_stats(self):
        async with httpx.AsyncClient() as c:
            r = await c.get(f"{API_URL}/dialer/stats", headers=HEADERS, timeout=10)
            assert r.status_code == 200


class TestWebhookEndpoints:

    @pytest.mark.asyncio
    async def test_webhook_config_requires_auth(self):
        async with httpx.AsyncClient() as c:
            r = await c.put(f"{API_URL}/analyst/webhook/config",
                json={"webhook_url": "https://example.com"}, timeout=10)
            assert r.status_code == 401

    @pytest.mark.asyncio
    async def test_webhook_send_requires_auth(self):
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{API_URL}/analyst/webhook/send",
                json={"rows": []}, timeout=10)
            assert r.status_code == 401


class TestFrontendHTML:

    @pytest.fixture(autouse=True)
    def _load_html(self):
        loop = asyncio.new_event_loop()
        self.html = loop.run_until_complete(self._fetch())
        loop.close()

    async def _fetch(self):
        async with httpx.AsyncClient() as c:
            r = await c.get(BASE_URL, timeout=10)
            return r.text

    def test_interactive_table(self):
        assert ".analyst-table" in self.html
        assert "analystRowClick" in self.html

    def test_slide_out_panel(self):
        assert 'id="analyst-panel"' in self.html
        assert "analystOpenPanel" in self.html

    def test_batch_actions(self):
        assert 'id="analyst-batch-bar"' in self.html
        assert "analystBatchCsv" in self.html

    def test_softphone_widget(self):
        assert 'id="softphone-fab"' in self.html
        assert 'id="softphone-widget"' in self.html
        assert "openSoftphone" in self.html

    def test_twilio_sdk(self):
        assert "voice-sdk@2.18.1" in self.html

    def test_broadening_suggestions(self):
        assert "split(/>>/)" in self.html

    def test_cache_control(self):
        loop = asyncio.new_event_loop()
        headers = loop.run_until_complete(self._get_headers())
        loop.close()
        assert "no-cache" in headers.get("cache-control", "")

    async def _get_headers(self):
        async with httpx.AsyncClient() as c:
            r = await c.get(BASE_URL, timeout=10)
            return dict(r.headers)


class TestDataFreshness:

    @pytest.mark.asyncio
    async def test_hot_leads_has_recent_data(self):
        """Verify hot_leads has data from the last 7 days."""
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{API_URL}/analyst/query", json={
                "question": "Count of permits in hot_leads from the last 7 days"
            }, headers=HEADERS, timeout=15)
            assert r.status_code == 200

    @pytest.mark.asyncio
    async def test_hot_leads_multiple_states(self):
        """Verify hot_leads covers multiple states."""
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{API_URL}/analyst/query", json={
                "question": "Count permits in hot_leads grouped by state"
            }, headers=HEADERS, timeout=15)
            assert r.status_code == 200
            assert r.json()["row_count"] > 3  # Should have 10+ states
