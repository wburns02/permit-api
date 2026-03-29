"""E2E tests for Actionable AI Analyst Results.

Tests both backend webhook endpoints and frontend interactive features
against the live Railway deployment.
"""

import asyncio
import json
import pytest
import httpx

# Use Railway's own domain (bypasses Cloudflare cache)
BASE_URL = "https://permit-api-production-6eae.up.railway.app"
API_URL = f"{BASE_URL}/v1"

# Will need a valid API key for authenticated endpoints
# We'll test unauthenticated behavior + the frontend HTML


class TestBackendWebhookEndpoints:
    """Test the webhook config/test/send endpoints exist and gate properly."""

    @pytest.mark.asyncio
    async def test_webhook_config_requires_auth(self):
        """PUT /v1/analyst/webhook/config requires API key."""
        async with httpx.AsyncClient() as client:
            resp = await client.put(
                f"{API_URL}/analyst/webhook/config",
                json={"webhook_url": "https://example.com"},
            )
            assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_webhook_config_get_requires_auth(self):
        """GET /v1/analyst/webhook/config requires API key."""
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{API_URL}/analyst/webhook/config")
            assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_webhook_test_requires_auth(self):
        """POST /v1/analyst/webhook/test requires API key."""
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{API_URL}/analyst/webhook/test")
            assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_webhook_send_requires_auth(self):
        """POST /v1/analyst/webhook/send requires API key."""
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{API_URL}/analyst/webhook/send",
                json={"rows": [{"test": True}]},
            )
            assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_analyst_query_endpoint_exists(self):
        """POST /v1/analyst/query exists (requires auth)."""
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{API_URL}/analyst/query",
                json={"question": "test"},
            )
            assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_analyst_suggestions_endpoint(self):
        """GET /v1/analyst/suggestions is public."""
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{API_URL}/analyst/suggestions")
            assert resp.status_code == 200
            data = resp.json()
            assert "suggestions" in data
            assert len(data["suggestions"]) > 0


class TestFrontendHTML:
    """Test that the frontend HTML contains all new interactive elements."""

    @pytest.fixture(autouse=True)
    def _load_html(self):
        """Load the frontend HTML once for all tests."""
        loop = asyncio.new_event_loop()
        self.html = loop.run_until_complete(self._fetch_html())
        loop.close()

    async def _fetch_html(self):
        async with httpx.AsyncClient() as client:
            resp = await client.get(BASE_URL)
            return resp.text

    def test_interactive_table_css(self):
        """CSS for interactive analyst table exists."""
        assert ".analyst-table" in self.html
        assert ".col-check" in self.html
        assert ".col-date" in self.html
        assert ".col-phone" in self.html
        assert ".analyst-row-active" in self.html
        assert ".analyst-row-selected" in self.html
        assert ".analyst-date-relative" in self.html

    def test_slide_out_panel_html(self):
        """Slide-out panel HTML elements exist."""
        assert 'id="analyst-panel"' in self.html
        assert 'id="analyst-panel-overlay"' in self.html
        assert 'id="analyst-panel-action"' in self.html
        assert 'id="analyst-panel-summary"' in self.html
        assert 'id="analyst-panel-intel"' in self.html
        assert "analyst-panel-header" in self.html

    def test_slide_out_panel_css(self):
        """Panel CSS with slide animation exists."""
        assert "#analyst-panel{" in self.html or "#analyst-panel {" in self.html
        assert ".analyst-call-btn" in self.html
        assert ".analyst-action-zone" in self.html
        assert ".analyst-permit-summary" in self.html
        assert ".analyst-permit-grid" in self.html
        assert ".analyst-secondary-actions" in self.html

    def test_batch_actions_bar_html(self):
        """Batch actions bar HTML exists."""
        assert 'id="analyst-batch-bar"' in self.html
        assert 'id="analyst-batch-count"' in self.html
        assert "analystSelectAll()" in self.html
        assert "analystClearSelection()" in self.html
        assert "analystBatchCsv()" in self.html
        assert "analystBatchClipboard()" in self.html
        assert "analystBatchWebhook()" in self.html
        assert "analystBatchDialer()" in self.html

    def test_batch_bar_css(self):
        """Batch bar CSS with slide-up animation exists."""
        assert "#analyst-batch-bar{" in self.html or "#analyst-batch-bar {" in self.html
        assert ".batch-count" in self.html

    def test_webhook_config_html(self):
        """Webhook config UI exists."""
        assert 'id="analyst-webhook-config"' in self.html
        assert 'id="analyst-webhook-url"' in self.html
        assert "analystSaveWebhook()" in self.html
        assert "analystTestWebhook()" in self.html

    def test_table_interaction_js(self):
        """Table interaction JS functions exist."""
        assert "function analystRowClick" in self.html
        assert "function analystToggleRow" in self.html
        assert "function analystToggleAll" in self.html
        assert "function analystUpdateBatchBar" in self.html

    def test_batch_action_js(self):
        """Batch action JS functions exist."""
        assert "function analystBatchCsv" in self.html
        assert "function analystBatchClipboard" in self.html
        assert "function analystBatchWebhook" in self.html
        assert "function analystBatchDialer" in self.html

    def test_panel_js(self):
        """Panel JS functions exist."""
        assert "function analystOpenPanel" in self.html
        assert "function analystClosePanel" in self.html
        assert "function analystPanelNav" in self.html
        assert "function _analystLoadIntel" in self.html
        assert "function _analystExtract" in self.html

    def test_webhook_config_js(self):
        """Webhook config JS functions exist."""
        assert "function analystLoadWebhookConfig" in self.html
        assert "function analystSaveWebhook" in self.html
        assert "function analystTestWebhook" in self.html

    def test_keyboard_shortcuts(self):
        """Keyboard shortcut handler for j/k/Escape exists."""
        assert "e.key === 'j'" in self.html
        assert "e.key === 'k'" in self.html

    def test_dialer_integration(self):
        """Dialer page pickup for analyst-queued leads exists."""
        assert "analyst_dialer_queue" in self.html

    def test_date_auto_detection(self):
        """Date field auto-detection array exists in table renderer."""
        assert "issue_date" in self.html
        assert "date_created" in self.html
        assert "DATE_FIELDS" in self.html

    def test_phone_detection(self):
        """Phone field detection exists in table renderer."""
        assert "PHONE_FIELDS" in self.html
        assert "contractor_phone" in self.html

    def test_column_priority(self):
        """Column priority ordering exists."""
        assert "PRIORITY" in self.html
        assert "prioritized" in self.html

    def test_broadening_suggestions(self):
        """Filter funnel suggestion chip rendering exists."""
        assert "split(/>>/)" in self.html

    def test_responsive_panel(self):
        """Mobile responsive CSS for panel exists."""
        assert "max-width:768px" in self.html

    def test_cache_control_headers(self):
        """HTML response has no-cache headers."""
        loop = asyncio.new_event_loop()
        headers = loop.run_until_complete(self._check_headers())
        loop.close()
        cache_control = headers.get("cache-control", "")
        assert "no-cache" in cache_control or "no-store" in cache_control

    async def _check_headers(self):
        async with httpx.AsyncClient() as client:
            resp = await client.get(BASE_URL)
            return dict(resp.headers)
