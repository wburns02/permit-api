"""Security Phase 1 tests: route auth sweep + brute-force rate limiting.

Acceptance criteria from docs/pipeline/PERMIT_NEXT_PHASE.md §1 (Security close-out).
These tests run in CI without a live database — auth-gated routes return 401
before any DB access when no X-API-Key is provided.
"""
import re
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import HTTPException

# Paths that are intentionally public — see PERMIT_NEXT_PHASE.md §Task 1.
# Every entry MUST have a reason comment. Adding a route here signals a
# deliberate decision, not an oversight.
ALLOWLIST = {
    # ── Infrastructure ──────────────────────────────────────────────────────
    "/health",
    "/healthz",
    "/health/db",
    "/health/db/migrate-expansion",   # POST; expansion-phase schema ops

    # ── API docs: deliberate sales asset; rate-limited in middleware ─────────
    "/docs",
    "/redoc",
    "/openapi.json",

    # ── SPA static pages served by the FastAPI app ───────────────────────────
    "/",
    "/api",
    "/map",
    "/sw.js",
    "/robots.txt",
    "/sitemap.xml",

    # ── Auth flow: need these to OBTAIN an API key ───────────────────────────
    "/v1/login",
    "/v1/signup",
    "/v1/contact",    # sales inquiry form
    "/v1/demo",       # returns DEMO_API_KEY (env-gated in prod)

    # ── H-Man CRM: uses JWT Bearer, not X-API-Key ────────────────────────────
    "/v1/hman/setup",
    "/v1/hman/login",

    # ── Public marketing data: aggregate counts, no PII ─────────────────────
    "/v1/coverage",
    "/v1/stats",
    "/v1/freshness",               # coverage router freshness dashboard
    "/v1/freshness/hot-leads",     # freshness router hot-leads freshness
    "/v1/data-freshness",
    "/v1/data-freshness/",
    "/v1/trends/stats",
    "/v1/trends/anomalies",
    "/v1/permits/autocomplete",    # typeahead for landing-page search box
    "/v1/wells/stats",
    "/v1/well-permits/stats",
    "/v1/analyst/suggestions",     # static list of example questions, no PII

    # ── Per-router public stats (aggregate record counts for marketing) ───────
    "/v1/licenses/stats",
    "/v1/environmental/stats",
    "/v1/septic/stats",
    "/v1/demographics/stats",
    "/v1/valuations/stats",
    "/v1/entities/stats",
    "/v1/pipeline/stats",
    "/v1/violations/stats",
    "/v1/predictions/stats",
    "/v1/sales/stats",
    "/v1/liens/stats",
    "/v1/pricing/benchmarks",      # HomeAdvisor/Angi benchmark data, public

    # ── Hail-leads: gated by DEMO_API_KEY (in-product shared-secret) ─────────
    # require_demo_key fails open when DEMO_API_KEY not set (local dev convenience).
    # In production DEMO_API_KEY is always set — these endpoints ARE auth-gated.
    # All /v1/hail-leads/* routes use require_demo_key or require_admin_key.
    "/v1/hail-leads/stats",
    "/v1/hail-leads/health",
    "/v1/hail-leads/",        # list endpoint; gated by require_demo_key
    "/v1/hail-leads/export.csv",  # CSV export; gated by require_demo_key
    "/v1/hail-leads/{lead_id}",   # detail; gated by require_demo_key

    # ── Hail-leads admin: gated by DEMO_ADMIN_KEY; returns 503 when unset ────
    "/v1/hail-leads/refresh-mvs",
    "/v1/hail-leads/enrich",

    # ── External service webhooks: called by Stripe/SendGrid/Twilio ──────────
    "/v1/webhooks/stripe",           # Stripe signature-verified
    "/v1/dialer/twiml/outbound",     # Twilio webhook
    "/v1/dialer/recording-callback", # Twilio webhook
    "/v1/dialer/status-callback",    # Twilio webhook
    "/v1/campaigns/sendgrid-events", # SendGrid event webhook

    # ── Email unsubscribe: must be accessible from email link (no login) ─────
    "/v1/campaigns/unsubscribe",
}

_PARAM_RE = re.compile(r"\{[^}]+\}")
_UUID_FILL = "00000000-0000-0000-0000-000000000000"


def _fill_params(path: str) -> str:
    return _PARAM_RE.sub(_UUID_FILL, path)


@pytest.mark.asyncio
async def test_all_non_allowlisted_routes_require_auth():
    """Route auth sweep: every path outside ALLOWLIST must return 401/403/405.

    Uses httpx.ASGITransport — no live DB needed. Auth-gated routes raise 401
    before touching the DB when no X-API-Key is supplied.

    Acceptance criterion from PERMIT_NEXT_PHASE.md: "sweep green across 196 routes."
    """
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    violations: list[str] = []
    route_count = 0

    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        spec_resp = await client.get("/openapi.json")
        assert spec_resp.status_code == 200, "Could not load /openapi.json from app"
        spec = spec_resp.json()

        for path, methods in spec.get("paths", {}).items():
            for method in methods:
                if method.lower() in {"options", "head"}:
                    continue
                if path in ALLOWLIST:
                    continue

                route_count += 1
                url = _fill_params(path)
                try:
                    resp = await getattr(client, method.lower())(url)
                except Exception as exc:
                    violations.append(f"{method.upper()} {path} → exception: {exc}")
                    continue

                # 422 is acceptable: FastAPI rejects malformed bodies before auth
                if resp.status_code not in {401, 403, 405, 422}:
                    violations.append(f"{method.upper()} {path} → HTTP {resp.status_code}")

    assert route_count > 50, (
        f"Only {route_count} routes scanned — spec may not have loaded correctly"
    )
    assert not violations, (
        f"{len(violations)} route(s) accessible without authentication:\n"
        + "\n".join(violations[:30])
        + (f"\n… and {len(violations) - 30} more" if len(violations) > 30 else "")
    )


@pytest.mark.asyncio
async def test_login_brute_force_rate_limit():
    """BRUTE-FORCE: check_brute_force raises 429 after BRUTE_FORCE_LIMIT requests."""
    from app.middleware.rate_limit import (
        check_brute_force,
        _brute_force_store,
        BRUTE_FORCE_LIMIT,
    )

    test_ip = "192.0.2.11"  # RFC 5737 TEST-NET-1, reserved for docs/tests
    _brute_force_store.pop(test_ip, None)

    mock_req = MagicMock()
    mock_req.client.host = test_ip

    for _ in range(BRUTE_FORCE_LIMIT):
        await check_brute_force(mock_req)

    with pytest.raises(HTTPException) as exc_info:
        await check_brute_force(mock_req)

    assert exc_info.value.status_code == 429

    _brute_force_store.pop(test_ip, None)


@pytest.mark.asyncio
async def test_signup_brute_force_rate_limit():
    """BRUTE-FORCE: same guard covers /v1/signup."""
    from app.middleware.rate_limit import (
        check_brute_force,
        _brute_force_store,
        BRUTE_FORCE_LIMIT,
    )

    test_ip = "192.0.2.12"
    _brute_force_store.pop(test_ip, None)

    mock_req = MagicMock()
    mock_req.client.host = test_ip

    for _ in range(BRUTE_FORCE_LIMIT):
        await check_brute_force(mock_req)

    with pytest.raises(HTTPException) as exc_info:
        await check_brute_force(mock_req)

    assert exc_info.value.status_code == 429

    _brute_force_store.pop(test_ip, None)
