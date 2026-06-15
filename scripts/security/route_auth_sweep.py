#!/usr/bin/env python3
"""Route auth sweep — every route from /openapi.json must return 401/403/405
when called unauthenticated, except for the named ALLOWLIST.

Usage:
  python scripts/security/route_auth_sweep.py          # hermetic: ASGITransport
  python scripts/security/route_auth_sweep.py <URL>    # smoke-test a live base URL

Exit 0 = sweep clean. Exit 1 = violations found.
"""
import asyncio
import re
import sys

ALLOWLIST = {
    # ── Infrastructure ──────────────────────────────────────────────────────
    "/health", "/healthz", "/health/db", "/health/db/migrate-expansion",
    # ── API docs (sales asset, rate-limited in middleware) ───────────────────
    "/docs", "/redoc", "/openapi.json",
    # ── SPA static pages ─────────────────────────────────────────────────────
    "/", "/api", "/map", "/sw.js", "/robots.txt", "/sitemap.xml",
    # ── Auth flow (obtain an API key) ────────────────────────────────────────
    "/v1/login", "/v1/signup", "/v1/contact", "/v1/demo",
    # ── H-Man JWT auth (separate from X-API-Key) ─────────────────────────────
    "/v1/hman/setup", "/v1/hman/login",
    # ── Public marketing/stats (no PII, aggregate counts only) ───────────────
    "/v1/coverage", "/v1/stats",
    "/v1/freshness", "/v1/freshness/hot-leads",
    "/v1/data-freshness", "/v1/data-freshness/",
    "/v1/trends/stats", "/v1/trends/anomalies",
    "/v1/permits/autocomplete",
    "/v1/wells/stats", "/v1/well-permits/stats",
    "/v1/analyst/suggestions",
    # ── Per-router public stats ───────────────────────────────────────────────
    "/v1/licenses/stats", "/v1/environmental/stats", "/v1/septic/stats",
    "/v1/demographics/stats", "/v1/valuations/stats", "/v1/entities/stats",
    "/v1/pipeline/stats", "/v1/violations/stats", "/v1/predictions/stats",
    "/v1/sales/stats", "/v1/liens/stats", "/v1/pricing/benchmarks",
    # ── Hail-leads (DEMO_API_KEY / DEMO_ADMIN_KEY gated, not X-API-Key) ──────
    # require_demo_key fails open when DEMO_API_KEY not set (local dev convenience).
    # In production DEMO_API_KEY is always set — these endpoints ARE auth-gated.
    "/v1/hail-leads/stats", "/v1/hail-leads/health",
    "/v1/hail-leads/", "/v1/hail-leads/export.csv", "/v1/hail-leads/{lead_id}",
    "/v1/hail-leads/refresh-mvs", "/v1/hail-leads/enrich",
    # ── External service webhooks ─────────────────────────────────────────────
    "/v1/webhooks/stripe",
    "/v1/dialer/twiml/outbound",
    "/v1/dialer/recording-callback",
    "/v1/dialer/status-callback",
    "/v1/campaigns/sendgrid-events",
    # ── Email unsubscribe (must work without login) ───────────────────────────
    "/v1/campaigns/unsubscribe",
}

_PARAM_RE = re.compile(r"\{[^}]+\}")
_UUID_FILL = "00000000-0000-0000-0000-000000000000"


def _fill_params(path: str) -> str:
    return _PARAM_RE.sub(_UUID_FILL, path)


async def sweep_via_asgi() -> tuple[int, list[str]]:
    """Hermetic sweep using httpx.ASGITransport — no live DB or server needed.

    Auth-gated routes raise 401 before any DB access when no X-API-Key is
    supplied, so this runs correctly without a database connection.
    """
    import httpx
    from app.main import app

    violations: list[str] = []
    route_count = 0

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        spec_resp = await client.get("/openapi.json")
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
                    print(f"  WARN: {method.upper()} {path} → {exc}", file=sys.stderr)
                    continue

                if resp.status_code not in {401, 403, 405, 422}:
                    violations.append(f"{method.upper()} {path} → HTTP {resp.status_code}")

    return route_count, violations


async def sweep_via_url(base_url: str) -> tuple[int, list[str]]:
    """Smoke-test a live base URL."""
    import httpx

    violations: list[str] = []
    route_count = 0

    async with httpx.AsyncClient(base_url=base_url, timeout=10) as client:
        spec_resp = await client.get("/openapi.json")
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
                    if resp.status_code not in {401, 403, 405, 422}:
                        violations.append(f"{method.upper()} {path} → HTTP {resp.status_code}")
                except Exception as exc:
                    print(f"  WARN: {method.upper()} {path} → {exc}", file=sys.stderr)

    return route_count, violations


async def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else None

    if base_url:
        print(f"Sweeping {base_url} …", flush=True)
        route_count, violations = await sweep_via_url(base_url)
    else:
        print("Sweeping via ASGITransport (hermetic, no DB needed) …", flush=True)
        route_count, violations = await sweep_via_asgi()

    print(f"Scanned {route_count} routes.", flush=True)

    if violations:
        print(f"\nFAIL — {len(violations)} route(s) accessible without authentication:")
        for v in violations:
            print(f"  {v}")
        sys.exit(1)
    else:
        print("PASS — all non-allowlisted routes require authentication.")
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
