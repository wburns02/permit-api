# PERMIT_TO_A Progress Log

## Phase 1: Security Close-Out — COMPLETE (2026-06-15)

**Branch:** `auto/permit-to-a-2026-06-15`

### What was built

#### Task 1 — Route auth sweep (permanent CI job)

- **Script:** `scripts/security/route_auth_sweep.py` — enumerates all 196 routes from `/openapi.json`, fires each UNAUTHENTICATED, asserts 401/403/405 for all non-allowlisted paths.
- **Test:** `tests/test_security_sweep.py::test_all_non_allowlisted_routes_require_auth` — uses `httpx.ASGITransport` (no live DB needed; auth-gated routes raise 401 before DB access).
- **CI job:** `.github/workflows/ci.yml` `auth-sweep` job — runs `pytest tests/test_security_sweep.py` on every push/PR.
- **Result:** 196 routes scanned, 0 violations. All non-allowlisted routes return 401/403/405.
- **Allowlist documented:** Every allowlisted route has an inline justification comment (infrastructure, auth flow, H-Man JWT, public marketing data, Twilio/Stripe/SendGrid webhooks, email unsubscribe, hail-leads demo-key gated).

#### Task 2 — Rate limiting

- **Brute-force limit on `/v1/login` and `/v1/signup`:** `check_brute_force()` in `app/middleware/rate_limit.py` — 10 requests/60s per IP, sliding window. Uses Redis when available, in-memory fallback for single-instance deployments.
- **Dependency wired:** `Depends(check_brute_force)` added to `login()` and `signup()` in `app/api/v1/auth.py`.
- **Docs/OpenAPI rate limiting:** `docs_rate_limit_middleware` in `app/main.py` — 60 requests/minute per IP on `/docs`, `/redoc`, `/openapi.json`.
- **Tests:** `test_login_brute_force_rate_limit`, `test_signup_brute_force_rate_limit` — verify 429 after BRUTE_FORCE_LIMIT rapid calls.

#### Task 3 — Secrets audit (gitleaks)

- **Scan:** `gitleaks detect --source . --log-opts="--all"` over full 483-commit history.
- **Report:** `docs/pipeline/gitleaks-report-2026-06-15.json` (12 raw findings).
- **Config:** `.gitleaks.toml` — per-commit allowlist with disposition for each finding:
  - `sk_test_placeholder` in ci.yml → FALSE POSITIVE (placeholder string)
  - `D263FA78...` UUID in scrape_energy_environmental.py → FALSE POSITIVE (data filter value)
  - `pl_live_iQIhA0cTg50qP1nW6ITuzwz7ltHdQF4i...` in history → ROTATED (scrubbed in 3d4da48)
  - `pl_live_GyZ72kR15lL7Q3TOO9w2OLf6P9HUEQZV...` in old docs → ROTATED (historical)
  - `Kne3XYPvChciFOG9DvQ01Ukm1wyBTdTQ` OpenGov key in docs + script → **OWNER GATE**: verify key is inactive or rotate.
- **CI job:** `.github/workflows/ci.yml` `secrets-audit` job runs on every push/PR. Clean with config applied.

#### Task 4 — Dependency pinning

- **Lock file:** `requirements.lock` generated via `pip-compile --generate-hashes` from `requirements.txt`.
- **2039 lines**, hash-pinned for all transitive dependencies.
- **Note:** CI `pip install` currently uses `requirements.txt` (unpinned). Next step: switch CI to `pip install -r requirements.lock` for reproducible builds.

### Test results

```
89 passed, 445 warnings in 2.22s
```

All pre-existing tests pass. 3 new security tests added.

### Owner gate items

1. **OpenGov API key** (`Kne3XYPvChciFOG9DvQ01Ukm1wyBTdTQ`): appears in `docs/opengov-investigation.md` and `scripts/scrape_opengov.py` (commit `a99c95b1`). Verify this key is inactive/revoked. If active, rotate and redact.

2. **Switch CI to hash-pinned lock**: update `.github/workflows/ci.yml` to `pip install -r requirements.lock` instead of `requirements.txt` to enforce reproducible builds.

### Completion criteria met

- [x] Sweep green across 196 routes in CI
- [x] Gitleaks clean (with config) or findings queued to owner
- [x] Rate limits return 429 under burst (`test_login_brute_force_rate_limit`, `test_signup_brute_force_rate_limit`)
- [x] Dependency lock with hashes (`requirements.lock`)
