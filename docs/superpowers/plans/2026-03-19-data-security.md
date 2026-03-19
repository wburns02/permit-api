# Data Security Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Protect PermitLookup's 800M+ records from bulk scraping and data resale with 5 layers of defense.

**Architecture:** Middleware-based defense layers that wrap existing endpoints without modifying endpoint logic. Result caps and abuse detection run in middleware, fingerprinting runs as a response transform, shadow-throttle degrades responses for flagged keys. All state tracked in Redis with in-memory fallback.

**Tech Stack:** FastAPI middleware, Redis (existing), SHA-256 hashing, zero-width Unicode encoding.

**Spec:** `docs/superpowers/specs/2026-03-19-data-security-design.md`

---

### Task 1: Result Caps Middleware (Layer 1)

**Files:**
- Create: `app/middleware/result_caps.py`
- Modify: `app/config.py` — add RESULT_CAP settings
- Modify: `app/services/stripe_service.py` — add RESULT_CAP_LIMITS dict

- [ ] **Step 1: Add result cap config to app/config.py**

Add after the OVERAGE_COST_CENTS line (~line 53):

```python
# Daily result caps (total records returned per day)
RESULT_CAP_FREE: int = 500
RESULT_CAP_EXPLORER: int = 2000
RESULT_CAP_PRO_LEADS: int = 10000
RESULT_CAP_REALTIME: int = 25000
RESULT_CAP_ENTERPRISE: int = 50000
```

- [ ] **Step 2: Add result cap limits to stripe_service.py**

Add after ALERT_LIMITS dict:

```python
RESULT_CAP_LIMITS = {
    PlanTier.FREE: settings.RESULT_CAP_FREE,
    PlanTier.EXPLORER: settings.RESULT_CAP_EXPLORER,
    PlanTier.PRO_LEADS: settings.RESULT_CAP_PRO_LEADS,
    PlanTier.REALTIME: settings.RESULT_CAP_REALTIME,
    PlanTier.ENTERPRISE: settings.RESULT_CAP_ENTERPRISE,
    PlanTier.STARTER: settings.RESULT_CAP_EXPLORER,
    PlanTier.PRO: settings.RESULT_CAP_PRO_LEADS,
}

def get_result_cap(plan: PlanTier) -> int:
    resolved = resolve_plan(plan)
    return RESULT_CAP_LIMITS.get(resolved, settings.RESULT_CAP_FREE)
```

- [ ] **Step 3: Create result_caps.py middleware**

Create `app/middleware/result_caps.py`:

```python
"""Daily result cap tracking — limits total records returned per day per user."""

import logging
from datetime import datetime, timezone
from fastapi import Request
from app.config import settings
from app.models.api_key import resolve_plan
from app.services.stripe_service import get_result_cap

logger = logging.getLogger(__name__)
_memory_store: dict[str, int] = {}

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None


async def _get_redis():
    if not settings.REDIS_URL or not aioredis:
        return None
    try:
        r = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        await r.ping()
        return r
    except Exception:
        return None


async def check_result_cap(request: Request, result_count: int) -> dict:
    """
    Check and increment daily result cap. Returns cap info.
    Call AFTER query, BEFORE response. Truncates results if over cap.
    Returns {"allowed": N, "capped": bool, "used_today": N, "daily_cap": N}
    """
    user = request.state.user
    plan = resolve_plan(user.plan)
    daily_cap = get_result_cap(plan)
    user_id = str(user.id)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"resultcap:{user_id}:{today}"

    redis = await _get_redis()
    if redis:
        used = int(await redis.get(key) or 0)
        remaining = max(0, daily_cap - used)
        allowed = min(result_count, remaining)
        await redis.incrby(key, allowed)
        await redis.expire(key, 172800)
        await redis.close()
    else:
        used = _memory_store.get(key, 0)
        remaining = max(0, daily_cap - used)
        allowed = min(result_count, remaining)
        _memory_store[key] = used + allowed

    return {
        "allowed": allowed,
        "capped": allowed < result_count,
        "used_today": used + allowed,
        "daily_cap": daily_cap,
    }
```

- [ ] **Step 4: Enforce page cap in all paginated endpoints**

In every endpoint that accepts `page: int = Query(...)`, add `le=20` constraint:
```python
page: int = Query(1, ge=1, le=20),
```

Files to update: `contractors.py`, `entities.py`, `violations.py`, `sales.py`, `liens.py`, `septic.py`, `environmental.py`, `licenses.py`, `demographics.py`, `valuations.py`

Also cap `page_size` to max 50 everywhere (some have `le=100`, change to `le=50`).

- [ ] **Step 5: Verify app loads**

Run: `cd /home/will/permit-api && python3 -c "from app.main import app; print(f'OK — {len(app.routes)} routes')"`

- [ ] **Step 6: Commit**

```bash
git add app/middleware/result_caps.py app/config.py app/services/stripe_service.py app/api/v1/*.py
git commit -m "feat: add result caps — daily record limits + page 20 cap"
```

---

### Task 2: Abuse Detection Engine (Layer 3)

**Files:**
- Create: `app/services/abuse_detector.py`
- Modify: `app/middleware/rate_limit.py` — call abuse detector on each request

- [ ] **Step 1: Create abuse_detector.py**

Create `app/services/abuse_detector.py`:

The abuse detector tracks 6 signals per API key in a rolling 1-hour window using Redis (or in-memory dict). Each request updates the signals and computes a score.

Signals to track per key:
- `requests_1h`: request count in last hour
- `pages_accessed`: list of page numbers accessed (detect sequential pagination)
- `request_times`: list of timestamps (detect bot-like regularity)
- `zips_queried`: set of unique ZIPs (detect geographic sweeping)
- `states_queried`: set of unique states
- `daily_utilization`: percentage of daily rate limit used

Score computation:
- High utilization (>80% of daily limit): +10
- Sequential pages (3+ consecutive): +20
- Fast requests (<1s avg sustained 10+ requests): +25
- Geographic sweep (>50 ZIPs in 1h): +15
- State sweep (>10 states in 1h): +15
- Bot-like timing (CV < 0.1 over 10+ requests): +20

Return: `{"score": int, "signals": dict, "level": "normal"|"elevated"|"shadow"|"alert"}`

- [ ] **Step 2: Integrate abuse detector into rate_limit.py**

At the end of `check_rate_limit()`, after the rate limit check, call:
```python
from app.services.abuse_detector import record_request, get_abuse_score
await record_request(request)
```

Store the abuse score on `request.state.abuse_score` for downstream use.

- [ ] **Step 3: Verify and commit**

```bash
python3 -c "from app.services.abuse_detector import get_abuse_score; print('OK')"
git add app/services/abuse_detector.py app/middleware/rate_limit.py
git commit -m "feat: add abuse detection engine — 6 signal scoring"
```

---

### Task 3: Shadow-Throttle (Layer 4)

**Files:**
- Create: `app/services/shadow_throttle.py`

- [ ] **Step 1: Create shadow_throttle.py**

Create `app/services/shadow_throttle.py`:

Two functions:
1. `should_throttle(abuse_score: int) -> dict` — returns throttle config:
   - score 0-50: `{"active": False}`
   - score 51-70: `{"active": True, "delay_seconds": 2, "max_results": 10, "strip_fields": ["description", "contractor_name", "lat", "lng"], "force_cold": True}`
   - score 71+: `{"active": True, "delay_seconds": 5, "max_results": 10, "strip_fields": [...], "force_cold": True}`

2. `apply_throttle(results: list[dict], config: dict) -> list[dict]` — applies degradation:
   - Truncate to `max_results`
   - Remove fields in `strip_fields` from each record
   - If `force_cold`, filter out records with dates < 180 days ago

3. `async throttle_delay(config: dict)` — `await asyncio.sleep(config["delay_seconds"])` if active

- [ ] **Step 2: Commit**

```bash
git add app/services/shadow_throttle.py
git commit -m "feat: add shadow-throttle — degrade responses for flagged keys"
```

---

### Task 4: Row-Level Fingerprinting (Layer 2)

**Files:**
- Create: `app/services/fingerprint.py`
- Modify: `app/config.py` — add FINGERPRINT_SALT setting

- [ ] **Step 1: Add fingerprint salt to config**

Add to Settings class in `app/config.py`:
```python
FINGERPRINT_SALT: str = "change-this-in-production-to-random-string"
```

- [ ] **Step 2: Create fingerprint.py**

Create `app/services/fingerprint.py`:

Functions:
1. `_get_seed(api_key_id: str) -> int` — `int(sha256(key_id + salt).hexdigest()[:8], 16)`

2. `apply_fingerprint(records: list[dict], api_key_id: str) -> list[dict]` — for each record:
   - If `valuation` or `sale_price` or `amount`: round to nearest 100, add `+(seed % 99)`
   - If `lat`/`lng`: offset by `(seed % 50) * 0.00001`
   - If `description` or `address`: insert zero-width chars at positions derived from seed

3. `trace_fingerprint(record: dict) -> list[dict]` — given a suspicious record, try all known API key IDs and return matches with confidence scores. Called by admin trace endpoint.

4. `_encode_zwc(text: str, seed: int) -> str` — insert zero-width chars
5. `_decode_zwc(text: str) -> int | None` — extract seed from zero-width chars

- [ ] **Step 3: Commit**

```bash
git add app/services/fingerprint.py app/config.py
git commit -m "feat: add row-level fingerprinting — numeric perturbation + zero-width encoding"
```

---

### Task 5: Enhanced Access Logging (Layer 5)

**Files:**
- Modify: `app/main.py` — add columns to migrate-expansion
- Modify: `app/models/api_key.py` — add columns to UsageLog model

- [ ] **Step 1: Add columns to UsageLog model**

In `app/models/api_key.py`, add to UsageLog class:
```python
result_count = Column(Integer)
response_bytes = Column(Integer)
query_hash = Column(String(64))
abuse_score = Column(Integer)
```

- [ ] **Step 2: Add migration DDL to main.py**

In the `migrate_expansion` endpoint, add ALTER TABLE statements for the new columns:
```python
for col, typ in [
    ("result_count", "INTEGER"),
    ("response_bytes", "INTEGER"),
    ("query_hash", "VARCHAR(64)"),
    ("abuse_score", "INTEGER"),
]:
    try:
        await db.execute(text(f"ALTER TABLE usage_logs ADD COLUMN {col} {typ}"))
        migrations.append(f"usage_logs.{col} added")
    except Exception:
        migrations.append(f"usage_logs.{col} already exists")
        await db.rollback()
```

- [ ] **Step 3: Commit**

```bash
git add app/models/api_key.py app/main.py
git commit -m "feat: enhanced access logging — result_count, query_hash, abuse_score"
```

---

### Task 6: Admin Dashboard Extensions

**Files:**
- Modify: `app/api/v1/admin.py` — add trace endpoint + abuse alerts

- [ ] **Step 1: Add trace endpoint**

Add to `app/api/v1/admin.py`:

```python
@router.post("/trace")
async def trace_data(request: Request, record: dict, db: AsyncSession = Depends(get_db)):
    """Admin-only: trace a data record back to the API key that downloaded it."""
    from app.services.fingerprint import trace_fingerprint
    matches = await trace_fingerprint(record, db)
    return {"matches": matches}
```

- [ ] **Step 2: Add abuse alerts endpoint**

```python
@router.get("/abuse-alerts")
async def abuse_alerts(request: Request, db: AsyncSession = Depends(get_db)):
    """Admin-only: view recent abuse detection alerts."""
    from app.services.abuse_detector import get_recent_alerts
    alerts = await get_recent_alerts()
    return {"alerts": alerts}
```

- [ ] **Step 3: Commit**

```bash
git add app/api/v1/admin.py
git commit -m "feat: admin trace + abuse alerts endpoints"
```

---

### Task 7: Integration — Wire All Layers Together

**Files:**
- Modify: All paginated search endpoints to call result_caps + fingerprint + shadow_throttle

- [ ] **Step 1: Create integration helper**

Create `app/services/response_guard.py` — single function that applies all security layers:

```python
async def guard_response(request: Request, results: list[dict]) -> tuple[list[dict], dict]:
    """Apply all security layers to a response. Returns (modified_results, metadata)."""
    api_key_id = str(request.state.api_key.id)
    abuse_score = getattr(request.state, 'abuse_score', 0)

    # Layer 4: Shadow-throttle
    throttle_config = should_throttle(abuse_score)
    if throttle_config["active"]:
        await throttle_delay(throttle_config)
        results = apply_throttle(results, throttle_config)

    # Layer 1: Result caps
    cap_info = await check_result_cap(request, len(results))
    if cap_info["capped"]:
        results = results[:cap_info["allowed"]]

    # Layer 2: Fingerprint
    results = apply_fingerprint(results, api_key_id)

    return results, {"capped": cap_info["capped"], "result_cap": cap_info}
```

- [ ] **Step 2: Apply guard_response to top 5 most-used search endpoints**

Add to each endpoint, just before the return statement:
```python
from app.services.response_guard import guard_response
results, meta = await guard_response(request, results)
```

Priority endpoints: `permits/search`, `contractors/search`, `entities/search`, `violations/search`, `sales/search`

- [ ] **Step 3: Deploy + test**

```bash
git add -A
git commit -m "feat: wire all 5 security layers into search endpoints"
git push origin main
railway up --detach
```

- [ ] **Step 4: Run Playwright to verify nothing broke**

```bash
cd /home/will/CrownHardware/frontend && npx playwright test e2e/permit-data-expansion.spec.ts --reporter=list
```

Expected: All tests still pass (security layers are transparent to normal usage).

- [ ] **Step 5: Test result cap by hitting an endpoint repeatedly**

```bash
# Hit licenses search multiple times, verify cap kicks in
for i in $(seq 1 30); do
  curl -s -H "X-API-Key: pl_live_GyZ72kR15lL7Q3TOO9w2OLf6P9HUEQZVvpwWfc-jWT8" \
    "https://permits.ecbtx.com/v1/licenses/search?state=CA&page=$i" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Page {$i}: {len(d.get(\"results\",[]))} results, capped={d.get(\"capped\",False)}')"
done
```
