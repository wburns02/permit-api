# PermitLookup Demo-Ready & Sales-Ready Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take PermitLookup from broken prototype to demo-ready product with working sales infrastructure, targeting first 20+ paying customers in 90 days.

**Architecture:** Three parallel workstreams — (1) Demo-Ready: hide broken nav, fix endpoints, seed data, polish flow; (2) Infrastructure: systemd services, Cloudflare Tunnel, deployment; (3) Sales-Ready: pricing simplification, email list, cold email, onboarding. All changes target the permit-api repo on R730-2 (100.87.214.106).

**Tech Stack:** FastAPI + Python 3.12, PostgreSQL 16, vanilla JS SPA (single index.html), Cloudflare Tunnel, systemd, SendGrid, Stripe, Instantly.ai

**Spec:** `docs/superpowers/specs/2026-03-26-demo-and-sales-ready-design.md`

---

## File Structure

### Files to Modify
- `app/static/index.html` (15,493 lines) — Nav cleanup, demo button on home, guided toast, error handling, pricing page, 960M→835M fix
- `app/api/v1/contractors.py:27` — Make `name` param optional
- `app/api/v1/coverage.py:33` — Fix coverage to work when jurisdictions table is empty
- `app/api/v1/analyst.py` — Verify parameter name, check API key
- `app/api/v1/auth.py:260-269` — Demo key from env var, rate limit
- `app/models/api_key.py:16-32` — Add DEMO tier, update PLAN_MIGRATION
- `app/services/stripe_service.py:11-31` — Update price map and limits
- `app/config.py:48-69` — Update rate limit defaults
- `app/main.py:105-118` — CORS origins

### Files to Create
- `backend/scripts/seed_demo_data.py` — Seed CRM contacts, deals, dialer queue
- `/etc/systemd/system/postgresql-permitlookup.service` — PostgreSQL auto-start
- `/etc/systemd/system/permitlookup-api.service` — API auto-start
- `backend/scripts/find_insurance_agents.py` — Locate and load insurance agent data

### Files on R730-2 (via SSH)
- `/home/will/permit-api/.env` — Add ANTHROPIC_API_KEY, DEMO_API_KEY
- `/home/will/permit-api/app/static/sw.js` — Cache bust on each deploy

---

## WORKSTREAM 1: DEMO-READY (Days 1-3)

### Task 1: Hide Broken Nav Items

**Files:**
- Modify: `app/static/index.html:1205-1238`

- [ ] **Step 1: Read the nav section**

Read lines 1205-1238 of `app/static/index.html` to see the current nav.

- [ ] **Step 2: Comment out broken nav items**

Comment out these lines (keep Search, Contractors, AI Analyst, Pricing):
- Line 1207: Alerts (`showPage('alerts')`)
- Line 1208: Leads (`showPage('leads')`)
- Line 1209: Dialer (`showPage('dialer')`)
- Line 1210: CRM (`showPage('crm')`)
- Line 1211: Playground (`showPage('playground')`)

- [ ] **Step 3: Trim Tools dropdown to working items only**

In the Tools dropdown (lines 1219-1236), keep ONLY:
- Line 1219: Risk Score (`showPage('risk')`)
- Line 1226: Reports (`showPage('report')`) — rename display text to "Property Report"
- Line 1228: Coverage (`showPage('coverage')`)
- Line 1235: Batch Lookup (`showPage('batch')`)

Comment out all others: Properties, Trends, Market, Activity Feed, Estimator, Requirements, Watchlist, Owner Intelligence, Value Analytics, Property Passport, Market Momentum, Property Compare, Jurisdiction Intel.

Use `<!-- HIDDEN: re-add when ready -->` comment syntax.

- [ ] **Step 4: Verify nav renders with only working items**

Deploy to R730-2:
```bash
cd /home/will/permit-api && git add app/static/index.html && git commit -m "feat: hide broken nav items — show only working pages"
ssh will@100.87.214.106 "cd /home/will/permit-api && git pull origin main"
```

Test: `curl -s https://r730-2-1.tailad2d5f.ts.net/ | grep -c 'showPage'` — should show ~10 (not 30+).

---

### Task 2: Fix Contractors Search — Make Name Optional

**Files:**
- Modify: `app/api/v1/contractors.py:27`

- [ ] **Step 1: Read contractors.py search endpoint**

Read the function signature at line 27.

- [ ] **Step 2: Make name parameter optional**

Change line 27 from:
```python
name: str = Query(..., min_length=2, description="Contractor name or company"),
```
To:
```python
name: str | None = Query(None, min_length=2, description="Contractor name or company"),
```

- [ ] **Step 3: Update the query logic to handle no name**

Find where `name` is used in the SQL query building. Add a condition: if `name` is None and no other filter is provided, return an error. If `name` is None but `state` or `city` is provided, search by those filters only.

- [ ] **Step 4: Test**

```bash
curl -s -H "X-API-Key: pl_live_iQIhA0cTg50qP1nW6ITuzwz7ltHdQF4iYhi_uP8eEYA" \
  "https://r730-2-1.tailad2d5f.ts.net/v1/contractors/search?state=TX" | head -200
```

Expected: Returns contractor results for TX (not a 500 error).

- [ ] **Step 5: Commit**

```bash
git add app/api/v1/contractors.py
git commit -m "fix: make contractor name optional — allow search by state/city alone"
```

---

### Task 3: Fix Coverage Endpoint

**Files:**
- Modify: `app/api/v1/coverage.py:33-38`

- [ ] **Step 1: Read coverage.py**

Read the full file to understand the current query.

- [ ] **Step 2: Add fallback when jurisdictions table is empty**

The coverage endpoint queries the `jurisdictions` table. On R730-2 replica, this table may be empty. Add a fallback that queries the `permits` table partitions directly:

```python
# If jurisdictions table is empty, fall back to permit partition stats
if not state_rows:
    state_rows = await safe_query(db,
        text("SELECT state_code AS state, COUNT(*) AS count FROM permits GROUP BY state_code ORDER BY count DESC"),
        timeout_ms=10000
    )
```

Or better: use the partitioned table metadata for instant results:
```python
if not state_rows:
    result = await db.execute(text(
        "SELECT replace(inhrelid::regclass::text, 'permits_', '') AS state, "
        "pg_stat_get_live_tuples(inhrelid) AS count "
        "FROM pg_inherits WHERE inhparent = 'permits'::regclass "
        "ORDER BY count DESC"
    ))
    state_rows = result.all()
```

- [ ] **Step 3: Test**

```bash
curl -s "https://r730-2-1.tailad2d5f.ts.net/v1/coverage" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'States: {d[\"total_states\"]}, Jurisdictions: {d[\"total_jurisdictions\"]}')"
```

Expected: `States: 50+, Jurisdictions: 3000+` (not `States: 0`).

- [ ] **Step 4: Commit**

```bash
git add app/api/v1/coverage.py
git commit -m "fix: coverage endpoint — fallback to partition stats when jurisdictions empty"
```

---

### Task 4: Add ANTHROPIC_API_KEY to R730-2

**Files:**
- Modify (on R730-2): `/home/will/permit-api/.env`

- [ ] **Step 1: Check if key exists in config.py**

Read `app/config.py` and search for `ANTHROPIC_API_KEY` to confirm the setting name.

- [ ] **Step 2: Get the API key**

Check if the key exists in the local permit-api repo or the user's environment:
```bash
grep ANTHROPIC /home/will/permit-api/.env 2>/dev/null
grep ANTHROPIC /home/will/.bashrc /home/will/.env 2>/dev/null
```

If not found locally, check if it's set on T430 or the user's main machine.

- [ ] **Step 3: Add to R730-2 .env**

```bash
ssh will@100.87.214.106 "echo 'ANTHROPIC_API_KEY=sk-ant-...' >> /home/will/permit-api/.env"
```

Also add the demo key as env var:
```bash
ssh will@100.87.214.106 "echo 'DEMO_API_KEY=pl_live_iQIhA0cTg50qP1nW6ITuzwz7ltHdQF4iYhi_uP8eEYA' >> /home/will/permit-api/.env"
```

- [ ] **Step 4: Test AI Analyst**

```bash
curl -s -X POST -H "X-API-Key: pl_live_iQIhA0cTg50qP1nW6ITuzwz7ltHdQF4iYhi_uP8eEYA" \
  -H "Content-Type: application/json" \
  -d '{"question": "How many permits were filed in Austin TX this month?"}' \
  "https://r730-2-1.tailad2d5f.ts.net/v1/analyst/query"
```

Expected: JSON response with AI-generated SQL + results (not 500 error).

---

### Task 5: Add Demo Button to Home Page

**Files:**
- Modify: `app/static/index.html`

- [ ] **Step 1: Find the search section on home page**

The search box is around line 1280-1284. Find the exact location.

- [ ] **Step 2: Add Try Demo button below search**

After the search box `</div>` and before Advanced Filters, add:

```html
<div style="text-align:center;margin-top:16px" id="demo-cta" class="demo-only-hide">
  <button onclick="demoLogin()" style="padding:10px 24px;background:linear-gradient(135deg,#7c3aed,#6366f1);color:#fff;border:none;border-radius:8px;font-size:15px;font-weight:600;cursor:pointer;transition:all .2s;box-shadow:0 2px 8px rgba(124,58,237,.3)">
    Try Demo (Free) — No account needed
  </button>
</div>
```

Add CSS/JS to hide this button when already logged in (check `currentKey`).

- [ ] **Step 3: Add guided toast after demo login**

Find the `demoLogin()` function (around line 6036). After the key is stored and nav updated, add:

```javascript
showToast('Welcome! Try searching "Austin TX" or click AI Analyst to ask a question.', 'success', 8000);
```

- [ ] **Step 4: Commit**

```bash
git add app/static/index.html
git commit -m "feat: add Try Demo button on home page + guided toast"
```

---

### Task 6: Fix Error Handling — No More "Internal Server Error"

**Files:**
- Modify: `app/static/index.html`

- [ ] **Step 1: Find the doSearch function**

Search for `function doSearch` in index.html.

- [ ] **Step 2: Add error handling to search**

In the `doSearch` function, find the `fetch` call. Wrap the response handling:

```javascript
if (!r.ok) {
  const errData = await r.json().catch(() => null);
  const msg = errData?.detail || 'Search failed. Try a more specific query.';
  document.getElementById('results-list').innerHTML =
    '<div style="text-align:center;padding:40px;color:var(--text3)">' + msg + '</div>';
  document.getElementById('results-area').classList.add('visible');
  return;
}
```

- [ ] **Step 3: Add global fetch error handler**

Find any other `fetch` calls throughout the JS that don't handle errors. Add try/catch wrappers to the most visible ones: `loadStats()`, `loadAnomalies()`, contractor search, coverage.

- [ ] **Step 4: Commit**

```bash
git add app/static/index.html
git commit -m "fix: friendly error messages — no more Internal Server Error"
```

---

### Task 7: Fix 960M → 835M Everywhere

**Files:**
- Modify: `app/static/index.html`

- [ ] **Step 1: Find all occurrences of 960**

```bash
grep -n '960' app/static/index.html | head -20
```

- [ ] **Step 2: Replace 960M+ with 835M+ throughout**

Replace all static references to "960M+" with "835M+" in the HTML. The `loadStats()` function will overwrite the hero stat with the live number from the API, but static text in descriptions, pricing cards, etc. needs to match.

Use find-and-replace: `960M+` → `835M+`

- [ ] **Step 3: Commit**

```bash
git add app/static/index.html
git commit -m "fix: update record count from 960M to 835M to match actual data"
```

---

### Task 8: Seed Demo Data (CRM + Dialer)

**Files:**
- Create: `backend/scripts/seed_demo_data.py`

- [ ] **Step 1: Read the CRM model to understand schema**

Read `app/models/` to find CRM contact, deal, and dialer models. Note table names, required fields, and UUID primary keys.

- [ ] **Step 2: Write seed script**

Create `backend/scripts/seed_demo_data.py`:

```python
"""Seed demo data for CRM contacts, deals, and dialer queue.
Idempotent — checks before inserting. Run on T430 primary."""

import asyncio
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from sqlalchemy import text
from app.database import primary_session_maker

DEMO_CONTACTS = [
    {"name": "Mike Rodriguez", "company": "Rodriguez Roofing", "email": "mike@rodriguezroofing.com", "phone": "512-555-0142", "type": "contractor"},
    {"name": "Sarah Chen", "company": "Lone Star Insurance", "email": "schen@lonestarins.com", "phone": "512-555-0187", "type": "insurance_agent"},
    # ... 13 more realistic contacts
]

DEMO_DEALS = [
    {"contact_name": "Mike Rodriguez", "value": 12500, "stage": "won", "description": "Q1 Roofing leads package"},
    {"contact_name": "Sarah Chen", "value": 5925, "stage": "proposal", "description": "Annual permit data subscription"},
    # ... 3 more deals at various stages
]

async def seed():
    async with primary_session_maker() as db:
        # Check if demo data already exists
        result = await db.execute(text("SELECT COUNT(*) FROM crm_contacts WHERE email LIKE '%@rodriguezroofing.com'"))
        if result.scalar() > 0:
            print("Demo data already exists, skipping.")
            return

        # Insert contacts, deals, dialer leads
        # ... (full implementation)

        await db.commit()
        print("Seeded 15 contacts, 5 deals, 20 dialer leads.")

if __name__ == "__main__":
    asyncio.run(seed())
```

- [ ] **Step 3: Run seed script on T430**

```bash
ssh will@100.87.214.106 "cd /home/will/permit-api && python3 backend/scripts/seed_demo_data.py"
```

- [ ] **Step 4: Verify**

```bash
curl -s -H "X-API-Key: pl_live_iQIhA0cTg50qP1nW6ITuzwz7ltHdQF4iYhi_uP8eEYA" \
  "https://r730-2-1.tailad2d5f.ts.net/v1/crm/contacts" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Contacts: {len(d)}')"
```

Expected: `Contacts: 15`

- [ ] **Step 5: Commit**

```bash
git add backend/scripts/seed_demo_data.py
git commit -m "feat: add seed script for demo CRM contacts, deals, and dialer leads"
```

---

## WORKSTREAM 2: INFRASTRUCTURE (Days 4-7)

### Task 9: Create Systemd Services

**Files:**
- Create (on R730-2): `/etc/systemd/system/postgresql-permitlookup.service`
- Create (on R730-2): `/etc/systemd/system/permitlookup-api.service`

- [ ] **Step 1: Create PostgreSQL service**

```bash
ssh will@100.87.214.106 "sudo tee /etc/systemd/system/postgresql-permitlookup.service << 'EOF'
[Unit]
Description=PostgreSQL for PermitLookup
After=network.target local-fs.target

[Service]
Type=forking
User=postgres
ExecStart=/usr/bin/pg_ctl start -D /mnt/ssd/pgdata -l /mnt/ssd/pgdata/logfile
ExecStop=/usr/bin/pg_ctl stop -D /mnt/ssd/pgdata -m fast
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF"
```

- [ ] **Step 2: Create API service**

```bash
ssh will@100.87.214.106 "sudo tee /etc/systemd/system/permitlookup-api.service << 'EOF'
[Unit]
Description=PermitLookup API
After=postgresql-permitlookup.service
Requires=postgresql-permitlookup.service

[Service]
Type=simple
User=will
WorkingDirectory=/home/will/permit-api
EnvironmentFile=/home/will/permit-api/.env
ExecStart=/usr/bin/python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 4
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF"
```

- [ ] **Step 3: Disable conflicting system PostgreSQL**

```bash
ssh will@100.87.214.106 "sudo systemctl disable postgresql 2>/dev/null; sudo systemctl stop postgresql 2>/dev/null"
```

- [ ] **Step 4: Enable and start new services**

```bash
ssh will@100.87.214.106 "sudo systemctl daemon-reload && \
  sudo systemctl enable postgresql-permitlookup && \
  sudo systemctl enable permitlookup-api && \
  sudo systemctl start postgresql-permitlookup && \
  sleep 3 && \
  sudo systemctl start permitlookup-api && \
  sleep 5 && \
  curl -s http://localhost:8080/v1/stats"
```

Expected: Stats JSON response.

- [ ] **Step 5: Test reboot recovery**

```bash
ssh will@100.87.214.106 "sudo reboot"
# Wait 2 minutes
curl -s https://r730-2-1.tailad2d5f.ts.net/v1/stats
```

Expected: Stats respond after reboot without manual intervention.

---

### Task 10: Set Up Cloudflare Tunnel

**Prerequisite:** User must create Cloudflare account and add ecbtx.com (change GoDaddy nameservers).

**Files:**
- Install on R730-2: `cloudflared`
- Create on R730-2: `/etc/cloudflared/config.yml`

- [ ] **Step 1: Install cloudflared**

```bash
ssh will@100.87.214.106 "sudo dnf install -y cloudflared 2>/dev/null || \
  curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-x86_64.rpm -o /tmp/cloudflared.rpm && \
  sudo rpm -i /tmp/cloudflared.rpm"
```

- [ ] **Step 2: Authenticate with Cloudflare**

This requires user interaction — run in interactive terminal:
```bash
ssh will@100.87.214.106 "cloudflared tunnel login"
```
Opens a browser URL for Cloudflare auth. User must click authorize.

- [ ] **Step 3: Create tunnel**

```bash
ssh will@100.87.214.106 "cloudflared tunnel create permitlookup"
```

Note the tunnel UUID from output.

- [ ] **Step 4: Configure tunnel**

```bash
ssh will@100.87.214.106 "sudo mkdir -p /etc/cloudflared && sudo tee /etc/cloudflared/config.yml << EOF
tunnel: <TUNNEL_UUID>
credentials-file: /home/will/.cloudflared/<TUNNEL_UUID>.json

ingress:
  - hostname: permits.ecbtx.com
    service: http://localhost:8080
  - service: http_status:404
EOF"
```

- [ ] **Step 5: Add DNS record in Cloudflare**

```bash
ssh will@100.87.214.106 "cloudflared tunnel route dns permitlookup permits.ecbtx.com"
```

- [ ] **Step 6: Install as systemd service**

```bash
ssh will@100.87.214.106 "sudo cloudflared service install && sudo systemctl enable cloudflared && sudo systemctl start cloudflared"
```

- [ ] **Step 7: Test**

```bash
curl -s https://permits.ecbtx.com/v1/stats
```

Expected: Stats JSON from R730-2 via Cloudflare Tunnel.

- [ ] **Step 8: Revert frontend API base to relative**

In `app/static/index.html`, ensure:
```javascript
const API = '';
```

This makes the frontend work from any domain (permits.ecbtx.com or the Tailscale URL).

---

### Task 11: Add UptimeRobot Monitoring

- [ ] **Step 1: Create free UptimeRobot account**

Go to uptimerobot.com, create free account.

- [ ] **Step 2: Add monitors**

Add HTTP monitors for:
- `https://permits.ecbtx.com/v1/stats` (5-min interval)
- `https://r730-2-1.tailad2d5f.ts.net/v1/stats` (5-min interval, backup)

Set alert email to willwalterburns@gmail.com.

---

## WORKSTREAM 3: SALES-READY (Days 7-30)

### Task 12: Simplify Pricing — Backend

**Files:**
- Modify: `app/models/api_key.py:16-32`
- Modify: `app/services/stripe_service.py:11-31`
- Modify: `app/config.py:48-69`

- [ ] **Step 1: Update PlanTier enum**

In `app/models/api_key.py`, update the PLAN_MIGRATION dict to map old tiers to new names:

```python
PLAN_MIGRATION: dict[str, "PlanTier"] = {
    "starter": PlanTier.EXPLORER,    # → Pro ($79)
    "pro": PlanTier.PRO_LEADS,       # → Business ($249)
    "realtime": PlanTier.PRO_LEADS,  # → Business ($249) — downgrade price, keep access
}
```

Note: We keep the existing enum values (EXPLORER, PRO_LEADS, ENTERPRISE) but display them as "Pro", "Business", "Enterprise" on the frontend. No database migration needed since we're just changing display names.

- [ ] **Step 2: Add DEMO plan handling**

In `app/models/api_key.py`, add to PLAN_MIGRATION:
```python
"demo": PlanTier.ENTERPRISE,  # Demo gets enterprise access but limited rate
```

- [ ] **Step 3: Update rate limits in config.py**

```python
RATE_LIMIT_FREE: int = 10
RATE_LIMIT_EXPLORER: int = 50       # "Pro" tier
RATE_LIMIT_PRO_LEADS: int = 250     # "Business" tier (was 150)
RATE_LIMIT_REALTIME: int = 500      # Unused, maps to Business
RATE_LIMIT_ENTERPRISE: int = 2000
RATE_LIMIT_DEMO: int = 25           # Demo key limit
```

- [ ] **Step 4: Commit**

```bash
git add app/models/api_key.py app/services/stripe_service.py app/config.py
git commit -m "feat: simplify pricing — Pro ($79), Business ($249), Enterprise (custom)"
```

---

### Task 13: Simplify Pricing — Frontend

**Files:**
- Modify: `app/static/index.html` (pricing section)

- [ ] **Step 1: Find pricing section**

Search for the pricing page in index.html (around line 1758+).

- [ ] **Step 2: Replace 6-tier pricing with 4 tiers**

Replace the pricing cards with:
- **Free** — $0, 10 lookups/day, cold data only. CTA: "Start Free"
- **Pro** — $79/mo, 50 lookups/day, 90-day data. CTA: Stripe checkout link
- **Business** — $249/mo, 250 lookups/day, all data + AI Analyst. CTA: Stripe checkout link (badge: "Most Popular")
- **Enterprise** — Custom pricing, unlimited, white-label API. CTA: "Contact Sales"

- [ ] **Step 3: Update Stripe checkout links**

Replace old Stripe price IDs with new ones. If new Stripe products haven't been created yet, use placeholder links that go to the contact form.

- [ ] **Step 4: Commit**

```bash
git add app/static/index.html
git commit -m "feat: simplified pricing page — 4 tiers instead of 6"
```

---

### Task 14: Locate Insurance Agent Email List

**Files:**
- Create: `backend/scripts/find_insurance_agents.py`

The exploration found 0 insurance agents in the `professional_licenses` table. The 224K contacts were claimed in strategy docs but may be on T430, in CSV files, or not yet scraped.

- [ ] **Step 1: Search T430 for insurance data**

```bash
ssh will@100.87.214.106 "psql -U will -h 192.168.7.83 permits -c \"SELECT tablename FROM pg_tables WHERE tablename LIKE '%insurance%' OR tablename LIKE '%agent%' OR tablename LIKE '%license%' ORDER BY tablename;\""
```

Also check:
```bash
ssh will@100.87.214.106 "psql -U will -h 192.168.7.83 permits -c \"SELECT source, COUNT(*) FROM professional_licenses GROUP BY source ORDER BY count DESC LIMIT 30;\""
```

- [ ] **Step 2: Search for CSV files on R730**

```bash
ssh will@100.82.237.57 "find /home/will -name '*insurance*' -o -name '*agent*' -o -name '*license*' 2>/dev/null | head -20"
ssh will@100.82.237.57 "find /mnt -name '*insurance*' -o -name '*agent*' 2>/dev/null | head -20"
```

- [ ] **Step 3: Check scraper scripts for insurance agent sources**

```bash
grep -rl 'insurance\|agent.*license' /home/will/permit-api/backend/scripts/ | head -10
```

Also check R730 scraper scripts:
```bash
ssh will@100.82.237.57 "grep -rl 'insurance' /home/will/scrape_*.py /home/will/run_*.py 2>/dev/null | head -10"
```

- [ ] **Step 4: If data exists — load it; if not — build scraper**

If found: write a script to load into `professional_licenses` table with `source = 'insurance_agents_[state]'`.

If not found: write scrapers for state insurance commissioner databases. Start with TX (TDI), FL (FLDFS), CA (CDI) — these are public databases with agent search APIs.

- [ ] **Step 5: Verify count**

```bash
ssh will@100.87.214.106 "psql -U will -h 192.168.7.83 permits -c \"SELECT source, COUNT(*) FROM professional_licenses WHERE source LIKE '%insurance%' GROUP BY source;\""
```

Target: 5,000+ TX agents minimum for first email campaign.

---

### Task 15: Demo Key Security

**Files:**
- Modify: `app/api/v1/auth.py:260-269`
- Modify: `app/config.py`

- [ ] **Step 1: Move demo key to environment variable**

In `app/config.py`, add:
```python
DEMO_API_KEY: str = "pl_live_iQIhA0cTg50qP1nW6ITuzwz7ltHdQF4iYhi_uP8eEYA"
```

In `app/api/v1/auth.py`, replace the hardcoded key:
```python
from app.config import settings

@router.post("/demo")
async def demo_login():
    return {
        "api_key": settings.DEMO_API_KEY,
        "email": "demo@permitlookup.com",
        "company_name": "PermitLookup Demo",
        "plan": "enterprise",
        "message": "Demo account ready. Full Enterprise access for exploration.",
    }
```

- [ ] **Step 2: Rate-limit demo key to 25 lookups/day**

In the rate limit middleware (`app/middleware/rate_limit.py`), add a check:
```python
if request.state.api_key and request.state.api_key.key_prefix == settings.DEMO_API_KEY[:12]:
    daily_limit = 25  # Demo key gets limited access
```

- [ ] **Step 3: Commit**

```bash
git add app/api/v1/auth.py app/config.py app/middleware/rate_limit.py
git commit -m "security: move demo key to env var, rate-limit to 25/day"
```

---

### Task 16: Signup & Onboarding Welcome Page

**Files:**
- Modify: `app/static/index.html`

- [ ] **Step 1: Add welcome page section**

After signup succeeds, instead of just showing the API key, redirect to a welcome page section:

```html
<div id="page-welcome" class="page" style="display:none">
  <div class="container" style="max-width:700px;margin:60px auto;text-align:center">
    <h1 style="font-size:32px;margin-bottom:16px">Welcome to PermitLookup!</h1>
    <p style="color:var(--text2);font-size:16px;margin-bottom:40px">Your account is ready. Here are 3 things to try:</p>

    <div style="display:grid;gap:20px;text-align:left">
      <div onclick="showPage('home')" style="padding:20px;background:var(--bg2);border-radius:12px;border:1px solid var(--border);cursor:pointer">
        <h3>1. Search Permits in Your Area</h3>
        <p style="color:var(--text3);margin-top:8px">Try searching "Austin TX" or your city to see recent building permits.</p>
      </div>
      <div onclick="showPage('analyst')" style="padding:20px;background:var(--bg2);border-radius:12px;border:1px solid var(--border);cursor:pointer">
        <h3>2. Ask the AI Analyst</h3>
        <p style="color:var(--text3);margin-top:8px">Ask natural language questions like "Show me roofing permits in Dallas over $50K"</p>
      </div>
      <div onclick="showPage('risk')" style="padding:20px;background:var(--bg2);border-radius:12px;border:1px solid var(--border);cursor:pointer">
        <h3>3. Check a Property's Risk Score</h3>
        <p style="color:var(--text3);margin-top:8px">Enter any address to see permit history, compliance risk, and construction signals.</p>
      </div>
    </div>
  </div>
</div>
```

- [ ] **Step 2: Redirect after signup/demo to welcome page**

In the signup success handler and demoLogin function, add:
```javascript
showPage('welcome');
```

- [ ] **Step 3: Commit**

```bash
git add app/static/index.html
git commit -m "feat: add welcome page for new signups with guided next steps"
```

---

### Task 17: Set Up Cold Email Infrastructure

This task is manual/operational — no code changes.

- [ ] **Step 1: Register 3 sending domains**

Purchase from Namecheap or GoDaddy (~$10 each):
- `permitdata.io`
- `buildingpermits.co`
- `propertyalerts.io`

(Or similar available domains)

- [ ] **Step 2: Set up DNS records for each domain**

For each domain, add:
- SPF: `v=spf1 include:_spf.instantly.ai ~all`
- DKIM: (provided by Instantly.ai during setup)
- DMARC: `v=DMARC1; p=none; rua=mailto:will@permitdata.io`

- [ ] **Step 3: Sign up for Instantly.ai ($97/mo)**

Create account, connect the 3 sending domains, start warmup.

- [ ] **Step 4: Write email sequences**

Create 3-email sequence in Instantly.ai:

**Email 1 (Day 0):**
Subject: `{{county_permit_count}} new building permits filed in {{county}} this week`
Body: Quick stat + CTA to free search.

**Email 2 (Day 3):**
Subject: `Re: {{county}} permits`
Body: Specific example address + permit detail. CTA: free 7-day trial.

**Email 3 (Day 7):**
Subject: `Quick question`
Body: Soft ask for 10-min demo.

- [ ] **Step 5: Wait 14 days for domain warmup**

Do NOT send real campaigns until warmup is complete (Day 21 minimum).

---

### Task 18: Build County Weekly Stats for Email Merge

**Files:**
- Create: `backend/scripts/county_weekly_stats.py`

The email subject lines need real data: "7 new building permits filed in [COUNTY] this week."

- [ ] **Step 1: Write stats query script**

```python
"""Generate county-level weekly permit stats for email merge fields."""

import asyncio
from sqlalchemy import text
from app.database import replica_session_maker

async def generate_stats():
    async with replica_session_maker() as db:
        result = await db.execute(text("""
            SELECT county, state_code, COUNT(*) as permit_count
            FROM permits
            WHERE date_created > NOW() - INTERVAL '7 days'
            AND county IS NOT NULL
            GROUP BY county, state_code
            ORDER BY permit_count DESC
        """))
        rows = result.all()
        # Export as CSV for Instantly.ai merge
        with open('/tmp/county_weekly_stats.csv', 'w') as f:
            f.write('county,state,permit_count\n')
            for r in rows:
                f.write(f'{r[0]},{r[1]},{r[2]}\n')
        print(f"Generated stats for {len(rows)} counties")

if __name__ == "__main__":
    asyncio.run(generate_stats())
```

- [ ] **Step 2: Set up as weekly cron**

```bash
ssh will@100.87.214.106 "echo '0 6 * * 1 cd /home/will/permit-api && python3 backend/scripts/county_weekly_stats.py' | crontab -"
```

- [ ] **Step 3: Commit**

```bash
git add backend/scripts/county_weekly_stats.py
git commit -m "feat: county weekly stats generator for email merge fields"
```

---

### Task 19: Convert Pilot Roofer to Paying Customer

This is an operational task — no code changes.

- [ ] **Step 1: Call the pilot roofer**

Ask for product feedback. What does he use? What's missing? What would make him pay?

- [ ] **Step 2: Offer discounted rate**

$49/mo for 3 months (vs $79 standard Pro). In exchange: 2-minute Loom testimonial.

- [ ] **Step 3: If yes — create Stripe subscription manually**

In Stripe dashboard: create customer, attach payment method, create subscription for Pro ($49/mo custom price for 3 months).

- [ ] **Step 4: Add testimonial to homepage**

Edit `app/static/index.html`, add below the hero section:

```html
<div style="text-align:center;padding:40px 20px;max-width:600px;margin:0 auto">
  <p style="font-size:16px;color:var(--text2);font-style:italic">"PermitLookup shows me who just filed a roofing permit — I call them the same day. Last month I closed 3 extra jobs worth $45K."</p>
  <p style="margin-top:12px;color:var(--text3);font-size:14px">— Mike R., Roofing Contractor, Hays County TX</p>
</div>
```

(Replace with actual name/quote after interview)

---

### Task 20: Deploy All Changes to R730-2

- [ ] **Step 1: Push all commits to GitHub**

```bash
cd /home/will/permit-api && git push origin main
```

- [ ] **Step 2: Pull on R730-2**

```bash
ssh will@100.87.214.106 "cd /home/will/permit-api && git pull origin main"
```

- [ ] **Step 3: Restart API**

```bash
ssh will@100.87.214.106 "sudo systemctl restart permitlookup-api"
```

- [ ] **Step 4: Bump service worker cache**

```bash
ssh will@100.87.214.106 "sed -i 's/permitlookup-v[0-9]*/permitlookup-v4/' /home/will/permit-api/app/static/sw.js"
```

- [ ] **Step 5: Full smoke test**

Run through the complete demo flow:
1. Visit `https://r730-2-1.tailad2d5f.ts.net/` (or permits.ecbtx.com if Cloudflare is set up)
2. Verify hero stats load (835M+)
3. Click "Try Demo" → verify toast appears
4. Search "Austin TX" → verify results load
5. Click "AI Analyst" → ask a question → verify response
6. Click "Risk Score" → enter an address → verify it works
7. Click "Coverage" → verify states show
8. Click "Contractors" → search by state → verify results
9. Click "Pricing" → verify 4 tiers show
10. Verify nav only shows working items (no Alerts, Leads, Dialer, CRM, Playground)

---

## SUMMARY: Execution Order

| Day | Tasks | Outcome |
|-----|-------|---------|
| 1 | Tasks 1-6 (nav, contractors, coverage, API key, demo button, errors) | Demo flow works end-to-end |
| 2 | Tasks 7-8 (960M fix, seed data) | Accurate numbers, non-empty CRM |
| 3 | Task 9 (systemd services) | Survives reboots |
| 4-7 | Tasks 10-11 (Cloudflare, monitoring) | permits.ecbtx.com works |
| 7-10 | Tasks 12-13 (pricing backend + frontend) | Clean 4-tier pricing |
| 7-14 | Tasks 14-15 (find insurance data, demo security) | Email list ready, demo secure |
| 10-14 | Tasks 16-18 (onboarding, email infra, county stats) | Sales infrastructure ready |
| 14+ | Tasks 19-20 (roofer conversion, deploy, smoke test) | First paying customer |
