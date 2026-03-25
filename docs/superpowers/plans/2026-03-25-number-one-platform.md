# PermitLookup #1 Platform — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every competitive gap to make PermitLookup the undisputed #1 property intelligence platform in the world — sub-second queries, mobile access, complete property data, AI valuations, contractor marketplace, real-time streams, enterprise integrations, and compliance certification.

**Architecture:** Each of the 10 features is independent and can be built in parallel. Priority order: speed first (unlocks everything), then data completeness, then growth features, then enterprise.

**Tech Stack:** FastAPI, PostgreSQL streaming replication, React Native/PWA, XGBoost, WebSockets, Zapier webhooks, Stripe Connect

---

## Phase 1: Speed (Task 1) — THE BLOCKER

### Task 1: Complete R730-2 Read Replica Setup

**Status:** pg_basebackup running now. When complete:

**Files:**
- Modify: Railway env vars — change DATABASE_URL to R730-2
- Modify: `app/main.py` — no code changes needed, just env var
- Modify: `start.sh` — simplify (direct connection, no SOCKS proxy)

- [ ] **Step 1: Wait for pg_basebackup to complete**

Check: `ssh will@100.87.214.106 "cat /tmp/basebackup.log"`
Expected: "pg_basebackup: base backup completed"

- [ ] **Step 2: Apply RAM-optimized postgresql.conf on R730-2**

The config was already written during setup but was overwritten by basebackup. Re-apply:
```
shared_buffers = 512GB
effective_cache_size = 700GB
work_mem = 1GB
maintenance_work_mem = 8GB
hot_standby = on
listen_addresses = '*'
```

- [ ] **Step 3: Start PostgreSQL on R730-2**

```bash
ssh will@100.87.214.106 "sudo systemctl enable --now postgresql"
```

Verify replication is streaming:
```bash
ssh will@192.168.7.83 "sudo -u postgres psql -c 'SELECT * FROM pg_stat_replication;'"
```

- [ ] **Step 4: Test query speed on R730-2**

```bash
ssh will@100.87.214.106 "psql -U will -d permits -c 'SELECT count(*) FROM hot_leads;'"
```

Should return in <1 second (vs 20+ seconds through SOCKS proxy).

- [ ] **Step 5: Update Railway to connect to R730-2**

```bash
cd /home/will/permit-api
railway variables --set "DATABASE_URL=postgresql://will@100.87.214.106:5432/permits"
```

Simplify start.sh — remove the entire SOCKS proxy setup:
```bash
#!/bin/sh
set -e
echo "Starting Tailscale..."
tailscaled --tun=userspace-networking --socks5-server=localhost:1055 &
sleep 3
tailscale up --authkey="${TAILSCALE_AUTHKEY}" --hostname=permit-api-railway
sleep 5
echo "Starting PermitLookup API..."
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}
```

Wait — Railway still needs Tailscale to reach R730-2 via Tailscale IP. But now it connects DIRECTLY to PostgreSQL on R730-2 (no SOCKS proxy in between). The connection is: Railway → Tailscale → R730-2:5432 (direct TCP, no socat/SOCKS5 middleman).

Actually, the simplest approach: use the same Tailscale + socat pattern but point to R730-2 instead of T430:

In start.sh, change the socat target from `100.122.216.15:5432` to `100.87.214.106:5432`.

- [ ] **Step 6: Deploy and verify**

```bash
git commit -am "perf: switch production DB to R730-2 read replica (768GB RAM)"
git push origin main && railway up --detach
```

Test: `curl -m 5 https://permits.ecbtx.com/v1/stats` — should return in <2 seconds.

- [ ] **Step 7: Disable T430 sleep AGAIN and set R730-2 to never sleep**

```bash
ssh will@100.87.214.106 "sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target"
```

- [ ] **Step 8: Commit**

---

## Phase 2: Data Completeness (Tasks 2-4)

### Task 2: Property Characteristics Scraper (beds/baths/sqft/year built)

**Files:**
- Create: `scripts/scrape_property_characteristics.py`
- Create: `app/models/property_characteristics.py` (or add table to data_layers.py)

**Table:**
```sql
CREATE TABLE IF NOT EXISTS property_characteristics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    address TEXT, city TEXT, state VARCHAR(2) NOT NULL, zip TEXT,
    county TEXT, parcel_id TEXT,
    bedrooms INTEGER, bathrooms FLOAT, sqft FLOAT,
    lot_size_sqft FLOAT, year_built INTEGER, stories INTEGER,
    property_type TEXT, -- SFR, condo, townhouse, multi-family
    construction_type TEXT, -- frame, brick, stucco
    roof_type TEXT, pool BOOLEAN, garage BOOLEAN,
    assessed_value FLOAT, tax_amount FLOAT,
    lat FLOAT, lng FLOAT,
    source TEXT NOT NULL);
```

**Data sources (county assessor data on Socrata):**
- Cook County IL: `datacatalog.cookcountyil.gov` — residential assessments with beds/baths/sqft
- NYC PLUTO: `data.cityofnewyork.us` — every property in NYC with characteristics
- LA County Assessor: `data.lacounty.gov` — property characteristics
- Search: `https://api.us.socrata.com/api/catalog/v1?q=property+characteristics+assessor&limit=30`

Also: Maryland statewide assessments (already loaded in `property_assessments` table — has year_built, sqft, bedrooms, bathrooms).

- [ ] **Step 1: Research and verify county assessor endpoints**
- [ ] **Step 2: Create scraper with 5+ county sources**
- [ ] **Step 3: Create API endpoint: GET /v1/properties/{address}/characteristics**
- [ ] **Step 4: Deploy, run, add to monthly cron**
- [ ] **Step 5: Commit**

---

### Task 3: Individual Property Value Estimates (ML Model)

**Files:**
- Create: `scripts/train_property_valuation_model.py`
- Create: `app/api/v1/valuation_estimate.py`

Train an XGBoost regression model to estimate individual property values using:
- Features: ZIP median prices (property_valuations), bedrooms, bathrooms, sqft, year_built, lot_size (property_characteristics), census demographics, permit history, school ratings
- Labels: actual sale prices (property_sales table — 8M+ records)
- Output: estimated value for any address

**Endpoint:** `GET /v1/properties/{address}/estimate`
Returns: `{estimated_value: 485000, confidence: 0.87, comparable_sales: [...], factors: [...]}`

- [ ] **Step 1: Build feature engineering pipeline**
- [ ] **Step 2: Train XGBoost regressor on property_sales**
- [ ] **Step 3: Create estimate endpoint**
- [ ] **Step 4: Deploy model to R730, add to weekly retrain cron**
- [ ] **Step 5: Commit**

---

### Task 4: Verified Contact Enrichment

**Files:**
- Create: `app/services/contact_enrichment.py`
- Modify: `app/api/v1/campaigns.py` — add enrichment before sending

Before sending email campaigns, verify contacts are deliverable:
- Use a free email verification API (e.g., Hunter.io free tier, or ZeroBounce free tier — 100/month)
- Or build basic MX record checking: verify the email domain has valid MX records
- Flag bounced emails after campaign sends (SendGrid bounce events already tracked)

For phone verification:
- Basic format validation (10 digits, valid area code)
- Twilio Lookup API ($0.005/lookup) for carrier + line type — optional, costs money

V1 approach (free): MX record validation + format checking. Flag invalid before send.

- [ ] **Step 1: Build MX validation service**
- [ ] **Step 2: Add pre-send validation to campaign runner**
- [ ] **Step 3: Commit**

---

## Phase 3: Growth Features (Tasks 5-8)

### Task 5: Mobile App (PWA)

**Files:**
- Create: `app/static/manifest.json`
- Create: `app/static/sw.js` (service worker)
- Modify: `app/static/index.html` — add PWA meta tags + manifest link

A Progressive Web App (PWA) is the fastest path to mobile — no app store submission, works on iOS + Android, installable from the browser.

Requirements:
- `manifest.json` with app name, icons, theme color, start_url
- Service worker for offline caching of static assets
- `<meta name="viewport">` already exists
- Add to homescreen prompt
- Touch-friendly UI adjustments (larger tap targets, bottom nav)

The existing responsive CSS should mostly work — the SPA was built with mobile breakpoints.

- [ ] **Step 1: Create manifest.json with PermitLookup branding**
- [ ] **Step 2: Create service worker (cache static assets + API responses)**
- [ ] **Step 3: Add PWA meta tags to index.html**
- [ ] **Step 4: Test on mobile browser — verify "Add to Home Screen" works**
- [ ] **Step 5: Commit**

---

### Task 6: Contractor Marketplace

**Files:**
- Create: `app/api/v1/marketplace.py`
- Create: `app/models/marketplace.py`
- Modify: `app/static/index.html` — add marketplace page

**Models:**
```python
class ContractorProfile(Base):
    __tablename__ = "contractor_profiles"
    id (UUID PK), user_id (FK api_users),
    business_name, description, trades (JSONB — ["roofing", "hvac"]),
    service_area (JSONB — ["78666", "78640"]), # ZIP codes
    license_number, license_state,
    website, phone, email,
    verified (Boolean), featured (Boolean),
    rating_score (Float), rating_count (Integer),
    permit_count (Integer — auto-computed from permits data),
    risk_score (Integer — auto-computed),
    created_at

class ContractorReview(Base):
    __tablename__ = "contractor_reviews"
    id (UUID PK), profile_id (FK contractor_profiles),
    reviewer_name, rating (Integer 1-5), review_text,
    verified_customer (Boolean), created_at
```

**Endpoints:**
- GET /v1/marketplace/search?trade=roofing&zip=78666 — search contractors ranked by data
- GET /v1/marketplace/profile/{id} — contractor detail with permits, license, risk score, reviews
- POST /v1/marketplace/profile — create/claim profile (contractor signs up)
- POST /v1/marketplace/reviews — leave a review

**Ranking algorithm:** Sort by composite score:
- Permit volume (30%) — more permits = more established
- License status (25%) — active + clean = higher rank
- Risk score (25%) — lower risk = higher rank
- Reviews (20%) — higher rating = higher rank

This disrupts Angi/HomeAdvisor because rankings are based on DATA, not ad spend.

- [ ] **Step 1: Create marketplace models**
- [ ] **Step 2: Create marketplace API with search + ranking**
- [ ] **Step 3: Create marketplace frontend page**
- [ ] **Step 4: Commit, deploy**

---

### Task 7: Real-Time Permit Stream (WebSocket)

**Files:**
- Create: `app/api/v1/stream.py`
- Modify: `app/main.py` — mount WebSocket route

**WebSocket endpoint:** `ws://permits.ecbtx.com/v1/stream`

Client connects with filters:
```json
{"states": ["TX", "CA"], "types": ["roofing", "hvac"], "zips": ["78666"]}
```

Server pushes new permits as they're loaded by scrapers:
```json
{"event": "new_permit", "data": {"address": "...", "type": "...", "contractor": "...", "phone": "..."}}
```

Implementation: The daily scraper scripts (central_tx_daily, all_metros_daily) post new permits to a Redis pub/sub channel. The WebSocket handler subscribes to that channel and forwards to connected clients.

Simpler V1 (no Redis): poll the hot_leads table every 60 seconds for records with scraped_at = today that haven't been pushed yet.

- [ ] **Step 1: Create WebSocket endpoint with filter support**
- [ ] **Step 2: Create polling mechanism for new permits**
- [ ] **Step 3: Add "Live Feed" page to frontend**
- [ ] **Step 4: Commit, deploy**

---

### Task 8: Zapier + HubSpot + Salesforce Integrations

**Files:**
- Create: `app/api/v1/integrations.py`

**Zapier:** We already have webhooks. Zapier just needs:
- A "trigger" endpoint that returns new permits matching filters (polling trigger)
- `GET /v1/integrations/zapier/poll?state=TX&type=roofing&since={timestamp}`
- Returns new permits since last poll
- Zapier polls every 5-15 minutes

**HubSpot:** Push contacts to HubSpot CRM when a lead is qualified
- `POST /v1/integrations/hubspot/push` — sends contact to user's HubSpot via their API key
- User provides their HubSpot API key in settings

**Salesforce:** Same pattern
- `POST /v1/integrations/salesforce/push` — sends lead to Salesforce

V1: Just build the Zapier polling trigger. HubSpot/Salesforce are POST endpoints that forward data — can be done by Zapier itself using our webhook system.

- [ ] **Step 1: Create Zapier polling trigger endpoint**
- [ ] **Step 2: Document the Zapier integration steps**
- [ ] **Step 3: Commit**

---

## Phase 4: Enterprise (Tasks 9-10)

### Task 9: White-Label API

**Files:**
- Create: `app/api/v1/whitelabel.py`
- Modify: `app/middleware/api_key_auth.py` — add white-label key support

White-label customers get:
- Their own API subdomain or custom header (`X-WhiteLabel-Key`)
- Responses with their branding (no "PermitLookup" references)
- Custom rate limits and pricing
- Revenue share or flat monthly fee

**Model:**
```python
class WhiteLabelPartner(Base):
    __tablename__ = "whitelabel_partners"
    id (UUID PK), partner_name, api_key_prefix,
    branding (JSONB — {company_name, logo_url, support_email}),
    rate_limit (Integer), monthly_fee (Float),
    is_active (Boolean), created_at
```

**Implementation:** In the auth middleware, detect white-label keys (different prefix, e.g., `wl_live_`). Apply partner-specific branding to responses. Strip PermitLookup references.

- [ ] **Step 1: Create white-label partner model**
- [ ] **Step 2: Modify auth middleware for white-label detection**
- [ ] **Step 3: Add branding injection to API responses**
- [ ] **Step 4: Commit**

---

### Task 10: SOC 2 / Compliance Preparation

**Files:**
- Create: `docs/compliance/soc2-prep.md`
- Create: `app/services/audit_logger.py`
- Modify: multiple endpoints — add audit logging

SOC 2 Type II requires:
- **Access controls** — we have API key auth + plan tiers ✅
- **Encryption in transit** — HTTPS via Railway ✅
- **Encryption at rest** — PostgreSQL on encrypted volume (need to verify)
- **Audit logging** — log every data access with who, what, when
- **Change management** — git history ✅
- **Incident response plan** — document needed
- **Business continuity** — T430 primary + R730-2 replica ✅
- **Vendor management** — document Railway, SendGrid, Anthropic dependencies

The main code work: comprehensive audit logging.

```python
class AuditLog(Base):
    __tablename__ = "audit_logs"
    id (UUID PK), user_id, api_key_id,
    action (String — "search", "export", "report", "login"),
    resource (String — "permits", "violations", "contacts"),
    details (JSONB — query params, result count),
    ip_address, user_agent,
    created_at
```

Add `log_audit()` calls to every sensitive endpoint (search, export, report, batch, analyst).

- [ ] **Step 1: Create audit log model + service**
- [ ] **Step 2: Add audit logging to top 10 endpoints**
- [ ] **Step 3: Write SOC 2 prep documentation**
- [ ] **Step 4: Commit**

---

## Execution Order

```
IMMEDIATE:  Task 1 (speed — R730-2 replica)
WEEK 1:     Task 5 (PWA mobile) + Task 2 (property characteristics)
WEEK 2:     Task 3 (valuation model) + Task 7 (WebSocket stream)
WEEK 3:     Task 6 (contractor marketplace) + Task 8 (Zapier)
WEEK 4:     Task 4 (contact enrichment) + Task 9 (white-label)
ONGOING:    Task 10 (SOC 2 compliance)
```

Task 1 is the blocker — everything demos better when it's fast.
Tasks 2-4 fill data gaps that customers will ask about.
Tasks 5-8 create growth loops and network effects.
Tasks 9-10 unlock enterprise sales.
