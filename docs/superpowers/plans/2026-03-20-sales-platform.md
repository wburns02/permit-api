# PermitLookup Sales Platform — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform PermitLookup from a data API into a sticky sales acceleration platform with CRM, pipeline tracking, commissions, team management, AI briefings, quote builder, and review automation — making it impossible for users to leave.

**Architecture:** All features build on the existing dialer backend (call_logs, lead_statuses, hot_leads tables) and PermitLookup SPA. Backend endpoints in FastAPI, frontend pages in the existing vanilla JS SPA. AI features use Claude Haiku via Anthropic SDK. Email via SendGrid. Patterns pulled from MAC Septic CRM (~/react-crm-api/) for call analysis, commissions, and Google STT.

**Tech Stack:** FastAPI, SQLAlchemy async, PostgreSQL, Anthropic Claude API, SendGrid, Google Cloud STT, vanilla JS SPA

**Existing code to reuse:**
- `~/react-crm-api/app/services/call_analysis_service.py` — call transcription → AI analysis pipeline
- `~/react-crm-api/app/services/commission_service.py` — commission calculation patterns
- `~/react-crm-api/app/services/google_stt_service.py` — real-time speech-to-text
- `~/react-crm-api/app/services/email_service.py` — SendGrid email patterns
- `~/react-crm-api/app/models/call_log.py` — call log model patterns
- `~/react-crm-api/app/models/customer.py` — CRM contact model patterns

---

## Phase 1: CRM Foundation (Tasks 1-3)
*Must be built first — other features depend on contacts and deals.*

### Task 1: CRM Models — Contacts, Deals, Notes

**Files:**
- Create: `app/models/crm.py`
- Modify: `app/main.py` — import models, add DDL to migrate-expansion

- [ ] **Step 1: Create CRM models**

`app/models/crm.py` — three tables:

**Contact** — a person or company the user is tracking:
- id (UUID PK), user_id (FK api_users), name, company, phone, email, address, city, state, zip
- lead_source (permit/referral/cold), lead_id (FK hot_leads.id — links back to the permit)
- tags (JSONB), created_at, updated_at

**Deal** — a potential sale in the pipeline:
- id (UUID PK), user_id (FK api_users), contact_id (FK contacts)
- title, stage (new/contacted/quoted/negotiating/won/lost), value (float)
- expected_close_date, actual_close_date, lost_reason
- notes, permit_number, permit_type, created_at, updated_at

**Note** — attached to a contact or deal:
- id (UUID PK), user_id (FK api_users), contact_id (FK), deal_id (FK)
- content (Text), note_type (call/email/meeting/task/system)
- created_at

Indexes: contacts(user_id, phone), contacts(user_id, email), deals(user_id, stage), deals(user_id, contact_id)

- [ ] **Step 2: Register models in main.py and add DDL to migrate-expansion**
- [ ] **Step 3: Verify app loads, commit**

---

### Task 2: CRM API Endpoints

**Files:**
- Create: `app/api/v1/crm.py`
- Modify: `app/main.py` — register router

Endpoints:
- `GET /v1/crm/contacts` — list user's contacts with search/filter
- `POST /v1/crm/contacts` — create contact (optionally from a hot_lead)
- `GET /v1/crm/contacts/{id}` — contact detail with deals + notes + call history
- `PUT /v1/crm/contacts/{id}` — update contact
- `GET /v1/crm/deals` — list deals with stage filter
- `POST /v1/crm/deals` — create deal (linked to contact)
- `PUT /v1/crm/deals/{id}` — update deal (stage change, value, etc.)
- `POST /v1/crm/notes` — add a note to a contact or deal
- `GET /v1/crm/pipeline` — pipeline summary (count + total value per stage)
- `POST /v1/crm/contacts/from-lead` — auto-create contact from a hot_lead record

All endpoints require auth (Explorer+). Follow existing dialer.py patterns.

- [ ] **Step 1: Create crm.py router with all endpoints**
- [ ] **Step 2: Register in main.py, add SPA route /crm**
- [ ] **Step 3: Verify app loads, commit**
- [ ] **Step 4: Deploy and run migration on production**

---

### Task 3: CRM Frontend Page

**Files:**
- Modify: `app/static/index.html` — add CRM page with contacts list, deal pipeline, contact detail view

The CRM page has 3 views:
1. **Contacts list** — searchable table with name, company, phone, lead source, deals count
2. **Pipeline board** — kanban-style columns (New → Contacted → Quoted → Negotiating → Won/Lost) with deal cards showing value
3. **Contact detail** — when you click a contact: their info, deals, notes timeline, call history from call_logs

Add nav link "CRM" between Dialer and Playground.

Use existing SPA patterns (page divs, showPage function, fetch with currentKey).

- [ ] **Step 1: Add CRM nav link and page HTML**
- [ ] **Step 2: Add JavaScript for contacts CRUD, pipeline view, contact detail**
- [ ] **Step 3: Wire "Save to CRM" button on dialer lead cards** — when viewing a lead in the dialer, one-click creates a contact
- [ ] **Step 4: Commit, deploy**
- [ ] **Step 5: Playwright test — verify CRM page loads, pipeline renders**

---

## Phase 2: Pipeline & Tracking (Tasks 4-6)

### Task 4: Pipeline Dashboard with Charts

**Files:**
- Modify: `app/api/v1/crm.py` — add dashboard stats endpoint
- Modify: `app/static/index.html` — add dashboard section to CRM page

Add `GET /v1/crm/dashboard`:
- Pipeline: count + total value per stage
- This week: new contacts, calls made, deals won, revenue
- Conversion funnel: leads → contacts → quoted → won (percentages)
- Top leads by value

Frontend: render as stat cards + simple bar chart (CSS-only, no chart library needed — use div widths as bars).

- [ ] **Step 1: Add dashboard endpoint**
- [ ] **Step 2: Add dashboard HTML/JS to CRM page**
- [ ] **Step 3: Commit, deploy, Playwright test**

---

### Task 5: Commission Tracking

**Files:**
- Create: `app/models/commission.py` — Commission model
- Modify: `app/api/v1/crm.py` — add commission endpoints
- Modify: `app/static/index.html` — add commissions section

Reference: `~/react-crm-api/app/services/commission_service.py`

**Commission model:**
- id, user_id, deal_id (FK deals), amount, rate, status (pending/paid), pay_date, created_at

Endpoints:
- `GET /v1/crm/commissions` — list user's commissions
- `GET /v1/crm/commissions/summary` — total earned, total pending, this month, last month

Auto-create commission when a deal stage changes to "won":
- Default rate: 10% of deal value
- Commission amount = deal.value * rate

Frontend: commissions summary card on the CRM dashboard.

- [ ] **Step 1: Create commission model + auto-creation logic**
- [ ] **Step 2: Add commission endpoints**
- [ ] **Step 3: Add to frontend dashboard**
- [ ] **Step 4: Commit, deploy, Playwright test**

---

### Task 6: Leaderboard

**Files:**
- Modify: `app/api/v1/crm.py` — add leaderboard endpoint
- Modify: `app/static/index.html` — add leaderboard section

`GET /v1/crm/leaderboard?period=week` (public within the account):
- Ranks users by: calls made, deals won, revenue closed, conversion rate
- Period: today, week, month, all-time

Frontend: leaderboard table on the CRM page with rank, name, calls, deals, revenue.

- [ ] **Step 1: Add leaderboard endpoint (queries call_logs + deals grouped by user)**
- [ ] **Step 2: Add leaderboard HTML/JS**
- [ ] **Step 3: Commit, deploy, Playwright test**

---

## Phase 3: AI & Automation (Tasks 7-9)

### Task 7: Morning Briefing Email

**Files:**
- Create: `app/services/morning_briefing.py`
- Create: `scripts/send_morning_briefings.sh` — cron script

Reference: `~/react-crm-api/app/services/email_service.py` for SendGrid patterns

Daily at 7 AM, send each active user an email:
- "Good morning [name]! Here's your sales briefing:"
- New permits in your area since yesterday: [count]
- Callbacks due today: [list with names + phones]
- Pipeline value: $[total] across [count] active deals
- Yesterday's stats: [calls], [new contacts], [deals won]
- CTA button: "Open Dialer →" linking to permits.ecbtx.com/#dialer

Use SendGrid with a simple HTML template. AI-generate a one-line motivational insight using Claude Haiku ("Based on your activity, focus on [ZIP] today — permit velocity is up 40%").

Cron: `0 7 * * 1-5 python3 -u /home/will/send_morning_briefings.py >> /tmp/briefings.log 2>&1`

- [ ] **Step 1: Create morning_briefing.py service (build email, query user stats)**
- [ ] **Step 2: Create send script for cron**
- [ ] **Step 3: Add cron to R730**
- [ ] **Step 4: Test by sending to willwalterburns@gmail.com**
- [ ] **Step 5: Commit**

---

### Task 8: AI Daily Digest + Callback Reminders

**Files:**
- Modify: `app/services/morning_briefing.py` — add AI digest
- Create: `app/services/callback_reminder.py` — SMS/email reminders

AI Daily Digest (added to morning email):
- Claude analyzes the user's last 7 days of calls and generates:
  - "Your hottest lead: [name] — they asked for a quote on [date]"
  - "Trend: roofing permits in [ZIP] up 30% this week"
  - "Suggestion: Call back [name] — similar leads close within 48 hours"

Callback Reminders:
- Check callback_date on call_logs every hour
- Send email reminder 1 hour before: "Reminder: Call back [name] at [time] — [notes from last call]"
- Cron: `0 * * * * python3 -u /home/will/check_callbacks.py >> /tmp/callbacks.log 2>&1`

- [ ] **Step 1: Add AI digest to morning briefing**
- [ ] **Step 2: Create callback reminder service + script**
- [ ] **Step 3: Add hourly cron**
- [ ] **Step 4: Commit**

---

### Task 9: Quote/Estimate Builder

**Files:**
- Create: `app/api/v1/quotes.py` — quote CRUD + PDF generation
- Create: `app/models/quote.py` — Quote model
- Modify: `app/static/index.html` — add quote builder UI
- Modify: `app/main.py` — register

**Quote model:**
- id, user_id, contact_id, deal_id
- items (JSONB: [{description, quantity, unit_price, total}])
- subtotal, tax_rate, tax_amount, total
- status (draft/sent/accepted/declined)
- valid_until (date), sent_at, accepted_at
- notes, terms
- company_name, company_phone, company_email (user's business info)

Endpoints:
- `POST /v1/quotes` — create from deal/contact
- `GET /v1/quotes/{id}` — get quote (returns HTML-renderable data)
- `PUT /v1/quotes/{id}` — update
- `POST /v1/quotes/{id}/send` — email the quote to the contact
- `GET /v1/quotes/{id}/pdf` — generate simple PDF (HTML-to-string, no wkhtmltopdf needed)

Frontend: "Create Quote" button on deal detail. Form with line items, auto-fills from permit data (address, project type, valuation as estimate basis).

- [ ] **Step 1: Create quote model**
- [ ] **Step 2: Create quotes API**
- [ ] **Step 3: Add quote builder UI**
- [ ] **Step 4: Commit, deploy, Playwright test**

---

## Phase 4: Growth Features (Tasks 10-12)

### Task 10: Review Request Automation

**Files:**
- Modify: `app/api/v1/crm.py` — add review request endpoint
- Modify: `app/services/morning_briefing.py` — reuse SendGrid

When a deal is marked "won":
- Auto-send a review request email to the contact after 7 days
- Template: "Hi [name], thank you for choosing [company]! We'd love your feedback. [Google Review Link]"
- Track: review_requested_at, review_link on the deal

Endpoint: `POST /v1/crm/deals/{id}/request-review` (manual trigger too)

- [ ] **Step 1: Add review request logic to deal stage-change handler**
- [ ] **Step 2: Add manual trigger endpoint**
- [ ] **Step 3: Commit**

---

### Task 11: Team Management

**Files:**
- Create: `app/models/team.py` — Team, TeamMember models
- Modify: `app/api/v1/crm.py` — add team endpoints
- Modify: `app/static/index.html` — add team management UI

**Team model:**
- id, name, owner_id (FK api_users), created_at

**TeamMember:**
- id, team_id, user_id, role (owner/manager/member), territories (JSONB: [zip codes or states])

Endpoints:
- `POST /v1/teams` — create team
- `POST /v1/teams/{id}/members` — add member by email
- `GET /v1/teams/{id}/members` — list members
- `PUT /v1/teams/{id}/members/{member_id}` — update role/territories
- `GET /v1/teams/{id}/dashboard` — team stats (aggregate of all members' calls/deals)

Territory assignment: each member gets ZIP codes or states. The dialer queue automatically filters to their territory.

- [ ] **Step 1: Create team models**
- [ ] **Step 2: Create team endpoints**
- [ ] **Step 3: Add team management UI section**
- [ ] **Step 4: Wire territory filtering into dialer queue**
- [ ] **Step 5: Commit, deploy, Playwright test**

---

### Task 12: Final Integration + Playwright Verification

**Files:**
- Modify: `frontend/e2e/permit-data-expansion.spec.ts` — add CRM + dialer tests
- Modify: `app/static/index.html` — polish and connect all features

- [ ] **Step 1: Add Playwright tests for CRM page, pipeline, contacts, deals**
- [ ] **Step 2: Add Playwright tests for dialer queue loading, save to CRM flow**
- [ ] **Step 3: Add Playwright tests for quote builder**
- [ ] **Step 4: Run full test suite — fix any failures**
- [ ] **Step 5: Final deploy**
- [ ] **Step 6: Update pricing page to highlight Sales Platform features**

---

## Execution Order

```
Phase 1 (CRM Foundation):  Task 1 → Task 2 → Task 3
Phase 2 (Pipeline):        Task 4 → Task 5 → Task 6  (can parallel with Phase 1 backend)
Phase 3 (AI/Automation):   Task 7 → Task 8 → Task 9  (independent)
Phase 4 (Growth):          Task 10 → Task 11 → Task 12
```

Tasks 1-3 are sequential (each builds on the prior).
Tasks 4-6, 7-9, and 10-11 are semi-independent and can be parallelized.
Task 12 is the final verification that ties everything together.
