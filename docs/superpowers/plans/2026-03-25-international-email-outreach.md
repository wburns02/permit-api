# International Expansion + Automated Email Outreach — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand PermitLookup to UK/Australia data AND launch automated email outreach to 224K+ contacts to drive first revenue.

**Architecture:** International scrapers follow existing patterns (httpx + psycopg2 → T430). Email outreach uses SendGrid with drip sequences, stored in a new campaign management system. CAN-SPAM compliant with unsubscribe tracking.

**Tech Stack:** FastAPI, SendGrid, psycopg2, httpx, Jinja2 templates

---

## Part A: Email Outreach System (Tasks 1-5)

### Task 1: Email Campaign Models + Database

**Files:**
- Create: `app/models/email_campaign.py`
- Modify: `app/main.py` — register models + DDL

Models:
```python
class EmailCampaign(Base):
    __tablename__ = "email_campaigns"
    id (UUID PK), user_id (FK api_users, nullable — system campaigns have no user),
    name (String 200), subject (String 500), body_html (Text), body_text (Text),
    target_audience (String 100 — "insurance_agents", "re_agents", "contractors", "all"),
    target_state (String 2, nullable — filter by state),
    status (String 20 — draft/active/paused/completed),
    sent_count (Integer default 0), open_count (Integer default 0),
    click_count (Integer default 0), unsubscribe_count (Integer default 0),
    signup_count (Integer default 0),
    send_rate (Integer default 200 — emails per hour),
    created_at, started_at, completed_at (DateTime)

class EmailRecipient(Base):
    __tablename__ = "email_recipients"
    id (UUID PK), campaign_id (FK email_campaigns),
    email (String 255 NOT NULL), name (String 500),
    company (String 500), state (String 2),
    license_type (String 100),
    status (String 20 — pending/sent/opened/clicked/unsubscribed/bounced),
    sent_at (DateTime), opened_at (DateTime), clicked_at (DateTime),
    unsubscribed_at (DateTime)

class EmailUnsubscribe(Base):
    __tablename__ = "email_unsubscribes"
    id (UUID PK), email (String 255 NOT NULL UNIQUE),
    reason (Text), unsubscribed_at (DateTime default now)
```

Indexes: recipients(campaign_id, status), recipients(email), unsubscribes(email)

- [ ] **Step 1: Create models**
- [ ] **Step 2: Register in main.py + add DDL to migrate-expansion**
- [ ] **Step 3: Commit**

---

### Task 2: Email Templates

**Files:**
- Create: `app/services/email_templates.py`

Industry-specific email templates using string formatting (no Jinja2 needed for v1):

**Template 1: Insurance Agents**
Subject: "Your competitors are using permit data to write better policies — are you?"
Body: Highlights environmental risk, flood zones, code violations, property reports. CTA: "Start free trial"

**Template 2: Real Estate Agents**
Subject: "Know about every property transaction before your competition"
Body: Highlights property sales, market trends, predictive analytics. CTA: "Get free access"

**Template 3: Contractors (Roofers, HVAC, etc.)**
Subject: "47 new {trade} permits filed in {state} this week — want the leads?"
Body: Highlights hot leads, dialer, fresh permits with contractor details. CTA: "Start free trial"

**Template 4: General/Follow-up**
Subject: "Quick follow-up — 923M+ property records at your fingertips"
Body: Broader pitch covering all features. CTA: "Try it free"

Each template includes:
- Professional HTML (dark theme matching app)
- Plain text version
- Unsubscribe link: `https://permits.ecbtx.com/unsubscribe?email={email}&token={token}`
- Physical mailing address (CAN-SPAM requirement): "PermitLookup, San Marcos, TX 78666"
- Personalization: {name}, {state}, {license_type}

- [ ] **Step 1: Create template functions returning (subject, html, text) tuples**
- [ ] **Step 2: Commit**

---

### Task 3: Email Sending Service

**Files:**
- Create: `app/services/email_outreach.py`

Core functions:

```python
async def send_campaign_batch(campaign_id, batch_size=50):
    """Send next batch of emails for a campaign."""
    # 1. Get campaign details
    # 2. Get next batch_size pending recipients (not in unsubscribe list)
    # 3. For each: personalize template, send via SendGrid
    # 4. Update recipient status to 'sent'
    # 5. Increment campaign sent_count

async def create_campaign_from_prospects(name, audience, state, template_key, send_rate=200):
    """Create a campaign targeting prospects from prospect_contacts table."""
    # 1. Create EmailCampaign record
    # 2. Query prospect_contacts WHERE email IS NOT NULL
    #    AND license_type matches audience
    #    AND email NOT IN (SELECT email FROM email_unsubscribes)
    #    AND state filter if provided
    # 3. Bulk insert EmailRecipient records
    # Return campaign_id + recipient count

def generate_unsubscribe_token(email):
    """HMAC token for secure unsubscribe links."""
    return hmac.new(settings.SECRET_KEY.encode(), email.encode(), hashlib.sha256).hexdigest()[:16]

def verify_unsubscribe_token(email, token):
    return generate_unsubscribe_token(email) == token
```

SendGrid integration:
```python
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, TrackingSettings, OpenTracking, ClickTracking

sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
message = Mail(
    from_email=("outreach@permitlookup.com", "PermitLookup"),
    to_emails=recipient_email,
    subject=subject,
    html_content=html_body,
    plain_text_content=text_body,
)
# Enable open + click tracking
message.tracking_settings = TrackingSettings(
    open_tracking=OpenTracking(True),
    click_tracking=ClickTracking(True, True),
)
sg.send(message)
```

- [ ] **Step 1: Create email outreach service**
- [ ] **Step 2: Commit**

---

### Task 4: Campaign API Endpoints + Webhook Tracking

**Files:**
- Create: `app/api/v1/campaigns.py`
- Modify: `app/main.py` — register router

Endpoints:
- POST /v1/campaigns — create campaign (admin only)
- GET /v1/campaigns — list campaigns
- POST /v1/campaigns/{id}/start — begin sending
- POST /v1/campaigns/{id}/pause — pause sending
- GET /v1/campaigns/{id}/stats — open/click/signup rates
- GET /v1/unsubscribe — unsubscribe page (public, renders HTML form)
- POST /v1/unsubscribe — process unsubscribe (public)
- POST /v1/campaigns/sendgrid-webhook — SendGrid event webhook (open/click/bounce events)

The SendGrid webhook receives events:
```json
[{"event": "open", "email": "...", "timestamp": ...},
 {"event": "click", "email": "...", "url": "...", "timestamp": ...},
 {"event": "bounce", "email": "...", "timestamp": ...}]
```

Update EmailRecipient status based on events.

- [ ] **Step 1: Create campaigns router with all endpoints**
- [ ] **Step 2: Add unsubscribe page HTML (simple, professional)**
- [ ] **Step 3: Register in main.py**
- [ ] **Step 4: Commit**

---

### Task 5: Campaign Runner Script (Cron)

**Files:**
- Create: `scripts/run_email_campaigns.py`

This runs every 10 minutes via cron on R730. For each active campaign:
1. Calculate how many emails to send this batch (send_rate / 6 per 10-min interval)
2. Call send_campaign_batch()
3. Log results

Also implements drip sequences:
- Day 0: Initial email (template for their industry)
- Day 3: Follow-up if no open ("Just checking in...")
- Day 7: Final follow-up with different angle ("Your competitors are already using this...")

The drip logic: when checking pending recipients, also check for recipients who were sent but haven't opened after 3 days → send follow-up with different subject/body.

Cron: `*/10 * * * * python3 -u /home/will/run_email_campaigns.py --db-host 100.122.216.15 >> /tmp/email_campaigns.log 2>&1`

- [ ] **Step 1: Create campaign runner script**
- [ ] **Step 2: Add drip sequence logic**
- [ ] **Step 3: Copy to R730, add to cron**
- [ ] **Step 4: Commit**

---

## Part B: International Data Expansion (Tasks 6-8)

### Task 6: UK Data Scrapers

**Files:**
- Create: `scripts/scrape_uk_data.py`

Known UK open data sources:
- **HM Land Registry Price Paid** — every property sale in England/Wales since 1995 (~28M records)
  Bulk CSV: `http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv`
  Fields: price, date, address, postcode, property_type, new_build, estate_type

- **Energy Performance Certificates** — every EPC in England/Wales (~24M)
  API: `https://epc.opendatacommunities.org/api/v1/domestic/search`
  Fields: address, postcode, current_energy_rating (A-G), potential_energy_rating, property_type

- **UK Companies House** — every registered company (~5M active)
  Bulk CSV: `https://download.companieshouse.gov.uk/en_output.html`
  Fields: company_name, company_number, address, status, incorporation_date, SIC_code

- **UK Planning Applications** — varies by council. London has good data.
  Try: data.london.gov.uk for planning applications

Tables: Reuse property_sales (source='uk_land_registry'), business_entities (source='uk_companies_house'), new table for EPC ratings.

- [ ] **Step 1: Create UK data scraper with Land Registry, Companies House, EPC sections**
- [ ] **Step 2: Deploy to R730, run initial load**
- [ ] **Step 3: Add to monthly cron**
- [ ] **Step 4: Commit**

---

### Task 7: Australia Data Scrapers

**Files:**
- Create: `scripts/scrape_australia_data.py`

Known Australian open data sources:
- **ABS Building Approvals** — national building approval statistics
  API: `https://api.data.abs.gov.au/data/ABS,BUILDING_APPROVALS/` (SDMX format)

- **NSW Planning Portal** — building approvals in New South Wales
  Try: `https://data.nsw.gov.au` search for "building approval" or "development application"

- **Victoria Building Authority** — registered builders
  Try: `https://data.vic.gov.au` search for "building" or "builder"

- **Queensland QBCC** — licensed builders/contractors
  Try: `https://data.qld.gov.au` search for "building" or "contractor"

- **Australian Business Register (ABR)** — all registered businesses
  API: `https://abr.business.gov.au/` (requires registration for bulk)

Research agent will provide exact verified endpoints. Structure the scraper with configs per state/territory.

- [ ] **Step 1: Create Australia scraper with verified endpoints**
- [ ] **Step 2: Deploy to R730, run initial load**
- [ ] **Step 3: Add to monthly cron**
- [ ] **Step 4: Commit**

---

### Task 8: Frontend + Campaign Management UI

**Files:**
- Modify: `app/static/index.html` — add Campaigns management page

Admin-only page for managing email campaigns:
- Campaign list: name, status, sent/opened/clicked counts, conversion rate
- Create campaign form: select audience (insurance/RE/contractors), state filter, template preview
- Campaign detail: recipient list with status, real-time open/click stats
- Start/pause buttons
- Unsubscribe list viewer

Also update the pricing page and hero to mention international coverage (UK/Australia).

- [ ] **Step 1: Add Campaigns page to admin section**
- [ ] **Step 2: Update hero/pricing for international coverage**
- [ ] **Step 3: Commit, deploy, test**

---

## Execution Order

```
Task 1 (models) → Task 2 (templates) → Task 3 (send service) → Task 4 (API) → Task 5 (cron runner)
Task 6 (UK data) — independent, can parallel
Task 7 (Australia data) — independent, can parallel
Task 8 (frontend) — after Tasks 4+6+7
```

Tasks 1-5 are sequential (email system builds on itself).
Tasks 6-7 are independent of each other and the email system.
Task 8 ties everything together.
