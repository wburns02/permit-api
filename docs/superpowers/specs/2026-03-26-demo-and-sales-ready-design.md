# PermitLookup: Demo-Ready & Sales-Ready Implementation Spec

> **Status:** Approved by user on 2026-03-26
> **Goal:** Take PermitLookup from pre-revenue prototype to demo-ready product with working sales infrastructure, targeting first 20+ paying customers in 90 days.

---

## Context & Problem

PermitLookup has 835M+ permit records, a working API, and real data from 3,143 US jurisdictions. But the product presentation is broken:

- **30 nav links, half go nowhere** — Risk Score, Watchlist, Passport, CRM, Dialer all dead-end
- **permits.ecbtx.com doesn't work** — Tailscale Funnel rejects the Host header; browsers get TLS errors
- **Zero paying customers** — one pilot roofer with free Pro Leads access
- **No sales infrastructure** — 224K insurance agent emails sitting unused
- **Services don't survive reboot** — PostgreSQL and API require manual start

The core technology works. The data is real. The execution gap is product polish + go-to-market.

---

## Architecture: Three Parallel Workstreams

| Workstream | Goal | Timeline |
|-----------|------|----------|
| **1. Demo-Ready** | Every visible page works, zero dead ends | Days 1-7 |
| **2. Domain & Infrastructure** | permits.ecbtx.com works, services auto-start | Days 4-7 |
| **3. Sales-Ready** | Cold email system, first paying customers | Days 7-90 |

**Design principle:** Hide what's not ready. A product with 8 polished features beats one with 30 half-built ones.

---

## Workstream 1: Demo-Ready Product

### 1.1 Nav Cleanup

**Reduce nav from 30 links to 10 that work.**

**Main nav (always visible):**
- Search
- Contractors
- AI Analyst
- Pricing
- API Docs

**Tools dropdown:**
- Risk Score
- Property Report
- Coverage Map
- Batch Lookup

**Hidden (remove from nav, re-add when ready):**
- Alerts, Leads, Dialer, CRM, Playground, Watchlist, Market Momentum, Owner Intelligence, Value Analytics, Property Compare, Jurisdiction Intel, Estimator, Requirements, Activity Feed, Property Passport

**Implementation:** Edit `index.html` nav section. Comment out hidden items with `<!-- HIDDEN: re-add when ready -->` so they're easy to restore.

### 1.2 Fix Broken Endpoints

| Page | Bug | Fix |
|------|-----|-----|
| **Contractors** | Missing required `name` param causes 500 | Make `name` optional in `/v1/contractors/search`, allow search by city+state alone |
| **Coverage** | Returns `total_states: 0` | Fix SQL query to aggregate state counts from permits table partitions |
| **AI Analyst** | POST expects `question` field, frontend may send `query` | Align parameter name; add ANTHROPIC_API_KEY to R730-2 `.env` |
| **Risk Score** | Page renders but backend call fails or returns nothing | Wire frontend Risk Score page to `/v1/analyst/report?address=...` endpoint which works |

### 1.3 Demo Flow Polish

**"Try Demo" button on home page:**
- Add a prominent button below the search box: "Try Demo (Free) — No account needed"
- Clicking calls `POST /v1/demo`, stores the key, refreshes nav to show "demo (Enterprise)"

**Guided toast after demo login:**
- Show: "Welcome! Try searching 'Austin TX' or click AI Analyst to ask a question about any property."
- Dismiss after 8 seconds or on click

**Error handling:**
- Replace all `Internal Server Error` responses with friendly messages
- Any API timeout shows: "This query is taking longer than expected. Try a more specific search."
- Any 404 shows: "This feature is coming soon."

**Loading states:**
- Search results: show skeleton cards while loading
- Stats: show pulsing placeholders (already implemented)

### 1.4 Seed Data

Even though CRM/Dialer are hidden from nav, seed them so they're not empty if someone navigates there directly.

| Table | Records | Source |
|-------|---------|--------|
| CRM Contacts | 15 | Generate realistic contractor/agent contacts |
| CRM Deals | 5 | Various pipeline stages (New Lead → Won) |
| Dialer Queue | 20 | Real permit addresses with contractor names from permits table |

**Implementation:** Python script `seed_demo_data.py` that inserts demo records. Idempotent (check before insert).

### 1.5 Stats Accuracy

- API returns 835M permits — use this honest number
- Hero text should say "835M+" not "960M+"
- Update any marketing copy that claims 960M to match reality
- Once ANALYZE completes on T430 and indexes replicate, the number may increase

---

## Workstream 2: Domain & Infrastructure

### 2.1 Cloudflare Tunnel

**Prerequisites (user action):**
1. Create free Cloudflare account at cloudflare.com
2. Add `ecbtx.com` as a site
3. Change GoDaddy nameservers to Cloudflare's (Cloudflare provides the values)

**Implementation (automated):**
1. Install `cloudflared` on R730-2: `sudo dnf install -y cloudflared`
2. Authenticate: `cloudflared tunnel login`
3. Create tunnel: `cloudflared tunnel create permitlookup`
4. Configure tunnel: `permits.ecbtx.com` → `http://localhost:8080`
5. Add CNAME record in Cloudflare: `permits` → `<tunnel-id>.cfargotunnel.com`
6. Install as systemd service: `cloudflared service install`
7. Enable Cloudflare proxy (orange cloud) for CDN + DDoS protection

**Result:** `https://permits.ecbtx.com` works globally with proper TLS, CDN caching for static assets, and DDoS protection. Cost: $0.

### 2.2 Systemd Services

Three services that auto-start on boot and restart on crash:

**postgresql-permitlookup.service:**
```ini
[Unit]
Description=PostgreSQL for PermitLookup
After=network.target local-fs.target

[Service]
Type=forking
User=postgres
ExecStart=/usr/bin/pg_ctl start -D /mnt/ssd/pgdata -l /mnt/ssd/pgdata/logfile
ExecStop=/usr/bin/pg_ctl stop -D /mnt/ssd/pgdata
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**permitlookup-api.service:**
```ini
[Unit]
Description=PermitLookup API
After=postgresql-permitlookup.service
Requires=postgresql-permitlookup.service

[Service]
Type=simple
User=will
WorkingDirectory=/home/will/permit-api
ExecStart=/usr/bin/python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --workers 4
Restart=always
RestartSec=5
Environment=PATH=/home/will/.local/bin:/usr/bin

[Install]
WantedBy=multi-user.target
```

**cloudflared.service:** Installed automatically by `cloudflared service install`.

### 2.3 SAS Drive Boot Migration

Already cloned, UUIDs fixed, fstab updated. Remaining:
1. Test boot via F11 → "Integrated RAID Controller 1: Fedora"
2. If successful, change permanent boot order in BIOS
3. SD card becomes backup boot device
4. Root partition on 1.1TB SAS drive (vs 27GB SD card)

### 2.4 Frontend Deployment Flow

Keep it simple — no separate build step:
1. Edit `index.html` (locally or on R730-2)
2. Commit + push to GitHub
3. On R730-2: `git pull && sudo systemctl restart permitlookup-api`
4. Bump service worker cache version to force browser refresh

### 2.5 Frontend API Base

- When served from `permits.ecbtx.com` (via Cloudflare Tunnel → localhost:8080): `const API = ''` (relative URLs work)
- Revert from the Tailscale absolute URL back to relative `''`
- CORS origins: keep both `permits.ecbtx.com` and `r730-2-1.tailad2d5f.ts.net`

---

## Workstream 3: Sales-Ready

### 3.1 Pricing Simplification

**Current (6 tiers, confusing):** Free / Explorer / Pro Leads / Real-Time / Enterprise / Intelligence

**New (4 tiers, clear):**

| Tier | Price | Lookups/Day | Target | CTA |
|------|-------|-------------|--------|-----|
| Free | $0 | 10 | Trial | "Start Free" |
| Pro | $79/mo | 50 | Solo contractors, individual agents | Stripe checkout |
| Business | $249/mo | 250 | Teams, agencies | Stripe checkout |
| Enterprise | Custom | Unlimited | Proptech, carriers | "Contact Sales" form |

**Implementation:**
- Update pricing page HTML
- Update Stripe product/price IDs
- Update backend `PlanTier` enum and `stripe_service.py` limits
- Update rate limiting thresholds

### 3.2 Insurance Agent Cold Email

**Phase 1 — Setup (Days 7-14):**
- Verify email list: query database for insurance agent contacts, count by state
- Export TX segment (target: 5,000-10,000 agents)
- Register 3 sending domains (e.g., permitdata.io, buildingpermits.co, propertyalerts.io)
- Set up Instantly.ai ($97/mo) for warm-up + sending
- Write 3-email sequence (hook → value → close)

**Phase 2 — TX Test (Days 14-21):**
- Send 500 emails to TX agents
- Track: open rate (target >40%), reply rate (target >3%)
- Book demos from replies

**Phase 3 — Optimize (Days 21-30):**
- A/B test subject lines
- Refine sequence based on reply patterns
- Scale to 1,000 emails

**Phase 4 — Scale (Days 30-90):**
- Expand to FL, CA, NC
- Ramp to 5,000-20,000 emails
- Goal: 20+ paying customers

**Email Sequence:**

Email 1 (Day 0) — The Hook:
> Subject: "7 new building permits filed in [COUNTY] this week"
> Quick stat about permit activity. CTA: "See permits in your area (free)"

Email 2 (Day 3) — The Value:
> Subject: "Re: [COUNTY] permits"
> Specific example address + permit. CTA: "Try it free for 7 days"

Email 3 (Day 7) — The Close:
> Subject: "Quick question"
> Soft ask for 10-min demo call.

### 3.3 Pilot Roofer Conversion

| Action | Timing |
|--------|--------|
| Call roofer, ask for product feedback | Day 1 |
| Offer: $49/mo for 3 months if signs up today | Day 1 |
| Ask for 2-min Loom testimonial in exchange | Day 1 |
| Add testimonial to homepage | Day 2 |

### 3.4 Signup & Onboarding

**Current flow:** Signup → API key shown → done.

**New flow:**
1. Click "Start Free Trial" on pricing → signup form
2. After signup → redirect to welcome page
3. Welcome page: "3 things to try" (search your city, try AI Analyst, view a property report)
4. Day 1 email: Welcome + quick start guide
5. Day 3 email: "Did you try searching permits?"
6. Day 7 email: "Your trial ends in 3 days — upgrade to keep access"

**Implementation:**
- Add welcome page section to `index.html`
- Add 3 SendGrid email templates (welcome, nudge, expiry warning)
- Add `trial_ends_at` field to ApiUser model (14 days from signup)
- Cron job or scheduled task to send drip emails

### 3.5 Social Proof

| Asset | When | Where |
|-------|------|-------|
| Roofer testimonial quote | Day 2 | Homepage below hero |
| "835M+ permits · 3,143 jurisdictions" badge | Day 1 | Hero section (already exists) |
| Sample property report PDF | Day 7 | Lead magnet in cold email |
| "How insurance agents use permit data" blog post | Day 14 | SEO + cold email link |
| Data freshness dashboard (public) | Day 21 | Footer link, credibility signal |

---

## Success Metrics

| Milestone | Target | By When |
|-----------|--------|---------|
| Demo flow: zero dead ends | All 10 nav links work | Day 3 |
| permits.ecbtx.com loads with TLS | Domain working | Day 7 |
| Services auto-start on reboot | systemd enabled | Day 3 |
| First paying customer (roofer) | $49/mo | Day 7 |
| First cold email batch | 500 emails sent | Day 14 |
| First insurance agent customer | $79/mo | Day 21 |
| 10 paying customers | ~$1K MRR | Day 45 |
| 20+ paying customers | $3K+ MRR | Day 90 |

---

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Cold email low response (<2%) | Medium | High | A/B test, target specific counties with high permit activity |
| Domain setup delayed (user action needed) | Medium | High | Use Tailscale URL for demos until Cloudflare is ready |
| Infrastructure fails (power outage) | Medium | Critical | Systemd auto-restart + Cloudflare caching for static |
| Roofer declines to pay | Low-Medium | Medium | Offer 3 months at $49 or find second pilot from permit data |
| Data accuracy questioned | Medium | High | Publish freshness dates per jurisdiction on coverage page |
| TCPA liability from dialer/cold email | Low | Critical | CAN-SPAM compliance in all emails, no auto-dialing |

---

## Out of Scope (Re-add Later)

These features exist in code but are hidden until they're polished:
- Alerts system (needs real-time permit webhooks)
- Leads management page
- Dialer with live calls
- CRM full workflow
- Market Momentum / Trends
- Owner Intelligence
- Property Compare
- Jurisdiction Intel
- Property Passport

Each gets its own spec → plan → implementation cycle when ready.

---

## Technical Notes

- **Frontend:** Single 15K-line vanilla JS `index.html`. All changes are direct edits.
- **Backend:** FastAPI with 29 router modules in `/app/api/v1/`. 179 endpoints total.
- **Database:** PostgreSQL 16, primary on T430 (writes), replica on R730-2 (reads).
- **Permits table:** 835M rows, partitioned by `LIST(state_code)`. Indexes building on T430.
- **Auth:** API key in `X-API-Key` header, SHA256 hashed in DB.
- **Email:** SendGrid already configured, templates need updating.
- **Payments:** Stripe, needs product/price IDs for new tier structure.
