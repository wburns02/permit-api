# PLAN.md: permits.ecbtx.com to A, Then Sell Hail Leads

Planning session: Fable 5, 2026-06-11. Offer decision supplied by owner in the question round.
Execution: Opus orchestrator in Claude Code, Sonnet subagents for mechanical work, CI plus automated checks verify, Fable evidence packets only at the five named risk points. No strategic decision remains downstream of this document.

---

## 0. Executive Summary

The asset is an A-grade data layer ($3.18B rows catalogued, 631 scrapers, enrichment gated at 98.16%) wearing a C+ business. The campaign sells **hail lead lists to Texas roofers**, chosen because it has the shortest path to campaign-ready (the `hail_leads` MV is live and needs only an API and a refresh cron), the clearest buyer with built-in storm urgency, and zero conflict with the owner's Mac Septic operations.

**Launch-blocking (campaign gate cannot open without these):**
1. Prod off the feature branch, CI gate live, Railway and prod serving identical code (Phase 0)
2. UPS installed, backups with a passed restore drill, failover drill passed with measured RTO (Phase 2)
3. Security list closed: route auth sweep across all 196 routes, pinned Dockerfile binaries, secrets audit (Phase 1)
4. Load test passed at campaign-projected volumes with Postgres OOM provoked and bounded (Phase 3)
5. Hail leads API live with scheduled refresh and data QA passed (Phase 4)
6. Monetization journey green end to end against Stripe test mode (Phase 5)
7. Funnel instrumented: source attribution, key activation, conversion events (Phase 5)
8. Deliverability ready: dedicated sending domain warmed, SPF/DKIM/DMARC verified (starts Phase 0, the long pole)

**Hardened in parallel with early customers (not gate-blocking):**
- Septic skiptrace enrichment (fast-follow offer, owner cost gate)
- Standby cutover automation beyond the manual runbook
- pgbouncer, offsite backup expansion, dependency automation
- Self-serve developer API as a marketed product
- /docs rate-limit tuning beyond initial limits

**Critical sequencing insight:** email domain warmup takes 2 to 4 weeks of calendar time. The sending domain is purchased and warmup begins in Phase 0, concurrent with all engineering, so deliverability is never the blocker when the gates open.

---

## 1. Offer Decision

**Decided: hail lead lists to roofers.** Owner confirmed 2026-06-11.

Per-offer analysis that informed the decision:

| Offer | Effort to campaign-ready | Funnel | Conflict | Verdict |
|---|---|---|---|---|
| Hail leads to roofers | Small: MV live, needs API + cron + QA | List sale or subscription, short cycle, storm urgency | None | **Selected** |
| Septic leads to septic cos | Medium: 517K candidates have ~0 phones, skiptrace spend required (~$0.07 to $0.15/record, owner gate) | List sale, owner knows buyer cold | Brushes against owner's own TX septic territory | Fast-follow |
| Self-serve permit API | Large: metering/billing state unknown, longest funnel | Developer self-serve, cold email converts poorly | None | Deferred, plumbing built anyway in Phase 5 |

The Phase 5 monetization plumbing (signup, keys, metering, Stripe) is common foundation: it serves hail leads first and both other offers later without rework. Texas does not license roofing contractors, so the buyer list comes from re-roof permits naming contractors, RCAT membership, and web scraping, not TDLR (investigation item, Phase 6).

---

## 2. Architecture Decision

**AMENDED 2026-06-11 during Phase 0.** Discovery: permits.ecbtx.com DNS is a CNAME to Railway. Railway has been the public-serving primary all along (and is why the API survived both power outages); the R730 is the warm spare. The decision below flips accordingly: **Railway stays primary serving; the R730 becomes the drilled standby** (cutover = DNS flip to the existing cloudflared tunnel hostname via the Cloudflare API). Everything else stands, and the stakes sharpen in one place: the T430 Postgres at home serves Railway through the pg.ecbtx.com tunnel, so home power and that tunnel remain the true SPOF. The UPS and the DB bounding work in Phase 2 are now THE resilience items, and the failover drill becomes: kill the DB path or the R730, measure what breaks, flip what's flippable.

Original decision text follows for the record; read R730/Railway roles as swapped.

**Decided: R730 stays primary serving; Railway becomes a real, drilled hot standby; the data moat and GPU enrichment stay home.**

Rationale: the 617GB permits DB on the T430 has data gravity (serving from Railway would push every query through one tunnel, which is the same SPOF being eliminated). Owner's standing preference is self-hosted with engineering time as the currency. The residential-power risk is bounded with a UPS and a tested cutover, not solved with a hosting migration.

Components:
- **Serving:** R730 uvicorn :8080 behind cloudflared, unchanged. Enrichment GPU workload stays power-capped at 150W per card; enrichment is paused during campaign week one (cheap insurance, owner already accepts pause/resume).
- **Standby:** Railway deployment runs identical code to prod (guaranteed by Phase 0 branch reconciliation), reaches the T430 over pg.ecbtx.com cloudflared primary and Tailscale fallback. Cutover is a DNS flip via the Cloudflare API (token already provisioned). Failover drill is the acceptance test: stop permit-api on R730, flip, measure. **RTO target: 15 minutes. RPO: 24h for permit data (nightly refresh cadence anyway), 1h for customer/billing tables (hourly dump of users, keys, usage, invoices).**
- **Database:** T430 stays primary. OOM bounding: app-side pool caps, statement_timeout verified at 20s, work_mem ceiling, bulk-load serialization encoded in cron schedules. pgbouncer is parallel-hardening, not gate-blocking.
- **Backups:** nightly dump of serving-critical tables to /dataPool plus hourly billing-table dumps; restore drill into a scratch instance is the acceptance test, not the cron existing. Offsite copy (Backblaze B2, roughly $6/TB/month) is an owner gate.
- **Power:** UPS on the R730 + T430 + network gear. Owner purchase gate: roughly $250 to $450 for a 1500VA line-interactive unit with USB, plus NUT for clean shutdown. Two outages in recent memory make this the single cheapest resilience dollar available.

---

## 3. Phase Sequence

Phases execute in order; Phase 0's deliverability task runs concurrently with everything.

### Phase 0: Cheap Wins and the Long Pole (2 to 3 days engineering)

**Objective:** Deploy/ops to B+, the cheapest security and resilience items closed, domain warmup started, grading rubric codified.

**Targets (from audit evidence):**
- Prod runs `feature/burns-l4-emit-soc-api` merged with main (commit 2069b92) by manual merge and restart; Railway auto-deploys `main`. Reconcile: merge the feature branch into main with the BURNS_L4_* flags default OFF, prod and Railway both serve main.
- Stray junk in prod working tree: `.premerge-20260611/`, `app/api/v1/analyst.py.bak.20260515-1553`. Remove.
- No CI on the repo. Add GitHub Actions: pytest against a **Postgres service container, not SQLite** (green SQLite is known to mask prod Postgres failures: enum vs varchar, UUID coercion), plus lint, plus a post-deploy prod smoke hitting /health and one authed endpoint.
- Deploy path becomes: push to main, R730 pulls and restarts via systemd timer or webhook, Railway auto-deploys, smoke verifies both.
- Dockerfile: pin Tailscale install to a versioned package with checksum (replace curl pipe sh), pin cloudflared to a versioned release with sha256 verification, add non-root user if the service tolerates it.
- UPS: owner purchase gate, cost stated above.
- Sending domain: owner purchase gate (~$12/yr, product-named .com recommended; do not send from ecbtx.com). Configure SPF/DKIM/DMARC on Resend (existing stack), begin warmup schedule immediately.
- Codify the grading rubric into `docs/pipeline/RUBRIC.md` (the seven components, what evidence earns each letter), so the Phase 6 re-grade is mechanical rather than vibes against vibes.

**Decisions already made:** main becomes the single deployed branch; L4 flags stay off; CI must use real Postgres; sending domain is dedicated and separate from ecbtx.com.

**Investigation checklist for builders:** confirm what diverges between the feature branch and main beyond the L4 emitters; inventory systemd unit and env files on the R730 before touching them; confirm Railway env parity; confirm the Tailscale binary is actually needed inside the container.

**Routing:** Sonnet for CI yaml, Dockerfile pins, junk cleanup, rubric drafting. Opus for the branch reconciliation itself. Owner for the two purchases.

**Dependencies:** none. This phase starts everything.

**Completion promise:** prod serves main at the same commit Railway serves; CI blocks a deliberately broken PR; smoke passes post-deploy; Dockerfile contains zero unpinned fetches; warmup is sending.

**Acceptance checklist (evidence only):** `git rev-parse HEAD` on R730 equals Railway's deployed SHA; a red-test PR shows a blocked merge; CI logs show Postgres service container; Dockerfile diff shows pins with checksums; Resend dashboard shows warmup activity; RUBRIC.md exists and covers all seven components.

**Verification tier:** Fable evidence packet for the **prod branch migration** (before/after unit files, SHAs, smoke output). CI plus orchestrator review for the rest.

### Phase 1: Security Close-Out (B- to A) (about 2 days)

**Objective:** the complete security list, each item with a verification condition, nothing left implicit.

**Targets:**
- Route auth sweep: script enumerates all 196 routes from openapi.json, probes each unauthenticated, asserts 401/403/405 everywhere except a named allowlist (/health, /docs, /openapi.json, /v1/login, /v1/signup, /v1/contact, /v1/demo). The sweep becomes a permanent CI job, not a one-time audit.
- /docs decision, made deliberately: **stays public as a sales asset** for the API product, with rate limiting on /docs and /openapi.json.
- Rate limiting on auth endpoints (login, signup) against brute force; per-key global limits (this doubles as metering groundwork for Phase 5).
- Secrets audit: gitleaks over full history; anything found goes to an owner gate for rotation (credential work is owner-only).
- Dependency pinning: lock file with hashes; renovate or dependabot for ongoing updates (automation part is parallel-hardening).

**Decisions already made:** docs stay public, rate-limited.

**Investigation checklist:** which routes currently lack auth by design vs by accident; whether any route returns data under 405 verbs; existing rate-limit middleware if any.

**Routing:** Sonnet for the sweep script, pins, rate-limit middleware. Opus reviews the allowlist. Owner for any credential rotation.

**Dependencies:** Phase 0 CI (the sweep lands as a CI job).

**Completion promise:** sweep green across 196 routes in CI; gitleaks clean or rotations done; rate limits return 429 under test.

**Acceptance checklist:** CI run showing the sweep job green with route count 196; gitleaks report; a scripted burst showing 429s on /v1/login.

**Verification tier:** CI plus orchestrator review.

### Phase 2: Resilience Architecture (D+ to A-) (4 to 5 days engineering, UPS shipping in parallel)

**Objective:** execute the Section 2 architecture: UPS, backups with restore drill, drilled Railway cutover.

**Targets:** everything in Section 2. Specifically: NUT configured for clean shutdown on both boxes; nightly serving-table dumps plus hourly billing-table dumps to /dataPool; restore drill into a scratch Postgres with row-count and checksum validation; Railway standby env parity confirmed; DNS cutover runbook written; **failover drill executed**: stop permit-api.service on the R730, flip DNS via Cloudflare API, verify Railway serves authed traffic against the T430, measure RTO, fail back, measure again.

**Decisions already made:** R730 primary, Railway standby, RTO 15 min, RPO 24h data / 1h billing, offsite is an owner gate.

**Investigation checklist:** Railway-to-T430 latency under load on both paths (pg.ecbtx.com and Tailscale); whether the Cloudflare token's scope covers the needed records; T430 disk headroom for scratch restores; what else on the R730 dies during a cutover and whether any of it is customer-visible.

**Routing:** Sonnet for backup scripts, NUT config, runbook drafting. Opus orchestrates the drill. Owner for UPS install and the offsite spend gate.

**Dependencies:** Phase 0 (code parity makes the standby real).

**Completion promise:** restore drill passed with validation; failover drill passed inside RTO target, both directions; UPS holds both boxes through a simulated outage.

**Acceptance checklist:** drill log with timestamps showing measured RTO; restore validation output; NUT test log showing clean shutdown; photo or syslog evidence of UPS carrying a pulled-plug test.

**Verification tier:** Fable evidence packets for the **architecture change** and the **failover drill**.

### Phase 3: Performance Load Test (B to A) (about 2 days)

**Objective:** fast under load, with the known OOM failure mode provoked deliberately and bounded.

**Targets:**
- Campaign volume math: tranche of 1,000 emails at realistic rates yields tens of signups and low hundreds of queries/day. 10x and 100x of that is still small in absolute terms, so the binding test is **concurrency and heavy queries**, not raw RPS: sustained 50 RPS authed search, 10 concurrent large exports, hail-leads list and filter patterns, run against staging or off-peak prod.
- Provoke the T430 OOM: concurrent heavy exports plus a bulk load, verify the bounds hold (pool caps, 20s statement_timeout, work_mem ceiling produce 429s/timeouts, never a killed backend).
- p95 targets: search under 2s, hail-leads list under 1s, export queue accepted under 500ms.

**Decisions already made:** test off-peak against prod plus staging; OOM gets provoked, not avoided.

**Investigation checklist:** current pool configuration; which endpoints do unbounded scans; whether export streams or buffers.

**Routing:** Sonnet builds k6/locust scripts; Opus designs scenarios and reads results.

**Dependencies:** Phase 2 bounds in place (otherwise the OOM test takes prod down).

**Completion promise:** all targets met; OOM provocation produces bounded failures only; results feed cost-per-query math for pricing.

**Acceptance checklist:** load test report with p95s; T430 logs showing zero OOM-killed backends during provocation; a written cost-per-query figure.

**Verification tier:** CI plus orchestrator review.

### Phase 4: Product Gap, Hail Leads (launch-blocking half) (2 to 3 days)

**Objective:** the thing the campaign sells exists as a product.

**Targets:**
- /v1/hail-leads endpoints: list and filter (county, date range, severity, permit linkage), CSV export, freshness endpoint. Auth required, metered like everything else.
- REFRESH MATERIALIZED VIEW CONCURRENTLY on a nightly cron sequenced after the storm-report loads (needs a unique index on the MV; verify or add).
- Data QA: spot-check MV rows against SPC source reports; **specifically check whether hail_leads inherits the state-default pollution defect recorded against hot_leads (LA parishes stamped TX)**; round-number result counts are treated as pagination-cap smells per standing practice.
- API docs for the new endpoints (public docs are the sales asset).

**Decisions already made:** hail leads ships through the existing API with the existing auth, not as a separate app.

**Investigation checklist:** MV definition and current row counts; storm-report load timing; whether the unique index exists; the hot_leads defect's blast radius.

**Routing:** Sonnet for endpoints, cron, docs. Opus for QA design.

**Dependencies:** Phase 0 CI (endpoints land with tests).

**Completion promise:** endpoints live and authed; refresh ran on schedule two consecutive nights; QA found zero state-pollution rows or the defect is fixed.

**Acceptance checklist:** authed curl transcripts; cron logs for two nights; QA report with sample-vs-source checks.

**Verification tier:** CI plus orchestrator review.

### Phase 5: Monetization Plumbing (F to C) (3 to 4 days)

**Objective:** a stranger with a credit card can become a metered, billed customer with no human in the loop, proven in test mode.

**Targets:**
- **Inventory first.** Verified to exist: signup, login, API key issuance and management routes, 401 enforcement. NOT verified: usage metering, tiering, billing. The first task inventories what is real and reports before building.
- Build what is missing: per-key usage metering (middleware counters into a Postgres table), tier limits with 429-on-exceed or overage, Stripe test-mode integration (checkout, webhooks, invoices).
- **The journey acceptance test:** a fake customer signs up, gets a key, hits the hail-leads API, exceeds the trial tier, gets metered, gets billed against a Stripe test-mode invoice. Every step evidenced.
- Funnel instrumentation: UTM source attribution on signup, key-activation event, conversion events into a queryable table.
- **Pricing decision: owner input required before this phase closes.** Orchestrator prepares the analysis: competitor scan (Construction Monitor at roughly $100 to $300/month/region, HBW, per-lead roofing sellers at roughly $20 to $100/lead) plus cost-per-query math from Phase 3.

**Decisions already made:** Stripe; trial tier exists; pricing numbers are owner-only.

**Investigation checklist:** what the signup flow actually writes; whether any metering scaffolding exists; webhook signature handling; where billing tables live (they join the 1h-RPO backup set from Phase 2).

**Routing:** Sonnet for metering middleware and events; Opus for Stripe architecture and the journey test design; owner for pricing.

**Dependencies:** Phase 4 (the journey runs against the hail-leads endpoints); Phase 1 rate-limit groundwork.

**Completion promise:** journey green end to end with evidence at every step; pricing decided and configured; funnel events flowing.

**Acceptance checklist:** journey transcript (signup response, key, 200s, 429 at tier, usage rows, Stripe test invoice ID); a funnel query returning the fake customer's full event trail.

**Verification tier:** Fable evidence packet for the **monetization journey**.

### Phase 6: Campaign Readiness and Campaign (1 week plus warmup already banked)

**Objective:** gates verified, re-grade passed, list built, copy drafted under the copy gate, staged sends under owner control.

**Entry gates (all verifiable, all must hold):**
1. Deploy/ops at B or better with CI live (Phase 0 evidence still green)
2. Resilience drill passed (Phase 2 RTO log)
3. Security list closed (Phase 1 sweep green in latest CI)
4. Load test passed at campaign-projected volumes (Phase 3 report)
5. Monetization journey green (Phase 5 packet)
6. Funnel instrumented (Phase 5 events queryable)
7. Deliverability: domain warmed (2 to 4 weeks elapsed since Phase 0), SPF/DKIM/DMARC verified
8. **Re-grade checkpoint:** rerun RUBRIC.md against measured reality. Projected grades validate or the gate stays shut.

**Copy gate (owner decision, encoded):** campaign copy drafting begins only when the re-grade shows **every non-monetization component at A**. Then Sonnet drafts sequences, owner reviews every word. Nothing customer-facing ships without the owner; every actual send is an owner button push.

**Campaign work:**
- List strategy: TX roofers from re-roof permits naming contractors, RCAT membership, targeted web scrape (TDLR is useless here; TX does not license roofers). Email enrichment method and any per-contact cost go to an owner gate.
- List hygiene: physical address, working unsubscribe, accurate headers, suppression list honored (table stakes for deliverability and CAN-SPAM).
- Sequence drafts post-copy-gate: storm-event hook, sample leads as proof, trial tier as the ask.
- Staged sends: 50, then 250, then 1,000. Tripwires in the risk register pause the schedule automatically.

**Routing:** Sonnet for list building and drafts; Opus for campaign orchestration; owner for enrichment spend, copy approval, and every send.

**Dependencies:** all prior phases.

**Completion promise:** first tranche sent by owner, funnel events flowing from real traffic, tripwires armed.

**Acceptance checklist:** gate evidence bundle (all eight items); re-grade table; suppression and unsubscribe tested; tranche-1 send log with bounce/complaint rates inside thresholds.

**Verification tier:** Fable evidence packet for the **campaign go gate**.

---

## 4. Monetization Journey (canonical spec)

Signup (UTM attributed) → email verify if present → API key issued → authed hail-leads queries succeed → trial tier exceeded → 429 or overage per pricing decision → usage rows correct → Stripe test-mode invoice generated and webhook-confirmed → funnel shows the full trail. One transcript, every arrow evidenced. This is the Phase 5 acceptance test and a permanent staging regression test thereafter.

---

## 5. Milestones with Grade Math

| Checkpoint | Deploy/ops | Resilience | Security | Perf | API | Data | Monetization | Overall |
|---|---|---|---|---|---|---|---|---|
| Today | C- | D+ | B- | B | B+ | A | F | C+ |
| M0: after Phase 0 | **B+** | C- (UPS) | B (pins) | B | B+ | A | F | B- |
| M1: after Phases 1+2 | B+ | **A-** (drills passed) | **A** | B | B+ | A | F | B |
| M2: after Phase 3 | B+ | A- | A | **A** | B+ | A | F | B+ |
| M3: after Phases 4+5 | B+ | A- | A | A | **A** (hail endpoints, docs, limits) | A | **C** (journey green, zero customers) | A- |
| M4: re-grade gate | A- (CI history) | A- | A | A | A | A | C | **A-** |
| M5: campaign live | A- | A- | A | A | A | A | **B then A** | A |

Monetization grade criteria, explicit: C = journey green, no customers. B = first paying customers. A = at least 10 paying customers or $1K MRR with monthly churn under 10%. Engineering cannot move it past C; only the campaign can.

Re-grade rule: M4 runs RUBRIC.md against measured evidence. Any component below projection blocks the campaign gate and routes per escalation.

---

## 6. Deferred Items

- **Septic skiptrace:** fast-follow offer. Tranche the spend: 5K records first (roughly $350 to $750) not the full 84K (roughly $6K to $13K). Owner gate. Not blocking the hail offer.
- **Self-serve developer API marketing:** plumbing ships in Phase 5; marketing it is a separate campaign after hail leads proves the funnel.
- **pgbouncer, renovate automation, offsite backup expansion, standby cutover automation:** parallel hardening once customers exist.
- **R730-2 revival:** offline; nothing in this plan needs it.
- **Burns L4 emitters:** stay flagged OFF; out of scope.

---

## 7. Risk Register

| # | Failure mode | Tripwire | Response |
|---|---|---|---|
| 1 | R730 dies during early customer traffic | External uptime monitor on /health (60s interval) alerts on 2 consecutive failures | Orchestrator executes the pre-authorized cutover runbook (drilled in Phase 2); owner informed, not awaited. Fail back only with owner go. |
| 2 | Prod branch migration breaks serving | Post-deploy smoke fails | Auto-rollback to the tagged pre-migration release (tag created before the migration; unit files and env snapshotted). Migration runs off-peak. Fable packet reviews before retry. |
| 3 | Campaign send outruns capacity or deliverability | Bounce rate over 2% or complaint rate over 0.1% in any tranche; or API p95 breach during a tranche | Sends pause automatically, owner notified. Next tranche requires owner go after cause analysis. |

---

## 8. Escalation Rules

- **Builder (Sonnet) retries:** mechanical failures, max 2 attempts, then up.
- **Orchestrator (Opus) decides:** within-phase tradeoffs, sequencing, anything covered by a decision already in this plan.
- **Owner decides:** money (UPS, domain, skiptrace, offsite, enrichment, pricing), credentials, every outward communication, every send, anything customer-visible.
- **New Fable planning session:** a phase's completion promise cannot be met as specified; the architecture decision is invalidated by evidence; or two consecutive Fable evidence packets reject the same phase.

**Standing constraints:** pricing and all outward communications are owner-only. Hardware and hosting spend are owner gates with monthly cost stated. Credential work is owner-only. Nothing renders or claims a number the system did not measure.

---

*Session ends with this document. No work begins here; no campaign copy exists here.*
