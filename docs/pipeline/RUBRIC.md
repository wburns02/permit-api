# RUBRIC.md: permits.ecbtx.com Re-Grade Rubric

Generated: 2026-06-11. Codified from the Phase 0 mandate in PLAN.md so the Phase 6
re-grade is mechanical, not vibes-against-vibes. Every grade level is a verifiable
condition: a command output, a CI result, a drill log, a measured number. No vibe
definitions.

Grades use A/B/C/D/F with + or - only where the PLAN.md milestone table calls for
them (A- on Resilience and Deploy/ops at the campaign gate). All other grades are
whole letters.

---

## 1. Data Asset

**Baseline (today): A**

| Grade | Verifiable condition |
|-------|----------------------|
| F | Scraper count below 100 OR total catalogued rows below 500M OR enrichment pipeline not running. Check: `SELECT COUNT(*) FROM jurisdictions WHERE record_count > 0;` plus catalog row count in HAVE.json. |
| C | Scrapers running and rows catalogued, but enrichment pipeline offline, accuracy unverified, or catalog outdated by more than 30 days. Check: last-modified timestamp on HAVE.json; enrichment worker status via `systemctl status enrichment-worker` (or equivalent). |
| B | 300 or more active scrapers, 2B or more catalogued rows, enrichment pipeline running with documented accuracy, catalog updated within 7 days. Check: scraper count in catalog, HAVE.json row total, enrichment pass rate in logs. |
| A | 600 or more active scrapers, 3B or more catalogued rows, enrichment cascade gating accuracy at 95 percent or above (documented in a run log), catalog updated within 7 days. Check: `grep "active_sources" docs/scraper-dataset-catalog-*.md | tail -1`, HAVE.json total, enrichment log showing pass rate. Current audit baseline: 631 scrapers, 3.18B rows, 98.16 percent cascade gate. |

---

## 2. API Surface

**Baseline (today): B+**

| Grade | Verifiable condition |
|-------|----------------------|
| F | Fewer than 50 routes OR auth not enforced (unauthenticated requests return 200 on protected routes). Check: `curl -s https://permits.ecbtx.com/openapi.json | jq '[.paths | keys[]] | length'`; `curl -s -o /dev/null -w "%{http_code}" https://permits.ecbtx.com/v1/permits`. |
| C | Auth enforced on most routes but no API key management (issuance, revocation), no signup flow, or fewer than 100 routes. Check: POST /v1/signup returns 200 or 201; GET /v1/keys returns 200 with valid key; route count above 100. |
| B | Auth enforced and verified by automated sweep, API key issuance and management live, signup and demo flows working, 150 or more routes. Route auth sweep CI job exists. Check: CI run showing sweep job green; curl POST /v1/signup; curl POST /v1/demo. |
| A | All of the above PLUS: hail-leads endpoints live and authed (/v1/hail-leads list, filter, CSV export, freshness), public /docs with rate limiting returning 429 under a scripted burst, per-key usage metering writing rows to usage_logs or equivalent. Check: `curl -H "X-API-Key: $KEY" https://permits.ecbtx.com/v1/hail-leads` returns 200; burst script hits /docs 200 times and shows 429s; `SELECT COUNT(*) FROM usage_logs WHERE created_at > now() - interval '1 hour';` returns non-zero after authed requests. |

---

## 3. Performance

**Baseline (today): B**

| Grade | Verifiable condition |
|-------|----------------------|
| F | /health p50 above 5s OR any authed endpoint returns 500 under single-user load. Check: `time curl -s https://permits.ecbtx.com/health`; single authed search request. |
| C | /health responds under 2s, auth checks under 1s, but no load test ever run and no pool caps or statement_timeout configured. Check: spot timings; `SHOW statement_timeout;` on T430 returns something other than 0. |
| B | /health under 2s, auth checks under 0.5s, statement_timeout set to 20s, pool caps documented. No sustained concurrent load test performed. Current audit baseline: /health 0.9s, auth checks 0.2s. Check: `time curl` for both; `SHOW statement_timeout;` on T430 returns '20s'. |
| A | All Phase 3 load targets met under concurrent load: search p95 under 2s at sustained 50 RPS authed, hail-leads list p95 under 1s, export queue accepted under 500ms. T430 OOM provocation (concurrent heavy exports plus bulk load) produces only 429s or 20s timeouts, zero OOM-killed backends. Check: load test report (k6 or locust) with p95s; T430 postgres log showing zero `OOM killer` lines during provocation; written cost-per-query figure in the report. |

---

## 4. Deploy / Ops

**Baseline (today): C-**

| Grade | Verifiable condition |
|-------|----------------------|
| F | Prod is not running OR no deployment process exists OR prod and Railway serve different codebases with no reconciliation plan. Check: `curl -s https://permits.ecbtx.com/health`; compare `git rev-parse HEAD` on R730 to Railway deployed SHA. |
| C | Prod serves a known commit, Railway exists as a deployment target, but: prod runs a long-lived feature branch different from Railway, no CI, stray junk files in the working tree, or manual-only deploys with no automation. Current audit baseline. Check: `git log --oneline -1` on R730; list of .bak and .premerge files in working tree. |
| B | Prod and Railway serve identical code from main. CI runs on every push: pytest against a Postgres service container (not SQLite), lint, and a post-deploy smoke hitting /health and one authed endpoint. No stray .bak or .premerge files in the prod working tree. Dockerfile contains no unpinned curl-pipe-sh or unverified binary fetches. Check: `git diff origin/main HEAD` on R730 is empty; CI run showing Postgres service container in logs; `find . -name "*.bak" -o -name "*.premerge*"` returns nothing; Dockerfile pins with checksums present. |
| A- | All B conditions PLUS: CI has a continuous green history of at least 14 days (no red merges to main in that window), deploy path is fully automated (push to main triggers R730 pull-and-restart via systemd timer or webhook AND Railway auto-deploys), and a post-deploy smoke runs automatically on both targets after every deploy. Check: GitHub Actions run history for main over the past 14 days; systemd timer or webhook unit file on R730 confirming automation; CI logs showing smoke against both R730 and Railway after a recent deploy. |

---

## 5. Resilience

**Baseline (today): D+**

| Grade | Verifiable condition |
|-------|----------------------|
| F | No backup of any kind exists OR R730 has suffered data loss from a known failure mode with no remediation. Check: `ls -la /dataPool/permits-backups/` (or equivalent); recent outage post-mortem. |
| C | Nightly backup cron exists and is running, but: no restore drill ever performed, no UPS, no standby deployment, Railway environment does not reach the T430 database. Check: cron entry present; last backup file timestamp; `curl -f https://permits-railway.up.railway.app/health` returns 200 or 200-class. |
| B | Nightly backup running and verified by at least one successful restore drill (row-count and checksum validation documented). UPS installed and carrying both R730 and T430. Railway standby confirmed to reach T430 on both primary and fallback paths. DNS cutover runbook written and reviewed. Check: restore drill log with row counts; UPS install evidence (syslog or NUT status); `psql` from Railway container to T430 via pg.ecbtx.com returning a row count; runbook file present in docs/. |
| A- | All B conditions PLUS: failover drill executed in both directions with measured RTO at or under 15 minutes. Restore drill passes with checksum validation. UPS carries a pulled-plug test (documented in NUT log or syslog). Hourly billing-table dumps running to /dataPool (users, keys, usage_logs, invoices). Check: drill log showing stop of permit-api.service on R730, DNS flip timestamp, first successful authed request to Railway target, RTO delta at or under 15 minutes, fail-back timestamp; `nut-monitor` or syslog entry for power-outage simulation; cron entry for hourly billing dump with last-run timestamp. |

---

## 6. Security

**Baseline (today): B-**

| Grade | Verifiable condition |
|-------|----------------------|
| F | Unauthenticated requests return 200 on protected routes OR API keys stored in plaintext in the database OR secrets committed to git history. Check: `curl -s -o /dev/null -w "%{http_code}" https://permits.ecbtx.com/v1/permits` returns 401; `SELECT key_hash FROM api_keys LIMIT 1` does not return a raw key string; `gitleaks detect --source . --no-git` returns zero findings (or all findings rotated). |
| C | Auth enforced on most routes but: Dockerfile contains unpinned curl-pipe-sh installs, no gitleaks audit run, no rate limiting on auth endpoints, dependencies not pinned with hashes. Check: Dockerfile for `curl ... | sh` pattern; pip freeze or requirements.txt for pinned hashes; `curl -X POST https://permits.ecbtx.com/v1/login` 100 times in a loop returns 429 at some point. |
| B | Auth enforced on all routes outside the named allowlist (/health, /docs, /openapi.json, /v1/login, /v1/signup, /v1/contact, /v1/demo). Dockerfile pins all external binary fetches with checksums. Dependencies locked with hashes. gitleaks run with no unrotated findings. Rate limiting returns 429 on auth endpoints under a scripted burst. Check: route auth sweep output (all routes probed, 401/403/405 outside allowlist); Dockerfile diff showing sha256 pins; `pip-audit` or equivalent clean; gitleaks report; burst test showing 429. |
| A | All B conditions PLUS: route auth sweep runs as a CI job on every push (blocking merge if any route outside the allowlist returns 200 unauthenticated), rate limiting live on /docs and /openapi.json, per-key global limits enforced (429 on exceed), renovate or dependabot configured for ongoing dependency updates. Check: CI run showing the sweep job green with route count at 196 or above; a scripted burst against /docs returning 429; a key-exceeded test returning 429; renovate/dependabot config file present. |

---

## 7. Monetization

**Baseline (today): F**

These grade criteria are verbatim from PLAN.md section 5 (Milestones with Grade Math),
supplemented with the Phase 4 and 5 acceptance conditions for lower grades.

| Grade | Verifiable condition |
|-------|----------------------|
| F | Signup, key issuance, or 401 enforcement not working end to end. OR usage metering writes zero rows to the database for authed requests. Check: POST /v1/signup returns 201; subsequent authed request returns 200 not 401; `SELECT COUNT(*) FROM usage_logs WHERE created_at > now() - interval '10 minutes';` returns non-zero after a test request. |
| C | Monetization journey green end to end in Stripe test mode, zero paying customers. Journey definition (from PLAN.md section 4): signup (UTM attributed) to email verify to API key issued to authed hail-leads queries succeed to trial tier exceeded to 429 or overage to usage rows correct to Stripe test-mode invoice generated and webhook-confirmed to funnel showing the full trail. Evidence: journey transcript showing all steps with HTTP response codes and Stripe test invoice ID; `SELECT COUNT(*) FROM api_users WHERE stripe_customer_id IS NOT NULL;` returns zero or only test records; funnel query returning the fake customer's full event trail. |
| B | First paying customers (at least 1 live Stripe subscription in production mode, not test mode). Check: `SELECT COUNT(*) FROM api_users WHERE plan != 'free' AND stripe_subscription_id IS NOT NULL;` returns 1 or more; Stripe dashboard (production) shows at least 1 active subscription; no test-mode customer IDs in that set. |
| A | At least 10 paying customers OR $1,000 MRR, with monthly churn under 10 percent. Engineering cannot move it past C; only the campaign can. Check: `SELECT COUNT(*) FROM api_users WHERE plan != 'free' AND stripe_subscription_id IS NOT NULL;` returns 10 or more, OR Stripe dashboard MRR at or above $1,000; churn rate calculation: customers lost in the past 30 days divided by customers at start of period, under 0.10. |

---

## How to Run a Re-Grade

Execute these steps in order. Each step produces a piece of evidence; record the output
verbatim in the re-grade log. A trained agent can run this end to end in under an hour.

**Prerequisites:** SSH access to R730, read access to T430 Postgres (via pg.ecbtx.com),
GitHub Actions access, Railway CLI or dashboard.

---

**Step 1. Data Asset**

```bash
# Row totals and scraper count from catalog
grep -E "active_sources|total_rows" /home/will/permit-api/docs/scraper-dataset-catalog-*.md | tail -5
# HAVE.json row total
python3 -c "import json,pathlib; d=json.loads(pathlib.Path('/home/will/permit-api/docs/pipeline/HAVE.json').read_text()); print(sum(v for v in d.values() if isinstance(v,int)))" 2>/dev/null || echo "HAVE.json not at expected path, locate and sum manually"
# Enrichment pass rate (last run)
journalctl -u enrichment-worker --since "48 hours ago" | grep -E "pass_rate|accuracy|gated" | tail -5
```

Grade against: 600+ scrapers + 3B+ rows + 95%+ pass rate = A; adjust down by the table
in section 1.

---

**Step 2. API Surface**

```bash
# Route count
curl -sf https://permits.ecbtx.com/openapi.json | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(list(d['paths'].keys())))"
# Auth enforced: probe a protected route without a key
curl -s -o /dev/null -w "auth_check_http_code=%{http_code}\n" https://permits.ecbtx.com/v1/permits
# Hail-leads endpoint live (requires a valid test key in $TEST_KEY)
curl -s -o /dev/null -w "hail_leads_http_code=%{http_code}\n" -H "X-API-Key: $TEST_KEY" https://permits.ecbtx.com/v1/hail-leads
# Docs rate limit: 20 rapid requests, look for 429
for i in $(seq 1 20); do curl -s -o /dev/null -w "%{http_code} "; done <<< "" ; curl -s -o /dev/null -w "%{http_code}\n" https://permits.ecbtx.com/docs
# Usage metering: confirm rows written after the above requests
psql "$DB_URL" -c "SELECT COUNT(*) FROM usage_logs WHERE created_at > now() - interval '5 minutes';"
```

Grade against the table in section 2.

---

**Step 3. Performance**

```bash
# Health and auth spot timings
time curl -sf https://permits.ecbtx.com/health > /dev/null
time curl -sf -o /dev/null -w "%{http_code}\n" -H "X-API-Key: $TEST_KEY" https://permits.ecbtx.com/v1/permits?state=TX&limit=1
# statement_timeout
psql "$DB_URL" -c "SHOW statement_timeout;"
```

For an A grade, locate the most recent load test report in docs/pipeline/ or CI
artifacts. Verify p95 figures (search under 2s, hail-leads under 1s, export under 500ms)
and confirm the OOM provocation section shows zero `OOM killer` postgres log lines.

---

**Step 4. Deploy / Ops**

```bash
# SHA parity: R730 vs Railway
# On R730:
cd /home/will/permit-api-live && git rev-parse HEAD
# Railway deployed SHA (via CLI or dashboard):
railway status 2>/dev/null || echo "Check Railway dashboard for deployed SHA manually"
# Stray junk files in prod tree
find /home/will/permit-api-live -name "*.bak" -o -name "*.premerge*" | head -20
# Dockerfile: no unpinned fetches
grep -n "curl.*|.*sh\|wget.*|.*sh" /home/will/permit-api-live/Dockerfile
# CI: last 5 runs on main
gh run list --repo wburns02/permit-api --branch main --limit 5
```

For A-, confirm 14-day green history and automated deploy trigger (systemd timer or
webhook unit file).

---

**Step 5. Resilience**

```bash
# Backup freshness
ls -lt /dataPool/permits-backups/ | head -5
# NUT / UPS status
upsc <ups-name> 2>/dev/null || systemctl status nut-monitor 2>/dev/null || echo "NUT not configured"
# Railway-to-T430 connectivity (run from Railway shell or equivalent)
# psql "postgresql://permit_api@pg.ecbtx.com:5432/permits" -c "SELECT 1;" 2>&1
```

For A-, locate the failover drill log in docs/pipeline/drills/ or equivalent. Confirm:
stop timestamp, DNS flip timestamp, first 200 response timestamp, RTO delta at or under
15 minutes, fail-back timestamp. Confirm hourly billing dump cron:

```bash
crontab -l | grep billing
ls -lt /dataPool/billing-dumps/ | head -5
```

---

**Step 6. Security**

```bash
# Route auth sweep (CI job output preferred; manual fallback):
# The sweep script enumerates openapi.json routes and probes each unauthenticated.
# Locate it: find /home/will/permit-api-live -name "sweep*" -o -name "auth_sweep*"
# Run it or check the last CI artifact.
# gitleaks
gitleaks detect --source /home/will/permit-api-live --no-git 2>&1 | tail -10
# Rate limit on login (100 requests, look for 429):
for i in $(seq 1 100); do curl -s -o /dev/null -w "%{http_code} " -X POST https://permits.ecbtx.com/v1/login -d '{"email":"x@x.com","password":"x"}' -H "Content-Type: application/json"; done
# Dependency audit
cd /home/will/permit-api-live && pip-audit 2>&1 | tail -10
```

---

**Step 7. Monetization**

```bash
# Customer counts
psql "$DB_URL" -c "SELECT plan, COUNT(*) FROM api_users GROUP BY plan ORDER BY COUNT(*) DESC;"
psql "$DB_URL" -c "SELECT COUNT(*) FROM api_users WHERE plan != 'free' AND stripe_subscription_id IS NOT NULL;"
# Usage rows written
psql "$DB_URL" -c "SELECT COUNT(*) FROM usage_logs WHERE created_at > now() - interval '24 hours';"
# Stripe: open dashboard or use CLI
stripe customers list --limit 5 2>/dev/null || echo "Check Stripe dashboard manually"
```

For a C grade (journey green), locate the Phase 5 journey transcript in
docs/pipeline/evidence/ and confirm every arrow in the canonical sequence (PLAN.md
section 4) has a documented HTTP response or database row. For B and A, the Stripe
dashboard production mode is the ground truth: active subscription count and MRR.

---

**Step 8. Produce the re-grade table**

Paste the evidence outputs into a re-grade log file. Assign each component a grade using
the tables above. Compare to the projected grades in PLAN.md section 5. Any component
below projection blocks the campaign gate and routes per PLAN.md section 8 escalation
rules.

| Component | Projected at gate | Measured | Pass |
|-----------|-------------------|----------|------|
| Data Asset | A | (fill) | (Y/N) |
| API Surface | A | (fill) | (Y/N) |
| Performance | A | (fill) | (Y/N) |
| Deploy/Ops | A- | (fill) | (Y/N) |
| Resilience | A- | (fill) | (Y/N) |
| Security | A | (fill) | (Y/N) |
| Monetization | C | (fill) | (Y/N) |

All rows must show Y for the campaign gate to open.
