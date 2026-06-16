# DNS Cutover Runbook — permits.ecbtx.com

**Current topology (as of 2026-06-15):**
- `permits.ecbtx.com` CNAME → Railway (PUBLIC PRIMARY)
- R730 serves identical `main` via its cloudflared tunnel (WARM STANDBY)
- T430 Postgres at home (`pg.ecbtx.com` / Tailscale `100.122.216.15:5432`) is the SPOF — both nodes read from it

**RTO target:** ≤15 minutes, both directions.

---

## Pre-flight checklist

Before executing any flip:

- [ ] Confirm the T430 Postgres is healthy on both paths:
  ```
  psql postgresql://will@pg.ecbtx.com:5432/permits -c "SELECT 1"
  psql postgresql://will@100.122.216.15:5432/permits -c "SELECT 1"
  ```
- [ ] Confirm the destination node is serving authed traffic:
  ```
  curl -s https://<destination-host>/health
  curl -s -H "X-API-Key: $TEST_API_KEY" https://<destination-host>/v1/permits/search?state=TX
  ```
  Expected: `/health` → 200, authed search → 200 (not 401/502)
- [ ] Record the current DNS value:
  ```
  dig permits.ecbtx.com CNAME +short
  ```
- [ ] Note the start time for RTO measurement.
- [ ] Notify any active users if this is a planned maintenance window.

---

## Required credentials

- **Cloudflare API token:** stored in `~/.secrets/cloudflare_dns_token` on the R730,
  or in the `CF_API_TOKEN` environment variable.  The token must have **Zone → DNS → Edit**
  scope on the `ecbtx.com` zone.
- **Zone ID:** found in Cloudflare dashboard → ecbtx.com → Overview → Zone ID
  (set `ZONE_ID` below before running commands)
- **Record ID:** the ID of the `permits` CNAME record (one-time lookup below)

---

## One-time: look up the CNAME record ID

```bash
export CF_API_TOKEN="$(cat ~/.secrets/cloudflare_dns_token)"
export ZONE_ID="<ecbtx.com zone id from dashboard>"

# List DNS records — find the 'permits' CNAME
curl -s -X GET \
  "https://api.cloudflare.com/client/v4/zones/${ZONE_ID}/dns_records?type=CNAME&name=permits.ecbtx.com" \
  -H "Authorization: Bearer ${CF_API_TOKEN}" \
  -H "Content-Type: application/json" \
  | python3 -m json.tool

# Note the "id" field — this is the RECORD_ID used below
export RECORD_ID="<record id from above>"
```

---

## Flip: Railway → R730 standby (primary fails)

```bash
export CF_API_TOKEN="$(cat ~/.secrets/cloudflare_dns_token)"
export ZONE_ID="<ecbtx.com zone id>"
export RECORD_ID="<permits CNAME record id>"
# R730's cloudflared tunnel public hostname (e.g. r730.tunnel.ecbtx.com)
export R730_TUNNEL_HOST="<r730-cloudflared-hostname>"

curl -s -X PATCH \
  "https://api.cloudflare.com/client/v4/zones/${ZONE_ID}/dns_records/${RECORD_ID}" \
  -H "Authorization: Bearer ${CF_API_TOKEN}" \
  -H "Content-Type: application/json" \
  --data "{\"content\": \"${R730_TUNNEL_HOST}\", \"proxied\": true}" \
  | python3 -m json.tool
```

Wait ~30s for DNS propagation, then verify:

```bash
dig permits.ecbtx.com CNAME +short
curl -s https://permits.ecbtx.com/health
curl -s -H "X-API-Key: $TEST_API_KEY" \
     "https://permits.ecbtx.com/v1/permits/search?state=TX&limit=1"
```

Expected: `/health` 200, authed search 200. Record the timestamp — subtract start time for RTO.

---

## Fail-back: R730 → Railway (restore primary)

```bash
export CF_API_TOKEN="$(cat ~/.secrets/cloudflare_dns_token)"
export ZONE_ID="<ecbtx.com zone id>"
export RECORD_ID="<permits CNAME record id>"
# Railway's assigned hostname (from Railway dashboard → permits-api → domain)
export RAILWAY_HOST="<railway-app>.up.railway.app"

curl -s -X PATCH \
  "https://api.cloudflare.com/client/v4/zones/${ZONE_ID}/dns_records/${RECORD_ID}" \
  -H "Authorization: Bearer ${CF_API_TOKEN}" \
  -H "Content-Type: application/json" \
  --data "{\"content\": \"${RAILWAY_HOST}\", \"proxied\": true}" \
  | python3 -m json.tool
```

Verify:

```bash
dig permits.ecbtx.com CNAME +short
curl -s https://permits.ecbtx.com/health
curl -s -H "X-API-Key: $TEST_API_KEY" \
     "https://permits.ecbtx.com/v1/permits/search?state=TX&limit=1"
```

Record the timestamp — subtract fail-back start time for fail-back RTO.

---

## Verify authed traffic after flip

A flip is only complete when authed traffic works against the T430:

```bash
# Replace $TEST_API_KEY with a valid Explorer+ key
curl -s -w "\nHTTP %{http_code}\n" \
  -H "X-API-Key: $TEST_API_KEY" \
  "https://permits.ecbtx.com/v1/permits/search?state=TX&city=Austin&limit=5"
```

Expected: HTTP 200 with JSON results (not 401 = key invalid, not 502 = DB unreachable).

---

## Drill log template

```
DRILL DATE: ____
OPERATOR:   ____
START:      ____

Phase 1 — pre-flight
  [ ] T430 healthy via pg.ecbtx.com
  [ ] T430 healthy via Tailscale
  [ ] destination node serves authed traffic
  [ ] current DNS recorded: ____

Phase 2 — flip to R730
  Flip issued at: ____
  Verify authed at: ____
  RTO (forward): ____  [target ≤15 min]

Phase 3 — fail-back to Railway
  Fail-back issued at: ____
  Verify authed at: ____
  RTO (return): ____   [target ≤15 min]

Result: PASS / FAIL
Notes: ____
```

---

## Investigation items (confirm before first drill)

1. Confirm Cloudflare token scope covers DNS Edit on ecbtx.com: run the one-time lookup above; a 403 means scope is insufficient.
2. Confirm R730 tunnel hostname by checking `cloudflared tunnel list` on the R730.
3. Confirm Railway app hostname from Railway dashboard → permits-api service → Domains.
4. Measure Railway↔T430 latency under load on both paths (`pg.ecbtx.com` and Tailscale) before the drill so you have a baseline.
5. Check what else on the R730 is customer-visible (e.g. other services behind the same cloudflared tunnel) and whether a DNS flip would affect them.

---

## Owner/orchestrator gates

- **Do NOT execute the flip autonomously.** This runbook is for human or orchestrator execution during a planned drill or real incident.
- **Off-peak only** for planned drills (Sunday 02:00–04:00 CT recommended).
- Produce a Fable evidence packet for the failover drill with before/after RTO measurements.
