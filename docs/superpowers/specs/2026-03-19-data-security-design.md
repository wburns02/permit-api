# Data Security & Anti-Scraping Protection — Design Spec

## Problem

PermitLookup has 800M+ records across 12 data layers. The data IS the product value. Without protection, a single API key holder could bulk-download everything and compete directly, or a customer could resell the data. Need layered defenses against both competitors scraping and customers reselling.

## Design: 5-Layer Defense

### Layer 1: Result Caps (prevent bulk download)

**Per-query hard cap:**
- Max 500 results per search endpoint, period
- Pagination capped at page 20 (page_size * 20 = max reachable)
- Applied via shared middleware that wraps all paginated endpoints

**Daily result cap by plan:**
| Plan | Daily Record Cap |
|------|-----------------|
| Free | 500 |
| Explorer | 2,000 |
| Pro Leads | 10,000 |
| Real-Time | 25,000 |
| Enterprise | 50,000 |
| Intelligence | 100,000 |

Tracked in Redis (or in-memory fallback) alongside existing rate limits. Separate counter: `resultcap:{user_id}:{date}`. Each response increments by the number of records returned.

**Implementation:** New middleware function `check_result_cap(request, result_count)` called AFTER query execution but BEFORE response. If over cap, truncate results and add `"capped": true` to response.

### Layer 2: Row-Level Fingerprinting (trace leaked data)

**Fingerprint seed:** Each API key gets a deterministic seed derived from `sha256(api_key_id + secret_salt)`. The salt is stored as an env var, never in code.

**Numeric field perturbation:**
- Valuation/price fields: round to nearest $100 then add +(seed % 99) dollars. E.g., $485,000 becomes $485,037 for one key, $485,072 for another.
- Lat/lng coordinates: offset by (seed % 50) * 0.00001 degrees (~1 meter). Invisible to users, detectable forensically.
- Dates (non-primary): offset by (seed % 3) - 1 days on fields like `scraped_at`, `last_updated`. Never touch `issue_date` or `sale_date`.

**Text field markers:**
- Insert zero-width Unicode characters (U+200B, U+200C, U+200D, U+FEFF) at deterministic positions in `description` and `address` fields
- Position pattern encodes the key ID

**Trace endpoint:** `GET /v1/admin/trace` — paste a JSON record, it reverse-engineers the fingerprint to identify the source API key. Admin-only.

**Implementation:** New service `app/services/fingerprint.py` with `apply_fingerprint(records, api_key_id)` called in each endpoint before returning results.

### Layer 3: Abuse Detection (catch scrapers)

**Signals tracked per API key (rolling 1-hour window):**

| Signal | Description | Points |
|--------|-------------|--------|
| High utilization | >80% of daily limit used | +10 |
| Sequential pagination | Pages 1,2,3,4,5... in order | +20 |
| Fast requests | <1s average between requests, sustained 10+ min | +25 |
| Geographic sweeping | >50 unique ZIPs queried in 1 hour | +15 |
| Systematic coverage | Queries span >10 states in 1 hour | +15 |
| Consistent timing | Coefficient of variation of inter-request time <0.1 (bot-like regularity) | +20 |

**Thresholds:**
- Score 0-30: Normal usage
- Score 31-50: Elevated monitoring (log extra detail)
- Score 51-70: Shadow-throttle activated
- Score 71+: Shadow-throttle + admin alert

**Implementation:** New service `app/services/abuse_detector.py`. Scores computed from Redis counters updated on each request. Called from rate_limit middleware.

### Layer 4: Shadow-Throttle Response (punish without alerting)

When abuse score exceeds 50:

1. **Degrade data quality:**
   - Return only 50% of available fields (drop description, contractor details, coordinates)
   - Only return records older than 180 days (force COLD data regardless of plan)
   - Reduce result count to max 10 per page

2. **Add artificial latency:**
   - Score 51-70: add 2s delay per request
   - Score 71+: add 5s delay per request

3. **Alert admin:**
   - Email notification with key ID, abuse score, signal breakdown, sample queries
   - Log to `abuse_alerts` table for dashboard visibility

4. **Admin actions (via dashboard):**
   - View abuse alerts
   - Override: clear shadow-throttle (false positive)
   - Escalate: revoke API key immediately
   - Blacklist: block by IP range

**Implementation:** Shadow-throttle state stored in Redis per API key. Checked in middleware before query execution. Degradation applied in fingerprint service (it already touches the response).

### Layer 5: Enhanced Access Logging

Extend existing `UsageLog` table with:

| New Column | Type | Description |
|-----------|------|-------------|
| result_count | INTEGER | Number of records returned |
| response_bytes | INTEGER | Response size in bytes |
| query_hash | VARCHAR(64) | SHA256 of search parameters (detect sweeping) |
| abuse_score | INTEGER | Abuse score at time of request |

This enables forensic analysis: "show me all queries from key X in the last 24 hours, sorted by result_count."

## File Structure

```
app/
  middleware/
    rate_limit.py          # Extended: result caps + abuse score check
    result_caps.py         # NEW: daily result cap tracking
  services/
    fingerprint.py         # NEW: row-level fingerprinting + trace
    abuse_detector.py      # NEW: abuse scoring engine
    shadow_throttle.py     # NEW: degradation + latency injection
  api/v1/
    admin.py               # Extended: abuse alerts dashboard + trace endpoint
```

## Implementation Order

1. Result caps middleware (Layer 1) — immediate protection
2. Abuse detection engine (Layer 3) — start monitoring
3. Shadow-throttle (Layer 4) — respond to detected abuse
4. Row-level fingerprinting (Layer 2) — trace leaked data
5. Enhanced logging (Layer 5) — forensics
6. Admin dashboard extensions — visibility

## Non-Goals

- DRM or encryption of responses (breaks API usability)
- IP-based blocking as primary defense (easily circumvented with proxies)
- Legal/contractual terms (important but out of scope for this technical spec)
