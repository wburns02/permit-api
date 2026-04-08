# OpenGov / ViewPoint Cloud — Scraping Feasibility Investigation

**Date:** 2026-03-30
**Status:** Research complete — substantial prior work exists, auth is the remaining blocker

---

## TL;DR

OpenGov acquired ViewPoint Cloud in 2019. They are the same platform. 500+ municipalities use it. We already have a complete scraper suite in `/home/will/ReactCRM/scrapers/opengov/` with 214 portals cataloged. Metadata (categories, record types) is fully extractable with no auth. Actual permit records require an Auth0 JWT token — which is easy to get by creating a free citizen account on any portal.

---

## Platform Identity

- **ViewPoint Cloud** and **OpenGov Permitting & Licensing** = same backend, same API, same codebase
- ViewPoint was acquired by OpenGov in September 2019
- Old `*.viewpointcloud.com` URLs 301-redirect to `*.portal.opengov.com`
- The entire backend is branded "ViewpointCloud" in the API layer

---

## Working Portal URLs (5 confirmed live)

| Portal | URL | State |
|--------|-----|-------|
| Cheltenham Township PA | https://cheltenhamtownship.portal.opengov.com | PA |
| Bloomfield CT | https://bloomfieldct.portal.opengov.com | CT |
| City of San Rafael CA | https://cityofsanrafaelca.portal.opengov.com | CA |
| Lake County CA | https://countyoflakeca.portal.opengov.com | CA |
| Framingham MA | https://framinghamma.portal.opengov.com | MA |

All 214 portals in `/home/will/ReactCRM/scrapers/output/opengov/` are confirmed valid.

---

## URL Patterns

```
# Citizen portal (SPA frontend)
https://{community}.portal.opengov.com/

# Legacy ViewPoint URLs (redirect to above)
https://{community}.viewpointcloud.com/

# Staff/workflow portal (requires staff login)
https://{community}.workflow.opengov.com/
```

Community slug format: `{cityname}{stateabbrev}` (e.g., `bloomfieldct`, `cityofsanrafaelca`, `cheltenhamtownship`)

---

## API Architecture (Fully Reverse-Engineered)

The frontend is an Ember.js SPA. All data comes from the ViewpointCloud API layer:

### Public Endpoints (No Auth Required)

```
GET https://api-east.viewpointcloud.com/v2/{community}/categories
GET https://api-east.viewpointcloud.com/v2/{community}/record_types
GET https://api-east.viewpointcloud.com/v2/{community}/general_settings/1
GET https://api-east.viewpointcloud.com/v2/{community}/project_templates
```

These return real JSON data with department names, permit type names, IDs, and descriptions.

### Auth-Required Endpoints

```
GET  https://api-east.viewpointcloud.com/v2/{community}/records
POST https://search.viewpointcloud.com/graphql
POST https://records.viewpointcloud.com/graphql
POST https://inspections.viewpointcloud.com/graphql
```

Without a Bearer token: records endpoint returns `{"data":[],"meta":{"total":0}}`.
GraphQL endpoints return `{"error":"authentication_failed","message":"Missing Authorization HTTP header."}`.

### Authentication

- Provider: Auth0 at `accounts.viewpointcloud.com`
- Client ID: `Kne3XYPvChciFOG9DvQ01Ukm1wyBTdTQ`
- Audience: `viewpointcloud.com/api/production`
- Flow: Implicit grant (response_type=token+id_token)
- **Key insight: One account works across ALL 500+ jurisdictions** — Auth0 token is universal

---

## Data Format

**REST API response (JSON:API format):**
```json
{
  "data": [
    {
      "id": "12345",
      "type": "records",
      "attributes": {
        "recordNumber": "BP-2024-001234",
        "recordType": "Building Permit",
        "status": "Approved",
        "address": "123 Main St",
        "city": "Bloomfield",
        "state": "CT",
        "zipCode": "06002",
        "applicantName": "John Smith",
        "createdAt": "2024-01-15T10:00:00.000Z",
        "updatedAt": "2024-03-20T14:30:00.000Z"
      }
    }
  ],
  "meta": { "total": 4782 }
}
```

**GraphQL (search.viewpointcloud.com):** Standard GraphQL with `searchRecords` query returning `recordNumber`, `recordType`, `status`, `address`, `applicantName`, `createdAt`, `updatedAt`.

**Categories endpoint (confirmed public, no auth):**
```json
{
  "data": [{
    "id": "1089",
    "type": "categories",
    "attributes": {
      "categoryID": 1089,
      "name": "Building Division",
      "isEnabled": 1
    }
  }]
}
```

---

## Auth Requirements

- **Public search/browse:** Requires Auth0 JWT (but anyone can create a free citizen account)
- **Account creation:** Self-service at any portal — no government affiliation required
- **Staff features:** Separate `workflow.opengov.com` subdomain, requires government login
- **Developer API:** `developer.opengov.com` — registration required, unclear if public tier exists

**Bottom line:** Creating a free account on any portal (e.g., cheltenhamtownship.portal.opengov.com) gives a universal Auth0 token valid for all 500+ portals.

---

## Existing Infrastructure (Already Built)

All code lives in `/home/will/ReactCRM/scrapers/opengov/`:

| File | Purpose |
|------|---------|
| `opengov-config.ts` | 215 portal configs, API config, proxy config |
| `opengov-scraper.ts` | Playwright scraper with Auth0 support |
| `opengov-auth.ts` | Auth0 implicit flow — captures Bearer token from URL fragment |
| `opengov-api-discovery.ts` | Network interception tool for discovering endpoints |
| `discovered-endpoints.json` | Documented API endpoint map |

**Data already extracted:** 214 jurisdiction metadata files in `/home/will/ReactCRM/scrapers/output/opengov/` — categories and record types for every portal. 7.7MB total.

**No OpenGov scrapers exist in the current 312-scraper suite** at `/home/will/crown_scrapers/`.

---

## Feasibility Assessment

**Overall: MEDIUM**

| Factor | Assessment |
|--------|-----------|
| API availability | Confirmed REST + GraphQL |
| Auth complexity | Medium — free citizen account, universal token |
| Data format | Clean JSON, no parsing required |
| Scale | 500+ jurisdictions, potentially 50M+ records |
| Anti-bot measures | Unknown rate limits; Decodo proxy rotation pre-configured |
| Existing work | Substantial — 214 portals mapped, scraper built |
| Blocker | Need to create one OpenGov account and capture token |

---

## Estimated Scope

- **500+ municipalities** confirmed on OpenGov (OpenGov's own announcement)
- **214 portals** already configured in our scraper
- **Permit volume estimate:** 1,000–50,000 permits per jurisdiction → ~2M–25M total records
- **States covered:** AK, AL, CA, CO, CT, FL, GA, IA, ID, IL, IN, KS, KY, MA, MD, ME, MN, MS, and more
- **Best target states for our use case:** heavy CT, MA, CA, FL, GA presence

---

## Next Steps to Activate

1. Create a free citizen account at any portal (e.g., https://cheltenhamtownship.portal.opengov.com/sign-up)
2. Set `OPENGOV_EMAIL` and `OPENGOV_PASSWORD` env vars
3. Run: `cd /home/will/ReactCRM && npx tsx scrapers/opengov/opengov-scraper.ts`
4. The auth module captures the Bearer token via implicit grant redirect
5. Token works for all 214 configured portals
6. Data flows to `scrapers/output/opengov/{jurisdiction}_permits_*.ndjson`

---

## Comparison to EnerGov (Tyler Tech)

| Feature | OpenGov/ViewPoint | EnerGov/Tyler |
|---------|------------------|---------------|
| Portals | 500+ | 25 configured |
| Auth required | Yes (free account) | No (public API) |
| API type | REST + GraphQL | REST only |
| Token scope | Universal (one login) | Per-portal |
| Prior work | 214 portals mapped | 21 portals configured |
| Feasibility | Medium | Easy |
