# iWorQ Permit Portal Investigation

**Date:** 2026-03-30
**Status:** Research complete — low priority, CAPTCHA-blocked

---

## TL;DR

iWorQ serves thousands of small governments, but every permit search on their citizen portal requires Google reCAPTCHA v2 (server-side verified). There is no public REST API. The portals are HTML-based with server-rendered search results. Scraping is technically possible with a CAPTCHA-solving service (2captcha, CapSolver), but given the small data volume per city (estimated 500-5,000 records each) and the number of discoverable portals, iWorQ is a **low-value, high-friction** target compared to EnerGov or Accela.

---

## URL Structure (Both Confirmed Working)

### New Citizen Portal System
All new portals use wildcard DNS (`*.portal.iworq.net` resolves to `52.34.35.204`) with application-level routing by city slug.

**Portal home:**
```
https://{cityslug}.portal.iworq.net/
```

**Permit search page:**
```
https://{cityslug}.portal.iworq.net/{CITYSLUG}/permits/600
```
(Module ID `600` = building permits — consistent across all portals tested)

**Permit application submission:**
```
https://{cityslug}.portal.iworq.net/{CITYSLUG}/new-permit/600/{templateId}
```

**Contractor/entity search:**
```
https://{cityslug}.portal.iworq.net/{CITYSLUG}/entities/1100
```

**Legacy fallback (non-city-specific):**
```
https://portal.iworq.net/portalhome/{cityslug}
```
This shows the portal home but with a demo/default config for unknown slugs.

### Old PHP System (Dead)
`https://iworq.net/iworq/` — login-gated PHP app. All public-facing paths return "page does not exist". Fully disabled for public access.

---

## Confirmed Working Portals (Public Permit Search)

| City/County | State | Portal URL | Portal ID |
|-------------|-------|-----------|-----------|
| Hitchcock | TX | hitchcock.portal.iworq.net | 693 |
| North Ogden | UT | northogden.portal.iworq.net | (new system) |
| Jackson County | FL | jacksoncounty.portal.iworq.net | 65 |
| Farmington | UT | farmingtonut.portal.iworq.net | 222 |
| Vernal | UT | vernal.portal.iworq.net | 261 |
| Moab | UT | moab.portal.iworq.net | 718 |
| Pima | AZ | pimaaz.portal.iworq.net | 3590 |
| Butler Township | OH | butlertownship.portal.iworq.net | 1655 |

Additional portals with portal home but no public permit search confirmed:
- Knott County, KY (slug: `knottcounty`, portal-id: 3184)
- Fayette County, KY (slug: `fayettecountyky`, portal-id: 1521)
- Springdale, UT (slug: `springdale`, portal-id: 3408)

---

## Data Fields Available (Permit Search Results)

Fields vary by city config. Common columns seen:

| Field | Notes |
|-------|-------|
| Permit # | Always present |
| Date | Present in some portals (North Ogden) |
| Parcel Address | Always present |
| Permit Type | Present in some (Hitchcock) |
| Status | Always present |
| Subdivision / Lot # | Present in some (North Ogden) |

**Not exposed in search results:** owner name, contractor, valuation, project description (may appear on individual permit detail pages accessible post-search).

---

## API Investigation

No public REST API found. The platform is a PHP/Laravel app (CSRF tokens visible in all pages). Internal JS routes use the pattern:

```
/{citySlug}/{resource}/{moduleId}
```

All search endpoints return a 302 redirect unless a valid Google reCAPTCHA token is present in the query string. Fake tokens are server-side rejected. No batch export, no JSON endpoint, no pagination without CAPTCHA.

The backend JavaScript (`portal.iworq.net/js/client.js`) revealed these internal routes:
- `/{dsn}/permits/{id}` — single permit detail
- `/{dsn}/entities/{moduleId}` — contractor/license search
- `/{dsn}/work-orders/{id}/{empId}` — work orders
- `/api/account/` — account management (authenticated)
- `/ai/fetch-token` and `/ai/prompt-actions` — AI features (authenticated)

---

## CAPTCHA Situation

Every permit search requires Google reCAPTCHA v2 Invisible:

- **Trigger:** Any GET request with `?searchField=...&search=...` params
- **Behavior:** Server validates the `g-recaptcha-response` token via Google's API before executing the query
- **Bypass result:** 302 redirect back to base URL (no data shown)
- **Site key:** `6Les_AYkAAAAACw9NzcxkcDVfvExxeyw2KS1cao_`

Date-based searches (`?searchField=issuedate&dateFrom=...&dateTo=...`) also redirect — no bypass found.

**Workarounds:**
- CAPTCHA-solving service (2captcha ~$1/1000, CapSolver ~$0.80/1000) — adds ~5 second delay per search
- Playwright with real browser — CAPTCHA still fires but can be solved programmatically with the solving service
- No simple HTTP-only approach exists

---

## Portal Discovery Method

iWorQ uses slug-based routing. Discovery requires knowing city slugs. Confirmed patterns:
- Use lowercase city/county name, no spaces
- State suffix sometimes added (e.g., `pimaaz`, `vernal` without suffix, `farmingtonut`)
- County portals: `{county}county` (e.g., `knottcounty`, `jacksoncounty`)

**Portal ID range:** 65 (Jackson County, old) to 3590+ (Pima AZ, newer) — suggests 200-500+ active customer portals, but not all enable public permit search.

**No enumerable API or directory** exists. Discovery requires slug guessing or web research.

---

## Data Volume Estimate

iWorQ explicitly targets small municipalities (population 1,000-50,000). Typical annual permit volume for these cities: 100-500/year. With 5-10 year history: **500-5,000 records per portal**.

Compare:
- EnerGov portal (Riverside County, CA): ~100,000 records
- Accela portal (large cities): 50,000-200,000 records
- iWorQ portal (small city): **500-5,000 records**

Even if 200 active portals were scraped with permit search enabled: **100,000-1M total records** — but spread across many tiny jurisdictions with minimal commercial construction value.

---

## Feasibility Assessment

| Factor | Rating | Notes |
|--------|--------|-------|
| URL consistency | Good | Module 600 = permits across all portals |
| Public access | Partial | Permit list page accessible, search CAPTCHA-gated |
| CAPTCHA | Blocking | Server-side verified reCAPTCHA v2 on every search |
| REST API | None | No public API, no JSON endpoints |
| Data volume | Low | 500-5,000 records per small-city portal |
| Data quality | Moderate | Address + permit type + status; owner/value not always shown |
| Portal count | ~200-500 | Not all enable public permit search |
| Discovery | Hard | No portal directory; slug-guessing required |

---

## Recommendation: Low Priority

**Do not build an iWorQ scraper** at this time. The combination of:
1. Mandatory reCAPTCHA on every search (adds per-query cost + 5s latency)
2. Very small record counts per city (500-5,000 vs 50,000+ for EnerGov/Accela)
3. No batch/export endpoint
4. No REST API
5. Hard-to-discover portal slugs

...means the cost-to-data-volume ratio is poor. The cities using iWorQ are the exact small jurisdictions (UT, ID, WY, MT, KY, TX panhandle) that have limited new construction activity and low commercial value for PermitLookup Pro.

**If iWorQ becomes worth doing later:**
- Use Playwright + CAPTCHA solving service (2captcha)
- Target the `/{citySlug}/permits/600` URL
- Build a slug discovery list by searching "iworq portal" on city websites
- Estimate 5-10 minutes per portal to scrape 500-5,000 records with CAPTCHA delays

---

## Files

- Investigation: `/home/will/permit-api/docs/iworq-investigation.md`
- No existing scrapers found in `/home/will/crown_scrapers/`
