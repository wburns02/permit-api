# Texas Building-Permit / New-Build Lead Platform — Plan

_Status: Phase 1a in progress (Brazoria County beachhead). Author: permit-api. Last updated: 2026-06-25._

## Vision

Build a statewide Texas **building-permit and new-build lead feed** on top of the
existing `permit-api` scraper framework. The product is a daily stream of
"someone is about to build / re-roof / put in a septic system here" leads,
sold to contractors. **Roofing is the first buyer** (storm + new-construction
roof demand is the highest-ticket, fastest-closing vertical and already has a
storm-driven leads product in this repo — `hail_leads`). Septic (MAC Septic),
electrical, HVAC, and solar are follow-on buyers off the same substrate.

**Brazoria County is the beachhead.** It is a fast-growing Gulf Coast county
with mixed jurisdiction tooling (MGO, CitizenServe, iWorQ, a public NENA 911
ArcGIS layer), which makes it a representative proving ground for every adapter
class we will need statewide. Prove the pipeline end-to-end in one county, then
generalize adapter-by-adapter.

## Leading-indicator thesis

Permits are a **lagging** signal — by the time a building permit issues, the
owner has already hired the GC. We want the **earliest** public signal of a new
structure. Ordered earliest → latest:

1. **911 / NENA NG911 address point created** (`CR_DATETIME`) — a new address is
   assigned before the structure is permitted. **This is the leading indicator**
   and the centerpiece of the generalization story (every county that publishes
   a NENA layer exposes the identical schema).
2. County clerk **deed / plat** records (lot split, new subdivision).
3. **OSSF / septic** permit (rural new build needs a septic permit before the
   house permit).
4. **Building permit** (the lagging signal everyone else sells).
5. Certificate of occupancy.

We harvest all of these into one landing table and let downstream type/geo/dedup
logic turn them into a single "new-build lead" per address.

## Architecture

```
                          ┌─────────────── vendor adapters (per platform) ───────────────┐
  MGO Connect ────────────┤ scrape_mgo_ctx.py            (registry: jurisdiction id)      │
  CitizenServe ───────────┤ scrape_citizenserve_ctx.py   (registry: installationID + host)│
  iWorQ ──────────────────┤ scrape_iworq.py              (registry: slug)                 │
  ArcGIS FeatureServer ───┤ scrape_central_tx_small_cities.py (registry: layer URL)       │
                          └──────────────────────────────────────────────────────────────┘
                          ┌─────────────── proxy layers (leading indicators) ────────────┐
  County 911 (NENA) ──────┤ scrape_county_911_addresses.py  (registry: county + ArcGIS URL)│  ◀── NEW, Phase 1a
  County clerk deeds ─────┤ (Phase 2)                                                       │
  TCEQ OSSF / septic ─────┤ (Phase 2 — already have 165K TX OSSF rows loaded)             │
                          └──────────────────────────────────────────────────────────────┘
                                              │
                                              ▼
                                ┌──────────────────────────┐
                                │  hot_leads  (landing)     │  UNIQUE (permit_number, source)
                                │                           │  partial UNIQUE (address, source) WHERE permit_number IS NULL
                                └──────────────────────────┘
                                              │  bridge_hot_leads_to_permits.py
                                              ▼
                                ┌──────────────────────────┐
                                │  permits / permits_tx     │  (served by the API + website)
                                └──────────────────────────┘
                                              │
                                ┌──────────────────────────┐
                                │ /v1/freshness/hot-leads   │  per-source freshness (GROUP BY source)
                                │ hot_leads_sources ledger  │  per-run load stats + high-water marks
                                └──────────────────────────┘
                                              │
                                ┌──────────────────────────┐
                                │ contact enrichment        │  CAD owner join + skip-trace (Phase 4)
                                │ (business_entities,        │  → owner / mailing / phone on each lead
                                │  property_sales joins)     │
                                └──────────────────────────┘
```

### What is REUSED (no greenfield)

- **Per-vendor scraper-script pattern** — config-driven registry + reusable
  `arcgis_fetch()` / `arcgis_normalize()` (canonical: `scrape_central_tx_small_cities.py`).
- **`hot_leads` landing table** — zero schema changes. Normalized columns
  permit_number / permit_type / work_class / address / city / state / zip /
  county / lat / lng / issue_date / applied_date / status / valuation /
  owner_name / applicant / contractor* / jurisdiction / source.
- **`bridge_hot_leads_to_permits.py`** — promotes hot_leads → permits/permits_tx.
- **`/v1/freshness/hot-leads` + `hot_leads_sources` ledger** — freshness monitor.
  New sources appear automatically (the endpoint does `GROUP BY source` live);
  the ledger captures per-run load counts and the incremental high-water mark.
- **Enrichment joins** — `business_entities` (contractor → registered agent /
  officer contact) and `property_sales` (address → CAD owner + mailing) already
  exist and run against hot_leads.

### The adapter gap (NEW adapters still to build)

The four platforms above cover a big slice of TX cities, but these common
permit platforms still need adapters:

- **eTRAKiT** (e.g. **Pearland**) — legacy ASP.NET portal, form-post search.
- **Click2Gov / CentralSquare** (e.g. **Lake Jackson**) — older municipal portal.
- **Tyler EnerGov / Civic Access** (e.g. Kyle, Lakeway) — OIDC-locked JSON API,
  needs a logged-in browser session (Playwright).
- **Cityworks** — public ArcGIS-backed portals, varies by deployment.

## Phased plan

### Phase 1a — Brazoria beachhead (THIS PHASE)

Four sources, all landing in `hot_leads`:

1. **Brazoria County 911 address-point puller** — `scrape_county_911_addresses.py`
   (NEW). NENA NG911 ArcGIS layer, incremental on `CR_DATETIME`, source tag
   `brazoria_co_911_addresses`, marked NEW-ADDRESS trigger (permit_number NULL,
   so it is intentionally NOT bridged into building permits). Written generically
   so adding another county is one `COUNTIES` registry entry.
2. **Angleton → MGO** — added jurisdiction id `404` to `scrape_mgo_ctx.py`.
3. **Freeport → CitizenServe** — added installationID `404` (host `www4`) to
   `scrape_citizenserve_ctx.py`. NOTE: Freeport's public search is reCAPTCHA-gated
   and exposes no default permit list, so the HTTP path yields 0 rows. Registered
   and documented as blocked; needs a session/Playwright path (Phase 1b+).
4. **iWorQ Brazoria (Freeport slug)** — already in `scrape_iworq.py` flagged
   `has_default_list: False` (CAPTCHA). Confirmed still CAPTCHA-gated; left
   documented as blocked, not fought.

### Phase 1b — More Brazoria jurisdictions

- **eTRAKiT adapter** → Pearland.
- **Click2Gov adapter** → Lake Jackson.
- Revisit Freeport / iWorQ CAPTCHA via authenticated browser session.

### Phase 2 — Proxy / leading-indicator layers

- County **clerk deed/plat** feed.
- **TCEQ OSSF / septic** permit feed (165K TX OSSF rows already loaded — wire as
  a hot_leads source + incremental refresh).
- Expand the 911 adapter to neighboring counties (one registry entry each).

### Phase 3 — Lead view (type-classify + geocode + dedup)

- Classify each hot_leads row to a trade (roof / septic / electrical / new-build).
- Geocode rows missing lat/lng.
- Dedup across sources by normalized address → one lead per address with the
  earliest trigger date and the richest field set.
- Materialized "new-build lead" view, filterable by county / trade / freshness.

### Phase 4 — Contact enrichment

- CAD-owner join (`property_sales`) + skip-trace (BatchData) → owner name,
  mailing address, phone on every lead.
- Contractor enrichment (`business_entities`) for the GC side.

### Then: generalize statewide, adapter-by-adapter

Each new TX county is: register its 911 ArcGIS layer (one line), register its
MGO/CitizenServe/iWorQ/eTRAKiT/Click2Gov jurisdictions (one line each). No new
code per county once the adapter for its platform exists.

## Operational notes

- **DB write path**: scrapers connect directly to the scraper DSN
  (`100.122.216.15:5432/permits`, user `will`). The read-only DSN times out on
  big scans; use the scraper connection for loads.
- **911 adapter dedup**: 911 rows have NULL permit_number and conflict on the
  partial unique index `(address, source) WHERE permit_number IS NULL`. They are
  excluded from the building-permit bridge by design (bridge requires
  `permit_number IS NOT NULL`).
- **911 freshness caveat**: the trigger is only as fresh as the county's GIS
  publish cadence. Brazoria's layer, at time of writing, lags the calendar by a
  few months (max `CR_DATETIME` ≈ 2026-02-26). Still the earliest signal we get,
  but not real-time. Track per-county lag in the ledger.
- **Prepared cron (NOT installed — for Will to enable):**

  ```cron
  # County 911 new-address triggers — daily 05:05 CT, 2-day look-back (incremental)
  5 5 * * * cd /home/will/permit-api-live && python3 scripts/scrape_county_911_addresses.py --county brazoria --since-days 2 >> /tmp/county_911_brazoria.log 2>&1
  # Angleton (MGO) is covered by the existing CTX MGO daily chain (id 404 now in registry):
  # 20 5 * * * ... scrape_mgo_ctx.py --days 7   (already scheduled; picks up Angleton automatically)
  ```
