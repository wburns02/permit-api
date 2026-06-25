# Texas Building-Permit / New-Build Lead Platform — Plan

_Status: Phases 1–4 shipped for the Brazoria County beachhead (source → classify → dedup → CAD owner/address → skip-trace phone → served). Author: permit-api. Last updated: 2026-06-25._

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

### Phase 1b — Incorporated-city permit portals (DONE — adapters built; Brazoria targets externally blocked)

Two NEW generic, registry-driven vendor adapters were built. Both run; both
Brazoria targets are externally walled (documented, not faked).

- **eTRAKiT adapter** — `scrape_etrakit.py` (NEW). Generic CentralSquare/eTRAKiT3
  `/etrakit3/Search/permit.aspx` driver (Playwright, ViewState-safe), config
  registry (`JURISDICTIONS`). Built-in connectivity gate (`--probe`) that refuses
  to fabricate when a host is unreachable. **Unlocks every reachable eTRAKiT TX
  city statewide** with one registry line.
  - **Pearland → BLOCKED (network):** `etrakit.pearlandtx.gov` resolves
    (170.76.141.9) but DROPS all inbound TCP on :80/:443 from our host (HTTP 000,
    TLS never completes) — an IP-level filter needing a Texas/residential egress.
    Round Rock's eTRAKiT was decommissioned (migration notice), confirming eTRAKiT
    is being sunset across TX. Adapter is ready; Pearland is unreachable from us.
- **Click2Gov adapter** — `scrape_click2gov.py` (NEW). Generic CentralSquare CEP
  `/Click2GovBP/selectpermit.html` driver (Playwright, OWASP_CSRFTOKEN session),
  config registry. `--probe` empirically measures the server result cap.
  - **Lake Jackson → BLOCKED (architecture, not CAPTCHA):** server hard-caps
    results at **10 per search** (verified across 7 street tokens — always exactly
    10), there is no date-range / browse-recent search, and the returned rows skew
    to ancient permits (1990s–2010s). Not enumerable as a fresh-lead feed. The
    county's **CentralSquare ArcGIS folder is token-gated** ("Token Required"), so
    the easier GIS path is not anonymously available either.
- **Freeport CitizenServe + iWorQ → BLOCKED (reCAPTCHA, confirmed):** a real
  headless-browser session rendered a live Google reCAPTCHA iframe on both the
  CitizenServe (installationID 404, www4) search page and the iWorQ search.
  Not bypassed (egregious ToS). NOTE: the existing iWorQ `freeport` registry slug
  actually resolves to **Freeport, ILLINOIS** (wrong state) — flagged for fix.

### Phase 2 — Proxy / leading-indicator layers (PARTIAL — OSSF wired; deeds/plats blocked)

- **County clerk deed/plat feed → BLOCKED (reCAPTCHA + subscription).** Brazoria
  clerk records are on Tyler eSearch (`brazoriacountytx-web.tylerhost.net`). The
  disclaimer "I Accept" button is gated by a live **Google reCAPTCHA** (visible
  `recaptcha/api2/anchor` iframe, sitekey `6LemVGAUAAAAAB_iW1wbaE4_s0Z5SoSakm6GI8St`),
  and the portal meters via `getSubscriptionTime` (subscription). Not bypassed.
  Fresh path = a paid Tyler eSearch subscription or a county-clerk bulk-data /
  PIA arrangement; no anonymous automation.
- **TCEQ OSSF / septic feed → WIRED (the inventory we hold).** `scrape_ossf_to_hot_leads.py`
  (NEW). Generic county-registry adapter that surfaces `ossf_permits_tx` into
  `hot_leads` as a rural new-build trigger (`permit_type='SEPTIC (OSSF)'`,
  `work_class='NEW-BUILD TRIGGER (OSSF)'`, real permit_number → bridge-eligible).
  PROVEN on Hays (223 rows round-tripped from `hot_leads`). Covers the 8 Central-TX
  counties we hold (~165K rows: Travis/Williamson/Bastrop/Ellis/Grayson/Fannin/
  Hays/Cooke). **Snapshot freshness: data ends ~2025-12-15** (a held snapshot, not
  a live feed); track per-source lag in the ledger.
  - **Brazoria OSSF → NOT HELD.** Verified **0** Brazoria rows in `ossf_permits_tx`.
    Brazoria is a TCEQ **Authorized Agent**: it issues/holds its own OSSF permits
    with no state API or bulk feed. Fresh path = **monthly county PIA** to Brazoria
    County Environmental Health (OSSF program). Documented in the adapter's
    `NOT_HELD` registry; no Brazoria feed is faked.
- Expand the 911 adapter to neighboring counties (one registry entry each) — still open.

### Phase 3 — Lead view (type-classify + geocode + dedup)  ✅ SHIPPED

- **Classify** — `app/services/permit_lead_classify.py` maps each row to a
  normalized `lead_class` (`new_construction` | `addition` | `remodel` | `other`)
  via source-aware rules. 911 / NENA address triggers → `new_construction` proxy.
  Rules exist as BOTH Python (`classify_permit`) and SQL (`lead_class_sql`); a
  pytest corpus of real Brazoria descriptions pins parity. Add a source = one
  line in `BRAZORIA_SOURCES`.
- **Geocode** — `scripts/geocode_brazoria_leads.py` backfills lat/lng for permit
  rows with an address but no coords (chiefly `mgo_angleton`) via the free US
  Census geocoder, into the shared `geocoded_addresses` cache (reuses the
  rural_score geocoder shape). Rate-limited, `--limit`-capped, source-filtered
  (indexed) — never a full scan. 911 rows already carry coords.
- **Dedup** — the `brazoria_permit_leads` MV collapses to one row per normalized
  address (`DISTINCT ON (address_norm)`), keeping the RICHEST row and aggregating
  every contributing `source` + the EARLIEST trigger date. A 911 point and a
  building permit at the same address merge into one lead.
- **View** — `brazoria_permit_leads` MV (created WITH NO DATA in the startup
  migration in `app/main.py`; unique index on `address_norm` for REFRESH
  CONCURRENTLY). Registered in `app/services/mv_refresh._MVS` so it refreshes on
  the same nightly path as `unserviced_hail_leads`. County-scoped via the source
  registry (NULL-county sources resolve through `source_county_sql`).
- **Serve** — `GET /v1/permit-leads/` (list), `/stats` (counts by class +
  contact-coverage gaps), `/export.csv`. Same auth posture as
  `/v1/hail-leads/unserviced` (`require_demo_key`), same MV-unpopulated-is-empty
  handling. Filters: county / lead_class / source / from_date / to_date /
  has_coords.

### Phase 4 — Contact enrichment  ✅ SHIPPED

- **CAD owner/address join (FREE)** — `scripts/load_brazoriacad.py` loads the
  Brazoria County GIS parcel layer (`maps.brazoriacountytx.gov` →
  `general/Parcels/MapServer/1`, ~280K parcels, no token) into `tx_cad_parcels`
  as `cad_source='BRAZORIACAD'` (`county_fips='48039'`): owner_name (`py_owner_name`),
  situs, appraised value, subdivision, legal description. Mirrors the existing
  per-county loaders (load_bcad.py). The `brazoria_permit_leads` MV (v3) LEFT
  JOINs it on a fully-collapsed normalized situs key to fill `owner_name`
  (`COALESCE(source owner, CAD owner)`), a canonical `mailable_address`
  (street, city, zip), plus bonus `market_value` / `subdivision` / `cad_parcel_id`.
  Geometry is NOT loaded — the owner join needs attributes only.
- **Skip-trace for phone (PAID, GATED)** — `scripts/skiptrace_brazoria_leads.py`
  takes leads that already have a CAD-attributed owner + address and resolves a
  best phone/email via BatchData skip-trace into `brazoria_lead_contacts`
  (keyed on `address_norm`). Dry-run by default; `--limit` hard-caps fresh
  lookups (default 25); already-cached addresses are skipped (no double-charge);
  priority ordering puts `new_construction` / `addition` first. Never fabricates
  a number — a miss writes a `hit=false` row. The MV LEFT JOINs it for
  `phone` / `email` / `skiptraced`.
- **Serve** — `/v1/permit-leads/` list + `/stats` + `/export.csv` now expose
  `owner_name`, `mailable_address`, `cad_owner_name`, `cad_matched`,
  `market_value`, `subdivision`, `phone`, `email`, `skiptraced`. Stats adds
  `with_owner_name` / `cad_matched` / `skiptraced` / `with_phone` coverage KPIs.
- **Still open (Will decision):** budget to skip-trace the full 2,012 (or the
  720-lead `new_construction`+`addition` priority subset); contractor enrichment
  (`business_entities`) for the GC side; Pearland portal proxy; OSSF/clerk PIAs.

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
  # OSSF septic new-build triggers — daily 05:25 CT, incremental from ledger high-water mark
  25 5 * * * cd /home/will/permit-api-live && python3 scripts/scrape_ossf_to_hot_leads.py --all >> /tmp/ossf_hot_leads.log 2>&1
  # eTRAKiT Pearland — daily 05:35 CT. DISABLED: host network-unreachable from us
  # (HTTP 000 on :80/:443). Enable only after a reachable egress/host is registered.
  # 35 5 * * * cd /home/will/permit-api-live && python3 scripts/scrape_etrakit.py --city pearland --days 7 >> /tmp/etrakit_pearland.log 2>&1
  # Click2Gov Lake Jackson — NOT scheduled: server caps results at 10/search, no
  # date browse → not a usable fresh feed. scrape_click2gov.py kept for other cities.
  ```

### Phase 1b/2 source verdicts (2026-06-25)

| Source | Adapter | Verdict | Reason / proof |
|--------|---------|---------|----------------|
| OSSF Hays (+7 CTX counties) | `scrape_ossf_to_hot_leads.py` | **PROVEN** | 223 Hays rows round-tripped from `hot_leads` (e.g. `OSSF-2025-4523 \| 401 LANGE RD \| ANNETTE LORENZ \| 2025-12-11`); ledger `ossf_hays` records_loaded=223 |
| Pearland eTRAKiT | `scrape_etrakit.py` | **BLOCKED** | network IP filter; :80/:443 drop all inbound (HTTP 000). Adapter ready + connectivity-gated |
| Lake Jackson Click2Gov | `scrape_click2gov.py` | **BLOCKED** | server caps at 10 results/search (measured), no date browse, ancient rows; county ArcGIS token-gated |
| Freeport CitizenServe | `scrape_citizenserve_ctx.py` (id 404) | **BLOCKED** | live Google reCAPTCHA on search (browser-confirmed) |
| iWorQ Freeport | `scrape_iworq.py` | **BLOCKED** | live reCAPTCHA; existing slug also points at Freeport **IL** (wrong state) |
| Brazoria clerk deeds/plats | (none) | **BLOCKED** | Tyler eSearch disclaimer behind Google reCAPTCHA + subscription metering |
| Brazoria OSSF | `scrape_ossf_to_hot_leads.py` (`NOT_HELD`) | **NOT HELD** | 0 rows held; TCEQ Authorized Agent, county-held, no API → monthly PIA |
