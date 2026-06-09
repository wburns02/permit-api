# permits.ecbtx.com: Permit Intelligence Platform Blueprint

**Date:** 2026-06-09
**Status:** Strategic blueprint, pre-execution
**Companion docs:** `scraper-dataset-catalog-2026-05-21.md`, `burns-layer-4/data-catalog/CATALOG.md`

---

## 0. Starting Position (read this first)

Every strategy doc for "build a permit platform" assumes day zero. We are not at day zero. The honest framing: we already operate one of the largest independent permit data operations in the country and have not productized it.

| Asset | State |
|---|---|
| Active scrapers (30d) | 631 sources, ~95 cron loaders on R730 |
| Warehouse | ~3.18B rows, 164 tables (T430 PG18 primary, r730-2 PG18.3 461GB secondary) |
| Permit rows | TX 78M, FL 230M, LA 100M + 53 city NDJSON dumps staged |
| Adjacent layers | Code violations 308M, property sales 102M, HMDA 80M, FEC 191M, business entities 99M, FCC broadband ~600M |
| Niche depth | 165K TX OSSF septic permits (8 counties), 517K-row `central_tx_septic_leads` MV, `hail_leads` storm×permit MV |
| Oil & gas (seed) | `oil_gas_wells` 5.39M rows, `oil_gas_production` 330K rows, RRC bulk source identified but not fully pulled |
| Enrichment | enrichment-worker.service 24/7 on R730, Qwen3.5:122b local (zero marginal token cost), enrichment_queue on T430 |
| Orchestration | Hatchet + event substrate (burns_events) live on r730-2 |
| Serving | permit-api (FastAPI) on Railway, permits.ecbtx.com, DB via pg.ecbtx.com Cloudflare Tunnel |
| Proven revenue motion | Lead lists (hail→roofers, septic→pumpers, contractor enrichment for H-Man) |

The blueprint below is therefore not "how to build it" but "how to convert a lead-gen warehouse into a defensible intelligence product." That conversion is mostly four things: a canonical schema, a public search/map experience, a metered API, and an oil & gas vertical (Rebound Dynamics alignment).

One constraint conflict to name up front: the prompt says "prefer modern cloud-native tooling." Standing rule here is sovereign over SaaS. Resolution: sovereign core (storage, compute, enrichment on R730/r730-2/T430), cloud edge only where it buys reach (Cloudflare Tunnel/Pages, Railway API edge). Marginal cost per enriched record stays near zero, which IS the moat (Section 8).

---

## 1. Product Vision

### Positioning

**"Every permit, every well, every parcel: one queryable surface."** Bloomberg Terminal economics, Google Maps interaction model, GitHub-style openness at the free tier.

### Target users, ranked by willingness to pay

| Tier | User | Problem solved | What they buy |
|---|---|---|---|
| 1 | Trade contractors (roofing, septic, electrical, HVAC) | "Who needs my service this week, near me?" | Lead lists + alerts. Already proven with hail_leads and septic. |
| 1 | Oil & gas service/supply companies | "Where is drilling/completion activity moving, who operates it?" | Rig & permit radar, operator dossiers, county heat. |
| 1 | Real estate investors / flippers | Distress + momentum signals (code violations × permits × sales) | Watchlists, distress index, comps. |
| 2 | Developers & builders | Pipeline visibility, competitor tracking, entitlement timelines | Developer Radar, timeline analytics. |
| 2 | Suppliers (lumber, concrete, equipment rental) | Demand forecasting by geography | Momentum index, bulk feeds. |
| 2 | Insurers / underwriters | Property history, unpermitted-work risk, roof age | API + property reports. |
| 3 | Municipalities | Benchmarking, regional dashboards | White-label dashboards. |
| 3 | Researchers / journalists / homeowners | Open access to public records | Free tier (growth + SEO engine). |

### Unique value proposition

1. **Cross-domain joins nobody else ships.** Permits × code violations × storm events × parcels × wells × business registries, in one schema. Shovels has permits. Enverus has wells. ATTOM has parcels. Nobody has the joins.
2. **Freshness.** 404 sources active in the last 24h. Government portals update us daily; aggregators resell quarterly batches.
3. **Cost structure.** Local 122B-parameter enrichment means we can afford to enrich every record, not a sample.
4. **Niche verticals as beachheads.** Septic intelligence and storm-damage leads are markets the big players ignore and the small players can't build.

### Why it becomes a standard

Not by claiming "every permit in America" on day one. Realistic ramp: be the unambiguous best for TX/TN/SC + the O&G corridor, prove the API with 10 paying integrations, then scale by vendor family (the OpenGov scraper already covers 348 jurisdictions; one adapter = hundreds of cities). The standard emerges from being the cheapest correct answer, not from marketing.

---

## 2. Data Architecture

### 2.1 Canonical permit schema

Today's `hot_leads` + per-source tables are write-optimized. Productizing needs a read-optimized canonical layer. Keep raw source tables as-is (lineage), add a canonical spine:

```sql
-- The intelligence object
CREATE TABLE canonical.permits (
    permit_id        UUID PRIMARY KEY,            -- native PG UUID per house rules
    source_id        TEXT NOT NULL,               -- matches data-catalog source_id scheme
    source_record_id TEXT NOT NULL,               -- jurisdiction's own permit number
    jurisdiction_id  UUID NOT NULL REFERENCES canonical.jurisdictions,
    permit_type      TEXT,                        -- raw, as issued
    category         TEXT,                        -- enriched taxonomy (Section 3)
    subcategory      TEXT,
    description_raw  TEXT,
    description_ai   TEXT,                        -- one-line AI summary
    status           TEXT,                        -- normalized lifecycle enum
    status_raw       TEXT,
    applied_date     DATE,
    issued_date      DATE,
    finaled_date     DATE,
    declared_value   NUMERIC,
    estimated_value_low  NUMERIC,                 -- enriched
    estimated_value_high NUMERIC,
    address_raw      TEXT,
    address_norm     TEXT,                        -- libpostal/usaddress normalized
    geom             GEOMETRY(Point, 4326),
    geocode_confidence REAL,
    parcel_id        UUID REFERENCES canonical.parcels,
    contractor_id    UUID REFERENCES canonical.contractors,
    owner_id         UUID REFERENCES canonical.entities,
    complexity_score REAL,
    confidence_score REAL,                        -- composite data-quality score
    freshness_at     TIMESTAMPTZ,                 -- last verified against source
    lineage          JSONB,                       -- ingest run id, transform versions
    embedding        VECTOR(1024),                -- bge-m3, pgvector
    UNIQUE (source_id, source_record_id)
) PARTITION BY RANGE (issued_date);
```

### 2.2 Entity relationship model

```
jurisdictions 1─* permits *─1 parcels 1─* wells (O&G, parcel/lease match)
permits *─1 contractors ─* licenses (TDLR, state boards)
contractors ─1 entities (business registry resolution)
permits 1─* inspections
permits 1─* documents (PDFs in object storage, OCR text + embedding)
parcels 1─* violations (code enforcement)
parcels 1─* sales / tax_assessments
geographies (county/tract/zip) ─ aggregates (momentum, trends, MVs)
wells 1─* production_monthly  wells *─1 operators (P-5 resolved to entities)
events (burns_events substrate): permit.issued, well.permitted, inspection.failed...
```

`entities` is the keystone table: contractors, owners, operators, and registered businesses all resolve into it. The resolver work already exists in Burns L4; reuse it.

### 2.3 Deduplication

Already battle-tested (DEDUP-REPORT.md). Codify as a standing pipeline:

1. **Exact:** (source_id, source_record_id) unique constraint.
2. **Cross-source:** same jurisdiction often appears via portal scrape AND Socrata AND vendor API. Block on (jurisdiction, permit_number_normalized); fall back to (parcel, issued_date ±3d, value ±10%, trigram-similar description ≥0.6).
3. **Survivorship:** prefer source with higher per-source confidence rating; merge non-null fields; keep all source pointers in `lineage`.
4. Standing rule applies: round row counts (10000 exactly) = pagination cap, re-pull before trusting.

### 2.4 Lineage, freshness, confidence

- Every ingest run gets a run_id; every canonical row carries `lineage.run_ids[]` + transform code version. Cheap (JSONB), answers "where did this number come from" forever.
- `freshness_at` updated on every source re-verify; UI badges records >30d stale.
- `confidence_score` = weighted: geocode confidence, dedup ambiguity, source reliability (per-source historical error rate), field completeness. Expose it in the API; competitors hide their data quality, we sell ours.

### 2.5 Source classes and ingestion

| Class | Method | Status |
|---|---|---|
| Vendor portals (OpenGov 348, MGO, Accela, EnerGov, Citizenserve, CityView, Click2Gov, iWorq, BSA) | Existing adapters, cron on R730 | Live, keep |
| Open data (Socrata/CKAN/ArcGIS) | Existing pullers | Live, keep |
| PDF permit docs, planning agendas | OCR (already proven on OSSF PDFs) → MinIO + text + embedding | Build (Phase 2) |
| Licensing (TDLR etc.), business registries, CADs, tax rolls | Existing (contractor_enrich.py, 99M entities, CAD dumps) | Live, extend |
| Storm/weather (SPC, storm_events, austin_311) | Live | Keep |
| **Oil & gas (see 2.6)** | Bulk downloads + weekly deltas | Seeded, expand now |
| Satellite/imagery, MLS, supplier signals | Defer. Expensive, low marginal value vs the above. | Phase 4+ |

All bulk pulls land on `/mnt/win11/Fedora/raw-public-data/` or `/dataPool/`, never the home drive.

### 2.6 Oil & gas data layer (priority vertical)

Held today: 5.39M wells + 330K production rows. The full acquisition list, in priority order. Almost all of this is free bulk download, which makes it a fast, cheap vertical to dominate:

| Source | Dataset | Cadence | Notes |
|---|---|---|---|
| **TX RRC** | W-1 drilling permits (daily ASCII/CSV) | Daily | The core radar signal. |
| TX RRC | Production Data Query dump (full PDQ) | Monthly | Lease-level oil/gas/condensate/water volumes back to 1993. Multi-GB. |
| TX RRC | Wellbore digest + API number master | Monthly | Joins everything. |
| TX RRC | Completions (W-2/G-1), plugging (W-3) | Weekly | Lifecycle tracking. |
| TX RRC | P-5 organizations + financial assurance | Monthly | Operator identity + solvency signal. |
| TX RRC | Pipeline permits (T-4), injection/disposal (H-10, UIC) | Monthly | Disposal capacity is a real intelligence product in the Permian. |
| **FracFocus** | Full SQL dump, chemical disclosures | Monthly | Completion design (fluid volumes, proppant proxies) per well. |
| TexNet (UT BEG) | Seismicity catalog | Daily API | Join to disposal wells: induced-seismicity risk scoring. |
| NM OCD | Permits, completions, production (ftp bulk) | Weekly | Permian completeness requires NM. |
| OK OCC / Corporation Commission | RBDMS exports, intents to drill | Weekly | |
| ND DMR Oil & Gas | Bakken permits/production (subscription is cheap, much is free) | Monthly | |
| LA SONRIS, CO ECMC, WY OGCC, KS KGS, CA CalGEM, PA DEP, OH ODNR, WV | State bulk files | Monthly | One adapter pattern, ~10 states covers ~95% of US onshore activity. |
| EIA | Bulk API (production, prices, rigs by basin) | Weekly | Context layers for dashboards. |
| Baker Hughes | Rig count (public XLSX) | Weekly | |
| BLM AFMSS | Federal APDs | Weekly | |
| BOEM/BSEE | Offshore leases, permits, production | Monthly | |
| PHMSA | Pipeline incidents | Monthly | |
| USGS | Produced waters DB | Once | |

Schema additions: `wells` (API14 key), `well_permits`, `production_monthly`, `operators`, `disposal_wells`, `frac_jobs`, `seismic_events`. Operators resolve into `entities` like contractors do.

Why this matters strategically: the building-permit market has Shovels/BuildZoom; the well-data market has Enverus at $30K+/seat. There is almost nothing in between, and the underlying data is free. A $200/mo "Enverus for people who can't afford Enverus" is a real product, and it makes you fluent in exactly the data Rebound Dynamics would care about.

### 2.7 Storage technologies

| Need | Choice | Rationale |
|---|---|---|
| Spine | PostgreSQL 18 + PostGIS + pgvector + pg_trgm (r730-2 primary for serving, T430 ingest) | Already there. One database, three index types, no sync jobs. |
| Raw documents | MinIO on r730-2 (or dataPool) | S3 API, sovereign. PDFs, NDJSON archives. |
| Search | Postgres FTS + trigram + pgvector first; OpenSearch only if p95 demands it | See Section 7. |
| Vector | pgvector (HNSW) | Avoid a separate vector DB; 1024-dim bge-m3 at tens of millions of rows is fine partitioned. |
| Tiles | PMTiles generated nightly + martin/pg_tileserv for live layers | Map-first UI without a tile bill. |
| Events | burns_events substrate (exists) | Alerts/webhooks ride it. |

---

## 3. AI Enrichment Pipeline

Foundation exists: enrichment-worker.service on R730, Qwen3.5:122b, queue on T430 (remember: `think:false` top-level in /api/chat). Extend the queue with typed enrichment tasks:

### 3.1 Per-record derivations

| Derivation | Method | Model/tool |
|---|---|---|
| Category/subcategory | Few-shot classify into fixed taxonomy (~40 categories incl. septic, solar, commercial TI, well pad, SWD) | Qwen3.5:122b, batch 24/7 |
| One-line summary (`description_ai`) | Generate from description + type + value | Qwen, cheap |
| Estimated value range | NOT an LLM job. Quantile regression on own comps (category × jurisdiction × sqft/era), declared value as feature | scikit-learn/XGBoost on T430 |
| Complexity score | Heuristic + model: type, value, inspection count, doc count | Same |
| Timeline prediction | Survival analysis on historical applied→issued→finaled per jurisdiction × category | lifelines, batch monthly |
| Contractor risk score | Permit volume, finaled ratio, failed-inspection rate, license status, complaint records | SQL + model |
| Inspection cadence | Historical pattern per jurisdiction × category | SQL |
| Comparables | pgvector KNN within radius + category filter | bge-m3 embeddings |
| Anomaly flags | Value z-score within cohort, impossible dates, contractor velocity spikes, owner-as-contractor patterns | Rules + isolation forest |
| Trend/momentum aggregates | MVs refreshed nightly (the hail_leads pattern, generalized) | Postgres |

### 3.2 Resolution and normalization

- **Address:** libpostal parse → Census Bureau batch geocoder (free, unlimited-ish) → self-hosted Nominatim for the misses. Store confidence.
- **Parcel match:** point-in-polygon against held CAD/parcel layers; fuzzy fallback on situs address.
- **Contractor/operator identity:** trigram blocking + LLM adjudication for ambiguous pairs (the Burns L4 resolver pattern). License joins via TDLR pipeline already written.
- **PDF NLP:** OCR (tesseract/got-ocr) → structured extraction prompt → fields land with `lineage.method='pdf_extract'` and lower default confidence.

### 3.3 Orchestration

Hatchet (already on r730-2) for DAGs: ingest → dedup → normalize → geocode → classify → embed → aggregate. Cron stays for dumb pulls; Hatchet owns anything with retries/fan-out. Langfuse tracer (shipped, no-op until keys) for LLM observability; eval suite pattern from `evals/analyzer/` reused for classifier regression tests.

Embeddings: bge-m3 (or nomic-embed) served by Ollama on R730. Frontier models (claude -p subscription path) reserved for low-volume, high-stakes jobs: taxonomy design, eval grading, gnarly PDF layouts. Never metered API for bulk (standing rule).

---

## 4. Search & Discovery Experience

### 4.1 Core search

- **Natural language → structured query.** Qwen compiles "large commercial permits in Austin approved this week" into a validated filter JSON (Zod schema both ends). Never let the LLM write SQL against prod; it emits filters, the API builds the query. Show the compiled filters as removable chips so users learn the structured language and trust the translation.
- **Facets:** jurisdiction, category, status, value band, date, contractor, has-parcel, confidence band.
- **Fuzzy:** pg_trgm on address/contractor/description.
- **Geo:** bbox + radius + draw-a-polygon (PostGIS), "in this county/tract" via held boundary layers.
- **Hybrid semantic:** pgvector KNN blended with FTS rank for description search.
- **Saved searches → alerts:** persisted filter JSON; nightly (free) or 15-min (paid) diff via burns_events; delivery email/SMS/webhook.

### 4.2 Discovery surfaces

- **Map-first home.** Full-bleed MapLibre map, clustered permit points, layer toggles (permits, violations, hail swaths, wells, parcels). The Cedar Creek heat maps proved the pattern; make it the product's front door.
- **Heatmaps + momentum choropleths** at county/tract zoom (pre-baked PMTiles nightly).
- **Timeline view** per parcel/neighborhood: permits, sales, violations, storms on one axis.
- **Contractor dashboards:** volume, mix, jurisdictions, finaled ratio, risk score.
- **O&G mode:** basin map, W-1s this week, rig trajectory, operator activity tables.
- **AI summaries** on every detail page (pre-computed, not on-request: latency and cost).

### 4.3 UX notes

- Desktop: three-pane (filters / map / results list), URL-addressable state so every view is shareable and SEO-crawlable.
- Mobile: map with bottom-sheet results, mandatory responsive per house rules. Contractors live on phones; the alert→detail→call-the-owner flow is the mobile job to be done.
- Detail pages are server-rendered public HTML at the free tier: millions of long-tail pages ("permit P2026-1234, 123 Main St") is the SEO growth engine. This is how BuildZoom got traffic; we do it with better data.

---

## 5. Signature Features

| # | Feature | What it is | Why defensible |
|---|---|---|---|
| 1 | **Permit Pulse** | Live regional feed (WebSocket, /api/v2/ws exists) of new permits/wells as scrapers land them | 404 sources/day freshness; aggregators on quarterly batches can't follow |
| 2 | **Lead Engine** | Productized hail_leads/septic pattern: filter → score → skiptrace-ready list → CSV/CRM push | Already revenue-proven; data moat is the storm×permit×parcel join |
| 3 | **Rig & Permit Radar** | O&G: new W-1s, operator moves, disposal capacity, induced-seismicity risk per county | Free-data Enverus alternative; nobody serves the sub-$1K/mo O&G buyer |
| 4 | **Distress Index** | Code violations (308M rows) × tax delinquency × permit inactivity per parcel | Violations corpus at this scale is rare; investors pay for it today |
| 5 | **Permit DNA** | Embedding-similarity engine: "show me permits like this one" across jurisdictions | Requires embeddings on the full corpus; cheap for us, expensive for others |
| 6 | **Developer Radar** | Entity-resolved tracking of builders/operators across LLC shells | Burns L4 resolver + 99M business entities; identity resolution is genuinely hard |
| 7 | **Construction Momentum Index** | Monthly composite per county/metro, published openly | Citation magnet (press, researchers) → backlinks → SEO moat |
| 8 | **Inspection Predictor** | Pass/fail likelihood + scheduling cadence from historical inspection records | Needs deep historical inspection data; we scrape it, portals discard it |
| 9 | **Zoning Copilot** | Plain-English constraint answers per parcel (zoning + OSSF rules + floodplain) | parcels.ecbtx.com screening logic generalized; LLM is local so margin holds |
| 10 | **Watchlists** | Follow any parcel, contractor, operator, or polygon; event-driven notifications | Sticky; cancellation means losing your configured radar |
| 11 | **Permit-to-Completion Tracker** | Lifecycle state machine w/ stall detection ("applied 180d, no issue") | Cross-status normalization across 631 sources is the hidden hard part, done |
| 12 | **Septic Intelligence** | OSSF age/capacity/soil layer for pump-out & replacement demand | 165K-permit corpus nobody else assembled; proven internally at Mac Septic |
| 13 | **AI Permit Assistant** | Chat over the corpus (NL search + detail tools), free tier rate-limited | Local inference = free-tier economics competitors can't match |

Common thread: each feature is a thin product layer over a join or a corpus that already exists in the warehouse. The defensibility is never the feature code; it is the data assembly cost behind it.

---

## 6. API & Developer Platform

### 6.1 Surface

REST-first on the existing FastAPI app. Skip GraphQL for now: solo-maintained, and every serious data buyer asks for REST + bulk anyway. Revisit if enterprise demand materializes.

```
GET  /v1/permits?jurisdiction=austin-tx&category=commercial_ti&issued_after=2026-06-01&min_value=500000
GET  /v1/permits/{id}                      # full intelligence object
GET  /v1/parcels/{id}/timeline             # permits+violations+sales+storms
GET  /v1/contractors/{id}                  # dossier + risk score
GET  /v1/wells?state=tx&county=midland&permitted_after=2026-06-01
GET  /v1/operators/{id}/activity
POST /v1/search                            # NL or structured filter JSON
POST /v1/webhooks                          # filter JSON + target URL, HMAC-signed
GET  /v1/exports/{dataset}?format=ndjson   # bulk, presigned MinIO URLs
GET  /v1/stats/momentum?geo=county:48453
```

Static routes before catch-all `/{id}` routes (house rule). Example payload:

```json
{
  "permit_id": "0b9c16ba-7e2f-44e6-b3cd-298c5b4be05c",
  "jurisdiction": {"name": "Austin, TX", "fips": "4805000"},
  "source_record_id": "2026-058221 PP",
  "category": "commercial_ti",
  "description_ai": "4,200 sqft restaurant build-out, $610K declared, Burnet Rd corridor.",
  "status": "issued",
  "issued_date": "2026-06-04",
  "declared_value": 610000,
  "estimated_value": {"low": 540000, "high": 720000},
  "address": "6800 Burnet Rd, Austin, TX 78757",
  "location": {"lat": 30.3402, "lon": -97.7405, "geocode_confidence": 0.97},
  "parcel_id": "…", "contractor": {"id": "…", "name": "…", "risk_score": 0.18},
  "complexity_score": 0.62, "confidence_score": 0.91,
  "freshness_at": "2026-06-09T03:12:00Z",
  "links": {"comparables": "/v1/permits/…/comparables", "documents": "…"}
}
```

### 6.2 Auth, limits, tiers

API keys (hashed at rest), per-key rate limits, usage metering in Postgres. Cookie auth stays for the web app; keys for the API (Bearer is fine here; the cookie-only rule is the CRM's, not the data API's). Public tier: 1K req/day, 30-day data delay, attribution required. Paid tiers: full freshness, webhooks, bulk.

### 6.3 SDKs and DX

OpenAPI spec → generated TypeScript + Python clients (fern or openapi-generator; don't hand-write). Docs site with runnable examples. The free tier with delayed data is the developer acquisition funnel; researchers and journalists publish with attribution and that compounds.

---

## 7. Technical Stack

Principle: keep what works, harden the serving path, add as little as possible.

| Layer | Current | Recommendation |
|---|---|---|
| Frontend | React 19 + Vite + Tailwind 4 patterns established | Same stack. MapLibre GL + deck.gl for heavy layers. Public detail pages pre-rendered (Astro or SSR route) for SEO. Cloudflare Pages. |
| API | FastAPI on Railway | Keep as edge. Add read replica routing to r730-2 so heavy queries never touch ingest. Fix the Railway webhook (known broken) before launch week, not during. |
| Primary DB | T430 PG18 (ingest), r730-2 PG18.3 (serve) | Formalize: T430 = ingest/staging, r730-2 = canonical + serving, logical replication between. Partition canonical.permits by issued_date year. |
| Search | none dedicated | Postgres-native first: tsvector + pg_trgm + pgvector HNSW. Defer OpenSearch until p95 > ~500ms at real load; a search cluster is ops weight that solo ops shouldn't carry early. |
| Geo | PostGIS | Keep. Nightly PMTiles bakes for static layers, martin for live tiles. |
| Object storage | filesystem | MinIO on r730-2/dataPool. |
| Queue/orchestration | cron + Hatchet + burns_events | Hatchet for DAGs and webhook fan-out; cron only for dumb pulls. |
| Enrichment compute | R730 Ollama (Qwen3.5:122b) | Keep. Add bge-m3 embeddings. claude -p for low-volume frontier jobs. No metered bulk API (standing rule). |
| Observability | Langfuse (keys pending), 20s statement_timeout | Add Prometheus + Grafana (scraper lag per source, queue depth, p95 by endpoint), uptime checks on /health, per-source freshness dashboard. The freshness dashboard IS a product feature too (status page builds buyer trust). |
| Backups | partial | Nightly pg_dump of canonical to MinIO + offsite copy. Non-negotiable before charging money. |

Cost note: incremental infra spend to launch ≈ $0 beyond power and existing Railway. That line belongs in every investor/pricing conversation.

---

## 8. Competitive Positioning

| Competitor class | Examples | Their weakness | Our edge |
|---|---|---|---|
| Gov portals | Accela/OpenGov public sites | One jurisdiction at a time, terrible search | We aggregate 631 of them with one query surface |
| Permit aggregators | Shovels.ai, BuildZoom | Permits only, monthly-ish refresh, no adjacent layers | Daily freshness + violations/storms/parcels/wells joins |
| Construction intelligence | Dodge, ConstructConnect | $$$, commercial bid focus, no residential/trades | Serve the trades + SMB segment they ignore |
| Property data | ATTOM, CoreLogic, Regrid | Parcel-centric, permits an afterthought, enterprise pricing | Permit-native, transparent pricing, confidence scores exposed |
| O&G intelligence | Enverus, S&P/IHS | $30K+/seat | Free-source coverage at 1% of the price for the long tail of buyers |

Moats, ranked by real durability:

1. **Cost structure.** Sovereign hardware + local LLM = near-zero marginal enrichment cost. Competitors paying per-token and per-seat cannot match a generous free tier or $99 entry pricing without losing money.
2. **Scraper estate.** 631 working adapters is years of accumulated breakage-fixing. It is boring, unglamorous, and very hard to replicate quickly.
3. **Cross-domain corpus.** The joins (violations × permits × storms × wells) compound: each new layer multiplies the value of existing ones.
4. **Entity graph.** Resolved contractors/operators/owners across LLC shells gets better with every record and is not purchasable.
5. **SEO surface.** Millions of public record pages, first-mover on long-tail queries.

---

## 9. Monetization

Lead with what already sells. Lead lists are proven revenue; subscriptions and API follow.

| Stream | Product | Price | Segment |
|---|---|---|---|
| Lead gen | Vertical lead lists + alert feeds (roofing/hail, septic, remodel, solar, O&G services) | $99 to $499/mo per metro per vertical; skiptraced lists at $0.10 to $0.25/record | Trade contractors. Sell what we already sell internally. |
| Pro subscription | Full search, watchlists, alerts, dashboards, exports | $49/mo individual, $149/mo team | Investors, small developers, agents |
| O&G Radar | Rig & Permit Radar + operator dossiers + disposal/seismicity layers | $199 to $499/mo | O&G service/supply, mineral buyers, landmen |
| API | Metered: $99/mo (50K calls) → $499/mo (500K + webhooks) → enterprise bulk | usage-based | Proptech, insurtech, lenders |
| Reports | Quarterly momentum reports per metro; custom pulls | $500 to $5K one-off | Suppliers, PE, economists. Free summary versions = marketing. |
| White-label | Municipal/regional dashboards | $500 to $2K/mo | EDCs, COGs, counties. Pursue inbound only; gov sales cycles will eat a solo operator. |

Free tier: 30-day-delayed data, public detail pages, momentum index. It is the growth engine, not lost revenue; the data is public record anyway, the freshness and intelligence are what's paid.

Realistic ramp framing: first milestone is 10 paying lead-gen accounts and 3 API customers (~$3K to $5K MRR), not a TAM slide. The architecture supports nationwide; the go-to-market starts where the data is deepest (TX/TN/SC + Permian).

---

## 10. 100-Day Execution Plan

Staffing reality: Will + Claude agents + David Gibson (analysis, read-only). No hires. Each phase lists the parallel tracks per the house parallelism rule. Every UI change ships with Playwright verification; every push gets `railway status` checked.

### Days 1–30: Foundation + O&G land-grab

| Track | Work |
|---|---|
| A. Canonical layer | `canonical.permits/parcels/contractors/entities/jurisdictions` schema + Alembic migrations; backfill top 50 sources via Hatchet DAG (dedup → normalize → geocode) |
| B. O&G acquisition (parallel, mostly downloads) | RRC full sweep (W-1 daily cron, PDQ dump, wellbore digest, P-5, H-10, T-4), FracFocus dump, TexNet, NM OCD, OK OCC, EIA, Baker Hughes. Land on /mnt/win11 + /dataPool, load `wells/well_permits/production_monthly/operators` |
| C. Serving path | r730-2 read-replica routing in permit-api; fix Railway GitHub webhook; MinIO up; nightly canonical backup |
| D. MVP search | /v1/permits + /v1/wells with facets + geo + trigram; map-first UI v1 (MapLibre, clustered points, filter pane) on permits.ecbtx.com |

Exit criteria: canonical layer serving 50 sources + full TX O&G stack queryable on a map.

### Days 31–60: Intelligence layer

| Track | Work |
|---|---|
| A. Enrichment at scale | Category classifier + summaries through Qwen queue (backfill TX first); bge-m3 embeddings; comps endpoint; eval suite for classifier (reuse evals/ pattern) |
| B. Accounts + alerts | Auth, saved searches, watchlists, alert delivery (email/SMS via existing Twilio/RC plumbing), Permit Pulse WebSocket feed |
| C. Analytics | Momentum index MVs, heatmap PMTiles bake, contractor dashboards, Distress Index v1 (violations × permits) |
| D. API beta | Keys, metering, rate limits, OpenAPI docs, NDJSON bulk exports; onboard 3 to 5 design partners hand-picked from existing network (roofers from hail leads, Rebound Dynamics contact for O&G radar feedback) |

Exit criteria: a stranger can sign up, save a search, get an alert, and pull the API.

### Days 61–100: Productize + first revenue

| Track | Work |
|---|---|
| A. Lead Engine GA | Self-serve list builder (filter → score → export/CRM push), Stripe billing, per-metro vertical packages |
| B. O&G Radar GA | Operator dossiers, disposal/seismicity layer, weekly digest email; price at $199 to start |
| C. SEO surface | Server-rendered public pages for permits/parcels/contractors (start with TX metros), sitemaps, momentum index published monthly as the PR hook |
| D. Hardening | Prometheus/Grafana, per-source freshness status page, load test search p95, decide on OpenSearch only if numbers force it |
| E. Launch loop | Pricing page live, 10 paying targets from existing contacts, momentum report #1 to local press/economists |

Priority order if anything slips: A-track canonical layer and O&G acquisition never slip (they are the asset); SEO and white-label slip first.

### Standing risks

1. **Scraper estate fragility.** 631 sources rot continuously. Budget ~20% of all engineering time to breakage forever; the freshness dashboard makes rot visible before customers do.
2. **ToS posture.** Public-records aggregation is defensible; keep attribution clean, honor robots where feasible, and never resell licensed third-party data (MLS, BatchData outputs) through the API.
3. **Solo-operator concentration.** Everything above is sized for one operator + agents. Resist any feature that adds a 24/7 ops burden (hence: no OpenSearch cluster, no GraphQL, no gov sales motion yet).
