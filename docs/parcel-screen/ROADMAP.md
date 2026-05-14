# Parcel Screen Roadmap

**Audience:** Will Burns, Rob Caputo. The permanent "what's next?" reference for parcels.ecbtx.com.

**TL;DR:** Four tiers, prioritized by impact and effort. Tier 1 is shipped or shipping this week. Tier 2 is the next wave of high-impact, medium-effort builds. Tier 3 is the big-lift, high-wow stretch goals. Tier 4 is the workflow and collaboration layer that turns the tool from a screener into a deal-management platform.

**How to use this roadmap:** Ping Will to bump priorities or call dibs on the next build. Items move tiers as scope shifts. Shipped items keep their shipped date inline so we can read the release history without spelunking git. Treat this as a living backlog, not a contract.

---

## Tier 1 (Shipped or shipping this week)

The foundation. Phase 1 plus the three Discover / Hunt / Map ladders, plus the statewide-overlay and Inland-Empire-expansion work happening this turn.

- **Parcel Screen page (Rialto + Santa Ana).** Type or paste an address or APN, get the full memo: parcel facts, state-law eligibility table, verify checklist, satellite map, permit history. The exact behavior of Rob's standalone skill, hosted. `Effort:` shipped. `Status:` Shipped (Phase 1, early May 2026).

- **Interactive ParcelMap on the result page.** Google-satellite base layer with the parcel polygon overlaid, zoom-aware, click-through to a larger view. Closes the "is this the right lot" gut-check in one glance. `Effort:` shipped. `Status:` Shipped (Phase 1, early May 2026).

- **Ladder 1: Discover leaderboard.** Pre-ranked Hot Picks of every parcel in a registered city, sorted by max possible CA state-law unit yield. Filter by path, min yield, limit. 23,930 Rialto parcels pre-scored and ready. Turns the tool from "look up one address" into "show me the best 50 lots in town." `Effort:` shipped. `Status:` Shipped (May 2026).

- **Ladder 2: Hunt natural-language query.** POST /v1/parcel-screen/hunt with Claude Haiku translating English to SQL filter. "Vacant SFR lots in Rialto over 0.5 acres that might qualify for SB-1123" returns a ranked top-20 with a one-line rationale per pick. `Effort:` shipped. `Status:` Shipped (May 2026).

- **Ladder 3: Map view.** Full-bleed MapLibre satellite map of a city with every candidate parcel colored by max-yield tier (grey, blue, green, gold, red). Click a parcel, side panel opens, one click to Screen. The "I want to look around" surface. `Effort:` shipped. `Status:` Shipped (May 2026).

- **Eight state-law markdown files plus density tables.** Rob's canonical state-housing-law cards (SB-9, SB-684, SB-1123, AB-2011, AB-1287, AB-130, density bonus, etc.) loaded into the eligibility engine. Density tables for Rialto A-1 and R-1A wired up. `Effort:` shipped. `Status:` Shipped (Phase 1).

- **Permit history cross-reference by APN.** Every screen joins against the 35M-row CA permits table on APN, surfaces prior building activity on the lot. Unique to the hosted version; Rob's standalone skill cannot do this. `Effort:` shipped. `Status:` Shipped (Phase 1).

- **Allowlist auth plus per-user audit log.** Passwordless email login, session API key in localStorage, allowlist gated by `PARCEL_SCREEN_ALLOWED_USERS` env var. Every screen run gets saved to `parcel_screens` so we have a full history per user. `Effort:` shipped. `Status:` Shipped (Phase 1).

- **Statewide CA exclusion overlays.** Live per-parcel point-queries against CalFire FHSZ, Alquist-Priolo earthquake fault zones, FMMP farmland, OHP / NRHP historic resources, FEMA SFHA flood. Each layer turns a "verify with planning" flag into an automatic yes-or-no. `Effort:` ~half a day per layer. `Status:` In Progress (May 2026).

- **Five new Inland Empire cities.** San Bernardino, Fontana, Colton, Riverside, Ontario. Esri endpoint discovery, jurisdiction seed, density-table backfill where available, hot-picks refresh. Expands the Discover / Hunt / Map surface from 1 city to 6. `Effort:` ~30 min discovery per city plus refresh time. `Status:` In Progress (May 2026).

---

## Tier 2 (Queued: high impact, medium effort)

The next wave. Each of these closes a real gap in the current workflow. None are speculative.

- **Saved searches plus alert digests.** Hunt page gets a "star this search" button. Server stores the natural-language query plus filters. Daily or weekly email digest fires when new matches appear (or pre-existing matches change state). Turns Hunt from one-shot into a watchlist. `Effort:` 1-2 days. `Status:` Queued.

- **CSV and PDF export.** Any screen result, any Hot Picks filtered list, any Hunt result set: one-click export. CSV for spreadsheet jockeys, PDF for sending to GCs and lenders. `Effort:` 1 day. `Status:` Queued.

- **Owner enrichment (phone plus email).** BatchData skip-trace integration attaches owner contact info (up to 5 phones with confidence + DNC flags, emails, mailing address, age) to each parcel. 90-day cache in `parcel_owner_enrichment` keyed on (state, city_slug, apn) so re-screens are free. Daily soft cap (50/user/day default) via `PARCEL_ENRICH_DAILY_CAP`. Mailing-address-different-from-site badge surfaces absentee-owner signal. Closes the screen-to-call gap. `Effort:` shipped in one day. `Status:` Shipped (2026-05-13).

- **SB-684 qualifying-infill 75% perimeter spatial test.** Right now SB-684 flags as "verify" for non-MF zones because the 75%-developed-perimeter test is not automated. Add the spatial query against neighboring parcels and infill candidates flip to auto-eligible. `Effort:` 3-4 hours. `Status:` Queued.

- **Adjacent-parcel permit activity.** Cross-reference the permits table against every parcel within 500 ft of the subject. Surfaces neighborhood-velocity signal: "5 ADUs permitted on this block in the last 18 months" is a real buy signal. Leverages the 35M-row permits table the hosted version already owns. `Effort:` 1 day. `Status:` Queued.

- **Tax delinquency and foreclosure flagging.** Pull county treasurer delinquent-tax rolls plus pre-foreclosure / NOD data where available. Surfaces motivated-seller signal alongside the yield math. `Effort:` 2-3 days (data source per county varies). `Status:` Queued.

- **Building footprint and sqft.** Pull building-footprint geometry from county GIS or Microsoft Building Footprints. Compute existing structure sqft. Feeds commercial demo-cost estimation and "tear-down candidate" scoring. `Effort:` 1-2 days. `Status:` Queued.

---

## Tier 3 (Stretch: big lifts, high wow)

The aspirational layer. Each of these is a meaningful build, but the "wow" payoff per item is high. Pick one, ship it, get an outsized demo win.

- **AI-generated rendering of proposed units.** Take the satellite tile plus the parcel polygon, feed it to a vision model (Replicate, Gemini Vision, or similar) with a prompt like "render 4 modern detached SFR units on this lot." Output a photoreal mockup right in the memo. The "bad ass pool-contractor mockup" moment. `Effort:` several days plus per-render API cost. `Status:` Stretch.

- **Lite pro-forma layer.** Estimated land cost (comp-pulled), hard cost per sqft by region, soft costs, financing assumption. Output a rough acquisition + dev + exit basis right under the eligibility table. Turns the screen into a go-or-no-go financial snapshot. `Effort:` 3-5 days for v1 (config-driven assumptions, comp pull is the hard part). `Status:` Stretch.

- **3D massing view.** MapLibre 3D with extruded building footprints capped at the allowed max height for the eligible path. "Here is what 35 ft of by-right SB-9 looks like on this lot" beats any spec sheet. `Effort:` 2-3 days. `Status:` Stretch.

- **Auto-discovery worker for new cities.** Server-side headless Chrome that runs Rob's Esri-endpoint discovery technique on demand. User types a new city name, worker scrapes the GIS viewer, infers parcels / zoning / GP / fire layers, drops a draft jurisdiction row for Will to approve. Cuts new-city onboarding from 30 minutes to 30 seconds. `Effort:` 3-4 days (browser automation is the bulk; the Chrome MCP pattern is already proven). `Status:` Stretch.

---

## Tier 4 (Stretch: workflow and collaboration)

This is the layer that turns the tool from a screener into a CRM-light deal-management platform. None of these are core to the screening engine, but each one extends the tool's usefulness past the initial screen.

- **Pipeline tags.** Per-parcel tag: Looking / Under Contract / Acquired / Passed. Filter the History page by tag. Adds light deal-pipeline tracking without standing up a separate CRM. `Effort:` 1 day. `Status:` Stretch.

- **Share-a-screen read-only links.** Generate a tokenized URL for a saved memo that anyone can open (GCs, lenders, partners, equity) without an account. Memo is read-only, no further screens, no other accounts visible. `Effort:` 1-2 days. `Status:` Stretch.

- **HOA, CC&R, easement lookups.** Surface known deal-killer signals: HOA covenants that ban subdivision, recorded easements on the polygon, CC&R restrictions. Data source varies by county (recorder's office, parcel attribute fields, sometimes Title-Pro). `Effort:` 3-5 days (data sourcing is the bulk). `Status:` Stretch.

- **Comp pulling.** Zillow or Redfin exit-value pull for the subject parcel and recent neighborhood comps. Feeds the pro-forma layer; also useful standalone for sanity-checking exit assumptions. `Effort:` 2-3 days (depends on data source legality and rate limits). `Status:` Stretch.

- **Mobile drive-by mode.** Geolocation-aware "what is around me" view: phone in hand, walking or driving a target area, the tool shows nearby Hot Picks colored by tier. Turns on-site scouting from "remember the address" into "tap to screen." `Effort:` 2-3 days (mostly responsive UI plus geolocation). `Status:` Stretch.

---

## Notes

- **The 35M-row CA permits cross-reference is unique to the hosted version.** Rob's standalone skill does not have this. Every Tier 2 item that touches permit-history (adjacent-parcel activity, neighborhood velocity) is a built-in advantage of staying on the hosted platform.

- **All shipped state-law files live in `data/parcel-screen/state-law/*.md`.** Keep the `last_verified` dates current. If a chaptered text changes (AB-130 trailer bill is the obvious near-term watch), update the markdown and bump the corresponding dict in `scripts/seed_parcel_screen.py`.

- **Allowlist gate is on by default.** New accounts need a UUID added to the `PARCEL_SCREEN_ALLOWED_USERS` env var by Will. There is no self-serve signup yet, by design. Revisit if and when the user list passes ~10.

---

*Last updated 2026-05-13.*
