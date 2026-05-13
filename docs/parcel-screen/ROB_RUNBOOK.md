# Rob's Parcel Screen Runbook

**Audience:** Rob Caputo. Hosted version of his Claude Code parcel-screen skill.

**TL;DR:** The whole thing lives at https://parcels.ecbtx.com. Will keeps it running. You can read all the code at https://github.com/wburns02/permit-api and https://github.com/wburns02/parcels-web. This doc covers what you can do yourself (add new cities, update state-law files, re-rank parcels) and what to ask Will for (Railway changes, custom domains, new accounts).

---

## 1. What you can do at parcels.ecbtx.com

Five surfaces, all share the same engine + database:

| Page | URL hash | What it does |
|---|---|---|
| **Screen** | `/` | The exact thing your skill does. Type or paste an address/APN, get the full memo: parcel facts, state-law eligibility table, verify checklist, satellite map, interactive Google Maps embed, permit history. |
| **Discover** | `/discover` | Pre-ranked leaderboard of all parcels in a registered city, sorted by max possible CA state-law unit yield. Filter by path / min yield / limit. One-click "Screen this" on any row. |
| **Hunt** | `/hunt` | Natural-language. "Vacant SFR lots in Rialto over 0.5 acres that might qualify for SB-1123." Claude Haiku translates → SQL filter → ranked top-20 with a one-line rationale per pick. |
| **Map** | `/map` | Full-bleed MapLibre satellite map of a city with every candidate parcel colored by max-yield tier (grey/blue/green/gold/red). Click any parcel → side panel → Screen this. |
| **History** | `/history` | Every screen you run gets saved. Click any row to re-open the saved memo. |

**Login:** passwordless — you enter your email (`caputo.robert@gmail.com`), it gives you a session API key stored in localStorage, you stay logged in until you click logout.

**Access:** restricted to allowlisted accounts. Right now: you + Will. New accounts need Will to add the UUID to the `PARCEL_SCREEN_ALLOWED_USERS` env var.

---

## 2. How the engine works

Five Postgres tables on T430 (one of Will's servers in Texas):

| Table | What's in it |
|---|---|
| `parcel_jurisdictions` | Cached Esri GIS REST endpoints per city (parcels_url, zoning_url, general_plan_url, fire_hazard_url, apn_field, spatial_reference_wkid, notes). One row per registered city. **Today: Rialto, Santa Ana.** |
| `parcel_zone_density` | Cached `du_per_ac` + dimensional standards per zone, per city. From your `density-tables/*.json` files. **Today: Rialto A-1, R-1A.** |
| `parcel_state_laws` | All 9 CA state-housing-law cards (your `state-law/*.md` files, JSON-converted). Eligibility checklist + yield formula. |
| `parcel_hot_picks` | Pre-scored bulk leaderboard. One row per parcel in each registered city, with computed `max_units`, `best_path`, `eligible_paths[]`, and full polygon geometry. **Today: 23,930 Rialto rows.** |
| `parcel_screens` | Audit log of every individual screen run. The full memo JSON is saved here. |

**Flow when you Screen one parcel:**
```
your input → lookup jurisdiction → Esri REST query (live) → normalize facts →
  spatial-join zoning/GP layer if needed → look up zone_density →
  run eligibility engine against parcel_state_laws → compute yield per law →
  join permits table by APN → save to parcel_screens → return memo
```

**Flow when Hot Picks / Hunt / Map runs:**
```
your input → query parcel_hot_picks (already populated by the nightly refresh) →
  apply filters (or LLM-translated filters for Hunt) → return top N
```

The nightly bulk refresh script (`scripts/refresh_hot_picks.py`) is what fills `parcel_hot_picks`. It pages through every parcel from a city's Esri server (~24K rows for Rialto), runs the same eligibility engine on each, and upserts the result.

---

## 3. How to add a new city

This is the most-likely thing you'll want to do yourself. Three steps. ~30 minutes for a new city.

### Step 3a — Discover the city's Esri REST endpoints

This is the technique from your original skill. Find the city's public GIS viewer, open it in Chrome MCP, run:

```javascript
async () => {
  const perf = performance.getEntriesByType('resource')
    .map(e => e.name)
    .filter(u => /FeatureServer|MapServer/i.test(u));
  return [...new Set(perf)];
}
```

You're looking for the URLs of:
- **Parcels** layer (must have APN attribute or address)
- **Zoning** layer
- **General Plan** layer
- **Specific Plan** layer (if any)
- **Fire Hazard** layer (if any)

Also note:
- The `apn_field` name on the parcel layer (some cities use `APN`, some `apn`, some `ASSESSMENT_NO`)
- The `spatialReference.wkid` (look at the response from a feature query — Rialto is 102100 Web Mercator, Santa Ana is 2230 NAD83 CA State Plane)
- Whether the parcel layer also carries `zone_code` / `gp_code` directly (Rialto does — Santa Ana doesn't, requires spatial join)

### Step 3b — Add the jurisdiction row

Two options:

**Option A — Pull request to the seed script** (recommended, version-controlled):
- Edit `scripts/seed_parcel_screen.py` on the `wburns02/permit-api` repo.
- Add a new dict to the `JURISDICTIONS` list. Use Rialto/Santa Ana entries as templates.
- Open a PR. Will merges it. The seed re-runs on next deploy and the city shows up.

**Option B — Direct SQL via Will** (faster, one-off):
- Ping Will with the endpoint URLs and city name.
- He runs an `INSERT INTO parcel_jurisdictions ...` against T430.

### Step 3c — Refresh hot-picks for the new city

Once the jurisdiction is registered, run the refresh:

```bash
# Will runs this — he has the SSH access:
ssh will@100.85.99.69 'cd /home/will/permit-api-live && PYTHONPATH=. python3 scripts/refresh_hot_picks.py --city <new_city_slug> 2>&1 | tail -10'
```

Takes ~20-60 seconds per city depending on parcel count. Once it finishes, the new city shows up in the Discover / Hunt / Map dropdowns.

If you want the new city scraped on a schedule: ping Will to add it to the nightly cron.

### Density tables (the optional but valuable third step)

If you have a `density-tables/<city>.json` from your skill, drop it into `data/parcel-screen/density-tables/` on the repo, open a PR. The seed script auto-picks it up. Without a density table for a city, the by-right yield math falls back to a default (1 du/ac), so it's worth doing for accuracy.

---

## 4. How to refresh / update a state law

Your 8 state-law `.md` files are committed to `data/parcel-screen/state-law/` in the `permit-api` repo. The structured JSON that the engine actually uses lives inline in `scripts/seed_parcel_screen.py` (the `STATE_LAWS` list).

**To update a law (e.g., AB-130 chaptered text changes):**

1. Edit the `.md` file in `data/parcel-screen/state-law/`. This is the canonical reference.
2. Update the corresponding dict in `scripts/seed_parcel_screen.py` so the engine's eligibility checklist + yield formula match your new text.
3. Bump `last_verified` to today's date on that dict.
4. Open a PR. Will merges + re-runs the seed.

**If you add a NEW state-law file** (say SB-XXX-2026 ships next year):
1. Drop a new `.md` in `data/parcel-screen/state-law/`.
2. Add a new dict to `STATE_LAWS` in the seed (use SB-684 as the template — it's fully populated).
3. (Optional) Add auto-check logic to `app/services/parcel_screen_service.py` `_run_auto_check` if the new law has eligibility criteria you can compute from GIS facts.

---

## 5. How to refresh hot-picks

Two ways:

**Manual (one city):**
```bash
ssh will@100.85.99.69 'cd /home/will/permit-api-live && PYTHONPATH=. python3 scripts/refresh_hot_picks.py --city rialto 2>&1 | tail -10'
```

**Cron** (eventually — not set up yet, ask Will when you're ready):
Will adds something like `0 4 * * * cd /home/will/permit-api-live && PYTHONPATH=. python3 scripts/refresh_hot_picks.py --all >> /tmp/parcel_refresh.log 2>&1` to the R730 crontab. Refreshes nightly.

The refresh is **idempotent** — re-running for the same city UPSERTs the rows, deletes anything no longer present (so removals propagate). Wall-clock is ~25s for Rialto's 24K parcels; expect proportional time for bigger cities.

---

## 6. What's deferred (Phase 2 / 3 / later)

These are the genuine accuracy gaps in the current tool. None of them block daily use, but each one closes a "verify with planning" item:

| Gap | Impact | Effort to fix |
|---|---|---|
| **CA statewide exclusion overlays** (CalFire FHSZ, Alquist-Priolo, FMMP, OHP, historic) | ~5 verify items per law could be auto-resolved instead of flagged | ~half a day per layer once Postgres has PostGIS enabled |
| **SB-684 qualifying-infill 75% perimeter test** | SB-684 currently flags as "verify" for non-MF zones. Adding the spatial test makes it auto-eligible for parcels that pass. | ~3-4 hours — straight spatial query against neighboring parcels |
| **AB-130 chaptered-text verification** | Carries a stale-warning flag because the 2025 budget trailer bill is near the model cutoff | Watch leginfo for amendments; bump `last_verified` |
| **AI rendering of new units** | The "bad ass pool-contractor mockup" stretch goal | Several days + Gemini or Replicate API costs |
| **More cities** | Currently Rialto-only for hot-picks. Santa Ana's parcels via OC are too thin for the bulk scrape | Each new city: ~30 min discovery + refresh |

---

## 7. When to ask Will (vs. self-serve)

**Self-serve via GitHub PRs (you've got read access):**
- New state-law `.md` file or update
- New density table `.json` file
- New jurisdiction in `JURISDICTIONS` seed
- Engine improvements (new auto-checks, better yield math)
- Frontend tweaks (DiscoverPage, HuntPage, etc.)

**Ask Will:**
- New person needs allowlist access (he adds UUID to env var)
- Custom-domain or DNS changes
- Railway environment variable changes (Anthropic key rotation, etc.)
- New cron schedule
- Manual one-off SQL on T430
- Anything where the production service goes down

**His contact:** willwalterburns@gmail.com, +1-979-236-1958

---

## 8. The repos

- **Backend:** https://github.com/wburns02/permit-api
  - `app/api/v1/parcel_screen.py` — endpoints
  - `app/services/parcel_screen_service.py` — single-parcel screen engine
  - `app/services/parcel_hot_picks.py` — bulk scraper + scorer
  - `app/models/parcel_screen.py` — DB tables
  - `scripts/refresh_hot_picks.py` — CLI refresh
  - `scripts/seed_parcel_screen.py` — jurisdictions + state laws + density tables
  - `data/parcel-screen/state-law/*.md` — your canonical law files
  - `data/parcel-screen/density-tables/*.json` — your density tables

- **Frontend:** https://github.com/wburns02/parcels-web
  - `src/pages/{ScreenPage,DiscoverPage,HuntPage,MapPage,HistoryPage,ScreenDetailPage}.tsx`
  - `src/components/ParcelMap.tsx` — the satellite + interactive map on the Screen result page
  - `src/api.ts` — TanStack Query hooks for every endpoint

---

## 9. If you ever want to fully self-host

When that day comes, ping Will. The setup is non-trivial (Railway account + Postgres provisioning + DNS + Cloudflare Pages + API tokens + env vars + DB migration) but very doable in an afternoon with a real runbook. Today's setup is the right answer because:

1. You inherit the 35M-row CA permits cross-reference for free (you'd lose this in a self-host).
2. Will keeps shipping improvements; you benefit automatically.
3. You don't burn time on ops.

If/when monetization, team handoff, or full independence becomes the goal — that's the right moment for the self-host migration.

---

*Last updated 2026-05-13 — Phase 1 + Ladders 1/2/3 shipped.*
