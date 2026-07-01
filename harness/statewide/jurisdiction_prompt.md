# Task: obtain building permits for ONE Texas jurisdiction and load a sample

You are running inside the `permit-api` repo (cwd is the repo root), so you have
the full existing scraper framework, the `psql` client, and Playwright available.
You are processing exactly ONE jurisdiction:

- **Jurisdiction:** {{NAME}} ({{JTYPE}})
- **FIPS:** {{FIPS}}
- **Suspected portal:** {{PORTAL_URL}}
- **A-priori vendor guess:** {{VENDOR}}  (re-verify this yourself; it may be wrong)
- **source_tag (use EXACTLY this as `hot_leads.source`):** `{{SOURCE_TAG}}`

## The one hard rule (from Will)

**NEVER declare a jurisdiction "unavailable" on a shallow check.** Public
building-permit data is almost always obtainable. A 403, a captcha, an SPA that
renders nothing, or a "no public search" page is the START of the hunt, not the
end. You only return `walled` after you have genuinely EXHAUSTED the options
below and can name the specific barrier.

The ONE legitimate clean "walled" case: a **Texas county**. Under Texas Local
Government Code Ch. 233, counties generally CANNOT issue residential building
permits in unincorporated areas. If this is a county and you confirm it issues
no building permits (only floodplain/septic/development permits), return
`walled` with barrier `"TX county — no residential building permits (Ch.233)"`.
That is the CORRECT answer for most counties, not a failure.

## Step 1 — Classify the vendor

Visit the permits page. Identify the portal software. Common vendors and the
fingerprint to look for:

- **Socrata** — `*.data.*.gov` / `/resource/<id>.json` open-data endpoint.
- **OpenGov / ViewPoint Cloud** — `*.viewpointcloud.com`, Auth0 login.
- **MGO Connect** — `mygovonline.com`, jurisdictionID-based API.
- **ArcGIS** — `*/rest/services/.../FeatureServer` or `MapServer`.
- **Accela Citizen Access** — `aca-prod.accela.com/<AGENCY>/`.
- **eTRAKiT** — `etrakit.*` path, ASP.NET viewstate.
- **Tyler EnerGov / SelfService** — `/energov*/selfservice`, `/EnerGovProd`.
- **CentralSquare Click2Gov** — `click2gov`, `/CommunityDevelopment`.
- **CitizenServe** — `citizenserve.com`.
- **Infor / Hansen / Rhythm** — city-hosted, often no public search.

## Step 2 — ROUTE

### Known vendor WITH an existing universal adapter -> config + run (do NOT write new code)

If the vendor matches one we already adapt, your job is a **config entry + a
run**, not a new scraper. Use the existing scripts:

- **Socrata** -> `scripts/scrape_all_metros_daily.py` pattern. You can also just
  `curl` the `/resource/<id>.json?$limit=200&$where=...` endpoint directly to
  fetch a sample, then load it.
- **OpenGov** -> `python3 scripts/scrape_opengov.py --community <slug> --dry-run`
  to preview, then load. The slug is the subdomain (e.g. `conroetx`).
- **MGO** -> `scripts/scrape_mgo_ctx.py` (find the jurisdictionID via its
  `/api/v3/cp/public/jurisdictions` listing).
- **ArcGIS** -> `python3 scripts/scrape_arcgis_permits.py` against the
  FeatureServer layer; query `?where=1=1&outFields=*&f=json&resultRecordCount=200`.

Pull EVERY permit in the recent window (default: issued or created in the last
180 days). **PAGINATE TO EXHAUSTION** — follow the API's next-page / offset /
`resultOffset` / cursor and keep fetching until a page returns fewer rows than the
page size. Do NOT stop at page one, and do NOT cap the result at a round number:
landing on exactly 200 / 500 / 1000 is the tell that pagination was skipped. If a
jurisdiction is genuinely high-volume, bound the pull by the date window, not by a
row ceiling. Then go to Step 3.

**Write any loader you create defensively.** Never let an HTTP/JSON hiccup raise
an unhandled exception: check `resp.status_code` before `resp.json()`, wrap the
parse in try/except, and on ANY fetch/parse failure print the JSON object from
Step 4 with `"status": "walled"` and a precise `barrier_if_walled`, then
`sys.exit(0)`. A walled jurisdiction is a clean outcome; a traceback is not.
Do NOT spawn subagents — do this work yourself in this one session.

### Unknown / walled vendor -> EXHAUST the hunt

Try these IN ORDER and stop at the first that yields real permit rows:

1. **Open-data backdoor.** Search `data.<city>.gov`, the state open-data hub,
   and Socrata/ArcGIS Hub for that city's permit dataset. Many "walled" Accela/
   eTRAKiT cities ALSO publish the same data as open data.
2. **Captured XHR.** If it's an SPA (Accela ACA, EnerGov SelfService, eTRAKiT),
   open it in Playwright, run a permit search, and capture the JSON/XHR the page
   calls (`browser_network_requests`). Replay that endpoint directly.
3. **ArcGIS / city GIS.** Many cities expose the same permit layer on their GIS
   FeatureServer even when the "portal" is gated. Probe `gis.<city>.gov` /
   `maps.<city>.gov` `/rest/services`.
4. **CAD eSearch / appraisal district** as a permit proxy if the city itself is
   truly dark (last resort; note it as the source).
5. **Proxy + 2Captcha** for hard-captcha portals (the repo has a 2captcha
   solver pattern — `scripts/ebr_captcha_token.py`). Only if 1-4 fail.

If ALL of the above genuinely fail, return `walled` and name the barrier
precisely (e.g. `"Accela ACA, XHR returns 403 behind Akamai, no open-data
mirror found"`).

## Step 3 — LOAD a sample into hot_leads

Insert the sample rows into the `hot_leads` table on the permits DB. **Tag every
row with `source = '{{SOURCE_TAG}}'`** — this exact string. The verifier keys on
it. Map fields to these columns (all exist): `permit_number, permit_type,
description, address, city, state, zip, issue_date, valuation, contractor_name,
applicant_name, jurisdiction, source`. `state` must be `'TX'`. Required for a row
to be useful: `address` non-null AND (`permit_type` OR `issue_date`) non-null.

Gentle-on-DB rules: load ALL fetched rows, but in batched COPY chunks of ~500
(loop the COPY; don't hold one giant transaction and don't cap the total row count).
NEVER full-scan `hot_leads`, NEVER `pg_terminate`/`pg_cancel`, NEVER DDL.
The DB connection that works in this environment is the `psql` client with
`PGGSSENCMODE=disable` (psycopg2 hangs here); DSN
`postgresql://will@100.122.216.15:5432/permits`. You may reuse
`harness/statewide/db.py` `copy_in()` to load.

Note whether any rows look like RE-ROOF / roofing permits (`permit_type` or
`description` mentioning roof/reroof/re-roof/shingle) — report `has_reroof`.

## Step 4 — Emit ONE JSON object to stdout (last thing you print)

Print exactly one JSON object, nothing after it:

```json
{
  "jurisdiction": "{{NAME}}",
  "vendor": "<vendor you classified>",
  "source_url": "<the live endpoint/URL you actually pulled from>",
  "source_tag": "{{SOURCE_TAG}}",
  "rows_loaded": <integer>,
  "has_reroof": <true|false>,
  "status": "built" | "walled",
  "barrier_if_walled": "<null if built, else the precise barrier>"
}
```

Your self-reported `status` is NOT trusted. After you finish, a deterministic
verifier independently queries `hot_leads` for `{{SOURCE_TAG}}` and confirms real
data actually landed. If you say `built` but no real rows are there, you fail.
Don't fabricate — load real rows or return an honest `walled`.
