# EBR year_built / sqft enrichment — deploy + captcha status

## What it does
`scripts/scrape_ebr_improvements.py` fills `year_built` / `building_sqft` for
`tx_cad_parcels` rows with `cad_source='EBRPA'` (East Baton Rouge Parish), the one
remaining gap in the Baton Rouge market. The authoritative bulk BRLA cadastral
feed has owner/situs/value but no improvement detail.

## How it works (verified live 2026-06-28)
The assessor data lives in SmartCAMA (`eastbatonrouge.smartcama.com`), gated by
**reCAPTCHA Enterprise v2 CHECKBOX** (site key
`6Le9Gb8eAAAAADRJkJ3Lt5aryjSL8dj8wQKVc7gm`, page `/Assessments/Search`). The flow:

1. Solve the checkbox → `POST /Captcha/VerifyCaptcha?token=<g-recaptcha-response>`
   marks the **session** captcha-valid server-side (`GET /Captcha/IsValid` → `true`).
   ONE solved token validates the session for many searches; the scraper re-solves
   only when `IsValid` flips false / a search 302-bounces. 2Captcha cost is
   ~$0.0018/solve, a tiny fraction of a cent per parcel.
2. Our `parcel_id` (e.g. `000-0004-3`) is the GIS/cadastral number, **NOT** the
   assessor AssessmentNumber (a short int like `43`/`2700514*`). So the scraper
   searches by **situs address** (`PhysicalStreetNumber`+`PhysicalStreetName`) via
   `POST /Assessments/SearchAjax` and address-matches the returned `PhysicalAddress`
   back to our parcel to get its assessor row `Id`.
3. year_built/sqft are in the per-assessment detail JSON:
   `POST /Assessments/FetchAssessment?Id=<id>` →
   `TaxItems[].WorkItems[].ConstructionDate` / `DepreciationYear` / `EffectiveSqft`.

### Captcha token provider (required)
- `--token-cmd '<cmd that prints a fresh g-recaptcha-response>'` — the bundled
  `scripts/ebr_captcha_token.py` does this via 2Captcha (method `userrecaptcha`,
  `enterprise=1`). Reads the 2Captcha key from `EBR_2CAPTCHA_KEY` env or the
  chmod-600 file `~/.config/permitlookup/2captcha.key`. **The key is a secret and
  is never committed or logged.**
- `--token-file <path>` — a human-pasted token (re-read on invalidation).

With no provider the scraper does a reachability probe and exits 4 with
instructions (never silently no-ops).

## SOURCE DATA CAVEAT (measured 2026-06-28, n=25)
EBR CAMA populates ConstructionDate **sparsely**:
- Parish-wide random: **~54% carry year_built**, ~77% carry sqft.
- The older inner-Baton-Rouge storm-lead set (ZIPs 70802–70806): **~0% year_built**,
  ~15% sqft — the assessor simply never recorded construction year there.
The scraper fills what the source has, records `no_year` (has sqft, no year) and
`no_improvement` (neither) so those parcels are not re-hit.

## Deploy on R730-2 (it can reach smartcama + the T430 DB)
```
# 1. sync repo
cd /home/will/permit-api-live && git fetch && git checkout origin/main && git pull

# 2. store the 2Captcha key (chmod 600, NOT in the repo)
mkdir -p ~/.config/permitlookup
printf '%s' '<2CAPTCHA_KEY>' > ~/.config/permitlookup/2captcha.key
chmod 600 ~/.config/permitlookup/2captcha.key

# 3. reachability probe (no token / key needed)
python3 scripts/scrape_ebr_improvements.py --probe-only      # expect reachable=True

# 4. install + start the service (the unit sets EBR_2CAPTCHA_KEY_FILE to the path
#    above and uses ebr_captcha_token.py as --token-cmd)
sudo cp scripts/deploy/ebr-improvements.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ebr-improvements
systemctl status ebr-improvements
```

The service does the EBR lead set first (the `unserviced_hail_leads` EBR arm via
the scraper's PRIORITY query), then the whole parish, resumable. Raw detail JSON
stages to `/dataPool/free_data/ebr/raw` (NOT /home/will, per Storage Policy).

NOTE on `--seed-csv`: it expects a file with a `parcel_id`/`assessment` column and
resolves situs from the DB. `batonrouge_best_skiptraced.csv` currently has no
parcel_id column, so the seed resolves to 0 rows and the scraper falls through to
the PRIORITY query (which already covers the storm-lead set). Add a `parcel_id`
column to that CSV to force an explicit ordering.

## Verify fill is climbing
```
psql -h 100.122.216.15 -U will permits -c \
 "SELECT count(*) total, count(year_built) yb, count(building_sqft) sqft
    FROM tx_cad_parcels WHERE cad_source='EBRPA' AND tax_year=2026;"
```
