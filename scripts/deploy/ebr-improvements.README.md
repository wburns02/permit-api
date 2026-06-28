# EBR year_built / sqft enrichment — deploy + captcha status

## What it does
`scripts/scrape_ebr_improvements.py` fills `year_built` / `building_sqft` for
`tx_cad_parcels` rows with `cad_source='EBRPA'` (East Baton Rouge Parish), the one
remaining gap in the Baton Rouge market. parcel_id == the assessor ASSESSMENT_NUM
(format `NNN-NNNN-N`), so no ID mapping is needed.

## BLOCKER: reCAPTCHA Enterprise (why this is not a fire-and-forget service)
The East Baton Rouge ASSESSOR property data lives in SmartCAMA
(`eastbatonrouge.smartcama.com`). Its data endpoint `POST /Assessments/SearchAjax`
is guarded by **reCAPTCHA Enterprise** and 302-bounces to an image-grid challenge
("select all images with bicycles") on every search without a fresh
`g-recaptcha-response` token. Verified 2026-06-28: the checkbox does not pass
frictionlessly for an automated browser. Unlike Brazoria's BCAD (whose captcha
only guarded the form, not the GetImprovements AJAX route), here there is NO open
backdoor:
- `Cadastral/Building_Footprint` (maps.brla.gov) has YEAR_BUILT but only ~0.9% fill.
- `TaxParcels_CAMA` (ArcGIS Online org ue9rwulIoeLEI9bj) has RESYRBLT/RESFLRAREA
  but is an ~18K-row demo subset on a different key (GIS PARCELID, not
  ASSESSMENT_NUM) and covers ~0 of the lead set.
- The authoritative `Cadastral/Tax_Parcel` feed we already load has NO year_built.

So the scraper REQUIRES a captcha token provider. It refuses (exit 4) without one
rather than silently no-op'ing.

## To make it run (provide ONE):
- `--token-cmd '<cmd that prints a fresh g-recaptcha-response>'`  (e.g. a
  2captcha/anti-captcha solver for the SmartCAMA site key), or
- `--token-file <path>`  (a human pastes a freshly-solved token; re-read on 302).

Env equivalents: `EBR_TOKEN_CMD`, `EBR_TOKEN_FILE`.

## Deploy on R730-2 (when a token source exists)
```
# 1. sync repo on R730-2 (it can reach smartcama + the T430 DB)
cd /home/will/permit-api-live && git fetch && git checkout origin/main

# 2. reachability probe (no token needed)
python3 scripts/scrape_ebr_improvements.py --probe-only        # expect reachable=True

# 3. install service + token override
sudo cp scripts/deploy/ebr-improvements.service /etc/systemd/system/
sudo systemctl edit ebr-improvements      # add Environment=EBR_TOKEN_CMD=...
sudo systemctl daemon-reload
sudo systemctl enable --now ebr-improvements

# priority lead parcels first, then full parish (resumable via
# ebr_improvement_progress checkpoint table). Seed the 50 postcard leads:
python3 scripts/scrape_ebr_improvements.py --priority-only \
  --seed-csv /home/will/outbound-engine/postcard-poc/out/batonrouge_best_skiptraced.csv \
  --token-file /tmp/ebr.tok
```

Raw HTML stages to `/dataPool/free_data/ebr/raw` (NOT /home/will, per Storage Policy).
