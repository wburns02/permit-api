# FracFocus Full Registry

## Source
- Landing page: https://fracfocus.org/data-download
- Bulk CSV export (current as of 2026-06-10): https://www.fracfocusdata.org/digitaldownload/FracFocusCSV.zip
  (~437 MB zip, ~3.5 GB extracted; regenerated nightly by FracFocus, file
  timestamps inside the zip show same-day builds)
- Alternates on the same page: `fracfocusdata.zip` (SQL Server .bak),
  `FracFocusNoOGData.zip` / `FracFocusNoOGCSV.zip` (chemical-only subsets).
  We use the CSV zip.

## Format discovered
Zip contents:
- `DisclosureList_1.csv` — one row per disclosure, 17 columns (~247K rows)
- `FracFocusRegistry_1..15.csv` — one row per disclosure-ingredient,
  31 columns (the 17 disclosure columns repeated + 14 ingredient columns),
  ~7.2M rows total (500K rows per shard except the last)
- `WaterSource_1.csv` — water-source detail (NOT loaded; small, add later if
  needed)
- `readme csv.txt` — data dictionary

## Download
```bash
mkdir -p /mnt/win11/Fedora/raw-public-data/fracfocus
cd /mnt/win11/Fedora/raw-public-data/fracfocus
curl -sL -o FracFocusCSV.zip "https://www.fracfocusdata.org/digitaldownload/FracFocusCSV.zip"
unzip -o -q FracFocusCSV.zip -d csv
```

## Load
```bash
python3 /home/will/permit-api-live/scripts/load_fracfocus.py \
    [--dir /mnt/win11/Fedora/raw-public-data/fracfocus/csv]
```
Full reload (DROP + CREATE + COPY), ~10 min. Builds:
- `fracfocus.disclosures` — one row per disclosure, PK `disclosure_id`,
  geom Point 4326, indexes on `api10`, `state_name`, gist(geom)
- `fracfocus.registry` — ingredient level, indexes on `disclosure_id`, `api10`

## Row counts (2026-06-10 load)
- `fracfocus.disclosures`: 247,482 total / 121,818 Texas; geom on 247,425
- `fracfocus.registry`: 7,166,509 total / 3,620,824 Texas
  (raw CSVs have 7,214,363 data lines; the 47,854 delta is multi-line
  records with embedded newlines in quoted fields, 0 rows skipped)

## API normalization
`APINumber` in the export is 14-digit undashed (older vintages were dashed;
loader strips non-digits regardless). Loader derives:
- `api_raw` — verbatim
- `api14` — digits right-padded with '0' to 14
- `api10` — first 10 digits (join key to `canonical.wells.api10` /
  `canonical.well_permits.api10`)
Only 8 of 121,818 TX rows have malformed APIs.

## Join keys + match rates (2026-06-10)
TX disclosures vs warehouse, on `api10`:
- `canonical.wells`: 76.4% of rows (86,913 / 115,261 distinct APIs = 75.4%)
- `canonical.well_permits`: 88.4% of rows (101,267 / 115,261 distinct)

Pre-2021 job years match `canonical.wells` at 99.5-99.9%. The shortfall is
entirely warehouse-side coverage, NOT API formatting:
1. `canonical.wells` content ends at completion_date 2020-11-20 (the RRC
   wellbore EWA extract is a ~Nov-2020 vintage). All post-2020 frack jobs
   miss.
2. `canonical.well_permits` has a volume hole mid-2023 through 2025
   (3,304 permits in 2023, 186 in 2024 vs ~10K/yr expected), so 2024-2026
   disclosures match at only 12-21%.
Fix is refreshing those two sources, not this loader.

## Refresh cadence recommendation
Weekly (Sunday night cron). The export is a full nightly snapshot; weekly is
plenty for lead-gen use. Loader is a clean full reload, idempotent.

## Gotchas
- Header row repeated in every Registry shard; loader skips per-file.
- Bogus job dates exist (e.g. 1955 "frack jobs"); loader nulls dates outside
  1990..today+1y but keeps the rows.
- Lat/lon mix NAD27/NAD83/WGS84 (`projection` column); loaded as-is into
  SRID 4326. Max ~10-80 m error; fine for 25 km proximity work, do not use
  for parcel-level joins.
- ~57 disclosures have lat/lon outside CONUS sanity bounds; geom left NULL.
- "Ingredient Container" placeholder rows exist in registry (FFVersion 1-2
  era disclosures with no real chemistry).
- MassIngredient uses scientific notation in places; numeric column handles.
- `fracfocus.registry` has no PK; `(disclosure_id, ingredients_id)` is NOT
  unique (ingredients_id is empty for v1 rows).
