# Universal Hot Leads Loader

**Date:** 2026-04-03
**Status:** Approved
**Project:** PermitLookup (permits.ecbtx.com)
**Priority:** Critical — data freshness is the product

## Problem

The AI Analyst queries `hot_leads` which only has ~12K records from 3 sources (Austin Socrata, Chicago, Cincinnati). The R730 runs 312 scrapers nightly producing .ndjson files covering all 50 states, but none of that data feeds into `hot_leads`. Users get 0 results or stale data for most cities/states.

## Solution

A nightly loader on R730 that scans ALL scraper .ndjson outputs, extracts permit-relevant records, fuzzy-maps fields to the `hot_leads` schema, and bulk-inserts into T430. Includes freshness tracking and a health endpoint.

## Architecture

```
R730 cron (6:00 AM daily, after scrapers finish)
  |-- Scan /home/will/crown_scrapers/data/*_YYYYMMDD.ndjson
  |-- Filter: only permit/building/construction files
  |-- For each file:
  |     |-- Read .ndjson lines
  |     |-- Parse raw_data JSON from each record
  |     |-- Fuzzy-map fields to hot_leads schema
  |     |-- Batch insert to T430 (dedupe on permit_number + address + state)
  |-- Log per-source results
  |-- Update hot_leads_sources tracking table
  |-- Report total loaded
```

## File Filtering

**Include** files matching (case-insensitive in filename):
- `permit`, `building`, `construction`, `code_enforcement`, `violation`, `inspection`

**Exclude** files matching:
- `address_database`, `parcel`, `cadastre`, `water`, `shellfish`, `land_records`, `property_owner`, `buyout`, `assessed_value`, `zoning`, `flood`, `septic`, `census`

## Field Fuzzy Mapping

Each .ndjson record has `raw_data` (JSON string) with source-specific field names. The loader parses `raw_data` and maps to `hot_leads` columns using pattern matching:

| hot_leads column | Patterns (first match wins, case-insensitive) |
|-----------------|----------------------------------------------|
| permit_number | permit_number, record_id, permitnumber, permit_no, case_number, job__, id |
| address | address, property_address, location, street_address, site_address |
| city | city, property_city, municipality, borough |
| state | From .ndjson top-level `state` field (2-letter code) |
| zip | zip, zip_code, postal_code, zipcode |
| description | description, project_description, work_description, scope_of_work, job_description |
| permit_type | permit_type, record_type, type_of_work, b1_app_type_alias |
| work_class | work_class, construction_type, record_type_type |
| issue_date | issue_date, date_opened, issuance_date, issued_date, issuedate, permitnumbercreateddate |
| valuation | valuation, project_value, estimated_cost, value, reported_cost |
| contractor_name | contractor_name, contractor, contractor_1_name |
| contractor_phone | contractor_phone, phone, permittee_s_phone__ |
| contractor_company | contractor_company, company, business_name, permittee_s_business_name |
| applicant_name | applicant_name, owner_name, owner_s_business_name |
| applicant_phone | applicant_phone, owner_s_phone__ |
| sqft | sqft, total_floor_area, square_feet |
| jurisdiction | jurisdiction, agency (or derived from filename) |
| source | Derived from filename (e.g., "ct_building_permits" from "ct_building_permits_20200101_to_current_20260403.ndjson") |

### Date Parsing

Issue dates come in multiple formats:
- ISO: `2026-04-01`, `2026-04-01T00:00:00`
- US: `04/01/2026`, `4/1/2026`
- Epoch ms: `1630569600000` (divide by 1000, convert)
- With time: `2026-04-01 12:00:00`

The loader tries each format and skips records with unparseable dates.

### Freshness Filter

Only load records with `issue_date` within the last 90 days. Older records are not "hot leads."

## Database Changes

### New unique index on hot_leads

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_hot_leads_dedup
ON hot_leads (permit_number, address, state)
WHERE permit_number IS NOT NULL AND address IS NOT NULL;
```

This enables `ON CONFLICT` upsert for deduplication.

### New tracking table

```sql
CREATE TABLE IF NOT EXISTS hot_leads_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_name TEXT NOT NULL,
    state TEXT,
    file_name TEXT,
    records_loaded INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,
    latest_issue_date DATE,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS idx_hls_source ON hot_leads_sources(source_name);
CREATE INDEX IF NOT EXISTS idx_hls_loaded ON hot_leads_sources(loaded_at DESC);
```

## New Script: scripts/load_hot_leads_universal.py

Location: `/home/will/permit-api/scripts/load_hot_leads_universal.py`
Runs on: R730 (192.168.7.71)
Connects to: T430 PostgreSQL (100.122.216.15:5432)

### CLI

```bash
# Normal daily run (today's files only)
python3 scripts/load_hot_leads_universal.py

# Load specific date
python3 scripts/load_hot_leads_universal.py --date 2026-04-02

# Load last N days (seed/backfill)
python3 scripts/load_hot_leads_universal.py --days 7

# Dry run (count records, don't insert)
python3 scripts/load_hot_leads_universal.py --dry-run

# Single file
python3 scripts/load_hot_leads_universal.py --file /path/to/file.ndjson
```

### Flow

1. Determine target date(s) from CLI args
2. Scan `/home/will/crown_scrapers/data/` for matching .ndjson files
3. Filter by include/exclude patterns
4. For each file:
   a. Open and read line by line (streaming, not load entire file)
   b. Parse `raw_data` JSON from each line
   c. Apply fuzzy field mapping
   d. Parse and validate issue_date (skip if > 90 days old or unparseable)
   e. Skip records missing both permit_number AND address
   f. Accumulate in batch (1000 records)
   g. Bulk upsert via `execute_values` with `ON CONFLICT DO UPDATE`
   h. Track counts per file
5. Update `hot_leads_sources` table with results
6. Print summary

### Batch Insert SQL

```sql
INSERT INTO hot_leads (
    id, permit_number, permit_type, work_class, description,
    address, city, state, zip, valuation, sqft, issue_date,
    contractor_company, contractor_name, contractor_phone,
    applicant_name, applicant_phone, jurisdiction, source
) VALUES %s
ON CONFLICT (permit_number, address, state)
WHERE permit_number IS NOT NULL AND address IS NOT NULL
DO UPDATE SET
    issue_date = COALESCE(EXCLUDED.issue_date, hot_leads.issue_date),
    valuation = COALESCE(EXCLUDED.valuation, hot_leads.valuation),
    contractor_name = COALESCE(EXCLUDED.contractor_name, hot_leads.contractor_name),
    contractor_phone = COALESCE(EXCLUDED.contractor_phone, hot_leads.contractor_phone),
    contractor_company = COALESCE(EXCLUDED.contractor_company, hot_leads.contractor_company),
    description = COALESCE(EXCLUDED.description, hot_leads.description),
    source = EXCLUDED.source
```

## New API Endpoint

### GET /v1/freshness/hot-leads

Returns per-source freshness data from `hot_leads_sources`:

```json
{
    "total_records": 45000,
    "sources": [
        {
            "source_name": "ct_building_permits",
            "state": "CT",
            "records_loaded": 2500,
            "latest_issue_date": "2026-04-02",
            "loaded_at": "2026-04-03T06:15:00Z",
            "status": "fresh"
        }
    ],
    "stale_sources": ["nc_building_permits"],
    "last_load": "2026-04-03T06:15:00Z"
}
```

Status: `fresh` (loaded today), `stale` (>24h), `error` (last load failed).

## Cron

On R730 (`crontab -e`):

```
0 6 * * * cd /home/will/permit-api-live && python3 scripts/load_hot_leads_universal.py >> /tmp/hot_leads_loader.log 2>&1
```

Runs at 6 AM after the 312 scrapers (midnight) and Socrata loaders (5:00-5:30 AM) finish.

## Existing Scripts

The existing `scrape_central_tx_daily.py` and `scrape_all_metros_daily.py` continue running at 5:00 and 5:30 AM. They load Socrata data with richer field mappings (phone numbers, contractor details). The universal loader at 6:00 AM adds everything else from the 312 scrapers. Dedup prevents duplicates.

## Out of Scope (Spec A and C)

- Fixing Socrata API 400 errors (Spec A — separate)
- Auto-healing query format detection (Spec A)
- Alerting/notifications when sources go stale (Spec C)
- Admin dashboard for monitoring (Spec C)
