# Remaining PermitLookup Items — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the schema mismatch blocking multiple features, finish predictive model training, add 27M+ new records (NYC ACRIS + LA violations), research utility + photo data, and fix Contact Sales links.

**Architecture:** Schema alignment first (unblocks everything else), then scrapers in parallel, then research tasks. All scrapers follow existing patterns (psycopg2 + Socrata/CKAN APIs). Predictive model retrains after schema fix.

**Tech Stack:** FastAPI, SQLAlchemy, psycopg2, httpx, XGBoost, Socrata APIs

---

## Critical Context: T430 vs ORM Column Mapping

The T430 `permits` table has DIFFERENT column names than the SQLAlchemy ORM model:

| ORM Model (permit.py) | T430 Actual Column | Notes |
|------------------------|-------------------|-------|
| `state` | `state_code` | CHAR(2) |
| `zip` | `zip_code` | TEXT |
| `issue_date` | `date_created` | TIMESTAMP (not DATE) |
| `permit_type` | `project_type` | TEXT |
| `valuation` | (does not exist) | No valuation column on T430 |
| `parcel_id` | `parcel_number` | TEXT |
| `original_id` | (does not exist) | Not on T430 |
| `address_normalized` | (does not exist) | Not on T430 |
| `expired_date` | (does not exist) | Not on T430 |
| `completed_date` | (does not exist) | Not on T430 |
| `contractor_name` | (does not exist) | Not on T430 |
| `contractor_company` | (does not exist) | Not on T430 |
| `jurisdiction` | (does not exist) | Not on T430 |
| `scraped_at` | (does not exist) | Not on T430 |

T430 has columns NOT in ORM: `county`, `parcel_number`, `category`, `project_name`, `ossf_details`, `system_type`, `subdivision`, `source_file`, `raw_data`

---

### Task 1: Schema Alignment — Fix ORM to match T430 (BLOCKER)

**Files:**
- Modify: `app/models/permit.py` — remap columns to T430 names
- Modify: `app/services/search_service.py` — update any references to old column names
- Modify: `app/api/v1/permits.py` — update search queries
- Modify: `app/api/v1/contractors.py` — update contractor queries (uses contractor_name which doesn't exist on T430)
- Modify: `scripts/train_predictive_model.py` — fix SQL to use T430 column names

- [ ] **Step 1: Update the Permit model to use T430 column names**

The safest approach: use SQLAlchemy `column` with explicit `name` parameter to map Python attribute names to T430 column names. This lets existing Python code keep using `Permit.state` while the SQL uses `state_code`.

```python
class Permit(Base):
    __tablename__ = "permits"

    id = Column("id", BigInteger, primary_key=True)  # T430 uses bigint, not UUID
    permit_number = Column("permit_number", Text)

    # Location — map Python names to T430 column names
    address = Column("address", Text)
    city = Column("city", Text)
    state = Column("state_code", String(2), nullable=False, index=True)  # ORM: .state → SQL: state_code
    zip = Column("zip_code", String(10), index=True)  # ORM: .zip → SQL: zip_code
    lat = Column("lat", Float)
    lng = Column("lng", Float)
    county = Column("county", Text)
    parcel_id = Column("parcel_number", String(200))  # ORM: .parcel_id → SQL: parcel_number

    # Permit details
    permit_type = Column("project_type", String(100), index=True)  # ORM: .permit_type → SQL: project_type
    work_type = Column("work_type", Text)
    trade = Column("trade", Text)
    status = Column("status", Text)
    description = Column("description", Text)
    # valuation does NOT exist on T430 — remove or make optional

    # Dates
    issue_date = Column("date_created", DateTime)  # ORM: .issue_date → SQL: date_created
    # expired_date, completed_date don't exist on T430

    # People
    owner_name = Column("owner_name", Text)
    applicant_name = Column("applicant_name", Text)
    # contractor_name, contractor_company don't exist on T430

    # Source
    source = Column("source", Text)
    source_file = Column("source_file", Text)

    # T430-specific columns
    category = Column("category", Text)
    project_name = Column("project_name", Text)
    subdivision = Column("subdivision", Text)
    raw_data = Column("raw_data", JSONB)

    # Full-text search
    search_vector = Column("search_vector", TSVECTOR)
```

- [ ] **Step 2: Update search_service.py PERMIT_COLUMNS**

The `PERMIT_COLUMNS` list references specific Permit attributes. Update to only include columns that exist on T430.

- [ ] **Step 3: Update contractors.py**

The contractor search uses `Permit.contractor_name` and `Permit.contractor_company` which don't exist on T430. Options:
- Search `applicant_name` instead (T430 has this)
- Or search the `raw_data` JSONB field
- For risk scoring, the contractor_licenses table is the primary source anyway

Update contractor search to use `Permit.applicant_name` as fallback when `contractor_name` is not available.

- [ ] **Step 4: Fix train_predictive_model.py SQL**

Replace all references to `zip`, `state`, `issue_date` with `zip_code`, `state_code`, `date_created` in the raw SQL queries.

- [ ] **Step 5: Verify app loads and test**

```bash
python3 -c "from app.main import app; print(f'OK — {len(app.routes)} routes')"
```

- [ ] **Step 6: Commit and deploy**

```bash
git add app/models/permit.py app/services/search_service.py app/api/v1/contractors.py app/api/v1/permits.py scripts/train_predictive_model.py
git commit -m "fix: align ORM model with T430 schema — zip_code, state_code, date_created"
git push origin main && railway up --detach
```

---

### Task 2: Retrain Predictive Model

**Files:**
- Modify: `scripts/train_predictive_model.py` (already fixed in Task 1)
- Run on R730

- [ ] **Step 1: Copy fixed script to R730**

```bash
scp scripts/train_predictive_model.py will@192.168.7.71:/home/will/train_predictive_model.py
```

- [ ] **Step 2: Run training**

```bash
ssh will@192.168.7.71 "nohup python3 -u /home/will/train_predictive_model.py --db-host 100.122.216.15 > /tmp/train_predictive.log 2>&1 &"
```

- [ ] **Step 3: Monitor until complete**

```bash
ssh will@192.168.7.71 "tail -5 /tmp/train_predictive.log"
```

Wait for "Training complete" or "Batch scoring complete" in log.

- [ ] **Step 4: Verify predictions written to T430**

```bash
ssh will@100.122.216.15 "psql -U will -d permits -c 'SELECT count(*) FROM permit_predictions;'"
```

Expected: >0 predictions.

- [ ] **Step 5: Test production endpoint**

```bash
curl -m 15 -s https://permits.ecbtx.com/v1/predictions/stats
```

Expected: `total_predictions` > 0.

---

### Task 3: NYC ACRIS Scraper (+16M deed/lien records)

**Files:**
- Create: `scripts/scrape_nyc_acris.py`

NYC ACRIS has 3 related datasets on Socrata at `data.cityofnewyork.us`:
- Master: `bnx9-e6tj` — 16.9M records (document_id, doc_type, document_amt, recorded_datetime, borough)
- Parties: `636b-3b5g` — 46M records (document_id, party_type, name, address)
- Legals: `8h5j-fqxa` — property identifiers (borough, block, lot)

Strategy: Scrape the master table, filtering by doc_type. Load deeds into `property_sales`, liens into `property_liens`.

- [ ] **Step 1: Create scrape_nyc_acris.py**

Socrata scraper with `--type deeds|liens|all`:
- Deeds filter: `$where=doc_type in ('DEED','DEED, TS')` → property_sales table
- Liens filter: `$where=doc_type in ('TL%26R','AL%26R','RTXL','DTL','MTGE','SAT')` → property_liens table
- Pagination: $limit=50000, $offset=N, $order=:id
- Map borough codes: 1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island
- DB: host=100.122.216.15, port=5432, db=permits, user=will

- [ ] **Step 2: Copy to R730 and run**

```bash
scp scripts/scrape_nyc_acris.py will@192.168.7.71:/home/will/
ssh will@192.168.7.71 "nohup python3 -u /home/will/scrape_nyc_acris.py --type all --db-host 100.122.216.15 > /tmp/scrape_acris.log 2>&1 &"
```

- [ ] **Step 3: Verify data loaded**

```bash
ssh will@100.122.216.15 "psql -U will -d permits -c \"SELECT source, count(*) FROM property_sales WHERE source LIKE 'nyc_acris%' GROUP BY source;\""
ssh will@100.122.216.15 "psql -U will -d permits -c \"SELECT source, count(*) FROM property_liens WHERE source LIKE 'nyc_acris%' GROUP BY source;\""
```

- [ ] **Step 4: Commit**

---

### Task 4: LA Code Violations Scraper (+11M records)

**Files:**
- Modify: `scripts/scrape_code_violations.py` — add LA config

LA Building Safety Inspections: `data.lacity.org/resource/9w5z-rg2h.json` — 11.3M records
Fields: address, permit, inspection_date, inspection, inspection_result, lat, lon

- [ ] **Step 1: Add LA config to scrape_code_violations.py**

Add to the CITY_CONFIGS dict:
```python
"la": {
    "base_url": "https://data.lacity.org/resource/9w5z-rg2h.json",
    "source": "la_building_safety",
    "state": "CA",
    "fields": {
        "violation_id": None,
        "address": "address",
        "city": lambda r: "Los Angeles",
        "zip": None,
        "violation_type": "inspection",
        "violation_code": None,
        "description": "inspection_result",
        "status": "inspection_result",
        "violation_date": "inspection_date",
        "inspection_date": "inspection_date",
        "lat": "lat",
        "lng": "lon",
    },
},
```

- [ ] **Step 2: Copy to R730 and run**

```bash
scp scripts/scrape_code_violations.py will@192.168.7.71:/home/will/
ssh will@192.168.7.71 "nohup python3 -u /home/will/scrape_code_violations.py --city la --db-host 100.122.216.15 > /tmp/scrape_violations_la.log 2>&1 &"
```

- [ ] **Step 3: Verify**

```bash
ssh will@100.122.216.15 "psql -U will -d permits -c \"SELECT count(*) FROM code_violations WHERE source = 'la_building_safety';\""
```

- [ ] **Step 4: Commit**

---

### Task 5: Contact Sales → Email Link

**Files:**
- Modify: `app/static/index.html` — update all "Contact Sales" buttons

- [ ] **Step 1: Find all Contact Sales buttons**

```bash
grep -n "Contact Sales" app/static/index.html
```

- [ ] **Step 2: Update to mailto link**

Replace `onclick="subscribe('enterprise')"` and `onclick="subscribe('intelligence')"` with:
```html
href="mailto:willwalterburns@gmail.com?subject=PermitLookup Enterprise Inquiry"
```

- [ ] **Step 3: Commit and deploy**

---

### Task 6: Utility Connection Data (Research + Build)

**Files:**
- Create: `scripts/scrape_utility_connections.py` (if APIs found)

- [ ] **Step 1: Research — dispatch research subagent**

Search Socrata for utility connection/disconnection data:
```
https://api.us.socrata.com/api/catalog/v1?q=utility+connection&limit=20
https://api.us.socrata.com/api/catalog/v1?q=water+connection&limit=20
https://api.us.socrata.com/api/catalog/v1?q=new+service+connection&limit=20
```

Also check: NYC DEP water/sewer, LA DWP, Chicago utilities

- [ ] **Step 2: Build scraper if data sources found**

Follow existing scraper patterns. Target `permits` table or new `utility_connections` table depending on data shape.

- [ ] **Step 3: Commit**

---

### Task 7: Photo Intelligence (Research Phase)

**Files:**
- None yet — research only

- [ ] **Step 1: Research Google Street View Static API**

Requirements:
- API key needed (Google Cloud Console)
- Cost: $7 per 1,000 requests (Street View Static API)
- At scale: 1M lookups = $7,000/month

- [ ] **Step 2: Assess ROI**

Photo intelligence revenue potential: $50-100K/yr. At $7/1K lookups, need to charge $0.05+ per photo to break even. This may not be viable at current scale without customers.

**Recommendation:** Skip photo intelligence until there's customer revenue to fund the Google API costs. Note this in the plan as deferred.

- [ ] **Step 3: Document decision**

Add note to CLAUDE.md: "Photo intelligence deferred — requires Google Street View API at $7/1K requests. Will implement when customer revenue justifies the cost."

---

## Execution Order

1. **Task 1: Schema alignment** (BLOCKER — unblocks Tasks 2, 3, 4)
2. **Task 5: Contact Sales fix** (quick win, do alongside Task 1)
3. **Task 2: Retrain predictive model** (after Task 1)
4. **Task 3: NYC ACRIS scraper** (independent, start after Task 1)
5. **Task 4: LA violations** (independent, start after Task 1)
6. **Task 6: Utility data research** (independent)
7. **Task 7: Photo intelligence** (research only, likely deferred)

Tasks 2, 3, 4, 5, 6 can run in parallel after Task 1 completes.
