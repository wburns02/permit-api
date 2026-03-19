# PermitLookup API

## Overview
Building permit + property intelligence API serving 1B+ records from 180+ jurisdictions across 50+ states.
6 data layers: permits, contractor licenses, EPA environmental risk, FEMA flood zones, septic systems, census demographics, and property valuations.
Monetized via Stripe subscription tiers (Free/Explorer/Pro Leads/Real-Time/Enterprise).

## Tech Stack
- **Backend**: FastAPI + Python 3.12 + SQLAlchemy 2.0 async + PostgreSQL
- **Auth**: API key (X-API-Key header), hashed with SHA-256
- **Billing**: Stripe subscriptions + metered usage for overages
- **Rate Limiting**: Redis-based (with in-memory fallback)
- **Deployment**: Railway (Docker)
- **Frontend**: Vanilla JS SPA (app/static/index.html)

## Key Directories
- `app/api/v1/` — API endpoints (15 routers)
- `app/models/` — SQLAlchemy models
- `app/services/` — Business logic (search, stripe, alerts, risk)
- `app/middleware/` — Auth + rate limiting
- `scripts/` — ETL and data migration tools

## API Routers (15 total)
| Router | Prefix | Auth | Purpose |
|--------|--------|------|---------|
| permits | /v1/permits | API Key | Permit search, bulk, freshness |
| auth | /v1 | Optional | Signup, key management |
| billing | /v1 | API Key | Usage, Stripe checkout |
| coverage | /v1/coverage | None | Public stats |
| contractors | /v1/contractors | API Key | Contractor search & profiles |
| alerts | /v1/alerts | API Key | Alert CRUD, webhooks |
| properties | /v1/properties | API Key | Property history, risk |
| market | /v1/market | API Key | Market intelligence |
| saved_searches | /v1/saved-searches | API Key | Saved search CRUD |
| admin | /v1/admin | Admin email | Scraper dashboard |
| licenses | /v1/licenses | API Key (Pro+) | Contractor license verification |
| environmental | /v1/environmental | API Key (Pro+) | EPA + FEMA risk |
| septic | /v1/septic | API Key (Pro+) | Septic system data |
| demographics | /v1/demographics | API Key (Explorer+) | Census ACS data |
| valuations | /v1/valuations | API Key (Explorer+) | Redfin market data |

## Database Tables
| Table | Purpose | Key Fields |
|-------|---------|------------|
| permits | 744M+ building permits | address, state, permit_type, issue_date |
| jurisdictions | Jurisdiction metadata | name, state, record_count |
| api_users | User accounts | email, plan, stripe IDs |
| api_keys | API key storage | key_hash, user_id |
| usage_logs | API usage tracking | user_id, endpoint, lookup_count |
| permit_alerts | Alert configurations | filters (JSONB), frequency |
| alert_execution_history | Alert run logs | match_count, delivery_status |
| saved_searches | Saved search configs | filters (JSONB) |
| contractor_licenses | 500K+ licenses (CA, FL) | license_number, status, state |
| epa_facilities | EPA FRS facilities | registry_id, lat, lng |
| fema_flood_zones | FEMA NFHL zones (50 states) | dfirm_id, fld_zone, sfha_tf |
| census_demographics | ACS 2023 block groups | state_fips, county_fips, tract |
| septic_systems | 5M+ septic records | address, system_type, state |
| property_valuations | Redfin ZIP market data | zip, median_sale_price, period |

## Pricing Tiers
| Tier | Price | Daily Limit | Data Freshness |
|------|-------|-------------|----------------|
| Free | $0 | 25 | Cold (180+ days) |
| Explorer | $79/mo | 100 | Mild+Cold (90+) |
| Pro Leads | $249/mo | 250 | Warm+ (30+) |
| Real-Time | $599/mo | 1,000 | All (HOT 0-30) |
| Enterprise | $1,499/mo | 10,000 | Everything |

## Data Layer Access by Plan
| Data Layer | Free | Explorer | Pro Leads+ |
|-----------|------|----------|------------|
| Permits (744M+) | Yes | Yes | Yes |
| FEMA flood stats | Yes | Yes | Yes |
| Demographics (county) | No | Yes | Yes |
| Property valuations | No | Yes | Yes |
| Contractor licenses | No | No | Yes |
| EPA environmental risk | No | No | Yes |
| Septic systems | No | No | Yes |
| Demographics (tract) | No | No | Yes |

## Data Loading
- `scripts/load_data_layers.py` — All-in-one loader for Tier 1 data (run on R730)
  - `--layer all` loads all 6 layers
  - `--layer contractor_licenses|epa_facilities|fema_flood_zones|census_demographics|septic_systems|property_valuations`
- Staging data: R730 `/mnt/data/staging/` (644GB across 157 directories)
- Target: T430 PostgreSQL (`100.122.216.15:5432/permits`)

## Address Matching
- Uses `pg_trgm` trigram index for fuzzy address matching
- `normalize_address()` standardizes abbreviations (St->ST, Avenue->AVE, etc.)
- tsvector full-text search on address + city + state + permit_number + names
