# PermitLookup API

## Overview
Building permit data API serving ~1B records from 180+ jurisdictions across 17+ states.
Monetized via Stripe subscription tiers (Free/Starter/Pro/Enterprise).

## Tech Stack
- **Backend**: FastAPI + Python 3.12 + SQLAlchemy 2.0 async + PostgreSQL
- **Auth**: API key (X-API-Key header), hashed with SHA-256
- **Billing**: Stripe subscriptions + metered usage for overages
- **Rate Limiting**: Redis-based (with in-memory fallback)
- **Deployment**: Railway (Docker)

## Key Directories
- `app/api/v1/` — API endpoints (permits, auth, billing, coverage)
- `app/models/` — SQLAlchemy models (permit, api_key)
- `app/services/` — Business logic (search, stripe)
- `app/middleware/` — Auth + rate limiting
- `scripts/` — ETL and data migration tools

## Data Source
- SQLite DB at `/mnt/win11/fedora-moved/Data/crm_permits.db` (3.58M local copy)
- Full dataset on T430: `/dataPool/data/databases/crm_permits.db` (~1B records)
- ETL: `scripts/etl_sqlite_to_pg.py` handles batch loading with address normalization

## API Endpoints
- `POST /v1/signup` — Create free account, get API key
- `GET /v1/permits/search` — Search by address, geo, filters
- `POST /v1/permits/bulk` — Bulk CSV upload (Starter+)
- `GET /v1/coverage` — Jurisdiction list (public, no auth)
- `GET /v1/stats` — Quick stats (public)
- `GET /v1/usage` — Current usage stats
- `GET /v1/api-keys` — List API keys
- `POST /v1/subscribe` — Stripe checkout for paid plans
- `POST /v1/webhooks/stripe` — Stripe webhook handler

## Pricing Tiers
| Tier | Price | Daily Limit |
|------|-------|-------------|
| Free | $0 | 100 lookups |
| Starter | $49/mo | 1,000 lookups |
| Pro | $149/mo | 10,000 lookups |
| Enterprise | $499/mo | Unlimited |

## Address Matching
- Uses `pg_trgm` trigram index for fuzzy address matching
- `normalize_address()` standardizes abbreviations (St→ST, Avenue→AVE, etc.)
- tsvector full-text search on address + city + state + permit_number + names
