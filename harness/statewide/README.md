# Statewide TX permit-scraping loop (pilot)

A harness that drives `claude -p` (the Claude **subscription** CLI, flat-rate)
over Texas jurisdictions, with a **deterministic verifier** gating each one.

The core principle: *a real autonomous loop is a harness with backpressure.*
Each jurisdiction must pass an independent, real-data verifier before it is
marked done. The agent's self-reported "success" is never trusted.

## Pieces

| File | Role |
|------|------|
| `registry.py` / `registry.db` | One SQLite row per jurisdiction. **This is the state** — the loop is resumable because nothing lives in memory. States: `pending → built → verified` or `→ walled`. |
| `seed_data.py` | ~25 representative TX jurisdictions: known-vendor cities (OpenGov/MGO/ArcGIS/Socrata), unknown/walled cities (Accela/eTRAKiT/EnerGov/Click2Gov/CitizenServe), and 3 counties (Ch.233 no-permit case). |
| `jurisdiction_prompt.md` | The task handed to `claude -p` for one jurisdiction: classify vendor → route (known adapter vs deep recon) → load a sample into `hot_leads` → emit one JSON object. Bakes in the don't-quit rule + framework references. |
| `run_loop.py` | The driver. Pulls `pending`, spawns `claude -p --model sonnet`, parses the agent JSON, **runs the verifier and gates on THAT**, writes the registry. Bounded parallelism (~3), resumable. |
| `verify.py` | The deterministic verifier (the backpressure). Independently queries `hot_leads` by `source_tag`: row count, required fields, plausibility, address variety, live-feed re-fetch. Returns pass/fail + reason. |
| `db.py` | Postgres access via the `psql` client (psycopg2 hangs on this Tailscale path). Bounded statement timeouts, idempotent `load_hot_leads` (temp-stage + `ON CONFLICT DO NOTHING`), never full-scans. |
| `test_verifier.py` | Live self-test: loads a REAL Austin Socrata sample (must PASS) and fabricated garbage (must FAIL). Proves the verifier is real backpressure. |

## Model / cost

Per-jurisdiction agent calls use **`claude -p --model sonnet`** — the Claude
subscription (flat-rate), never the metered Anthropic API (`ANTHROPIC_API_KEY`
is explicitly stripped from the agent's env). Sonnet is the right tier for bulk
recon/build and conserves subscription quota vs Opus. The model is a config
constant (`MODEL`) at the top of `run_loop.py`. The verifier and driver are
deterministic code — no model.

## Usage

```bash
cd harness/statewide
python3 run_loop.py seed              # seed the 25 pilot jurisdictions
python3 run_loop.py run --parallel 3  # drive the loop (resumable)
python3 run_loop.py status            # registry state
python3 test_verifier.py              # prove the verifier (needs DB)
```

## Gentle-on-DB guarantees

- Loads only into `hot_leads` (the existing landing table), bounded samples.
- Every read filters by the indexed `source` column or is `LIMIT`-bounded.
- Per-statement timeout + per-subprocess wall-clock kill.
- **Never** `count(*)` over the whole table, **never** `pg_terminate`/`pg_cancel`,
  **never** DDL beyond the registry's own SQLite.

## Scaling to all of TX

The pilot proves the loop + verifier. To reach ~1,200 cities + 254 counties:
the cost is subscription wall-clock (Sonnet, ~2 min/known-vendor juris, longer
for deep recon), not API dollars. The long tail is unique/walled portals; the
known-vendor buckets (OpenGov 214 portals, MGO 105 TX, Socrata, ArcGIS) collapse
to config rows. See the PR description for the full scaling assessment.
