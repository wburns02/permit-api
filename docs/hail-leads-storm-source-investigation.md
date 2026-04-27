# Hail Leads — Storm-Source Investigation (2026-04-27)

## TL;DR

The `hail_leads` materialized view joins `hot_leads x storm_events`.
`spc_storm_reports` is **NOT** a dependency of the MV. The
`storm_events_load: missing` heartbeat on `/v1/hail-leads/health` is
accurate — it is correctly flagging an upstream loader gap.

## How we confirmed

A one-shot `GET /v1/hail-leads/_diag` endpoint was added (and then
removed) to walk `pg_depend` against the MV's rewrite rule and read
`pg_class.reltuples` for the candidate source tables. Findings, captured
verbatim from the live primary Postgres at `100.122.216.15:5432/permits`:

| Metric                       | Value     |
| ---------------------------- | --------- |
| `hail_leads` MV row count    | ~17.3M    |
| `storm_events` row count     | 58,609    |
| `storm_events` rows last 30d | 0         |
| `spc_storm_reports` count    | 2,890     |
| MV depends on storm_events   | yes       |
| MV depends on spc_reports    | **no**    |

## Implication

The daily SPC loader is running and writing to `spc_storm_reports`, but
that data is invisible to the MV — so it does not contribute to the
product. Until `storm_events` is fed again, fresh leads will continue to
be 0 even though the MV is being refreshed and the SPC cron is healthy.

## Options to restore freshness

- **Option A — restore the NOAA `storm_events` loader.** Highest-fidelity
  data (full storm metadata, severity, geometry). NOAA publishes with a
  60–90 day lag and the dataset is bulk-monthly, so "fresh leads" still
  carry that source-of-truth delay even after a fix.
- **Option B — rewrite the `hail_leads` MV to also pull from
  `spc_storm_reports`.** Daily freshness (next-day after a hail event),
  smaller dataset, less metadata. Requires reconciling the join keys and
  schema — `spc_storm_reports` does not have the same columns/grain as
  `storm_events`.
- **Option C — both.** Use `storm_events` for historical/severity context
  and `spc_storm_reports` (UNION ALL or LATERAL) for daily freshness.
  Most product-correct outcome; largest schema change.

## Action taken in this loop

- Removed the `_diag` endpoint and supporting Pydantic schemas
  (`HailLeadsDiag`, `StormTypeCount`).
- Documented the finding here and at `/home/will/HAIL_LEADS_BUILD_PLAN.md`.
- Left the `storm_events_load` heartbeat in place — it is accurate.
- Loader fix is a separate, scoped follow-up — not in this loop.
