# Canonical Dedup Report: 2026-06-10

## Result

**All 75 partitions clean. Zero duplicates. Zero partitions in an unknown state.**

| Table | Partitions checked | Dup key | Dirty partitions |
|---|---|---|---|
| canonical.production_monthly | 36 (p1993-p2027 + default) | (well_type, district, lease_number, prod_month) | 0 |
| canonical.permits | 39 (p1990-p2027 + default) | (source_id, source_record_id) | 0 |

Full per-partition log: `/mnt/win11/Fedora/raw-public-data/rrc/dq_partition_sweep.log`
Sweep tool: `scripts/dq_partition_sweep.sh` (per-partition GROUP BY with 1GB
work_mem and a 600s per-partition timeout; rerunnable any time).

Non-partitioned tables, verified same pass: operators 0 dup operator
numbers, disposal_wells 0 dup UIC numbers, wells 0 dup api14,
well_permits 0 dup permit numbers.

## Why the original check wedged

A single GROUP BY over all 78M production rows ran 10+ hours on the 86GB
T430 (hash agg spilled; the OR-on-bind-parameter pattern also blocked
partition pruning). Killed via pg_cancel_backend. Lesson encoded in the
sweep script: check partitions independently, never the parent.

## Dedup strategy in force (loaders)

- production_monthly: full reload per PDQ refresh (delete source='rrc_pdq',
  COPY back); source dump is already unique per lease-cycle.
- permits: identity map canonical.permit_keys (PK source_id,
  source_record_id) + DISTINCT ON latest scrape at backfill time.
- well_permits: UNIQUE (state, permit_number); amendments collapse via
  DISTINCT ON sequence (lowest seq = newest amendment wins).
- wells: UNIQUE partial index (state, api14); completion-level rows collapse
  to wellbore keeping most recent completion.

## Anomalies found and dispositioned (2026-06-09 pass)

- 1 malformed api10 ('425' fragment): nulled; loader now requires len==8.
- 2 future completion dates (2061, 2070 century typos): nulled; loader now
  rejects dates beyond today.
- 96,684 well_permits with approved_date < submitted_date: 88% are
  amendments (amendment submission vs original approval). Documented as
  data semantics, not corrected.
- 226,634 wells (22%) with unresolved operator_id: operators absent from
  the current P-5 file (historical/defunct). Expected; operator_name_raw
  retained on every row.
