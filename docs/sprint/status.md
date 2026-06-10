# Rig & Permit Radar Sprint Status

## Phase 0: Data Quality Gate (complete, 2026-06-10 06:16 CT)

The wedged 78M-row full-scan dup check was killed and replaced with a
per-partition sweep (scripts/dq_partition_sweep.sh). All 75 partitions of
canonical.production_monthly and canonical.permits verified clean: zero
duplicates, zero errors, zero partitions unknown. Non-partitioned O&G
tables also clean on their identity keys. Two real defects from the wider
DQ pass (malformed api10 fragment, two century-typo completion dates) were
fixed in place with loader guards committed; the 96K approved-before-
submitted permits were dispositioned as amendment semantics, not errors.
Full report: docs/dq/dedup_report.md. Next: Phase 1 ship.

## Phase 1: W-1 Watchlist Alerts (complete, 2026-06-10)

Extended the existing alert engine rather than forking: permit_alerts
gained source_type ('permits' | 'well_permits') via the startup
auto-migration, criteria stay in the existing JSONB filters column so new
watchable fields (depth, field name, district already included) need no
migration. Matching runs against canonical.well_permits with an
approved_date cursor and explicit no-backfill semantics; digests group by
county; email rendering is escaped; webhooks carry source_type; daily and
weekly cadence ride the existing per-watchlist frequency enum and
scheduler batches (R730 runs the scheduler). Verified: 5/5 integration
tests pass against the live warehouse through the real execute_alert +
webhook delivery pipeline, including a MIDLAND county digest with correct
contents, zero-match no-fire, lease pattern + min_depth filtering, and
no-backfill on activation. Next: Phase 4 eval set construction.

## Phase 2: FracFocus + TexNet (agent running)

## Phase 3: Railway Deploy Hardening (agent running; /healthz + parallel
boot committed as 490fefc, deploy observation in progress)
