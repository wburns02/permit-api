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

## Phase 2: FracFocus + TexNet (complete, 2026-06-10)

Both datasets loaded and verified: fracfocus.disclosures 247,482 (121,818
TX) plus 7.17M ingredient-level registry rows from the nightly CSV
snapshot, and texnet.events 48,473 earthquakes (2016-12 to present) pulled
from the TexNet ArcGIS catalog after confirming no bulk/FDSN endpoint
exists. The texnet.swd_seismicity MV materializes 1.92M commercial-SWD x
event pairs within 25km with sane spot checks (an M5.1 at 1,269m from a
Fisher County SWD matches the 2024 Hermleigh sequence). FracFocus match
rates: 88.4% vs well_permits, 76.4% vs wells; the sub-80% wells rate was
investigated per the gate and isolated to warehouse vintage, not API
formatting: the RRC wellbore EWA extract is Nov-2020 content and the
daf802 permit master has a 2023-25 hole (186 permits in 2024 vs ~10K
expected). Remediation completed same day: 1,041 daily archive files
backfilled 2023-25, restoring per-year counts to trend (2023: 9,225,
2024: 8,657, 2025: 7,420 vs 186 in 2024 before) and lifting the FracFocus
match rate from 88.4% to 98.8% (99.7% for 2023+ disclosures). OPEN
follow-up: canonical.wells completion data remains Nov-2020 vintage; the
MFT EWA file itself is the stale artifact, so a different RRC source
(nightly completions zips or statewide API files) is needed for wellbore
recency. Pipelines documented in docs/acquisition/.

## Phase 3: Railway Deploy Hardening (complete, 2026-06-10)

Zero public 502s observed across a full deploy (54 polls at 5s through
push, build, swap, and 3.5 minutes post-SUCCESS), achieved in one
iteration (commit 490fefc). Root cause was double: no healthcheck gated
Railway's traffic swap, and a /dev/tcp probe in start.sh (a bash-ism that
always fails under dash) burned a guaranteed 30s every boot. Now uvicorn
binds in ~3s gated only on the cloudflared DB listener, Tailscale boots in
the background, /healthz (no DB dependency) gates the swap with a 300s
timeout, and the whole bootstrap is guarded behind RAILWAY_ENVIRONMENT so
R730's plain systemd path is untouched. R730 verified active with
/health and /healthz both 200 after merge.

## Phase 4: Enrichment at Scale (complete, 2026-06-10)

Eval-gated and running. Taxonomy v1 (35 closed categories, derived from
top-200 TX permit_type frequencies); eval set of 500 stratified TX permits
labeled by qwen3.5:122b with a claude critique pass (86.6% agreement, 67
critic overrides, exclusions logged). Gate iteration trail: 122B+v1 passed
at 93.87% but at ~4 rows/min the 3.8M-row bulk run would take ~660 days;
35B prompt iterations v2 and v3 each fixed two categories and broke two
others; the fix that ended the whack-a-mole was recognizing the contested
cases are deterministic: a rules layer (pre_classify, 37 unit tests from
real confusion rows) handles trade-prefix/sign/demolition/flatwork permits
at 100% accuracy and the GPU-resident 35B classifies only the genuinely
fuzzy remainder. Hybrid result: 97.14% overall, no major category below
94%, GATE PASS, independently recomputed from artifacts. Bulk run
tx_bulk_v2 is live on R730 at ~34 rows/min (8.5x the 122B): recent slice
(2024-26) enriched in ~6 days, 2020+ in ~25, full 3.8M in ~78. Production
safety added after an incident: an un-gated prompt variant briefly wrote
56 rows (purged); canonical.classifier_gate + a startup guard now ensure
only eval-gated classifier versions can write enrichment rows. Momentum
MV SQL is staged and can ship when recent-slice coverage lands.

## Sprint complete

All five phases hit their completion promises. Operational notes: a
self-rewaking Fable background agent caused repeated dual-operator
interference and one production data incident during Phase 4 (purged,
gate-guarded); per Will's directive the standing model pipeline is now
Fable plan -> Opus orchestrate -> Sonnet build -> Fable test. The
rules+LLM hybrid pattern and the eval-gate-before-bulk-write guard are
the durable architecture takeaways.
