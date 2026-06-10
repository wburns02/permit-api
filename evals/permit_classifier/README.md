# TX Permit Classifier Eval — Rig & Permit Radar Phase 4

Eval suite for the closed-set category classifier over `canonical.permits`
(TX universe, ~3.81M rows) running on local qwen3.5:122b (R730 Ollama).
Structure borrowed from `react-crm-api/evals/analyzer/`.

## TX universe definition

`canonical.jurisdictions.state='TX'` is polluted (hot_leads `state` column
defaults to 'TX'; MGO-scraped LA/FL/TN/SC/... jurisdictions all got stamped
", TX"). The vetted universe is:

```sql
jurisdiction_id IN (SELECT id FROM canonical.jurisdictions WHERE state='TX')
AND source_id IN (SELECT source_id FROM canonical.enrichment_tx_sources)
```

`canonical.enrichment_tx_sources` (121 sources, each with a recorded basis) is
created by `scripts/permit_enrichment_schema.sql`. Result: 3,808,966 rows
(vs 3,878,957 raw state='TX'; ~70K excluded as known non-TX, ambiguous, or
test sources). Zip-prefix audit of hot_leads (`data/tx_source_zip_audit.txt`)
drove the exclusions; notable trap: `mgo_gonzales` is Gonzales LA, not
Gonzales TX (99.8% 707xx zips).

## Pipeline

1. `sample_eval_set.py` — 500 permits stratified across permit_type frequency
   buckets (top-20 / rank 21-200 / long tail, 167 each), decades (~100 per
   decade bucket pre-1990 through 2020s), with per-stratum source_id
   round-robin -> `eval_candidates.jsonl`
2. `label_qwen.py` — candidate labels from qwen3.5:122b with the production
   prompt (`scripts/permit_classifier_lib.py`), batches of 8, JSON-schema
   constrained output -> `qwen_labels.jsonl`
3. `critique_claude.py` — independent second pass via `claude -p`
   (subscription CLI, batches of 25): agree/disagree + better category + one-
   line reason -> `critique.jsonl`
4. `build_eval_set.py` — merge -> `eval_set_v1.jsonl` + `exclusions.jsonl`
5. `run_eval.py` — score a prompt version against the final labels ->
   `scores_v1.md`. Exit 0 only if the gate passes.

## Disagreement resolution policy (applied by build_eval_set.py)

- Critique agrees -> Qwen label stands (provenance `qwen+claude-agree`).
- Critique disagrees -> resolved ONLY by these explicit rules (the side
  matching the rule wins, regardless of which model said it):
  - **R1 trade-stays-trade**: permit_type starting with
    Electrical/Plumbing/Mechanical keeps the trade category even when the
    description names the parent project (new house etc.).
  - **R1b flatwork-stays-flatwork**: permit_type starting with
    Driveway/Sidewalk/Paving keeps `driveway_flatwork` (same own-scope
    principle).
  - **R2 specific-beats-general**: description clearly naming roof / solar /
    pool / septic / irrigation / fence / sign / foundation repair / driveway-
    sidewalk / demolition wins over a generic building or trade label.
  - **R3 empty-generic**: generic department-bucket permit_type with an
    empty/uninformative description -> `other_unknown`.
  - **R4 attached-vs-detached**: detached shed/garage/carport ->
    `accessory_structure`; attached addition -> `residential_addition`.
- Disagreements no rule covers are EXCLUDED with the reason logged in
  `exclusions.jsonl` (id, both labels, critique reason).
- `resolutions.json` ({id: {final, why}}) allows manual spot overrides and
  takes precedence over everything.

## Known caveat

Candidate labels originate from the same model+prompt being evaluated, so
items where critique simply agreed are not independent ground truth; the eval
primarily certifies (a) frontier-model agreement with the label set and
(b) stability of the prompt against regressions when iterating. Disagreed-and-
rule-resolved items are the discriminative slice.

## Gate

>= 90% overall accuracy AND no category with >= 20 eval examples below 80%.
Iterate on the PROMPT only (bump `PROMPT_VERSION` in
`scripts/permit_classifier_lib.py`), never on the eval data.
