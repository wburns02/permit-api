# Permit Classifier Cascade Eval — rules_v2+qwen3.5-35b/taxonomy_v1/prompt_v4

Run: 2026-06-11T15:04:08.407601+00:00  
Eval set: eval_set_v1.jsonl (489 scored of 489)  
LLM predictions: preds_qwen3.5-35b_prompt_v4.jsonl (offline, cached)  
Rules layer handled: 337/489 (68.9%)  

## Overall: 98.16% (480/489) — gate PASS

Gate: >=90% overall AND no category with >=20 examples below 80%. Failing categories: none

## Per-category

| category | n | accuracy |
|---|---|---|
| other_unknown | 130 | 99.2% |
| mechanical_hvac | 61 | 98.4% |
| plumbing | 56 | 100.0% |
| electrical | 52 | 98.1% |
| land_development | 40 | 100.0% |
| sign | 28 | 100.0% |
| residential_new | 21 | 95.2% |
| demolition | 21 | 95.2% |
| driveway_flatwork | 12 | 100.0% |
| commercial_remodel_ti | 9 | 100.0% |
| residential_addition | 7 | 100.0% |
| admin_licensing | 7 | 85.7% |
| accessory_structure | 6 | 83.3% |
| roofing | 6 | 100.0% |
| pool_spa | 5 | 100.0% |
| irrigation | 5 | 100.0% |
| residential_remodel | 5 | 80.0% |
| row_utility | 4 | 100.0% |
| foundation_repair | 2 | 100.0% |
| solar | 2 | 100.0% |
| fence | 2 | 50.0% |
| event_temporary | 2 | 100.0% |
| code_enforcement | 2 | 100.0% |
| commercial_new | 1 | 100.0% |
| grading_sitework | 1 | 100.0% |
| fire_systems | 1 | 100.0% |
| tree_landscape | 1 | 100.0% |

## Misclassifications

- `accessory_structure` -> `driveway_flatwork` (llm): [Building Permit / Addition] addition to existing parking to add covered parking
refer to pmt#2014-012483-491
- `residential_new` -> `multifamily_new` (llm): [Commercial Building Permit / New] Construct new residential cottage in multifamily community/ 3rd Party Plan Review and Inspections/ Winston Services, Inc
- `fence` -> `grading_sitework` (llm): [None] Construction of 60ft. retaining wall
- `other_unknown` -> `mobile_home` (llm): [None] Relocate
- `demolition` -> `mechanical_hvac` (llm): [Mechanical Permit / Demolition] Demolition Of Interior Partitions Plumbing***
- `admin_licensing` -> `fire_systems` (llm): [Commercial Certificate of Occupancy (CO)] Stag Fire Protection
- `electrical` -> `oil_gas_surface` (llm): [Electrical Permit / Demolition] Replace 3 12000 Galgas Tanks/1 1000 Bulk Oil
- `mechanical_hvac` -> `plumbing` (llm): [Plumbing Permit / Remodel] Mechanical Changeout (Ecsd)
- `residential_remodel` -> `multifamily_new` (llm): [Building Permit / Remodel] Remodel Exist To Create 2 Eff.Apts.& Office
