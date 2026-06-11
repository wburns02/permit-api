# Permit Classifier Eval — rules_v1+qwen3.5-35b/taxonomy_v1/prompt_v4

Run: 2026-06-10T17:01:56.665592+00:00  
Eval set: eval_set_v1.jsonl (489 scored of 489)  
Rules layer handled: 202/489 (41.3%)  

## Overall: 97.14% (475/489) — gate PASS

Gate: >=90% overall AND no category with >=20 examples below 80%. Failing categories: none

## Per-category

| category | n | accuracy |
|---|---|---|
| other_unknown | 130 | 98.5% |
| mechanical_hvac | 61 | 96.7% |
| plumbing | 56 | 96.4% |
| electrical | 52 | 94.2% |
| land_development | 40 | 100.0% |
| sign | 28 | 100.0% |
| residential_new | 21 | 95.2% |
| demolition | 21 | 100.0% |
| driveway_flatwork | 12 | 100.0% |
| commercial_remodel_ti | 9 | 100.0% |
| residential_addition | 7 | 100.0% |
| admin_licensing | 7 | 85.7% |
| accessory_structure | 6 | 66.7% |
| roofing | 6 | 100.0% |
| pool_spa | 5 | 100.0% |
| irrigation | 5 | 100.0% |
| residential_remodel | 5 | 80.0% |
| row_utility | 4 | 100.0% |
| foundation_repair | 2 | 100.0% |
| solar | 2 | 100.0% |
| fence | 2 | 100.0% |
| event_temporary | 2 | 100.0% |
| code_enforcement | 2 | 100.0% |
| commercial_new | 1 | 100.0% |
| grading_sitework | 1 | 100.0% |
| fire_systems | 1 | 100.0% |
| tree_landscape | 1 | 100.0% |

## Misclassifications

- `accessory_structure` -> `driveway_flatwork`: [Building Permit / Addition] addition to existing parking to add covered parking
refer to pmt#2014-012483-491
- `residential_new` -> `multifamily_new`: [Commercial Building Permit / New] Construct new residential cottage in multifamily community/ 3rd Party Plan Review and Inspections/ Winston Services, Inc
- `admin_licensing` -> `fire_systems`: [None] Annual Fire Inspection / Wolfe Physical Therapy
- `other_unknown` -> `residential_new`: [Legacy Permit  - Legacy] RESIDENTIAL BUILDING
- `other_unknown` -> `residential_new`: [Legacy Permit  - Legacy] RESIDENTIAL BUILDING
- `accessory_structure` -> `other_unknown`: [Legacy Permit  - Legacy] ACCESSORY BUILDING
- `electrical` -> `oil_gas_surface`: [Electrical Permit / Demolition] Replace 3 12000 Galgas Tanks/1 1000 Bulk Oil
- `mechanical_hvac` -> `demolition`: [Mechanical Permit / Demolition] Demo Acc(Gar)Bldg & Recreate New Acc Bldg (Gar
- `plumbing` -> `mobile_home`: [Plumbing Permit / Demolition] Move Mh Onto Lot
- `electrical` -> `mechanical_hvac`: [Mechanical Permit / Remodel] Replace Service Panel
- `mechanical_hvac` -> `plumbing`: [Plumbing Permit / Remodel] Mechanical Changeout (Ecsd)
- `plumbing` -> `multifamily_new`: [Plumbing Permit / New] Unit# 3 - New 2-story Condominium Residence with attached garage covered porches and patio.  **SMART HOUSING - CONDO RES
- `electrical` -> `mechanical_hvac`: [Electrical Permit / Remodel] Mechanical Changeout Residential (Rmd)Permit Expired****
- `residential_remodel` -> `multifamily_new`: [Building Permit / Remodel] Remodel Exist To Create 2 Eff.Apts.& Office
