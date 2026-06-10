# Permit Classifier Eval — qwen3.5-35b/taxonomy_v1/prompt_v3

Run: 2026-06-10T16:46:24.436739+00:00  
Eval set: eval_set_v1.jsonl (489 scored of 489)  

## Overall: 90.59% (443/489) — gate FAIL

Gate: >=90% overall AND no category with >=20 examples below 80%. Failing categories: ['plumbing', 'electrical']

## Per-category

| category | n | accuracy |
|---|---|---|
| other_unknown | 130 | 99.2% |
| mechanical_hvac | 61 | 83.6% |
| plumbing | 56 | 75.0%  <-- GATE FAIL |
| electrical | 52 | 71.2%  <-- GATE FAIL |
| land_development | 40 | 100.0% |
| sign | 28 | 100.0% |
| residential_new | 21 | 95.2% |
| demolition | 21 | 90.5% |
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
| fence | 2 | 100.0% |
| event_temporary | 2 | 100.0% |
| code_enforcement | 2 | 100.0% |
| commercial_new | 1 | 100.0% |
| grading_sitework | 1 | 100.0% |
| fire_systems | 1 | 100.0% |
| tree_landscape | 1 | 100.0% |

## Misclassifications

- `mechanical_hvac` -> `residential_addition`: [Mechanical Permit / Addition and Remodel] new addition and interior remodel to existing sf res
- `accessory_structure` -> `driveway_flatwork`: [Building Permit / Addition] addition to existing parking to add covered parking
refer to pmt#2014-012483-491
- `plumbing` -> `residential_addition`: [Plumbing Permit / Addition and Remodel] remodel garage to convert into an office and add a 2nd story to create a bedroom & a bath
- `residential_new` -> `multifamily_new`: [Commercial Building Permit / New] Construct new residential cottage in multifamily community/ 3rd Party Plan Review and Inspections/ Winston Services, Inc
- `plumbing` -> `mobile_home`: [Plumbing Permit] Mobile Home
- `demolition` -> `mechanical_hvac`: [Mechanical Permit / Demolition] Demolition Of Interior Partitions Plumbing***
- `plumbing` -> `commercial_remodel_ti`: [Plumbing Permit / Interior Demo Non-Structural] Office/Warehouse Insulate And Condition Office
- `plumbing` -> `other_unknown`: [Plumbing / Single Family] 
- `admin_licensing` -> `fire_systems`: [Commercial Certificate of Occupancy (CO)] Stag Fire Protection
- `other_unknown` -> `row_utility`: [Council Agenda Item] Item J.1: Deliberation and possible action to award Bid #26-088 to Adventures Outback (AO Services) for the FM2770 12-in
- `electrical` -> `multifamily_new`: [Electrical Work Authorization / Building] Addie's Point Multi-Family
- `electrical` -> `commercial_new`: [Electrical Permit / Building] The Magna Company LLC
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Interrior Walls And Raised Floor
- `electrical` -> `oil_gas_surface`: [Electrical Permit / Demolition] Replace 3 12000 Galgas Tanks/1 1000 Bulk Oil
- `plumbing` -> `mobile_home`: [Plumbing Permit / Demolition] Move Mh Onto Lot
- `plumbing` -> `mobile_home`: [Plumbing Permit / Remodel] Connect Mobile Home
- `electrical` -> `residential_new`: [Electrical Permit / New] New One Story Residence W/Attached Garge
- `plumbing` -> `accessory_structure`: [Plumbing Permit / New] Replace Existing Carport With Garage/Workshop
- `mechanical_hvac` -> `plumbing`: [Plumbing Permit / Remodel] Mechanical Changeout (Ecsd)
- `mechanical_hvac` -> `other_unknown`: [Mechanical Permit / New] 2-Slip Boat Dock
- `electrical` -> `mobile_home`: [Electrical Permit / Remodel] Connect M/H Only
- `mechanical_hvac` -> `commercial_remodel_ti`: [Mechanical Permit / Remodel] Interior Remodel To Existing Admn/Bus Offices
- `plumbing` -> `other_unknown`: [Plumbing / Standalone] 
- `plumbing` -> `other_unknown`: [Plumbing / Standalone] 
- `plumbing` -> `other_unknown`: [Plumbing / Umbrella] 
- `electrical` -> `other_unknown`: [Electrical / Umbrella] 
- `electrical` -> `other_unknown`: [Electrical / Standalone] 
- `plumbing` -> `multifamily_new`: [Plumbing Permit / New] Unit# 3 - New 2-story Condominium Residence with attached garage covered porches and patio.  **SMART HOUSING - CONDO RES
- `mechanical_hvac` -> `other_unknown`: [Mechanical / Umbrella] 
- `plumbing` -> `other_unknown`: [Plumbing / Standalone] 
- `plumbing` -> `other_unknown`: [Plumbing / Standalone] 
- `electrical` -> `other_unknown`: [Electrical / Standalone] 
- `plumbing` -> `other_unknown`: [Plumbing / Standalone] 
- `electrical` -> `other_unknown`: [Electrical / Standalone] 
- `electrical` -> `other_unknown`: [Electrical / Umbrella] 
- `electrical` -> `mechanical_hvac`: [Electrical Permit / Remodel] Mechanical Changeout Residential (Rmd)Permit Expired****
- `electrical` -> `mechanical_hvac`: [Electrical Permit / Remodel] Replace & Relocate Cooling Tower Commercial
- `mechanical_hvac` -> `other_unknown`: [Mechanical Permit / New] 3-H-T  BCD
- `electrical` -> `residential_remodel`: [Electrical Permit / Remodel] Remodel & Reroof Apts
- `mechanical_hvac` -> `electrical`: [Mechanical Permit / Remodel] Upgrade Electrical Service
- `mechanical_hvac` -> `mobile_home`: [Mechanical Permit / Remodel] Connect Mobile Home
- `mechanical_hvac` -> `commercial_remodel_ti`: [Mechanical Permit / Remodel] Rem Shell Space To Create Restaurant
- `mechanical_hvac` -> `residential_remodel`: [Mechanical Permit / Remodel] Apt Complex 130 Units
- `electrical` -> `residential_remodel`: [Electrical Permit / Remodel] Remodel
- `residential_remodel` -> `multifamily_new`: [Building Permit / Remodel] Remodel Exist To Create 2 Eff.Apts.& Office
- `electrical` -> `commercial_remodel_ti`: [Electrical Permit / Remodel] Comm Lease Space
