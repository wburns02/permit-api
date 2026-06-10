# Permit Classifier Eval — qwen3.5-122b/taxonomy_v1/prompt_v1

Run: 2026-06-10T14:10:33.674892+00:00  
Eval set: eval_set_v1.jsonl (489 scored of 489)  

## Overall: 93.87% (459/489) — gate PASS

Gate: >=90% overall AND no category with >=20 examples below 80%. Failing categories: none

## Per-category

| category | n | accuracy |
|---|---|---|
| other_unknown | 130 | 100.0% |
| mechanical_hvac | 61 | 83.6% |
| plumbing | 56 | 82.1% |
| electrical | 52 | 92.3% |
| land_development | 40 | 100.0% |
| sign | 28 | 100.0% |
| residential_new | 21 | 100.0% |
| demolition | 21 | 100.0% |
| driveway_flatwork | 12 | 50.0% |
| commercial_remodel_ti | 9 | 100.0% |
| residential_addition | 7 | 100.0% |
| admin_licensing | 7 | 100.0% |
| accessory_structure | 6 | 100.0% |
| roofing | 6 | 100.0% |
| pool_spa | 5 | 100.0% |
| irrigation | 5 | 100.0% |
| residential_remodel | 5 | 100.0% |
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

- `driveway_flatwork` -> `residential_remodel`: [Driveway / Sidewalks] Enclose Exist Garage For A Laundry/Utilityrm
- `driveway_flatwork` -> `commercial_remodel_ti`: [Driveway / Sidewalks / Modification] Change Of Use From Retail/Office
- `driveway_flatwork` -> `commercial_remodel_ti`: [Driveway / Sidewalks / Modification] Remodel For Admin & Professional Offices
- `mechanical_hvac` -> `commercial_remodel_ti`: [Mechanical Permit / Addition] Add To Extend Library & Remodel Patio To *****
- `mechanical_hvac` -> `residential_addition`: [Mechanical Permit / Addition] One Story Addition To Extend Living Room
- `driveway_flatwork` -> `commercial_remodel_ti`: [Driveway / Sidewalks / Modification] Finish-Out For Silicon Laboratories
- `plumbing` -> `residential_addition`: [Plumbing Permit / Addition and Remodel] remodel garage to convert into an office and add a 2nd story to create a bedroom & a bath
- `mechanical_hvac` -> `accessory_structure`: [Mechanical Permit / Addition] Addn To Res To Create Decks Storage & Carport
- `plumbing` -> `mobile_home`: [Plumbing Permit] Mobile Home
- `plumbing` -> `demolition`: [Plumbing Permit / Interior Demo Non-Structural] Remodel/Repair Portable Classroom & Relocate
- `plumbing` -> `demolition`: [Plumbing Permit / Interior Demo Non-Structural] Relocate/Remodel Portable Classroom
- `mechanical_hvac` -> `demolition`: [Mechanical Permit / Interior Demo Non-Structural] Remodel/Repair Portable Classroom & Relocate
- `plumbing` -> `demolition`: [Plumbing Permit / Interior Demo Non-Structural] Remodel/Repair Portable Classroom & Relcoate
- `plumbing` -> `other_unknown`: [Plumbing / Single Family] 
- `driveway_flatwork` -> `residential_addition`: [Driveway / Sidewalks / Demo] Demo & Addn To Exist Res To Create New Gar**
- `electrical` -> `demolition`: [Electrical Permit / Demolition] Replace 3 12000 Galgas Tanks/1 1000 Bulk Oil
- `electrical` -> `demolition`: [Electrical Permit / Demolition] Apartments
- `mechanical_hvac` -> `demolition`: [Mechanical Permit / Demolition] One Stry Frm Res W/Mas Ven And Att Gar
- `mechanical_hvac` -> `demolition`: [Mechanical Permit / Demolition] Demo Acc(Gar)Bldg & Recreate New Acc Bldg (Gar
- `plumbing` -> `mobile_home`: [Plumbing Permit / Demolition] Move Mh Onto Lot
- `plumbing` -> `mobile_home`: [Plumbing Permit / Demolition] Move Duplex Onto Lot & Repair & Remodel
- `mechanical_hvac` -> `residential_remodel`: [Mechanical Permit / Demolition] Remodel Residence
- `electrical` -> `commercial_remodel_ti`: [Electrical Permit / Demolition] Tenant Finish Out To Create Addm.Office
- `mechanical_hvac` -> `commercial_remodel_ti`: [Mechanical Permit / Demolition] Tenant Finish Out To Create Addm.Office
- `plumbing` -> `mobile_home`: [Plumbing Permit / Remodel] Connect Mobile Home
- `plumbing` -> `accessory_structure`: [Plumbing Permit / New] Replace Existing Carport With Garage/Workshop
- `driveway_flatwork` -> `residential_new`: [Driveway / Sidewalks / New] Sf Residence W/Attached Garage CovD Porch & CovD Patio
- `electrical` -> `mechanical_hvac`: [Electrical Permit / Remodel] Replace & Relocate Cooling Tower Commercial
- `mechanical_hvac` -> `electrical`: [Mechanical Permit / Remodel] Upgrade Electrical Service
- `mechanical_hvac` -> `commercial_remodel_ti`: [Mechanical Permit / Remodel] Rem Shell Space To Create Restaurant

## Methods note

- Eval set: 500 sampled, 489 kept, 11 excluded (model disagreement with no
  resolution rule; logged in exclusions.jsonl).
- Label provenance: 433 qwen+claude-agree (86.6% raw agreement), 27
  R1-trade-stays-trade, 23 R2-specific-beats-general, 6
  R1b-flatwork-stays-flatwork.
- prompt_v1 predictions are the label-pass outputs themselves (temp 0, same
  model+prompt), so accuracy here measures the share of labels that survived
  the independent claude critique + rule resolution — i.e. how often the
  v1 prompt already lands on the rule-correct side. The discriminative slice
  is the 56 rule-resolved disagreements.
- Known v1 weakness (drives the 30 misses): trade/flatwork permits whose
  description names the parent project (rule-1 drift), and trade permits with
  Demolition / Interior-Demo subtypes. prompt_v2 targets exactly this.
