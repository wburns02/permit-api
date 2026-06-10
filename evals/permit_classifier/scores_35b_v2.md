# Permit Classifier Eval — qwen3.5-35b/taxonomy_v1/prompt_v2

Run: 2026-06-10T16:37:31.562550+00:00  
Eval set: eval_set_v1.jsonl (489 scored of 489)  

## Overall: 91.41% (447/489) — gate FAIL

Gate: >=90% overall AND no category with >=20 examples below 80%. Failing categories: ['sign', 'demolition']

## Per-category

| category | n | accuracy |
|---|---|---|
| other_unknown | 130 | 100.0% |
| mechanical_hvac | 61 | 96.7% |
| plumbing | 56 | 98.2% |
| electrical | 52 | 100.0% |
| land_development | 40 | 100.0% |
| sign | 28 | 60.7%  <-- GATE FAIL |
| residential_new | 21 | 95.2% |
| demolition | 21 | 9.5%  <-- GATE FAIL |
| driveway_flatwork | 12 | 100.0% |
| commercial_remodel_ti | 9 | 100.0% |
| residential_addition | 7 | 100.0% |
| admin_licensing | 7 | 71.4% |
| accessory_structure | 6 | 83.3% |
| roofing | 6 | 100.0% |
| pool_spa | 5 | 100.0% |
| irrigation | 5 | 60.0% |
| residential_remodel | 5 | 80.0% |
| row_utility | 4 | 100.0% |
| foundation_repair | 2 | 100.0% |
| solar | 2 | 50.0% |
| fence | 2 | 50.0% |
| event_temporary | 2 | 100.0% |
| code_enforcement | 2 | 100.0% |
| commercial_new | 1 | 100.0% |
| grading_sitework | 1 | 100.0% |
| fire_systems | 1 | 100.0% |
| tree_landscape | 1 | 100.0% |

## Misclassifications

- `accessory_structure` -> `driveway_flatwork`: [Building Permit / Addition] addition to existing parking to add covered parking
refer to pmt#2014-012483-491
- `solar` -> `electrical`: [Electrical Permit / Auxiliary Power] Install new elec solar system to exist res only.
- `residential_new` -> `multifamily_new`: [Commercial Building Permit / New] Construct new residential cottage in multifamily community/ 3rd Party Plan Review and Inspections/ Winston Services, Inc
- `fence` -> `grading_sitework`: [None] Construction of 60ft. retaining wall
- `admin_licensing` -> `other_unknown`: [None] Annual Fire Inspection / Wolfe Physical Therapy
- `sign` -> `electrical`: [Electrical Permit] Berm Sign For One Lacosta
- `sign` -> `electrical`: [Electrical Permit] Pole Sign For Reader Board
- `sign` -> `electrical`: [Electrical Permit] Building Sign For Logo
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Garage -- Residential
- `demolition` -> `mechanical_hvac`: [Mechanical Permit / Demolition] Demolition Residence(Rear Residence)
- `demolition` -> `mechanical_hvac`: [Mechanical Permit / Demolition] Demolition Of Interior Partitions Plumbing***
- `demolition` -> `mechanical_hvac`: [Mechanical Permit / Demolition] Demolish Commercial
- `demolition` -> `driveway_flatwork`: [Driveway / Sidewalks / Demo] Demo Existing Garage Only
- `sign` -> `electrical`: [Electrical Permit / Projecting] Projecting sign Athenia Bar & Grill
- `sign` -> `electrical`: [Electrical Permit / Roof] Remove existing Cricket roof sign and install new Cricket roof sign in the same location.
- `admin_licensing` -> `fire_systems`: [Commercial Certificate of Occupancy (CO)] Stag Fire Protection
- `sign` -> `electrical`: [Electrical Permit / Billboard] Pole Sign For Tanglewood Village Tenants
- `demolition` -> `mechanical_hvac`: [Mechanical Permit / Demolition] Demolish Comm Bldg. To Min Stds.(45 Days)
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Interrior Walls And Raised Floor
- `demolition` -> `electrical`: [Electrical Permit / Demolition] Demolish Min Sta Carport
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Existing Building
- `sign` -> `electrical`: [Electrical Permit / Change Out] Pole Sign For Daybridge Learning Center
- `sign` -> `electrical`: [Electrical Permit / Billboard] Building Sign Ace Hardware
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Room Of Existing Residence
- `sign` -> `electrical`: [Electrical Permit / Billboard] Building Sign For Wholefoods Market
- `demolition` -> `mechanical_hvac`: [Mechanical Permit / Demolition] Demolish And Replace For 3 Motel Rooms
- `demolition` -> `electrical`: [Electrical Permit / Demolition] Demolish One Story Garage
- `sign` -> `electrical`: [Electrical Permit / Billboard] Building Sign For Yankee Clipper
- `sign` -> `electrical`: [Electrical Permit / Billboard] Reface To Existing To Pole Sign Bright Banc
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Res
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Existing Residential Only
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Exist Res
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Garage
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Exist. Res
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Res-Min Stand
- `plumbing` -> `mobile_home`: [Plumbing Permit / Remodel] Connect Mobile Home
- `mechanical_hvac` -> `plumbing`: [Plumbing Permit / Remodel] Mechanical Changeout (Ecsd)
- `irrigation` -> `plumbing`: [Plumbing Permit / Remodel] Install Irrigation System Only
- `irrigation` -> `plumbing`: [Plumbing Permit / Remodel] Irrigation System Residential
- `mechanical_hvac` -> `electrical`: [Mechanical Permit / Remodel] Upgrade Electrical Service
- `residential_remodel` -> `multifamily_new`: [Building Permit / Remodel] Remodel Exist To Create 2 Eff.Apts.& Office
- `demolition` -> `driveway_flatwork`: [Driveway / Sidewalks / Demo] Demolish Residence & Detatched Garage
