# Permit Classifier Eval — qwen3.5-122b/taxonomy_v1/prompt_v2

Run: 2026-06-10T16:36:26.995848+00:00  
Eval set: eval_set_v1.jsonl (489 scored of 489)  

## Overall: 90.18% (441/489) — gate FAIL

Gate: >=90% overall AND no category with >=20 examples below 80%. Failing categories: ['sign', 'demolition']

## Per-category

| category | n | accuracy |
|---|---|---|
| other_unknown | 130 | 100.0% |
| mechanical_hvac | 61 | 98.4% |
| plumbing | 56 | 100.0% |
| electrical | 52 | 98.1% |
| land_development | 40 | 100.0% |
| sign | 28 | 25.0%  <-- GATE FAIL |
| residential_new | 21 | 100.0% |
| demolition | 21 | 4.8%  <-- GATE FAIL |
| driveway_flatwork | 12 | 100.0% |
| commercial_remodel_ti | 9 | 100.0% |
| residential_addition | 7 | 100.0% |
| admin_licensing | 7 | 71.4% |
| accessory_structure | 6 | 100.0% |
| roofing | 6 | 100.0% |
| pool_spa | 5 | 100.0% |
| irrigation | 5 | 60.0% |
| residential_remodel | 5 | 100.0% |
| row_utility | 4 | 100.0% |
| foundation_repair | 2 | 100.0% |
| solar | 2 | 50.0% |
| fence | 2 | 100.0% |
| event_temporary | 2 | 100.0% |
| code_enforcement | 2 | 100.0% |
| commercial_new | 1 | 100.0% |
| grading_sitework | 1 | 100.0% |
| fire_systems | 1 | 100.0% |
| tree_landscape | 1 | 100.0% |

## Misclassifications

- `solar` -> `electrical`: [Electrical Permit / Auxiliary Power] Install new elec solar system to exist res only.
- `sign` -> `electrical`: [Electrical Sign New Construction / OFFICE BUILDING] ERECT ATTACHED SIGN
- `sign` -> `electrical`: [Electrical Permit] Berm Sign For One Lacosta
- `sign` -> `electrical`: [Electrical Permit] Pole Sign For Reader Board
- `demolition` -> `accessory_structure`: [Building Permit / Demolition] Demolish Ext Carport & Front Porch/See Final Permit#1984-015306
- `sign` -> `electrical`: [Electrical Permit] Building Sign For Logo
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Garage -- Residential
- `demolition` -> `mechanical_hvac`: [Mechanical Permit / Demolition] Demolition Residence(Rear Residence)
- `demolition` -> `mechanical_hvac`: [Mechanical Permit / Demolition] Demolition Of Interior Partitions Plumbing***
- `demolition` -> `mechanical_hvac`: [Mechanical Permit / Demolition] Demolish Commercial
- `sign` -> `electrical`: [Electrical Permit / Billboard] Wall Sign Blockbuster Video East Elev
- `sign` -> `electrical`: [Electrical Permit / Demolition] Wall Sign Ibc Bank (North Elevation)
- `sign` -> `electrical`: [Electrical Permit / Billboard] Wall Sign Floor King East Elev
- `sign` -> `electrical`: [Electrical Permit / Billboard] Billboard Relocation From 7002 N Fm 620 To 8105 Research Blvd.
- `sign` -> `electrical`: [Electrical Permit / Billboard] New Freestanding Sign Mazda South/35Oah
- `demolition` -> `driveway_flatwork`: [Driveway / Sidewalks / Demo] Demo Existing Garage Only
- `sign` -> `electrical`: [Electrical Permit / Projecting] Projecting sign Athenia Bar & Grill
- `sign` -> `electrical`: [Electrical Permit / Roof] Remove existing Cricket roof sign and install new Cricket roof sign in the same location.
- `sign` -> `electrical`: [Electrical Permit / Projecting] New double sided exposed neon illuminated projecting wall sign for SWAN DIVE.
- `sign` -> `electrical`: [Electrical Permit / Projecting] 19-3131 WEST MICHELADAS PROJECTING SIGN 333 E 2ND ST
- `admin_licensing` -> `other_unknown`: [Commercial Certificate of Occupancy (CO)] roofing supplies
- `admin_licensing` -> `other_unknown`: [Commercial Certificate of Occupancy (CO)] Stag Fire Protection
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
- `sign` -> `electrical`: [Electrical Permit / Billboard] Building Sign For Yankee Clipper
- `sign` -> `electrical`: [Electrical Permit / Billboard] Building Sign For Cimarron Pawn Shop
- `demolition` -> `plumbing`: [Plumbing Permit / Demolition] Demolish Res-Min Stand
- `electrical` -> `mechanical_hvac`: [Mechanical Permit / Remodel] Replace Service Panel
- `mechanical_hvac` -> `plumbing`: [Plumbing Permit / Remodel] Mechanical Changeout (Ecsd)
- `irrigation` -> `plumbing`: [Plumbing Permit / Remodel] Install Irrigation System Only
- `irrigation` -> `plumbing`: [Plumbing Permit / Remodel] Irrigation System Residential
- `demolition` -> `driveway_flatwork`: [Driveway / Sidewalks / Demo] Demolish Residence & Detatched Garage
