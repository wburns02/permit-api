"""Seed parcel-screen data: jurisdictions + state laws + density tables.

Idempotent — uses upsert semantics so re-running is safe.

Usage:
    cd /home/will/permit-api-live
    PYTHONPATH=. python3 scripts/seed_parcel_screen.py

Origin: Rob's `.claude/skills/parcel-screen/` skill. The canonical text of each
state law lives at `data/parcel-screen/state-law/*.md` in this repo. The
structured eligibility/yield JSON below is hand-crafted to mirror Rob's
markdown — if you update an .md file, update the corresponding dict here.
"""

import argparse
import asyncio
import json
import logging
import os
from datetime import date
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.database import async_session_maker
from app.models.parcel_screen import (
    ParcelJurisdiction,
    ParcelStateLaw,
    ParcelZoneDensity,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("seed_parcel_screen")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data" / "parcel-screen"


# ===========================================================================
# Jurisdictions
# ===========================================================================
JURISDICTIONS = [
    {
        "state": "CA",
        "city_slug": "rialto",
        "display_name": "Rialto, CA",
        "gis_viewer_url": "https://gis.rialtoca.gov/portal/apps/experiencebuilder/experience/?id=89d11c74cb054b3aa22f28759e6296c7",
        "parcels_url": "https://gis.rialtoca.gov/server/rest/services/Hosted/Rialto_Parcels_12_24/FeatureServer/24",
        "zoning_url": "https://gis.rialtoca.gov/server/rest/services/Hosted/Rialto_Zoning/FeatureServer/0",
        "general_plan_url": "https://gis.rialtoca.gov/server/rest/services/Hosted/General_Plan/FeatureServer/0",
        "specific_plan_url": "https://gis.rialtoca.gov/server/rest/services/Hosted/Specific_Plan_Boundaries/FeatureServer/0",
        "fire_hazard_url": "https://gis.rialtoca.gov/server/rest/services/Hosted/RIA_FHZ_SRA_LRA_202404/FeatureServer/0",
        "apn_field": "apn",
        "spatial_reference_wkid": 102100,
        "notes": "Parcel layer carries zone_code, zone_desc, gp_code, gp_desc, sp_code, sp_desc, fire_zonre (sic) directly. Coordinates in Web Mercator; dim correction needed.",
        "last_verified": date(2026, 5, 11),
    },
    {
        "state": "CA",
        "city_slug": "santa-ana",
        "display_name": "Santa Ana, CA",
        "gis_viewer_url": "https://gis-santa-ana.opendata.arcgis.com/",
        "parcels_url": "https://www.ocgis.com/arcpub/rest/services/Map_Layers/Parcels/MapServer/0",
        "zoning_url": "https://gis.santa-ana.org/server/rest/services/Public/PBA_ZoningClassifications/FeatureServer/0",
        "general_plan_url": "https://gis.santa-ana.org/server/rest/services/Public/PBA_GeneralPlan/FeatureServer/0",
        "specific_plan_url": "https://services1.arcgis.com/u3G8zpmDyNtG4F4e/arcgis/rest/services/GP_Web_Maps_specific_plan_boundaries_230324/FeatureServer/0",
        "focus_areas_url": "https://gis.santa-ana.org/server/rest/services/Public/PBA_FocusAreas/FeatureServer/0",
        "apn_field": "ASSESSMENT_NO",
        "address_field": "SITE_ADDRESS",
        "spatial_reference_wkid": 2230,
        "notes": "Parcels from OC County (ocgis.com), thin. Zoning/GP/specific plan on city's gis.santa-ana.org. NAD83 CA State Plane Zone VI — units FEET, no Mercator correction.",
        "last_verified": date(2026, 5, 11),
    },
]


# ===========================================================================
# State laws — full, mirroring Rob's data/parcel-screen/state-law/*.md
# ===========================================================================
def _gis(id_: str, label: str) -> dict:
    return {"id": id_, "label": label, "category": "gis"}


def _verify(id_: str, label: str) -> dict:
    return {"id": id_, "label": label, "category": "verify"}


STATE_LAWS = [
    # -----------------------------------------------------------------------
    # By-right (always present, no eligibility checks)
    # -----------------------------------------------------------------------
    {
        "state": "CA",
        "law_id": "by-right",
        "display_order": 0,
        "name": "By-right (base zone)",
        "code_section": "Local zoning ordinance",
        "summary": "Whatever the base zone permits ministerially. Always applies as the floor.",
        "eligibility_checklist": [
            {"id": "always", "label": "Always applies", "category": "gis", "auto_pass": True},
        ],
        "yield_formula": {
            "expr": "floor(acres * du_per_ac)",
            "min": 1,
            "note": "For single-unit zones this is 1. For non-residential zones (A-1, C-1, etc.), functionally 1 unless rezoned.",
        },
        "caveats_md": "Base by-right yield is just the zone's du/ac × acreage. ADU stacking, density bonus, and state-law pathways layer ON TOP of this number.",
        "last_verified": date(2026, 5, 11),
    },
    # -----------------------------------------------------------------------
    # SB-9 — Urban Lot Split + Duplex on R-1
    # -----------------------------------------------------------------------
    {
        "state": "CA",
        "law_id": "sb9",
        "display_order": 1,
        "name": "SB-9 (Urban lot split + duplex on R-1)",
        "code_section": "Gov Code §65852.21, §66411.7",
        "effective_date": date(2022, 1, 1),
        "leginfo_url": "https://leginfo.legislature.ca.gov/faces/billNavClient.xhtml?bill_id=202120220SB9",
        "summary": "Ministerial: (a) up to 2 units on a single-family-zoned lot AND (b) a one-time urban lot split that divides the parent lot into two lots, each of which may host up to 2 units. Maximum yield: 4 units on a former R-1 parcel.",
        "eligibility_checklist": [
            _gis("zone_r1", "Zone is single-family residential (R-1, R-1-A, RS, etc.)"),
            _gis("urbanized", "Within urbanized area or urban cluster (US Census)"),
            _gis("not_fhsz", "Not in SRA / Very High FHSZ (or mitigation met)"),
            _gis("not_flood", "Not in FEMA Special Flood Hazard Area (or mitigation met)"),
            _gis("not_farmland", "Not on Prime / Statewide Importance / Unique Farmland (DOC FMMP)"),
            _gis("not_fault", "Not in Alquist-Priolo earthquake fault zone"),
            _gis("not_hazwaste", "Not on Hazardous Waste Site (Cortese list / DTSC)"),
            _gis("not_conservation", "Not in conservation easement or habitat conservation area"),
            _gis("not_historic", "Not historic landmark / not in historic district"),
            _gis("min_lot_post_split", "Parent lot ≥ 2,400 sqft (each new lot ≥ 1,200 sqft post-split)"),
            _verify("no_tenant_3yr", "No tenant-occupied housing demo/alteration in past 3 years"),
            _verify("owner_occ_3yr", "Applicant affirms owner-occupancy for ≥ 3 years post lot-split"),
            _verify("no_rent_restrict", "No rent-restricted (deed-restricted affordable) housing on parcel"),
            _verify("no_ellis_15yr", "No Ellis Act withdrawal in past 15 years"),
            _verify("local_ordinance_sb9", "Local SB-9 implementing ordinance — objective design standards"),
        ],
        "yield_formula": {
            "max_units_with_split": 4,
            "max_units_no_split": 2,
            "min_unit_sqft": 800,
            "min_setback_ft": 4,
            "min_lot_split_pct": 40,
            "note": "2 units × 2 lots = 4 with urban lot split. 2 units without split. Local cannot require parking beyond 1 space/unit (0 within 0.5 mi of major transit).",
        },
        "caveats_md": "Only one lot split per original parcel — not recursive. Local can impose objective design standards but cannot make project physically infeasible. SB-9 stacks with State ADU in some configs (verify local).",
        "last_verified": date(2026, 5, 11),
    },
    # -----------------------------------------------------------------------
    # SB-684 — Small-Lot SF Subdivision
    # -----------------------------------------------------------------------
    {
        "state": "CA",
        "law_id": "sb684",
        "display_order": 2,
        "name": "SB-684 (Small-Lot SF Subdivision)",
        "code_section": "Gov Code §66499.40 et seq.",
        "effective_date": date(2024, 7, 1),
        "leginfo_url": "https://leginfo.legislature.ca.gov/faces/billNavClient.xhtml?bill_id=202320240SB684",
        "summary": "Ministerial small-lot subdivision: MF-zoned parcel (or qualifying urban infill site) up to 5 ac → up to 10 small lots (≥600 sqft each), one SF home per lot. Starter-home, for-sale orientation.",
        "eligibility_checklist": [
            _gis("zone_mf_or_infill", "Zone is multi-family residential OR qualifying urban infill site"),
            _gis("size_le_5ac", "Parcel ≤ 5 acres"),
            _gis("urbanized", "Within urbanized area or urban cluster"),
            _gis("not_fhsz", "Not in SRA / VHFHSZ (or mitigation met)"),
            _gis("not_flood", "Not in FEMA SFHA (or mitigation met)"),
            _gis("not_farmland", "Not Prime / Statewide Importance / Unique Farmland"),
            _gis("not_fault", "Not in Alquist-Priolo fault zone"),
            _gis("not_hazwaste", "Not on Hazardous Waste Site"),
            _gis("not_historic", "Not historic landmark / district"),
            _gis("not_coastal", "Not in coastal zone (unless qualifying urbanized portion)"),
            _gis("perimeter_urban_75pct", "≥75 % of perimeter within ¼ mile is developed urban (qualifying infill path)"),
            _verify("no_demo_10yr", "No housing demo displacing rent-restricted / tenant / Ellis-Act housing in past 10 yrs"),
            _verify("no_deed_restrict", "No deed restriction limiting subdivision"),
            _verify("ministerial_design", "Subdivider commits to ministerial design standards in local ordinance"),
            _verify("local_ordinance_sb684", "Local SB-684 implementing ordinance adopted"),
            _verify("prevailing_wage_local", "Prevailing wage: state generally not required, local ordinance may impose"),
        ],
        "yield_formula": {
            "max_units": 10,
            "min_lot_sqft": 600,
            "max_dwelling_sqft_local_floor": 1750,
            "density_floor_he_sites": 1.00,
            "density_floor_non_he_sites": 0.66,
            "density_alt_floors_du_per_ac": {"metro": 30, "suburban_large": 20, "suburban_small": 15, "rural": 10},
            "note": "10-unit statutory ceiling. GP density is a FLOOR not a cap (66 % for non-HE sites). Stacks with AB-130 remainder.",
        },
        "caveats_md": "'Qualifying infill' requires 75 % perimeter test — careful spatial mapping. 10-lot cap is per parent parcel, cannot phase. For-sale orientation (starter-home intent). Stacks with State ADU on each new lot (verify local).",
        "last_verified": date(2026, 5, 11),
    },
    # -----------------------------------------------------------------------
    # SB-1123 — Extends SB-684 to vacant SFR lots
    # -----------------------------------------------------------------------
    {
        "state": "CA",
        "law_id": "sb1123",
        "display_order": 3,
        "name": "SB-1123 (Extends SB-684 to vacant SFR lots)",
        "code_section": "Gov Code §66499.40 et seq. (amendment)",
        "effective_date": date(2025, 1, 1),
        "leginfo_url": "https://leginfo.legislature.ca.gov/faces/billNavClient.xhtml?bill_id=202320240SB1123",
        "summary": "Extends SB-684 small-lot subdivision pathway to vacant single-family-zoned lots. Same 10-lot, 600-sqft minimum.",
        "eligibility_checklist": [
            _gis("zone_sfr", "Zone is single-family residential"),
            _gis("size_le_5ac", "Parcel ≤ 5 acres"),
            _gis("urbanized", "Within urbanized area or urban cluster"),
            _gis("not_fhsz", "Not in SRA / VHFHSZ (or mitigation met)"),
            _gis("not_flood", "Not in FEMA SFHA (or mitigation met)"),
            _gis("not_farmland", "Not Prime / Statewide Importance / Unique Farmland"),
            _gis("not_fault", "Not in Alquist-Priolo fault zone"),
            _gis("not_hazwaste", "Not on Hazardous Waste Site"),
            _gis("not_historic", "Not historic landmark / district"),
            _gis("not_coastal", "Not in coastal zone (unless qualifying urbanized portion)"),
            _gis("vacancy_indicator", "Improvement value = $0 OR no building footprint (suggests vacant, not definitive)"),
            _verify("truly_vacant", "Truly vacant on the ground — inspect or check recent satellite imagery"),
            _verify("no_demo_10yr", "No housing demo displacing rent-restricted / tenant / Ellis-Act housing in past 10 yrs"),
            _verify("no_deed_restrict", "No deed restriction limiting subdivision"),
            _verify("local_ordinance_sb1123", "Local SB-684 / SB-1123 implementing ordinance covers vacant-SFR path"),
        ],
        "yield_formula": {
            "max_units": 10,
            "min_lot_sqft": 600,
            "density_floor_he_sites": 1.00,
            "density_floor_non_he_sites": 0.66,
            "note": "Identical to SB-684. AB-130 remainder on vacant lot typically about non-residential preservation (sheds, infrastructure) since there's no existing housing.",
        },
        "caveats_md": "'Vacant' is the key gate — not a GIS field, always flag for ground verification. Recently demolished parcels (within 10 yrs of tenant-occupied demo) excluded. Cities still adapting procedures — SB-1123 is a 2024 amendment some may not have updated for.",
        "last_verified": date(2026, 5, 11),
    },
    # -----------------------------------------------------------------------
    # SB-1211 — Up to 8 detached ADUs on MF lots
    # -----------------------------------------------------------------------
    {
        "state": "CA",
        "law_id": "sb1211",
        "display_order": 4,
        "name": "SB-1211 (Up to 8 detached ADUs on MF lots)",
        "code_section": "Gov Code §65852.2 (amendment)",
        "effective_date": date(2025, 1, 1),
        "leginfo_url": "https://leginfo.legislature.ca.gov/faces/billNavClient.xhtml?bill_id=202320240SB1211",
        "summary": "Raises detached-ADU cap on MF lots from 2 to 8. Also clarifies conversion of non-livable space on MF properties.",
        "eligibility_checklist": [
            _gis("zone_mf", "Zone is multi-family residential (R-2, R-3, R-4, RM, RH) or mixed-use permitting MF"),
            _gis("existing_residential", "Parcel has existing residential structure (improvement value > $0, zone-consistent)"),
            _verify("existing_unit_count", "Count of existing legal residential units"),
            _verify("setback_feasibility", "Lot dimensions support up to 8 detached units alongside existing"),
            _verify("local_ordinance_adu", "City has updated ADU ordinance to reflect 2 → 8 detached cap"),
            _verify("utility_capacity", "Utilities and infrastructure can handle 8 added ADUs"),
        ],
        "yield_formula": {
            "max_added_detached_adus": 8,
            "max_conversion_adus_rule": "at least 1 per 4 existing units OR 25 % of existing count (whichever greater)",
            "detached_adu_max_sqft": 1200,
            "note": "Existing + up to 8 detached ADUs + conversion ADUs. Example: 4-unit apartment → 4 + 8 + 1 = 13 total.",
        },
        "caveats_md": "Detached only — internal/attached ADUs follow general State ADU law. Cannot require replacement parking. High leverage for existing MF property acquisitions (added units ministerially approved, no entitlement risk).",
        "last_verified": date(2026, 5, 11),
    },
    # -----------------------------------------------------------------------
    # State ADU/JADU
    # -----------------------------------------------------------------------
    {
        "state": "CA",
        "law_id": "state-adu",
        "display_order": 5,
        "name": "State ADU/JADU Law",
        "code_section": "Gov Code §65852.2 / §65852.22",
        "leginfo_url": "https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=65852.2&lawCode=GOV",
        "summary": "Ministerial, by-right ADU + JADU approval. 60-day shot clock. No replacement parking. State preempts restrictive local rules.",
        "eligibility_checklist": [
            _gis("zone_residential", "Zone is single-family or multi-family residential (or mixed-use permitting residential)"),
            _gis("primary_dwelling", "Has existing or proposed primary dwelling (ADU is 'accessory' — needs a primary)"),
            _verify("local_ordinance_adu", "City ADU ordinance is current with state law"),
            _verify("setback_compliance", "4 ft side/rear for new construction; 0 ft for conversion of existing structures"),
            _verify("utility_connections", "Utility connections at the lot"),
        ],
        "yield_formula": {
            "sfr_add": "+1 ADU (up to 1,200 sqft; local must allow ≥ 800) + 1 JADU (≤ 500 sqft within primary)",
            "sfr_total_units": "existing + 1 ADU + 1 JADU = up to 3 on SFR lot",
            "mf_add_detached": "see SB-1211 (up to 8 detached ADUs)",
            "mf_conversion_adus_rule": "at least 1 per 4 existing units OR 25 % of count",
            "approval_clock_days": 60,
            "note": "No replacement parking. Zero parking within 0.5 mi of transit, in historic districts, permit-required parking zones, or car-share zones.",
        },
        "caveats_md": "An ADU and a JADU on the same SFR lot is permitted. State preempts older local restrictive ordinances. Separate ADU sale allowed in specific conditions (qualifying nonprofit / community land trust).",
        "last_verified": date(2026, 5, 11),
    },
    # -----------------------------------------------------------------------
    # AB-2011 / SB-6 — Housing on commercial corridors
    # -----------------------------------------------------------------------
    {
        "state": "CA",
        "law_id": "ab2011-sb6",
        "display_order": 6,
        "name": "AB-2011 / SB-6 (Housing on commercial corridors)",
        "code_section": "Gov Code §65912.100 et seq. (AB-2011); §65852.24 (SB-6)",
        "effective_date": date(2023, 7, 1),
        "leginfo_url": "https://leginfo.legislature.ca.gov/faces/billNavClient.xhtml?bill_id=202120220AB2011",
        "summary": "Ministerial housing on commercial / office / retail / parking parcels along qualifying corridors. AB-2011 = 100 % affordable or mixed-income; SB-6 = any income mix (prevailing wage + skilled & trained required).",
        "eligibility_checklist": [
            _gis("zone_commercial", "Zone is commercial / office / retail / parking (permits non-residential by-right)"),
            _gis("corridor_row_width", "Parcel abuts qualifying corridor (≥70 ft ROW urban, ≥100 ft suburban)"),
            _gis("not_industrial", "Not heavy industrial; not adjacent to active heavy industrial"),
            _gis("not_tribal_sacred", "Not on Tribal / Sacred land"),
            _gis("not_hazwaste", "Not on Hazardous Waste Site"),
            _gis("not_fhsz", "Not in SRA / VHFHSZ (or mitigation met)"),
            _gis("not_flood", "Not in FEMA SFHA (or mitigation met)"),
            _verify("no_demo_10yr", "No housing demo of rent-restricted / tenant / Ellis-Act housing in past 10 yrs"),
            _verify("no_displacement", "No existing housing displaced (or full replacement + relocation)"),
            _verify("prevailing_wage", "Prevailing wage commitment (both bills)"),
            _verify("skilled_trained_workforce", "Skilled & trained workforce (AB-2011 ≥50 units; SB-6 always)"),
            _verify("affordability_ab2011", "AB-2011 affordability: 8 % very-low + 5 % extremely-low OR 100 % lower-income"),
            _verify("local_implementing_ordinance", "Local AB-2011 / SB-6 implementing ordinance"),
        ],
        "yield_formula": {
            "ab2011_urban_du_per_ac_floor": 30,
            "ab2011_suburban_du_per_ac_floor": 20,
            "ab2011_smaller_du_per_ac_floor": 15,
            "sb6_density_rule": "Housing Element density or zone density, whichever greater",
            "max_height_ft": "35–65 ft floor depending on tier",
            "min_far": "0.6–3.0 floor depending on tier",
            "note": "Stacks with Density Bonus Law for significantly higher effective density. Zero parking within 0.5 mi of transit.",
        },
        "caveats_md": "Labor-cost premium (prevailing wage + STW) raises hard costs — pencils best on ≥50 units. AB-2011 affordability premium compresses revenue; density-bonus stacking is critical to math. Corridor definition is technical — verify against city's adopted corridor map.",
        "last_verified": date(2026, 5, 11),
    },
    # -----------------------------------------------------------------------
    # Density Bonus Law
    # -----------------------------------------------------------------------
    {
        "state": "CA",
        "law_id": "density-bonus",
        "display_order": 7,
        "name": "Density Bonus Law",
        "code_section": "Gov Code §65915 (amended by AB-1287 2023, AB-1893 2024)",
        "leginfo_url": "https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=65915&lawCode=GOV",
        "summary": "Local must grant additional units, concessions, waivers, and parking reductions to projects including affordable units. Stacks on top of base zoning AND most other state law programs. AB-1287 unlocks up to 100 % bonus.",
        "eligibility_checklist": [
            _gis("any_residential_project", "Any qualifying residential project (base zoning or stacked state-law)"),
            _verify("affordability_durable", "Affordability commitment is durable (55-year regulatory agreement typical)"),
            _verify("local_db_ordinance", "Local Density Bonus ordinance is current"),
            _verify("affordability_threshold_met", "Project includes one of: 5 % very-low / 10 % low / 10 % moderate (for-sale) / 100 % affordable / senior / special-needs"),
            _verify("concession_justification", "Concessions are cost-reducing or feasibility-enabling (city can deny if not justified)"),
        ],
        "yield_formula": {
            "bonus_pct_range": [20, 100],
            "max_bonus_single_category": 50,
            "max_bonus_stacked": 100,
            "concessions_by_tier": {"low": 1, "mid": 2, "high": 3, "100pct_affordable": 4},
            "parking_studio_1br": 0.5,
            "parking_2_3br": 1.0,
            "parking_4br_plus": 1.5,
            "parking_near_transit_zero": True,
            "note": "total_units = base × (1 + bonus_pct). Bonus pct depends on affordability type + percentage (Gov Code §65915(f) lookup).",
        },
        "caveats_md": "Most powerful when stacked with SB-684 or AB-2011/SB-6. AB-1287 unlocks 100 % bonus in single-purpose affordable projects — game-changer for nonprofit affordable developers. Affordability premium compresses revenue — model net economics carefully.",
        "last_verified": date(2026, 5, 11),
    },
    # -----------------------------------------------------------------------
    # AB-130 — Remainder parcel modifier
    # -----------------------------------------------------------------------
    {
        "state": "CA",
        "law_id": "ab130",
        "display_order": 8,
        "name": "AB-130 (Remainder parcel modifier)",
        "code_section": "Amends Subdivision Map Act / SHRA",
        "effective_date": date(2025, 1, 1),
        "leginfo_url": "https://leginfo.legislature.ca.gov/faces/billNavClient.xhtml?bill_id=202520260AB130",
        "summary": "Modifier on SB-684 / SB-1123: developer can retain a 'remainder parcel' containing existing improvement that does NOT count toward the 10-lot cap. Unlocks parcels where preserving existing structure beats demolition.",
        "eligibility_checklist": [
            _gis("qualifies_sb684_or_sb1123", "Project qualifies for SB-684 or SB-1123 — see those checklists"),
            _gis("existing_improvement", "Existing improvement on parcel (improvement value > $0)"),
            _gis("sufficient_lot_area", "Lot has area for 10 new ≥600-sqft lots + remainder"),
            _verify("structure_legal", "Existing structure is legal / permitted (not a code violation)"),
            _verify("structure_to_remain", "Developer commits to preservation, not demolition"),
            _verify("remainder_no_new_residential", "Remainder parcel does not contain new residential units"),
            _verify("remainder_uses_compliant", "Remainder uses: existing structures, open space, infrastructure, or non-residential"),
            _verify("sale_lease_restrictions", "Sale / lease / financing restrictions on remainder comply with statute"),
            _verify("local_procedure_supports", "Local SB-684 procedure accommodates remainder designations"),
            _verify("statute_recency", "Verify against current chaptered statute — AB-130 is recent (2025 budget trailer)"),
        ],
        "yield_formula": {
            "sb684_sb1123_yield": "up to 10 new SF units (per parent statute)",
            "ab130_modifier": "+1 retained existing structure (NOT counted toward 10) OR +0 if remainder is open-space / infrastructure",
            "total_dwellings_typical": 11,
            "note": "10 new + (1 retained if applicable) = 11 total dwellings. Retained unit on separate restricted-sale lot.",
        },
        "caveats_md": "Remainder cannot host NEW residential units — purpose is preservation, infrastructure, or open space. Sale/lease/financing restrictions prevent abuse. Modifier only — parcel must independently qualify for SB-684 or SB-1123 first. Verify chaptered text — recent legislation near training cutoff.",
        "last_verified": date(2026, 5, 11),
    },
]


# ===========================================================================
# Density tables — load from data/parcel-screen/density-tables/*.json
# ===========================================================================
def _load_density_tables() -> list[dict]:
    """Convert Rob's per-city JSON files into ParcelZoneDensity rows."""
    rows = []
    density_dir = DATA_DIR / "density-tables"
    if not density_dir.is_dir():
        log.warning(f"density-tables dir missing at {density_dir}")
        return rows

    for fp in sorted(density_dir.glob("*.json")):
        city_slug = fp.stem  # e.g. "rialto"
        try:
            data = json.loads(fp.read_text())
        except Exception as e:
            log.error(f"failed to parse {fp.name}: {e}")
            continue

        meta = data.get("_meta", {})
        source_url = meta.get("source_url")
        last_verified = None
        if meta.get("last_verified"):
            try:
                last_verified = date.fromisoformat(meta["last_verified"])
            except Exception:
                pass

        gp_designations = data.get("_gp_designations")

        for zone_code, attrs in data.items():
            if zone_code.startswith("_"):
                continue  # _meta, _gp_designations
            if not isinstance(attrs, dict):
                continue

            # Normalize "verify" / null / string values to None on numeric fields
            def _num(v):
                if v is None:
                    return None
                if isinstance(v, (int, float)):
                    return v
                return None  # strings like "same as R-1A — verify" become NULL

            rows.append({
                "state": "CA",  # Rob's files are CA today; would key from path in a state-aware structure later
                "city_slug": city_slug,
                "zone_code": zone_code,
                "zone_desc": attrs.get("zone_name"),
                "du_per_ac": _num(attrs.get("du_per_ac")),
                "min_lot_sqft": _num(attrs.get("min_lot_sqft")),
                "min_lot_width_ft": _num(attrs.get("min_lot_width_ft")),
                "max_height_ft": _num(attrs.get("max_height_ft")),
                "front_setback_ft": _num(attrs.get("front_setback_ft")),
                "side_setback_ft": _num(attrs.get("side_setback_ft")),
                "rear_setback_ft": _num(attrs.get("rear_setback_ft")),
                "max_lot_coverage_pct": _num(attrs.get("max_lot_coverage_pct")),
                "is_residential": "Y" if attrs.get("is_residential") else ("N" if attrs.get("is_residential") is False else "?"),
                "gp_designations": gp_designations,
                "source_url": source_url,
                "notes": attrs.get("notes"),
                "last_verified": last_verified,
            })
    return rows


# ===========================================================================
# Upsert helpers
# ===========================================================================
async def upsert_jurisdictions(session):
    for j in JURISDICTIONS:
        stmt = insert(ParcelJurisdiction).values(**j).on_conflict_do_update(
            index_elements=["state", "city_slug"],
            set_={k: j[k] for k in j if k not in ("state", "city_slug")},
        )
        await session.execute(stmt)
        log.info(f"Upserted jurisdiction: {j['state']}/{j['city_slug']}")


async def upsert_state_laws(session):
    for law in STATE_LAWS:
        stmt = insert(ParcelStateLaw).values(**law).on_conflict_do_update(
            index_elements=["state", "law_id"],
            set_={k: law[k] for k in law if k not in ("state", "law_id")},
        )
        await session.execute(stmt)
        log.info(f"Upserted state law: {law['state']}/{law['law_id']} ({law['name']})")


async def upsert_zone_density(session):
    rows = _load_density_tables()
    for row in rows:
        stmt = insert(ParcelZoneDensity).values(**row).on_conflict_do_update(
            index_elements=["state", "city_slug", "zone_code"],
            set_={k: row[k] for k in row if k not in ("state", "city_slug", "zone_code")},
        )
        await session.execute(stmt)
        log.info(f"Upserted zone density: {row['state']}/{row['city_slug']}/{row['zone_code']}")


async def main(db_host: str | None):
    if db_host:
        os.environ["DATABASE_URL"] = f"postgresql+asyncpg://will@{db_host}:5432/permits"
        from importlib import reload
        from app import database
        reload(database)

    async with async_session_maker() as session:
        await upsert_jurisdictions(session)
        await upsert_state_laws(session)
        await upsert_zone_density(session)
        await session.commit()
        log.info("Seed complete.")

        result = await session.execute(select(ParcelJurisdiction))
        log.info(f"Total jurisdictions: {len(result.scalars().all())}")
        result = await session.execute(select(ParcelStateLaw))
        log.info(f"Total state laws: {len(result.scalars().all())}")
        result = await session.execute(select(ParcelZoneDensity))
        log.info(f"Total zone-density rows: {len(result.scalars().all())}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-host", default=None)
    args = ap.parse_args()
    asyncio.run(main(args.db_host))
