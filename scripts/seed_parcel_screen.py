"""Seed parcel-screen data: jurisdictions + state laws.

Run once after the parcel-screen tables exist. Idempotent — uses upsert
semantics so re-running is safe.

Usage:
    python3 scripts/seed_parcel_screen.py [--db-host 100.122.216.15]

Origin: Rob's `.claude/skills/parcel-screen/` skill — see his design doc at
/mnt/win11/Fedora/home-offload/Downloads/parcel-screen-design-share-2026-05-12.md
"""

import argparse
import asyncio
import logging
import os
from datetime import date

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.database import async_session_maker
from app.models.parcel_screen import (
    ParcelJurisdiction,
    ParcelStateLaw,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("seed_parcel_screen")


# ---------------------------------------------------------------------------
# Jurisdictions — from Rob's known-jurisdictions.json
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# State laws — SB-684 fully spec'd from Rob's doc; others stubbed pending share
# ---------------------------------------------------------------------------
STATE_LAWS = [
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
    {
        "state": "CA",
        "law_id": "sb9",
        "display_order": 1,
        "name": "SB-9 (Urban lot split + duplex on R-1)",
        "code_section": "Gov Code §65852.21, §66411.7",
        "effective_date": date(2022, 1, 1),
        "leginfo_url": "https://leginfo.legislature.ca.gov/faces/billNavClient.xhtml?bill_id=202120220SB9",
        "summary": "Allows ministerial urban lot split + duplex on each lot in R-1 zones. Up to 4 units total.",
        "eligibility_checklist": [
            {"id": "zone_r1", "label": "Zone is R-1 / single-family residential", "category": "gis"},
            {"id": "urbanized", "label": "Parcel in urbanized area or urban cluster", "category": "gis"},
            {"id": "not_fhsz", "label": "Not in High/Very High Fire Hazard Severity Zone", "category": "gis"},
            {"id": "not_flood", "label": "Not in FEMA Special Flood Hazard Area", "category": "gis"},
            {"id": "not_farmland", "label": "Not on Prime Farmland / Farmland of Statewide Importance", "category": "gis"},
            {"id": "not_fault", "label": "Not in Alquist-Priolo earthquake fault zone", "category": "gis"},
            {"id": "not_historic", "label": "Not historic landmark / not in historic district", "category": "gis"},
            {"id": "min_lot_post_split", "label": "Each resulting lot ≥ 1,200 sqft post-split", "category": "gis"},
            {"id": "no_tenant_displace", "label": "No tenant-occupied housing displaced in past 3 years", "category": "verify"},
            {"id": "no_rent_restrict", "label": "Not subject to deed restriction or affordability covenant", "category": "verify"},
            {"id": "owner_occ_affidavit", "label": "Owner affidavit: will occupy 1 unit for 3 yrs (if lot split)", "category": "verify"},
        ],
        "yield_formula": {
            "expr": "min(4, 2 * 2)",
            "max_units": 4,
            "note": "Urban lot split → 2 lots × duplex each = 4 max. Without lot split → just duplex = 2 max.",
        },
        "caveats_md": "Local cities can add objective design standards but cannot impose discretionary review or block ministerial approval. Verify Rialto's / SantaAna's local SB-9 implementing ordinance.",
        "last_verified": date(2026, 5, 11),
    },
    {
        "state": "CA",
        "law_id": "sb684",
        "display_order": 2,
        "name": "SB-684 (Small-Lot SF Subdivision)",
        "code_section": "Gov Code §66499.40 et seq.",
        "effective_date": date(2024, 7, 1),
        "leginfo_url": "https://leginfo.legislature.ca.gov/faces/billNavClient.xhtml?bill_id=202320240SB684",
        "summary": "Ministerial small-lot subdivision pathway. MF-zoned or qualifying urban infill site up to 5 acres → up to 10 small lots (≥600 sqft each), one SF home per lot.",
        "eligibility_checklist": [
            {"id": "zone_mf_or_infill", "label": "Zone is multi-family residential OR qualifying urban infill site", "category": "gis"},
            {"id": "size_le_5ac", "label": "Parcel ≤ 5 acres", "category": "gis"},
            {"id": "urbanized", "label": "Within urbanized area or urban cluster", "category": "gis"},
            {"id": "not_fhsz", "label": "Not in SRA / VHFHSZ (unless mitigation)", "category": "gis"},
            {"id": "not_flood", "label": "Not in FEMA SFHA (unless mitigation)", "category": "gis"},
            {"id": "not_farmland", "label": "Not Prime/Statewide Importance/Unique Farmland", "category": "gis"},
            {"id": "not_fault", "label": "Not in Alquist-Priolo fault zone", "category": "gis"},
            {"id": "not_hazwaste", "label": "Not on Hazardous Waste Site", "category": "gis"},
            {"id": "not_historic", "label": "Not historic landmark / district", "category": "gis"},
            {"id": "not_coastal", "label": "Not in coastal zone (unless qualifying urbanized portion)", "category": "gis"},
            {"id": "perimeter_urban_75pct", "label": "≥75% of perimeter within ¼ mile is developed urban (for qualifying infill path)", "category": "gis"},
            {"id": "no_demo_10yr", "label": "No housing demo displacing rent-restricted/tenant/Ellis-Act housing in past 10 yrs", "category": "verify"},
            {"id": "no_deed_restrict", "label": "No deed restriction limiting subdivision", "category": "verify"},
            {"id": "local_ordinance", "label": "Local SB-684 implementing ordinance adopted", "category": "verify"},
        ],
        "yield_formula": {
            "expr": "min(10, max_lots_by_min_lot_size)",
            "max_units": 10,
            "min_lot_sqft": 600,
            "max_dwelling_sqft_local_floor": 1750,
            "density_floor_he_sites": 1.0,
            "density_floor_non_he_sites": 0.66,
            "density_alt_floors_du_per_ac": {"metro": 30, "suburban_large": 20, "suburban_small": 15, "rural": 10},
            "note": "10-unit statutory ceiling. GP density is a FLOOR not a cap. Stacks with AB-130 remainder for retained existing structure.",
        },
        "caveats_md": "'Qualifying infill' definition is technical — 75% perimeter test needs careful spatial mapping. For-sale orientation (starter-home intent). Stacks with State ADU on each new lot.",
        "last_verified": date(2026, 5, 11),
    },
    {
        "state": "CA",
        "law_id": "sb1123",
        "display_order": 3,
        "name": "SB-1123 (Extends SB-684 to vacant SFR lots)",
        "code_section": "Gov Code §66499.40 (amendment)",
        "effective_date": date(2025, 7, 1),
        "leginfo_url": "https://leginfo.legislature.ca.gov/faces/billNavClient.xhtml?bill_id=202320240SB1123",
        "summary": "Extends SB-684 small-lot subdivision pathway to VACANT single-family-zoned lots. Same 10-lot ceiling.",
        "eligibility_checklist": [
            {"id": "stub", "label": "Full eligibility checklist not yet loaded — see Rob's sb1123.md", "category": "verify"},
        ],
        "yield_formula": {"max_units": 10, "stub": True, "note": "See SB-684 yield math; same 10-lot ceiling, 600 sqft min."},
        "caveats_md": "STUB — pending Rob's sb1123.md. Treat results as 'possibly eligible, verify against actual statute' until loaded.",
        "last_verified": None,
    },
    {
        "state": "CA",
        "law_id": "sb1211",
        "display_order": 4,
        "name": "SB-1211 (Up to 8 detached ADUs on MF lots)",
        "code_section": "Gov Code §65852.2",
        "effective_date": date(2025, 1, 1),
        "leginfo_url": "https://leginfo.legislature.ca.gov/faces/billNavClient.xhtml?bill_id=202320240SB1211",
        "summary": "Raises detached-ADU cap on multi-family lots from 2 to 8.",
        "eligibility_checklist": [
            {"id": "stub", "label": "Full eligibility checklist not yet loaded — see Rob's sb1211.md", "category": "verify"},
        ],
        "yield_formula": {"max_added_units": 8, "stub": True, "note": "Existing units + up to 8 detached ADUs."},
        "caveats_md": "STUB — pending Rob's sb1211.md.",
        "last_verified": None,
    },
    {
        "state": "CA",
        "law_id": "state-adu",
        "display_order": 5,
        "name": "State ADU/JADU law",
        "code_section": "Gov Code §65852.2 / §65852.22",
        "summary": "On single-family lots: +1 ADU + 1 JADU. On multi-family lots: per-unit ADUs allowed.",
        "eligibility_checklist": [
            {"id": "stub", "label": "Full eligibility checklist not yet loaded — see Rob's state-adu.md", "category": "verify"},
        ],
        "yield_formula": {"sf_add": "+1 ADU + 1 JADU", "mf_add": "per-unit ADUs", "stub": True},
        "caveats_md": "STUB — pending Rob's state-adu.md.",
        "last_verified": None,
    },
    {
        "state": "CA",
        "law_id": "ab2011-sb6",
        "display_order": 6,
        "name": "AB-2011 / SB-6 (Housing on commercial corridors)",
        "code_section": "Gov Code §65912.100 et seq. / §65913.5",
        "summary": "Ministerial housing on qualifying commercial-zoned corridor parcels. Density floors by tier (15-30+ du/ac).",
        "eligibility_checklist": [
            {"id": "stub", "label": "Full eligibility checklist not yet loaded — see Rob's ab2011-sb6.md", "category": "verify"},
        ],
        "yield_formula": {"stub": True, "note": "Density depends on corridor classification (urban/suburban) and ROW width."},
        "caveats_md": "STUB — pending Rob's ab2011-sb6.md. Requires corridor ROW width verification.",
        "last_verified": None,
    },
    {
        "state": "CA",
        "law_id": "density-bonus",
        "display_order": 7,
        "name": "Density Bonus Law",
        "code_section": "Gov Code §65915",
        "summary": "Up to 100% bonus (AB-1287 stacking) on top of base if affordable units committed.",
        "eligibility_checklist": [
            {"id": "stub", "label": "Full eligibility checklist not yet loaded — see Rob's density-bonus.md", "category": "verify"},
        ],
        "yield_formula": {"bonus_pct_range": [35, 100], "stub": True, "note": "base_units × (1 + bonus%)"},
        "caveats_md": "STUB — pending Rob's density-bonus.md.",
        "last_verified": None,
    },
    {
        "state": "CA",
        "law_id": "ab130",
        "display_order": 8,
        "name": "AB-130 (Remainder parcel modifier)",
        "code_section": "Gov Code §66411.7 (amendment)",
        "summary": "Modifier on SB-684/1123: existing improvement preserved on remainder parcel, not counted toward 10-unit limit.",
        "eligibility_checklist": [
            {"id": "stub", "label": "Full eligibility checklist not yet loaded — see Rob's ab130.md", "category": "verify"},
        ],
        "yield_formula": {"add": "+ retained existing structure", "stub": True},
        "caveats_md": "STUB — pending Rob's ab130.md. Best play when valuable improvement exists on a subdividable parcel.",
        "last_verified": None,
    },
]


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


async def main(db_host: str | None):
    if db_host:
        os.environ["DATABASE_URL"] = f"postgresql+asyncpg://will@{db_host}:5432/permits"
        # reload the engine
        from importlib import reload
        from app import database
        reload(database)

    async with async_session_maker() as session:
        await upsert_jurisdictions(session)
        await upsert_state_laws(session)
        await session.commit()
        log.info("Seed complete.")

        # Sanity print
        result = await session.execute(select(ParcelJurisdiction))
        log.info(f"Total jurisdictions: {len(result.scalars().all())}")
        result = await session.execute(select(ParcelStateLaw))
        log.info(f"Total state laws: {len(result.scalars().all())}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-host", default=None)
    args = ap.parse_args()
    asyncio.run(main(args.db_host))
