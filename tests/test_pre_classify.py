"""Unit tests for pre_classify() - the deterministic rules layer.

Test cases are drawn directly from the misclassification lines in
evals/permit_classifier/scores_35b_v2.md and scores_35b_v3.md.
Each case uses the eval-set label as the expected result.

Run: python3 -m pytest tests/test_pre_classify.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from permit_classifier_lib import pre_classify  # noqa: E402


def _row(permit_type: str, description_raw: str = "") -> dict:
    return {"permit_type": permit_type, "description_raw": description_raw}


# ---------------------------------------------------------------------------
# Sign exceptions (v2 confusions: sign -> electrical)
# ---------------------------------------------------------------------------

class TestSignExceptions:
    def test_billboard_subtype_pole_sign_desc(self):
        """v2: [Electrical Permit / Billboard] Pole Sign For Ace Hardware"""
        assert pre_classify(_row("Electrical Permit / Billboard", "Pole Sign For Ace Hardware")) == "sign"

    def test_projecting_subtype(self):
        """v2: [Electrical Permit / Projecting] Projecting sign Athenia Bar & Grill"""
        assert pre_classify(_row("Electrical Permit / Projecting", "Projecting sign Athenia Bar & Grill")) == "sign"

    def test_berm_sign_in_desc(self):
        """v2: [Electrical Permit] Berm Sign For One Lacosta"""
        assert pre_classify(_row("Electrical Permit", "Berm Sign For One Lacosta")) == "sign"

    def test_building_sign_for_pattern(self):
        """v2: [Electrical Permit / Billboard] Building Sign For Wholefoods Market"""
        assert pre_classify(_row("Electrical Permit / Billboard", "Building Sign For Wholefoods Market")) == "sign"

    def test_sign_with_demolition_subtype_still_sign(self):
        """v3: [Electrical Permit / Demolition] Wall Sign Ibc Bank (North Elevation) - sign beats demo subtype"""
        assert pre_classify(_row("Electrical Permit / Demolition", "Wall Sign Ibc Bank (North Elevation)")) == "sign"

    def test_change_out_subtype_pole_sign(self):
        """v2: [Electrical Permit / Change Out] Pole Sign For Daybridge Learning Center"""
        assert pre_classify(_row("Electrical Permit / Change Out", "Pole Sign For Daybridge Learning Center")) == "sign"

    def test_reface_pole_sign(self):
        """v2: [Electrical Permit / Billboard] Reface To Existing To Pole Sign Bright Banc"""
        assert pre_classify(_row("Electrical Permit / Billboard", "Reface To Existing To Pole Sign Bright Banc")) == "sign"

    def test_reader_board_in_desc(self):
        """v2: [Electrical Permit] Pole Sign For Reader Board"""
        assert pre_classify(_row("Electrical Permit", "Pole Sign For Reader Board")) == "sign"

    def test_roof_sign_replace(self):
        """v2: [Electrical Permit / Roof] Remove existing Cricket roof sign and install new Cricket roof sign"""
        assert pre_classify(_row(
            "Electrical Permit / Roof",
            "Remove existing Cricket roof sign and install new Cricket roof sign in the same location."
        )) == "sign"

    def test_awning_sign(self):
        """eval: [Electrical Permit / Awning] Awning sign Royal Blue Grocery -> sign"""
        assert pre_classify(_row("Electrical Permit / Awning", "Awning sign Royal Blue Grocery")) == "sign"

    def test_roof_sign_short(self):
        """eval: [Electrical Permit / Roof] Roof Sign* -> sign"""
        assert pre_classify(_row("Electrical Permit / Roof", "Roof Sign*")) == "sign"


# ---------------------------------------------------------------------------
# Demolition exceptions (v2 confusions: demolition -> plumbing/mechanical/electrical)
# ---------------------------------------------------------------------------

class TestDemolitionExceptions:
    def test_plumbing_demo_subtype_demolish_garage(self):
        """v2: [Plumbing Permit / Demolition] Demolish Garage -- Residential"""
        assert pre_classify(_row("Plumbing Permit / Demolition", "Demolish Garage -- Residential")) == "demolition"

    def test_mechanical_demo_subtype_demolition_residence(self):
        """v2: [Mechanical Permit / Demolition] Demolition Residence(Rear Residence)"""
        assert pre_classify(_row("Mechanical Permit / Demolition", "Demolition Residence(Rear Residence)")) == "demolition"

    def test_mechanical_demo_subtype_demolish_comm_bldg(self):
        """v2: [Mechanical Permit / Demolition] Demolish Comm Bldg. To Min Stds.(45 Days)"""
        assert pre_classify(_row("Mechanical Permit / Demolition", "Demolish Comm Bldg. To Min Stds.(45 Days)")) == "demolition"

    def test_plumbing_demo_subtype_demolish_res(self):
        """v2: [Plumbing Permit / Demolition] Demolish Res"""
        assert pre_classify(_row("Plumbing Permit / Demolition", "Demolish Res")) == "demolition"

    def test_plumbing_demo_subtype_demolish_existing_building(self):
        """v2: [Plumbing Permit / Demolition] Demolish Existing Building"""
        assert pre_classify(_row("Plumbing Permit / Demolition", "Demolish Existing Building")) == "demolition"

    def test_electrical_demo_subtype_demolish_garage(self):
        """v2: [Electrical Permit / Demolition] Demolish One Story Garage"""
        assert pre_classify(_row("Electrical Permit / Demolition", "Demolish One Story Garage")) == "demolition"

    def test_electrical_demo_min_sta_carport(self):
        """v2: [Electrical Permit / Demolition] Demolish Min Sta Carport"""
        assert pre_classify(_row("Electrical Permit / Demolition", "Demolish Min Sta Carport")) == "demolition"

    def test_interior_demo_nonstruc_remodel_stays_trade(self):
        """v3: [Plumbing Permit / Interior Demo Non-Structural] Remodel/Repair Portable Classroom stays plumbing"""
        assert pre_classify(_row(
            "Plumbing Permit / Interior Demo Non-Structural",
            "Remodel/Repair Portable Classroom & Relocate"
        )) == "plumbing"

    def test_demo_subtype_interior_partitions_defers_to_llm(self):
        """v2: [Mechanical Permit / Demolition] Demolition Of Interior Partitions - defer to LLM (conservative)"""
        # Label is demolition but this is ambiguous; safer to let LLM decide
        assert pre_classify(_row(
            "Mechanical Permit / Demolition",
            "Demolition Of Interior Partitions Plumbing***"
        )) is None


# ---------------------------------------------------------------------------
# Trade stay (v3 confusions: electrical/plumbing/mechanical -> other_unknown)
# ---------------------------------------------------------------------------

class TestTradeStay:
    def test_electrical_standalone_empty_desc(self):
        """v3: [Electrical / Standalone] empty description -> electrical"""
        assert pre_classify(_row("Electrical / Standalone", "")) == "electrical"

    def test_plumbing_standalone_empty_desc(self):
        """v3: [Plumbing / Standalone] empty description -> plumbing"""
        assert pre_classify(_row("Plumbing / Standalone", "")) == "plumbing"

    def test_mechanical_umbrella_empty_desc(self):
        """v3: [Mechanical / Umbrella] empty description -> mechanical_hvac"""
        assert pre_classify(_row("Mechanical / Umbrella", "")) == "mechanical_hvac"

    def test_mechanical_addition_short_desc(self):
        """v3: [Mechanical Permit / Addition] One Story Addition To Extend Living Room -> mechanical_hvac"""
        assert pre_classify(_row("Mechanical Permit / Addition", "One Story Addition To Extend Living Room")) == "mechanical_hvac"

    def test_electrical_remodel_short_desc(self):
        """v3: [Electrical Permit / Remodel] Remodel -> electrical"""
        assert pre_classify(_row("Electrical Permit / Remodel", "Remodel")) == "electrical"

    def test_plumbing_mechanical_changeout_defers(self):
        """v2/v3: [Plumbing Permit / Remodel] Mechanical Changeout - cross-trade, defer to LLM"""
        # Label is mechanical_hvac despite plumbing permit type; we defer
        assert pre_classify(_row("Plumbing Permit / Remodel", "Mechanical Changeout (Ecsd)")) is None


# ---------------------------------------------------------------------------
# Irrigation exceptions (v2 confusions: irrigation -> plumbing)
# ---------------------------------------------------------------------------

class TestIrrigationExceptions:
    def test_plumbing_remodel_install_irrigation(self):
        """v2: [Plumbing Permit / Remodel] Install Irrigation System Only"""
        assert pre_classify(_row("Plumbing Permit / Remodel", "Install Irrigation System Only")) == "irrigation"

    def test_plumbing_remodel_irrigation_system_residential(self):
        """v2: [Plumbing Permit / Remodel] Irrigation System Residential"""
        assert pre_classify(_row("Plumbing Permit / Remodel", "Irrigation System Residential")) == "irrigation"

    def test_plumbing_irrigation_permit_type(self):
        """eval: [Plumbing Irrigation Permit / Existing] -> irrigation (irrigation in permit_type)"""
        assert pre_classify(_row("Plumbing Irrigation Permit / Existing", "Building No: 2; Unit No: 208")) == "irrigation"


# ---------------------------------------------------------------------------
# Solar exception (v2 confusion: solar -> electrical)
# ---------------------------------------------------------------------------

class TestSolarException:
    def test_electrical_auxiliary_solar_system(self):
        """v2: [Electrical Permit / Auxiliary Power] Install new elec solar system"""
        assert pre_classify(_row("Electrical Permit / Auxiliary Power", "Install new elec solar system to exist res only.")) == "solar"


# ---------------------------------------------------------------------------
# Flatwork (Driveway/Sidewalk/Paving)
# ---------------------------------------------------------------------------

class TestFlatwork:
    def test_flatwork_stays_with_remodel_desc(self):
        """v2: [Driveway / Sidewalks / Modification] Remodel For Admin & Professional Offices -> driveway_flatwork"""
        assert pre_classify(_row("Driveway / Sidewalks / Modification", "Remodel For Admin & Professional Offices")) == "driveway_flatwork"

    def test_flatwork_demo_with_replace_stays_flatwork(self):
        """eval: [Driveway / Sidewalks / Demo] Demo and replace... -> driveway_flatwork, not demolition"""
        assert pre_classify(_row("Driveway / Sidewalks / Demo", "Demo and replace 200 L.F. existing commercial sidewalk")) == "driveway_flatwork"

    def test_flatwork_demo_addn_stays_flatwork(self):
        """eval: [Driveway / Sidewalks / Demo] Demo & Addn To Exist Res -> driveway_flatwork"""
        assert pre_classify(_row("Driveway / Sidewalks / Demo", "Demo & Addn To Exist Res To Create New Gar**")) == "driveway_flatwork"

    def test_flatwork_demo_only_garage_becomes_demolition(self):
        """v2: [Driveway / Sidewalks / Demo] Demo Existing Garage Only -> demolition"""
        assert pre_classify(_row("Driveway / Sidewalks / Demo", "Demo Existing Garage Only")) == "demolition"


# ---------------------------------------------------------------------------
# Septic/Cutover
# ---------------------------------------------------------------------------

class TestSepticCutover:
    def test_plumbing_cutover_tank_abandonment(self):
        """standard: [Plumbing Permit / Cut Over/Tank Abandonment] -> septic_ossf"""
        assert pre_classify(_row("Plumbing Permit / Cut Over/Tank Abandonment", "City sewer cut over to residence only.")) == "septic_ossf"


# ---------------------------------------------------------------------------
# Non-trade/non-flatwork returns None
# ---------------------------------------------------------------------------

class TestNonTradeReturnsNone:
    def test_building_permit_returns_none(self):
        assert pre_classify(_row("Building Permit / Remodel", "New Storage Shed")) is None

    def test_generic_permit_returns_none(self):
        assert pre_classify(_row("Building Inspections and Permits", "")) is None
