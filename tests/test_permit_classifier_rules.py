"""Regression tests for the rules_v2 pre-classification layer.

Cases are drawn from eval-set evidence (evals/permit_classifier/): each one
either reproduces a labeled eval row or pins a guard that eval misses showed
to be mandatory. pre_classify returning None means "defer to the LLM".
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from permit_classifier_lib import pre_classify  # noqa: E402

CASES = [
    # Trade types: long descriptions stay in the trade unless a specific
    # scope or another trade is named (rules_v2 R1i).
    ("Electrical Permit / New", "x" * 200, "electrical"),
    ("Electrical Permit / New", "wire new pool equipment and spa " + "x" * 60, None),
    ("Plumbing Permit / Remodel", "Mechanical Changeout", None),
    ("Plumbing Backflow / Annual", "backflow test", "plumbing"),
    # Generic department buckets: empty description -> other_unknown (T1).
    ("Building Inspections and Permits", "", "other_unknown"),
    ("Building Inspections and Permits", "some actual work", None),
    # Direct type maps with the scope-conflict guard (T2).
    ("Re-Roof Permit", "remove and replace shingles", "roofing"),
    ("Re-Roof Permit", "install solar panels", None),
    ("Residential Accessory Struct / New", "NEW SWIMMING POOL", None),
    ("Residential Accessory Struct / New", "new detached workshop", "accessory_structure"),
    ("Garage Sale", "", "event_temporary"),
    ("Contractor Registration", "", "admin_licensing"),
    ("Plumbing Irrigation Permit / Existing", "", "irrigation"),
    # Residential/commercial building types (T3).
    ("Residential Building Permit / New", "NEW SFR", "residential_new"),
    ("Residential Building Permit / Remodel", "Install 22 Roof mounted solar panels", None),
    ("Commercial Building Permit / New",
     "Construct new residential cottage in multifamily community", None),
    ("Commercial Building Permit / New", "X TEAM /// Reserve Capital Partners", "commercial_new"),
    ("Building (BU) Single Family  Alteration / SINGLE FAMILY", "INTERIOR REMODEL",
     "residential_remodel"),
    ("Building (BU) Single Family  New Construction / TWO FAMILY D", "NEW DUPLEX",
     "residential_new"),
    ("Building (BU) Commercial  Renovation / OFFICE BUILDING", "INTERIOR REMODEL ONLY",
     "commercial_remodel_ti"),
    # Building-permit demolition subtype (T5).
    ("Building Permit / Demolition", "", "demolition"),
    ("Building Permit / Demolition", "Demolish Ext Carport & Front Porch", "demolition"),
    # Empty type (T4).
    ("", "", "other_unknown"),
    ("", "Foundation Repair", None),
    # Generic "Building Permit" stays with the LLM (res/com is undecidable).
    ("Building Permit / Remodel", "kitchen remodel", None),
    # rules_v1 behaviors that must not regress.
    ("Driveway / Sidewalks", "new approach", "driveway_flatwork"),
    ("Electrical Permit / Billboard", "Pole Sign For Ace Hardware", "sign"),
    ("Plumbing Permit / Cut Over/Tank Abandonment", "City sewer cut over.", "septic_ossf"),
    ("Plumbing / Standalone", "", "plumbing"),
]


@pytest.mark.parametrize("permit_type,desc,expected", CASES)
def test_pre_classify(permit_type, desc, expected):
    got = pre_classify({"permit_type": permit_type, "description_raw": desc})
    assert got == expected
