"""Permit-lead classification — source-aware rules mapping a raw permit row to a
normalized `lead_class`.

Phase 3 of the Brazoria TX permit-lead feed turns `hot_leads` source rows into a
deduplicated, classified lead view (`brazoria_permit_leads`). This module is the
single, documented home of the classification rules so the encoding is auditable
and extensible per source (permit-encoding varies wildly by vendor).

Normalized classes
-------------------
    new_construction  — a brand-new structure (or its earliest sub-trade permit:
                        new-build plumbing/electrical/HVAC, NH/NEW HOME BUILD,
                        certificate of occupancy for a new build). A freshly
                        created 911 address point is treated as a
                        new_construction PROXY (the address is assigned before
                        the structure is permitted — the leading indicator).
    addition          — square-footage added to an existing structure
                        (room/slab/garage addition, carport, accessory
                        structure, deck/patio cover, shed, pergola).
    remodel           — work on an existing structure that does NOT add a new
                        structure or footprint (re-roof, re-pipe, HVAC change-out,
                        electrical upgrade, foundation repair, siding repair,
                        interior finish-out).
    other             — non-construction permits that ride the same portal
                        (garage sale, food/health permit, special event, gas
                        test, irrigation, fence, driveway, utility/fiber work).

Design
------
The rules are expressed BOTH as Python (`classify_permit`, used by tests and any
Python-side batch job) AND as a SQL `CASE` expression (`lead_class_sql`, used by
the `brazoria_permit_leads` materialized view) so the same logic runs in both
places. Keep the two in sync — the test-suite asserts parity on a sample corpus.

Source-aware
------------
Rules are keyed first on `source` (so a 911 row is classified by its source, not
its description), then fall through to a generic description/permit_type matcher
for permit sources. Adding a new source with bespoke encoding = add a branch in
`classify_permit` and a matching `WHEN` in `lead_class_sql`.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Source registry — which hot_leads.source values belong to the Brazoria feed,
# and which county each maps to. The MV and any county-scoped query use this so
# a NULL-county source (e.g. mgo_angleton) still resolves to its county WITHOUT
# a full-table scan, and so future Brazoria sources are picked up by adding ONE
# row here (no code change elsewhere).
#
#   source value                 -> (county, is_address_trigger)
# is_address_trigger=True marks 911/NENA new-address proxies (no permit number).
# ---------------------------------------------------------------------------
BRAZORIA_SOURCES: dict[str, tuple[str, bool]] = {
    "mgo_angleton": ("Brazoria", False),
    "brazoria_co_911_addresses": ("Brazoria", True),
    # Future Brazoria jurisdictions land here, one line each:
    # "etrakit_pearland":      ("Brazoria", False),   # Phase 1b
    # "click2gov_lake_jackson":("Brazoria", False),   # Phase 1b
    # "tceq_ossf_brazoria":    ("Brazoria", False),   # Phase 2
}

# Sources whose rows are a NEW-CONSTRUCTION proxy regardless of description
# (911 / NENA new-address points). Derived from BRAZORIA_SOURCES.
ADDRESS_TRIGGER_SOURCES: frozenset[str] = frozenset(
    s for s, (_c, trig) in BRAZORIA_SOURCES.items() if trig
)


def brazoria_sources_sql() -> str:
    """SQL list literal of all Brazoria source values, e.g. ('a','b')."""
    vals = ", ".join("'" + s.replace("'", "''") + "'" for s in BRAZORIA_SOURCES)
    return f"({vals})"


def trigger_sources_sql() -> str:
    """SQL list literal of address-trigger (911) sources, e.g. ('a')."""
    vals = ", ".join(
        "'" + s.replace("'", "''") + "'" for s in sorted(ADDRESS_TRIGGER_SOURCES)
    )
    # Guard against an empty IN () which is a SQL syntax error.
    return f"({vals})" if vals else "('__none__')"


def source_county_sql(source_col: str = "hl.source") -> str:
    """SQL CASE mapping source -> county, for sources with NULL county column."""
    whens = "\n".join(
        f"WHEN {source_col} = '{s}' THEN '{county}'"
        for s, (county, _t) in BRAZORIA_SOURCES.items()
    )
    return f"CASE\n{whens}\nELSE NULL END"


# ---------------------------------------------------------------------------
# Description / permit-type regexes. Ordered by precedence — first match wins.
# Patterns are case-insensitive and matched against
# "<permit_type> <work_class> <description>" joined text.
# ---------------------------------------------------------------------------

# OTHER first — these are non-construction permits that share the portal and
# must NOT be misread as construction (e.g. "GARAGE SALE" contains "garage").
_OTHER_RE = re.compile(
    r"(garage\s*sale|^\s*gs\s*$|health\s*(permit|food)|food\s*(permit|service|"
    r"establishment|vending|renewal)|retail\s*(health|food)|school\s*food|"
    r"tavern|\bbar\b|event|festival|\bmarket\b|crawfish|bbq|"
    r"gas\s*test|\bannual\b|mh\s*renewal|culvert|irrigation|sprinkler|\blawn\b|"
    r"\bfence\b|privacy\s*fence|driveway|parking\s*lot|fiber|\bcable\b|"
    r"certificate\s*for\s*new\s*location|civil\s*work|drainage|"
    r"backflow)",
    re.IGNORECASE,
)

# NEW CONSTRUCTION — a brand-new structure or its earliest sub-trade permit.
_NEW_CONSTRUCTION_RE = re.compile(
    r"(new\s*home\s*build|new\s*construction|new\s*residential|"
    r"\bnew\s*rnc\b|\bnew\s*rwc\b|\brnc\s*install|"
    r"\bnh\b|new\s*home\s*whole\s*system|new\s*home|"
    r"new\s*electrical\s*residential\s*construction|"
    r"in\s*new\s*construction|for\s*new\s*home\s*build|"
    r"certificate\s*of\s*occupancy)",
    re.IGNORECASE,
)

# ADDITION — footprint/structure added to an existing property.
_ADDITION_RE = re.compile(
    r"(\baddition\b|\badd(ing)?\b.*\b(room|bedroom|bath|slab|sqft|sq\s*ft|"
    r"square\s*f)|slab\s*extension|extend\s*driveway|carport|"
    r"accessory\s*structure|portable\s*shed|\bshed\b|pergola|gazebo|"
    r"metal\s*building|office\s*building|new\s*walls|patio\s*cover|"
    r"build\s*on\s*site|concrete\s*slab|pour\s*a\b)",
    re.IGNORECASE,
)

# REMODEL — work on an existing structure, no new footprint.
_REMODEL_RE = re.compile(
    r"(re-?roof|roof\s*replace|full\s*roof|replace.*roof|"
    r"re-?pipe|replace.*(plumbing|sewer|water\s*line|pipe)|"
    r"replace.*(hvac|condenser|condensor|furnace|coil|evaporator)|"
    r"change\s*out|hvac.*(replace|install)|install.*hvac|new\s*hvac|"
    r"foundation|level\s*foundation|repair|remodel|renovat|"
    r"service\s*upgrade|meter\s*loop|service\s*riser|new.*service|"
    r"siding|sheetrock|insulation|interior|finish\s*out|solar|"
    r"generator|electrical\s*service|re-?model|tunnel)",
    re.IGNORECASE,
)


def classify_permit(
    source: str | None,
    permit_type: str | None = None,
    work_class: str | None = None,
    description: str | None = None,
) -> str:
    """Return the normalized lead_class for a permit row.

    Source-aware: a 911/NENA new-address source is always `new_construction`
    (the leading-indicator proxy). Permit sources fall through to the
    description/permit_type matcher.
    """
    if source and source in ADDRESS_TRIGGER_SOURCES:
        return "new_construction"

    blob = " ".join(
        x for x in (permit_type or "", work_class or "", description or "") if x
    ).strip()
    if not blob:
        return "other"

    # Precedence: OTHER (non-construction) is checked first so portal noise
    # (garage sale / food / fence / gas test) never reads as construction.
    if _OTHER_RE.search(blob):
        return "other"
    if _NEW_CONSTRUCTION_RE.search(blob):
        return "new_construction"
    if _ADDITION_RE.search(blob):
        return "addition"
    if _REMODEL_RE.search(blob):
        return "remodel"
    return "other"


# ---------------------------------------------------------------------------
# SQL mirror of the rules above, for use inside the materialized view.
#
# Mirrors classify_permit():
#   1. 911/address-trigger sources -> new_construction
#   2. OTHER regex   -> other
#   3. NEW regex     -> new_construction
#   4. ADDITION regex-> addition
#   5. REMODEL regex -> remodel
#   6. fallback      -> other
#
# Postgres regex (~*) is case-insensitive. Patterns are POSIX ERE equivalents of
# the Python regexes (POSIX ERE lacks \b, so word-boundaries use (^| ) / ( |$)
# or rely on substring matching, acceptable for this coarse classifier).
# ---------------------------------------------------------------------------

_SQL_OTHER = (
    r"garage *sale|^ *gs *$|health *(permit|food)|food *(permit|service|establishment|vending|renewal)|"
    r"retail *(health|food)|school *food|tavern|event|festival|market|crawfish|bbq|"
    r"gas *test|annual|mh *renewal|culvert|irrigation|sprinkler|lawn|fence|driveway|"
    r"parking *lot|fiber| cable|certificate *for *new *location|civil *work|drainage|backflow"
)
_SQL_NEW = (
    r"new *home *build|new *construction|new *residential|new *rnc|new *rwc|rnc *install|"
    r"new *home *whole *system|new *home|new *electrical *residential *construction|"
    r"in *new *construction|for *new *home *build|certificate *of *occupancy|(^| )nh( |$)"
)
_SQL_ADDITION = (
    r"addition|slab *extension|extend *driveway|carport|accessory *structure|"
    r"portable *shed|shed|pergola|gazebo|metal *building|office *building|new *walls|"
    r"patio *cover|build *on *site|concrete *slab|pour *a "
)
_SQL_REMODEL = (
    r"re-?roof|roof *replace|full *roof|replace.*roof|re-?pipe|"
    r"replace.*(plumbing|sewer|water *line|pipe)|replace.*(hvac|condenser|condensor|furnace|coil|evaporator)|"
    r"change *out|hvac.*(replace|install)|install.*hvac|new *hvac|foundation|level *foundation|"
    r"repair|remodel|renovat|service *upgrade|meter *loop|service *riser|new.*service|"
    r"siding|sheetrock|insulation|interior|finish *out|solar|generator|electrical *service|tunnel"
)


def lead_class_sql(*, blob_expr: str, source_col: str = "hl.source") -> str:
    """Return a SQL CASE expression computing lead_class.

    Args:
        blob_expr:  a SQL expression yielding the joined
                    "permit_type work_class description" text to match (~* is
                    case-insensitive, so no need to lower()).
        source_col: column holding the source (e.g. ``hl.source``).
    """
    return f"""
        CASE
            WHEN {source_col} IN {trigger_sources_sql()} THEN 'new_construction'
            WHEN {blob_expr} ~* '{_SQL_OTHER}'    THEN 'other'
            WHEN {blob_expr} ~* '{_SQL_NEW}'      THEN 'new_construction'
            WHEN {blob_expr} ~* '{_SQL_ADDITION}' THEN 'addition'
            WHEN {blob_expr} ~* '{_SQL_REMODEL}'  THEN 'remodel'
            ELSE 'other'
        END
    """
