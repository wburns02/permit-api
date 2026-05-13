"""Parcel-screen service: GIS REST pull, state-law eligibility, yield math.

Origin: Rob's `.claude/skills/parcel-screen/` SKILL.md — productized.

Phase 1 scope:
- Pull parcel facts from a cached city's GIS REST endpoints (Esri FeatureServer).
- Run eligibility checks for CA state laws against the parcel facts.
- Compute yield per program.
- Cross-reference with our permits DB to surface past permits for the APN.

Out of scope for Phase 1 (TODO Phase 2):
- Auto-discovery of GIS endpoints for new cities (Rob's Chrome MCP trick — needs server-side headless browser).
- Spatial 75% perimeter urban-use test (SB-684 qualifying-infill).
- Statewide exclusion overlays (CalFire FHSZ, Alquist-Priolo, FMMP, OHP) — overlays are flagged as [VERIFY] for now.
- Mercator dimension correction (parcel `acres` from GIS attributes is used directly).
"""

import logging
import math
from typing import Any

import httpx
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.parcel_screen import (
    ParcelJurisdiction,
    ParcelStateLaw,
    ParcelZoneDensity,
)
from app.services.parcel_overlays import query_all_overlays

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Esri REST helpers
# ---------------------------------------------------------------------------
async def _query_feature_layer(
    layer_url: str,
    where: str,
    out_fields: str = "*",
    return_geometry: bool = True,
    geometry: dict | None = None,
    out_sr: int | None = None,
    timeout: float = 8.0,
) -> dict:
    """Run an Esri FeatureServer/MapServer query and return parsed JSON.

    `out_sr` requests the geometry in a specific spatial reference (e.g. 4326
    for WGS84 lat/lng). When set, the Esri server reprojects the geometry
    server-side — no client-side projection math needed.
    """
    params = {
        "where": where,
        "outFields": out_fields,
        "returnGeometry": "true" if return_geometry else "false",
        "f": "json",
    }
    if out_sr is not None:
        params["outSR"] = str(out_sr)
    if geometry:
        params["geometry"] = geometry["geom"]
        params["geometryType"] = geometry.get("type", "esriGeometryEnvelope")
        params["spatialRel"] = geometry.get("rel", "esriSpatialRelIntersects")
        params["inSR"] = str(geometry.get("inSR", 4326))

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.get(f"{layer_url}/query", params=params)
        r.raise_for_status()
        return r.json()


async def pull_parcel_facts(
    jurisdiction: ParcelJurisdiction,
    address: str | None,
    apn: str | None,
) -> dict:
    """Pull parcel attributes from the city's GIS REST layers.

    Returns a normalized dict regardless of source schema. Caller deals with
    None values via the per-law eligibility engine.
    """
    parcels_url = jurisdiction.parcels_url
    if not parcels_url:
        raise ValueError(f"jurisdiction {jurisdiction.state}/{jurisdiction.city_slug} has no parcels_url cached")

    apn_field = jurisdiction.apn_field or "APN"

    # Build WHERE clause
    if apn:
        # Strip non-alphanumeric in case of formatting variance (e.g., "013-111-119" → "013111119")
        apn_clean = "".join(c for c in apn if c.isalnum())
        where = f"{apn_field} = '{apn_clean}' OR {apn_field} = '{apn}'"
    elif address and jurisdiction.address_field:
        # Use address LIKE — split on first space to grab number + street
        parts = address.strip().split(None, 1)
        if len(parts) == 2:
            street_num, rest = parts
            where = f"{jurisdiction.address_field} LIKE '%{street_num}%{rest.split(',')[0].strip()}%'"
        else:
            where = f"{jurisdiction.address_field} LIKE '%{address}%'"
    elif address:
        # Try a generic ADDRESS / SITE_ADDR / ADDR field on the parcel layer
        where = f"ADDRESS LIKE '%{address}%' OR SITE_ADDR LIKE '%{address}%' OR Address LIKE '%{address}%'"
    else:
        raise ValueError("must provide address or apn")

    # Ask the Esri server to reproject geometry to WGS84 lat/lng so the
    # frontend can render the polygon on a standard slippy-tile map without
    # client-side projection math.
    resp = await _query_feature_layer(parcels_url, where=where, out_sr=4326)
    features = resp.get("features", [])
    if not features:
        return {"error": "parcel not found", "where": where}

    feat = features[0]
    attrs = feat.get("attributes", {})
    geom = feat.get("geometry")
    sr = resp.get("spatialReference", {})

    # Build WGS84 outputs the frontend uses to render the parcel on a map.
    geometry_wgs84 = None
    centroid_lat = None
    centroid_lng = None
    if geom and geom.get("rings"):
        # Esri Polygon "rings" are [[ [lon,lat], [lon,lat], ... ], ...]
        # in 4326 since we passed outSR=4326. Same shape as GeoJSON
        # Polygon coordinates — frontend can pass straight to maplibre.
        geometry_wgs84 = {"type": "Polygon", "coordinates": geom["rings"]}
        # Centroid: simple ring-average of the outer ring. Good enough for
        # map.flyTo / fitBounds anchoring.
        outer = geom["rings"][0] if geom["rings"] else []
        if outer:
            centroid_lng = sum(p[0] for p in outer) / len(outer)
            centroid_lat = sum(p[1] for p in outer) / len(outer)

    # Normalize attribute keys to a stable lower-case shape
    norm = {k.lower(): v for k, v in attrs.items()}

    # Pull common fields with fallbacks across schemas
    parcel_acres = (
        norm.get("acres")
        or norm.get("acreage")
        or norm.get("acres_calc")
        or (norm.get("shape__area") / 43560 if norm.get("shape__area") else None)
    )

    # `parcel_acres` is the raw acreage if we found one above; fall back to
    # the SB-County `Acreage` field and the Riverside `ACREAGE` field (both
    # land in `norm["acreage"]` after lowercasing).
    if parcel_acres is None:
        parcel_acres = norm.get("acreage")

    # Some jurisdictions (SB County, Riverside County) store the
    # improvement value as a string ("12345.00") rather than a number — coerce.
    def _to_float(v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        try:
            return float(str(v).replace(",", "").strip())
        except (ValueError, TypeError):
            return None

    facts = {
        "raw_attributes": attrs,
        "geometry": geom,
        "geometry_wgs84": geometry_wgs84,
        "lat": centroid_lat,
        "lng": centroid_lng,
        "spatial_reference": sr,
        "apn": (
            norm.get(apn_field.lower())
            or norm.get("apn")
            or norm.get("parcelno")
            or norm.get("parcelnumber")  # SB County
            or apn
        ),
        "owner_name": norm.get("owner_name") or norm.get("owner") or norm.get("ownername"),
        "owner_addr": (
            norm.get("owner_addr")
            or norm.get("owner_address")
            or norm.get("mailing_address")
            or norm.get("mail_street")  # Riverside CREST
        ),
        "address": (
            norm.get("site_address")
            or norm.get("address")
            or norm.get("situs")
            or norm.get("situs_street")  # Riverside CREST
        ),
        "acres": _to_float(parcel_acres),
        # SB County's parcels layer stores the *city name* in the `Zoning`
        # field (e.g. "CITY OF FONTANA"), which isn't a real zone code. The
        # statewide-zoning spatial join below will fill in the real value.
        # Reject obvious placeholders here so the join actually fires.
        "zone_code": (
            norm.get("zone_code")
            or norm.get("zone")
            or norm.get("zonecode")
            or (None if (norm.get("zoning") or "").upper().startswith("CITY OF") else norm.get("zoning"))
        ),
        "zone_desc": (
            norm.get("zone_desc")
            or norm.get("zonedesc")
            or (None if (norm.get("zoning") or "").upper().startswith("CITY OF") else norm.get("zoning"))
        ),
        "gp_code": norm.get("gp_code") or norm.get("genplan") or norm.get("gp_general"),
        "gp_desc": norm.get("gp_desc") or norm.get("gpdesc") or norm.get("gp_generaldesc"),
        "sp_code": norm.get("sp_code") or norm.get("specific_plan"),
        "sp_desc": norm.get("sp_desc") or norm.get("specific_plan_desc"),
        "fire_zone": norm.get("fire_zonre") or norm.get("fire_zone") or norm.get("fhsz"),
        "year_built": norm.get("year_built") or norm.get("yearbuilt"),
        "land_value": _to_float(
            norm.get("land_value") or norm.get("landvalue") or norm.get("land")
        ),
        # SB County uses "ImprovementValue" (→ improvementvalue); Riverside CREST
        # uses "STRUCTURES" (→ structures); Rialto carries an "impr_value" alias.
        "impr_value": _to_float(
            norm.get("impr_value")
            or norm.get("imprvalue")
            or norm.get("improvement_value")
            or norm.get("improvementvalue")
            or norm.get("structures")
        ),
        "tax_status": norm.get("tax_status") or norm.get("taxstatus"),
    }

    # If parcel layer is thin (e.g., Santa Ana via OC), the zone/GP fields will be None.
    # Spatially query the zoning + GP layers to fill them in.
    if not facts["zone_code"] and jurisdiction.zoning_url and geom:
        try:
            zoning_attrs = await _spatial_join_first(jurisdiction.zoning_url, geom, sr)
            if zoning_attrs:
                zn = {k.lower(): v for k, v in zoning_attrs.items()}
                # Includes the CA Statewide Zoning South layer fields (`code`,
                # `description`, `jurisdiction`) used by our IE cities.
                facts["zone_code"] = (
                    zn.get("zoneclass")
                    or zn.get("zone_code")
                    or zn.get("zone")
                    or zn.get("code")
                )
                facts["zone_desc"] = (
                    zn.get("zonedesc")
                    or zn.get("zone_desc")
                    or zn.get("description")
                )
        except Exception as e:
            logger.warning(f"zoning spatial join failed: {e}")

    if not facts["gp_code"] and jurisdiction.general_plan_url and geom:
        try:
            gp_attrs = await _spatial_join_first(jurisdiction.general_plan_url, geom, sr)
            if gp_attrs:
                gn = {k.lower(): v for k, v in gp_attrs.items()}
                facts["gp_code"] = gn.get("gp_general") or gn.get("gp_code") or gn.get("genplan")
                facts["gp_desc"] = gn.get("gp_generaldesc") or gn.get("gp_desc")
                facts["gp_density_intensity"] = gn.get("gp_dens_intens")
        except Exception as e:
            logger.warning(f"GP spatial join failed: {e}")

    return facts


async def _spatial_join_first(layer_url: str, parcel_geom: dict, parcel_sr: dict) -> dict | None:
    """Spatial-query a layer at the parcel centroid, return first hit's attributes."""
    # Use parcel centroid as a point query — simpler + faster than envelope intersect
    rings = parcel_geom.get("rings") or []
    if not rings or not rings[0]:
        return None
    pts = rings[0]
    cx = sum(p[0] for p in pts) / len(pts)
    cy = sum(p[1] for p in pts) / len(pts)

    wkid = parcel_sr.get("wkid") or parcel_sr.get("latestWkid") or 4326
    resp = await _query_feature_layer(
        layer_url,
        where="1=1",
        return_geometry=False,
        geometry={
            "geom": f"{cx},{cy}",
            "type": "esriGeometryPoint",
            "rel": "esriSpatialRelIntersects",
            "inSR": wkid,
        },
    )
    features = resp.get("features", [])
    return features[0]["attributes"] if features else None


# ---------------------------------------------------------------------------
# Eligibility engine
# ---------------------------------------------------------------------------
def _check_zone_residential(zone_code: str | None) -> bool:
    """SFR zones only."""
    if not zone_code:
        return False
    z = zone_code.upper().strip()
    sfr_patterns = ("R-1", "R1", "SFR", "RS", "R-S", "SF")
    return any(z.startswith(p) for p in sfr_patterns)


def _check_zone_mf(zone_code: str | None) -> bool:
    """Multi-family zones."""
    if not zone_code:
        return False
    z = zone_code.upper().strip()
    mf_patterns = ("R-2", "R2", "R-3", "R3", "R-4", "R4", "MF", "RM", "RH", "MU")
    return any(z.startswith(p) for p in mf_patterns)


def _check_zone_any_residential(zone_code: str | None) -> bool:
    """Any residential — SFR or MF — for State ADU eligibility."""
    return _check_zone_residential(zone_code) or _check_zone_mf(zone_code)


def _check_zone_commercial(zone_code: str | None) -> bool:
    """Commercial / office / retail / parking — for AB-2011 / SB-6 eligibility.

    Conservative: only matches obvious commercial prefixes. Mixed-use that's
    primarily residential won't match (correct — AB-2011 wants non-residential
    by-right zones).
    """
    if not zone_code:
        return False
    z = zone_code.upper().strip()
    commercial_patterns = ("C-1", "C1", "C-2", "C2", "C-3", "C3", "C-G", "CG", "CC", "CR", "CO", "CN", "O-", "OFC", "OP", "P-", "PKG")
    return any(z.startswith(p) for p in commercial_patterns) or z in ("C", "P", "O")


def _evaluate_eligibility(law: ParcelStateLaw, facts: dict) -> dict:
    """Run a law's auto-checkable eligibility items against parcel facts.

    Returns:
      {
        "auto_checks": [{id, label, status: "pass"|"fail"|"unknown", reason}],
        "verify_items": [{id, label}],
        "auto_eligible": bool,  # True only if ALL gis checks pass AND there's at least one fact-backed pass
        "verify_pending": int,  # count of [VERIFY] items
      }

    Eligibility logic:
    - 'fail' on any auto-check → not auto-eligible (the law definitely doesn't apply)
    - 'unknown' is recorded as a verify item but doesn't fail by itself
    - Auto-eligible requires (no fails) AND (at least one actual 'pass' from a fact-backed
      check, not just 'auto_pass' items) so a law with NO meaningful auto-checks doesn't
      light up green by default.
    """
    auto = []
    verify = []
    has_fail = False
    has_fact_pass = False

    checklist = law.eligibility_checklist or []
    for item in checklist:
        if item.get("category") == "verify":
            verify.append({"id": item["id"], "label": item["label"]})
            continue

        # Auto-check item — apply per law_id logic
        status, reason = _run_auto_check(law.law_id, item, facts)
        auto.append({"id": item["id"], "label": item["label"], "status": status, "reason": reason})
        if status == "fail":
            has_fail = True
        elif status == "pass" and not item.get("auto_pass"):
            has_fact_pass = True
        elif status == "unknown":
            verify.append({"id": item["id"], "label": item["label"]})

    # Special case: by-right's only check is auto_pass — let it remain eligible
    is_by_right_style = len([i for i in checklist if i.get("category") == "gis"]) == 1 and any(i.get("auto_pass") for i in checklist)
    auto_eligible = (not has_fail) and (has_fact_pass or is_by_right_style)

    return {
        "auto_checks": auto,
        "verify_items": verify,
        "auto_eligible": auto_eligible,
        "verify_pending": len(verify),
    }


def _run_auto_check(law_id: str, item: dict, facts: dict) -> tuple[str, str | None]:
    """Run a single auto-check. Returns (status, reason)."""
    item_id = item.get("id")

    # Universal auto-pass for the by-right law's "always" item
    if item.get("auto_pass"):
        return "pass", "Always applies"

    zone = facts.get("zone_code")
    acres = facts.get("acres")
    impr_value = facts.get("impr_value")

    # Universal items shared across multiple laws ----------------------------
    if item_id == "zone_r1" or item_id == "zone_sfr":
        return ("pass" if _check_zone_residential(zone) else "fail", f"zone={zone}")
    if item_id == "zone_mf":
        return ("pass" if _check_zone_mf(zone) else "fail", f"zone={zone}")
    if item_id == "zone_residential":
        # Prefer the density-table explicit flag when we have it (handles cases
        # like Rialto A-1 where the zone permits SF dwellings even though the
        # code letter doesn't match the SFR/MF prefix patterns).
        flag = facts.get("zone_is_residential_flag")
        if flag is True:
            return "pass", f"zone={zone} is residential per density table"
        if _check_zone_any_residential(zone):
            return "pass", f"zone={zone}"
        if flag is False:
            return "fail", f"zone={zone} flagged non-residential in density table"
        return "fail", f"zone={zone}"
    if item_id == "zone_commercial":
        return ("pass" if _check_zone_commercial(zone) else "fail", f"zone={zone}")
    if item_id == "size_le_5ac":
        if acres is None:
            return "unknown", "lot size not on GIS"
        return ("pass" if acres <= 5.0 else "fail", f"{acres:.2f} acres")
    if item_id == "sufficient_lot_area":
        if acres is None:
            return "unknown", "lot size not on GIS"
        # 10 lots × 600 sqft min + ~25 % for streets/setbacks + room for remainder ≈ 0.2 ac minimum
        return ("pass" if acres >= 0.2 else "fail", f"{acres:.2f} acres (need ≥0.2 ac for 10 lots + remainder)")

    # SB-9 specific ---------------------------------------------------------
    if law_id == "sb9" and item_id == "min_lot_post_split":
        if not acres:
            return "unknown", "lot size not on GIS"
        sqft = acres * 43560
        return ("pass" if sqft >= 2400 else "fail", f"{sqft:.0f} sqft (need ≥2400 for 2 lots × 1200 sqft each)")

    # SB-684 specific -------------------------------------------------------
    if law_id == "sb684" and item_id == "zone_mf_or_infill":
        if _check_zone_mf(zone):
            return "pass", f"zone={zone} is MF"
        return "unknown", f"zone={zone} not MF; eligibility hinges on qualifying-infill perimeter test (TODO Phase 2)"

    # SB-1123 specific ------------------------------------------------------
    if law_id == "sb1123" and item_id == "vacancy_indicator":
        if impr_value is None:
            return "unknown", "improvement value not on GIS"
        # $0 improvement is the suggestive vacancy signal
        return ("pass" if (impr_value or 0) == 0 else "fail", f"impr_value=${impr_value:,.0f}")

    # SB-1211 specific ------------------------------------------------------
    if law_id == "sb1211" and item_id == "existing_residential":
        if impr_value is None:
            return "unknown", "improvement value not on GIS"
        return ("pass" if (impr_value or 0) > 0 else "fail", f"impr_value=${impr_value:,.0f}")

    # State ADU specific ----------------------------------------------------
    if law_id == "state-adu" and item_id == "primary_dwelling":
        if impr_value is None:
            return "unknown", "improvement value not on GIS — verify existing primary"
        return ("pass" if (impr_value or 0) > 0 else "unknown", f"impr_value=${impr_value:,.0f}")

    # AB-130 specific -------------------------------------------------------
    if law_id == "ab130":
        if item_id == "qualifies_sb684_or_sb1123":
            # Couldn't check parent-statute eligibility from here cleanly; flag
            return "unknown", "Cross-references SB-684/SB-1123 eligibility — see those rows"
        if item_id == "existing_improvement":
            if impr_value is None:
                return "unknown", "improvement value not on GIS"
            return ("pass" if (impr_value or 0) > 0 else "fail", f"impr_value=${impr_value:,.0f}")

    # Density Bonus -- general residential project --------------------------
    if law_id == "density-bonus" and item_id == "any_residential_project":
        if _check_zone_any_residential(zone) or _check_zone_commercial(zone):
            return "pass", f"zone={zone} can host residential (including via AB-2011 / SB-6 stack)"
        return "unknown", f"zone={zone} — verify residential eligibility"

    # ------------------------------------------------------------------
    # Statewide overlay-backed exclusion checks (fire / fault / farmland /
    # historic / flood). Overlays come from app/services/parcel_overlays.py
    # and are run BEFORE the eligibility engine in run_parcel_screen.
    # ------------------------------------------------------------------
    overlays = facts.get("overlays") or {}

    if item_id == "not_fhsz":
        fhsz = overlays.get("fhsz") or {}
        in_zone = fhsz.get("in_zone")
        if in_zone is True:
            return "fail", f"FHSZ class={fhsz.get('class')}"
        if in_zone is False:
            return "pass", "Not in CalFire FHSZ (SRA + LRA checked)"
        # Fall back to the parcel-level fire_zone attribute (Rialto carries
        # this inline) when the statewide overlay didn't answer.
        fz = facts.get("fire_zone")
        if fz is not None:
            fz_lower = str(fz).lower().strip()
            if fz_lower in ("", "none", "lra", "low", "0"):
                return "pass", f"fire zone={fz} (parcel attr)"
            return "fail", f"fire zone={fz} (parcel attr)"
        return "unknown", f"FHSZ overlay error: {fhsz.get('error') or 'no result'}"

    if item_id == "not_fault":
        ap = overlays.get("alquist_priolo") or {}
        in_zone = ap.get("in_zone")
        if in_zone is True:
            return "fail", f"Alquist-Priolo zone: {ap.get('quad') or 'CA fault zone'}"
        if in_zone is False:
            return "pass", "Not in Alquist-Priolo fault zone"
        return "unknown", f"Alquist-Priolo overlay error: {ap.get('error') or 'no result'}"

    if item_id == "not_farmland":
        fm = overlays.get("fmmp") or {}
        in_zone = fm.get("in_zone")
        if in_zone is True:
            return "fail", f"FMMP class={fm.get('class')}"
        if in_zone is False:
            ptype = fm.get("polygon_ty")
            return "pass", f"Not Prime/Statewide/Unique farmland (polygon_ty={ptype})"
        return "unknown", f"FMMP overlay error: {fm.get('error') or 'no result'}"

    if item_id == "not_historic":
        hist = overlays.get("historic") or {}
        in_district = hist.get("in_district")
        if in_district is True:
            return "fail", f"NRHP listing: {hist.get('name') or 'historic district'}"
        if in_district is False:
            return "pass", "Not in National Register polygon (local districts not covered)"
        return "unknown", f"Historic overlay error: {hist.get('error') or 'no result'}"

    if item_id == "not_flood":
        fl = overlays.get("flood") or {}
        in_sfha = fl.get("in_sfha")
        if in_sfha is True:
            return "fail", f"FEMA SFHA zone={fl.get('zone')}"
        if in_sfha is False:
            zone = fl.get("zone")
            return "pass", f"Not in FEMA SFHA (zone={zone or 'unmapped'})"
        return "unknown", f"Flood overlay error: {fl.get('error') or 'no result'}"

    # Other deferred checks we haven't built overlays for yet
    if item_id in ("not_hazwaste", "not_coastal", "urbanized", "perimeter_urban_75pct", "not_conservation"):
        return "unknown", "statewide overlay not loaded (deferred to Phase 2)"

    return "unknown", "no auto-check implemented"


def _compute_yield(law: ParcelStateLaw, facts: dict, zone_density: ParcelZoneDensity | None) -> dict:
    """Compute max unit yield per the law's yield_formula."""
    formula = law.yield_formula or {}
    if formula.get("stub"):
        return {"max_units": formula.get("max_units"), "stub": True, "note": formula.get("note", "")}

    acres = facts.get("acres") or 0
    du_per_ac = float(zone_density.du_per_ac) if zone_density and zone_density.du_per_ac else None

    if law.law_id == "by-right":
        if du_per_ac is None:
            return {"max_units": None, "note": "zone density not cached; need to scrape zoning code"}
        units = max(1, math.floor(acres * du_per_ac))
        return {"max_units": units, "calc": f"floor({acres:.2f} × {du_per_ac}) = {units}"}

    if law.law_id == "sb9":
        # Up to 4 if urban lot split + duplex per resulting lot
        return {"max_units": 4, "note": "Urban lot split (2 lots) + duplex each = 4"}

    if law.law_id == "sb684":
        # 10-unit statutory ceiling
        max_lots_by_size = math.floor((acres * 43560) / 600) if acres else None
        max_units = min(10, max_lots_by_size) if max_lots_by_size else 10
        return {
            "max_units": max_units,
            "calc": f"min(10, floor({acres or '?':.2f} × 43560 / 600)) = {max_units}",
            "note": "10-unit statutory ceiling, 600 sqft min lot. GP density is a floor, not a cap.",
        }

    return {"max_units": formula.get("max_units"), "formula": formula}


# ---------------------------------------------------------------------------
# Permit history cross-reference
# ---------------------------------------------------------------------------
async def fetch_permit_history(
    db: AsyncSession,
    apn: str | None,
    address: str | None,
    state: str,
    limit: int = 25,
) -> list[dict]:
    """Pull past permits for this parcel from our existing permits table.

    Uses APN-only matching (fast — there's an index). Address ILIKE was tried
    initially but it scans the full 776M-row permits table and hits the 20s
    statement_timeout. Rob's parcel APNs typically appear as the permit_number
    on issued permits anyway.

    On failure (timeout, lock, anything), we roll back the savepoint and return
    an empty list — the caller still gets a usable parcel screen.
    """
    if not apn:
        return []

    apn_clean = "".join(c for c in apn if c.isalnum())
    apn_variants = list({apn, apn_clean})

    sql = """
        SET LOCAL statement_timeout = '6s';
        SELECT permit_number, address, city, state_code, project_type, work_type,
               status, description, date_created, owner_name, applicant_name
        FROM permits
        WHERE state_code = :state
          AND (permit_number = ANY(:apn_variants) OR description ILIKE :apn_like)
        ORDER BY date_created DESC NULLS LAST
        LIMIT :limit
    """
    params = {
        "state": state,
        "apn_variants": apn_variants,
        "apn_like": f"%{apn_clean}%",
        "limit": limit,
    }

    # Wrap in a savepoint so a timeout doesn't poison the outer transaction.
    try:
        async with db.begin_nested():
            # Apply statement_timeout BEFORE the SELECT, in the same transaction.
            await db.execute(text("SET LOCAL statement_timeout = '6s'"))
            result = await db.execute(
                text("""
                    SELECT permit_number, address, city, state_code, project_type, work_type,
                           status, description, date_created, owner_name, applicant_name
                    FROM permits
                    WHERE state_code = :state
                      AND (permit_number = ANY(:apn_variants) OR description ILIKE :apn_like)
                    ORDER BY date_created DESC NULLS LAST
                    LIMIT :limit
                """),
                params,
            )
            rows = []
            for row in result.mappings():
                rows.append({
                    "permit_number": row.get("permit_number"),
                    "address": row.get("address"),
                    "city": row.get("city"),
                    "state": row.get("state_code"),
                    "project_type": row.get("project_type"),
                    "work_type": row.get("work_type"),
                    "status": row.get("status"),
                    "description": row.get("description"),
                    "date": row.get("date_created").isoformat() if row.get("date_created") else None,
                    "owner_name": row.get("owner_name"),
                    "applicant_name": row.get("applicant_name"),
                })
            return rows
    except Exception as e:
        logger.warning(f"permit history query failed (savepoint rolled back): {e}")
        return []


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
async def run_parcel_screen(
    db: AsyncSession,
    state: str,
    city_slug: str,
    address: str | None,
    apn: str | None,
) -> dict:
    """Top-level parcel-screen orchestration.

    Returns the full memo structure as a dict (matches the JSON the frontend
    renders into Rob's memo layout).
    """
    # 1. Resolve jurisdiction
    result = await db.execute(
        select(ParcelJurisdiction).where(
            ParcelJurisdiction.state == state,
            ParcelJurisdiction.city_slug == city_slug,
        )
    )
    jurisdiction = result.scalar_one_or_none()
    if not jurisdiction:
        raise ValueError(f"jurisdiction not registered: {state}/{city_slug}")

    # 2. Pull parcel facts from GIS
    facts = await pull_parcel_facts(jurisdiction, address=address, apn=apn)
    if facts.get("error"):
        return {"status": "parcel_not_found", "jurisdiction": city_slug, "details": facts}

    # 2b. Fire statewide CA exclusion overlays in parallel against the parcel
    # centroid. Each overlay times out at 4s and individual failures don't
    # poison the batch — failed overlays come back with `error` + `in_zone=None`
    # and the eligibility engine flags those items "unknown" (verify).
    lat = facts.get("lat")
    lng = facts.get("lng")
    if lat is not None and lng is not None:
        try:
            facts["overlays"] = await query_all_overlays(lat, lng)
        except Exception as e:
            logger.warning(f"overlay batch failed: {e}")
            facts["overlays"] = {"error": str(e)}
    else:
        facts["overlays"] = None

    # 3. Look up zone density (may be None → flagged in yield calc)
    zone_density = None
    if facts.get("zone_code"):
        result = await db.execute(
            select(ParcelZoneDensity).where(
                ParcelZoneDensity.state == state,
                ParcelZoneDensity.city_slug == city_slug,
                ParcelZoneDensity.zone_code == facts["zone_code"],
            )
        )
        zone_density = result.scalar_one_or_none()

    # Promote density-table hints into facts so the eligibility engine can
    # use them (e.g., Rialto A-1 is is_residential=true even though its zone
    # code doesn't match the SFR/MF pattern matchers).
    if zone_density:
        facts["zone_is_residential_flag"] = (zone_density.is_residential == "Y")
        facts["zone_du_per_ac"] = float(zone_density.du_per_ac) if zone_density.du_per_ac else None
        facts["zone_min_lot_sqft"] = zone_density.min_lot_sqft

    # 4. Test each state law
    result = await db.execute(
        select(ParcelStateLaw)
        .where(ParcelStateLaw.state == state)
        .order_by(ParcelStateLaw.display_order)
    )
    laws = result.scalars().all()

    law_results = []
    for law in laws:
        elig = _evaluate_eligibility(law, facts)
        yld = _compute_yield(law, facts, zone_density)
        is_stale = law.last_verified is None
        law_results.append({
            "law_id": law.law_id,
            "name": law.name,
            "code_section": law.code_section,
            "summary": law.summary,
            "leginfo_url": law.leginfo_url,
            "eligibility": elig,
            "yield": yld,
            "caveats_md": law.caveats_md,
            "last_verified": law.last_verified.isoformat() if law.last_verified else None,
            "stale_warning": is_stale,
        })

    # 5. Permit history cross-reference
    permit_history = await fetch_permit_history(
        db,
        apn=facts.get("apn"),
        address=facts.get("address") or address,
        state=state,
    )

    # 6. Zoning / GP mismatch flag
    mismatch = None
    if facts.get("gp_code") and facts.get("zone_code"):
        gp_residential = any(s in (facts["gp_code"] or "").upper() for s in ("R", "RES"))
        zone_residential = _check_zone_residential(facts["zone_code"]) or _check_zone_mf(facts["zone_code"])
        if gp_residential and not zone_residential:
            mismatch = {
                "type": "zoning_gp_mismatch",
                "note": "Current zoning is non-residential but GP indicates residential intent. Per Gov Code §65860 city is obligated to rezone toward consistency.",
            }

    return {
        "status": "ok",
        "jurisdiction": {
            "state": jurisdiction.state,
            "city_slug": jurisdiction.city_slug,
            "display_name": jurisdiction.display_name,
            "gis_viewer_url": jurisdiction.gis_viewer_url,
            "last_verified": jurisdiction.last_verified.isoformat() if jurisdiction.last_verified else None,
        },
        "parcel": {
            "apn": facts.get("apn"),
            "address": facts.get("address"),
            "owner_name": facts.get("owner_name"),
            "owner_addr": facts.get("owner_addr"),
            "owner_occupied": _is_owner_occupied(facts),
            "acres": facts.get("acres"),
            "year_built": facts.get("year_built"),
            "land_value": facts.get("land_value"),
            "impr_value": facts.get("impr_value"),
            "zone_code": facts.get("zone_code"),
            "zone_desc": facts.get("zone_desc"),
            "gp_code": facts.get("gp_code"),
            "gp_desc": facts.get("gp_desc"),
            "sp_code": facts.get("sp_code"),
            "sp_desc": facts.get("sp_desc"),
            "fire_zone": facts.get("fire_zone"),
            # WGS84 lat/lng centroid + polygon for frontend map rendering.
            "lat": facts.get("lat"),
            "lng": facts.get("lng"),
            "geometry_wgs84": facts.get("geometry_wgs84"),
            "raw_attributes": facts.get("raw_attributes"),
            # Statewide CA exclusion overlays (CalFire FHSZ, Alquist-Priolo,
            # FMMP, NRHP, FEMA SFHA). One dict per overlay id; failed
            # overlays carry `error` + `in_zone=None`.
            "overlays": facts.get("overlays"),
        },
        "zoning_gp_mismatch": mismatch,
        "zone_density_loaded": zone_density is not None,
        "laws": law_results,
        "permit_history": permit_history,
        "permit_history_count": len(permit_history),
        "phase2_deferred": [
            "SB-684 qualifying-infill perimeter test (spatial 75% urban-use)",
            "Mercator dimension correction for irregular parcels",
            "Auto-discovery of GIS endpoints for new cities (Chrome MCP server-side)",
            "Bake overlays into Hot Picks refresh (currently on-screen only — 120K+ extra Esri calls per city per refresh)",
        ],
    }


def _is_owner_occupied(facts: dict) -> bool:
    """Detect owner-occupancy by comparing mailing addr to site addr."""
    owner_addr = (facts.get("owner_addr") or "").strip().lower()
    site_addr = (facts.get("address") or "").strip().lower()
    if not owner_addr or not site_addr:
        return False
    # Loose match — first 10 chars of street should match
    return owner_addr[:10] == site_addr[:10]
