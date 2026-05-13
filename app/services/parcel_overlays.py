"""Statewide California exclusion-overlay point queries.

For each parcel screened, fire five parallel point-in-polygon queries against
free statewide Esri layers to auto-resolve the GIS items the eligibility
engine previously had to flag as "verify with planning":

    not_fhsz       — CalFire Fire Hazard Severity Zones (SRA + LRA)
    not_fault      — CGS Alquist-Priolo Earthquake Fault Zones
    not_farmland   — DOC FMMP Important Farmland
    not_historic   — NPS National Register of Historic Places (polygons)
    not_flood      — FEMA NFHL Flood Hazard Zones (S_FLD_HAZ_AR)

Hard constraints (echoed from the ticket):
- Each call: 4 s timeout.
- Combined wall-clock: under 5 s (asyncio.gather makes this trivial).
- Individual failure (timeout, 5xx, malformed JSON) MUST NOT fail the whole
  batch — that overlay's slot gets `{error: "...", in_zone: None}` and the
  rest still resolve.

URLs were probed live on 2026-05-13. If any moves, the contract is just
"Esri FeatureServer/MapServer that accepts a point-intersect query against
WGS84 lat/lng". Swap the `layer_url` and (if attribute names differ) the
`extract` function.

Out of scope for this module (Phase 2.5):
- Baking overlays into `parcel_hot_picks.py` refresh — that's 120K + extra
  Esri calls per city per refresh. Run on-screen only.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-overlay attribute extractors
# ---------------------------------------------------------------------------
# Each extractor turns an Esri "attributes" dict (when the parcel centroid
# DID land inside a polygon) into the shape we want to record. When no
# feature was returned, the caller short-circuits to `{in_zone: False}`
# (or `in_district: False` for historic) before invoking the extractor.

def _extract_fhsz(attrs: dict) -> dict:
    # SRA layer has SRA + HAZ_CLASS; LRA layer has HAZ_CLASS + VH_REC + INCORP.
    haz = (attrs.get("HAZ_CLASS") or "").strip() or None
    # SB-9 / SB-684 exclusion is "SRA OR LRA-Very-High". Use the haz class
    # text exactly as the source publishes it.
    return {
        "in_zone": True,
        "class": haz,
        "raw": {k: attrs.get(k) for k in ("SRA", "HAZ_CLASS", "VH_REC", "INCORP") if k in attrs},
    }


def _extract_alquist(attrs: dict) -> dict:
    return {
        "in_zone": True,
        "quad": attrs.get("QUAD_NAME"),
        "map_released": attrs.get("MAP_RELEASED"),
    }


def _extract_fmmp(attrs: dict) -> dict:
    # FMMP polygon_ty values: "P" (Prime), "S" (Statewide Importance),
    # "U" (Unique), "L" (Local Importance), "G" (Grazing), "X" (Other),
    # "D" (Urban / Built-Up), "W" (Water), "nv" (Not surveyed), "sAC"
    # (Semi-Ag), "R" (Rural Residential), "V" (Vacant). The exclusion gate
    # for SB-684 etc. is Prime / Statewide / Unique.
    code = (attrs.get("polygon_ty") or "").strip()
    EXCLUSION = {"P": "Prime Farmland", "S": "Farmland of Statewide Importance", "U": "Unique Farmland"}
    return {
        "in_zone": code in EXCLUSION,  # only the 3 exclusion classes count
        "class": EXCLUSION.get(code),
        "polygon_ty": code or None,
        "county": attrs.get("county_nam") or attrs.get("County"),
        "year": attrs.get("upd_year"),
    }


def _extract_nrhp(attrs: dict) -> dict:
    return {
        "in_district": True,
        "name": attrs.get("RESNAME"),
        "bnd_type": attrs.get("BND_TYPE"),
    }


def _extract_flood(attrs: dict) -> dict:
    # FEMA "in SFHA" gate is the SFHA_TF flag, NOT just "did we hit a polygon".
    # The whole country is covered by Zone X / Zone D / etc. — SFHA_TF=='T'
    # means the parcel is in a special flood hazard area (A/AE/AH/AO/V/VE).
    sfha = (attrs.get("SFHA_TF") or "").strip().upper()
    zone = (attrs.get("FLD_ZONE") or "").strip() or None
    return {
        "in_sfha": sfha == "T",
        "zone": zone,
        "subtype": attrs.get("ZONE_SUBTY"),
    }


# ---------------------------------------------------------------------------
# Overlay registry
# ---------------------------------------------------------------------------
STATEWIDE_OVERLAYS: dict[str, dict[str, Any]] = {
    # CalFire FHSZ — SRA (State Responsibility Area) tier.
    # NOTE: The published statewide service exposes SRA (layer 0) and LRA
    # (layer 1) as separate feature layers. We fire BOTH and combine — a
    # parcel in EITHER is "in_zone". SB-9 / SB-684 etc. flag LRA Very-High
    # as well as SRA.
    "fhsz_sra": {
        "name": "CalFire FHSZ — State Responsibility Area",
        "layer_url": "https://services.gis.ca.gov/arcgis/rest/services/Environment/Fire_Severity_Zones/MapServer/0",
        "extract": _extract_fhsz,
        "source_url": "https://osfm.fire.ca.gov/what-we-do/community-wildfire-preparedness-and-mitigation/fire-hazard-severity-zones",
    },
    "fhsz_lra": {
        "name": "CalFire FHSZ — Local Responsibility Area",
        "layer_url": "https://services.gis.ca.gov/arcgis/rest/services/Environment/Fire_Severity_Zones/MapServer/1",
        "extract": _extract_fhsz,
        "source_url": "https://osfm.fire.ca.gov/what-we-do/community-wildfire-preparedness-and-mitigation/fire-hazard-severity-zones",
    },
    "alquist_priolo": {
        "name": "CGS Alquist-Priolo Earthquake Fault Zones",
        "layer_url": "https://services2.arcgis.com/zr3KAIbsRSUyARHG/arcgis/rest/services/CGS_Alquist_Priolo_Fault_Zones/FeatureServer/0",
        "extract": _extract_alquist,
        "source_url": "https://www.conservation.ca.gov/cgs/alquist-priolo",
    },
    "fmmp": {
        "name": "DOC FMMP Important Farmland",
        "layer_url": "https://gis.conservation.ca.gov/server/rest/services/DLRP/CaliforniaImportantFarmland_2022/FeatureServer/0",
        "extract": _extract_fmmp,
        "source_url": "https://www.conservation.ca.gov/dlrp/fmmp",
    },
    "historic": {
        "name": "NPS National Register of Historic Places (polygons)",
        "layer_url": "https://services3.arcgis.com/OYP7N6mAJJCyH6hd/ArcGIS/rest/services/NationalRegisterofHistoricPlaces_Polygon/FeatureServer/0",
        "extract": _extract_nrhp,
        "source_url": "https://www.nps.gov/subjects/nationalregister/index.htm",
    },
    "flood": {
        "name": "FEMA National Flood Hazard Layer — Flood Hazard Zones",
        "layer_url": "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28",
        "extract": _extract_flood,
        "source_url": "https://www.fema.gov/flood-maps/national-flood-hazard-layer",
    },
}


# ---------------------------------------------------------------------------
# Esri point query
# ---------------------------------------------------------------------------
async def _query_one(
    overlay_id: str,
    cfg: dict,
    lat: float,
    lng: float,
    client: httpx.AsyncClient,
) -> tuple[str, dict]:
    """Run a single point-intersect query against an Esri FeatureServer/MapServer layer.

    Returns `(overlay_id, result_dict)`. On failure returns a result with
    `error: "..."` and `in_zone=None` so the caller can flag the item as
    "unknown" rather than failing the whole screen.
    """
    params = {
        "geometry": f"{lng},{lat}",
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "false",
        "f": "json",
    }
    url = f"{cfg['layer_url']}/query"
    try:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
    except httpx.TimeoutException:
        return overlay_id, {"error": "timeout", "in_zone": None, "source_url": cfg.get("source_url")}
    except httpx.HTTPStatusError as e:
        return overlay_id, {"error": f"http_{e.response.status_code}", "in_zone": None, "source_url": cfg.get("source_url")}
    except Exception as e:
        return overlay_id, {"error": f"{type(e).__name__}: {e}", "in_zone": None, "source_url": cfg.get("source_url")}

    # Esri returns 200 with an `error` body on bad requests — treat as failure.
    if isinstance(data, dict) and data.get("error"):
        return overlay_id, {
            "error": f"esri_error_{data['error'].get('code')}",
            "in_zone": None,
            "source_url": cfg.get("source_url"),
        }

    feats = data.get("features", []) if isinstance(data, dict) else []
    if not feats:
        # No polygon contains the point. Build the "out" shape per overlay
        # contract — historic uses `in_district`, flood uses `in_sfha`.
        if overlay_id == "historic":
            out: dict[str, Any] = {"in_district": False, "name": None}
        elif overlay_id == "flood":
            # Outside ANY NFHL polygon usually means unmapped — treat as
            # not-in-SFHA but mark the source so callers can see why.
            out = {"in_sfha": False, "zone": None, "subtype": None, "note": "no NFHL polygon at point"}
        else:
            out = {"in_zone": False, "class": None}
        out["source_url"] = cfg.get("source_url")
        return overlay_id, out

    extracted = cfg["extract"](feats[0].get("attributes", {}) or {})
    extracted["source_url"] = cfg.get("source_url")
    return overlay_id, extracted


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------
async def query_all_overlays(lat: float, lng: float, timeout: float = 4.0) -> dict[str, dict]:
    """Fire all statewide overlay point-queries in parallel.

    Combines the two FHSZ layers (SRA + LRA) into a single `fhsz` slot for
    the eligibility engine — a parcel in EITHER tier counts as in-zone.

    Total wall-clock is bounded by `timeout` (≈ 4 s default) because every
    call uses the same per-request timeout and they run concurrently.
    """
    async with httpx.AsyncClient(timeout=timeout) as client:
        results = await asyncio.gather(
            *[_query_one(k, v, lat, lng, client) for k, v in STATEWIDE_OVERLAYS.items()],
            return_exceptions=False,
        )

    out: dict[str, dict] = dict(results)

    # Fold fhsz_sra + fhsz_lra into a single `fhsz` slot. A parcel in either
    # the SRA tier or the LRA-Very-High tier should fail the "not_fhsz" gate.
    sra = out.pop("fhsz_sra", {}) or {}
    lra = out.pop("fhsz_lra", {}) or {}
    # If either subquery errored AND the other didn't resolve cleanly, mark
    # combined as None so the engine flags it "verify". If at least one
    # resolved cleanly with a definitive answer, prefer that.
    combined: dict[str, Any] = {"sra": sra, "lra": lra, "source_url": sra.get("source_url") or lra.get("source_url")}
    if sra.get("in_zone") is True or lra.get("in_zone") is True:
        cls = sra.get("class") if sra.get("in_zone") else lra.get("class")
        combined["in_zone"] = True
        combined["class"] = cls
    elif sra.get("in_zone") is False and lra.get("in_zone") is False:
        combined["in_zone"] = False
        combined["class"] = None
    else:
        # At least one errored and neither came back as a definitive True.
        combined["in_zone"] = None
        combined["class"] = None
        combined["error"] = sra.get("error") or lra.get("error") or "unknown"
    out["fhsz"] = combined

    return out
