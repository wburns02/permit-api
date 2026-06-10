"""Shared library for the TX permit classifier (Rig & Permit Radar Phase 4).

Builds the classification prompt from config/permit_taxonomy_v1.json, calls
Ollama (qwen3.5:122b) with `think: false` as a TOP-LEVEL key and a JSON-schema
constrained `format`, and parses/validates the output.

Used by:
  - evals/permit_classifier/label_qwen.py   (candidate labels for eval set)
  - evals/permit_classifier/run_eval.py     (gate scoring)
  - scripts/enrich_permits_bulk.py          (3.8M-row bulk run)
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import httpx

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://100.85.99.69:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3.5:122b")

_HERE = Path(__file__).resolve().parent
# Works both from repo (/home/will/permit-api/scripts) and live dir.
TAXONOMY_PATH = Path(
    os.environ.get("PERMIT_TAXONOMY_PATH", _HERE.parent / "config" / "permit_taxonomy_v1.json")
)

PROMPT_VERSION = "prompt_v2"


def load_taxonomy() -> dict:
    with open(TAXONOMY_PATH) as f:
        return json.load(f)


def category_keys(tax: dict | None = None) -> list[str]:
    tax = tax or load_taxonomy()
    return [c["key"] for c in tax["categories"]]


def classifier_version(tax: dict | None = None) -> str:
    tax = tax or load_taxonomy()
    return f"qwen3.5-122b/{tax['version']}/{PROMPT_VERSION}"


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------
_NO_SUMMARY_HEADER = "You classify building permits from Texas jurisdictions into exactly one category from a closed set.\n"

_DECISION_RULES = """DECISION RULES (apply in order):
1. HARD RULE — trade permits: if permit_type begins with "Electrical", "Plumbing", or "Mechanical", the category is that trade (electrical / plumbing / mechanical_hvac) NO MATTER WHAT the description says. Descriptions routinely restate the PARENT PROJECT (new house, living-room addition, tenant finish-out, remodel, demolition, mobile home move) — that never changes the trade category. Subtype words in the permit_type itself (New / Remodel / Repair / Change Out / Addition / Upgrade / Umbrella / Standalone / Shell / Loop / Demolition / Interior Demo Non-Structural) also stay in the trade category. The ONLY exceptions are when the permit_type itself names a more specific scope: Irrigation or Lawn Sprinkler -> irrigation; Fireline -> fire_systems; Cut Over/Tank Abandonment -> septic_ossf; Solar or Photovoltaic -> solar (Backflow stays plumbing).
2. HARD RULE — flatwork: if permit_type begins with "Driveway", "Sidewalk", or "Paving", the category is driveway_flatwork regardless of the description (same own-scope principle as rule 1).
3. Specific beats general for BUILDING permits only: if permit_type is a generic building/residential/commercial permit and the description clearly shows solar, pool/spa, septic, roofing, foundation repair, irrigation/sprinkler, fence, sign, demolition, or driveway/sidewalk work, use that specific category. Example: "Re-Roof Permit" -> roofing; "Building Permit / Repair" + "REROOF" description -> roofing.
4. residential vs commercial vs multifamily: single-family/duplex/townhome -> residential_*; apartment/condo buildings (3+ units) new construction -> multifamily_new; multifamily remodels -> residential_remodel; offices, retail, restaurants, warehouses, industrial, institutional -> commercial_*.
5. "Addition and Remodel" building permits -> residential_addition (or commercial_remodel_ti if commercial).
6. Generic department-only permit_type ("Building Inspections and Permits", "Historical", "Engineering", "Police Department"): classify from the description. If the description is empty or uninformative, use other_unknown. Do not guess.
7. Standalone Building/Demolition permits for removing a structure (or interior demo) -> demolition. This does NOT apply to trade permits with a Demolition subtype (rule 1 wins).
8. Detached garages, sheds, carports, barns, decks, gazebos on BUILDING permits -> accessory_structure. Attached additions to the dwelling -> residential_addition.
9. Street/road/bridge construction and maintenance, seal coat, overlay -> grading_sitework. Work in public right-of-way, utility locates, franchise utility, barricades, water/sewer service connections by a utility -> row_utility.
10. Building-permit sewer cut-over with septic tank abandonment -> septic_ossf. City sewer connection without septic involvement -> plumbing."""


def build_system_prompt(tax: dict | None = None, include_summary: bool = True) -> str:
    tax = tax or load_taxonomy()
    cat_lines = []
    for c in tax["categories"]:
        cat_lines.append(f"- {c['key']}: {c['description']}")
    cats = "\n".join(cat_lines)
    if not include_summary:
        return _NO_SUMMARY_HEADER + f"""
CATEGORIES (use the key exactly as written):
{cats}

{_DECISION_RULES}

OUTPUT: For each permit in the input JSON array, return one result object with ONLY these keys:
- id: the permit's id, echoed exactly
- category: one category key from the list
- confidence: 0.0-1.0, your confidence in the category
Do NOT include a summary or any other key.

Return ONLY a compact single-line JSON object {{"results": [...]}} with results in the same order as the input. No markdown fences, no extra whitespace.

EXAMPLES:
Input permit: {{"id": "ex1", "permit_type": "Electrical Permit / New", "description": "New 1-story single-family residence attached garage covered entry porch and patio.", "declared_value": null}}
Result: {{"id": "ex1", "category": "electrical", "confidence": 0.97}}

Input permit: {{"id": "ex2", "permit_type": "Building Permit / Remodel", "description": "New Storage Shed", "declared_value": 400}}
Result: {{"id": "ex2", "category": "accessory_structure", "confidence": 0.9}}

Input permit: {{"id": "ex3", "permit_type": "Plumbing Permit / Cut Over/Tank Abandonment", "description": "City sewer cut over to residence only.", "declared_value": null}}
Result: {{"id": "ex3", "category": "septic_ossf", "confidence": 0.85}}

Input permit: {{"id": "ex4", "permit_type": "Building Inspections and Permits", "description": "", "declared_value": null}}
Result: {{"id": "ex4", "category": "other_unknown", "confidence": 0.95}}

Input permit: {{"id": "ex5", "permit_type": "Building Permit / Repair", "description": "REROOF - replace comp shingles, decking as needed", "declared_value": 12000}}
Result: {{"id": "ex5", "category": "roofing", "confidence": 0.97}}

Input permit: {{"id": "ex6", "permit_type": "Mechanical Permit / Addition", "description": "One Story Addition To Extend Living Room", "declared_value": null}}
Result: {{"id": "ex6", "category": "mechanical_hvac", "confidence": 0.97}}

Input permit: {{"id": "ex7", "permit_type": "Plumbing Permit / Interior Demo Non-Structural", "description": "Remodel/Repair Portable Classroom & Relocate", "declared_value": null}}
Result: {{"id": "ex7", "category": "plumbing", "confidence": 0.95}}

Input permit: {{"id": "ex8", "permit_type": "Driveway / Sidewalks / Modification", "description": "Remodel For Admin & Professional Offices", "declared_value": null}}
Result: {{"id": "ex8", "category": "driveway_flatwork", "confidence": 0.95}}
"""
    return f"""You classify building permits from Texas jurisdictions into exactly one category from a closed set, and write a short factual summary.

CATEGORIES (use the key exactly as written):
{cats}

{_DECISION_RULES}

SUMMARY RULES:
- 1 short factual sentence (under 20 words), 2 only if essential, built ONLY from the given fields (permit type, description, declared value). No speculation, no marketing language, no invented details. Do not repeat the address or date in the summary.
- If the description is empty, summarize what the permit type and other fields state.

OUTPUT: For each permit in the input JSON array, return one result object with:
- id: the permit's id, echoed exactly
- category: one category key from the list
- confidence: 0.0-1.0, your confidence in the category
- summary: the 1-2 sentence factual summary

Return ONLY JSON matching the required schema, with results in the same order as the input.

EXAMPLES:
Input permit: {{"id": "ex1", "permit_type": "Electrical Permit / New", "description": "New 1-story single-family residence attached garage covered entry porch and patio.", "declared_value": null}}
Result: {{"id": "ex1", "category": "electrical", "confidence": 0.97, "summary": "Electrical permit for a new one-story single-family residence with attached garage."}}

Input permit: {{"id": "ex2", "permit_type": "Building Permit / Remodel", "description": "New Storage Shed", "declared_value": 400}}
Result: {{"id": "ex2", "category": "accessory_structure", "confidence": 0.9, "summary": "Permit for a new storage shed with a declared value of $400."}}

Input permit: {{"id": "ex3", "permit_type": "Plumbing Permit / Cut Over/Tank Abandonment", "description": "City sewer cut over to residence only.", "declared_value": null}}
Result: {{"id": "ex3", "category": "septic_ossf", "confidence": 0.85, "summary": "Plumbing permit to cut over a residence to city sewer with septic tank abandonment."}}

Input permit: {{"id": "ex4", "permit_type": "Building Inspections and Permits", "description": "", "declared_value": null}}
Result: {{"id": "ex4", "category": "other_unknown", "confidence": 0.95, "summary": "Building inspections and permits record with no description provided."}}

Input permit: {{"id": "ex5", "permit_type": "Building Permit / Repair", "description": "REROOF - replace comp shingles, decking as needed", "declared_value": 12000}}
Result: {{"id": "ex5", "category": "roofing", "confidence": 0.97, "summary": "Reroof replacing composition shingles and decking as needed, declared value $12,000."}}

Input permit: {{"id": "ex6", "permit_type": "Mechanical Permit / Addition", "description": "One Story Addition To Extend Living Room", "declared_value": null}}
Result: {{"id": "ex6", "category": "mechanical_hvac", "confidence": 0.97, "summary": "Mechanical permit for a one-story addition extending the living room."}}

Input permit: {{"id": "ex7", "permit_type": "Plumbing Permit / Interior Demo Non-Structural", "description": "Remodel/Repair Portable Classroom & Relocate", "declared_value": null}}
Result: {{"id": "ex7", "category": "plumbing", "confidence": 0.95, "summary": "Plumbing permit under a portable classroom remodel, repair and relocation project."}}

Input permit: {{"id": "ex8", "permit_type": "Driveway / Sidewalks / Modification", "description": "Remodel For Admin & Professional Offices", "declared_value": null}}
Result: {{"id": "ex8", "category": "driveway_flatwork", "confidence": 0.95, "summary": "Driveway and sidewalk modification under an office remodel project."}}
"""


def _output_schema(tax: dict | None = None, include_summary: bool = True) -> dict:
    keys = category_keys(tax)
    if not include_summary:
        return {
            "type": "object",
            "properties": {
                "results": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "category": {"type": "string", "enum": keys},
                            "confidence": {"type": "number"},
                        },
                        "required": ["id", "category", "confidence"],
                        "additionalProperties": False,
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["results"],
        }
    return {
        "type": "object",
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "category": {"type": "string", "enum": keys},
                        "confidence": {"type": "number"},
                        "summary": {"type": "string"},
                    },
                    "required": ["id", "category", "confidence", "summary"],
                },
            }
        },
        "required": ["results"],
    }


def permit_payload(row: dict) -> dict:
    """Trim a DB row to the fields the model sees."""
    dv = row.get("declared_value")
    if dv is not None:
        try:
            dv = float(dv)
        except (TypeError, ValueError):
            dv = None
    return {
        "id": str(row["id"]),
        "permit_type": (row.get("permit_type") or "")[:300],
        "description": (row.get("description_raw") or "")[:1000],
        "address": (row.get("address_raw") or "")[:200],
        "declared_value": dv,
        "issued_date": str(row.get("issued_date") or ""),
    }


def classify_batch(
    rows: list[dict],
    *,
    system_prompt: str,
    tax: dict,
    client: httpx.Client | None = None,
    timeout: float = 600.0,
    num_predict: int | None = None,
    include_summary: bool = True,
) -> tuple[dict[str, dict], dict]:
    """Classify a batch of permit rows. Returns ({id: result}, stats).

    Each row needs: id, permit_type, description_raw, address_raw,
    declared_value, issued_date.

    Wire format uses short positional ids ("1".."N") instead of echoing
    36-char UUIDs: the UUID echo alone costs ~30 output tokens per item,
    which at ~4 tok/s on the R730 is ~7s/permit. Results are mapped back
    to the real ids here.
    """
    short_to_real = {str(i + 1): str(r["id"]) for i, r in enumerate(rows)}
    payload = []
    for i, r in enumerate(rows):
        p = permit_payload(r)
        p["id"] = str(i + 1)
        payload.append(p)
    user_msg = "Classify these permits:\n" + json.dumps(payload, ensure_ascii=False)
    body: dict[str, Any] = {
        "model": OLLAMA_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_msg},
        ],
        "stream": False,
        "think": False,  # MUST be top-level; nested under options it is ignored
        "keep_alive": "24h",  # avoid unload/reload churn between batches
        "format": _output_schema(tax, include_summary=include_summary),
        "options": {
            # Per-item budget with short positional ids: ~50 tokens covers
            # id+category+confidence+syntax+whitespace; ~130 with a summary.
            "num_predict": num_predict or ((130 if include_summary else 50) * len(rows) + 120),
            "temperature": 0.0,
            # 8K ctx (vs 262K default) shrinks KV cache so more of the 122B
            # fits on the two 3090s. System prompt ~2.3K + 8-permit batch
            # fits comfortably.
            "num_ctx": 8192,
        },
    }
    cl = client or httpx
    r = cl.post(f"{OLLAMA_URL}/api/chat", json=body, timeout=timeout)
    r.raise_for_status()
    j = r.json()
    text = (j.get("message") or {}).get("content") or ""
    stats = {
        "prompt_tokens": int(j.get("prompt_eval_count") or 0),
        "output_tokens": int(j.get("eval_count") or 0),
        "eval_duration_ns": int(j.get("eval_duration") or 0),
        "total_duration_ns": int(j.get("total_duration") or 0),
    }
    # The `format` schema is NOT reliably enforced by Ollama for this model
    # (observed 0.17.5 + qwen3.5:122b): the model may emit ```json fences,
    # a bare array, or extra keys. Parse defensively.
    raw = text.strip()
    if raw.startswith("```"):
        raw = "\n".join(
            ln for ln in raw.splitlines() if not ln.strip().startswith("```")
        ).strip()
    start_obj = raw.find("{")
    start_arr = raw.find("[")
    if start_arr != -1 and (start_obj == -1 or start_arr < start_obj):
        raw = raw[start_arr : raw.rfind("]") + 1]
    elif start_obj != -1:
        raw = raw[start_obj : raw.rfind("}") + 1]
    parsed = json.loads(raw)
    if isinstance(parsed, list):  # bare array
        items = parsed
    else:
        items = parsed.get("results", [])
    keys = set(category_keys(tax))
    out: dict[str, dict] = {}
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        short = str(item.get("id", "")).strip()
        # map positional id back to the real permit id; fall back to order
        pid = short_to_real.get(short) or (
            short_to_real.get(str(idx + 1)) if len(items) == len(rows) else None
        )
        cat = item.get("category")
        if pid and cat in keys:
            conf = item.get("confidence")
            try:
                conf = max(0.0, min(1.0, float(conf)))
            except (TypeError, ValueError):
                conf = None
            out[pid] = {
                "category": cat,
                "confidence": conf,
                "summary": (item.get("summary") or "").strip()[:600],
            }
    return out, stats
