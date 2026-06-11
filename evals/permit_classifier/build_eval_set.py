"""Merge Qwen candidate labels + Claude critique into eval_set_v1.jsonl.

Resolution policy (documented; applied automatically, manual overrides allowed
via resolutions.json):
  - critique verdict == agree            -> final = qwen label, provenance "qwen+claude-agree"
  - disagree, resolvable by an explicit RULE below -> final per rule, provenance "rule:<name>"
  - disagree, no rule                    -> EXCLUDED, reason logged to exclusions.jsonl

Explicit resolution rules (mirror the taxonomy decision rules; the side that
matches the rule wins regardless of which model said it):
  R1 trade-permit-stays-trade: when permit_type starts with a trade
     (Electrical/Plumbing/Mechanical) permit and the description merely names
     the parent project, the trade category wins.
  R2 specific-beats-general: when the description clearly names a specific
     scope (roof, pool, solar, septic, irrigation, fence, sign, foundation,
     driveway/sidewalk, demolition), the specific category wins.
  R3 empty-desc-generic-type: generic department/bucket permit_type with
     empty/uninformative description -> other_unknown wins.
  R4 attached-vs-detached: detached structures -> accessory_structure;
     attached additions -> residential_addition.

A human-edited resolutions.json {id: {"final": key, "why": str}} takes
precedence over everything (used for spot-fixes after review).
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "scripts"))
from permit_classifier_lib import load_taxonomy  # noqa: E402

CANDS = HERE / "eval_candidates.jsonl"
QWEN = HERE / "qwen_labels.jsonl"
CRIT = HERE / "critique.jsonl"
MANUAL = HERE / "resolutions.json"
OUT = HERE / "eval_set_v1.jsonl"
EXCL = HERE / "exclusions.jsonl"

TRADE_RE = re.compile(r"^\s*(electrical|plumbing|mechanical)\b", re.I)
TRADE_CAT = {"electrical": "electrical", "plumbing": "plumbing", "mechanical": "mechanical_hvac"}
SPECIFIC = [
    (re.compile(r"re-?roof|shingle|roof replace|roof repair", re.I), "roofing"),
    (re.compile(r"\bsolar\b|photovoltaic|\bpv system", re.I), "solar"),
    (re.compile(r"swimming pool|in.?ground pool|\bspa\b|hot tub", re.I), "pool_spa"),
    (re.compile(r"septic|ossf|aerobic system|drain ?field", re.I), "septic_ossf"),
    (re.compile(r"irrigation|lawn sprinkler", re.I), "irrigation"),
    (re.compile(r"\bfence\b|\bfencing\b", re.I), "fence"),
    (re.compile(r"\bsign\b|billboard|banner", re.I), "sign"),
    (re.compile(r"foundation (repair|level|pier)", re.I), "foundation_repair"),
    (re.compile(r"driveway|sidewalk|drive approach|curb cut", re.I), "driveway_flatwork"),
    (re.compile(r"demolish|demolition|interior demo", re.I), "demolition"),
]
GENERIC_TYPES = re.compile(
    r"^(historical( records)?|building inspections?( and permits)?|building department"
    r"|engineering|public works.*|police department|fire department|health department"
    r"|community development|planning and zoning|parks & recreation)$", re.I)


def resolve(cand: dict, qwen_cat: str, claude_cat: str | None) -> tuple[str | None, str]:
    """Returns (final_category or None-to-exclude, provenance)."""
    pt = (cand.get("permit_type") or "").strip()
    desc = (cand.get("description_raw") or "").strip()
    both = {qwen_cat, claude_cat} - {None}

    # R2 first: specific scope in description wins even over trade types.
    for rx, cat in SPECIFIC:
        if rx.search(desc) and cat in both:
            return cat, "rule:R2-specific-beats-general"
    # R1: trade permit_type keeps the trade category.
    m = TRADE_RE.match(pt)
    if m:
        tcat = TRADE_CAT[m.group(1).lower()]
        if tcat in both:
            return tcat, "rule:R1-trade-stays-trade"
    # R1b: driveway/sidewalk/paving permit types stay flatwork even when the
    # description names the parent project (same own-scope principle as R1).
    if re.match(r"^\s*(driveway|sidewalk|paving)\b", pt, re.I) and "driveway_flatwork" in both:
        return "driveway_flatwork", "rule:R1b-flatwork-stays-flatwork"
    # R3: generic bucket + empty description -> other_unknown.
    if GENERIC_TYPES.match(pt) and len(desc) < 8 and "other_unknown" in both:
        return "other_unknown", "rule:R3-empty-generic"
    # R4: detached keyword -> accessory_structure.
    if re.search(r"detached|shed|carport|gazebo|barn\b", desc, re.I) and "accessory_structure" in both:
        return "accessory_structure", "rule:R4-detached-accessory"
    if re.search(r"attached|addition to (house|residence|dwelling)", desc, re.I) and "residential_addition" in both:
        return "residential_addition", "rule:R4-attached-addition"
    return None, "unresolved"


def main() -> None:
    cands = {json.loads(l)["id"]: json.loads(l) for l in open(CANDS)}
    qwen = {json.loads(l)["id"]: json.loads(l) for l in open(QWEN)}
    crit = {json.loads(l)["id"]: json.loads(l) for l in open(CRIT)}
    manual = json.loads(MANUAL.read_text()) if MANUAL.exists() else {}
    tax = load_taxonomy()

    kept, excluded = 0, 0
    with open(OUT, "w") as fo, open(EXCL, "w") as fx:
        for cid, cand in cands.items():
            q = qwen.get(cid)
            c = crit.get(cid)
            if not q:
                fx.write(json.dumps({"id": cid, "reason": "no qwen label produced"}) + "\n")
                excluded += 1
                continue
            if cid in manual:
                final, prov = manual[cid]["final"], f"manual:{manual[cid].get('why','')}"
            elif not c:
                fx.write(json.dumps({"id": cid, "reason": "no critique verdict"}) + "\n")
                excluded += 1
                continue
            elif c["verdict"] == "agree":
                final, prov = q["category"], "qwen+claude-agree"
            else:
                final, prov = resolve(cand, q["category"], c.get("better"))
                if final is None:
                    fx.write(json.dumps({
                        "id": cid, "reason": "models disagree, no resolution rule",
                        "qwen": q["category"], "claude": c.get("better"),
                        "claude_reason": c.get("reason"),
                        "permit_type": cand.get("permit_type"),
                        "description": (cand.get("description_raw") or "")[:200],
                    }, ensure_ascii=False) + "\n")
                    excluded += 1
                    continue
            rec = {k: cand[k] for k in (
                "id", "source_id", "source_record_id", "permit_type", "description_raw",
                "address_raw", "declared_value", "issued_date", "freq_bucket", "decade")}
            rec["label"] = final
            rec["provenance"] = prov
            rec["taxonomy_version"] = tax["version"]
            fo.write(json.dumps(rec, ensure_ascii=False) + "\n")
            kept += 1
    print(f"kept {kept}, excluded {excluded} -> {OUT}")


if __name__ == "__main__":
    main()
