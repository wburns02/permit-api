"""Second-pass label critique with `claude -p` (subscription CLI, low volume).

Reads eval_candidates.jsonl + qwen_labels.jsonl, sends batches of 25 to the
frontier model, asks it to AGREE or flag a disagreement with its own category
pick and a one-line reason. Writes critique.jsonl. Resumable by batch.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "scripts"))
from permit_classifier_lib import category_keys, load_taxonomy  # noqa: E402

CANDS = HERE / "eval_candidates.jsonl"
QWEN = HERE / "qwen_labels.jsonl"
OUT = HERE / "critique.jsonl"
BATCH = 25

PROMPT_TMPL = """You are auditing machine-generated category labels for Texas building permits.

The closed category set (key: description):
{cats}

Decision rules the labeler was supposed to follow:
1. Classify by the permit's OWN scope, not the parent project (a standalone electrical permit pulled for a new house is "electrical", not "residential_new").
2. Specific beats general (a generic building permit whose description says "reroof" is "roofing").
3. Trade subtype words (New/Remodel/Repair/Change Out) keep trade permits in their trade category.
4. Single-family/duplex/townhome -> residential_*; new apartment/condo (3+ units) -> multifamily_new; multifamily remodels -> residential_remodel; offices/retail/industrial -> commercial_*.
5. "Addition and Remodel" building permits -> residential_addition (or commercial_remodel_ti).
6. Generic department-only types with empty/uninformative descriptions -> other_unknown.
7. Detached garage/shed/carport/deck -> accessory_structure; attached additions -> residential_addition.
8. Road/bridge/seal-coat work -> grading_sitework; right-of-way/utility/barricade -> row_utility.
9. Sewer cut-over WITH septic tank abandonment -> septic_ossf; plain city-sewer connection -> plumbing.

For each permit below, the "label" field is the machine's pick. Output STRICT JSON: a list of objects, one per permit, each {{"id": "...", "verdict": "agree"|"disagree", "better": "<category key or null>", "reason": "<one line, only when disagree>"}}.
Output ONLY the JSON list, no markdown fences, no commentary.

Permits:
{permits}
"""


def main() -> None:
    cands = {json.loads(l)["id"]: json.loads(l) for l in open(CANDS)}
    labels = {json.loads(l)["id"]: json.loads(l) for l in open(QWEN)}
    tax = load_taxonomy()
    keys = set(category_keys(tax))
    cats = "\n".join(f"- {c['key']}: {c['description']}" for c in tax["categories"])

    done: set[str] = set()
    if OUT.exists():
        done = {json.loads(l)["id"] for l in open(OUT)}

    items = [
        {
            "id": cid,
            "permit_type": c.get("permit_type"),
            "description": (c.get("description_raw") or "")[:600],
            "declared_value": c.get("declared_value"),
            "label": labels[cid]["category"],
        }
        for cid, c in cands.items()
        if cid in labels and cid not in done
    ]
    print(f"{len(items)} to critique ({len(done)} already done)")

    with open(OUT, "a") as f:
        for i in range(0, len(items), BATCH):
            chunk = items[i : i + BATCH]
            prompt = PROMPT_TMPL.format(cats=cats, permits=json.dumps(chunk, ensure_ascii=False, indent=0))
            t0 = time.time()
            res = subprocess.run(
                ["claude", "-p", prompt],
                capture_output=True, text=True, timeout=600,
            )
            raw = res.stdout.strip()
            if raw.startswith("```"):
                raw = "\n".join(l for l in raw.splitlines() if not l.strip().startswith("```"))
            try:
                start, end = raw.index("["), raw.rindex("]") + 1
                verdicts = json.loads(raw[start:end])
            except (ValueError, json.JSONDecodeError) as e:
                print(f"batch {i//BATCH}: parse failure ({e}); stdout head: {raw[:300]!r}; stderr: {res.stderr[:200]!r}")
                continue
            got = 0
            for v in verdicts:
                vid = str(v.get("id", ""))
                if vid not in {c["id"] for c in chunk}:
                    continue
                better = v.get("better")
                if better is not None and better not in keys:
                    better = None
                f.write(json.dumps({
                    "id": vid,
                    "verdict": v.get("verdict", "agree"),
                    "better": better,
                    "reason": (v.get("reason") or "").strip(),
                }, ensure_ascii=False) + "\n")
                got += 1
            f.flush()
            print(f"batch {i//BATCH}: {got}/{len(chunk)} verdicts in {time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
