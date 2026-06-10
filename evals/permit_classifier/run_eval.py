"""Score the classifier prompt against eval_set_v1.jsonl with qwen3.5:122b.

Gate: >=90% overall accuracy AND no category with >=20 eval examples below 80%.

Usage: python3 run_eval.py [--limit N] [--out scores_v1.md]
Predictions are cached per prompt version in preds_<PROMPT_VERSION>.jsonl so
re-scoring after a crash doesn't re-run the model.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import httpx

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "scripts"))
from permit_classifier_lib import (  # noqa: E402
    PROMPT_VERSION, build_system_prompt, classify_batch, classifier_version, load_taxonomy,
)

EVAL = HERE / "eval_set_v1.jsonl"
BATCH = 8


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--out", default=str(HERE / "scores_v1.md"))
    args = ap.parse_args()

    rows = [json.loads(l) for l in open(EVAL)]
    if args.limit:
        rows = rows[: args.limit]
    preds_path = HERE / f"preds_{PROMPT_VERSION}.jsonl"
    preds: dict[str, dict] = {}
    if preds_path.exists():
        preds = {json.loads(l)["id"]: json.loads(l) for l in open(preds_path)}
    todo = [r for r in rows if r["id"] not in preds]
    print(f"eval set {len(rows)}, cached {len(rows)-len(todo)}, to run {len(todo)} [{PROMPT_VERSION}]")

    tax = load_taxonomy()
    # Category gate only: skip summaries for ~3x throughput. Matches how the
    # eval labels were generated (no-summary wire format).
    sp = build_system_prompt(tax, include_summary=False)
    t0 = time.time()
    with httpx.Client() as client, open(preds_path, "a") as f:
        for i in range(0, len(todo), BATCH):
            chunk = todo[i : i + BATCH]
            try:
                results, _ = classify_batch(chunk, system_prompt=sp, tax=tax, client=client, include_summary=False)
            except Exception as e:  # noqa: BLE001
                print(f"batch {i//BATCH} failed: {e}; singles")
                results = {}
                for r in chunk:
                    try:
                        s, _ = classify_batch([r], system_prompt=sp, tax=tax, client=client, include_summary=False)
                        results.update(s)
                    except Exception as e2:  # noqa: BLE001
                        print(f"  {r['id']}: {e2}")
            for r in chunk:
                if r["id"] in results:
                    rec = {"id": r["id"], **results[r["id"]]}
                    preds[r["id"]] = rec
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            done_n = min(i + BATCH, len(todo))
            el = time.time() - t0
            print(f"[{done_n}/{len(todo)}] {el:.0f}s, {done_n/el*60:.1f} permits/min", flush=True)

    # Score
    per_cat: dict[str, list[int]] = defaultdict(list)
    confusions: list[dict] = []
    n_ok = n_tot = 0
    for r in rows:
        p = preds.get(r["id"])
        if not p:
            continue
        ok = int(p["category"] == r["label"])
        per_cat[r["label"]].append(ok)
        n_ok += ok
        n_tot += 1
        if not ok:
            confusions.append({"id": r["id"], "label": r["label"], "pred": p["category"],
                               "permit_type": r["permit_type"],
                               "desc": (r.get("description_raw") or "")[:120]})
    overall = n_ok / n_tot if n_tot else 0.0
    gate_cat_fail = []
    lines = []
    for cat in sorted(per_cat, key=lambda c: -len(per_cat[c])):
        oks = per_cat[cat]
        acc = sum(oks) / len(oks)
        flag = ""
        if len(oks) >= 20 and acc < 0.80:
            gate_cat_fail.append(cat)
            flag = "  <-- GATE FAIL"
        lines.append(f"| {cat} | {len(oks)} | {acc:.1%}{flag} |")
    gate_pass = overall >= 0.90 and not gate_cat_fail

    md = [
        f"# Permit Classifier Eval — {classifier_version(tax)}",
        f"\nRun: {datetime.now(timezone.utc).isoformat()}  ",
        f"Eval set: {EVAL.name} ({n_tot} scored of {len(rows)})  ",
        f"\n## Overall: {overall:.2%} ({n_ok}/{n_tot}) — gate {'PASS' if gate_pass else 'FAIL'}",
        f"\nGate: >=90% overall AND no category with >=20 examples below 80%."
        f" Failing categories: {gate_cat_fail or 'none'}",
        "\n## Per-category\n\n| category | n | accuracy |\n|---|---|---|",
        *lines,
        "\n## Misclassifications\n",
    ]
    for c in confusions:
        md.append(f"- `{c['label']}` -> `{c['pred']}`: [{c['permit_type']}] {c['desc']}")
    Path(args.out).write_text("\n".join(md) + "\n")
    print(f"\nOVERALL {overall:.2%}  gate={'PASS' if gate_pass else 'FAIL'}  fails={gate_cat_fail}")
    print(f"report -> {args.out}")
    sys.exit(0 if gate_pass else 1)


if __name__ == "__main__":
    main()
