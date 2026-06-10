"""Build the final eval set and score the classifier against it.

Inputs:  eval_candidates.jsonl (500 sampled TX permits)
         qwen_labels.jsonl     (candidate labels, category-only pass)
         critique.jsonl        (claude -p verdicts: agree | disagree+better)

Resolution rule (documented per sprint Phase 4 task 1):
  - agree            -> qwen label becomes the eval label
  - disagree+better  -> the critique's category becomes the eval label
                        (the critic sees the same fields and is the stronger
                        model; spot-check disagreements in the report)
  - disagree, no better / missing critique -> EXCLUDED, logged with reason

Then scores the classifier (a FRESH classification pass over the eval set,
so the gate measures the prompt, not label memorization) and writes
eval_set_v1.jsonl + scores_v1.md.

Gate: >=90% overall AND no category with >=20 eval examples below 80%.

Usage: python3 score_eval.py [--skip-classify]  (run from repo root)
"""
import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import httpx

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "scripts"))
from permit_classifier_lib import (  # noqa: E402
    build_system_prompt, classify_batch, load_taxonomy, category_keys,
)

BATCH = 8
PROMPT_VERSION = "prompt_v1"


def load_jsonl(p):
    return [json.loads(l) for l in open(p) if l.strip()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-classify", action="store_true",
                    help="score an existing predictions.jsonl instead of re-running")
    args = ap.parse_args()

    candidates = {r["id"]: r for r in load_jsonl(HERE / "eval_candidates.jsonl")}
    labels = {r["id"]: r for r in load_jsonl(HERE / "qwen_labels.jsonl")}
    critiques = {r["id"]: r for r in load_jsonl(HERE / "critique.jsonl")}

    final, excluded = [], []
    for pid, row in candidates.items():
        lab = labels.get(pid)
        crit = critiques.get(pid)
        if not lab:
            excluded.append({"id": pid, "reason": "no qwen label"})
            continue
        if not crit:
            excluded.append({"id": pid, "reason": "no critique verdict"})
            continue
        if crit["verdict"] == "agree":
            label, prov = lab["category"], "qwen+critic_agree"
        elif crit.get("better"):
            label, prov = crit["better"], f"critic_override: {crit.get('reason','')[:120]}"
        else:
            excluded.append({"id": pid, "reason": f"disagree without alternative: {crit.get('reason','')[:120]}"})
            continue
        final.append({**row, "eval_label": label, "label_provenance": prov})

    with open(HERE / "eval_set_v1.jsonl", "w") as f:
        for r in final:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    with open(HERE / "exclusions_v1.jsonl", "w") as f:
        for r in excluded:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    overrides = sum(1 for r in final if r["label_provenance"].startswith("critic_override"))
    print(f"eval set: {len(final)} rows ({overrides} critic overrides), {len(excluded)} excluded")

    # fresh classification pass (category-only) against the eval set
    pred_path = HERE / "predictions_v1.jsonl"
    if not args.skip_classify:
        tax = load_taxonomy()
        sp = build_system_prompt(tax)
        preds = {}
        t0 = time.time()
        with httpx.Client() as client, open(pred_path, "w") as f:
            for i in range(0, len(final), BATCH):
                chunk = final[i:i + BATCH]
                results, _ = classify_batch(
                    chunk, system_prompt=sp, tax=tax, client=client,
                    include_summary=False)
                for r in chunk:
                    if r["id"] in results:
                        rec = {"id": r["id"], **results[r["id"]]}
                        preds[r["id"]] = rec
                        f.write(json.dumps(rec) + "\n")
                print(f"[classify {min(i+BATCH,len(final))}/{len(final)}] "
                      f"{(time.time()-t0):.0f}s", flush=True)
    preds = {r["id"]: r for r in load_jsonl(pred_path)}

    # score
    per_cat = defaultdict(lambda: [0, 0])  # label -> [correct, total]
    overall = [0, 0]
    confusion = defaultdict(int)
    for r in final:
        p = preds.get(r["id"])
        if not p:
            continue
        ok = p["category"] == r["eval_label"]
        per_cat[r["eval_label"]][0] += ok
        per_cat[r["eval_label"]][1] += 1
        overall[0] += ok
        overall[1] += 1
        if not ok:
            confusion[(r["eval_label"], p["category"])] += 1

    acc = overall[0] / overall[1] if overall[1] else 0
    major_fail = [
        (c, n[0] / n[1], n[1]) for c, n in per_cat.items()
        if n[1] >= 20 and n[0] / n[1] < 0.80
    ]
    gate = acc >= 0.90 and not major_fail

    lines = [
        f"# Permit Classifier Eval Scores ({PROMPT_VERSION})",
        "",
        f"- Eval set: {len(final)} rows ({overrides} critic overrides, {len(excluded)} excluded)",
        f"- Scored: {overall[1]}  Overall accuracy: **{acc:.1%}**",
        f"- Gate (>=90% overall, no >=20-example category <80%): **{'PASS' if gate else 'FAIL'}**",
        "",
        "| Category | n | Accuracy |",
        "|---|---|---|",
    ]
    for c, (corr, tot) in sorted(per_cat.items(), key=lambda kv: -kv[1][1]):
        lines.append(f"| {c} | {tot} | {corr/tot:.1%} |")
    if confusion:
        lines += ["", "## Top confusions (eval_label -> predicted)", ""]
        for (a, b), n in sorted(confusion.items(), key=lambda kv: -kv[1])[:15]:
            lines.append(f"- {a} -> {b}: {n}")
    (HERE / "scores_v1.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines[:8]))
    print(f"\ngate: {'PASS' if gate else 'FAIL'}")
    sys.exit(0 if gate else 1)


if __name__ == "__main__":
    main()
