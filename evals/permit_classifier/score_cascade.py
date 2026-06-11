"""Offline cascade scorer: rules layer + cached LLM predictions.

Scores a rules+LLM cascade against eval_set_v1.jsonl WITHOUT calling the
model: rows the rules layer claims are scored on the rule's answer; every
other row is scored on a cached per-row LLM prediction file (pure-LLM preds,
e.g. preds_qwen3.5-35b_prompt_v4.jsonl). This lets rules iterations run in
milliseconds and keeps the GPU out of the loop.

Gate (same as run_eval.py): >=90% overall AND no category with >=20 eval
examples below 80%.

Usage:
  python3 score_cascade.py                          # score, print report
  python3 score_cascade.py --preds preds_qwen3.5-35b_prompt_v4.jsonl
  python3 score_cascade.py --write-gate             # on PASS, insert gate row
  python3 score_cascade.py --out scores_rules_v2_cascade.md

--write-gate inserts (version, overall_accuracy, eval_report) into
canonical.classifier_gate so the production bulk runner's gate guard accepts
the new rules-prefixed classifier version. Only runs on a passing gate.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "scripts"))
from permit_classifier_lib import (  # noqa: E402
    RULES_VERSION, classifier_version, load_taxonomy, pre_classify,
)

EVAL = HERE / "eval_set_v1.jsonl"
DEFAULT_PREDS = HERE / "preds_qwen3.5-35b_prompt_v4.jsonl"
GATE_DSN = os.environ.get("ENRICH_DB_DSN", "host=100.122.216.15 port=5432 dbname=permits user=will")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--preds", default=str(DEFAULT_PREDS),
                    help="pure-LLM per-row predictions jsonl (id, category)")
    ap.add_argument("--out", default="")
    ap.add_argument("--write-gate", action="store_true")
    args = ap.parse_args()

    rows = [json.loads(l) for l in open(EVAL)]
    preds = {}
    for l in open(args.preds):
        d = json.loads(l)
        preds[d["id"]] = d

    missing = [r["id"] for r in rows if r["id"] not in preds]
    if missing:
        print(f"WARNING: {len(missing)} eval rows have no cached LLM prediction; "
              "they are scored only if a rule fires.")

    per_cat: dict[str, list[int]] = defaultdict(list)
    confusions: list[dict] = []
    rule_wrong: list[dict] = []
    n_ok = n_tot = n_rules = 0
    for r in rows:
        rule_cat = pre_classify(r)
        if rule_cat is not None:
            pred_cat, src = rule_cat, "rules"
            n_rules += 1
        elif r["id"] in preds:
            pred_cat, src = preds[r["id"]]["category"], "llm"
        else:
            continue
        ok = int(pred_cat == r["label"])
        per_cat[r["label"]].append(ok)
        n_ok += ok
        n_tot += 1
        if not ok:
            c = {"id": r["id"], "label": r["label"], "pred": pred_cat, "src": src,
                 "permit_type": r["permit_type"],
                 "desc": (r.get("description_raw") or "")[:120]}
            confusions.append(c)
            if src == "rules":
                rule_wrong.append(c)

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

    tax = load_taxonomy()
    version = f"{RULES_VERSION}+{classifier_version(tax)}"
    preds_name = Path(args.preds).name
    print(f"cascade {version}  (LLM preds: {preds_name})")
    print(f"rules handled {n_rules}/{n_tot} ({n_rules/n_tot:.1%}), "
          f"rules WRONG on {len(rule_wrong)}")
    print(f"OVERALL {overall:.2%} ({n_ok}/{n_tot})  "
          f"gate={'PASS' if gate_pass else 'FAIL'}  fails={gate_cat_fail}")
    for c in rule_wrong:
        print(f"  RULE MISS {c['label']} -> {c['pred']}: [{c['permit_type']}] {c['desc']}")

    if args.out:
        md = [
            f"# Permit Classifier Cascade Eval — {version}",
            f"\nRun: {datetime.now(timezone.utc).isoformat()}  ",
            f"Eval set: {EVAL.name} ({n_tot} scored of {len(rows)})  ",
            f"LLM predictions: {preds_name} (offline, cached)  ",
            f"Rules layer handled: {n_rules}/{n_tot} ({n_rules/n_tot:.1%})  ",
            f"\n## Overall: {overall:.2%} ({n_ok}/{n_tot}) — gate {'PASS' if gate_pass else 'FAIL'}",
            f"\nGate: >=90% overall AND no category with >=20 examples below 80%."
            f" Failing categories: {gate_cat_fail or 'none'}",
            "\n## Per-category\n\n| category | n | accuracy |\n|---|---|---|",
            *lines,
            "\n## Misclassifications\n",
        ]
        for c in confusions:
            md.append(f"- `{c['label']}` -> `{c['pred']}` ({c['src']}): "
                      f"[{c['permit_type']}] {c['desc']}")
        Path(args.out).write_text("\n".join(md) + "\n")
        print(f"report -> {args.out}")

    if args.write_gate:
        if not gate_pass:
            print("gate FAIL: refusing to write gate row")
            sys.exit(1)
        import psycopg
        report_rel = f"evals/permit_classifier/{Path(args.out).name}" if args.out else ""
        with psycopg.connect(GATE_DSN, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO canonical.classifier_gate (version, overall_accuracy, eval_report) "
                "VALUES (%s,%s,%s) ON CONFLICT (version) DO UPDATE SET "
                "overall_accuracy=EXCLUDED.overall_accuracy, eval_report=EXCLUDED.eval_report, gated_at=now()",
                (version, round(overall, 4), report_rel),
            )
        print(f"gate row written: {version} @ {overall:.2%}")

    sys.exit(0 if gate_pass else 1)


if __name__ == "__main__":
    main()
