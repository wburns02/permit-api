"""Measure rules-layer coverage on a random sample of unenriched TX permits.

Input: data/rules_v2_sample.csv (permit_id, permit_type, description_raw),
pulled from the unenriched TX universe. Prints overall hit rate plus the
top uncovered permit_type prefixes so the next rule to write is obvious.

Usage: python3 coverage_sample.py [--top 25] [--by-rule]
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "scripts"))
from permit_classifier_lib import RULES_VERSION, pre_classify  # noqa: E402

SAMPLE = HERE.parent.parent / "data" / "rules_v2_sample.csv"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--by-rule", action="store_true",
                    help="show hit counts per returned category")
    args = ap.parse_args()

    n = hits = 0
    uncovered = Counter()
    by_cat = Counter()
    with open(SAMPLE) as f:
        for row in csv.DictReader(f):
            n += 1
            cat = pre_classify({
                "permit_type": row["permit_type"],
                "description_raw": row["description_raw"],
            })
            if cat is not None:
                hits += 1
                by_cat[cat] += 1
            else:
                prefix = (row["permit_type"] or "(empty)").split("/")[0].strip()[:50]
                uncovered[prefix or "(empty)"] += 1

    print(f"{RULES_VERSION}: {hits}/{n} covered ({hits/n:.1%}); "
          f"{n - hits} deferred to LLM")
    if args.by_rule:
        print("\nhits by category:")
        for cat, c in by_cat.most_common():
            print(f"  {cat:24s} {c:7d}")
    print(f"\ntop {args.top} uncovered permit_type prefixes:")
    for prefix, c in uncovered.most_common(args.top):
        print(f"  {c:7d}  {prefix}")


if __name__ == "__main__":
    main()
