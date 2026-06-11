"""Generate candidate labels for the eval set with qwen3.5:122b.

Reads eval_candidates.jsonl, writes qwen_labels.jsonl (id, category,
confidence, summary). Resumable: skips ids already present in the output.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import httpx

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent / "scripts"))
from permit_classifier_lib import build_system_prompt, classify_batch, load_taxonomy  # noqa: E402

IN = HERE / "eval_candidates.jsonl"
OUT = HERE / "qwen_labels.jsonl"
BATCH = 8


def main() -> None:
    rows = [json.loads(l) for l in open(IN)]
    done: set[str] = set()
    if OUT.exists():
        done = {json.loads(l)["id"] for l in open(OUT)}
    todo = [r for r in rows if r["id"] not in done]
    print(f"{len(rows)} total, {len(done)} done, {len(todo)} to label")

    tax = load_taxonomy()
    sp = build_system_prompt(tax, include_summary=False)
    t0 = time.time()
    n_out_tokens = 0
    with httpx.Client() as client, open(OUT, "a") as f:
        for i in range(0, len(todo), BATCH):
            chunk = todo[i : i + BATCH]
            stats: dict = {}  # stays bound even if every call below fails
            try:
                results, stats = classify_batch(chunk, system_prompt=sp, tax=tax, client=client, include_summary=False)
            except Exception as e:  # noqa: BLE001
                print(f"batch {i//BATCH} failed: {e}; retrying singly", flush=True)
                results = {}
                for r in chunk:
                    try:
                        single, stats = classify_batch([r], system_prompt=sp, tax=tax, client=client, include_summary=False)
                        results.update(single)
                    except Exception as e2:  # noqa: BLE001
                        print(f"  single {r['id']} failed: {e2}", flush=True)
                        time.sleep(10)  # let a reloading model settle
            n_out_tokens += stats.get("output_tokens", 0)
            missing = [r["id"] for r in chunk if r["id"] not in results]
            if missing:
                print(f"batch {i//BATCH}: {len(missing)} ids missing from output: {missing}")
            for r in chunk:
                if r["id"] in results:
                    rec = {"id": r["id"], **results[r["id"]]}
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            el = time.time() - t0
            done_n = min(i + BATCH, len(todo))
            print(
                f"[{done_n}/{len(todo)}] {el:.0f}s elapsed, "
                f"{done_n/el*60:.1f} permits/min, {n_out_tokens/el:.1f} out-tok/s",
                flush=True,
            )


if __name__ == "__main__":
    main()
