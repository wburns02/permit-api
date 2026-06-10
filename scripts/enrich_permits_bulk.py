"""Bulk TX permit classification — Rig & Permit Radar Phase 4.

Classifies canonical.permits (TX universe, ~3.8M rows) with qwen3.5:122b via
Ollama into canonical.permit_enrichment. Recent permits first
(ORDER BY issued_date DESC, permit_id DESC), keyset-paginated, checkpointed in
canonical.enrichment_progress so a crash/restart resumes from the last
committed batch. Rows with NULL issued_date (~27K) are processed in a final
phase keyed by permit_id.

TX universe = jurisdiction.state='TX' AND source_id IN
canonical.enrichment_tx_sources (see permit_enrichment_schema.sql for why the
raw state='TX' label cannot be trusted).

Run on R730 (so Ollama is local):
  systemd-run --user --unit=permit-enrich-bulk \
    --working-directory=/home/will/permit-api-live/scripts \
    -E OLLAMA_URL=http://127.0.0.1:11434 \
    python3 enrich_permits_bulk.py --run-id tx_bulk_v1

Logs tokens/sec + rows/min + ETA every 100 LLM batches.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import httpx
import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, str(Path(__file__).resolve().parent))
from permit_classifier_lib import (  # noqa: E402
    _RULES_PREFIX, build_system_prompt, classify_batch, classifier_version,
    load_taxonomy, pre_classify,
)

DSN = os.environ.get("ENRICH_DB_DSN", "host=100.122.216.15 port=5432 dbname=permits user=will")
LLM_BATCH = int(os.environ.get("ENRICH_LLM_BATCH", "8"))
DB_CHUNK = int(os.environ.get("ENRICH_DB_CHUNK", "320"))
LOG_EVERY = 100  # LLM batches
NULL_DATE_SENTINEL = "0001-01-01"  # cursor value marking the NULL-date phase

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger("enrich_bulk")

TX_FILTER = """
  p.jurisdiction_id IN (SELECT id FROM canonical.jurisdictions WHERE state='TX')
  AND p.source_id IN (SELECT source_id FROM canonical.enrichment_tx_sources)
"""

FETCH_DATED = f"""
SELECT p.permit_id AS id, p.source_id, p.source_record_id, p.permit_type,
       p.description_raw, p.address_raw, p.declared_value, p.issued_date
FROM canonical.permits p
WHERE {TX_FILTER}
  AND p.issued_date IS NOT NULL
  AND (p.issued_date, p.permit_id) < (%(d)s::date, %(pid)s::uuid)
ORDER BY p.issued_date DESC, p.permit_id DESC
LIMIT %(lim)s
"""

FETCH_NULL = f"""
SELECT p.permit_id AS id, p.source_id, p.source_record_id, p.permit_type,
       p.description_raw, p.address_raw, p.declared_value, p.issued_date
FROM canonical.permits p
WHERE {TX_FILTER}
  AND p.issued_date IS NULL
  AND p.permit_id < %(pid)s::uuid
ORDER BY p.permit_id DESC
LIMIT %(lim)s
"""

def _retry_singly(batch, sp, tax, client, stats) -> dict:
    """Per-row fallback after a batch-level model failure. Connection errors
    back off and retry the same row (never skip rows on outages); content
    errors are logged and the row is skipped (counted as failed upstream)."""
    results: dict[str, dict] = {}
    for r in batch:
        while True:
            try:
                single, st = classify_batch([r], system_prompt=sp, tax=tax, client=client)
                results.update(single)
                stats["output_tokens"] = stats.get("output_tokens", 0) + st.get("output_tokens", 0)
                break
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
                log.warning("Ollama unreachable in single retry (%s); backing off 60s", e)
                time.sleep(60)
            except Exception as e:  # noqa: BLE001
                log.error("permit %s failed permanently: %s", r["id"], e)
                break
    return results


UPSERT = """
INSERT INTO canonical.permit_enrichment
  (permit_id, source_id, source_record_id, category, category_confidence,
   summary, classifier_version)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (source_id, source_record_id) DO NOTHING
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", default="tx_bulk_v1")
    ap.add_argument("--max-rows", type=int, default=0, help="stop after N rows (testing)")
    args = ap.parse_args()

    tax = load_taxonomy()
    sp = build_system_prompt(tax)
    cv = classifier_version(tax)

    # autocommit: never hold a transaction open across an LLM call (the
    # warehouse enforces idle_in_transaction_session_timeout). Writes use
    # short explicit transactions per batch.
    conn = psycopg.connect(DSN, row_factory=dict_row, autocommit=True)
    conn.execute("SET statement_timeout = '600s'")

    # GATE GUARD: production enrichment only runs classifier versions that
    # passed the eval gate (canonical.classifier_gate, written by a passing
    # eval run). Prevents un-gated prompt/model changes from writing rows.
    for v in (cv, f"rules_v1+{cv}"):
        if not conn.execute(
            "SELECT 1 FROM canonical.classifier_gate WHERE version=%s", (v,)
        ).fetchone():
            log.error("classifier version %r has NOT passed the eval gate; refusing to run", v)
            sys.exit(2)

    # --- resume or init progress row
    prog = conn.execute(
        "SELECT * FROM canonical.enrichment_progress WHERE run_id=%s", (args.run_id,)
    ).fetchone()
    if prog:
        cursor_date = prog["last_issued_date"]
        cursor_pid = prog["last_permit_id"]
        rows_done = prog["rows_done"]
        rows_failed = prog["rows_failed"]
        log.info("resuming run %s at (%s, %s), %d done", args.run_id, cursor_date, cursor_pid, rows_done)
    else:
        cursor_date = "9999-12-31"
        cursor_pid = "ffffffff-ffff-ffff-ffff-ffffffffffff"
        rows_done = rows_failed = 0
        conn.execute(
            "INSERT INTO canonical.enrichment_progress (run_id, classifier_version, last_issued_date, last_permit_id) "
            "VALUES (%s,%s,%s,%s)",
            (args.run_id, cv, cursor_date, cursor_pid),
        )
        log.info("new run %s [%s]", args.run_id, cv)

    remaining = conn.execute(
        f"""SELECT count(*) AS n FROM canonical.permits p
            LEFT JOIN canonical.permit_enrichment e
              ON e.source_id=p.source_id AND e.source_record_id=p.source_record_id
            WHERE {TX_FILTER} AND e.source_id IS NULL"""
    ).fetchone()["n"]
    log.info("rows remaining to enrich: %d", remaining)

    null_phase = str(cursor_date) == NULL_DATE_SENTINEL
    batch_count = 0
    window_rows = 0
    window_out_tokens = 0
    window_t0 = time.time()

    client = httpx.Client()
    session_done = 0
    while True:
        if args.max_rows and session_done >= args.max_rows:
            log.info("hit --max-rows, stopping")
            break
        if not null_phase:
            chunk = conn.execute(FETCH_DATED, {"d": cursor_date, "pid": cursor_pid, "lim": DB_CHUNK}).fetchall()
            if not chunk:
                log.info("dated phase complete; switching to NULL issued_date phase")
                null_phase = True
                cursor_date = NULL_DATE_SENTINEL
                cursor_pid = "ffffffff-ffff-ffff-ffff-ffffffffffff"
                continue
        else:
            chunk = conn.execute(FETCH_NULL, {"pid": cursor_pid, "lim": DB_CHUNK}).fetchall()
            if not chunk:
                log.info("ALL DONE: no rows left")
                break

        # skip rows already enriched (restart overlap)
        existing = set()
        if chunk:
            sids = [r["source_id"] for r in chunk]
            srids = [r["source_record_id"] for r in chunk]
            res = conn.execute(
                "SELECT e.source_id, e.source_record_id FROM canonical.permit_enrichment e "
                "JOIN unnest(%s::text[], %s::text[]) AS k(sid, srid) "
                "ON e.source_id = k.sid AND e.source_record_id = k.srid",
                (sids, srids),
            ).fetchall()
            existing = {(r["source_id"], r["source_record_id"]) for r in res}

        for i in range(0, len(chunk), LLM_BATCH):
            full_batch = chunk[i : i + LLM_BATCH]
            batch = [r for r in full_batch
                     if (r["source_id"], r["source_record_id"]) not in existing]

            # Pre-classify deterministic rules before calling LLM.
            rules_hits: dict[str, dict] = {}
            llm_batch = []
            for r in batch:
                cat = pre_classify(r)
                if cat is not None:
                    desc_snippet = (r.get("description_raw") or "")[:80]
                    ptype = r.get("permit_type") or ""
                    summary = f"{ptype}: {desc_snippet}" if desc_snippet else ptype
                    rules_hits[str(r["id"])] = {
                        "category": cat,
                        "confidence": 1.0,
                        "summary": summary[:600],
                        "classifier_version": _RULES_PREFIX + cv,
                    }
                else:
                    llm_batch.append(r)

            results: dict[str, dict] = {}
            stats: dict = {}
            if llm_batch:
                # Connection-level failures mean Ollama is down/restarting:
                # back off WITHOUT advancing the cursor, forever. A 31K-row
                # cursor skip happened on 2026-06-10 when Ollama briefly
                # served the wrong model store; never burn rows on outages.
                while True:
                    try:
                        results, stats = classify_batch(llm_batch, system_prompt=sp, tax=tax, client=client)
                        break
                    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
                        log.warning("Ollama unreachable (%s); backing off 60s, cursor held", e)
                        time.sleep(60)
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 404:
                            log.warning("model missing (404: %s); backing off 120s, cursor held",
                                        e.response.text[:120])
                            time.sleep(120)
                            continue
                        log.warning("batch failed (%s); retrying singly", e)
                        results = _retry_singly(llm_batch, sp, tax, client, stats)
                        break
                    except Exception as e:  # noqa: BLE001
                        log.warning("batch failed (%s); retrying singly", e)
                        results = _retry_singly(llm_batch, sp, tax, client, stats)
                        break
            # Merge rules + LLM results
            for rid, res in rules_hits.items():
                results[rid] = res
            # checkpoint: cursor = last row of this LLM batch (ordered DESC)
            last = full_batch[-1]
            if null_phase:
                cursor_pid = last["id"]
            else:
                cursor_date, cursor_pid = last["issued_date"], last["id"]
            with conn.transaction():
                for r in batch:
                    res = results.get(str(r["id"]))
                    if res:
                        row_cv = res.get("classifier_version", cv)
                        conn.execute(UPSERT, (
                            r["id"], r["source_id"], r["source_record_id"],
                            res["category"], res["confidence"], res["summary"], row_cv,
                        ))
                        rows_done += 1
                        session_done += 1
                        window_rows += 1
                    else:
                        rows_failed += 1
                conn.execute(
                    "UPDATE canonical.enrichment_progress SET last_issued_date=%s, last_permit_id=%s, "
                    "rows_done=%s, rows_failed=%s, updated_at=now() WHERE run_id=%s",
                    (str(cursor_date), cursor_pid, rows_done, rows_failed, args.run_id),
                )
            window_out_tokens += stats.get("output_tokens", 0)
            batch_count += 1
            if batch_count % LOG_EVERY == 0:
                el = time.time() - window_t0
                rpm = window_rows / el * 60 if el else 0
                eta_days = (remaining - session_done) / (rpm * 60 * 24) if rpm else -1
                log.info(
                    "progress: %d rows this session (%d total), %.1f rows/min, %.1f out-tok/s, "
                    "remaining %d, ETA %.1f days, cursor=(%s)",
                    session_done, rows_done, rpm, window_out_tokens / el if el else 0,
                    remaining - session_done, eta_days, cursor_date,
                )
                window_rows = 0
                window_out_tokens = 0
                window_t0 = time.time()

    conn.close()


if __name__ == "__main__":
    main()
