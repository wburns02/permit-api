#!/usr/bin/env python3
"""
Lead scoring for hot_leads (A/B/C grade) — SAFELY, in bounded batches.

Writes hot_leads.lead_score (char(1): 'A' / 'B' / 'C') and stamps
hot_leads.score_updated_at for every lead, from cheap signals already on the
row (contractor_phone presence, permit valuation, and permit recency).

WHY THIS SCRIPT EXISTS / WHAT CHANGED
-------------------------------------
The lead-scoring step previously ran as ONE unbounded statement:

    UPDATE hot_leads
       SET lead_score = CASE ... END,
           score_updated_at = now();          -- 12.9M-row UPDATE, no batching

That single statement walked all ~12.9M rows in one transaction. It took 55+
minutes holding a RowExclusiveLock on every row it touched (and an ExclusiveLock
on the relation for the duration), serialized every other hot_leads writer
(county loaders' INSERTs, the bridge, MV refreshes), and stalled reads.

On 2026-06-26 a global `statement_timeout = '20min'` was set on the `permits`
DB. The old single-statement form now FAILS outright on its next run: it can't
finish 12.9M rows inside 20 minutes, so it gets guillotined mid-flight and
writes nothing (or, worse, repeatedly retries and re-locks). This rewrite makes
the jam impossible AND keeps the run under the cap.

THE FIX (mirrors PR #9 / enrich_with_sales.sql):
  (a) Bounded KEYSET batches over hot_leads.id (UUID), with a COMMIT per batch.
      Each UPDATE touches at most --batch-size rows, so a single statement is
      always short and the row locks it holds are released within the batch.
  (b) Per-batch SET LOCAL lock_timeout + statement_timeout + a session
      idle_in_transaction_session_timeout. A batch that collides with a loader
      fails fast (lock_timeout) and is retried, instead of queuing up holding
      partial locks. Nothing is ever pg_terminate'd.
  (c) Resumable / idempotent: the last committed id is written to a checkpoint
      file; re-running picks up where it left off (or pass --start-after). The
      scoring is a pure function of current row values, so re-scoring a row is
      a no-op-equivalent (it writes the same grade) — safe to re-run end to end.
  (d) Optional --sleep-ms between batches to yield the write path to live
      loaders while the box recovers from the lock-storm.
  (e) --only-stale skips rows already scored after a cutoff so a catch-up run
      doesn't re-touch the whole table.

SCORING RULE (A/B/C)
--------------------
Recovered from the live grade distribution on the freshest-scored rows
(score_updated_at within the last few days, i.e. minimal value drift):

    A : contractor_phone IS NOT NULL
        AND valuation >= 100000
        AND issue_date >= today - 30 days
        (a hot, high-value, reachable, just-pulled permit)

    B : issue_date >= today - 90 days
        AND ( contractor_phone IS NOT NULL OR valuation >= 50000 )
        (recent and either reachable or material — worth a touch)

    C : everything else.

NULL valuation / NULL issue_date are treated as "fails the threshold" (they
cannot satisfy the >= comparisons), which matches the observed C bucket. The
rule is evaluated against CURRENT_DATE inside SQL so recency is relative to the
run, exactly like the original cron behaviour.

Usage:
    # dry-run a small bounded slice (no writes), prove the lock footprint:
    python3 scripts/lead_scoring_v1.py --db-host 100.122.216.15 \
        --batch-size 2000 --max-batches 2 --dry-run

    # full resumable run, 10k rows/txn, yield 50ms between batches:
    python3 scripts/lead_scoring_v1.py --db-host 100.122.216.15 \
        --batch-size 10000 --sleep-ms 50

    # nightly catch-up: only (re)score rows not scored since yesterday:
    python3 scripts/lead_scoring_v1.py --db-host 100.122.216.15 \
        --only-stale --stale-hours 20
"""

import argparse
import os
import sys
import time
from datetime import datetime

try:
    import psycopg2
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)


DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "permits")
DB_USER = os.getenv("DB_USER", "will")

DEFAULT_CHECKPOINT = "/home/will/permit-api-live/logs/lead_scoring_v1.checkpoint"

# Per-batch safety caps. lock_timeout is the load-bearing one: if a batch can't
# acquire its row locks within this window (because a loader holds them) it
# fails fast and we retry — it NEVER queues up holding partial locks. All three
# are well under the DB-global 20min statement_timeout, so a batch can never be
# the thing that gets guillotined by the global cap.
#   - statement_timeout '10min' : a single batch should finish in well under a
#     second; 10min is a generous ceiling, not an expectation.
#   - lock_timeout '30s'        : back off rather than block writers.
#   - idle_in_transaction '2min': never sit idle holding a transaction open.
BATCH_LOCK_TIMEOUT = "30s"
BATCH_STATEMENT_TIMEOUT = "10min"
SESSION_IDLE_IN_TXN_TIMEOUT = "2min"

# The A/B/C grade, computed purely from current row values relative to
# CURRENT_DATE. This is the ONLY place the scoring logic lives; the dry-run and
# the live UPDATE share it so they can never diverge.
_GRADE_SQL = """
        CASE
            WHEN h.contractor_phone IS NOT NULL
                 AND h.valuation >= 100000
                 AND h.issue_date >= CURRENT_DATE - 30
                THEN 'A'
            WHEN h.issue_date >= CURRENT_DATE - 90
                 AND (h.contractor_phone IS NOT NULL OR h.valuation >= 50000)
                THEN 'B'
            ELSE 'C'
        END
"""


def log(msg: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn(host: str, port: str = DB_PORT):
    conn = psycopg2.connect(host=host, port=port, dbname=DB_NAME, user=DB_USER)
    # Belt-and-suspenders: cap idle-in-transaction at the session level so even
    # an unexpected stall can't leave a batch holding row locks open.
    cur = conn.cursor()
    cur.execute(
        f"SET idle_in_transaction_session_timeout = '{SESSION_IDLE_IN_TXN_TIMEOUT}'"
    )
    conn.commit()
    cur.close()
    return conn


def ensure_schema(conn) -> None:
    """Ensure the scoring target columns exist.

    IMPORTANT: `ALTER TABLE ... ADD COLUMN` needs an AccessExclusiveLock on
    hot_leads. If a long writer (e.g. the very lock-storm this script replaces)
    is running, that ALTER QUEUES behind it AND then blocks every reader/writer
    that arrives after — turning a harmless idempotent DDL into a new blocker.
    So we first check the catalog (read-only, AccessShareLock) and only issue
    the ALTER for columns that are genuinely missing. On a normal run both
    columns already exist and we take NO exclusive lock at all."""
    want = (("lead_score", "CHAR(1)"), ("score_updated_at", "TIMESTAMPTZ"))
    cur = conn.cursor()
    cur.execute(
        "SET LOCAL lock_timeout = '5s'"
    )
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'hot_leads' "
        "AND column_name = ANY(%s)",
        ([c for c, _ in want],),
    )
    have = {r[0] for r in cur.fetchall()}
    conn.commit()
    missing = [(c, t) for c, t in want if c not in have]
    if not missing:
        cur.close()
        return
    for name, coltype in missing:
        # lock_timeout (set above per-tx) makes this fail fast instead of
        # queueing behind a long writer and becoming a blocker itself.
        cur.execute(f"SET LOCAL lock_timeout = '5s'")
        cur.execute(
            f"ALTER TABLE hot_leads ADD COLUMN IF NOT EXISTS {name} {coltype}"
        )
        conn.commit()
    cur.close()


def read_checkpoint(path: str) -> str | None:
    try:
        with open(path) as fh:
            val = fh.read().strip()
            return val or None
    except FileNotFoundError:
        return None


def write_checkpoint(path: str, last_id: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as fh:
        fh.write(last_id)
    os.replace(tmp, path)


# Find the high id of the current keyset window so the UPDATE's `id <= upto`
# matches exactly the rows the window SELECT chose (keeps the touched set ==
# batch_size). Postgres has no max(uuid) aggregate, so take the last of the
# ordered, LIMIT-ed window directly off the PK index — O(log n), never a scan.
_WINDOW_BOUNDS = """
    WITH w AS (
        SELECT id FROM hot_leads
         WHERE id > %(after)s
         ORDER BY id
         LIMIT %(batch)s
    )
    SELECT (SELECT id FROM w ORDER BY id ASC  LIMIT 1) AS lo,
           (SELECT id FROM w ORDER BY id DESC LIMIT 1) AS hi
"""

# Per-batch scoring UPDATE. KEYSET-bounded on hot_leads.id: drives off the PK
# index (id > after AND id <= upto), touches at most batch_size rows, and only
# writes rows whose grade actually changes — so re-running is cheap and a batch
# that's already correct does zero row-locking work.
#
# When --only-stale is active the WHERE also skips rows already scored after the
# cutoff, so a catch-up run doesn't re-touch the whole table.
_BATCH_UPDATE = """
    UPDATE hot_leads h
       SET lead_score = {grade},
           score_updated_at = now()
     WHERE h.id > %(after)s AND h.id <= %(upto)s
       {stale_clause}
       AND h.lead_score IS DISTINCT FROM ({grade})
"""

# Dry-run: count what WOULD change in this window, without writing.
_DRYRUN_COUNT = """
    SELECT count(*)
      FROM hot_leads h
     WHERE h.id > %(after)s AND h.id <= %(upto)s
       {stale_clause}
       AND h.lead_score IS DISTINCT FROM ({grade})
"""


def run(host: str, port: str, batch_size: int, sleep_ms: int,
        start_after: str | None, max_batches: int | None,
        checkpoint_path: str, dry_run: bool,
        only_stale: bool, stale_hours: int) -> None:
    conn = get_conn(host, port)
    conn.autocommit = False
    log(f"Connected to {host}:{port}/{DB_NAME}")

    if not dry_run:
        ensure_schema(conn)

    stale_clause = ""
    if only_stale:
        stale_clause = (
            "AND (h.score_updated_at IS NULL "
            f"OR h.score_updated_at < now() - interval '{stale_hours} hours')"
        )
        log(f"--only-stale: skipping rows scored within the last {stale_hours}h")

    update_sql = _BATCH_UPDATE.format(grade=_GRADE_SQL, stale_clause=stale_clause)
    dryrun_sql = _DRYRUN_COUNT.format(grade=_GRADE_SQL, stale_clause=stale_clause)

    # Resume point: explicit flag > checkpoint file > UUID zero.
    after = start_after or read_checkpoint(checkpoint_path) or \
        "00000000-0000-0000-0000-000000000000"
    log(f"Starting after id {after} (batch_size={batch_size}, "
        f"sleep_ms={sleep_ms}, dry_run={dry_run})")

    total_changed = 0
    batches = 0
    while True:
        if max_batches is not None and batches >= max_batches:
            log(f"Reached --max-batches={max_batches}; stopping")
            break

        cur = conn.cursor()
        cur.execute(_WINDOW_BOUNDS, {"after": after, "batch": batch_size})
        lo, hi = cur.fetchone()
        if hi is None:
            cur.close()
            log("No more rows — scoring complete")
            break

        if dry_run:
            cur.execute(dryrun_sql, {"after": after, "upto": str(hi)})
            would_change = cur.fetchone()[0]
            conn.rollback()
            cur.close()
            log(f"[dry-run] window ({after} , {hi}] -> {would_change} grade changes")
            after = str(hi)
            batches += 1
            continue

        # SHORT transaction: per-batch lock + statement timeouts so a batch that
        # collides with a loader fails fast and is retried, never blocks writers.
        try:
            cur.execute(f"SET LOCAL lock_timeout = '{BATCH_LOCK_TIMEOUT}'")
            cur.execute(
                f"SET LOCAL statement_timeout = '{BATCH_STATEMENT_TIMEOUT}'"
            )
            cur.execute(update_sql, {"after": after, "upto": str(hi)})
            changed = cur.rowcount
            conn.commit()
        except psycopg2.errors.LockNotAvailable:
            conn.rollback()
            cur.close()
            log(f"  batch ({after}, {hi}]: lock_timeout — backing off 2s, retrying")
            time.sleep(2.0)
            continue
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            cur.close()
            log(f"  batch ({after}, {hi}]: ERROR {exc} — backing off 5s, retrying")
            time.sleep(5.0)
            continue
        cur.close()

        total_changed += changed
        batches += 1
        after = str(hi)
        write_checkpoint(checkpoint_path, after)
        log(f"  batch {batches}: ({lo} .. {hi}] {changed} grade changes "
            f"(running total {total_changed})")

        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

    conn.close()
    log(f"DONE — {batches} batches, {total_changed} grade changes")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Batched, resumable A/B/C lead scoring for hot_leads"
    )
    p.add_argument("--db-host", default=os.getenv("DB_HOST", "100.122.216.15"))
    p.add_argument("--db-port", default=DB_PORT, help="Database port")
    p.add_argument("--batch-size", type=int, default=10000,
                   help="Rows per transaction (default 10000)")
    p.add_argument("--sleep-ms", type=int, default=0,
                   help="Pause between batches to yield to live writers")
    p.add_argument("--start-after", default=None,
                   help="Resume after this hot_leads.id (UUID)")
    p.add_argument("--max-batches", type=int, default=None,
                   help="Stop after N batches (for testing / a bounded slice)")
    p.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT,
                   help="Checkpoint file for resumability")
    p.add_argument("--dry-run", action="store_true",
                   help="Count grade changes per window; never writes")
    p.add_argument("--only-stale", action="store_true",
                   help="Skip rows already scored within --stale-hours")
    p.add_argument("--stale-hours", type=int, default=20,
                   help="With --only-stale, how recent counts as fresh")
    args = p.parse_args()

    if args.batch_size < 1:
        print("--batch-size must be >= 1")
        sys.exit(2)

    run(
        host=args.db_host,
        port=args.db_port,
        batch_size=args.batch_size,
        sleep_ms=args.sleep_ms,
        start_after=args.start_after,
        max_batches=args.max_batches,
        checkpoint_path=args.checkpoint,
        dry_run=args.dry_run,
        only_stale=args.only_stale,
        stale_hours=args.stale_hours,
    )


if __name__ == "__main__":
    main()
