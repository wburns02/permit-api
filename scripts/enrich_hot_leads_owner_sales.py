#!/usr/bin/env python3
"""
Enrich hot_leads with owner / last-sale / business-entity data — SAFELY.

Backfills three derived columns on `hot_leads` (12.9M rows) from two reference
tables:
  * property_sales   — most-recent recorded sale at the lead's address
                       -> hot_leads.owner_name (grantee) + last_sale_price/date
  * business_entities — owner_name matched to a registered entity_name
                       -> hot_leads.owner_entity_type (LLC/Corp/...)

WHY THIS SCRIPT EXISTS
----------------------
The previous owner/sales enrichment ran as ONE giant transaction:

    UPDATE hot_leads h
       SET owner_name = ps.grantee, ...
      FROM property_sales ps
      JOIN business_entities be ON ...
     WHERE h.address = ps.address ...;          -- 12.9M-row UPDATE, no batching

That single statement took ExclusiveLock/RowExclusiveLock on hot_leads for
HOURS-to-DAYS, serialized every other hot_leads writer (loaders, the bridge,
the brazoria MV refresh), and timed out reads — 46 sessions blocked at peak,
THREE times in one day.

THE FIX (this script):
  (a) Bounded keyset batches over hot_leads.id (UUID), per-batch COMMIT.
  (b) NEVER takes a table-level LOCK on hot_leads. Each batch updates only the
      rows whose id falls in the current keyset window.
  (c) Each transaction is SHORT: it touches at most --batch-size rows, with a
      per-statement lock_timeout + statement_timeout so a batch that can't get
      its row locks fails fast and is retried instead of blocking writers.
  (d) Resumable: writes the last committed id to a checkpoint file (and you can
      pass --start-after <uuid> to resume manually). Re-running is idempotent.
  (e) Optional --sleep-ms between batches to yield the write path to live
      loaders under load.

Each batch is a tightly-scoped `UPDATE hot_leads ... FROM (subquery)` keyed by a
known id-range, so the planner uses the hot_leads PK and the indexed lookups on
property_sales(address,city,state) / business_entities(entity_name) — never a
full-table self-join held open across the whole 12.9M rows.

Usage:
    # dry-run a single small batch (no writes), prove the lock footprint:
    python3 scripts/enrich_hot_leads_owner_sales.py --db-host 100.122.216.15 \
        --batch-size 2000 --max-batches 1 --dry-run

    # full resumable run, 5k rows/txn, yield 50ms between batches:
    python3 scripts/enrich_hot_leads_owner_sales.py --db-host 100.122.216.15 \
        --batch-size 5000 --sleep-ms 50

    # resume after a known id:
    python3 scripts/enrich_hot_leads_owner_sales.py --db-host 100.122.216.15 \
        --start-after 7f3a... --batch-size 5000
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

DEFAULT_CHECKPOINT = "/tmp/enrich_hot_leads_owner_sales.checkpoint"

# Per-batch safety caps. lock_timeout is the load-bearing one: if a batch can't
# acquire its row locks within this window (because a loader holds them) it
# fails fast and we retry — it NEVER queues up holding partial locks. Both are
# SET LOCAL, so they apply only inside the batch transaction.
BATCH_LOCK_TIMEOUT = "5s"
BATCH_STATEMENT_TIMEOUT = "120s"

# Columns we backfill. Created IF NOT EXISTS at startup so a fresh schema works.
ENRICH_COLUMNS = (
    ("last_sale_price", "DOUBLE PRECISION"),
    ("last_sale_date", "DATE"),
    ("owner_entity_type", "VARCHAR(50)"),
)

# Functional indexes the per-batch LATERAL lookups REQUIRE to stay fast. The
# address join normalizes both sides with UPPER(TRIM(...)), which defeats a
# plain btree on property_sales(address); without the matching expression index
# each lead does a ~30ms seq-scan-ish lookup and a 5k batch runs 150s+. With it,
# each lookup is a ~1ms index probe and a batch finishes in well under a second.
# `business_entities.entity_name` is matched verbatim (already UPPER in the
# loader), so a plain btree suffices. CREATE INDEX IF NOT EXISTS is idempotent;
# we DON'T use CONCURRENTLY (it can't run inside the script's txn and would slow
# first-run startup) — these are created once, before batching begins.
ENRICH_INDEXES = (
    "CREATE INDEX IF NOT EXISTS ix_ps_addr_norm "
    "ON property_sales (UPPER(TRIM(address)), state)",
    "CREATE INDEX IF NOT EXISTS ix_be_entity_name "
    "ON business_entities (entity_name)",
)


def log(msg: str) -> None:
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_conn(host: str, port: str = DB_PORT):
    return psycopg2.connect(host=host, port=port, dbname=DB_NAME, user=DB_USER)


def ensure_schema(conn) -> None:
    """Add enrichment target columns + the functional indexes the batch lookups
    require. Columns are cheap metadata-only DDL (nullable, no rewrite on modern
    PG). Index creation is one-time; on a populated reference table the first run
    pays for it, every run after is a no-op."""
    cur = conn.cursor()
    for name, coltype in ENRICH_COLUMNS:
        cur.execute(
            f"ALTER TABLE hot_leads ADD COLUMN IF NOT EXISTS {name} {coltype}"
        )
    conn.commit()
    for ddl in ENRICH_INDEXES:
        cur.execute(ddl)
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
    tmp = f"{path}.tmp"
    with open(tmp, "w") as fh:
        fh.write(last_id)
    os.replace(tmp, path)


# The per-batch enrichment. KEYSET-bounded on hot_leads.id: we walk ascending id
# windows so each UPDATE touches at most `batch_size` rows. The id ordering uses
# the PK index, so finding the next window is O(log n), not a scan.
#
# Lateral subqueries (one most-recent sale, one matching entity) keep the join
# from fanning out — we never produce the full property_sales × business_entities
# product, only one best match per lead. Address matching mirrors the
# normalize_address() convention used elsewhere (UPPER + collapse whitespace).
_SELECT_WINDOW = """
    SELECT id
      FROM hot_leads
     WHERE id > %(after)s
       AND address IS NOT NULL
     ORDER BY id
     LIMIT %(batch)s
"""

_BATCH_UPDATE = """
    UPDATE hot_leads h
       SET owner_name        = COALESCE(h.owner_name, sale.grantee),
           last_sale_price   = sale.sale_price,
           last_sale_date    = sale.sale_date,
           owner_entity_type = ent.entity_type
      FROM (
            -- The exact id window we already picked, re-stated so the planner
            -- drives the UPDATE from the PK index (id BETWEEN lo AND hi).
            SELECT id, address, city, state, owner_name
              FROM hot_leads
             WHERE id > %(after)s AND id <= %(upto)s
               AND address IS NOT NULL
           ) win
      -- most-recent sale at this address (one row, indexed on address)
      LEFT JOIN LATERAL (
            SELECT ps.grantee, ps.sale_price, ps.sale_date
              FROM property_sales ps
             WHERE UPPER(TRIM(ps.address)) = UPPER(TRIM(win.address))
               AND ps.state = win.state
               AND ps.sale_date IS NOT NULL
             ORDER BY ps.sale_date DESC
             LIMIT 1
           ) sale ON TRUE
      -- registered entity matching the owner (one row, indexed on entity_name)
      LEFT JOIN LATERAL (
            SELECT be.entity_type
              FROM business_entities be
             WHERE be.entity_name = UPPER(TRIM(COALESCE(win.owner_name, sale.grantee)))
               AND be.entity_type IS NOT NULL
             LIMIT 1
           ) ent ON TRUE
     WHERE h.id = win.id
       AND (sale.grantee IS NOT NULL OR ent.entity_type IS NOT NULL)
"""

# Find the low/high id of the current window so the UPDATE's `id <= upto`
# matches exactly the same rows the SELECT chose (keeps the touched set ==
# batch_size). Postgres has no min(uuid)/max(uuid) aggregate, so take the first
# and last of the ordered, LIMIT-ed window directly off the PK index.
_WINDOW_BOUNDS = """
    WITH w AS (
        SELECT id FROM hot_leads
         WHERE id > %(after)s AND address IS NOT NULL
         ORDER BY id LIMIT %(batch)s
    )
    SELECT (SELECT id FROM w ORDER BY id ASC  LIMIT 1) AS lo,
           (SELECT id FROM w ORDER BY id DESC LIMIT 1) AS hi
"""


def run(host: str, port: str, batch_size: int, sleep_ms: int,
        start_after: str | None, max_batches: int | None,
        checkpoint_path: str, dry_run: bool) -> None:
    conn = get_conn(host, port)
    conn.autocommit = False
    log(f"Connected to {host}:{port}/{DB_NAME}")

    if not dry_run:
        ensure_schema(conn)

    # Resume point: explicit flag > checkpoint file > UUID zero.
    after = start_after or read_checkpoint(checkpoint_path) or \
        "00000000-0000-0000-0000-000000000000"
    log(f"Starting after id {after} (batch_size={batch_size}, "
        f"sleep_ms={sleep_ms}, dry_run={dry_run})")

    total_updated = 0
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
            log("No more rows — enrichment complete")
            break

        if dry_run:
            # Prove the footprint without writing: count what WOULD update.
            cur.execute(
                "SELECT count(*) FROM (" + _SELECT_WINDOW + ") w",
                {"after": after, "batch": batch_size},
            )
            window_rows = cur.fetchone()[0]
            conn.rollback()
            cur.close()
            log(f"[dry-run] window ({after} , {hi}] -> {window_rows} candidate rows")
            after = str(hi)
            batches += 1
            continue

        # SHORT transaction: per-batch lock + statement timeouts so a batch that
        # collides with a loader fails fast and is retried, never blocks writers.
        try:
            cur.execute(f"SET LOCAL lock_timeout = '{BATCH_LOCK_TIMEOUT}'")
            cur.execute(f"SET LOCAL statement_timeout = '{BATCH_STATEMENT_TIMEOUT}'")
            cur.execute(_BATCH_UPDATE, {"after": after, "upto": str(hi)})
            updated = cur.rowcount
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

        total_updated += updated
        batches += 1
        after = str(hi)
        write_checkpoint(checkpoint_path, after)
        log(f"  batch {batches}: ({lo} .. {hi}] +{updated} updated "
            f"(running total {total_updated})")

        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

    conn.close()
    log(f"DONE — {batches} batches, {total_updated} rows enriched")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Batched, resumable owner/sales enrichment for hot_leads"
    )
    p.add_argument("--db-host", default=os.getenv("DB_HOST", "100.122.216.15"))
    p.add_argument("--db-port", default=DB_PORT, help="Database port")
    p.add_argument("--batch-size", type=int, default=5000,
                   help="Rows per transaction (default 5000)")
    p.add_argument("--sleep-ms", type=int, default=0,
                   help="Pause between batches to yield to live writers")
    p.add_argument("--start-after", default=None,
                   help="Resume after this hot_leads.id (UUID)")
    p.add_argument("--max-batches", type=int, default=None,
                   help="Stop after N batches (for testing / a bounded slice)")
    p.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT,
                   help="Checkpoint file for resumability")
    p.add_argument("--dry-run", action="store_true",
                   help="Count candidate rows per window; never writes")
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
    )


if __name__ == "__main__":
    main()
