"""Platform-stability fixes — hermetic guards for two regressions.

FIX 1: the hot_leads enrichment lock-storm. The previous owner/sales enrichment
ran a single giant `UPDATE hot_leads ... FROM property_sales JOIN
business_entities`, holding table-level locks for hours and serializing every
writer. The replacement (scripts/enrich_hot_leads_owner_sales.py) MUST stay
batched, short-locked, and never take a table-level LOCK.

FIX 2: the brazoria_permit_leads / unserviced_hail_leads MVs self-heal EMPTY on
every restart (created/rebuilt WITH NO DATA) and only populate at the 04:25 UTC
nightly cron. The startup path MUST populate any unpopulated MV right after the
migrations so endpoints serve real data immediately after a deploy.

All tests are hermetic: they inspect script/source structure, not runtime
behaviour (which needs the live 12.9M-row T430 Postgres — an owner acceptance
gate, proven separately in the PR description).
"""
import inspect
import re

ENRICH_SCRIPT = "scripts/enrich_hot_leads_owner_sales.py"


def _read(path: str) -> str:
    with open(path) as fh:
        return fh.read()


# ── FIX 1: enrichment must be batched + short-locked, never table-locked ──────

def test_enrichment_takes_no_table_lock_on_hot_leads():
    """The whole point: no `LOCK TABLE hot_leads` anywhere in the writer."""
    src = _read(ENRICH_SCRIPT).upper()
    assert "LOCK TABLE" not in src, "enrichment must never take a table-level lock"


def test_enrichment_is_batched_with_per_batch_commit():
    src = _read(ENRICH_SCRIPT)
    # bounded batches
    assert "--batch-size" in src
    assert re.search(r"LIMIT\s+%\(batch\)s", src), "batches must be LIMIT-bounded"
    # per-batch commit inside the loop (not one giant transaction)
    assert "conn.commit()" in src
    # autocommit OFF so each batch is its own explicit transaction
    assert "autocommit = False" in src


def test_enrichment_has_short_lock_and_statement_timeouts():
    """Each batch sets SET LOCAL lock_timeout/statement_timeout so a collision
    with a live loader fails fast and retries instead of blocking writers."""
    src = _read(ENRICH_SCRIPT)
    assert "SET LOCAL lock_timeout" in src
    assert "SET LOCAL statement_timeout" in src
    # lock_timeout must be a short, finite window (NOT disabled '0')
    assert re.search(r"BATCH_LOCK_TIMEOUT\s*=\s*[\"']\d+\s*s", src)
    assert "LockNotAvailable" in src, "must catch + retry on lock_timeout"


def test_enrichment_is_resumable():
    src = _read(ENRICH_SCRIPT)
    assert "--start-after" in src
    assert "checkpoint" in src.lower()
    assert "write_checkpoint" in src and "read_checkpoint" in src


def test_enrichment_keyset_paginates_by_id():
    """Bounded keyset over the PK (id > after ... ORDER BY id LIMIT) — never an
    unbounded full-table self-join held open across all 12.9M rows."""
    src = _read(ENRICH_SCRIPT)
    assert re.search(r"id\s*>\s*%\(after\)s", src)
    assert "ORDER BY id" in src


# ── FIX 2: startup must populate unpopulated MVs immediately ──────────────────

def test_refresh_unpopulated_exists_and_uses_ispopulated_detection():
    from app.services import mv_refresh

    assert hasattr(mv_refresh, "refresh_unpopulated")
    src = inspect.getsource(mv_refresh.refresh_unpopulated)
    # Detects empties via the authoritative pg flag, not cron_heartbeat staleness
    assert "pg_matviews" in src
    assert "ispopulated = false" in src
    # Refreshes only the detected MVs (reuses _refresh_one)
    assert "_refresh_one" in src


def test_startup_migration_calls_refresh_unpopulated():
    """The migration body (re)creates MVs WITH NO DATA; the lock-holding worker
    must populate them right after, so a deploy never serves empty."""
    src = _read("app/main.py")
    assert "refresh_unpopulated" in src
    # called after the migration body, inside the lock-guarded run
    body_idx = src.index("_run_startup_migrations_body(_text, primary_engine)")
    call_idx = src.index("await refresh_unpopulated()")
    assert call_idx > body_idx, "populate must run AFTER the migration body"


def test_both_live_mvs_are_in_the_refresh_registry():
    """Both the brazoria permit-lead feed AND the live unserviced hail product
    must be in _MVS so refresh_unpopulated covers them."""
    from app.services.mv_refresh import _MVS

    relnames = {relname for _, relname in _MVS}
    assert "brazoria_permit_leads" in relnames
    assert "unserviced_hail_leads" in relnames


# ── Smith County (Tyler/Lindale) arm wiring — hermetic source guards ─────────
def test_smith_arm_wired_into_unserviced_mv():
    """The Smith/SMITHCAD (Tyler/Lindale, East TX) arm must be fully wired into
    the unserviced_hail_leads MV: storm CTE, geometry LATERAL, BCAD-style attr
    join, county_source, the final UNION arm, and the staleness sentinel."""
    src = _read("app/main.py")
    # storm + candidate CTEs
    assert "smith_storms AS (" in src
    assert "smith_candidate_parcels AS (" in src
    assert "smith_candidate_with_addr AS (" in src
    assert "smith_rows AS (" in src
    # joins the dedicated geometry table to tx_cad_parcels on SMITHCAD
    assert "smith_parcel_geometries" in src
    assert "tcp.cad_source = 'SMITHCAD'" in src
    # county_source label + UNION arm
    assert "'Smith'::text" in src
    assert "FROM smith_rows" in src
    # Lindale/Smith bbox (East TX): lat 32.0-32.8, lon -95.7..-95.0
    assert "BETWEEN 32.0  AND 32.8" in src
    assert "BETWEEN -95.7 AND -95.0" in src
    # serviced-exclusion against hail_leads_list for the county
    assert "WHERE county ILIKE 'smith'" in src
    # staleness sentinel includes smith geometry so a stale def is rebuilt
    sentinel_idx = src.index("stale live definition detected")
    head = src[:sentinel_idx]
    assert '"smith_parcel_geometries" in live_def' in head


def test_smith_arm_captures_city_for_lindale_filter():
    """The Smith arm must project a city column (UPPER situs_city) so the
    Lindale-city subset is filterable via the /unserviced city= filter."""
    src = _read("app/main.py")
    smith_block = src[src.index("smith_candidate_with_addr AS ("):src.index("smith_rows AS (")]
    assert "UPPER(tcp.situs_city) AS city" in smith_block
