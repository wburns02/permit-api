"""Phase 2 Resilience tests — backup scripts, restore drill, cutover runbook, parity check.

All tests are hermetic: they inspect the *content and structure* of the
scripts, not runtime behaviour.  Executing scripts requires a live T430
Postgres and /dataPool mount; those are acceptance gates run by the owner,
not CI.

Acceptance criteria from docs/pipeline/PERMIT_NEXT_PHASE.md §Task A–E.
"""
import os
import re
import stat

import pytest

# ── Paths ────────────────────────────────────────────────────────────────────

DUMP_SERVING = "scripts/backup/dump_serving.sh"
DUMP_BILLING = "scripts/backup/dump_billing.sh"
RESTORE_DRILL = "scripts/backup/restore_drill.sh"
PARITY_CHECK = "scripts/backup/standby_parity.sh"
CUTOVER_RUNBOOK = "docs/pipeline/CUTOVER_RUNBOOK.md"
BACKUP_TIMER_NIGHTLY = "scripts/backup/permit-backup-serving.timer"
BACKUP_TIMER_HOURLY = "scripts/backup/permit-backup-billing.timer"
BACKUP_SVC_SERVING = "scripts/backup/permit-backup-serving.service"
BACKUP_SVC_BILLING = "scripts/backup/permit-backup-billing.service"

# ── Expected table sets (from PERMIT_NEXT_PHASE.md §3) ───────────────────────

SERVING_TABLES = {
    "permits",
    "jurisdictions",
    "contractor_licenses",
    "epa_facilities",
    "fema_flood_zones",
    "census_demographics",
    "septic_systems",
    "property_valuations",
}

BILLING_TABLES = {
    "api_users",
    "api_keys",
    "usage_logs",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _read(path: str) -> str:
    with open(path) as fh:
        return fh.read()


def _is_executable(path: str) -> bool:
    mode = os.stat(path).st_mode
    return bool(mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))


# ═══════════════════════════════════════════════════════════════════════════════
# Task A — dump_serving.sh (24h-RPO nightly backup)
# ═══════════════════════════════════════════════════════════════════════════════

class TestDumpServing:
    def test_file_exists(self):
        assert os.path.isfile(DUMP_SERVING), f"Missing: {DUMP_SERVING}"

    def test_file_is_executable(self):
        assert _is_executable(DUMP_SERVING), f"Not executable: {DUMP_SERVING}"

    def test_targets_t430(self):
        body = _read(DUMP_SERVING)
        assert "100.122.216.15" in body, "Must connect to T430 at 100.122.216.15"

    def test_targets_datapool_serving(self):
        body = _read(DUMP_SERVING)
        assert "/dataPool/backups/serving" in body, "Must write to /dataPool/backups/serving"

    def test_gzips_output(self):
        body = _read(DUMP_SERVING)
        assert "gzip" in body or ".gz" in body, "Dumps must be gzip-compressed"

    def test_timestamped_filenames(self):
        body = _read(DUMP_SERVING)
        # Expects date or timestamp in the filename construction
        assert re.search(r"\$\(date|%Y|%F|timestamp", body), (
            "Filenames must be timestamped"
        )

    def test_includes_all_serving_tables(self):
        body = _read(DUMP_SERVING)
        missing = [t for t in SERVING_TABLES if t not in body]
        assert not missing, f"dump_serving.sh is missing tables: {missing}"

    def test_has_retention_prune(self):
        body = _read(DUMP_SERVING)
        # Retention prune: find + delete or rm of old files
        assert re.search(r"find .*(mtime|days|delete|rm)", body, re.IGNORECASE), (
            "Must prune old backups (retention policy)"
        )

    def test_uses_pg_dump(self):
        body = _read(DUMP_SERVING)
        assert "pg_dump" in body, "Must use pg_dump"


# ═══════════════════════════════════════════════════════════════════════════════
# Task A — dump_billing.sh (1h-RPO hourly backup)
# ═══════════════════════════════════════════════════════════════════════════════

class TestDumpBilling:
    def test_file_exists(self):
        assert os.path.isfile(DUMP_BILLING), f"Missing: {DUMP_BILLING}"

    def test_file_is_executable(self):
        assert _is_executable(DUMP_BILLING), f"Not executable: {DUMP_BILLING}"

    def test_targets_t430(self):
        body = _read(DUMP_BILLING)
        assert "100.122.216.15" in body, "Must connect to T430 at 100.122.216.15"

    def test_targets_datapool_billing(self):
        body = _read(DUMP_BILLING)
        assert "/dataPool/backups/billing" in body, "Must write to /dataPool/backups/billing"

    def test_gzips_output(self):
        body = _read(DUMP_BILLING)
        assert "gzip" in body or ".gz" in body, "Dumps must be gzip-compressed"

    def test_includes_all_billing_tables(self):
        body = _read(DUMP_BILLING)
        missing = [t for t in BILLING_TABLES if t not in body]
        assert not missing, f"dump_billing.sh is missing tables: {missing}"

    def test_future_invoice_table_mentioned(self):
        """Phase 5 adds invoices — script must include it by name so Phase 5 needs no change."""
        body = _read(DUMP_BILLING)
        assert "invoices" in body, (
            "Must reference 'invoices' table (added in Phase 5) so the script "
            "needs no update when that table is created"
        )

    def test_has_retention_prune(self):
        body = _read(DUMP_BILLING)
        assert re.search(r"find .*(mtime|days|delete|rm)", body, re.IGNORECASE), (
            "Must prune old backups"
        )

    def test_uses_pg_dump(self):
        body = _read(DUMP_BILLING)
        assert "pg_dump" in body


# ═══════════════════════════════════════════════════════════════════════════════
# Task B — systemd timers
# ═══════════════════════════════════════════════════════════════════════════════

class TestSystemdTimers:
    def test_nightly_timer_exists(self):
        assert os.path.isfile(BACKUP_TIMER_NIGHTLY), f"Missing: {BACKUP_TIMER_NIGHTLY}"

    def test_hourly_timer_exists(self):
        assert os.path.isfile(BACKUP_TIMER_HOURLY), f"Missing: {BACKUP_TIMER_HOURLY}"

    def test_nightly_service_exists(self):
        assert os.path.isfile(BACKUP_SVC_SERVING), f"Missing: {BACKUP_SVC_SERVING}"

    def test_hourly_service_exists(self):
        assert os.path.isfile(BACKUP_SVC_BILLING), f"Missing: {BACKUP_SVC_BILLING}"

    def test_nightly_timer_frequency(self):
        """Nightly timer should fire once a day (OnCalendar=daily or *-*-* 02:*)."""
        body = _read(BACKUP_TIMER_NIGHTLY)
        assert re.search(r"OnCalendar\s*=\s*(daily|\*-\*-\*)", body), (
            "Nightly timer must use OnCalendar=daily or a daily cron expression"
        )

    def test_hourly_timer_frequency(self):
        """Hourly timer should fire every hour."""
        body = _read(BACKUP_TIMER_HOURLY)
        assert re.search(r"OnCalendar\s*=\s*(\*-\*-\*\s+\*:00|hourly)", body, re.IGNORECASE) or \
               re.search(r"OnUnitActiveSec\s*=\s*1h", body), (
            "Billing timer must fire hourly"
        )

    def test_nightly_service_calls_dump_serving(self):
        body = _read(BACKUP_SVC_SERVING)
        assert "dump_serving.sh" in body, "Nightly service must call dump_serving.sh"

    def test_hourly_service_calls_dump_billing(self):
        body = _read(BACKUP_SVC_BILLING)
        assert "dump_billing.sh" in body, "Hourly service must call dump_billing.sh"


# ═══════════════════════════════════════════════════════════════════════════════
# Task C — restore_drill.sh
# ═══════════════════════════════════════════════════════════════════════════════

class TestRestoreDrill:
    def test_file_exists(self):
        assert os.path.isfile(RESTORE_DRILL), f"Missing: {RESTORE_DRILL}"

    def test_file_is_executable(self):
        assert _is_executable(RESTORE_DRILL), f"Not executable: {RESTORE_DRILL}"

    def test_uses_pg_restore_or_psql(self):
        body = _read(RESTORE_DRILL)
        assert "pg_restore" in body or "psql" in body, "Must use pg_restore or psql"

    def test_validates_row_counts(self):
        body = _read(RESTORE_DRILL)
        assert re.search(r"COUNT\s*\(\s*\*\s*\)", body, re.IGNORECASE), (
            "Must validate row counts with COUNT(*)"
        )

    def test_validates_checksums(self):
        body = _read(RESTORE_DRILL)
        assert re.search(r"md5|sha256|checksum|hash", body, re.IGNORECASE), (
            "Must validate checksums (md5/sha256) against source"
        )

    def test_exits_nonzero_on_mismatch(self):
        body = _read(RESTORE_DRILL)
        assert re.search(r"exit\s+1|exit\s+\$\?", body), (
            "Must exit non-zero on row-count or checksum mismatch"
        )

    def test_uses_scratch_db(self):
        body = _read(RESTORE_DRILL)
        assert re.search(r"scratch|restore_drill|_drill|_tmp|_test", body, re.IGNORECASE), (
            "Must restore into a scratch/temporary database, not production"
        )

    def test_covers_serving_tables(self):
        body = _read(RESTORE_DRILL)
        missing = [t for t in {"permits", "api_users"} if t not in body]
        assert not missing, f"restore_drill.sh doesn't reference key tables: {missing}"


# ═══════════════════════════════════════════════════════════════════════════════
# Task D — CUTOVER_RUNBOOK.md
# ═══════════════════════════════════════════════════════════════════════════════

class TestCutoverRunbook:
    def test_file_exists(self):
        assert os.path.isfile(CUTOVER_RUNBOOK), f"Missing: {CUTOVER_RUNBOOK}"

    def test_has_preflight_section(self):
        body = _read(CUTOVER_RUNBOOK)
        assert re.search(r"pre.?flight|pre-check|checklist", body, re.IGNORECASE), (
            "Must have a pre-flight / pre-check section"
        )

    def test_references_cloudflare_api(self):
        body = _read(CUTOVER_RUNBOOK)
        assert re.search(r"cloudflare", body, re.IGNORECASE), (
            "Must reference Cloudflare API for DNS flip"
        )

    def test_references_zone_and_record(self):
        body = _read(CUTOVER_RUNBOOK)
        assert re.search(r"zone.?id|record.?id|ZONE_ID|RECORD_ID", body, re.IGNORECASE), (
            "Must include zone_id and record_id placeholders"
        )

    def test_has_failback_section(self):
        body = _read(CUTOVER_RUNBOOK)
        assert re.search(r"fail.?back|rollback|revert", body, re.IGNORECASE), (
            "Must document how to fail back after a cutover"
        )

    def test_has_verify_authed_traffic_step(self):
        body = _read(CUTOVER_RUNBOOK)
        assert re.search(r"verify|authed|401|health|curl", body, re.IGNORECASE), (
            "Must include a step to verify authed traffic after flipping DNS"
        )

    def test_has_rto_target(self):
        body = _read(CUTOVER_RUNBOOK)
        assert re.search(r"15\s*min|RTO", body, re.IGNORECASE), (
            "Must reference the 15-minute RTO target"
        )

    def test_does_not_exercise_the_flip(self):
        """Runbook documents the flip but must not auto-execute it."""
        body = _read(CUTOVER_RUNBOOK)
        # The runbook should instruct humans, not have a live curl -X PATCH call
        # against api.cloudflare.com embedded as a shell block that runs on read.
        # We just check it's a markdown doc, not a shell script.
        assert not body.strip().startswith("#!/"), (
            "CUTOVER_RUNBOOK.md must be a markdown doc, not a shell script"
        )

    def test_mentions_token_location(self):
        body = _read(CUTOVER_RUNBOOK)
        assert re.search(r"token|CF_API_TOKEN|CLOUDFLARE_TOKEN", body, re.IGNORECASE), (
            "Must document where the Cloudflare API token lives"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Task E — standby_parity.sh
# ═══════════════════════════════════════════════════════════════════════════════

class TestStandbyParity:
    def test_file_exists(self):
        assert os.path.isfile(PARITY_CHECK), f"Missing: {PARITY_CHECK}"

    def test_file_is_executable(self):
        assert _is_executable(PARITY_CHECK), f"Not executable: {PARITY_CHECK}"

    def test_checks_git_sha(self):
        body = _read(PARITY_CHECK)
        assert re.search(r"git rev-parse|git log|SHA|commit", body, re.IGNORECASE), (
            "Must verify both nodes run the same git commit"
        )

    def test_probes_pg_ecbtx_com(self):
        body = _read(PARITY_CHECK)
        assert "pg.ecbtx.com" in body, "Must probe T430 via pg.ecbtx.com cloudflared path"

    def test_probes_tailscale_fallback(self):
        body = _read(PARITY_CHECK)
        assert "100.122.216.15" in body, "Must probe T430 via Tailscale IP fallback"

    def test_diffs_env_vars(self):
        body = _read(PARITY_CHECK)
        assert re.search(r"env|ENV|environment|printenv", body, re.IGNORECASE), (
            "Must diff env-var key sets between nodes"
        )

    def test_exits_nonzero_on_parity_failure(self):
        body = _read(PARITY_CHECK)
        assert re.search(r"exit\s+1", body), "Must exit 1 when parity checks fail"
