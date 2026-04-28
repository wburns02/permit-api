#!/bin/bash
# Wrapper for cleanup_hot_leads_garbage_dates.sql that logs to a stable path.
# Scheduled via `at` for 3 AM CDT to avoid daytime lock contention.

set -e
LOG=/home/will/permit-api/logs/hot_leads_cleanup_$(date +%Y%m%d_%H%M%S).log
mkdir -p /home/will/permit-api/logs

{
    echo "=== hot_leads cleanup started at $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
    psql -h 100.122.216.15 -p 5432 -U will -d permits \
         -f /home/will/permit-api/scripts/cleanup_hot_leads_garbage_dates.sql
    rc=$?
    echo "=== finished at $(date -u +%Y-%m-%dT%H:%M:%SZ) — exit code $rc ==="
} 2>&1 | tee "$LOG"
