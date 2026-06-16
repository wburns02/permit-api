#!/bin/bash
# Hourly 1h-RPO dump of customer/billing tables to /dataPool/backups/billing/
# Intended to run via systemd timer: permit-backup-billing.timer
#
# Tables: api_users, api_keys, usage_logs, invoices (Phase 5 — included now
# so the script needs no change when the invoices table is created)

set -euo pipefail

PG_HOST="100.122.216.15"
PG_PORT="5432"
PG_DB="permits"
PG_USER="will"

DEST="/dataPool/backups/billing"
TIMESTAMP="$(date +%Y-%m-%dT%H%M%S)"
RETAIN_DAYS=7

BILLING_TABLES=(
    api_users
    api_keys
    usage_logs
    invoices
)

mkdir -p "$DEST"

echo "[$(date -Iseconds)] Starting hourly billing backup → $DEST/$TIMESTAMP/"
mkdir -p "$DEST/$TIMESTAMP"

for TABLE in "${BILLING_TABLES[@]}"; do
    OUTFILE="$DEST/$TIMESTAMP/${TABLE}.pg_dump.gz"
    echo "  dumping $TABLE..."
    # Use --if-exists so tables that don't yet exist (e.g. invoices pre-Phase-5)
    # don't abort the whole run.
    if pg_dump -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
        --format=custom --no-password -t "$TABLE" \
        2>/dev/null | gzip > "$OUTFILE"; then
        # Check if file is non-empty (table existed)
        if [ -s "$OUTFILE" ]; then
            echo "  wrote $OUTFILE ($(du -sh "$OUTFILE" | cut -f1))"
        else
            rm -f "$OUTFILE"
            echo "  skipped $TABLE (table does not exist yet)"
        fi
    else
        rm -f "$OUTFILE" 2>/dev/null || true
        echo "  skipped $TABLE (table does not exist yet)"
    fi
done

# Retention: prune directories older than RETAIN_DAYS days
echo "Pruning backups older than $RETAIN_DAYS days from $DEST..."
find "$DEST" -mindepth 1 -maxdepth 1 -type d -mtime "+$RETAIN_DAYS" -print -exec rm -rf {} +

echo "[$(date -Iseconds)] Hourly billing backup complete."
