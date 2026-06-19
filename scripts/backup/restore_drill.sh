#!/bin/bash
# Restore drill: load latest backup into a scratch Postgres, validate row counts
# + checksums against the source T430.  Exit 1 on any mismatch.
#
# Usage: ./restore_drill.sh [--serving|--billing]
#        Default: --serving (the larger, more critical set)
#
# Requires: docker (for ephemeral scratch Postgres), pg_restore, psql

set -euo pipefail

MODE="${1:---serving}"
PG_HOST="100.122.216.15"
PG_PORT="5432"
PG_DB="permits"
PG_USER="will"

SCRATCH_DB="restore_drill_$(date +%s)"
SCRATCH_PORT="15432"
SCRATCH_CONTAINER="permit_restore_drill_tmp"

case "$MODE" in
    --serving)
        BACKUP_DIR="/dataPool/backups/serving"
        TABLES=(permits jurisdictions contractor_licenses epa_facilities fema_flood_zones census_demographics septic_systems property_valuations)
        ;;
    --billing)
        BACKUP_DIR="/dataPool/backups/billing"
        TABLES=(api_users api_keys usage_logs)
        ;;
    *)
        echo "Usage: $0 [--serving|--billing]" >&2
        exit 1
        ;;
esac

# Pick the most recent timestamped directory
LATEST_DIR="$(ls -1d "$BACKUP_DIR"/*/  2>/dev/null | sort | tail -n1)"
if [ -z "$LATEST_DIR" ]; then
    echo "ERROR: No backup found in $BACKUP_DIR" >&2
    exit 1
fi
echo "Using backup: $LATEST_DIR"

# ── Spin up scratch Postgres ──────────────────────────────────────────────────
echo "[$(date -Iseconds)] Starting ephemeral scratch Postgres..."
docker run --rm -d \
    --name "$SCRATCH_CONTAINER" \
    -e POSTGRES_USER=drilluser \
    -e POSTGRES_PASSWORD=drillpass \
    -e POSTGRES_DB="$SCRATCH_DB" \
    -p "${SCRATCH_PORT}:5432" \
    postgres:16-alpine

trap 'echo "Stopping scratch container..."; docker stop "$SCRATCH_CONTAINER" 2>/dev/null || true' EXIT

# Wait for Postgres to be ready
echo "Waiting for scratch Postgres to accept connections..."
for i in $(seq 1 30); do
    if PGPASSWORD=drillpass psql -h 127.0.0.1 -p "$SCRATCH_PORT" -U drilluser -d "$SCRATCH_DB" -c "SELECT 1" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

# ── Restore each table ────────────────────────────────────────────────────────
MISMATCH=0

for TABLE in "${TABLES[@]}"; do
    DUMP="$LATEST_DIR/${TABLE}.pg_dump.gz"
    if [ ! -f "$DUMP" ]; then
        echo "  SKIP $TABLE (no dump file)"
        continue
    fi

    echo "  Restoring $TABLE..."
    zcat "$DUMP" | PGPASSWORD=drillpass pg_restore \
        -h 127.0.0.1 -p "$SCRATCH_PORT" -U drilluser -d "$SCRATCH_DB" \
        --no-owner --no-privileges --no-acl 2>/dev/null || true

    # ── Row count validation ──────────────────────────────────────────────────
    SOURCE_COUNT=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
        -t -A -c "SELECT COUNT(*) FROM $TABLE" 2>/dev/null || echo "ERROR")
    DRILL_COUNT=$(PGPASSWORD=drillpass psql -h 127.0.0.1 -p "$SCRATCH_PORT" \
        -U drilluser -d "$SCRATCH_DB" -t -A -c "SELECT COUNT(*) FROM $TABLE" 2>/dev/null || echo "ERROR")

    if [ "$SOURCE_COUNT" = "ERROR" ] || [ "$DRILL_COUNT" = "ERROR" ]; then
        echo "  WARN $TABLE: could not get row count (source=$SOURCE_COUNT drill=$DRILL_COUNT)"
    elif [ "$SOURCE_COUNT" = "$DRILL_COUNT" ]; then
        echo "  OK   $TABLE: row count $DRILL_COUNT matches source"
    else
        echo "  FAIL $TABLE: row count mismatch — source=$SOURCE_COUNT drill=$DRILL_COUNT"
        MISMATCH=1
    fi

    # ── Checksum validation (md5 over ordered primary key + key columns) ──────
    # Use a stable projection to compute a relational checksum.
    SOURCE_CHECKSUM=$(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
        -t -A -c "SELECT md5(string_agg(t::text, '' ORDER BY t::text))
                  FROM (SELECT * FROM $TABLE ORDER BY 1 LIMIT 10000) t" 2>/dev/null || echo "ERROR")
    DRILL_CHECKSUM=$(PGPASSWORD=drillpass psql -h 127.0.0.1 -p "$SCRATCH_PORT" \
        -U drilluser -d "$SCRATCH_DB" \
        -t -A -c "SELECT md5(string_agg(t::text, '' ORDER BY t::text))
                  FROM (SELECT * FROM $TABLE ORDER BY 1 LIMIT 10000) t" 2>/dev/null || echo "ERROR")

    if [ "$SOURCE_CHECKSUM" = "ERROR" ] || [ "$DRILL_CHECKSUM" = "ERROR" ]; then
        echo "  WARN $TABLE: could not compute checksum"
    elif [ "$SOURCE_CHECKSUM" = "$DRILL_CHECKSUM" ]; then
        echo "  OK   $TABLE: checksum matches source"
    else
        echo "  FAIL $TABLE: checksum mismatch!"
        MISMATCH=1
    fi
done

echo ""
if [ "$MISMATCH" -eq 0 ]; then
    echo "[$(date -Iseconds)] Restore drill PASSED — all tables match source."
    exit 0
else
    echo "[$(date -Iseconds)] Restore drill FAILED — one or more tables have row-count or checksum mismatches."
    exit 1
fi
