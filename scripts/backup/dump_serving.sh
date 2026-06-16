#!/bin/bash
# Nightly 24h-RPO dump of serving-critical tables to /dataPool/backups/serving/
# Intended to run via systemd timer: permit-backup-serving.timer
# Sequences AFTER nightly refresh; do not run during a bulk load.
#
# Tables: permits, jurisdictions, contractor_licenses, epa_facilities,
#         fema_flood_zones, census_demographics, septic_systems,
#         property_valuations

set -euo pipefail

PG_HOST="100.122.216.15"
PG_PORT="5432"
PG_DB="permits"
PG_USER="will"

DEST="/dataPool/backups/serving"
TIMESTAMP="$(date +%Y-%m-%dT%H%M%S)"
RETAIN_DAYS=14

SERVING_TABLES=(
    permits
    jurisdictions
    contractor_licenses
    epa_facilities
    fema_flood_zones
    census_demographics
    septic_systems
    property_valuations
)

mkdir -p "$DEST"

echo "[$(date -Iseconds)] Starting nightly serving backup → $DEST/$TIMESTAMP/"
mkdir -p "$DEST/$TIMESTAMP"

for TABLE in "${SERVING_TABLES[@]}"; do
    OUTFILE="$DEST/$TIMESTAMP/${TABLE}.pg_dump.gz"
    echo "  dumping $TABLE..."
    pg_dump -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" \
        --format=custom --no-password -t "$TABLE" \
        | gzip > "$OUTFILE"
    echo "  wrote $OUTFILE ($(du -sh "$OUTFILE" | cut -f1))"
done

# Retention: prune directories older than RETAIN_DAYS days
echo "Pruning backups older than $RETAIN_DAYS days from $DEST..."
find "$DEST" -mindepth 1 -maxdepth 1 -type d -mtime "+$RETAIN_DAYS" -print -exec rm -rf {} +

echo "[$(date -Iseconds)] Nightly serving backup complete."
