#!/usr/bin/env bash
# Per-partition duplicate sweep for canonical.production_monthly and
# canonical.permits. Replaces the full-scan dup check that wedged for 10h.
# Each partition is checked independently with its own timeout so one bad
# plan cannot stall the sweep.
set -uo pipefail
DSN="host=100.122.216.15 dbname=permits user=will"
OUT=${1:-/mnt/win11/Fedora/raw-public-data/rrc/dq_partition_sweep.log}
: > "$OUT"

parts() {
  psql "$DSN" -t -A -c "
    SELECT c.relname FROM pg_class c
    JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_inherits i ON i.inhrelid=c.oid
    WHERE n.nspname='canonical' AND i.inhparent = ('canonical.' || \$\$$1\$\$)::regclass
    ORDER BY 1"
}

check() {  # table, dup-key columns
  local tbl=$1 key=$2
  local r
  r=$(psql "$DSN" -t -A -c "
      SET work_mem='1GB'; SET statement_timeout='600s';
      SELECT count(*) FROM (
        SELECT 1 FROM canonical.\"$tbl\" GROUP BY $key HAVING count(*)>1) d" 2>&1 | tail -1)
  echo -e "$tbl\t$key\t$r" | tee -a "$OUT"
}

echo "== production_monthly partitions ==" | tee -a "$OUT"
for p in $(parts production_monthly); do
  check "$p" "well_type, district, lease_number, prod_month"
done

echo "== permits partitions ==" | tee -a "$OUT"
for p in $(parts permits); do
  check "$p" "source_id, source_record_id"
done

echo "== sweep done $(date -Is) ==" | tee -a "$OUT"
