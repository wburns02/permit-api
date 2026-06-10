#!/usr/bin/env bash
# Nightly RRC refresh: daily W-1 drilling permit file -> canonical.well_permits.
# RRC posts daf420.dat nightly (master+trailer with lat/long).
# Cron (R730): 30 11 * * * (11:30 UTC = 05:30/06:30 CT, after RRC's overnight post)
set -uo pipefail
SCRIPTS="$(cd "$(dirname "$0")" && pwd)"
OUT=${RRC_OUT:-/mnt/data/staging/rrc}
mkdir -p "$OUT"
STAMP=$(date +%Y%m%d)

python3 "$SCRIPTS/rrc_mft_fetch.py" 5f07cc72-2e79-4df8-ade1-9aeb792e03fc \
  --get daf420.dat --out "$OUT/daf420.$STAMP.dat" || exit 1
python3 "$SCRIPTS/load_rrc_w1.py" "$OUT/daf420.$STAMP.dat" --source rrc_daf420 || exit 1
# keep two weeks of dailies
find "$OUT" -name 'daf420.*.dat' -mtime +14 -delete
echo "[rrc_daily_refresh] done $(date -Is)"
