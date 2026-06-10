#!/usr/bin/env bash
# RRC bulk pull, pass 2: items the first driver never reached.
set -uo pipefail
FETCH="python3 /home/will/permit-api-live/scripts/rrc_mft_fetch.py"
OUT=/mnt/win11/Fedora/raw-public-data/rrc
cd "$OUT"
log() { echo "[$(date +%H:%M:%S)] $*"; }

log "wellbore EWA report"
$FETCH 650649b7-e019-4d77-a8e0-d118d6455381 --get OG_WELLBORE_EWA_Report.csv --out wellbore/OG_WELLBORE_EWA_Report.csv

log "P-5 orf850"
$FETCH 04652169-eed6-4396-9019-2e270e790f6c --get orf850.txt.gz --out p5/orf850.txt.gz

log "UIC uif700a + layouts"
for f in uif700a.txt.gz uif700a.Readme.txt uif700a.uimnh10_layout uif700a.uimnh10h_layout UIW700L2.txt; do
  $FETCH d2438c05-b42f-45a8-b0c6-edceb0912767 --get "$f" --out "uic/$f"
done

log "completions 2026 nightly zips"
$FETCH ed7ab066-879f-40b6-8144-2ae4b6810c04 --get-all --match -2026.zip --outdir completions

log "pass 2 done"
du -sh "$OUT"/*
