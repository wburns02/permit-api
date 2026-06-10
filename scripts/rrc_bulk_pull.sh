#!/usr/bin/env bash
# RRC bulk acquisition driver. Lands files in /mnt/win11/Fedora/raw-public-data/rrc/.
# Storage rule: NEVER /home/will or /tmp for data.
set -uo pipefail
FETCH="python3 /home/will/permit-api-live/scripts/rrc_mft_fetch.py"
OUT=/mnt/win11/Fedora/raw-public-data/rrc
mkdir -p "$OUT"/{pdq,drilling_permits,wellbore,p5,uic,completions}
cd "$OUT"

log() { echo "[$(date +%H:%M:%S)] $*"; }

# 1. PDQ production dump (the big one) in background of this script
log "starting PDQ_DSV.zip"
$FETCH 1f5ddb8d-329a-4459-b7f8-177b4f5ee60d --get PDQ_DSV.zip --out pdq/PDQ_DSV.zip &
PDQ_PID=$!

# 2. Drilling permit daily master+trailer: current + yearly archives
log "drilling permit daf420 archives"
$FETCH 5f07cc72-2e79-4df8-ade1-9aeb792e03fc --get-all --match daf420 --outdir drilling_permits

# 3. Full historical drilling permit master+trailer (monthly snapshot)
log "drilling permit full master (monthly link)"
$FETCH beeeab0c-7d07-4111-af88-783c93677b2c --get-all --outdir drilling_permits

# 4. Wellbore EWA current report
log "wellbore EWA report"
$FETCH 650649b7-e019-4d77-a8e0-d118d6455381 --get OG_WELLBORE_EWA_Report.csv --out wellbore/OG_WELLBORE_EWA_Report.csv

# 5. P-5 organizations (ASCII)
log "P-5 orf850"
$FETCH 04652169-eed6-4396-9019-2e270e790f6c --get orf850.txt.gz --out p5/orf850.txt.gz

# 6. UIC injection (ASCII + layouts)
log "UIC uif700a"
for f in uif700a.txt.gz uif700a.Readme.txt uif700a.uimnh10_layout uif700a.uimnh10h_layout UIW700L2.txt; do
  $FETCH d2438c05-b42f-45a8-b0c6-edceb0912767 --get "$f" --out "uic/$f"
done

# 7. Completions: 2026 nightly zips (full historical sweep is a later pass)
log "completions 2026"
$FETCH ed7ab066-879f-40b6-8144-2ae4b6810c04 --get-all --match -2026.zip --outdir completions

log "waiting on PDQ"
wait $PDQ_PID && log "PDQ done" || log "PDQ FAILED"
ls -laR "$OUT" | tail -40
log "all done"
