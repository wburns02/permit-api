#!/usr/bin/env bash
# R730 auto-deploy: pull main, restart, smoke, roll back on failure.
# Installed as systemd timer permit-api-deploy.timer (every 5 min).
# Runs as user `will` (passwordless sudo for systemctl restart).
set -euo pipefail

REPO=/home/will/permit-api-live
LOG=/home/will/logs/permit-api-deploy.log
mkdir -p "$(dirname "$LOG")"

log() { echo "$(date -Is) $*" >> "$LOG"; }

cd "$REPO"
git fetch -q origin main

LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)
[ "$LOCAL" = "$REMOTE" ] && exit 0

log "deploying $LOCAL -> $REMOTE"
git reset --hard -q origin/main
sudo systemctl restart permit-api.service
sleep 5

smoke() {
  local health search paths
  health=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 http://localhost:8080/health)
  search=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "http://localhost:8080/v1/permits/search?q=smoke")
  paths=$(curl -s --max-time 10 http://localhost:8080/openapi.json | python3 -c 'import sys,json;print(len(json.load(sys.stdin)["paths"]))' 2>/dev/null || echo 0)
  [ "$health" = "200" ] && [ "$search" = "401" ] && [ "$paths" -ge 196 ]
}

if smoke; then
  log "deploy OK at $REMOTE (health 200, auth 401, paths >=196)"
else
  log "SMOKE FAILED at $REMOTE, rolling back to $LOCAL"
  git reset --hard -q "$LOCAL"
  sudo systemctl restart permit-api.service
  sleep 5
  if smoke; then
    log "rollback OK at $LOCAL"
  else
    log "ROLLBACK SMOKE ALSO FAILED, service needs a human"
  fi
  exit 1
fi
