#!/usr/bin/env bash
# Control the statewide permit loop. Usage: ./loopctl.sh {pause|resume|stop|status|watch}
#
#   pause   graceful: stop claiming new jurisdictions, let the <=2 in-flight
#           agents finish, then the driver exits. Progress is in registry.db.
#           Use this BEFORE taking the permits DB down for maintenance.
#   resume  clear the pause and relaunch; picks up remaining `pending` rows.
#   stop    hard SIGTERM the driver now (in-flight agents may linger a moment).
#   status  is the driver running + registry counts.
#   watch   tail the live log.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"
PAUSE="$HERE/PAUSE"
LOG="$HERE/loop_live.log"

case "${1:-status}" in
  pause)
    touch "$PAUSE"
    echo "PAUSE set. Driver will stop claiming new work and exit after in-flight"
    echo "agents finish (<=2, up to ~15 min each). Watch: $0 watch"
    ;;
  resume)
    rm -f "$PAUSE"
    if pgrep -f "run_loop.py run" >/dev/null; then
      echo "driver already running"; exit 0
    fi
    set -a; . /home/will/.config/macseptic/mgo.env 2>/dev/null || true; set +a
    nohup python3 run_loop.py run >> "$LOG" 2>&1 &
    echo "resumed pid=$! (logging to $LOG)"
    ;;
  stop)
    pkill -f "run_loop.py run" && echo "SIGTERM sent to driver" || echo "no driver running"
    ;;
  status)
    pgrep -af "run_loop.py run" | grep -v grep || echo "driver: not running"
    [ -f "$PAUSE" ] && echo "PAUSE: present (will not spawn)" || echo "PAUSE: clear"
    python3 run_loop.py status 2>/dev/null | head -1
    ;;
  watch)
    tail -f "$LOG"
    ;;
  *)
    echo "usage: $0 {pause|resume|stop|status|watch}"; exit 2
    ;;
esac
