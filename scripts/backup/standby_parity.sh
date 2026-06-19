#!/bin/bash
# Standby parity check — verify Railway and R730 standby run identical main,
# and that T430 Postgres is reachable over both paths.
#
# Exit codes:
#   0 — all checks pass
#   1 — one or more parity failures
#
# Prerequisites: RAILWAY_TOKEN, CF_API_TOKEN set in environment;
#                psql and curl available.

set -euo pipefail

PASS=0
FAIL=0
NOTES=()

ok()   { echo "  OK   $*"; PASS=$((PASS+1)); }
fail() { echo "  FAIL $*"; FAIL=$((FAIL+1)); NOTES+=("$*"); }
warn() { echo "  WARN $*"; }

echo "===== Standby Parity Check — $(date -Iseconds) ====="

# ── 1. Git SHA match ──────────────────────────────────────────────────────────
echo ""
echo "── 1. Git commit parity ──"

LOCAL_SHA=$(git -C "$(dirname "$(realpath "$0")")/../.." rev-parse HEAD 2>/dev/null || echo "unknown")
echo "   local HEAD: $LOCAL_SHA"

# Fetch Railway's deployed SHA from the /health endpoint
RAILWAY_URL="${RAILWAY_APP_URL:-https://permits.ecbtx.com}"
RAILWAY_SHA=$(curl -sf "$RAILWAY_URL/health" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('git_sha','unknown'))" 2>/dev/null || echo "unknown")
echo "   Railway /health git_sha: $RAILWAY_SHA"

if [ "$RAILWAY_SHA" = "unknown" ]; then
    warn "Could not fetch Railway SHA (check RAILWAY_APP_URL or /health response format)"
elif [ "$LOCAL_SHA" = "$RAILWAY_SHA" ]; then
    ok "Railway SHA matches local main ($RAILWAY_SHA)"
else
    fail "SHA mismatch — local=$LOCAL_SHA Railway=$RAILWAY_SHA"
fi

# Check R730 standby via its cloudflared tunnel hostname
R730_URL="${R730_TUNNEL_URL:-http://localhost:8000}"
R730_SHA=$(curl -sf "$R730_URL/health" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('git_sha','unknown'))" 2>/dev/null || echo "unknown")
echo "   R730 /health git_sha: $R730_SHA"

if [ "$R730_SHA" = "unknown" ]; then
    warn "Could not fetch R730 SHA (check R730_TUNNEL_URL or R730 availability)"
elif [ "$LOCAL_SHA" = "$R730_SHA" ]; then
    ok "R730 SHA matches local main ($R730_SHA)"
else
    fail "SHA mismatch — local=$LOCAL_SHA R730=$R730_SHA"
fi

# ── 2. T430 reachability — cloudflared path (pg.ecbtx.com) ───────────────────
echo ""
echo "── 2. T430 reachability via pg.ecbtx.com ──"

if psql "postgresql://will@pg.ecbtx.com:5432/permits" \
       -c "SELECT 1 AS alive" -t -A --connect-timeout=10 >/dev/null 2>&1; then
    ok "T430 reachable via pg.ecbtx.com"
else
    fail "T430 NOT reachable via pg.ecbtx.com — cloudflared tunnel may be down"
fi

# ── 3. T430 reachability — Tailscale fallback (100.122.216.15) ───────────────
echo ""
echo "── 3. T430 reachability via Tailscale (100.122.216.15) ──"

if psql "postgresql://will@100.122.216.15:5432/permits" \
       -c "SELECT 1 AS alive" -t -A --connect-timeout=10 >/dev/null 2>&1; then
    ok "T430 reachable via Tailscale 100.122.216.15"
else
    fail "T430 NOT reachable via Tailscale 100.122.216.15 — check Tailscale status"
fi

# ── 4. Environment key-set diff ───────────────────────────────────────────────
echo ""
echo "── 4. Env-var key-set parity ──"

# Fetch Railway env-var key names via Railway CLI (if available)
if command -v railway >/dev/null 2>&1; then
    RAILWAY_KEYS=$(railway variables 2>/dev/null | awk '{print $1}' | sort || echo "")
    LOCAL_KEYS=$(env | cut -d= -f1 | sort)
    DIFF=$(comm -3 <(echo "$RAILWAY_KEYS") <(echo "$LOCAL_KEYS") || true)
    if [ -z "$DIFF" ]; then
        ok "Railway env keys match local environment"
    else
        warn "Env key diff detected (may be expected — review):"
        echo "$DIFF" | head -20 | sed 's/^/     /'
    fi
else
    warn "railway CLI not available — skipping env-var diff (install: npm i -g @railway/cli)"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "===== Parity check complete: $PASS passed, $FAIL failed ====="

if [ "${#NOTES[@]}" -gt 0 ]; then
    echo "Failures:"
    for NOTE in "${NOTES[@]}"; do
        echo "  - $NOTE"
    done
fi

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
