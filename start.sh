#!/bin/sh
set -e

if [ -n "$TAILSCALE_AUTHKEY" ]; then
    echo "Starting Tailscale..."
    tailscaled --tun=userspace-networking --socks5-server=localhost:1055 --state=/tmp/tailscale-state --socket=/tmp/tailscale.sock &
    sleep 3

    # Use --force-reauth to get a fresh session, avoids stale node conflicts
    tailscale --socket=/tmp/tailscale.sock up --authkey="$TAILSCALE_AUTHKEY" --hostname=permit-api-railway --force-reauth
    echo "Tailscale connected:"
    tailscale --socket=/tmp/tailscale.sock ip -4

    # Forward local port 15432 → R730 PostgreSQL via SOCKS5 tunnel proxy
    echo "Setting up PostgreSQL tunnel to R730..."
    python3 /app/tunnel_proxy.py &
    sleep 1

    # Verify tunnel works
    if socat -T2 - TCP:localhost:15432 < /dev/null 2>/dev/null; then
        echo "PostgreSQL tunnel verified on localhost:15432"
    else
        echo "WARNING: PostgreSQL tunnel may not be ready yet"
    fi

    # Keepalive: ping R730 every 60s to prevent Railway NAT from killing
    # idle Tailscale connections (PollNetMap: unexpected EOF after ~2.5min)
    (
        while true; do
            sleep 60
            tailscale --socket=/tmp/tailscale.sock ping --timeout=5s 100.85.99.69 >/dev/null 2>&1 || true
        done
    ) &

    # Watchdog: reconnect Tailscale if connection drops
    (
        while true; do
            sleep 30
            if ! tailscale --socket=/tmp/tailscale.sock status >/dev/null 2>&1; then
                echo "WATCHDOG: Tailscale connection lost, reconnecting..."
                tailscale --socket=/tmp/tailscale.sock up --authkey="$TAILSCALE_AUTHKEY" --hostname=permit-api-railway --force-reauth 2>/dev/null || true
                sleep 5
            fi
        done
    ) &
else
    echo "WARNING: TAILSCALE_AUTHKEY not set — Tailscale disabled, using DATABASE_URL directly"
fi

echo "Starting PermitLookup API..."
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}
