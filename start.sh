#!/bin/sh
set -e

if [ -n "$TAILSCALE_AUTHKEY" ]; then
    echo "Starting Tailscale..."
    tailscaled --tun=userspace-networking --state=/tmp/tailscale-state --socket=/tmp/tailscale.sock &
    sleep 3

    tailscale --socket=/tmp/tailscale.sock up --authkey="$TAILSCALE_AUTHKEY" --hostname=permit-api-railway
    echo "Tailscale connected:"
    tailscale --socket=/tmp/tailscale.sock ip -4

    # Forward local port 15432 → R730 PostgreSQL (100.85.99.69:5432) via tailscale nc
    # Use stdbuf to disable pipe buffering (prevents hangs on larger result sets)
    echo "Setting up PostgreSQL tunnel to R730..."
    socat TCP-LISTEN:15432,fork,reuseaddr EXEC:"stdbuf -i0 -o0 -e0 tailscale --socket=/tmp/tailscale.sock nc 100.85.99.69 5432" &
    sleep 1

    # Verify tunnel works
    if socat -T2 - TCP:localhost:15432 < /dev/null 2>/dev/null; then
        echo "PostgreSQL tunnel verified on localhost:15432"
    else
        echo "WARNING: PostgreSQL tunnel may not be ready yet"
    fi
else
    echo "WARNING: TAILSCALE_AUTHKEY not set — Tailscale disabled, using DATABASE_URL directly"
fi

echo "Starting PermitLookup API..."
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}
