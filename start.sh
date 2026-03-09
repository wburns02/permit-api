#!/bin/sh
set -e

if [ -n "$TAILSCALE_AUTHKEY" ]; then
    echo "Starting Tailscale..."
    tailscaled --tun=userspace-networking --state=/tmp/tailscale-state --socket=/tmp/tailscale.sock --socks5-server=localhost:1055 &
    sleep 3

    tailscale --socket=/tmp/tailscale.sock up --authkey="$TAILSCALE_AUTHKEY" --hostname=permit-api-railway
    echo "Tailscale connected:"
    tailscale --socket=/tmp/tailscale.sock ip -4

    # Forward local port 15432 → R730 PostgreSQL (100.85.99.69:5432) via Tailscale SOCKS5
    # This lets asyncpg connect to localhost:15432 without SOCKS support
    echo "Setting up PostgreSQL tunnel to R730..."
    socat TCP-LISTEN:15432,fork,reuseaddr SOCKS4A:127.0.0.1:100.85.99.69:5432,socksport=1055 &
    sleep 1

    echo "PostgreSQL tunnel ready on localhost:15432"
else
    echo "WARNING: TAILSCALE_AUTHKEY not set — Tailscale disabled, using DATABASE_URL directly"
fi

echo "Starting PermitLookup API..."
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}
