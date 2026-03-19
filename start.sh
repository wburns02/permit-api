#!/bin/sh
set -e

echo "Starting Tailscale..."
tailscaled --tun=userspace-networking --socks5-server=localhost:1055 --outbound-http-proxy-listen=localhost:1056 &
sleep 2
tailscale up --authkey="${TAILSCALE_AUTHKEY}" --hostname=permit-api-railway
echo "Tailscale connected"

# Wait for Tailscale to establish connection to T430
echo "Waiting for T430 connectivity..."
for i in $(seq 1 30); do
    if tailscale ping 100.122.216.15 --timeout=2s >/dev/null 2>&1; then
        echo "T430 reachable via Tailscale"
        break
    fi
    echo "  Attempt $i/30..."
    sleep 2
done

# Set up port forwarding: localhost:5432 -> T430:5432 via Tailscale SOCKS proxy
echo "Setting up database proxy..."
socat TCP-LISTEN:5432,fork,reuseaddr SOCKS4A:localhost:100.122.216.15:5432,socksport=1055 &
sleep 1

echo "Starting PermitLookup API..."
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}
