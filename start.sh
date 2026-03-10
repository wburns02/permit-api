#!/bin/sh
set -e

if [ -n "$TAILSCALE_AUTHKEY" ]; then
    echo "Starting Tailscale..."
    tailscaled --tun=userspace-networking --socks5-server=localhost:1055 --state=/tmp/tailscale-state --socket=/tmp/tailscale.sock &
    sleep 3

    tailscale --socket=/tmp/tailscale.sock up --authkey="$TAILSCALE_AUTHKEY" --hostname=permit-api-railway
    echo "Tailscale connected:"
    tailscale --socket=/tmp/tailscale.sock ip -4

    # Configure dante socksify to use Tailscale SOCKS5 proxy
    cat > /etc/socks.conf <<SOCKSEOF
route {
    from: 0.0.0.0/0 to: 100.0.0.0/8 via: 127.0.0.1 port = 1055
    proxyprotocol: socks_v5
    method: none
}
SOCKSEOF

    echo "SOCKS5 proxy configured for Tailscale network (100.0.0.0/8)"
else
    echo "WARNING: TAILSCALE_AUTHKEY not set — Tailscale disabled, using DATABASE_URL directly"
fi

echo "Starting PermitLookup API..."
if [ -n "$TAILSCALE_AUTHKEY" ]; then
    # Use socksify to route all connections to Tailscale IPs through SOCKS5
    exec socksify uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}
else
    exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}
fi
