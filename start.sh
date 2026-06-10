#!/bin/sh
set -e

# ---------------------------------------------------------------------------
# Railway container entrypoint.
#
# The network bootstrap (Tailscale + SOCKS fallback proxies + Cloudflare PG
# tunnel) is guarded behind RAILWAY_ENVIRONMENT so this script degrades to a
# plain `exec uvicorn` on any other host (e.g. R730 systemd, which runs
# uvicorn directly anyway).
#
# Boot order is PARALLEL, not serial:
#   - tailscaled / `tailscale up` / the T430 ping check run in the background.
#     They only serve the FALLBACK DB path (127.0.0.1:5442) plus the R730-2
#     replica (5433) and anthropic proxy (9877).
#   - The SOCKS5 fallback proxies start immediately (they dial tailscaled's
#     SOCKS port lazily, per client connection, so they don't need to wait).
#   - The LIVE DB path is the cloudflared listener on 127.0.0.1:5432. uvicorn
#     is gated only on that listener being up (typically <3s).
#
# Combined with the /healthz deploy healthcheck in railway.toml, Railway keeps
# the previous container serving until uvicorn here responds, so the public
# domain never 502s during a deploy.
# ---------------------------------------------------------------------------

if [ -n "${RAILWAY_ENVIRONMENT}" ]; then

  # --- Tailscale: fallback/replica paths only — fully backgrounded -----------
  (
    echo "Starting Tailscale (background)..."
    tailscaled --tun=userspace-networking --socks5-server=localhost:1055 --outbound-http-proxy-listen=localhost:1056 &
    sleep 3
    # Reset Tailscale state so we always rejoin as the SAME node identity instead
    # of spawning a new permit-api-railway-N zombie on every container restart.
    tailscale up --reset --authkey="${TAILSCALE_AUTHKEY}" --hostname=permit-api-railway || echo "WARN: tailscale up failed"
    # Prefer the stable Ashburn (iad) DERP relay (TCP/443) over the flapping direct UDP path
    tailscale debug force-prefer-derp 27 2>/dev/null || true
    # Log (don't block on) route confirmation to T430 — this path is fallback only.
    for i in $(seq 1 30); do
      if tailscale ping --until-direct=false --c=1 100.122.216.15 > /dev/null 2>&1; then
        echo "Tailscale route to T430 confirmed after ${i}s"
        break
      fi
      [ "$i" -eq 30 ] && echo "WARN: Tailscale route to T430 not confirmed after 30s"
      sleep 1
    done
  ) &

  # --- SOCKS5 TCP fallback proxies (dial tailscaled lazily per connection) ---
  # Port 5442 → T430 (100.122.216.15:5432) — primary FALLBACK (live path is cloudflared on 5432)
  # Port 5433 → R730-2 (100.125.210.69:5432) — replica, handles reads
  # Port 9877 → R730-2 anthropic proxy
  python3 -c "
import socket, threading, struct, sys

def socks5_connect(target_host, target_port, proxy_host='127.0.0.1', proxy_port=1055):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((proxy_host, proxy_port))
    # SOCKS5 greeting
    s.send(b'\x05\x01\x00')
    resp = s.recv(2)
    if resp != b'\x05\x00':
        raise Exception(f'SOCKS5 greeting failed: {resp}')
    # SOCKS5 connect
    addr = socket.inet_aton(target_host)
    port_bytes = struct.pack('!H', target_port)
    s.send(b'\x05\x01\x00\x01' + addr + port_bytes)
    resp = s.recv(10)
    if resp[1] != 0:
        raise Exception(f'SOCKS5 connect failed: status {resp[1]}')
    return s

def forward(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except:
        pass
    finally:
        try: src.close()
        except: pass
        try: dst.close()
        except: pass

def make_handler(target_host, target_port, label):
    def handle_client(client):
        try:
            remote = socks5_connect(target_host, target_port)
            t1 = threading.Thread(target=forward, args=(client, remote), daemon=True)
            t2 = threading.Thread(target=forward, args=(remote, client), daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
        except Exception as e:
            print(f'Proxy error ({label}): {e}', file=sys.stderr)
            try: client.close()
            except: pass
    return handle_client

def start_proxy(local_port, target_host, target_port, label):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('127.0.0.1', local_port))
    server.listen(50)
    print(f'SOCKS5 TCP proxy listening on 127.0.0.1:{local_port} -> {label} ({target_host}:{target_port})')
    handler = make_handler(target_host, target_port, label)
    while True:
        client, addr = server.accept()
        threading.Thread(target=handler, args=(client,), daemon=True).start()

# Primary (T430) — writes. Kept on 5442 as a Tailscale FALLBACK only.
# The live DB path is the Cloudflare tunnel bound to 127.0.0.1:5432 (see below),
# because Railway's egress NAT destabilises the Tailscale direct path post-outage.
t_primary = threading.Thread(target=start_proxy, args=(5442, '100.122.216.15', 5432, 'T430-primary-fallback'), daemon=True)
t_primary.start()

# Replica (R730-2) — reads
t_replica = threading.Thread(target=start_proxy, args=(5433, '100.125.210.69', 5432, 'R730-2-replica'), daemon=True)
t_replica.start()

# Anthropic proxy (R730-2) — AI API calls
t_anthropic = threading.Thread(target=start_proxy, args=(9877, '100.125.210.69', 9877, 'R730-2-anthropic-proxy'), daemon=True)
t_anthropic.start()

# Keep main thread alive
t_primary.join()
" &

  # --- Live DB transport: Cloudflare tunnel client (outbound TCP/443) --------
  # Binds 127.0.0.1:5432 -> pg.ecbtx.com -> cloudflared on T430 -> Postgres.
  # Auth is the permit_api scram password in DATABASE_URL (pg.ecbtx.com has no
  # public port; reachable only via the cloudflared access protocol + DB password).
  echo "Starting Cloudflare tunnel client for Postgres (pg.ecbtx.com -> 127.0.0.1:5432)..."
  cloudflared access tcp --hostname pg.ecbtx.com --url 127.0.0.1:5432 > /tmp/cf-pg.log 2>&1 &
  # Gate uvicorn only on the LOCAL listener being up. NOTE: the old probe used
  # /dev/tcp, a bash-ism that always fails under dash (/bin/sh here), which
  # silently burned a guaranteed 30s on every boot. Use a real TCP connect.
  for i in $(seq 1 30); do
    if python3 -c "import socket; socket.create_connection(('127.0.0.1', 5432), 1).close()" 2>/dev/null; then
      echo "Cloudflare PG listener ready after ${i} attempt(s)"
      break
    fi
    [ "$i" -eq 30 ] && echo "WARN: Cloudflare PG listener not ready after 30 attempts; continuing anyway"
    sleep 1
  done

fi

echo "Starting PermitLookup API..."
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080} --timeout-keep-alive 30
