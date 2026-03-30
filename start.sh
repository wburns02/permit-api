#!/bin/sh
set -e

echo "Starting Tailscale..."
tailscaled --tun=userspace-networking --socks5-server=localhost:1055 --outbound-http-proxy-listen=localhost:1056 &
sleep 3
tailscale up --authkey="${TAILSCALE_AUTHKEY}" --hostname=permit-api-railway
echo "Tailscale connected, waiting for routes..."
sleep 5

# SOCKS5 TCP proxy: forwards local ports through Tailscale to PostgreSQL servers
# Port 5432 → T430 (100.122.216.15:5432) — primary, handles writes
# Port 5433 → R730-2 (100.87.214.106:5432) — replica, handles reads
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

# Primary (T430) — writes
t_primary = threading.Thread(target=start_proxy, args=(5432, '100.122.216.15', 5432, 'T430-primary'), daemon=True)
t_primary.start()

# Replica (R730-2) — reads
t_replica = threading.Thread(target=start_proxy, args=(5433, '100.87.214.106', 5432, 'R730-2-replica'), daemon=True)
t_replica.start()

# Keep main thread alive
t_primary.join()
" &
sleep 2

echo "Starting PermitLookup API..."
# Set HTTP proxy for outbound HTTPS (Tailscale outbound proxy)
# This ensures httpx (used by Anthropic SDK) routes through Tailscale correctly
export HTTPS_PROXY=http://localhost:1056
export HTTP_PROXY=http://localhost:1056
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080} --timeout-keep-alive 30
