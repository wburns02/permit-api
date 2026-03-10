"""Async TCP proxy: localhost:15432 → R730 PostgreSQL via Tailscale SOCKS5 proxy.

Uses Tailscale's built-in SOCKS5 proxy (port 1055) for reliable TCP streaming,
avoiding tailscale nc subprocess pipe buffering issues.
"""

import asyncio
import struct
import sys


async def socks5_connect(dest_host: str, dest_port: int, proxy_host: str = "127.0.0.1", proxy_port: int = 1055):
    """Connect to a destination through a SOCKS5 proxy, return (reader, writer)."""
    reader, writer = await asyncio.open_connection(proxy_host, proxy_port)

    # SOCKS5 greeting: version=5, 1 auth method, no auth (0x00)
    writer.write(b"\x05\x01\x00")
    await writer.drain()

    # Server response: version + chosen method
    resp = await reader.readexactly(2)
    if resp != b"\x05\x00":
        raise ConnectionError(f"SOCKS5 auth failed: {resp.hex()}")

    # SOCKS5 connect request: version=5, cmd=connect(1), reserved=0, addr_type=IPv4(1)
    addr_bytes = bytes(int(x) for x in dest_host.split("."))
    port_bytes = struct.pack("!H", dest_port)
    writer.write(b"\x05\x01\x00\x01" + addr_bytes + port_bytes)
    await writer.drain()

    # Server response: version, status, reserved, addr_type
    resp = await reader.readexactly(4)
    if resp[1] != 0x00:
        raise ConnectionError(f"SOCKS5 connect failed: status={resp[1]}")

    # Read bound address (skip it)
    if resp[3] == 0x01:  # IPv4
        await reader.readexactly(4 + 2)  # 4 bytes addr + 2 bytes port
    elif resp[3] == 0x03:  # Domain
        domain_len = (await reader.readexactly(1))[0]
        await reader.readexactly(domain_len + 2)
    elif resp[3] == 0x04:  # IPv6
        await reader.readexactly(16 + 2)

    return reader, writer


async def pipe_data(src_reader, dst_writer, label=""):
    """Copy data from reader to writer until EOF."""
    try:
        while True:
            data = await src_reader.read(65536)
            if not data:
                break
            dst_writer.write(data)
            await dst_writer.drain()
    except (ConnectionError, BrokenPipeError, asyncio.CancelledError, asyncio.IncompleteReadError):
        pass


async def handle_client(client_reader, client_writer):
    """Forward TCP traffic between client and R730 PostgreSQL via SOCKS5."""
    try:
        pg_reader, pg_writer = await socks5_connect("100.85.99.69", 5432)
    except Exception as e:
        print(f"SOCKS5 connect failed: {e}", flush=True)
        client_writer.close()
        return

    try:
        await asyncio.gather(
            pipe_data(client_reader, pg_writer, "client→pg"),
            pipe_data(pg_reader, client_writer, "pg→client"),
        )
    except Exception:
        pass
    finally:
        pg_writer.close()
        client_writer.close()


async def main():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 15432)
    print("PostgreSQL SOCKS5 tunnel proxy listening on localhost:15432", flush=True)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
