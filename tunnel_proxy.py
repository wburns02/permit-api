"""Async TCP proxy: localhost:15432 → R730 PostgreSQL via tailscale nc.

Replaces socat EXEC which has pipe buffering issues causing hangs
on responses larger than ~1KB.
"""

import asyncio
import sys


async def handle_client(reader, writer):
    """Forward TCP traffic between client and tailscale nc subprocess."""
    proc = await asyncio.create_subprocess_exec(
        "tailscale", "--socket=/tmp/tailscale.sock", "nc", "100.85.99.69", "5432",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )

    async def client_to_pg():
        """Forward client data to PostgreSQL via tailscale nc stdin."""
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    if proc.stdin:
                        proc.stdin.close()
                    break
                proc.stdin.write(data)
                await proc.stdin.drain()
        except (ConnectionError, BrokenPipeError, asyncio.CancelledError):
            pass

    async def pg_to_client():
        """Forward PostgreSQL responses to client via tailscale nc stdout."""
        try:
            while True:
                data = await proc.stdout.read(65536)
                if not data:
                    writer.close()
                    break
                writer.write(data)
                await writer.drain()
        except (ConnectionError, BrokenPipeError, asyncio.CancelledError):
            pass

    try:
        await asyncio.gather(client_to_pg(), pg_to_client())
    except Exception:
        pass
    finally:
        if proc.returncode is None:
            proc.kill()
        writer.close()


async def main():
    server = await asyncio.start_server(handle_client, "127.0.0.1", 15432)
    print("PostgreSQL tunnel proxy listening on localhost:15432", flush=True)
    await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
