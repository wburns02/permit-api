"""In-process TTL cache for slow read-only endpoints.

Dependency-free: just dict + asyncio.Lock + monotonic timestamps.
Used by freshness endpoints where the underlying queries (e.g. GROUP BY
on 15M-row hot_leads) cost tens of seconds and we don't need second-level
accuracy.

Pattern:
    cache = TTLCache(ttl_seconds=300)

    async def endpoint():
        return await cache.get_or_set("key", compute_coro_factory)

The factory MUST be a zero-arg callable returning a coroutine (so we only
materialize the awaitable when we actually need to refresh).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Dict

logger = logging.getLogger(__name__)


class TTLCache:
    def __init__(self, ttl_seconds: int = 300) -> None:
        self.ttl = ttl_seconds
        self._store: Dict[str, tuple[float, Any]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def _lock_for(self, key: str) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    def peek(self, key: str) -> Any | None:
        """Return cached value (any age) or None. Does not refresh."""
        entry = self._store.get(key)
        return entry[1] if entry else None

    def is_fresh(self, key: str) -> bool:
        entry = self._store.get(key)
        if entry is None:
            return False
        ts, _ = entry
        return (time.monotonic() - ts) < self.ttl

    def set(self, key: str, value: Any) -> None:
        self._store[key] = (time.monotonic(), value)

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[Any]],
    ) -> Any:
        """Return cached value if fresh, otherwise compute, cache, and return.

        Concurrent callers for the same key serialize on a per-key lock so we
        compute at most once per refresh window.
        """
        if self.is_fresh(key):
            return self._store[key][1]

        lock = self._lock_for(key)
        async with lock:
            # Re-check after acquiring the lock — another coroutine may have
            # refreshed while we waited.
            if self.is_fresh(key):
                return self._store[key][1]
            try:
                value = await factory()
            except Exception as e:
                logger.exception("endpoint_cache factory failed for key=%s: %s", key, e)
                # If we have any stale value, serve it; else re-raise.
                stale = self.peek(key)
                if stale is not None:
                    return stale
                raise
            self.set(key, value)
            return value


# Module-level singleton — 5-minute TTL by default.
freshness_cache = TTLCache(ttl_seconds=300)
