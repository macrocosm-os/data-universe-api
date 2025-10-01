from __future__ import annotations
from abc import ABC, abstractmethod
import redis.asyncio as redis


class RateLimiter(ABC):
    @abstractmethod
    async def hit(self, key: str, op: str, *, limit: int, window: int) -> bool:
        """
        Atomically increment (+1) the counter for (key, op) within a fixed window.
        Returns False iff the NEW count is > limit (i.e., reject this hit).
        Must set TTL on the first hit in a window to `window` seconds.
        """
        ...

    @abstractmethod
    async def check(self, key: str, op: str, *, limit: int) -> bool:
        """
        Read-only. Does NOT modify counters or TTL.
        Returns False iff current count is >= limit (i.e., a new hit would be rejected).
        """
        ...

    @abstractmethod
    async def readyz(self) -> bool:
        """
        Read-only health check. Returns True iff the limiter's backend is reachable
        and ready to serve requests.
        """
        ...


class NoRateLimiter(RateLimiter):
    """
    No-op implementation: always allows.
    Useful for local/dev or when limiting is disabled by config.
    """

    async def hit(self, key: str, op: str, *, limit: int, window: int) -> bool:
        return True

    async def check(self, key: str, op: str, *, limit: int) -> bool:
        return True

    async def readyz(self) -> bool:
        return True


class RedisRateLimiter(RateLimiter):
    """
    Minimal per-key, per-operation fixed-window rate limiter.
    Key format: {prefix}:{op}:{key}

    Semantics:
    - hit(): INCR; if first hit set EXPIRE(window); return count <= limit
    - check(): GET; return count < limit
    """

    # Atomic: INCR; if first hit set EXPIRE; return count
    _HIT_LUA = """
    local k = KEYS[1]
    local window = tonumber(ARGV[1])
    if not window or window <= 0 then
        return redis.error_reply("window must be > 0")
    end
    local c = redis.call('INCR', k)
    if c == 1 then
        redis.call('EXPIRE', k, window)
    end
    return c
    """

    def __init__(
        self, client: redis.Redis, *, prefix: str = "data-universe-api"
    ) -> None:
        self.client = client
        self.prefix = prefix

    def _rkey(self, key: str, op: str) -> str:
        return f"{self.prefix}:{op}:{key}"

    async def hit(self, key: str, op: str, *, limit: int, window: int) -> bool:
        if limit <= 0:
            return False
        if window <= 0:
            raise ValueError("window must be > 0")

        rkey = self._rkey(key, op)
        count = await self.client.eval(self._HIT_LUA, 1, rkey, int(window))
        return int(count) <= int(limit)

    async def check(self, key: str, op: str, *, limit: int) -> bool:
        if limit <= 0:
            return False

        rkey = self._rkey(key, op)
        raw = await self.client.get(rkey)
        count = int(raw) if raw is not None else 0
        return count < int(limit)

    async def readyz(self) -> bool:
        try:
            # redis.asyncio exposes `ping()` as an async method.
            pong = await self.client.ping()
            # Some Redis clients return True, others return b'PONG'
            return bool(pong)
        except Exception:
            return False
