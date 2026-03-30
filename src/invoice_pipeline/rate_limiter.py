"""Redis token bucket rate limiter for Gemini API calls.

Shared across all image worker instances via Redis.  The Lua script executes
atomically — token check and decrement happen in a single round-trip with no
race condition possible.

Usage:
    limiter = GeminiRateLimiter(redis_client, rpm=settings.GEMINI_RATE_LIMIT_RPM)
    limiter.acquire()   # blocks (sleeps locally) until a token is available
"""
from __future__ import annotations

import time

import redis as redis_lib

from invoice_pipeline import settings

# Lua script: check and decrement one token.
# Returns 1 if a token was acquired, 0 if the bucket is empty.
_RATE_LIMIT_SCRIPT = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local refill_rate = tonumber(ARGV[3])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1]) or capacity
local last_refill = tonumber(bucket[2]) or now

local elapsed = now - last_refill
local refill = elapsed * refill_rate
tokens = math.min(capacity, tokens + refill)

if tokens >= 1 then
    tokens = tokens - 1
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    return 1
else
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
    return 0
end
"""


class GeminiRateLimiter:
    def __init__(self, redis_client: redis_lib.Redis, rpm: int, key: str | None = None) -> None:  # type: ignore[type-arg]
        self._redis = redis_client
        self._rpm = rpm
        self._refill_rate = rpm / 60.0  # tokens per second
        self._key = key or settings.GEMINI_RATE_LIMITER_KEY
        self._sha: str | None = None

    def _get_sha(self) -> str:
        if self._sha is None:
            self._sha = self._redis.script_load(_RATE_LIMIT_SCRIPT)
        return self._sha

    def acquire(self, max_wait: float = 120.0, poll_interval: float = 1.0) -> None:
        """Block until a token is available.

        Workers that cannot acquire a token sleep locally — they do not requeue
        the Celery task (which would waste a queue slot and a DB round-trip).

        Raises TimeoutError if max_wait is exceeded.
        """
        deadline = time.monotonic() + max_wait
        while True:
            sha = self._get_sha()
            try:
                result = self._redis.evalsha(
                    sha,
                    1,
                    self._key,
                    self._rpm,
                    time.time(),
                    self._refill_rate,
                )
            except redis_lib.ResponseError:
                # Script evicted from Redis script cache — reload and retry.
                self._sha = None
                continue

            if result == 1:
                return

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Gemini rate limiter: could not acquire token within {max_wait}s"
                )
            time.sleep(min(poll_interval, remaining))


_limiter: GeminiRateLimiter | None = None


def get_limiter(redis_client: redis_lib.Redis) -> GeminiRateLimiter:  # type: ignore[type-arg]
    """Return the process-level rate limiter, creating it on first call."""
    global _limiter
    if _limiter is None:
        _limiter = GeminiRateLimiter(redis_client, rpm=settings.GEMINI_RATE_LIMIT_RPM)
    return _limiter
