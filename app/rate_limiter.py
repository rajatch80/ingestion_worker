from __future__ import annotations
import time
from redis import Redis
from typing import Optional

# Atomic sliding-window limiter using Redis ZSET + EXPIRE via Lua.
RATE_LIMIT_LUA = """
-- KEYS[1] = zset key
-- ARGV[1] = now_ms
-- ARGV[2] = window_ms
-- ARGV[3] = limit
-- Steps:
-- 1) Remove timestamps outside window
-- 2) Count remaining
-- 3) If < limit: add now_ms, set expire, return 1 else return 0
local key       = KEYS[1]
local now_ms    = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local limit     = tonumber(ARGV[3])

redis.call('ZREMRANGEBYSCORE', key, 0, now_ms - window_ms)
local count = redis.call('ZCARD', key)
if count < limit then
  redis.call('ZADD', key, now_ms, tostring(now_ms))
  -- expire a bit beyond window to avoid leaks
  redis.call('PEXPIRE', key, window_ms + 2000)
  return 1
else
  return 0
end
"""

class RateLimiter:
    def __init__(self, redis: Redis, limit: int, window_sec: int, prefix: str = "rate"):
        self.redis = redis
        self.limit = int(limit)
        self.window_ms = int(window_sec * 1000)
        self.prefix = prefix
        self._lua = self.redis.register_script(RATE_LIMIT_LUA)

    def key(self, customer_id: str) -> str:
        return f"{self.prefix}:{customer_id}"

    def allow(self, customer_id: str, now_ms: Optional[int] = None) -> bool:
        if not customer_id:
            return False
        now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        res = self._lua(keys=[self.key(customer_id)], args=[now_ms, self.window_ms, self.limit])
        return bool(int(res))
