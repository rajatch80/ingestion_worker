from __future__ import annotations
import json
from typing import Optional, Tuple
from redis import Redis
from .config import cfg

class QueueClient:
    def __init__(self, redis: Redis, key: str):
        self.redis = redis
        self.key = key

    def push(self, item: dict) -> None:
        self.redis.rpush(self.key, json.dumps(item))

    def pop(self, timeout: int) -> Optional[Tuple[str, dict]]:
        res = self.redis.blpop(self.key, timeout=timeout)
        if not res:
            return None
        _, raw = res
        try:
            return self.key, json.loads(raw.decode("utf-8"))
        except Exception:
            # poison pill protection or corrupted payload
            return self.key, {"__raw__": raw.decode("utf-8"), "__parse_error__": True}
