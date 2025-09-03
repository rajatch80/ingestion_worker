from __future__ import annotations
import os, signal, sys, time
from typing import Optional
from redis import Redis
from .config import cfg
from .logger import get_logger
from .queue_client import QueueClient
from .validator import validate_record
from .rate_limiter import RateLimiter
from .db import MongoDAO

log = get_logger()
shutdown = False

def _handle_sigterm(signum, frame):
    global shutdown
    shutdown = True
    log.info("signal_received", signum=signum)

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

class Worker:
    def __init__(self, redis_url: str, queue_key: str):
        self.redis = Redis.from_url(redis_url, decode_responses=False)
        self.queue = QueueClient(self.redis, queue_key)
        self.dao = MongoDAO(cfg.MONGO_URI, cfg.MONGO_DB, cfg.MONGO_COLLECTION)
        self.ratelimiter = RateLimiter(self.redis, cfg.RATE_LIMIT_LIMIT, cfg.RATE_LIMIT_WINDOW_SEC)

    def process_one(self) -> Optional[bool]:
        popped = self.queue.pop(timeout=cfg.WORKER_POLL_TIMEOUT_SEC)
        if popped is None:
            return None  # timeout / idle
        key, item = popped

        # guard parse errors
        if item.get("__parse_error__"):
            log.error("parse_error", status="error", reason="Invalid JSON", raw=item.get("__raw__"))
            return False

        ok, payload = validate_record(item)
        if not ok:
            log.error("validation_failed", **payload)
            return False

        customer_id = item.get("customerId")

        # rate limiting based on processing time (ingest time)
        allowed = self.ratelimiter.allow(customer_id)
        if not allowed:
            log.error("rate_limited", status="error", customerId=customer_id, reason="Rate limit exceeded")
            return False

        # insert
        _id = self.dao.insert_record(payload)
        log.info("ingested", status="success", customerId=customer_id, _id=_id)
        return True

    def run(self, max_messages: int = 0):
        processed = 0
        while not shutdown:
            res = self.process_one()
            if res is not None:
                processed += 1
            if max_messages and processed >= max_messages:
                break

if __name__ == "__main__":
    log.info("worker_start", redis=cfg.REDIS_URL, queue=cfg.QUEUE_KEY)
    w = Worker(cfg.REDIS_URL, cfg.QUEUE_KEY)
    w.run(max_messages=cfg.WORKER_MAX_MESSAGES)
    log.info("worker_exit")
