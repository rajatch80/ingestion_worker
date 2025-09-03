from __future__ import annotations
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from redis import Redis

from app.logger import get_logger
from app.config import cfg

log = get_logger()

# ------------------------------------------------------------------------------
# In-memory identity registry (per-process). Not persisted across restarts.
# Maps customerId -> (name, email)
# ------------------------------------------------------------------------------
_IDENTITY_CACHE: Dict[str, Tuple[str, str]] = {}


# ------------------------------
# Utility functions
# ------------------------------
def now_iso() -> str:
    """Return current UTC time in ISO 8601 with 'Z' suffix."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def rand_name() -> str:
    first = random.choice(
        [
            "John", "Jane", "Bob", "Alice", "Charlie", "Eve", "Grace", "Frank",
            "Ivy", "Liam", "Mia", "Noah", "Olivia", "Peter", "Quinn", "Rachel",
            "Sam", "Tom", "Sarah", "Mike", "Lisa", "David", "Emma", "James",
            "Mary", "Anna", "Chris", "Daniel", "Emily",
        ]
    )
    last = random.choice(
        [
            "Doe", "Smith", "Wilson", "Brown", "Davis", "Johnson", "Lee",
            "Miller", "Chen", "Davis", "Wilson", "Brown",
        ]
    )
    return f"{first} {last}"


def rand_email(name: str) -> str:
    user = name.lower().replace(" ", ".")
    domain = random.choice(["example.com", "gmail.com", "yahoo.com"])
    return f"{user}@{domain}"


def maybe_invalid_email(_: str) -> str:
    # ignore input; return a known-bad email pattern
    return random.choice(
        [
            "invalid.email",
            "@example.com",
            "mike@",
            "test.@.com",
            "david@example..com",
            "peter@example",
            "test@domain.123",
        ]
    )


def maybe_invalid_created_at() -> str:
    return random.choice(["2024-03-26", "2024-13-45T12:00:00Z", "not-a-date"])


# ------------------------------
# Identity helpers
# ------------------------------
def get_or_create_identity(customer_id: str) -> Tuple[str, str]:
    """
    Return a stable (name, email) for the given customer_id from in-memory cache.
    If not present, mint a new identity and store it in the cache.
    """
    ident = _IDENTITY_CACHE.get(customer_id)
    if ident is not None:
        return ident

    name = rand_name()
    email = rand_email(name)
    _IDENTITY_CACHE[customer_id] = (name, email)
    return name, email


# ------------------------------
# Record generator (uses stable identity, fresh createdAt)
# ------------------------------
def generate_record(customer_id: str, invalid_rate: float) -> dict:
    """
    Generate a record using a stable (name, email) per customer_id (in-memory cache).
    createdAt is always fresh. If invalid_rate triggers, corrupt ONLY the emitted
    record fields (do not mutate the cached identity).
    """
    base_name, base_email = get_or_create_identity(customer_id)
    created_at = now_iso()

    record = {
        "customerId": customer_id,
        "name": base_name,
        "email": base_email,
        "createdAt": created_at,
    }

    # Possibly emit an invalid variant without touching the stored identity
    if random.random() < max(0.0, min(1.0, invalid_rate)):
        field = random.choice(["email", "createdAt", "name", "customerId"])
        if field == "email":
            record["email"] = maybe_invalid_email(base_email)
        elif field == "createdAt":
            record["createdAt"] = maybe_invalid_created_at()
        elif field == "name":
            record["name"] = " "  # blank name in emitted record only
        elif field == "customerId":
            record["customerId"] = ""  # missing customerId in emitted record only

    return record


# ------------------------------
# Main loop
# ------------------------------
def main():
    redis = Redis.from_url(cfg.REDIS_URL, decode_responses=False)
    queue_key = cfg.QUEUE_KEY

    customers: List[str] = [c.strip() for c in cfg.GEN_CUSTOMERS.split(",") if c.strip()]
    rpm = max(1, int(cfg.GEN_RPM))
    per_customer_interval = 60.0 / rpm
    jitter_ms = int(cfg.GEN_JITTER_MS)
    invalid_rate = max(0.0, min(1.0, float(cfg.GEN_INVALID_RATE)))

    # Optionally warm identities so the first emit per customer is stable immediately
    for c in customers:
        get_or_create_identity(c)

    log.info(
        "generator_start",
        customers=customers,
        rpm=rpm,
        interval=per_customer_interval,
        invalid_rate=invalid_rate,
    )
    next_emit = {c: time.time() for c in customers}

    while True:
        now = time.time()
        for c in customers:
            if now >= next_emit[c]:
                payload = generate_record(c, invalid_rate)
                redis.rpush(queue_key, json.dumps(payload).encode("utf-8"))

                jitter = random.uniform(-jitter_ms / 1000.0, jitter_ms / 1000.0) if jitter_ms > 0 else 0.0
                next_emit[c] = now + max(0.1, per_customer_interval + jitter)

                log.info("queued", customerId=c)
        time.sleep(0.05)


if __name__ == "__main__":
    main()
