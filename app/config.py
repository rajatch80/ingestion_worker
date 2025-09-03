from __future__ import annotations
import os
from dataclasses import dataclass

def getenv_str(name: str, default: str) -> str:
    return os.getenv(name, default)

def getenv_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def getenv_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default

@dataclass(frozen=True)
class Config:
    LOG_LEVEL: str = getenv_str("LOG_LEVEL", "INFO")

    REDIS_URL: str = getenv_str("REDIS_URL", "redis://localhost:6379/0")
    QUEUE_KEY: str = getenv_str("QUEUE_KEY", "ingest:queue")

    RATE_LIMIT_LIMIT: int = getenv_int("RATE_LIMIT_LIMIT", 5)
    RATE_LIMIT_WINDOW_SEC: int = getenv_int("RATE_LIMIT_WINDOW_SEC", 60)

    MONGO_URI: str = getenv_str("MONGO_URI", "mongodb://localhost:27017")
    MONGO_DB: str = getenv_str("MONGO_DB", "ingestion")
    MONGO_COLLECTION: str = getenv_str("MONGO_COLLECTION", "customers")

    WORKER_POLL_TIMEOUT_SEC: int = getenv_int("WORKER_POLL_TIMEOUT_SEC", 5)
    WORKER_MAX_MESSAGES: int = getenv_int("WORKER_MAX_MESSAGES", 0)

    # generator
    GEN_CUSTOMERS: str = getenv_str("GEN_CUSTOMERS", "1,2,3")
    GEN_RPM: int = getenv_int("GEN_RPM", 5)
    GEN_JITTER_MS: int = getenv_int("GEN_JITTER_MS", 300)
    GEN_INVALID_RATE: float = getenv_float("GEN_INVALID_RATE", 0.05)

cfg = Config()
