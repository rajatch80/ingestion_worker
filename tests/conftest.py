import os, time, json, pytest
from redis import Redis
from pymongo import MongoClient

from app.config import Config
from app.worker import Worker
from app.db import MongoDAO

@pytest.fixture(scope="session")
def test_cfg():
    # point tests at local docker or your env
    os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
    os.environ.setdefault("QUEUE_KEY", "test:ingest:queue")
    os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
    os.environ.setdefault("MONGO_DB", "ingestion_test")
    os.environ.setdefault("MONGO_COLLECTION", "customers")
    os.environ.setdefault("RATE_LIMIT_LIMIT", "5")
    os.environ.setdefault("RATE_LIMIT_WINDOW_SEC", "60")
    return Config()

@pytest.fixture(autouse=True)
def cleanup_redis_mongo(test_cfg):
    r = Redis.from_url(test_cfg.REDIS_URL, decode_responses=False)
    r.delete(test_cfg.QUEUE_KEY)
    # clean rate keys
    for key in r.scan_iter("rate:*"):
        r.delete(key)

    mc = MongoClient(test_cfg.MONGO_URI)
    mc.drop_database(test_cfg.MONGO_DB)
    yield
    r.delete(test_cfg.QUEUE_KEY)
    for key in r.scan_iter("rate:*"):
        r.delete(key)
    mc.drop_database(test_cfg.MONGO_DB)

@pytest.fixture
def redis_client(test_cfg):
    return Redis.from_url(test_cfg.REDIS_URL, decode_responses=False)

@pytest.fixture
def mongo_dao(test_cfg):
    return MongoDAO(test_cfg.MONGO_URI, test_cfg.MONGO_DB, test_cfg.MONGO_COLLECTION)

@pytest.fixture
def worker(test_cfg):
    return Worker(test_cfg.REDIS_URL, test_cfg.QUEUE_KEY)

def push(redis_client, key, item):
    redis_client.rpush(key, json.dumps(item).encode("utf-8"))
