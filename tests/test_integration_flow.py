import json, time
from app.config import Config
from app.worker import Worker

SAMPLES = {
  "samples": [
    {"customerId": "1","name": "John Doe","email": "john@example.com","createdAt": "2024-03-26T12:00:00Z"},
    {"customerId": "1","name": "Jane Smith","email": "jane@example.com","createdAt": "2024-03-26T12:00:15Z"},
    {"customerId": "2","name": "Frank Miller","email": "invalid.email","createdAt": "2024-03-26T12:02:00Z"},
    {"customerId": "1","name": "Bob Wilson","email": "bob@example.com","createdAt": "2024-03-26T12:00:30Z"},
    {"customerId": "1","name": "Alice Brown","email": "alice@example.com","createdAt": "2024-03-26T12:00:45Z"},
    {"customerId": "1","name": "Charlie Davis","email": "charlie@example.com","createdAt": "2024-03-26T12:01:00Z"},
    {"customerId": "1","name": "Eve Wilson","email": "eve@example.com","createdAt": "2024-03-26T12:01:15Z"}
  ]
}

def test_integration_rate_limit_and_store(worker, redis_client, mongo_dao):
    cfg = Config()
    # push 7 events for customer 1 + 1 invalid for customer 2
    for rec in SAMPLES["samples"]:
        redis_client.rpush(cfg.QUEUE_KEY, json.dumps(rec).encode("utf-8"))

    # limit = 5/min; we don't want to wait 1 min, so force limit to 3 for test
    worker.ratelimiter.limit = 3
    worker.run(max_messages=len(SAMPLES["samples"]))

    # At most 3 ingested for customer 1, 1 invalid email rejected
    cnt = mongo_dao.col.count_documents({"customerId": "1"})
    assert cnt == 3
