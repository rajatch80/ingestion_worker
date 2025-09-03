import json
from app.worker import Worker
from app.config import Config

def test_worker_processes_valid_and_rejects_invalid(worker, redis_client, mongo_dao):
    q = Config().QUEUE_KEY
    # Valid
    redis_client.rpush(q, json.dumps({
        "customerId":"1","name":"John Doe","email":"john@example.com","createdAt":"2024-03-26T12:00:00Z"
    }).encode("utf-8"))
    # Invalid email
    redis_client.rpush(q, json.dumps({
        "customerId":"2","name":"Jane","email":"invalid.email","createdAt":"2024-03-26T12:00:00Z"
    }).encode("utf-8"))
    # Parse error
    redis_client.rpush(q, b'{"bad_json": ')  # truncated

    # Rate limit small: set to 1 and push twice
    worker.ratelimiter.limit = 1
    # For customer 3: first ok, second blocked
    redis_client.rpush(q, json.dumps({
        "customerId":"3","name":"A","email":"a@example.com","createdAt":"2024-03-26T12:00:00Z"
    }).encode("utf-8"))
    redis_client.rpush(q, json.dumps({
        "customerId":"3","name":"A","email":"a@example.com","createdAt":"2024-03-26T12:00:30Z"
    }).encode("utf-8"))

    # Run 5 messages
    worker.run(max_messages=5)

    # Check DB has 2 successful (cust 1 and first cust 3)
    assert mongo_dao.col.count_documents({}) == 2
