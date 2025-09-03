# Customer Ingestion Worker

Production-grade, testable ingestion worker in **Python 3.11** using:

- **Redis** → queue + atomic **per-customer sliding-window rate limiter** (Lua + TTL)  
- **MongoDB** → permanent storage with **idempotent inserts**  
- **Pydantic v2 + email-validator + dateutil** → strict validation  
- **pytest** → unit & integration tests  
- **docker-compose** → one-command local stack (Redis, Mongo, Worker, Generator)

---

## Table of Contents

1. [Quick Start](#quick-start)  
2. [Project Layout](#project-layout)  
3. [How It Works (Step-by-Step Flow)](#how-it-works-step-by-step-flow)  
4. [How Requirements Are Met](#how-requirements-are-met)  
5. [Configuration](#configuration)  
6. [Running Tests](#running-tests)  
7. [Observability & Sample Logs](#observability--sample-logs)  
8. [Operational Notes](#operational-notes)
9. [Troubleshooting](#troubleshooting)

---

## Quick Start

### 0) Prerequisites
- Docker & Docker Compose v2
- (Optional) Python 3.11 if you want to run tests from your host

### 1) Clone & configure
```bash
cp .env.example .env
# Edit .env if needed; defaults are sensible for local compose.
```

### 2) Build & run the full stack
```bash
docker compose up --build
# or run detached:
# docker compose up --build -d
```

This starts:
- `redis` (port 6379)
- `mongo` (port 27017)
- `generator` → pushes JSON records to Redis at configurable per-customer rate
- `worker` → consumes queue, validates, rate-limits, inserts into Mongo

### 3) Watch logs
```bash
docker compose logs -f worker generator
```

You’ll see JSON logs like:
```json
{ "event":"queued","customerId":"1", "...": "..." }
{ "event":"ingested","status":"success","customerId":"1","_id":"...sha256..." }
{ "event":"validation_failed","status":"error","customerId":"2","reason":"Invalid email" }
{ "event":"rate_limited","status":"error","customerId":"1","reason":"Rate limit exceeded" }
```

### 4) Inspect Mongo (optional)
```bash
docker exec -it $(docker ps -qf name=mongo) mongosh
> use ingestion
> db.customers.countDocuments()
> db.customers.find().limit(3).pretty()
```

---

## Project Layout

```
ingestion-worker/
├─ docker-compose.yml
├─ .env.example
├─ requirements.txt
├─ README.md
├─ app/
│  ├─ config.py           # env & defaults
│  ├─ logger.py           # structured JSON logs (structlog)
│  ├─ models.py           # pydantic model: CustomerRecord
│  ├─ validator.py        # validate_record(raw) -> (bool, payload|error)
│  ├─ rate_limiter.py     # Redis Lua sliding-window limiter (ZSET + TTL)
│  ├─ queue_client.py     # Redis list client (BLPOP)
│  ├─ db.py               # MongoDAO (idempotent insert via deterministic _id)
│  ├─ worker.py           # worker main loop + signal handling
│  └─ errors.py           # typed error log shape
├─ generator/
│  └─ generator.py        # queue simulator (per-customer RPM, jitter, invalids)
├─ Dockerfile.worker
├─ Dockerfile.generator
└─ tests/
   ├─ conftest.py         # fixtures, cleanup for Redis/Mongo
   ├─ test_validator.py   # validation unit tests
   ├─ test_rate_limiter.py# rate limiter unit tests
   ├─ test_worker_unit.py # worker happy/invalid/parse/rate-limit cases
   └─ test_integration_flow.py # end-to-end batch flow
```

---

## How It Works (Step-by-Step Flow)

High-level:

```
[Generator] → Redis List → [Worker]
                        ├─ Validate (pydantic + email + ISO date)
                        ├─ Rate limit per customer (Redis ZSET+Lua sliding window)
                        ├─ Insert into Mongo (idempotent _id)
                        └─ JSON logs (success / invalid / rate-limited)
```

Detailed sequence:

1. **Generator emits records**  
   - For each `customerId` (e.g., `1,2,3`), emits `GEN_RPM` records/minute with jitter.  
   - Randomly corrupts a small fraction (`GEN_INVALID_RATE`) to exercise validation.  
   - Pushes JSON to Redis list `QUEUE_KEY` via `RPUSH`.

2. **Worker waits on queue**  
   - Uses `BLPOP(QUEUE_KEY, timeout=WORKER_POLL_TIMEOUT_SEC)`.  
   - If timeout, it loops again (idle). On SIGINT/SIGTERM, it exits gracefully.

3. **Parse & validation**  
   - Parses JSON; on failure, logs `{status:error, reason:Invalid JSON}`.  
   - Validates via `CustomerRecord`:
     - `customerId`: non-empty string
     - `name`: not blank
     - `email`: RFC-conformant (email-validator)
     - `createdAt`: ISO8601 parsable (dateutil)
   - On validation error: logs `{status:error, reason:<ex>}`; message is dropped.

4. **Per-customer rate limiting (5/min by default)**  
   - **Atomic sliding window** in Redis using Lua & ZSET (using LUA script to ensure atomicity as we are firing multiple commands to redis):
     - Removes timestamps older than `window`  
     - Counts remaining; if `< limit`, adds current timestamp and sets TTL  
     - Returns `1` (allow) or `0` (deny) in a single round-trip  
   - On deny: logs `{status:error, reason:Rate limit exceeded}`; message is dropped.

5. **Mongo insert (idempotent)**  
   - Builds `_id = sha256(customerId|email|createdAt)` to dedupe duplicates/crash-retries.  
   - Inserts `{...payload, ingestedAt: nowUTC}`.  
   - On duplicate key: logs warning and treats as success (idempotent).

6. **Structured logs**  
   - All events (ingested, validation_failed, rate_limited, parse_error) are JSON for easy ingestion into ELK/Datadog.

---

## Configuration

All values can be set via `.env`. Defaults are shown in `.env.example`.

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Log verbosity |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection for queue & limiter |
| `QUEUE_KEY` | `ingest:queue` | Redis list used as the queue |
| `RATE_LIMIT_LIMIT` | `5` | Allowed ingests per `window` per customer |
| `RATE_LIMIT_WINDOW_SEC` | `60` | Sliding window size (seconds) |
| `MONGO_URI` | `mongodb://mongo:27017` | Mongo connection string |
| `MONGO_DB` | `ingestion` | Database name |
| `MONGO_COLLECTION` | `customers` | Collection name |
| `WORKER_POLL_TIMEOUT_SEC` | `5` | `BLPOP` timeout |
| `WORKER_MAX_MESSAGES` | `0` | 0 = run forever; >0 = process N and exit (useful for tests) |
| `GEN_CUSTOMERS` | `1,2,3,4,5` | Comma separated customer IDs for generator |
| `GEN_RPM` | `5` | Records per minute **per customer** |
| `GEN_JITTER_MS` | `500` | Per-emit random jitter (+/- ms) |
| `GEN_INVALID_RATE` | `0.05` | Probability a generated record is intentionally invalid |

---

## Running Tests

1) Start Redis & Mongo via compose (you can keep worker/generator off if you like):
```bash
docker compose up -d redis mongo
```

2) Create a virtualenv and install deps:
```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

3) Run the tests:
```bash
pytest -q --maxfail=1 --disable-warnings
```

Tests do the following:
- Reset test Redis keys and drop the `ingestion_test` DB between tests
- Unit test validation and limiter (time-travel via injected `now_ms`)
- Worker unit test (valid, invalid email, parse error, rate-limit)
- Integration test (batch flow → expect capped inserts per limit)

---

## Observability & Sample Logs

All logs are structured JSON via `structlog`. That makes them ingestion-friendly (ELK/Datadog/Grafana Loki).

Examples:

**Success**
```json
{ 
  "timestamp":"2025-09-03T08:05:21.123Z",
  "level":"info",
  "event":"ingested",
  "status":"success",
  "customerId":"1",
  "_id":"f0a..."
}
```

**Validation failure**
```json
{ 
  "timestamp":"2025-09-03T08:05:22.045Z",
  "level":"error",
  "event":"validation_failed",
  "status":"error",
  "customerId":"2",
  "reason":"Invalid email address."
}
```

**Rate limited**
```json
{ 
  "timestamp":"2025-09-03T08:05:23.006Z",
  "level":"error",
  "event":"rate_limited",
  "status":"error",
  "customerId":"1",
  "reason":"Rate limit exceeded"
}
```

**Parse error**
```json
{ 
  "timestamp":"2025-09-03T08:05:23.998Z",
  "level":"error",
  "event":"parse_error",
  "status":"error",
  "reason":"Invalid JSON",
  "raw":"{\"bad_json\": "
}
```

---

## Operational Notes

### Rate limiter details (why it’s safe)
- **Sliding window** per customer implemented with a Redis **ZSET** whose scores are timestamps (ms).  
- On each check (Lua script):
  1. `ZREMRANGEBYSCORE key 0 now-window` (prune old)
  2. `ZCARD key` (count recent)
  3. If `< limit`: `ZADD key now now` + `PEXPIRE key window+2s` → **allow**  
     else → **deny**
- Entire check is **atomic** (Lua), one network round-trip, resilient to concurrency across many workers.

### Idempotent writes
- `_id = sha256(customerId|email|createdAt)` prevents duplicates from retries or concurrently processed duplicates.

### Backpressure & scaling
- `BLPOP` yields one item per pop; create **N worker replicas** to scale horizontally.  
- Redis list is fine for **at-most-once** consumption. For at-least-once with acknowledgements, prefer **Redis Streams** (future work).

### Graceful shutdown
- SIGINT/SIGTERM flips a flag; the loop exits after the current iteration.

### Failure modes
- **Redis down**: worker blocks/fails to pop; exits on repeated failures (you can wrap with retry logic if needed).  
- **Mongo down**: insert fails; currently no retry queue.

---

## Troubleshooting

- **Worker does nothing**  
  - Check generator logs; ensure it’s queuing.  
  - `docker compose logs -f generator`  
  - Or inspect Redis: `redis-cli -h 127.0.0.1 LLEN ingest:queue`

- **“Invalid JSON” errors**  
  - Expected if you purposely test broken payloads. Verify generator settings and any manual pushes.

- **Mongo duplicates**  
  - Expected: idempotency by `_id` avoids double inserts. If you see many duplicates, verify your `_id` derivation (`customerId|email|createdAt`).

- **Too many rate-limited logs**  
  - Your `GEN_RPM` is above `RATE_LIMIT_LIMIT`. Lower `GEN_RPM` or raise the limit/window.

- **Tests failing to connect**  
  - Ensure `redis` and `mongo` are up locally: `docker compose up -d redis mongo`.  
  - Tests assume `localhost` endpoints (see `tests/conftest.py`).
