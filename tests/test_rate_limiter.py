import time
from redis import Redis
from app.rate_limiter import RateLimiter

def test_rate_allows_within_limit(redis_client):
    rl = RateLimiter(redis_client, limit=5, window_sec=60)
    cid = "custA"
    base = int(time.time()*1000)
    results = [rl.allow(cid, base + i*10) for i in range(5)]
    assert all(results)
    # 6th should fail
    assert rl.allow(cid, base + 60) is False

def test_rate_resets_after_window(redis_client):
    rl = RateLimiter(redis_client, limit=2, window_sec=1)
    cid = "custB"
    t0 = int(time.time()*1000)
    assert rl.allow(cid, t0) is True
    assert rl.allow(cid, t0+10) is True
    assert rl.allow(cid, t0+20) is False
    # move beyond window
    assert rl.allow(cid, t0+1100) is True
