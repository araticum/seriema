import redis
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_conn = redis.from_url(REDIS_URL)

def is_duplicate(dedupe_key: str, ttl_seconds: int = 300) -> bool:
    if not dedupe_key:
        return False
    # setnx returns True if key was set (i.e., not duplicate)
    is_new = redis_conn.setnx(dedupe_key, "1")
    if is_new:
        redis_conn.expire(dedupe_key, ttl_seconds)
    return not is_new

def acquire_idempotency_key(key: str, ttl_seconds: int = 3600) -> bool:
    if not key:
        return False
    acquired = redis_conn.setnx(key, "1")
    if acquired:
        redis_conn.expire(key, ttl_seconds)
    return bool(acquired)
