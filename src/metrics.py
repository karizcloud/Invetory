import os, redis
K = "metrics"
def r(): return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)
def incr(field, n=1): r().hincrby(K, field, n)
def setv(field, v): r().hset(K, field, v)
def all_metrics(): return r().hgetall(K)
def stream_enabled(): return r().get("stream:enabled") == "1"
def set_stream(on):
    r().set("stream:enabled", "1" if on else "0"); setv("stream_status", "running" if on else "stopped")
