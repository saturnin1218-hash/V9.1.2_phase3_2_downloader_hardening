from pathlib import Path

from binance_quant_builder.rate_limiter import SQLiteRateLimiter, ThreadRateLimiter, build_rate_limiter


def test_sqlite_rate_limiter_initializes(tmp_path):
    db = tmp_path / "rl.sqlite3"
    rl = SQLiteRateLimiter(db_path=str(db), max_per_min=60000, timeout=5.0)
    rl.acquire()
    assert db.exists()


def test_build_rate_limiter_noop():
    rl = build_rate_limiter("noop", 60, None, 60.0, "redis://localhost:6379/0")
    rl.acquire()


def test_build_rate_limiter_shared_memory():
    rl = build_rate_limiter("shared_memory", 60, None, 60.0, "redis://localhost:6379/0")
    assert isinstance(rl, ThreadRateLimiter)


def test_redis_rate_limiter_retries_then_fails(monkeypatch):
    import sys
    from types import SimpleNamespace
    from binance_quant_builder.rate_limiter import RedisRateLimiter

    class FakePipe:
        def watch(self, key):
            raise RuntimeError("boom")
        def reset(self):
            return None

    class FakeRedisClient:
        def pipeline(self):
            return FakePipe()

    fake_mod = SimpleNamespace(Redis=SimpleNamespace(from_url=lambda url: FakeRedisClient()))
    monkeypatch.setitem(sys.modules, "redis", fake_mod)
    rl = RedisRateLimiter("redis://localhost:6379/0", max_retries=2)
    import pytest
    with pytest.raises(RuntimeError):
        rl.acquire()


def test_thread_rate_limiter_accepts_weight_parameter():
    rl = ThreadRateLimiter(max_per_min=60000)
    rl.acquire(weight=4)
