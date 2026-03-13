from __future__ import annotations

import os
import sys
from types import SimpleNamespace

from binance_quant_builder.legacy import v7_9_4_compat as legacy


def test_api_rate_limit_wait_noop_returns_immediately(monkeypatch):
    legacy._SESSION_LOCAL.api_rate_backend = "noop"
    called = {"sleep": 0}
    monkeypatch.setattr("binance_quant_builder.legacy.v7_9_4_compat.time.sleep", lambda *a, **k: called.__setitem__("sleep", called["sleep"] + 1))
    legacy.api_rate_limit_wait(max_per_min=60, jitter=0.1)
    assert called["sleep"] == 0


def test_api_rate_limit_wait_redis_mocked(monkeypatch):
    os.environ["BINANCE_API_RATE_REDIS_URL"] = "redis://localhost:6379/0"
    legacy._SESSION_LOCAL.api_rate_backend = "redis"

    class FakePipe:
        store = {"binance_api_rate_limit": b"0"}
        def watch(self, key):
            self.key = key
        def get(self, key):
            return self.store.get(key)
        def multi(self):
            return None
        def set(self, key, value, ex=None):
            self.store[key] = str(value).encode()
        def execute(self):
            return [True]
        def reset(self):
            return None

    class FakeRedisClient:
        def pipeline(self):
            return FakePipe()

    fake_mod = SimpleNamespace(Redis=SimpleNamespace(from_url=lambda url: FakeRedisClient()))
    monkeypatch.setitem(sys.modules, "redis", fake_mod)
    monkeypatch.setattr("binance_quant_builder.legacy.v7_9_4_compat.time.sleep", lambda *a, **k: None)
    legacy.api_rate_limit_wait(max_per_min=60, jitter=0.0)
