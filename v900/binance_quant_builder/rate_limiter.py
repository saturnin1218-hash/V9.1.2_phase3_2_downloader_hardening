from __future__ import annotations

import sqlite3
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol


class RateLimiter(Protocol):
    def acquire(self, weight: int = 1) -> None: ...


def _normalize_weight(weight: int) -> int:
    try:
        return max(1, int(weight))
    except Exception:
        return 1


@dataclass
class NoopRateLimiter:
    def acquire(self, weight: int = 1) -> None:
        return None


class ThreadRateLimiter:
    def __init__(self, max_per_min: int = 60):
        self.max_per_min = max(1, int(max_per_min))
        self.delay = 60.0 / float(self.max_per_min)
        self._lock = threading.Lock()
        self._next_ts = 0.0

    def acquire(self, weight: int = 1) -> None:
        slot_count = _normalize_weight(weight)
        with self._lock:
            now = time.time()
            wait = max(0.0, self._next_ts - now)
            self._next_ts = max(now, self._next_ts) + (self.delay * slot_count)
        if wait > 0:
            time.sleep(wait)


class SQLiteRateLimiter:
    def __init__(self, db_path: Optional[str] = None, max_per_min: int = 60, timeout: float = 60.0):
        self.db_path = db_path or str(Path(tempfile.gettempdir()) / "binance_api_rate_limit.sqlite3")
        self.max_per_min = max(1, int(max_per_min))
        self.timeout = float(timeout)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=self.timeout, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(f"PRAGMA busy_timeout={int(self.timeout * 1000)};")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS rate_limit_state (id INTEGER PRIMARY KEY CHECK (id = 1), next_ts REAL NOT NULL)"
            )
            cur = conn.execute("SELECT next_ts FROM rate_limit_state WHERE id = 1")
            if cur.fetchone() is None:
                conn.execute("INSERT INTO rate_limit_state (id, next_ts) VALUES (1, 0.0)")

    def acquire(self, weight: int = 1) -> None:
        delay = 60.0 / float(self.max_per_min)
        slot_count = _normalize_weight(weight)
        retry_sleep = 0.05
        for _ in range(200):
            try:
                with self._connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    current = conn.execute("SELECT next_ts FROM rate_limit_state WHERE id = 1").fetchone()[0]
                    now = time.time()
                    wait = max(0.0, float(current) - now)
                    next_ts = max(now, float(current)) + (delay * slot_count)
                    conn.execute("UPDATE rate_limit_state SET next_ts = ? WHERE id = 1", (next_ts,))
                    conn.commit()
                if wait > 0:
                    time.sleep(wait)
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower():
                    raise
                time.sleep(retry_sleep)
                retry_sleep = min(retry_sleep * 1.5, 1.0)
        raise RuntimeError("Rate limiter SQLite saturé: database is locked")


class RedisRateLimiter:
    def __init__(self, redis_url: str, key: str = "binance_api_rate_limit", max_per_min: int = 60, max_retries: int = 200):
        try:
            import redis
        except Exception as exc:
            raise RuntimeError("Le package redis n'est pas installé.") from exc
        self.client = redis.Redis.from_url(redis_url)
        self.key = key
        self.max_per_min = max(1, int(max_per_min))
        self.delay = 60.0 / float(self.max_per_min)
        self.max_retries = max(1, int(max_retries))

    def acquire(self, weight: int = 1) -> None:
        slot_count = _normalize_weight(weight)
        retries = 0
        while retries < self.max_retries:
            now = time.time()
            pipe = self.client.pipeline()
            try:
                pipe.watch(self.key)
                current = pipe.get(self.key)
                current_ts = float(current) if current is not None else 0.0
                wait = max(0.0, current_ts - now)
                next_ts = max(now, current_ts) + (self.delay * slot_count)
                pipe.multi()
                pipe.set(self.key, next_ts, ex=120)
                pipe.execute()
                if wait > 0:
                    time.sleep(wait)
                return
            except Exception as exc:
                retries += 1
                if retries >= self.max_retries:
                    raise RuntimeError(f"Rate limiter Redis indisponible après {self.max_retries} tentatives") from exc
                time.sleep(0.05)
            finally:
                try:
                    pipe.reset()
                except Exception:
                    pass


def build_rate_limiter(backend: str, max_per_min: int, sqlite_path: str | None, sqlite_timeout: float, redis_url: str):
    backend = backend.lower()
    if backend == "shared_memory":
        backend = "thread_local"
    if backend == "sqlite":
        return SQLiteRateLimiter(db_path=sqlite_path, max_per_min=max_per_min, timeout=sqlite_timeout)
    if backend == "redis":
        return RedisRateLimiter(redis_url=redis_url, max_per_min=max_per_min)
    if backend == "thread_local":
        return ThreadRateLimiter(max_per_min=max_per_min)
    if backend == "noop":
        return NoopRateLimiter()
    raise ValueError(f"Backend de rate limiter inconnu: {backend}")
