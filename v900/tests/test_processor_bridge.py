from __future__ import annotations

from pathlib import Path

from binance_quant_builder.models import AppConfig, RunConfig
from binance_quant_builder.processor import _build_legacy_namespace, run_pipeline


class DummyLogger:
    def warning(self, *args, **kwargs):
        return None

    def info(self, *args, **kwargs):
        return None


def test_build_legacy_namespace_contains_required_fields(tmp_path):
    cfg = AppConfig(run=RunConfig(symbol="BTCUSDT", out_dir=str(tmp_path), start="2024-01-01", end="2024-01-02"))
    args = _build_legacy_namespace(cfg, DummyLogger())
    assert args.chunksize == 250000
    assert args.max_download_workers == 8
    assert args.batch_size == 1
    assert args.api_rate_backend == "sqlite"
    assert args.api_rate_limit_db is None


def test_run_pipeline_calls_legacy_without_symbol_kw(tmp_path, monkeypatch):
    cfg = AppConfig(run=RunConfig(symbol="BTCUSDT", out_dir=str(tmp_path), start="2024-01-01", end="2024-01-02"))

    captured = {}

    def fake_validate(args):
        captured["validated"] = True
        assert hasattr(args, "chunksize")

    def fake_run(args):
        captured["run_called"] = True
        return {"export_csv": str(Path(tmp_path) / "dummy.csv")}

    monkeypatch.setattr("binance_quant_builder.processor.legacy.validate_args", fake_validate)
    monkeypatch.setattr("binance_quant_builder.processor.legacy._run_pipeline", fake_run)

    import pandas as pd
    pd.DataFrame({"a": [1]}).to_csv(Path(tmp_path) / "dummy.csv", index=False)

    result = run_pipeline(cfg)
    assert captured["validated"] is True
    assert captured["run_called"] is True
    assert result["status"] == "ok"
    assert result["runtime_rate_backend"] == "sqlite"



def test_build_legacy_namespace_preserves_redis_backend(tmp_path):
    from binance_quant_builder.models import RateLimitConfig
    cfg = AppConfig(
        run=RunConfig(symbol="BTCUSDT", out_dir=str(tmp_path), start="2024-01-01", end="2024-01-02"),
        rate_limit=RateLimitConfig(backend="redis", redis_url="redis://localhost:6379/1"),
    )
    args = _build_legacy_namespace(cfg, DummyLogger())
    assert args.api_rate_backend == "redis"


def test_run_pipeline_sets_redis_env(tmp_path, monkeypatch):
    from binance_quant_builder.models import RateLimitConfig
    cfg = AppConfig(
        run=RunConfig(symbol="BTCUSDT", out_dir=str(tmp_path), start="2024-01-01", end="2024-01-02"),
        rate_limit=RateLimitConfig(backend="redis", redis_url="redis://localhost:6379/9"),
    )

    def fake_validate(args):
        return None

    def fake_run(args):
        return {"export_csv": str(Path(tmp_path) / "dummy.csv")}

    monkeypatch.setattr("binance_quant_builder.processor.legacy.validate_args", fake_validate)
    monkeypatch.setattr("binance_quant_builder.processor.legacy._run_pipeline", fake_run)

    import os
    import pandas as pd
    pd.DataFrame({"a": [1]}).to_csv(Path(tmp_path) / "dummy.csv", index=False)

    result = run_pipeline(cfg)
    assert os.environ["BINANCE_API_RATE_REDIS_URL"] == "redis://localhost:6379/9"
    assert result["runtime_rate_backend"] == "redis"
