from binance_quant_builder.models import AppConfig, RunConfig
from binance_quant_builder.validate import validate_config


def test_validate_config_ok():
    cfg = AppConfig(run=RunConfig(symbol="BTCUSDT"))
    assert validate_config(cfg) == []


def test_validate_config_invalid_dates():
    cfg = AppConfig(run=RunConfig(symbol="BTCUSDT", start="2026-01-01", end="2025-01-01"))
    errors = validate_config(cfg)
    assert any("run.start" in e for e in errors)


def test_validate_config_invalid_datetime_format():
    cfg = AppConfig(run=RunConfig(symbol="BTCUSDT", start="not-a-date", end="2025-01-01"))
    errors = validate_config(cfg)
    assert any("invalide" in e for e in errors)
