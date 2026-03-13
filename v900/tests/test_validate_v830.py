from binance_quant_builder.models import AppConfig, FeatureConfig, LegacyBridgeConfig, ParallelConfig, RateLimitConfig, RunConfig
from binance_quant_builder.validate import validate_config


def test_validate_config_rejects_invalid_metrics_period(tmp_path):
    cfg = AppConfig(
        run=RunConfig(symbol='BTCUSDT', out_dir=str(tmp_path), use_legacy_pipeline=False, native_mode='plan', native_metrics_period='7h'),
        parallel=ParallelConfig(),
        rate_limit=RateLimitConfig(backend='noop'),
        features=FeatureConfig(),
        legacy_bridge=LegacyBridgeConfig(),
    )
    errors = validate_config(cfg)
    assert any('native_metrics_period' in e for e in errors)
