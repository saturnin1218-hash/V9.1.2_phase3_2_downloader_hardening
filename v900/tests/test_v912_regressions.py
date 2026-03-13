from pathlib import Path

import pandas as pd

from binance_quant_builder.api import run_native_feature_only
from binance_quant_builder.models import AppConfig, FeatureConfig, LegacyBridgeConfig, ParallelConfig, RateLimitConfig, RunConfig
from binance_quant_builder.rate_limiter import ThreadRateLimiter, build_rate_limiter
from binance_quant_builder.utils.time import ensure_utc_timestamp


def _cfg(tmp_path: Path) -> AppConfig:
    return AppConfig(
        run=RunConfig(symbol='BTCUSDT', out_dir=str(tmp_path), use_legacy_pipeline=False, native_mode='feature-only'),
        parallel=ParallelConfig(),
        rate_limit=RateLimitConfig(backend='noop'),
        features=FeatureConfig(nan_report=False, rolling_window=2, triple_barrier_horizon=1),
        legacy_bridge=LegacyBridgeConfig(),
    )


def test_ensure_utc_timestamp_parses_numeric_milliseconds():
    s = pd.Series([1704067200000, 1704067260000])
    out = ensure_utc_timestamp(s)
    assert str(out.iloc[0]) == '2024-01-01 00:00:00+00:00'
    assert str(out.iloc[1]) == '2024-01-01 00:01:00+00:00'


def test_build_rate_limiter_thread_local_aliases_shared_memory():
    assert isinstance(build_rate_limiter('shared_memory', 60, None, 60.0, 'redis://localhost:6379/0'), ThreadRateLimiter)
    assert isinstance(build_rate_limiter('thread_local', 60, None, 60.0, 'redis://localhost:6379/0'), ThreadRateLimiter)


def test_native_feature_only_numeric_timestamp_input_is_correct(tmp_path: Path):
    cfg = _cfg(tmp_path)
    raw_path = tmp_path / 'raw.csv'
    pd.DataFrame({
        'agg_trade_id': [1, 2, 3, 4],
        'price': [100, 101, 102, 103],
        'quantity': [1, 1, 1, 1],
        'first_trade_id': [1, 2, 3, 4],
        'last_trade_id': [1, 2, 3, 4],
        'timestamp': [1704067200000, 1704069000000, 1704070800000, 1704072600000],
        'is_buyer_maker': [True, False, True, False],
        'is_best_match': [True, True, True, True],
    }).to_csv(raw_path, index=False)
    cfg.run.native_input_path = str(raw_path)

    result = run_native_feature_only(cfg)
    out = pd.read_csv(result['path'])
    assert out['timestamp'].iloc[0].startswith('2024-01-01 00:00:00')


def test_native_feature_only_exposes_partial_sidecar_coverage_warning(tmp_path: Path):
    cfg = _cfg(tmp_path)
    trades = pd.DataFrame({
        'agg_trade_id': [1, 2, 3, 4],
        'price': [100, 101, 102, 103],
        'quantity': [1, 1, 1, 1],
        'first_trade_id': [1, 2, 3, 4],
        'last_trade_id': [1, 2, 3, 4],
        'timestamp': pd.to_datetime(['2024-01-01 00:00:00', '2024-01-01 00:30:00', '2024-01-01 01:00:00', '2024-01-01 01:30:00'], utc=True),
        'is_buyer_maker': [True, False, True, False],
        'is_best_match': [True, True, True, True],
    })
    trades.to_csv(tmp_path / cfg.run.native_raw_trades_filename, index=False)

    funding = pd.DataFrame({
        'timestamp': pd.to_datetime(['2024-01-01 01:00:00'], utc=True),
        'funding_rate': [0.002],
        'mark_price': [102.5],
    })
    funding.to_csv(tmp_path / cfg.run.native_funding_filename, index=False)

    result = run_native_feature_only(cfg)
    assert result['coverage_warnings'] is not None
    assert result['coverage_warnings']['funding']['warning'] == 'partial_coverage'
