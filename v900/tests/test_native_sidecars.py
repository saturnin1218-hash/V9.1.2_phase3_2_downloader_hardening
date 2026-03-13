from pathlib import Path

import pandas as pd

from binance_quant_builder.api import run_native_feature_only
from binance_quant_builder.models import AppConfig, FeatureConfig, LegacyBridgeConfig, ParallelConfig, RateLimitConfig, RunConfig


def _cfg(tmp_path: Path) -> AppConfig:
    return AppConfig(
        run=RunConfig(symbol='BTCUSDT', out_dir=str(tmp_path), use_legacy_pipeline=False, native_mode='feature-only'),
        parallel=ParallelConfig(),
        rate_limit=RateLimitConfig(backend='noop'),
        features=FeatureConfig(nan_report=False, rolling_window=2, triple_barrier_horizon=1),
        legacy_bridge=LegacyBridgeConfig(),
    )


def test_native_feature_only_merges_sidecars(tmp_path: Path):
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
        'timestamp': pd.to_datetime(['2023-12-31 23:00:00', '2024-01-01 01:00:00'], utc=True),
        'funding_rate': [0.001, 0.002],
        'mark_price': [99.5, 102.5],
    })
    funding.to_csv(tmp_path / cfg.run.native_funding_filename, index=False)

    metrics = pd.DataFrame({
        'timestamp': pd.to_datetime(['2024-01-01 00:00:00', '2024-01-01 01:00:00'], utc=True),
        'sum_open_interest': [10, 12],
        'sum_open_interest_value': [1000, 1200],
    })
    metrics.to_csv(tmp_path / cfg.run.native_metrics_filename, index=False)

    liq = pd.DataFrame({
        'timestamp': pd.to_datetime(['2024-01-01 00:10:00', '2024-01-01 01:10:00'], utc=True),
        'liquidation_qty': [2, 3],
        'liquidation_notional': [200, 330],
    })
    liq.to_csv(tmp_path / cfg.run.native_liquidations_filename, index=False)

    result = run_native_feature_only(cfg)
    out = pd.read_csv(result['path'])
    assert 'funding_rate' in out.columns
    assert 'sum_open_interest' in out.columns
    assert 'liquidation_events' in out.columns
    assert (tmp_path / cfg.run.native_merged_filename).exists()
