from __future__ import annotations

from pathlib import Path

import pandas as pd

from binance_quant_builder.models import AppConfig, RunConfig
from binance_quant_builder.processor import run_pipeline


def _sample_trades_csv(path: Path) -> Path:
    df = pd.DataFrame(
        {
            "agg_trade_id": list(range(1, 13)),
            "price": [100, 101, 102, 101, 103, 104, 103, 105, 106, 107, 108, 107],
            "quantity": [1, 2, 1.5, 1.2, 2.1, 0.9, 1.0, 1.1, 1.3, 0.7, 0.8, 1.2],
            "first_trade_id": list(range(1, 13)),
            "last_trade_id": list(range(1, 13)),
            "timestamp": [
                "2024-01-01T00:00:00Z",
                "2024-01-01T00:10:00Z",
                "2024-01-01T00:20:00Z",
                "2024-01-01T01:00:00Z",
                "2024-01-01T01:10:00Z",
                "2024-01-01T01:20:00Z",
                "2024-01-01T02:00:00Z",
                "2024-01-01T02:10:00Z",
                "2024-01-01T02:20:00Z",
                "2024-01-01T03:00:00Z",
                "2024-01-01T03:10:00Z",
                "2024-01-01T03:20:00Z",
            ],
            "is_buyer_maker": [False, True, False, True, False, True, False, False, True, False, True, False],
            "is_best_match": [True] * 12,
        }
    )
    df.to_csv(path, index=False)
    return path


def test_phase3_full_multisidecar_with_polars_backend(tmp_path: Path):
    raw_path = _sample_trades_csv(tmp_path / "raw.csv")

    pd.DataFrame(
        {
            "timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z", "2024-01-01T03:00:00Z"],
            "funding_rate": [0.0001, 0.0002, 0.0003, 0.0004],
            "mark_price": [100.5, 102.2, 104.8, 106.1],
        }
    ).to_csv(tmp_path / "native_funding.csv", index=False)

    pd.DataFrame(
        {
            "timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z", "2024-01-01T03:00:00Z"],
            "sum_open_interest": [1000, 1010, 1035, 1040],
            "sum_open_interest_value": [100000, 103000, 109000, 112000],
            "cmc_circulating_supply": [19000000] * 4,
            "count_toptrader_long_short_ratio": [1.01, 1.05, 1.08, 1.03],
        }
    ).to_csv(tmp_path / "native_metrics.csv", index=False)

    pd.DataFrame(
        {
            "timestamp": [
                "2024-01-01T00:05:00Z",
                "2024-01-01T00:25:00Z",
                "2024-01-01T01:05:00Z",
                "2024-01-01T02:05:00Z",
            ],
            "liquidation_notional": [1000.0, 1500.0, 2000.0, 3000.0],
            "liquidation_qty": [10.0, 12.0, 8.0, 20.0],
        }
    ).to_csv(tmp_path / "native_liquidations.csv", index=False)

    cfg = AppConfig(
        run=RunConfig(
            symbol="BTCUSDT",
            out_dir=str(tmp_path),
            use_legacy_pipeline=False,
            native_mode="feature-only",
            native_input_path=str(raw_path),
            native_include_funding=True,
            native_include_metrics=True,
            native_include_liquidations=True,
            native_acceleration_backend="polars",
        )
    )

    result = run_pipeline(cfg)
    out = pd.read_csv(result["result"]["path"])
    assert {"funding_rate", "sum_open_interest", "liquidation_events", "label_tb", "notional_z_96"}.issubset(out.columns)
    assert len(out) >= 4
