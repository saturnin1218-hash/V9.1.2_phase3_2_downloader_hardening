from __future__ import annotations

import numpy as np
import pandas as pd

from binance_quant_builder.accelerated import polars_available
from binance_quant_builder.features.microstructure import build_microstructure_features_from_trades


def _sample_trades() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": [
                "2024-01-01T00:00:00Z",
                "2024-01-01T00:20:00Z",
                "2024-01-01T00:59:59Z",
                "2024-01-01T01:00:00Z",
                "2024-01-01T01:20:00Z",
                "2024-01-01T01:59:59Z",
            ],
            "price": [100, 101, 102, 103, 104, 105],
            "quantity": [1, 2, 1, 1, 2, 1],
            "is_buyer_maker": [False, True, False, True, False, True],
        }
    )


def test_microstructure_pandas_uses_right_labeled_bars():
    df = _sample_trades()
    out = build_microstructure_features_from_trades(df, freq="1h", acceleration_backend="pandas")
    assert out["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ").tolist() == [
        "2024-01-01T00:00:00Z",
        "2024-01-01T01:00:00Z",
        "2024-01-01T02:00:00Z",
    ]
    assert out["trade_count"].tolist() == [1, 3, 2]


def test_microstructure_polars_matches_pandas_bar_labels_and_counts():
    if not polars_available():
        return
    df = _sample_trades()
    pd_out = build_microstructure_features_from_trades(df, freq="1h", acceleration_backend="pandas")
    pl_out = build_microstructure_features_from_trades(df, freq="1h", acceleration_backend="polars")
    assert pd_out["timestamp"].tolist() == pl_out["timestamp"].tolist()
    for col in ["trade_count", "quantity_sum", "notional_sum", "price_open", "price_close"]:
        assert np.allclose(pd_out[col].to_numpy(dtype=float), pl_out[col].to_numpy(dtype=float), equal_nan=True)
