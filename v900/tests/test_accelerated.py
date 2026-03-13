from __future__ import annotations

import pandas as pd

from binance_quant_builder.accelerated import (
    polars_available,
    resample_liquidations_optional_polars,
)
from binance_quant_builder.merge import safe_merge_asof_past


def test_safe_merge_asof_past_pandas_backend_matches_expected():
    left = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01 01:00:00", "2024-01-01 02:00:00"], utc=True),
            "x": [1, 2],
        }
    )
    right = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01 00:30:00", "2024-01-01 02:30:00"], utc=True),
            "funding_rate": [0.01, 0.02],
        }
    )
    out = safe_merge_asof_past(left, right, on="timestamp", tolerance="2h", backend="pandas")
    assert out.loc[0, "funding_rate"] == 0.01
    assert out.loc[1, "funding_rate"] == 0.01


def test_resample_liquidations_optional_polars_matches_pandas_shape():
    liq = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                [
                    "2024-01-01T00:10:00Z",
                    "2024-01-01T00:20:00Z",
                    "2024-01-01T01:10:00Z",
                ],
                utc=True,
            ),
            "liquidation_notional": [1000.0, 1500.0, 2000.0],
            "liquidation_qty": [10.0, 12.0, 8.0],
        }
    )
    pandas_out = resample_liquidations_optional_polars(liq, freq="1h", backend="pandas")
    auto_out = resample_liquidations_optional_polars(liq, freq="1h", backend="auto")
    pd.testing.assert_frame_equal(
        pandas_out.fillna(0).reset_index(drop=True),
        auto_out.fillna(0).reset_index(drop=True),
        check_dtype=False,
    )


def test_auto_backend_is_safe_without_polars():
    liq = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01T00:10:00Z"], utc=True),
            "liquidation_notional": [1000.0],
            "liquidation_qty": [10.0],
        }
    )
    out = resample_liquidations_optional_polars(liq, freq="1h", backend="auto")
    assert not out.empty
    assert "liquidation_events" in out.columns
    assert polars_available() in {True, False}
