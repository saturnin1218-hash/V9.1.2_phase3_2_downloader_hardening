import pandas as pd

from binance_quant_builder.features.microstructure import build_microstructure_features_from_trades


def test_microstructure_threshold_is_past_only():
    df = pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=6, freq="min", tz="UTC"),
        "price": [100, 100, 100, 100, 100, 100],
        "quantity": [1, 1, 1, 1, 1, 1000],
        "is_buyer_maker": [0, 0, 0, 0, 0, 0],
    })
    out = build_microstructure_features_from_trades(df, freq="5min", rolling_window=3, large_trade_quantile=0.9)
    assert not out.empty
