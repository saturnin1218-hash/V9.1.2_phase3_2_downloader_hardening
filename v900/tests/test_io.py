from binance_quant_builder.utils.io import canonicalize_columns, pick_first_existing
import pandas as pd


def test_canonicalize_columns():
    df = pd.DataFrame(columns=["Open Interest", "Trade-Count"])
    out = canonicalize_columns(df)
    assert list(out.columns) == ["open_interest", "trade_count"]


def test_pick_first_existing():
    assert pick_first_existing(["timestamp", "price"], ["time", "timestamp"]) == "timestamp"
