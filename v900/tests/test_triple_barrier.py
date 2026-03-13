import pandas as pd

from binance_quant_builder.labels.triple_barrier import triple_barrier_label_ohlc


def test_triple_barrier_tp_first():
    df = pd.DataFrame({
        "close": [100, 100, 100],
        "high": [100, 103, 101],
        "low": [100, 99, 99],
    })
    out = triple_barrier_label_ohlc(df, horizon=2, pt=0.02, sl=0.02)
    assert out.iloc[0] == 1.0


def test_triple_barrier_sl_first():
    df = pd.DataFrame({
        "close": [100, 100, 100],
        "high": [100, 101, 103],
        "low": [100, 97, 99],
    })
    out = triple_barrier_label_ohlc(df, horizon=2, pt=0.02, sl=0.02)
    assert out.iloc[0] == -1.0


def test_triple_barrier_end_of_series_no_index_error():
    df = pd.DataFrame({
        "close": [100, 101],
        "high": [100, 102],
        "low": [100, 100],
    })
    out = triple_barrier_label_ohlc(df, horizon=24, pt=0.01, sl=0.01)
    assert len(out) == 2
