import numpy as np
import pandas as pd

from binance_quant_builder.utils.numeric import normalize_bool_series, rolling_zscore, safe_divide, safe_to_numeric


def test_safe_to_numeric_handles_pd_na():
    s = pd.Series([1, pd.NA, "3"])
    out = safe_to_numeric(s)
    assert out.isna().sum() == 1
    assert float(out.iloc[2]) == 3.0


def test_normalize_bool_series():
    s = pd.Series([True, False, "true", "0", "yes", "no"])
    out = normalize_bool_series(s)
    assert list(out.fillna(-1).astype(int))[:4] == [1, 0, 1, 0]


def test_safe_divide_masks_zero():
    out = safe_divide(pd.Series([1.0, 2.0]), pd.Series([1.0, 0.0]))
    assert out.iloc[0] == 1.0
    assert np.isnan(out.iloc[1])


def test_rolling_zscore_past_only():
    s = pd.Series(range(1, 11), dtype=float)
    z = rolling_zscore(s, window=3, min_periods=3)
    assert pd.isna(z.iloc[0])
    assert pd.isna(z.iloc[1])
    assert pd.isna(z.iloc[2])
