from __future__ import annotations

import numpy as np
import pandas as pd


def safe_to_numeric(series: pd.Series, errors: str = "coerce") -> pd.Series:
    if series is None:
        return pd.Series(dtype="float64")
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    if str(series.dtype) == "boolean":
        return series.astype("Int8").astype("float64")
    out = series.replace({pd.NA: np.nan})
    return pd.to_numeric(out, errors=errors)


def normalize_bool_series(series: pd.Series) -> pd.Series:
    if not isinstance(series, pd.Series):
        series = pd.Series(series)
    if str(series.dtype) == "boolean":
        return series.astype("Int8")
    lowered = series.astype(str).str.strip().str.lower()
    mapping = {
        "true": 1, "false": 0,
        "1": 1, "0": 0,
        "t": 1, "f": 0,
        "yes": 1, "no": 0,
    }
    out = lowered.map(mapping)
    return out.astype("float64")


def safe_divide(numerator: pd.Series | np.ndarray, denominator: pd.Series | np.ndarray) -> pd.Series:
    num = pd.Series(numerator)
    den = pd.Series(denominator)
    result = pd.Series(np.nan, index=num.index, dtype="float64")
    mask = den.notna() & (den != 0)
    result.loc[mask] = (num.loc[mask] / den.loc[mask]).astype("float64")
    return result


def rolling_zscore(series: pd.Series, window: int, min_periods: int | None = None) -> pd.Series:
    if min_periods is None:
        min_periods = max(5, min(window, 20))
    mean = series.shift(1).rolling(window=window, min_periods=min_periods).mean()
    std = series.shift(1).rolling(window=window, min_periods=min_periods).std(ddof=0)
    out = (series - mean) / std.replace(0.0, np.nan)
    return out.astype("float64")
