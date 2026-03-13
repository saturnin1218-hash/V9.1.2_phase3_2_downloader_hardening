from __future__ import annotations

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype


def parse_binance_datetime(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    if s.dropna().empty:
        return pd.to_datetime(series, errors="coerce", utc=True)
    sample = s.dropna().iloc[0]
    if sample > 10**15:
        unit = "us"
    elif sample > 10**12:
        unit = "ms"
    elif sample > 10**9:
        unit = "s"
    else:
        unit = "s"
    return pd.to_datetime(s, unit=unit, errors="coerce", utc=True)


def ensure_utc_timestamp(series: pd.Series) -> pd.Series:
    if is_datetime64_any_dtype(series):
        ts = pd.to_datetime(series, errors="coerce", utc=True)
        if getattr(ts.dt, 'tz', None) is None:
            ts = ts.dt.tz_localize('UTC')
        return ts

    if is_numeric_dtype(series):
        return parse_binance_datetime(series)

    numeric = pd.to_numeric(series, errors="coerce")
    valid_numeric = numeric.notna()
    if bool(valid_numeric.any()):
        numeric_ratio = float(valid_numeric.mean())
        sample = float(numeric[valid_numeric].iloc[0])
        if numeric_ratio >= 0.80 and abs(sample) >= 10**9:
            return parse_binance_datetime(series)

    ts = pd.to_datetime(series, errors="coerce", utc=True)
    if getattr(ts.dt, 'tz', None) is None:
        ts = ts.dt.tz_localize('UTC')
    return ts
