from __future__ import annotations

import numpy as np
import pandas as pd


def triple_barrier_label_ohlc(df: pd.DataFrame, price_col: str = "close", high_col: str = "high", low_col: str = "low", horizon: int = 24, pt: float = 0.02, sl: float = 0.02, neutral_label: float = 0.0) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="float64")
    close = pd.to_numeric(df[price_col], errors="coerce").to_numpy(dtype=float)
    high = pd.to_numeric(df[high_col], errors="coerce").to_numpy(dtype=float)
    low = pd.to_numeric(df[low_col], errors="coerce").to_numpy(dtype=float)
    n = len(df)
    labels = np.full(n, neutral_label, dtype=float)
    for i in range(n):
        p0 = close[i]
        if not np.isfinite(p0):
            labels[i] = np.nan
            continue
        actual_horizon = min(int(horizon), n - i - 1)
        if actual_horizon <= 0:
            labels[i] = neutral_label
            continue
        future_high = high[i + 1 : i + 1 + actual_horizon]
        future_low = low[i + 1 : i + 1 + actual_horizon]
        future_close = close[i + 1 : i + 1 + actual_horizon]
        upper = p0 * (1.0 + pt)
        lower = p0 * (1.0 - sl)
        tp_hits = np.where(future_high >= upper)[0]
        sl_hits = np.where(future_low <= lower)[0]
        tp_first = int(tp_hits[0]) if tp_hits.size else None
        sl_first = int(sl_hits[0]) if sl_hits.size else None
        if tp_first is not None and sl_first is not None:
            if tp_first < sl_first:
                labels[i] = 1.0
            elif sl_first < tp_first:
                labels[i] = -1.0
            else:
                labels[i] = -1.0
        elif tp_first is not None:
            labels[i] = 1.0
        elif sl_first is not None:
            labels[i] = -1.0
        else:
            last_close = future_close[actual_horizon - 1]
            if np.isfinite(last_close):
                if last_close >= upper:
                    labels[i] = 1.0
                elif last_close <= lower:
                    labels[i] = -1.0
                else:
                    labels[i] = neutral_label
            else:
                labels[i] = neutral_label
    return pd.Series(labels, index=df.index, name=f"triple_barrier_h{horizon}")
