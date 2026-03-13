from __future__ import annotations

from time import perf_counter
from typing import Iterable

import numpy as np
import pandas as pd

from .utils.numeric import normalize_bool_series, rolling_zscore, safe_divide, safe_to_numeric
from .utils.time import ensure_utc_timestamp

try:  # pragma: no cover - optional dependency
    import polars as pl  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pl = None


DEFAULT_ACCELERATION_BACKEND = "auto"


def polars_available() -> bool:
    return pl is not None


def should_use_polars(backend: str = DEFAULT_ACCELERATION_BACKEND) -> bool:
    backend_n = (backend or DEFAULT_ACCELERATION_BACKEND).lower()
    if backend_n == "pandas":
        return False
    if backend_n == "polars":
        return polars_available()
    return polars_available()


def _to_polars(df: pd.DataFrame) -> "pl.DataFrame":
    if pl is None:  # pragma: no cover - defensive
        raise RuntimeError("polars n'est pas installé")
    out = df.copy()
    if "timestamp" in out.columns:
        out["timestamp"] = ensure_utc_timestamp(out["timestamp"]).dt.tz_localize(None)
    return pl.from_pandas(out)


def _from_polars(df: "pl.DataFrame") -> pd.DataFrame:
    out = df.to_pandas()
    if "timestamp" in out.columns:
        out["timestamp"] = ensure_utc_timestamp(out["timestamp"])
    return out


def merge_asof_past_optional_polars(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    on: str = "timestamp",
    tolerance: str | pd.Timedelta | None = None,
    suffix: str | None = None,
    exclude_columns: Iterable[str] | None = None,
    backend: str = DEFAULT_ACCELERATION_BACKEND,
) -> pd.DataFrame:
    if left.empty or right.empty or on != "timestamp" or not should_use_polars(backend):
        return _merge_asof_past_pandas(
            left,
            right,
            on=on,
            tolerance=tolerance,
            suffix=suffix,
            exclude_columns=exclude_columns,
        )

    exclude = set(exclude_columns or [])
    right_p = right.copy()
    if suffix:
        rename_map = {
            col: f"{col}{suffix}"
            for col in right_p.columns
            if col != on and col not in exclude
        }
        right_p = right_p.rename(columns=rename_map)

    left_pl = _to_polars(left).sort(on).unique(subset=[on], keep="last", maintain_order=True)
    right_pl = _to_polars(right_p).sort(on).unique(subset=[on], keep="last", maintain_order=True)

    tol = None if tolerance is None else pd.Timedelta(tolerance)
    result = left_pl.join_asof(
        right_pl,
        on=on,
        strategy="backward",
        tolerance=None if tol is None else f"{int(tol.total_seconds() * 1_000_000)}us",
    )
    return _from_polars(result)


def _merge_asof_past_pandas(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    on: str = "timestamp",
    tolerance: str | pd.Timedelta | None = None,
    suffix: str | None = None,
    exclude_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    left_p = left.copy()
    right_p = right.copy()
    left_p[on] = ensure_utc_timestamp(left_p[on])
    right_p[on] = ensure_utc_timestamp(right_p[on])
    left_p = left_p.sort_values(on).drop_duplicates(subset=[on], keep="last").reset_index(drop=True)
    right_p = right_p.sort_values(on).drop_duplicates(subset=[on], keep="last").reset_index(drop=True)

    exclude = set(exclude_columns or [])
    if suffix:
        rename_map = {
            col: f"{col}{suffix}"
            for col in right_p.columns
            if col != on and col not in exclude
        }
        right_p = right_p.rename(columns=rename_map)

    tol = None if tolerance is None else pd.Timedelta(tolerance)
    return pd.merge_asof(
        left_p,
        right_p,
        on=on,
        direction="backward",
        allow_exact_matches=True,
        tolerance=tol,
    )


def resample_liquidations_optional_polars(
    liq: pd.DataFrame,
    *,
    freq: str,
    backend: str = DEFAULT_ACCELERATION_BACKEND,
) -> pd.DataFrame:
    if liq.empty or not should_use_polars(backend):
        return _resample_liquidations_pandas(liq, freq=freq)
    liq_pl = _to_polars(liq)
    if "liquidation_notional" not in liq_pl.columns or "liquidation_qty" not in liq_pl.columns:
        return _resample_liquidations_pandas(liq, freq=freq)

    every = _pandas_freq_to_polars(freq)
    result = (
        liq_pl.sort("timestamp")
        .group_by_dynamic("timestamp", every=every, period=every, label="right", closed="right")
        .agg(
            pl.col("liquidation_qty").count().alias("liquidation_events"),
            pl.col("liquidation_notional").sum().alias("liquidation_notional_sum"),
            pl.col("liquidation_qty").sum().alias("liquidation_qty_sum"),
        )
        .sort("timestamp")
    )
    return _from_polars(result)


def _resample_liquidations_pandas(liq: pd.DataFrame, *, freq: str) -> pd.DataFrame:
    return (
        liq.set_index("timestamp")
        .resample(freq, label="right", closed="right")
        .agg(
            liquidation_events=("liquidation_qty", "count"),
            liquidation_notional_sum=("liquidation_notional", "sum"),
            liquidation_qty_sum=("liquidation_qty", "sum"),
        )
        .reset_index()
    )


def _past_only_large_trade_threshold(notional: pd.Series, window: int, quantile: float) -> pd.Series:
    min_periods = max(2, min(window, 10))
    rolled = notional.shift(1).rolling(window=window, min_periods=min_periods).quantile(quantile)
    expanding = notional.shift(1).expanding(min_periods=2).quantile(quantile)
    return rolled.fillna(expanding)


def build_microstructure_features_optional_polars(
    df: pd.DataFrame,
    *,
    freq: str = "1h",
    large_trade_quantile: float = 0.95,
    rolling_window: int = 1000,
    backend: str = DEFAULT_ACCELERATION_BACKEND,
) -> pd.DataFrame:
    if df.empty or not should_use_polars(backend):
        return _build_microstructure_features_pandas(
            df,
            freq=freq,
            large_trade_quantile=large_trade_quantile,
            rolling_window=rolling_window,
        )

    enriched = _prepare_trade_frame(df, large_trade_quantile=large_trade_quantile, rolling_window=rolling_window)
    every = _pandas_freq_to_polars(freq)
    out = _to_polars(enriched)
    agg = (
        out.sort("timestamp")
        .group_by_dynamic("timestamp", every=every, period=every, label="right", closed="right")
        .agg(
            pl.col("price").first().alias("price_open"),
            pl.col("price").max().alias("price_high"),
            pl.col("price").min().alias("price_low"),
            pl.col("price").last().alias("price_close"),
            pl.col("price").count().alias("trade_count"),
            pl.col("quantity").sum().alias("quantity_sum"),
            pl.col("notional").sum().alias("notional_sum"),
            pl.col("buy_notional").sum().alias("buy_notional"),
            pl.col("sell_notional").sum().alias("sell_notional"),
            pl.col("large_buy_notional").sum().alias("large_buy_notional"),
            pl.col("large_sell_notional").sum().alias("large_sell_notional"),
            pl.col("is_large_trade").sum().alias("large_trade_count"),
        )
        .sort("timestamp")
    )
    return _finalize_microstructure_features(_from_polars(agg))


def _prepare_trade_frame(df: pd.DataFrame, *, large_trade_quantile: float, rolling_window: int) -> pd.DataFrame:
    out = df.copy(deep=False)
    out["timestamp"] = ensure_utc_timestamp(out["timestamp"])
    out["price"] = safe_to_numeric(out["price"])
    out["quantity"] = safe_to_numeric(out["quantity"])
    if "is_buyer_maker" in out.columns:
        out["is_buyer_maker"] = normalize_bool_series(out["is_buyer_maker"]).fillna(0)
    else:
        out["is_buyer_maker"] = 0.0
    out = out.sort_values("timestamp")
    out["notional"] = out["price"] * out["quantity"]
    out["buy_notional"] = np.where(out["is_buyer_maker"] == 0, out["notional"], 0.0)
    out["sell_notional"] = np.where(out["is_buyer_maker"] == 1, out["notional"], 0.0)
    threshold = _past_only_large_trade_threshold(out["notional"], window=rolling_window, quantile=large_trade_quantile)
    out["large_trade_threshold"] = threshold
    out["is_large_trade"] = (out["notional"] >= threshold).astype("float64")
    out["large_buy_notional"] = np.where((out["is_buyer_maker"] == 0) & (out["is_large_trade"] == 1), out["notional"], 0.0)
    out["large_sell_notional"] = np.where((out["is_buyer_maker"] == 1) & (out["is_large_trade"] == 1), out["notional"], 0.0)
    return out


def _finalize_microstructure_features(agg: pd.DataFrame) -> pd.DataFrame:
    if agg.empty:
        return agg
    out = agg.copy()
    out["large_trade_share"] = safe_divide(out["large_trade_count"], out["trade_count"])
    out["signed_notional_imbalance"] = safe_divide(out["buy_notional"] - out["sell_notional"], out["notional_sum"])
    out["large_signed_imbalance"] = safe_divide(
        out["large_buy_notional"] - out["large_sell_notional"],
        out["large_buy_notional"] + out["large_sell_notional"],
    )
    out["amihud_24"] = compute_amihud_illiquidity(out["price_close"], out["notional_sum"], window=24)
    out["vpin_50"] = compute_vpin(out["buy_notional"], out["sell_notional"], window=50)
    out["notional_z_96"] = rolling_zscore(out["notional_sum"], window=96)
    return out


def _build_microstructure_features_pandas(
    df: pd.DataFrame,
    *,
    freq: str = "1h",
    large_trade_quantile: float = 0.95,
    rolling_window: int = 1000,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    out = _prepare_trade_frame(df, large_trade_quantile=large_trade_quantile, rolling_window=rolling_window)
    agg = (
        out.set_index("timestamp")
        .resample(freq, label="right", closed="right")
        .agg(
        price_open=("price", "first"),
        price_high=("price", "max"),
        price_low=("price", "min"),
        price_close=("price", "last"),
        trade_count=("price", "count"),
        quantity_sum=("quantity", "sum"),
        notional_sum=("notional", "sum"),
        buy_notional=("buy_notional", "sum"),
        sell_notional=("sell_notional", "sum"),
        large_buy_notional=("large_buy_notional", "sum"),
        large_sell_notional=("large_sell_notional", "sum"),
            large_trade_count=("is_large_trade", "sum"),
        )
        .reset_index()
    )
    return _finalize_microstructure_features(agg)


def compute_amihud_illiquidity(close: pd.Series, volume_quote: pd.Series, window: int = 24) -> pd.Series:
    returns = pd.Series(close).pct_change()
    illiq = returns.abs() / pd.Series(volume_quote).replace(0, np.nan)
    return illiq.rolling(window=window, min_periods=max(5, min(window, 10))).mean()


def compute_vpin(buy_volume: pd.Series, sell_volume: pd.Series, window: int = 50) -> pd.Series:
    imbalance = (pd.Series(buy_volume) - pd.Series(sell_volume)).abs()
    total = (pd.Series(buy_volume) + pd.Series(sell_volume)).replace(0, np.nan)
    return (
        imbalance.rolling(window, min_periods=max(5, min(window, 10))).sum()
        / total.rolling(window, min_periods=max(5, min(window, 10))).sum()
    )


def benchmark_backend(
    func,
    /,
    *args,
    repeat: int = 3,
    warmup: int = 1,
    **kwargs,
) -> dict[str, float]:
    for _ in range(max(warmup, 0)):
        func(*args, **kwargs)
    samples: list[float] = []
    for _ in range(max(repeat, 1)):
        start = perf_counter()
        func(*args, **kwargs)
        samples.append(perf_counter() - start)
    avg = float(sum(samples) / len(samples))
    best = float(min(samples))
    return {"repeat": float(len(samples)), "avg_seconds": avg, "best_seconds": best}


def _pandas_freq_to_polars(freq: str) -> str:
    mapping = {
        "1min": "1m",
        "5min": "5m",
        "15min": "15m",
        "30min": "30m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
    }
    return mapping.get(freq, freq)
