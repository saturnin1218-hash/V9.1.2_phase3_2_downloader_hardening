from __future__ import annotations

import pandas as pd

from ..accelerated import (
    _build_microstructure_features_pandas,
    build_microstructure_features_optional_polars,
    compute_amihud_illiquidity,
    compute_vpin,
)


def build_microstructure_features_from_trades(
    df: pd.DataFrame,
    freq: str = "1h",
    large_trade_quantile: float = 0.95,
    rolling_window: int = 1000,
    acceleration_backend: str = "pandas",
) -> pd.DataFrame:
    backend = (acceleration_backend or "pandas").lower()
    if backend in {"auto", "polars"}:
        return build_microstructure_features_optional_polars(
            df,
            freq=freq,
            large_trade_quantile=large_trade_quantile,
            rolling_window=rolling_window,
            backend=backend,
        )
    return _build_microstructure_features_pandas(
        df,
        freq=freq,
        large_trade_quantile=large_trade_quantile,
        rolling_window=rolling_window,
    )


__all__ = [
    "build_microstructure_features_from_trades",
    "compute_amihud_illiquidity",
    "compute_vpin",
]
