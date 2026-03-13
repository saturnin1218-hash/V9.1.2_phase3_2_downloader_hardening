from __future__ import annotations

from typing import Iterable

import pandas as pd

from .accelerated import merge_asof_past_optional_polars
from .utils.time import ensure_utc_timestamp


def _prepare_frame(df: pd.DataFrame, on: str) -> pd.DataFrame:
    out = df.copy()
    out[on] = ensure_utc_timestamp(out[on])
    out = out.sort_values(on).drop_duplicates(subset=[on], keep="last").reset_index(drop=True)
    return out


def safe_merge_asof_past(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    on: str = "timestamp",
    tolerance: str | pd.Timedelta | None = None,
    suffix: str | None = None,
    exclude_columns: Iterable[str] | None = None,
    backend: str = "auto",
) -> pd.DataFrame:
    if left.empty:
        return left.copy()
    if right.empty:
        return left.copy()

    left_p = _prepare_frame(left, on=on)
    right_p = _prepare_frame(right, on=on)

    return merge_asof_past_optional_polars(
        left_p,
        right_p,
        on=on,
        tolerance=tolerance,
        suffix=suffix,
        exclude_columns=exclude_columns,
        backend=backend,
    )
