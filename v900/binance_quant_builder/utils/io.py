from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd


def normalize_colname(name: str) -> str:
    return (
        str(name)
        .strip()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .lower()
    )


def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy(deep=False)
    out.columns = [normalize_colname(c) for c in out.columns]
    return out


def pick_first_existing(columns: Iterable[str], candidates: Iterable[str]) -> Optional[str]:
    normalized = {str(c).lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in normalized:
            return normalized[cand.lower()]
    return None
