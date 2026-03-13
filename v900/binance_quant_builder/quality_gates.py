from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(slots=True)
class QualityGateResult:
    status: str
    checks: list[dict[str, Any]]


def evaluate_quality_gates(
    df: pd.DataFrame,
    *,
    max_na_ratio_critical: float = 0.15,
    min_rows: int = 2,
    min_label_classes: int = 2,
) -> QualityGateResult:
    checks: list[dict[str, Any]] = []

    rows_ok = len(df) >= min_rows
    checks.append({"name": "min_rows", "ok": rows_ok, "value": int(len(df)), "threshold": min_rows, "severity": "fatal"})

    critical_cols = [c for c in ['timestamp', 'open', 'high', 'low', 'close', 'label_tb'] if c in df.columns]
    na_frame = df[critical_cols] if critical_cols else df
    if len(na_frame.columns) > 0:
        na_ratio = float(na_frame.isna().mean().max())
    else:
        na_ratio = 1.0
    checks.append({"name": "max_na_ratio", "ok": na_ratio <= max_na_ratio_critical, "value": na_ratio, "threshold": max_na_ratio_critical, "severity": "fatal"})

    if "label_tb" in df.columns:
        nclasses = int(pd.Series(df["label_tb"]).dropna().nunique())
        checks.append({"name": "label_diversity", "ok": nclasses >= min_label_classes, "value": nclasses, "threshold": min_label_classes, "severity": "warning"})

    if "timestamp" in df.columns:
        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        monotone = bool(ts.is_monotonic_increasing and not ts.duplicated().any())
        checks.append({"name": "timestamp_integrity", "ok": monotone, "value": monotone, "threshold": True, "severity": "fatal"})

    fatal_failed = any((not c["ok"]) and c["severity"] == "fatal" for c in checks)
    warning_failed = any((not c["ok"]) and c["severity"] == "warning" for c in checks)
    status = "FAIL" if fatal_failed else ("PASS_WITH_WARNINGS" if warning_failed else "PASS")
    return QualityGateResult(status=status, checks=checks)
