from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def build_nan_coverage_report(df: pd.DataFrame) -> pd.DataFrame:
    total = max(len(df), 1)
    report = pd.DataFrame({
        "column": list(df.columns),
        "nan_count": [int(df[c].isna().sum()) for c in df.columns],
    })
    report["nan_pct"] = report["nan_count"] / float(total)
    first_valid = []
    for c in df.columns:
        idx = df[c].first_valid_index()
        first_valid.append(None if idx is None else str(idx))
    report["first_valid_index"] = first_valid
    return report.sort_values(["nan_pct", "column"], ascending=[False, True]).reset_index(drop=True)


def export_nan_coverage_report(df: pd.DataFrame, out_prefix: str | Path) -> tuple[Path, Path]:
    out_prefix = Path(out_prefix)
    report = build_nan_coverage_report(df)
    csv_path = out_prefix.with_suffix(".csv")
    json_path = out_prefix.with_suffix(".json")
    report.to_csv(csv_path, index=False)
    json_path.write_text(report.to_json(orient="records", force_ascii=False, indent=2), encoding="utf-8")
    return csv_path, json_path
