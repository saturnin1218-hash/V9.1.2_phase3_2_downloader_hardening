import pandas as pd

from binance_quant_builder.report.nan_report import build_nan_coverage_report


def test_nan_report_contains_pct():
    df = pd.DataFrame({"a": [1, None], "b": [None, None]})
    report = build_nan_coverage_report(df)
    assert set(["column", "nan_count", "nan_pct"]).issubset(report.columns)
