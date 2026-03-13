import pandas as pd

from binance_quant_builder.downloader import _to_ms
from binance_quant_builder.utils.time import ensure_utc_timestamp


def test_ensure_utc_timestamp_series():
    s = pd.Series(["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"])
    out = ensure_utc_timestamp(s)
    assert str(out.dt.tz) == 'UTC'


def test_to_ms_respects_timezone_offsets():
    utc_ms = _to_ms("2024-01-01T00:00:00+00:00")
    plus2_ms = _to_ms("2024-01-01T00:00:00+02:00")
    assert utc_ms - plus2_ms == 2 * 60 * 60 * 1000
