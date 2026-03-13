import pandas as pd

from binance_quant_builder.merge import safe_merge_asof_past


def test_safe_merge_asof_past_backward_only():
    left = pd.DataFrame({
        'timestamp': pd.to_datetime(['2024-01-01 01:00:00', '2024-01-01 02:00:00'], utc=True),
        'x': [1, 2],
    })
    right = pd.DataFrame({
        'timestamp': pd.to_datetime(['2024-01-01 00:30:00', '2024-01-01 02:30:00'], utc=True),
        'funding_rate': [0.01, 0.02],
    })
    out = safe_merge_asof_past(left, right, on='timestamp', tolerance='2h')
    assert out.loc[0, 'funding_rate'] == 0.01
    assert out.loc[1, 'funding_rate'] == 0.01
