from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from binance_quant_builder.models import AppConfig, QualityConfig, RunConfig
from binance_quant_builder.processor import run_pipeline


def test_native_feature_only_writes_lineage_and_metrics(tmp_path: Path):
    df = pd.DataFrame(
        {
            'agg_trade_id': [1, 2, 3, 4, 5, 6],
            'price': [100, 101, 102, 101, 103, 104],
            'quantity': [1, 2, 1.5, 1.2, 2.1, 0.9],
            'first_trade_id': [1, 2, 3, 4, 5, 6],
            'last_trade_id': [1, 2, 3, 4, 5, 6],
            'timestamp': [
                '2024-01-01T00:00:00Z',
                '2024-01-01T00:10:00Z',
                '2024-01-01T00:20:00Z',
                '2024-01-01T01:00:00Z',
                '2024-01-01T01:10:00Z',
                '2024-01-01T01:20:00Z',
            ],
            'is_buyer_maker': [False, True, False, True, False, True],
            'is_best_match': [True, True, True, True, True, True],
        }
    )
    raw_path = tmp_path / 'raw.csv'
    df.to_csv(raw_path, index=False)
    cfg = AppConfig(
        run=RunConfig(symbol='BTCUSDT', out_dir=str(tmp_path), use_legacy_pipeline=False, native_mode='feature-only', native_input_path=str(raw_path)),
        quality=QualityConfig(allow_degraded_export=True),
    )
    result = run_pipeline(cfg)
    lineage = json.loads(Path(result['stage_lineage_manifest']).read_text(encoding='utf-8'))
    metrics = json.loads(Path(result['execution_metrics_manifest']).read_text(encoding='utf-8'))
    assert lineage['stages'][0]['stage'] == 'feature-only'
    assert metrics['total_stages'] == 1
    assert metrics['stages'][0]['attempt_count'] == 1
