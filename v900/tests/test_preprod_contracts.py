from pathlib import Path

import pandas as pd

from binance_quant_builder.models import AppConfig, QualityConfig, RunConfig
from binance_quant_builder.processor import run_pipeline
from binance_quant_builder.quality_gates import evaluate_quality_gates
from binance_quant_builder.schema_contracts import validate_trades_contract


def test_trades_contract_detects_negative_price():
    df = pd.DataFrame({
        'timestamp': ['2024-01-01T00:00:00Z'],
        'price': [-1],
        'quantity': [1],
    })
    result = validate_trades_contract(df)
    assert not result.passed
    assert any('price' in e for e in result.errors)


def test_quality_gate_warning_for_single_label_class():
    df = pd.DataFrame({
        'timestamp': ['2024-01-01T00:00:00Z', '2024-01-01T01:00:00Z'],
        'open': [1, 2], 'high': [2, 3], 'low': [1, 2], 'close': [2, 3],
        'label_tb': [0, 0],
    })
    result = evaluate_quality_gates(df)
    assert result.status == 'PASS_WITH_WARNINGS'


def test_native_feature_only_writes_preprod_manifests(tmp_path: Path):
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
    assert Path(result['run_manifest']).exists()
    assert Path(result['artifacts_manifest']).exists()
    assert Path(result['contract_manifest']).exists()
    assert Path(result['quality_manifest']).exists()
