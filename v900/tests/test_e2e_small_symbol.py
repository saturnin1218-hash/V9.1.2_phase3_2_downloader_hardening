from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from binance_quant_builder.models import AppConfig, RunConfig
from binance_quant_builder.processor import run_pipeline


def _sample_trades_csv(path: Path) -> Path:
    df = pd.DataFrame(
        {
            "agg_trade_id": list(range(1, 10)),
            "price": [100, 101, 102, 101, 103, 104, 103, 105, 106],
            "quantity": [1, 2, 1.5, 1.2, 2.1, 0.9, 1.0, 1.1, 1.3],
            "first_trade_id": list(range(1, 10)),
            "last_trade_id": list(range(1, 10)),
            "timestamp": [
                "2024-01-01T00:00:00Z",
                "2024-01-01T00:10:00Z",
                "2024-01-01T00:20:00Z",
                "2024-01-01T01:00:00Z",
                "2024-01-01T01:10:00Z",
                "2024-01-01T01:20:00Z",
                "2024-01-01T02:00:00Z",
                "2024-01-01T02:10:00Z",
                "2024-01-01T02:20:00Z",
            ],
            "is_buyer_maker": [False, True, False, True, False, True, False, False, True],
            "is_best_match": [True] * 9,
        }
    )
    df.to_csv(path, index=False)
    return path


def test_native_feature_only_e2e_with_sidecars_and_manifests(tmp_path: Path):
    raw_path = _sample_trades_csv(tmp_path / "raw.csv")

    funding = pd.DataFrame(
        {
            "timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"],
            "funding_rate": [0.0001, 0.0002, 0.0003],
            "mark_price": [100.5, 102.2, 104.8],
        }
    )
    funding.to_csv(tmp_path / "native_funding.csv", index=False)

    metrics = pd.DataFrame(
        {
            "timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z", "2024-01-01T02:00:00Z"],
            "sum_open_interest": [1000, 1010, 1035],
            "sum_open_interest_value": [100000, 103000, 109000],
            "cmc_circulating_supply": [19000000, 19000000, 19000000],
            "count_toptrader_long_short_ratio": [1.01, 1.05, 1.08],
        }
    )
    metrics.to_csv(tmp_path / "native_metrics.csv", index=False)

    cfg = AppConfig(
        run=RunConfig(
            symbol="BTCUSDT",
            out_dir=str(tmp_path),
            use_legacy_pipeline=False,
            native_mode="feature-only",
            native_input_path=str(raw_path),
        )
    )

    result = run_pipeline(cfg)
    feature_path = Path(result["result"]["path"])
    assert feature_path.exists()

    df = pd.read_csv(feature_path)
    assert {"label_tb", "funding_rate", "mark_price", "sum_open_interest"}.issubset(df.columns)

    for key in ["run_manifest", "artifacts_manifest", "contract_manifest", "quality_manifest", "execution_metrics_manifest", "stage_lineage_manifest"]:
        assert Path(result[key]).exists(), key

    quality = json.loads(Path(result["quality_manifest"]).read_text(encoding="utf-8"))
    assert quality["status"] in {"PASS", "PASS_WITH_WARNINGS"}

    artifacts = json.loads(Path(result["artifacts_manifest"]).read_text(encoding="utf-8"))
    assert artifacts["artifacts"]
