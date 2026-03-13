from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from binance_quant_builder.models import AppConfig, RunConfig
from binance_quant_builder.processor import run_pipeline
from binance_quant_builder.validate import validate_config


def _sample_trades_csv(path: Path) -> Path:
    df = pd.DataFrame(
        {
            "agg_trade_id": [1, 2, 3, 4, 5, 6],
            "price": [100, 101, 102, 101, 103, 104],
            "quantity": [1, 2, 1.5, 1.2, 2.1, 0.9],
            "first_trade_id": [1, 2, 3, 4, 5, 6],
            "last_trade_id": [1, 2, 3, 4, 5, 6],
            "timestamp": [
                "2024-01-01T00:00:00Z",
                "2024-01-01T00:10:00Z",
                "2024-01-01T00:20:00Z",
                "2024-01-01T01:00:00Z",
                "2024-01-01T01:10:00Z",
                "2024-01-01T01:20:00Z",
            ],
            "is_buyer_maker": [False, True, False, True, False, True],
            "is_best_match": [True, True, True, True, True, True],
        }
    )
    df.to_csv(path, index=False)
    return path


def test_validate_feature_only_requires_existing_input(tmp_path: Path):
    cfg = AppConfig(
        run=RunConfig(
            symbol="BTCUSDT",
            out_dir=str(tmp_path),
            use_legacy_pipeline=False,
            native_mode="feature-only",
            native_input_path=str(tmp_path / "missing.csv"),
        )
    )
    errors = validate_config(cfg)
    assert any("native_input_path" in e for e in errors)


def test_native_feature_only_exports_features(tmp_path: Path):
    raw_path = _sample_trades_csv(tmp_path / "raw.csv")
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
    assert result["mode"] == "native-feature-only"
    output_path = Path(result["result"]["path"])
    assert output_path.exists()
    df = pd.read_csv(output_path)
    assert "label_tb" in df.columns
    assert "signed_notional_imbalance" in df.columns


def test_native_export_only_writes_manifest(tmp_path: Path):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    input_path = tmp_path / "input.csv"
    df.to_csv(input_path, index=False)
    cfg = AppConfig(
        run=RunConfig(
            symbol="BTCUSDT",
            out_dir=str(tmp_path),
            use_legacy_pipeline=False,
            native_mode="export-only",
            native_input_path=str(input_path),
            native_export_format="csv",
            native_export_filename="export.csv",
        )
    )
    result = run_pipeline(cfg)
    assert result["mode"] == "native-export-only"
    export_path = Path(result["result"]["export_path"])
    manifest_path = Path(result["result"]["manifest"])
    assert export_path.exists()
    assert manifest_path.exists()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["rows"] == 2


def test_native_full_chains_steps(tmp_path: Path, monkeypatch):
    raw_path = _sample_trades_csv(tmp_path / "native_raw_trades.csv")

    def fake_download(config):
        return {"mode": "download-only", "rows": 6, "path": str(raw_path), "columns": ["timestamp"]}

    monkeypatch.setattr("binance_quant_builder.processor.run_native_download_only", fake_download)

    cfg = AppConfig(
        run=RunConfig(
            symbol="BTCUSDT",
            out_dir=str(tmp_path),
            use_legacy_pipeline=False,
            native_mode="full",
        )
    )
    result = run_pipeline(cfg)
    assert result["mode"] == "native-full"
    assert result["result"]["download"]["rows"] == 6
    assert Path(result["result"]["feature"]["path"]).exists()
    assert Path(result["result"]["export"]["export_path"]).exists()


def test_native_feature_only_empty_result_returns_existing_path(tmp_path: Path):
    raw_path = tmp_path / "raw.csv"
    pd.DataFrame(columns=["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "timestamp", "is_buyer_maker", "is_best_match"]).to_csv(raw_path, index=False)
    cfg = AppConfig(
        run=RunConfig(
            symbol="BTCUSDT",
            out_dir=str(tmp_path),
            use_legacy_pipeline=False,
            native_mode="feature-only",
            native_input_path=str(raw_path),
            native_features_filename="features",
        )
    )
    result = run_pipeline(cfg)
    output_path = Path(result["result"]["path"])
    assert output_path.exists()
    assert output_path.name == "features.csv"
