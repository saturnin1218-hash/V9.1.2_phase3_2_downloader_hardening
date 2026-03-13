from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from binance_quant_builder.config import load_config
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


def _run_cli_check(command: str, config_path: Path) -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "binance_quant_builder.cli", command, "--config", str(config_path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise SystemExit(f"CLI {command} KO: {proc.stderr or proc.stdout}")


def _read_feature_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    raise SystemExit(f"Format features non supporté pour le smoke check: {path.name}")


def main() -> None:
    config_path = ROOT / "config.example.yaml"
    cfg = load_config(str(config_path))
    errors = validate_config(cfg)
    if errors:
        raise SystemExit(f"config.example.yaml invalide: {errors}")

    _run_cli_check("validate-config", config_path)
    _run_cli_check("show-config", config_path)

    with TemporaryDirectory() as td:
        out_dir = Path(td)
        raw = _sample_trades_csv(out_dir / "raw.csv")
        runtime_cfg = AppConfig(
            run=RunConfig(
                symbol="BTCUSDT",
                out_dir=str(out_dir),
                use_legacy_pipeline=False,
                native_mode="feature-only",
                native_input_path=str(raw),
            )
        )
        result = run_pipeline(runtime_cfg)
        payload = result.get("result", {})

        feature_path = Path(payload["path"])
        if not feature_path.exists():
            raise SystemExit("Le fichier features attendu n'a pas été généré.")

        for key in ["run_manifest", "artifacts_manifest", "contract_manifest", "quality_manifest"]:
            path = Path(result[key])
            if not path.exists():
                raise SystemExit(f"Artefact manquant: {key} -> {path}")

        df = _read_feature_frame(feature_path)
        required_columns = {"timestamp", "open", "high", "low", "close", "label_tb"}
        missing = sorted(required_columns.difference(df.columns))
        if missing:
            raise SystemExit(f"Colonnes critiques manquantes: {missing}")

        quality = json.loads(Path(result["quality_manifest"]).read_text(encoding="utf-8"))
        if quality["status"] not in {"PASS", "PASS_WITH_WARNINGS"}:
            raise SystemExit(f"Quality check KO: {quality['status']}")

    print("quality_check: OK")


if __name__ == "__main__":
    main()
