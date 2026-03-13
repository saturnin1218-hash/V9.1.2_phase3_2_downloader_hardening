import json
from pathlib import Path

from binance_quant_builder.models import AppConfig, RunConfig
from binance_quant_builder.processor import run_pipeline


def test_native_plan_exports_manifest(tmp_path: Path):
    cfg = AppConfig(
        run=RunConfig(symbol="BTCUSDT", out_dir=str(tmp_path), use_legacy_pipeline=False)
    )
    result = run_pipeline(cfg)
    assert result["mode"] == "native-plan"
    manifest = Path(result["manifest"])
    assert manifest.exists()
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert payload["symbol"] == "BTCUSDT"
