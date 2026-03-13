from __future__ import annotations

import json
from dataclasses import asdict

from typer.testing import CliRunner

from binance_quant_builder.cli import app
from binance_quant_builder.models import AppConfig, RunConfig


runner = CliRunner()


def _write_config(tmp_path):
    cfg = AppConfig(run=RunConfig(symbol="BTCUSDT", use_legacy_pipeline=True, native_mode="plan"))
    path = tmp_path / "config.json"
    path.write_text(json.dumps(asdict(cfg)), encoding="utf-8")
    return path


def test_validate_config_cli_ok(tmp_path):
    cfg_path = _write_config(tmp_path)
    result = runner.invoke(app, ["validate-config", "--config", str(cfg_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["errors"] == []
    assert payload["config"]["run"]["symbol"] == "BTCUSDT"



def test_show_config_cli_outputs_json(tmp_path):
    cfg_path = _write_config(tmp_path)
    result = runner.invoke(app, ["show-config", "--config", str(cfg_path)])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["run"]["symbol"] == "BTCUSDT"
    assert payload["run"]["native_mode"] == "plan"
