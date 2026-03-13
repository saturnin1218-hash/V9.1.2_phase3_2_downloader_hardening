from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def test_benchmark_phase3_emits_environment_metadata(tmp_path: Path):
    path = tmp_path / "trades.csv"
    pd.DataFrame(
        {
            "timestamp": ["2024-01-01T00:00:00Z", "2024-01-01T00:30:00Z"],
            "price": [100, 101],
            "quantity": [1, 2],
            "is_buyer_maker": [False, True],
        }
    ).to_csv(path, index=False)
    out = tmp_path / "bench.json"
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "benchmark_phase3.py"), str(path), "--output", str(out), "--repeat", "1"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["versions"]["python"]
    assert payload["dataset"]["memory_bytes"] > 0
    assert payload["benchmark_scope"] in {"pandas_only", "hybrid_pandas_to_polars"}


def test_quality_check_script_runs_from_repo_root():
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "quality_check.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout
    assert "quality_check: OK" in proc.stdout
