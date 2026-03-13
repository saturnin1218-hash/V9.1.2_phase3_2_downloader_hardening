from __future__ import annotations

from datetime import datetime, timezone
import re


def _slug(text: str) -> str:
    value = re.sub(r"[^A-Za-z0-9]+", "_", str(text).strip()).strip("_")
    return value or "NA"


def make_run_id(symbol: str, freq: str, version: str = "9.0.0") -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run_{stamp}_{_slug(symbol)}_{_slug(freq)}_v{_slug(version).replace('_','_')}"


def make_stage_id(run_id: str, stage_name: str) -> str:
    return f"{run_id}__{_slug(stage_name)}"


def make_artifact_id(run_id: str, artifact_name: str) -> str:
    return f"{run_id}__artifact__{_slug(artifact_name)}"
