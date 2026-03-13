from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict

from .models import (
    AppConfig,
    FeatureConfig,
    LegacyBridgeConfig,
    ParallelConfig,
    QualityConfig,
    RateLimitConfig,
    RetryConfig,
    RunConfig,
)


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception as exc:
        raise RuntimeError("PyYAML n'est pas installé. Installe l'extra 'yaml'.") from exc
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError("Le fichier YAML doit contenir un mapping à la racine.")
    return data


def load_config(path: str | Path) -> AppConfig:
    cfg_path = Path(path)
    suffix = cfg_path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        raw = _load_yaml(cfg_path)
    elif suffix == ".toml":
        import tomllib
        raw = tomllib.loads(cfg_path.read_text(encoding="utf-8"))
    elif suffix == ".json":
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    else:
        raise ValueError(f"Format de config non supporté: {suffix}")
    return AppConfig(
        run=RunConfig(**raw.get("run", {})),
        parallel=ParallelConfig(**raw.get("parallel", {})),
        rate_limit=RateLimitConfig(**raw.get("rate_limit", {})),
        features=FeatureConfig(**raw.get("features", {})),
        quality=QualityConfig(**raw.get("quality", {})),
        retry=RetryConfig(**raw.get("retry", {})),
        legacy_bridge=LegacyBridgeConfig(**raw.get("legacy_bridge", {})),
    )


def dump_config(config: AppConfig) -> Dict[str, Any]:
    return asdict(config)
