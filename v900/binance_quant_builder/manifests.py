from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def file_metadata(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    stat = p.stat()
    return {
        'path': str(p),
        'size_bytes': stat.st_size,
        'sha256': sha256_file(p),
        'exists': p.exists(),
    }


def write_json(data: dict[str, Any], out_path: str | Path) -> Path:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    return p


def build_execution_metrics_manifest(run_id: str, metrics: dict[str, Any]) -> dict[str, Any]:
    return {
        'run_id': run_id,
        'kind': 'execution_metrics',
        **metrics,
    }


def build_stage_lineage_manifest(run_id: str, lineage: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        'run_id': run_id,
        'kind': 'stage_lineage',
        'stages': lineage,
    }
