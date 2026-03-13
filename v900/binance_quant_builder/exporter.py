from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from .errors import ArtifactWriteError
from .manifests import file_metadata


def export_json_manifest(data: dict[str, Any], out_path: str | Path) -> Path:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(prefix=path.stem + ".", suffix=".tmp", dir=str(path.parent))
    os.close(tmp_fd)
    tmp_path = Path(tmp_name)
    try:
        tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp_path.replace(path)
        return path
    except Exception as exc:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise ArtifactWriteError(f"Impossible d'écrire le manifeste {path}: {exc}") from exc


def export_dataframe(df: pd.DataFrame, out_path: str | Path, fmt: str | None = None) -> Path:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    export_format = (fmt or path.suffix.lstrip('.') or 'csv').lower()
    if export_format == 'csv':
        if path.suffix.lower() != '.csv':
            path = path.with_suffix('.csv')
        tmp = path.with_suffix(path.suffix + '.tmp')
        df.to_csv(tmp, index=False)
        tmp.replace(path)
        return path
    if export_format == 'parquet':
        if path.suffix.lower() != '.parquet':
            path = path.with_suffix('.parquet')
        tmp = path.with_suffix(path.suffix + '.tmp')
        try:
            df.to_parquet(tmp, index=False)
            tmp.replace(path)
            return path
        except ImportError:
            csv_path = path.with_suffix('.csv')
            df.to_csv(csv_path, index=False)
            return csv_path
    raise ValueError(f"Format d'export non supporté: {export_format}")


def dataframe_artifact_metadata(df: pd.DataFrame, path: str | Path) -> dict[str, Any]:
    meta = file_metadata(path)
    meta.update({"rows": int(len(df)), "columns": list(df.columns)})
    return meta
