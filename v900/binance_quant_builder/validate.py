from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from .api import NATIVE_MODES
from .models import AppConfig


VALID_MARKETS = {"futures_um", "spot"}
VALID_FREQS = {"1min", "5min", "15min", "1h", "4h", "1d"}
VALID_PARALLEL_BACKENDS = {"thread", "process"}
VALID_RATE_LIMIT_BACKENDS = {"sqlite", "redis", "noop", "shared_memory", "thread_local"}
VALID_EXPORT_FORMATS = {"csv", "parquet"}
VALID_METRICS_PERIODS = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}


def validate_config(config: AppConfig) -> list[str]:
    errors: list[str] = []

    if not config.run.symbol or not config.run.symbol.strip():
        errors.append("run.symbol est obligatoire")
    if config.run.market not in VALID_MARKETS:
        errors.append(f"run.market invalide: {config.run.market}")
    if config.run.freq not in VALID_FREQS:
        errors.append(f"run.freq invalide: {config.run.freq}")
    if config.parallel.backend not in VALID_PARALLEL_BACKENDS:
        errors.append(f"parallel.backend invalide: {config.parallel.backend}")
    if config.rate_limit.backend.lower() not in VALID_RATE_LIMIT_BACKENDS:
        errors.append(f"rate_limit.backend invalide: {config.rate_limit.backend}")
    if config.run.native_mode not in NATIVE_MODES:
        errors.append(f"run.native_mode invalide: {config.run.native_mode}")
    if config.run.native_export_format.lower() not in VALID_EXPORT_FORMATS:
        errors.append(f"run.native_export_format invalide: {config.run.native_export_format}")
    if config.run.native_metrics_period not in VALID_METRICS_PERIODS:
        errors.append(f"run.native_metrics_period invalide: {config.run.native_metrics_period}")
    if int(config.parallel.workers) < 1:
        errors.append("parallel.workers doit être >= 1")
    if int(config.rate_limit.max_per_min) < 1:
        errors.append("rate_limit.max_per_min doit être >= 1")
    if float(config.rate_limit.sqlite_timeout) <= 0:
        errors.append("rate_limit.sqlite_timeout doit être > 0")
    if int(config.legacy_bridge.chunksize) < 1:
        errors.append("legacy_bridge.chunksize doit être >= 1")
    if int(config.legacy_bridge.max_download_workers) < 1:
        errors.append("legacy_bridge.max_download_workers doit être >= 1")
    if int(config.legacy_bridge.batch_size) < 1:
        errors.append("legacy_bridge.batch_size doit être >= 1")
    if int(config.features.ml_normalization_window) < 2:
        errors.append("features.ml_normalization_window doit être >= 2")
    if int(config.features.triple_barrier_horizon) < 1:
        errors.append("features.triple_barrier_horizon doit être >= 1")
    if not (0 < float(config.features.large_trade_quantile) < 1):
        errors.append("features.large_trade_quantile doit être dans ]0,1[")
    if int(config.features.rolling_window) < 2:
        errors.append("features.rolling_window doit être >= 2")
    if int(config.run.native_download_limit) < 1:
        errors.append("run.native_download_limit doit être >= 1")
    if pd_timedelta_or_none(config.run.native_merge_tolerance) is None:
        errors.append(f"run.native_merge_tolerance invalide: {config.run.native_merge_tolerance}")
    if not (0 <= float(config.quality.max_na_ratio_critical) <= 1):
        errors.append("quality.max_na_ratio_critical doit être dans [0,1]")
    if int(config.quality.min_rows) < 1:
        errors.append("quality.min_rows doit être >= 1")
    if int(config.quality.min_label_classes) < 1:
        errors.append("quality.min_label_classes doit être >= 1")

    try:
        start_ts = pd.Timestamp(config.run.start)
        end_ts = pd.Timestamp(config.run.end)
        if start_ts.tzinfo is None:
            start_ts = start_ts.tz_localize('UTC')
        else:
            start_ts = start_ts.tz_convert('UTC')
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize('UTC')
        else:
            end_ts = end_ts.tz_convert('UTC')
        if start_ts >= end_ts:
            errors.append("run.start doit être strictement antérieur à run.end")
    except Exception:
        errors.append("run.start ou run.end invalide(s)")

    if config.rate_limit.backend.lower() == "sqlite" and config.rate_limit.sqlite_path:
        try:
            Path(config.rate_limit.sqlite_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            errors.append(f"sqlite_path invalide: {exc}")

    if not config.run.use_legacy_pipeline and config.run.native_mode in {"feature-only", "export-only"}:
        input_path = Path(config.run.native_input_path) if config.run.native_input_path else Path(config.run.out_dir) / config.run.native_raw_trades_filename
        if not input_path.exists():
            errors.append(f"run.native_input_path introuvable pour le mode {config.run.native_mode}: {input_path}")

    return errors


def pd_timedelta_or_none(value: str):
    try:
        import pandas as pd
        return pd.Timedelta(value)
    except Exception:
        return None


def config_summary(config: AppConfig) -> dict[str, Any]:
    data = asdict(config)
    data["execution_mode"] = "legacy-bridge" if config.run.use_legacy_pipeline else f"native-{config.run.native_mode}"
    return data
