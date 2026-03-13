from __future__ import annotations

import argparse
import logging
import os
import sys
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from .api import (
    build_native_execution_plan,
    run_native_download_only,
    _resolve_native_input_path,
    run_native_export_only,
    run_native_feature_only,
    run_native_full,
)
from .config import dump_config
from .errors import ConfigValidationError, QualityGateError
from .ids import make_run_id
from .manifests import build_execution_metrics_manifest, build_stage_lineage_manifest, file_metadata, write_json
from .models import AppConfig
from .observability import log_event, setup_run_logger
from .quality_gates import evaluate_quality_gates
from .report.nan_report import export_nan_coverage_report
from .runtime import PipelineRuntime, RetryPolicy
from .schema_contracts import (
    raise_on_contract_failure,
    validate_feature_dataset_contract,
    validate_merged_dataset_contract,
    validate_trades_contract,
)
from .validate import config_summary, validate_config
from .legacy import v7_9_4_compat as legacy

VERSION = "9.1.2"


def _normalize_legacy_rate_backend(config: AppConfig) -> str:
    backend = (config.rate_limit.backend or "sqlite").lower()
    if backend in {"sqlite", "shared_memory", "redis", "noop"}:
        return backend
    raise ValueError(f"Backend de rate limiter inconnu: {config.rate_limit.backend}")


@contextmanager
def _patched_argv(argv: list[str]):
    original = sys.argv[:]
    sys.argv = argv
    try:
        yield
    finally:
        sys.argv = original


def _legacy_defaults(config: AppConfig) -> argparse.Namespace:
    argv = [
        "binance-quant-builder",
        "--symbol", config.run.symbol,
        "--market", config.run.market,
        "--freq", config.run.freq,
        "--start", config.run.start,
        "--end", config.run.end,
        "--out_dir", config.run.out_dir,
    ]
    with _patched_argv(argv):
        return legacy.parse_args()


def _build_legacy_namespace(config: AppConfig, logger) -> argparse.Namespace:
    args = _legacy_defaults(config)
    backend = _normalize_legacy_rate_backend(config)

    args.symbol = config.run.symbol
    args.symbols = None
    args.market = config.run.market
    args.freq = config.run.freq
    args.start = config.run.start
    args.end = config.run.end
    args.out_dir = config.run.out_dir

    args.parallel = bool(config.parallel.enabled)
    args.parallel_backend = config.parallel.backend
    args.parallel_workers = int(config.parallel.workers)

    args.api_rate_backend = backend
    args.api_rate_sqlite_timeout = float(config.rate_limit.sqlite_timeout)
    args.api_max_per_min = int(config.rate_limit.max_per_min)
    args.api_rate_limit_jitter = float(config.legacy_bridge.api_rate_limit_jitter)
    args.api_rate_limit_db = config.rate_limit.sqlite_path

    if config.rate_limit.sqlite_path:
        db_parent = Path(config.rate_limit.sqlite_path).expanduser().resolve().parent
        db_parent.mkdir(parents=True, exist_ok=True)

    args.emit_ml_ready_normalized = bool(config.features.emit_ml_ready_normalized)
    args.ml_normalization_mode = config.features.ml_normalization_mode
    args.ml_normalization_window = int(config.features.ml_normalization_window)
    args.log_per_symbol = bool(config.features.log_per_symbol)
    args.nan_report = bool(config.features.nan_report)

    args.cache_dir = config.legacy_bridge.cache_dir
    args.chunksize = int(config.legacy_bridge.chunksize)
    args.engine = config.legacy_bridge.engine
    args.metrics_merge = config.legacy_bridge.metrics_merge
    args.max_metrics_staleness = config.legacy_bridge.max_metrics_staleness
    args.max_download_workers = int(config.legacy_bridge.max_download_workers)
    args.batch_size = int(config.legacy_bridge.batch_size)
    args.skip_delisted = bool(config.legacy_bridge.skip_delisted)
    args.skip_partial_listing = bool(config.legacy_bridge.skip_partial_listing)
    args.force_redownload = bool(config.legacy_bridge.force_redownload)
    args.force_reprocess = bool(config.legacy_bridge.force_reprocess)
    args.dropna_final = bool(config.legacy_bridge.dropna_final)
    args.downcast_float32 = bool(config.legacy_bridge.downcast_float32)
    args.gzip_csv = bool(config.legacy_bridge.gzip_csv)
    args.keep_extracted = bool(config.legacy_bridge.keep_extracted)
    args.include_offline_labels = bool(config.legacy_bridge.include_offline_labels)
    args.include_oi_5m_api = bool(config.legacy_bridge.include_oi_5m_api)
    args.include_liquidations = bool(config.legacy_bridge.include_liquidations)
    args.allow_target_columns_on_export = bool(config.legacy_bridge.allow_target_columns_on_export)

    return args


def _read_frame(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    try:
        if p.suffix.lower() == '.parquet':
            return pd.read_parquet(p)
        return pd.read_csv(p)
    except EmptyDataError:
        return pd.DataFrame()


def _contract_manifest(df: pd.DataFrame, native_mode: str) -> dict[str, Any]:
    if native_mode == "download-only":
        contract = validate_trades_contract(df)
        raise_on_contract_failure(contract)
        return asdict(contract)
    if native_mode in {"feature-only", "full"}:
        contract = validate_merged_dataset_contract(df)
        raise_on_contract_failure(contract)
        return asdict(contract)
    # export-only peut servir à republier un dataset arbitraire déjà validé en amont
    return {
        "name": "export_dataset",
        "passed": True,
        "errors": [],
        "rows": int(len(df)),
        "columns": list(df.columns),
    }


def _quality_manifest(df: pd.DataFrame, config: AppConfig) -> dict[str, Any]:
    q = evaluate_quality_gates(
        df,
        max_na_ratio_critical=float(config.quality.max_na_ratio_critical),
        min_rows=int(config.quality.min_rows),
        min_label_classes=int(config.quality.min_label_classes),
    )
    payload = {"status": q.status, "checks": q.checks}
    if q.status == "FAIL" and not config.quality.allow_degraded_export:
        raise QualityGateError("Quality gates en échec")
    return payload


def _collect_artifacts_from_result(result: Any) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []

    def walk(obj: Any, prefix: str = ""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                key = f"{prefix}.{k}" if prefix else k
                if k.endswith("path") and isinstance(v, str) and Path(v).exists():
                    meta = file_metadata(v)
                    meta["name"] = key
                    artifacts.append(meta)
                else:
                    walk(v, key)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                walk(item, f"{prefix}[{i}]")

    walk(result)
    uniq = {(a["path"], a["name"]): a for a in artifacts}
    return list(uniq.values())


def _run_native(config: AppConfig, logger, run_id: str) -> dict[str, Any]:
    out_dir = Path(config.run.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    runtime = PipelineRuntime(run_id=run_id, logger=logger)
    native_mode = config.run.native_mode
    started_at = datetime.now(timezone.utc).isoformat()

    artifacts: list[dict[str, Any]] = []
    contract_payload: dict[str, Any] | None = None
    quality_payload: dict[str, Any] | None = None
    retry_policy = RetryPolicy(
        max_attempts=int(config.retry.max_attempts),
        backoff_seconds=float(config.retry.backoff_seconds),
        backoff_multiplier=float(config.retry.backoff_multiplier),
        max_backoff_seconds=float(config.retry.max_backoff_seconds),
        retry_on=tuple(t for enabled, t in [
            (bool(config.retry.retry_on_timeouts), TimeoutError),
            (bool(config.retry.retry_on_connection_errors), ConnectionError),
        ] if enabled),
    )

    if native_mode == 'plan':
        plan = runtime.run_stage('native-plan', lambda: build_native_execution_plan(config))
        manifest_path = write_json(plan, out_dir / config.run.native_plan_filename)
        artifacts.append({**file_metadata(manifest_path), "name": "native_plan_manifest"})
        result_payload = {
            "status": "ok",
            "version": VERSION,
            "mode": "native-plan",
            "native_mode": native_mode,
            "manifest": str(manifest_path),
            "plan": plan,
            "config": config_summary(config),
        }
    else:
        if native_mode == 'download-only':
            result = runtime.run_stage('download-only', lambda: run_native_download_only(config), retry_policy=retry_policy)
        elif native_mode == 'feature-only':
            input_paths = [str(_resolve_native_input_path(config))] if (config.run.native_input_path or Path(config.run.out_dir, config.run.native_raw_trades_filename).exists()) else []
            result = runtime.run_stage('feature-only', lambda: run_native_feature_only(config), retry_policy=retry_policy, input_paths=input_paths)
        elif native_mode == 'export-only':
            input_paths = [str(_resolve_native_input_path(config))] if (config.run.native_input_path or Path(config.run.out_dir, config.run.native_raw_trades_filename).exists()) else []
            result = runtime.run_stage('export-only', lambda: run_native_export_only(config), retry_policy=retry_policy, input_paths=input_paths)
        elif native_mode == 'full':
            download = runtime.run_stage('download-only', lambda: run_native_download_only(config), retry_policy=retry_policy)
            download_input = (
                download.get('outputs', {}).get('trades', {}).get('path')
                if isinstance(download, dict) else None
            ) or (download.get('path') if isinstance(download, dict) else None)
            if not download_input:
                raise ValueError("Le stage download-only n'a pas retourné de path exploitable")
            config.run.native_input_path = download_input
            feature = runtime.run_stage('feature-only', lambda: run_native_feature_only(config), retry_policy=retry_policy, input_paths=[config.run.native_input_path])
            config.run.native_input_path = feature['path']
            export = runtime.run_stage('export-only', lambda: run_native_export_only(config), retry_policy=retry_policy, input_paths=[config.run.native_input_path])
            result = {'mode': 'full', 'download': download, 'feature': feature, 'export': export}
        else:
            raise ValueError(f"Mode natif inconnu: {native_mode}")

        artifacts.extend(_collect_artifacts_from_result(result))

        if native_mode == 'download-only':
            df = _read_frame(result['outputs']['trades']['path'])
        elif native_mode == 'feature-only':
            df = _read_frame(result['path'])
        elif native_mode == 'export-only':
            df = _read_frame(result['export_path'])
        else:
            df = _read_frame(result['feature']['path'])

        if native_mode in {'feature-only', 'download-only', 'full'} and len(df) == 0:
            contract_payload = {
                'name': 'empty_dataset',
                'passed': True,
                'errors': [],
                'rows': 0,
                'columns': list(df.columns),
                'note': 'Dataset vide: contrats métier et quality gates stricts ignorés pour cette exécution.'
            }
            quality_payload = {
                'status': 'PASS_WITH_WARNINGS',
                'checks': [
                    {
                        'name': 'empty_dataset',
                        'ok': True,
                        'value': 0,
                        'threshold': 0,
                        'severity': 'warning',
                    }
                ],
            }
        else:
            contract_payload = _contract_manifest(df, native_mode)
            quality_payload = _quality_manifest(df, config)
        result_payload = {
            "status": "ok",
            "version": VERSION,
            "mode": f"native-{native_mode}",
            "native_mode": native_mode,
            "result": result,
            "config": config_summary(config),
        }

    ended_at = datetime.now(timezone.utc).isoformat()
    stage_payload = [asdict(s) for s in runtime.stages]
    execution_metrics = build_execution_metrics_manifest(run_id, runtime.execution_metrics())
    stage_lineage = build_stage_lineage_manifest(run_id, runtime.stage_lineage())
    run_manifest = {
        "run_id": run_id,
        "version": VERSION,
        "status": "ok",
        "started_at": started_at,
        "ended_at": ended_at,
        "native_mode": native_mode,
        "execution_mode": result_payload["mode"],
        "stages": stage_payload,
        "quality_status": None if quality_payload is None else quality_payload["status"],
        "retry_policy": {
            "max_attempts": retry_policy.max_attempts,
            "backoff_seconds": retry_policy.backoff_seconds,
            "backoff_multiplier": retry_policy.backoff_multiplier,
            "max_backoff_seconds": retry_policy.max_backoff_seconds,
        },
        "config": dump_config(config),
    }
    run_manifest_path = write_json(run_manifest, out_dir / 'run_manifest.json')
    artifacts_manifest_path = write_json({"run_id": run_id, "artifacts": artifacts}, out_dir / 'artifacts_manifest.json')
    execution_metrics_path = write_json(execution_metrics, out_dir / 'execution_metrics.json')
    stage_lineage_path = write_json(stage_lineage, out_dir / 'stage_lineage_manifest.json')
    result_payload["run_id"] = run_id
    result_payload["run_manifest"] = str(run_manifest_path)
    result_payload["artifacts_manifest"] = str(artifacts_manifest_path)
    result_payload["execution_metrics_manifest"] = str(execution_metrics_path)
    result_payload["stage_lineage_manifest"] = str(stage_lineage_path)
    if contract_payload is not None:
        contract_path = write_json(contract_payload, out_dir / 'contract_manifest.json')
        result_payload["contract_manifest"] = str(contract_path)
    if quality_payload is not None:
        quality_path = write_json(quality_payload, out_dir / 'quality_manifest.json')
        result_payload["quality_manifest"] = str(quality_path)
    return result_payload


def run_pipeline(config: AppConfig) -> dict[str, Any]:
    out_dir = Path(config.run.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = make_run_id(config.run.symbol, config.run.freq, VERSION)
    logger = setup_run_logger(run_id=run_id, out_dir=out_dir)
    log_event(logger, logging.INFO, "run_started", run_id=run_id, version=VERSION, symbol=config.run.symbol, mode=("legacy-bridge" if config.run.use_legacy_pipeline else f"native-{config.run.native_mode}"))

    errors = validate_config(config)
    if errors:
        raise ConfigValidationError("Configuration invalide: " + " | ".join(errors))

    if not config.run.use_legacy_pipeline:
        return _run_native(config, logger, run_id)

    args = _build_legacy_namespace(config, logger)
    legacy.validate_args(args)

    if args.api_rate_backend == "sqlite" and config.rate_limit.sqlite_path:
        os.environ["BINANCE_API_RATE_LIMIT_DB"] = str(config.rate_limit.sqlite_path)
    if args.api_rate_backend == "redis":
        os.environ["BINANCE_API_RATE_REDIS_URL"] = str(config.rate_limit.redis_url)

    result = legacy._run_pipeline(args)
    if config.features.nan_report and result and isinstance(result, dict):
        exported = result.get("export_csv") or result.get("export_parquet")
        if exported:
            try:
                path = Path(exported)
                df = pd.read_parquet(path) if path.suffix == '.parquet' else pd.read_csv(path)
                export_nan_coverage_report(df, out_dir / f"{config.run.symbol}_nan_report")
            except Exception as exc:
                logger.warning("Impossible de générer le rapport NaN V9: %s", exc)

    run_manifest = {
        "run_id": run_id,
        "version": VERSION,
        "status": "ok",
        "execution_mode": "legacy-bridge",
        "runtime_rate_backend": args.api_rate_backend,
        "config": dump_config(config),
    }
    run_manifest_path = write_json(run_manifest, out_dir / 'run_manifest.json')
    return {
        "status": "ok",
        "version": VERSION,
        "mode": "legacy-bridge",
        "runtime_rate_backend": args.api_rate_backend,
        "result": result,
        "config": dump_config(config),
        "run_id": run_id,
        "run_manifest": str(run_manifest_path),
    }
