from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .accelerated import resample_liquidations_optional_polars
from .downloader import (
    build_http_session,
    fetch_aggtrades_dataframe,
    fetch_funding_rate_dataframe,
    fetch_liquidations_dataframe,
    fetch_open_interest_hist_dataframe,
)
from .exporter import export_dataframe, export_json_manifest
from .features.microstructure import build_microstructure_features_from_trades
from .labels.triple_barrier import triple_barrier_label_ohlc
from .merge import safe_merge_asof_past
from .models import AppConfig
from .rate_limiter import build_rate_limiter
from .report.nan_report import export_nan_coverage_report
from .utils.io import canonicalize_columns
from .utils.numeric import rolling_zscore
from .utils.time import ensure_utc_timestamp


@dataclass(slots=True)
class RuntimeContext:
    rate_backend: str
    session_user_agent: str
    has_rate_limiter: bool


NATIVE_MODES = {"plan", "download-only", "feature-only", "export-only", "full"}


def build_runtime_context(config: AppConfig) -> RuntimeContext:
    session = build_http_session()
    limiter = build_rate_limiter(
        backend=config.rate_limit.backend,
        max_per_min=config.rate_limit.max_per_min,
        sqlite_path=config.rate_limit.sqlite_path,
        sqlite_timeout=config.rate_limit.sqlite_timeout,
        redis_url=config.rate_limit.redis_url,
    )
    return RuntimeContext(
        rate_backend=("thread_local" if config.rate_limit.backend.lower() == "shared_memory" else config.rate_limit.backend.lower()),
        session_user_agent=session.headers.get("User-Agent", "python-requests"),
        has_rate_limiter=limiter is not None,
    )


def build_native_execution_plan(config: AppConfig) -> dict[str, Any]:
    ctx = build_runtime_context(config)
    return {
        "version": "9.1.2",
        "mode": "native-plan",
        "native_mode": config.run.native_mode,
        "symbol": config.run.symbol,
        "market": config.run.market,
        "freq": config.run.freq,
        "date_range": {"start": config.run.start, "end": config.run.end},
        "out_dir": config.run.out_dir,
        "parallel": {
            "enabled": bool(config.parallel.enabled),
            "backend": config.parallel.backend,
            "workers": int(config.parallel.workers),
        },
        "rate_limit": {
            "backend": ctx.rate_backend,
            "max_per_min": int(config.rate_limit.max_per_min),
            "sqlite_path": config.rate_limit.sqlite_path,
        },
        "native_outputs": {
            "raw_trades": str(Path(config.run.out_dir) / config.run.native_raw_trades_filename),
            "funding": str(Path(config.run.out_dir) / config.run.native_funding_filename),
            "metrics": str(Path(config.run.out_dir) / config.run.native_metrics_filename),
            "liquidations": str(Path(config.run.out_dir) / config.run.native_liquidations_filename),
            "merged": str(Path(config.run.out_dir) / config.run.native_merged_filename),
            "features": str(Path(config.run.out_dir) / config.run.native_features_filename),
            "export": str(Path(config.run.out_dir) / config.run.native_export_filename),
        },
        "native_supplemental": {
            "acceleration_backend": config.run.native_acceleration_backend,
            "include_funding": bool(config.run.native_include_funding),
            "include_metrics": bool(config.run.native_include_metrics),
            "include_liquidations": bool(config.run.native_include_liquidations),
            "merge_tolerance": config.run.native_merge_tolerance,
            "metrics_period": config.run.native_metrics_period,
        },
        "legacy_bridge_enabled": False,
        "preprod_controls": {"contracts": True, "quality_gates": True, "manifests": True, "structured_logs": True},
        "native_modules": [
            "config",
            "validate",
            "api",
            "downloader",
            "merge",
            "rate_limiter",
            "exporter",
            "features.microstructure",
            "labels.triple_barrier",
            "report.nan_report",
        ],
    }


def _resolve_native_input_path(config: AppConfig) -> Path:
    if config.run.native_input_path:
        return Path(config.run.native_input_path)
    return Path(config.run.out_dir) / config.run.native_raw_trades_filename


def _default_sidecar_paths(config: AppConfig) -> dict[str, Path]:
    out_dir = Path(config.run.out_dir)
    return {
        'funding': out_dir / config.run.native_funding_filename,
        'metrics': out_dir / config.run.native_metrics_filename,
        'liquidations': out_dir / config.run.native_liquidations_filename,
        'merged': out_dir / config.run.native_merged_filename,
    }


def _load_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == '.parquet':
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _time_coverage_summary(df: pd.DataFrame, expected_start: str, expected_end: str, key: str = "timestamp") -> dict[str, Any]:
    requested_start = pd.Timestamp(expected_start)
    requested_end = pd.Timestamp(expected_end)
    if requested_start.tzinfo is None:
        requested_start = requested_start.tz_localize("UTC")
    else:
        requested_start = requested_start.tz_convert("UTC")
    if requested_end.tzinfo is None:
        requested_end = requested_end.tz_localize("UTC")
    else:
        requested_end = requested_end.tz_convert("UTC")

    payload: dict[str, Any] = {
        "requested_start": requested_start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "requested_hours": max((requested_end - requested_start).total_seconds() / 3600.0, 0.0),
        "rows": int(len(df)),
    }
    if df.empty or key not in df.columns:
        payload.update({"coverage_ratio": 0.0, "warning": "empty_sidecar"})
        return payload

    ts = ensure_utc_timestamp(df[key]).dropna().sort_values()
    if ts.empty:
        payload.update({"coverage_ratio": 0.0, "warning": "invalid_timestamps"})
        return payload

    observed_start = ts.iloc[0]
    observed_end = ts.iloc[-1]
    requested_seconds = max((requested_end - requested_start).total_seconds(), 0.0)
    observed_seconds = max((min(observed_end, requested_end) - max(observed_start, requested_start)).total_seconds(), 0.0)
    coverage_ratio = 1.0 if requested_seconds == 0 else max(0.0, min(1.0, observed_seconds / requested_seconds))
    payload.update({
        "observed_start": observed_start.isoformat(),
        "observed_end": observed_end.isoformat(),
        "coverage_ratio": float(coverage_ratio),
        "warning": "partial_coverage" if coverage_ratio < 0.95 else None,
    })
    return payload


def run_native_download_only(config: AppConfig) -> dict[str, Any]:
    session = build_http_session()
    limiter = build_rate_limiter(
        backend=config.rate_limit.backend,
        max_per_min=config.rate_limit.max_per_min,
        sqlite_path=config.rate_limit.sqlite_path,
        sqlite_timeout=config.rate_limit.sqlite_timeout,
        redis_url=config.rate_limit.redis_url,
    )
    out_dir = Path(config.run.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    trades_df, trades_diagnostics = fetch_aggtrades_dataframe(
        symbol=config.run.symbol,
        market=config.run.market,
        start=config.run.start,
        end=config.run.end,
        session=session,
        rate_limiter=limiter,
        limit=config.run.native_download_limit,
        return_diagnostics=True,
    )
    trades_path = out_dir / config.run.native_raw_trades_filename
    trades_df.to_csv(trades_path, index=False)
    trades_manifest = {
        'symbol': config.run.symbol,
        'market': config.run.market,
        'path': str(trades_path),
        'rows': int(len(trades_df)),
        'columns': list(trades_df.columns),
        'diagnostics': trades_diagnostics,
    }
    trades_manifest_path = export_json_manifest(trades_manifest, out_dir / 'native_download_trades_manifest.json')

    outputs: dict[str, Any] = {
        'trades': {'rows': int(len(trades_df)), 'path': str(trades_path), 'columns': list(trades_df.columns), 'diagnostics': trades_diagnostics, 'manifest': str(trades_manifest_path)}
    }

    if config.run.market == 'futures_um' and config.run.native_include_funding:
        funding_df = fetch_funding_rate_dataframe(config.run.symbol, config.run.start, config.run.end, session=session, rate_limiter=limiter, limit=config.run.native_download_limit)
        funding_path = out_dir / config.run.native_funding_filename
        funding_df.to_csv(funding_path, index=False)
        outputs['funding'] = {'rows': int(len(funding_df)), 'path': str(funding_path), 'columns': list(funding_df.columns), 'coverage': _time_coverage_summary(funding_df, config.run.start, config.run.end)}

    if config.run.market == 'futures_um' and config.run.native_include_metrics:
        metrics_df = fetch_open_interest_hist_dataframe(config.run.symbol, config.run.start, config.run.end, period=config.run.native_metrics_period, session=session, rate_limiter=limiter)
        metrics_path = out_dir / config.run.native_metrics_filename
        metrics_df.to_csv(metrics_path, index=False)
        outputs['metrics'] = {'rows': int(len(metrics_df)), 'path': str(metrics_path), 'columns': list(metrics_df.columns), 'coverage': _time_coverage_summary(metrics_df, config.run.start, config.run.end)}

    if config.run.market == 'futures_um' and config.run.native_include_liquidations:
        liq_df = fetch_liquidations_dataframe(config.run.symbol, config.run.start, config.run.end, session=session, rate_limiter=limiter, limit=config.run.native_download_limit)
        liq_path = out_dir / config.run.native_liquidations_filename
        liq_df.to_csv(liq_path, index=False)
        outputs['liquidations'] = {'rows': int(len(liq_df)), 'path': str(liq_path), 'columns': list(liq_df.columns), 'coverage': _time_coverage_summary(liq_df, config.run.start, config.run.end)}

    return {'mode': 'download-only', 'outputs': outputs}


def _load_native_input_frame(config: AppConfig):
    input_path = _resolve_native_input_path(config)
    if not input_path.exists():
        raise FileNotFoundError(f"Fichier d'entrée natif introuvable: {input_path}")
    return _load_frame(input_path), input_path


def _merge_sidecars(features: pd.DataFrame, config: AppConfig) -> tuple[pd.DataFrame, dict[str, Any]]:
    sidecars = _default_sidecar_paths(config)
    merged = features.copy()
    merge_info: dict[str, Any] = {}
    tolerance = config.run.native_merge_tolerance

    funding_path = sidecars['funding']
    if funding_path.exists():
        funding = canonicalize_columns(_load_frame(funding_path))
        if 'timestamp' in funding.columns:
            funding['timestamp'] = ensure_utc_timestamp(funding['timestamp'])
            merged = safe_merge_asof_past(merged, funding[['timestamp', 'funding_rate', 'mark_price']], on='timestamp', tolerance=tolerance, backend=config.run.native_acceleration_backend)
            merge_info['funding'] = {'path': str(funding_path), 'rows': int(len(funding)), 'coverage': _time_coverage_summary(funding, config.run.start, config.run.end)}

    metrics_path = sidecars['metrics']
    if metrics_path.exists():
        metrics = canonicalize_columns(_load_frame(metrics_path))
        if 'timestamp' in metrics.columns:
            metrics['timestamp'] = ensure_utc_timestamp(metrics['timestamp'])
            keep_cols = [c for c in ['timestamp', 'sum_open_interest', 'sum_open_interest_value', 'cmc_circulating_supply', 'count_toptrader_long_short_ratio'] if c in metrics.columns]
            merged = safe_merge_asof_past(merged, metrics[keep_cols], on='timestamp', tolerance=tolerance, backend=config.run.native_acceleration_backend)
            merge_info['metrics'] = {'path': str(metrics_path), 'rows': int(len(metrics)), 'coverage': _time_coverage_summary(metrics, config.run.start, config.run.end)}

    liq_path = sidecars['liquidations']
    if liq_path.exists():
        liq = canonicalize_columns(_load_frame(liq_path))
        if 'timestamp' in liq.columns:
            liq['timestamp'] = ensure_utc_timestamp(liq['timestamp'])
            liq['liquidation_notional'] = pd.to_numeric(liq.get('liquidation_notional'), errors='coerce')
            liq['liquidation_qty'] = pd.to_numeric(liq.get('liquidation_qty'), errors='coerce')
            liq_agg = resample_liquidations_optional_polars(
                liq,
                freq=config.run.freq,
                backend=config.run.native_acceleration_backend,
            )
            merged = safe_merge_asof_past(merged, liq_agg, on='timestamp', tolerance=tolerance, backend=config.run.native_acceleration_backend)
            merge_info['liquidations'] = {'path': str(liq_path), 'rows': int(len(liq)), 'coverage': _time_coverage_summary(liq, config.run.start, config.run.end)}

    merged_path = export_dataframe(merged, Path(config.run.out_dir) / config.run.native_merged_filename, fmt=None)
    merge_info['merged'] = {'path': str(merged_path), 'rows': int(len(merged))}
    return merged, merge_info


def run_native_feature_only(config: AppConfig) -> dict[str, Any]:
    trades_df, input_path = _load_native_input_frame(config)
    trades_df = canonicalize_columns(trades_df)
    if 'timestamp' not in trades_df.columns:
        raise ValueError("Le fichier natif doit contenir une colonne timestamp")
    features = build_microstructure_features_from_trades(
        trades_df,
        freq=config.run.freq,
        large_trade_quantile=float(config.features.large_trade_quantile),
        rolling_window=int(config.features.rolling_window),
        acceleration_backend=config.run.native_acceleration_backend,
    )
    if features.empty:
        requested_path = Path(config.run.out_dir) / config.run.native_features_filename
        out_path = export_dataframe(features, requested_path, fmt=None)
        return {"mode": "feature-only", "input": str(input_path), "rows": 0, "path": str(out_path)}

    features = features.rename(columns={
        'price_open': 'open',
        'price_high': 'high',
        'price_low': 'low',
        'price_close': 'close',
    })
    features['timestamp'] = ensure_utc_timestamp(features['timestamp'])
    features['return_1'] = pd.to_numeric(features['close'], errors='coerce').pct_change()
    features['close_z_96'] = rolling_zscore(pd.to_numeric(features['close'], errors='coerce'), window=96)
    features['label_tb'] = triple_barrier_label_ohlc(
        features,
        price_col='close',
        high_col='high',
        low_col='low',
        horizon=int(config.features.triple_barrier_horizon),
        pt=float(config.features.triple_barrier_pt),
        sl=float(config.features.triple_barrier_sl),
    )

    merged, merge_info = _merge_sidecars(features, config)
    out_path = export_dataframe(merged, Path(config.run.out_dir) / config.run.native_features_filename, fmt=None)
    nan_exports = None
    if config.features.nan_report:
        nan_exports = tuple(str(p) for p in export_nan_coverage_report(merged, Path(config.run.out_dir) / f"{config.run.symbol}_native_nan_report"))
    coverage_warnings = {name: meta.get("coverage") for name, meta in merge_info.items() if isinstance(meta, dict) and isinstance(meta.get("coverage"), dict) and meta["coverage"].get("warning")}
    return {
        "mode": "feature-only",
        "input": str(input_path),
        "rows": int(len(merged)),
        "path": str(out_path),
        "columns": list(merged.columns),
        "nan_report": nan_exports,
        "merge_info": merge_info,
        "coverage_warnings": coverage_warnings or None,
    }


def run_native_export_only(config: AppConfig) -> dict[str, Any]:
    df, input_path = _load_native_input_frame(config)
    export_path = export_dataframe(df, Path(config.run.out_dir) / config.run.native_export_filename, fmt=config.run.native_export_format)
    manifest = {
        "version": "9.1.2",
        "mode": "export-only",
        "input_path": str(input_path),
        "export_path": str(export_path),
        "rows": int(len(df)),
        "columns": list(df.columns),
    }
    manifest_path = export_json_manifest(manifest, Path(config.run.out_dir) / 'native_export_manifest.json')
    return {**manifest, "manifest": str(manifest_path)}


def run_native_full(config: AppConfig) -> dict[str, Any]:
    download = run_native_download_only(config)
    download_input = (download.get('outputs', {}).get('trades', {}).get('path') if isinstance(download, dict) else None) or (download.get('path') if isinstance(download, dict) else None)
    if not download_input:
        raise ValueError("Le stage download-only n'a pas retourné de path exploitable")
    config.run.native_input_path = download_input
    feature = run_native_feature_only(config)
    config.run.native_input_path = feature['path']
    export = run_native_export_only(config)
    return {
        'mode': 'full',
        'download': download,
        'feature': feature,
        'export': export,
    }
