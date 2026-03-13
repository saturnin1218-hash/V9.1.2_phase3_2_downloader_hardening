from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass(slots=True)
class RunConfig:
    symbol: str
    market: str = "futures_um"
    freq: str = "1h"
    start: str = "2024-01-01"
    end: str = "2026-03-10"
    out_dir: str = "./output"
    use_legacy_pipeline: bool = True
    native_mode: str = "plan"
    native_plan_filename: str = "native_execution_plan.json"
    native_raw_trades_filename: str = "native_raw_trades.csv"
    native_funding_filename: str = "native_funding.csv"
    native_metrics_filename: str = "native_metrics.csv"
    native_liquidations_filename: str = "native_liquidations.csv"
    native_merged_filename: str = "native_merged.csv"
    native_features_filename: str = "native_features.csv"
    native_export_filename: str = "native_export.csv"
    native_input_path: Optional[str] = None
    native_export_format: str = "csv"
    native_download_limit: int = 1000
    native_include_funding: bool = True
    native_include_metrics: bool = True
    native_include_liquidations: bool = False
    native_merge_tolerance: str = "6h"
    native_metrics_period: str = "1h"
    native_acceleration_backend: str = "auto"


@dataclass(slots=True)
class ParallelConfig:
    enabled: bool = False
    backend: str = "thread"
    workers: int = 4


@dataclass(slots=True)
class RateLimitConfig:
    backend: str = "sqlite"
    max_per_min: int = 60
    sqlite_timeout: float = 60.0
    sqlite_path: Optional[str] = None
    redis_url: str = "redis://localhost:6379/0"


@dataclass(slots=True)
class FeatureConfig:
    emit_ml_ready_normalized: bool = False
    ml_normalization_mode: str = "rolling_zscore"
    ml_normalization_window: int = 252
    log_per_symbol: bool = False
    nan_report: bool = False
    triple_barrier_horizon: int = 24
    triple_barrier_pt: float = 0.02
    triple_barrier_sl: float = 0.02
    large_trade_quantile: float = 0.95
    rolling_window: int = 1000


@dataclass(slots=True)
class QualityConfig:
    max_na_ratio_critical: float = 0.15
    min_rows: int = 2
    min_label_classes: int = 2
    allow_degraded_export: bool = False


@dataclass(slots=True)
class LegacyBridgeConfig:
    cache_dir: str = "binance_cache"
    chunksize: int = 250_000
    engine: str = "c"
    metrics_merge: str = "asof"
    max_metrics_staleness: str = "6h"
    max_download_workers: int = 8
    batch_size: int = 1
    api_rate_limit_jitter: float = 0.10
    skip_delisted: bool = False
    skip_partial_listing: bool = False
    force_redownload: bool = False
    force_reprocess: bool = False
    dropna_final: bool = False
    downcast_float32: bool = False
    gzip_csv: bool = False
    keep_extracted: bool = False
    include_offline_labels: bool = False
    include_oi_5m_api: bool = False
    include_liquidations: bool = False
    allow_target_columns_on_export: bool = False


@dataclass(slots=True)
class RetryConfig:
    max_attempts: int = 3
    backoff_seconds: float = 0.5
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 5.0
    retry_on_timeouts: bool = True
    retry_on_connection_errors: bool = True


@dataclass(slots=True)
class AppConfig:
    run: RunConfig
    parallel: ParallelConfig = field(default_factory=ParallelConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    features: FeatureConfig = field(default_factory=FeatureConfig)
    quality: QualityConfig = field(default_factory=QualityConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    legacy_bridge: LegacyBridgeConfig = field(default_factory=LegacyBridgeConfig)

    @property
    def out_dir_path(self) -> Path:
        return Path(self.run.out_dir)
