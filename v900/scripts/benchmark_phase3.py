from __future__ import annotations

import argparse
import json
import platform
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from binance_quant_builder.accelerated import benchmark_backend, polars_available
from binance_quant_builder.features.microstructure import build_microstructure_features_from_trades

try:  # pragma: no cover - optional dependency
    import polars as pl  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pl = None

try:  # pragma: no cover - package metadata may be unavailable in editable mode
    from importlib.metadata import version as pkg_version
except Exception:  # pragma: no cover - fallback
    pkg_version = None


def _load_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    df = pd.read_csv(path)
    if "timestamp" in df.columns:
        parsed = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        if parsed.notna().any():
            df["timestamp"] = parsed
    return df


def _package_version() -> str | None:
    if pkg_version is None:
        return None
    try:
        return pkg_version("binance-quant-builder")
    except Exception:
        return None


def _build_payload(df: pd.DataFrame, args: argparse.Namespace, pandas_metrics: dict[str, float]) -> dict:
    payload = {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "freq": args.freq,
        "rolling_window": int(args.rolling_window),
        "large_trade_quantile": float(args.large_trade_quantile),
        "repeat": int(args.repeat),
        "input": str(Path(args.input).resolve()),
        "input_format": Path(args.input).suffix.lower().lstrip(".") or "csv",
        "benchmark_scope": "hybrid_pandas_to_polars" if polars_available() else "pandas_only",
        "benchmark_note": (
            "Le backend polars mesure le flux actuel du projet: lecture pandas éventuelle, "
            "conversion pandas→polars puis agrégation polars. Ce n'est pas un benchmark "
            "polars end-to-end pur."
        ),
        "dataset": {
            "memory_bytes": int(df.memory_usage(deep=True).sum()),
            "timestamp_dtype": str(df["timestamp"].dtype) if "timestamp" in df.columns else None,
        },
        "versions": {
            "package": _package_version(),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "pandas": pd.__version__,
            "polars": getattr(pl, "__version__", None) if pl is not None else None,
        },
        "polars_available": polars_available(),
        "pandas": pandas_metrics,
    }
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark phase 3 pandas vs polars sur les features microstructure.")
    parser.add_argument("input", help="Chemin vers un CSV/Parquet de trades natifs")
    parser.add_argument("--freq", default="1h")
    parser.add_argument("--rolling-window", type=int, default=1000)
    parser.add_argument("--large-trade-quantile", type=float, default=0.95)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--output", default="phase3_benchmark.json")
    args = parser.parse_args()

    df = _load_frame(Path(args.input))

    pandas_metrics = benchmark_backend(
        build_microstructure_features_from_trades,
        df,
        freq=args.freq,
        rolling_window=args.rolling_window,
        large_trade_quantile=args.large_trade_quantile,
        acceleration_backend="pandas",
        repeat=args.repeat,
        warmup=1,
    )
    payload = _build_payload(df, args, pandas_metrics)

    if polars_available():
        polars_metrics = benchmark_backend(
            build_microstructure_features_from_trades,
            df,
            freq=args.freq,
            rolling_window=args.rolling_window,
            large_trade_quantile=args.large_trade_quantile,
            acceleration_backend="polars",
            repeat=args.repeat,
            warmup=1,
        )
        payload["polars"] = polars_metrics
        payload["speedup_best_vs_pandas"] = (
            pandas_metrics["best_seconds"] / polars_metrics["best_seconds"]
            if polars_metrics["best_seconds"] > 0
            else None
        )

    out_path = Path(args.output)
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    print(f"Benchmark écrit dans {out_path}")


if __name__ == "__main__":
    main()
