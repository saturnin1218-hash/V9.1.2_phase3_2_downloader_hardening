import argparse
import hashlib
import json
import logging
import re
import shutil
import zipfile
import threading
import time
import random
import os
import sqlite3
import tempfile
import atexit
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import gc
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable

# ============================================================
# CONFIG — ordre de déclaration corrigé, toutes les constantes
# référencées AVANT leur utilisation
# ============================================================

PIPELINE_VERSION = "V7.9.4-patched-rate-backend-timeout-mlnorm-redis-noop"

REQUEST_TIMEOUT = 60
DEFAULT_CHUNKSIZE = 250_000
DEFAULT_MAX_DOWNLOAD_WORKERS = 8
DEFAULT_MAX_METRICS_STALENESS = "6h"
VALID_FREQ = ["1min", "5min", "15min", "1h", "4h", "1d"]
SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9_]{3,}[A-Z0-9]$")

# ── Trades ──────────────────────────────────────────────────
TRADES_TIME_CANDIDATES = [
    "transact_time", "timestamp", "time", "T",
]
TRADES_QTY_CANDIDATES = ["quantity", "qty", "q"]
TRADES_PRICE_CANDIDATES = ["price", "p"]
TRADES_SIDEFLAG_CANDIDATES = [
    "is_buyer_maker", "m", "buyer_maker",
]

# ── Metrics ─────────────────────────────────────────────────
METRICS_TIME_CANDIDATES = [
    "create_time", "timestamp", "time",
]
METRICS_OI_CANDIDATES = [
    "sum_open_interest", "open_interest", "sum_open_interest_value",
]
METRICS_LS_RATIO_CANDIDATES = [
    "count_long_short_ratio", "long_short_ratio",
]
METRICS_LA_RATIO_CANDIDATES = [
    "count_long_account", "long_account", "long_account_ratio",
]
METRICS_SA_RATIO_CANDIDATES = [
    "count_short_account", "short_account", "short_account_ratio",
]
METRICS_SYMBOL_CANDIDATES = ["symbol", "pair"]
REQUIRED_METRIC_OUTPUT_COLS = [
    "sum_open_interest", "count_long_short_ratio",
]

# ── Funding — DOIT être déclaré AVANT SCHEMA_CONTRACTS ─────
FUNDING_TIME_CANDIDATES = [
    "create_time", "funding_time", "calc_time", "timestamp", "time",
]
FUNDING_RATE_CANDIDATES = [
    "funding_rate", "last_funding_rate",
]

# ── Contrats de schéma (référence les listes ci-dessus) ────
SCHEMA_CONTRACTS = {
    "metrics": {
        "time": METRICS_TIME_CANDIDATES,
        "required_any": [
            METRICS_OI_CANDIDATES,
            METRICS_LS_RATIO_CANDIDATES,
            METRICS_LA_RATIO_CANDIDATES,
            METRICS_SA_RATIO_CANDIDATES,
        ],
        "symbol": METRICS_SYMBOL_CANDIDATES,
    },
    "funding": {
        "time": FUNDING_TIME_CANDIDATES,
        "required_any": [FUNDING_RATE_CANDIDATES],
        "symbol": METRICS_SYMBOL_CANDIDATES,
    },
}

# ── Trade buckets ───────────────────────────────────────────
TRADE_NOTIONAL_BUCKET_EDGES = [
    0.0, 100.0, 1_000.0, 10_000.0, 100_000.0, np.inf,
]
TRADE_NOTIONAL_BUCKET_NAMES = [
    "tiny", "small", "medium", "large", "whale",
]

FLOW_FILL_EXACT_COLS = {
    "liquidations_count", "long_liq_count", "short_liq_count",
    "total_liq_qty", "long_liq_qty", "short_liq_qty",
    "total_liq_notional", "long_liq_notional", "short_liq_notional",
    "liq_long_share", "liq_short_share", "liq_imbalance",
}

STATE_FFILL_EXACT_COLS = {
    "sum_open_interest", "count_long_short_ratio",
    "count_long_account", "count_short_account",
    "funding_rate", "funding_rate_change", "funding_rate_abs",
    "oi_api_5m", "oi_api_5m_value", "oi_api_5m_change_abs",
    "oi_api_5m_change_pct",
}

# ── Headerless CSV schemas ──────────────────────────────────
HEADERLESS_TRADE_COLUMNS = [
    "agg_trade_id", "price", "qty", "first_trade_id",
    "last_trade_id", "transact_time", "is_buyer_maker",
]
HEADERLESS_METRICS_COLUMNS_9 = [
    "symbol", "sum_open_interest", "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio", "count_long_account",
    "count_short_account", "timestamp",
]
HEADERLESS_METRICS_COLUMNS_8 = [
    "symbol", "sum_open_interest", "sum_open_interest_value",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio", "count_long_account", "timestamp",
]
HEADERLESS_METRICS_COLUMNS_5 = [
    "symbol", "long_short_ratio", "long_account",
    "short_account", "timestamp",
]
HEADERLESS_METRICS_COLUMNS_4 = [
    "symbol", "sum_open_interest", "sum_open_interest_value",
    "timestamp",
]
HEADERLESS_FUNDING_COLUMNS_4 = [
    "symbol", "funding_rate", "funding_time", "mark_price",
]
HEADERLESS_FUNDING_COLUMNS_3 = [
    "symbol", "funding_rate", "funding_time",
]

# ── Liquidations / OI API ─────────────────────────────────
LIQUIDATION_TIME_CANDIDATES = [
    "trade_time", "event_time", "timestamp",
    "time", "update_time", "transact_time",
]
LIQUIDATION_SIDE_CANDIDATES = ["side", "s"]
LIQUIDATION_QTY_CANDIDATES = [
    "accumulated_filled_quantity", "cum_qty", "z",
    "original_quantity", "qty", "q",
]
LIQUIDATION_PRICE_CANDIDATES = [
    "average_price", "avg_price", "ap",
    "price", "p",
]
HEADERLESS_LIQUIDATION_COLUMNS_11 = [
    "symbol", "side", "order_type", "time_in_force",
    "original_quantity", "price", "average_price",
    "order_status", "last_filled_quantity",
    "accumulated_filled_quantity", "trade_time",
]
HEADERLESS_LIQUIDATION_COLUMNS_12 = [
    "symbol", "side", "order_type", "time_in_force",
    "original_quantity", "price", "average_price",
    "order_status", "last_filled_quantity",
    "accumulated_filled_quantity", "trade_time",
    "event_time",
]

# ── URLs Binance Vision ────────────────────────────────────
BASE_URLS = {
    "futures_um": "https://data.binance.vision/data/futures/um",
    "futures_cm": "https://data.binance.vision/data/futures/cm",
    "spot": "https://data.binance.vision/data/spot",
}

# ── Downcast : colonnes à ne JAMAIS downcaster ─────────────
# (ratios, z-scores, funding dérivés où la précision compte)
DOWNCAST_EXCLUDE_PATTERNS = [
    "_z_", "_pct", "ratio", "funding_rate", "entropy",
    "whale_pressure", "imbalance",
]

logger = logging.getLogger("BinanceDatasetBuilderV67")


# ============================================================
# LOGGING
# ============================================================

def _handler_exists(handler_type: type, log_path: Path | None = None) -> bool:
    target = str(log_path.resolve()) if log_path is not None else None
    for h in logger.handlers:
        if isinstance(h, handler_type):
            if target is None:
                return True
            existing = getattr(h, "baseFilename", None)
            if existing and str(Path(existing).resolve()) == target:
                return True
    return False


def setup_logger(log_path: Path | None = None, level: int = logging.INFO) -> None:
    """Configure le logger global de manière idempotente.

    - Ajoute toujours un StreamHandler unique.
    - Ajoute un FileHandler unique par chemin demandé.
    - N'échoue jamais si le logging fichier pose problème.
    """
    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    if not _handler_exists(logging.StreamHandler):
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    if log_path is not None and not _handler_exists(logging.FileHandler, log_path):
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(log_path, encoding="utf-8")
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except Exception:
            pass

    logger.propagate = False


def log(msg: str, level: str = "INFO") -> None:
    getattr(logger, level.lower())(msg)


# ============================================================
# CLI
# ============================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "V6.9 Binance Vision/API → dataset quant. "
            "Incrémental, robuste mémoire, multi-symboles, "
            "funding regimes dynamiques, runnable VS Code."
        ),
    )
    p.add_argument("--symbol", help="Symbole unique (ex: BTCUSDT)")
    p.add_argument(
        "--symbols",
        help=(
            "Liste de symboles séparés par des virgules "
            "(ex: BTCUSDT,ETHUSDT,SOLUSDT)"
        ),
    )
    p.add_argument(
        "--symbols_file",
        help="Fichier texte avec un symbole par ligne",
    )
    p.add_argument(
        "--market", default="futures_um", choices=list(BASE_URLS),
    )
    p.add_argument("--freq", default="1h", choices=VALID_FREQ)
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--out_dir", required=True)
    p.add_argument("--cache_dir", default="binance_cache")
    p.add_argument("--chunksize", type=int, default=DEFAULT_CHUNKSIZE)
    p.add_argument(
        "--engine", default="c", choices=["c", "python", "pyarrow"],
    )
    p.add_argument(
        "--metrics_merge", default="asof",
        choices=["asof", "left_ffill", "inner", "left"],
    )
    p.add_argument(
        "--max_metrics_staleness", default=DEFAULT_MAX_METRICS_STALENESS,
    )
    p.add_argument(
        "--max_download_workers", type=int,
        default=DEFAULT_MAX_DOWNLOAD_WORKERS,
    )
    p.add_argument("--force_redownload", action="store_true")
    p.add_argument("--force_reprocess", action="store_true")
    p.add_argument("--dropna_final", action="store_true")
    p.add_argument("--downcast_float32", action="store_true")
    p.add_argument("--gzip_csv", action="store_true")
    p.add_argument("--keep_extracted", action="store_true")
    p.add_argument(
        "--include_offline_labels", action="store_true",
        help=(
            "Ajoute target_triple_barrier au dataset final. "
            "Label offline avec look-ahead assumé, à ne pas "
            "utiliser comme feature temps réel."
        ),
    )
    p.add_argument(
        "--include_oi_5m_api", action="store_true",
        help=(
            "Ajoute l'open interest 5m via l'API officielle "
            "Binance Futures. Historique limité à ~30 jours."
        ),
    )
    p.add_argument(
        "--include_liquidations", action="store_true",
        help=(
            "Ajoute les liquidations historiques Binance Vision "
            "(liquidationSnapshot) quand disponibles."
        ),
    )
    p.add_argument(
        "--allow_target_columns_on_export", action="store_true",
        help=(
            "Autorise l'export des colonnes target_* (labels ML). "
            "ATTENTION : ces colonnes utilisent du look-ahead et ne doivent "
            "jamais être utilisées pour backtest ou live trading."
        ),
    )
    p.add_argument(
        "--keep_cvd_raw_on_export",
        action="store_true",
        help=(
            "Conserve la colonne cvd brute dans les exports finaux. "
            "Par défaut, elle est exclue car son niveau dépend du point de départ local."
        ),
    )
    p.add_argument(
        "--funding_spike_abs_threshold", type=float, default=0.001,
        help=(
            "Seuil absolu mild de funding au-delà duquel un spike est signalé "
            "(ex: 0.001 = 0.10%%)."
        ),
    )
    p.add_argument(
        "--funding_spike_rel_multiplier", type=float, default=5.0,
        help=(
            "Multiplicateur du MAD glissant pour détecter les spikes contextuels de funding."
        ),
    )
    p.add_argument(
        "--api_max_per_min", type=float, default=60.0,
        help=(
            "Nombre maximum cible de requêtes API REST Binance par minute, "
            "par processus. Les téléchargements de fichiers Vision ne sont pas concernés."
        ),
    )
    p.add_argument(
        "--api_rate_limit_jitter", type=float, default=0.10,
        help=(
            "Jitter aléatoire additionnel (en secondes) appliqué aux requêtes API REST "
            "pour lisser les rafales."
        ),
    )
    p.add_argument(
        "--api_rate_backend", default="sqlite", choices=["sqlite", "shared_memory", "redis", "noop"],
        help=(
            "Backend du rate limiter API. 'sqlite' = coordination inter-processus via SQLite; "
            "'shared_memory' = mode léger sans SQLite, qui répartit simplement le quota entre workers; "
            "'redis' = coordination inter-processus via Redis; 'noop' = désactivation explicite."
        ),
    )
    p.add_argument(
        "--api_rate_sqlite_timeout", type=float, default=60.0,
        help=(
            "Timeout SQLite (secondes) pour le backend sqlite du rate limiter inter-processus."
        ),
    )
    p.add_argument(
        "--batch_size", type=int, default=1,
        help=(
            "Taille de batch logique pour le mode multi-symboles."
        ),
    )
    p.add_argument(
        "--parallel", action="store_true",
        help="Active le traitement parallèle des symboles à l'intérieur de chaque batch.",
    )
    p.add_argument(
        "--parallel_backend", default="process", choices=["process", "thread"],
        help=(
            "Backend de parallélisme pour le mode batch. 'process' est recommandé "
            "pour les calculs lourds par symbole; 'thread' reste utile pour l'I/O."
        ),
    )
    p.add_argument(
        "--parallel_workers", type=int, default=0,
        help=(
            "Nombre de workers pour --parallel. 0 = min(batch_size, CPU/logique)."
        ),
    )
    p.add_argument(
        "--skip_delisted", action="store_true",
        help=(
            "Ignore les symboles delistés ou partiels à cause d'un listing tardif "
            "ou d'un delisting/settlement avant --end."
        ),
    )
    p.add_argument(
        "--skip_partial_listing", action="store_true",
        help=(
            "Ignore les symboles listés après --start ou delistés avant --end, "
            "même s'ils sont partiellement couverts sur la période demandée."
        ),
    )
    p.add_argument(
        "--log_per_symbol", action="store_true",
        help=(
            "Crée un fichier de log dédié par symbole dans output/logs/ en plus de la console."
        ),
    )
    p.add_argument(
        "--nan_report", action="store_true",
        help=(
            "Exporte un rapport JSON/CSV de couverture des NaN par feature et de l'impact du dropna final."
        ),
    )
    p.add_argument(
        "--emit_ml_ready_normalized", action="store_true",
        help=(
            "Exporte un dataset ML-ready supplémentaire avec normalisation stricte passée par symbole."
        ),
    )
    p.add_argument(
        "--ml_normalization_mode", default="rolling_zscore", choices=["rolling_zscore", "expanding_zscore"],
        help=(
            "Mode de normalisation pour l'export ML-ready."
        ),
    )
    p.add_argument(
        "--ml_normalization_window", type=int, default=252,
        help=(
            "Fenêtre de normalisation pour le mode rolling_zscore de l'export ML-ready."
        ),
    )
    return p.parse_args()


def _parse_symbol_list(args: argparse.Namespace) -> List[str]:
    symbols: List[str] = []
    if getattr(args, "symbol", None):
        symbols.append(str(args.symbol).strip())
    raw_symbols = getattr(args, "symbols", None)
    if raw_symbols:
        symbols.extend([s.strip() for s in str(raw_symbols).split(",") if s.strip()])
    symbols_file = getattr(args, "symbols_file", None)
    if symbols_file:
        for line in Path(symbols_file).read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                symbols.append(line)
    cleaned: List[str] = []
    seen = set()
    for sym in symbols:
        up = sym.upper()
        if up and up not in seen:
            cleaned.append(up)
            seen.add(up)
    return cleaned


def validate_args(args: argparse.Namespace) -> None:
    symbols = _parse_symbol_list(args)
    if not symbols:
        raise ValueError(
            "Fournir au moins un symbole via --symbol, --symbols ou --symbols_file"
        )
    for sym in symbols:
        if not SYMBOL_RE.match(sym.upper()):
            raise ValueError(
                f"Symbole invalide: {sym} (ex: BTCUSDT, BTCUSD_PERP)"
            )
    args.symbols_resolved = symbols
    args.symbol = symbols[0]
    if args.chunksize <= 0:
        raise ValueError("--chunksize doit être > 0")
    if args.max_download_workers <= 0:
        raise ValueError("--max_download_workers doit être > 0")
    if args.batch_size <= 0:
        raise ValueError("--batch_size doit être > 0")
    if args.parallel_workers < 0:
        raise ValueError("--parallel_workers doit être >= 0")
    if args.api_max_per_min <= 0:
        raise ValueError("--api_max_per_min doit être > 0")
    if args.api_rate_limit_jitter < 0:
        raise ValueError("--api_rate_limit_jitter doit être >= 0")
    if args.funding_spike_abs_threshold < 0:
        raise ValueError("--funding_spike_abs_threshold doit être >= 0")
    if args.funding_spike_rel_multiplier < 0:
        raise ValueError("--funding_spike_rel_multiplier doit être >= 0")
    start = pd.Timestamp(args.start)
    end = pd.Timestamp(args.end)
    if end < start:
        raise ValueError("--end doit être >= --start")
    if args.engine == "pyarrow" and args.chunksize is not None:
        log(
            "PyArrow + chunksize surconsomme la RAM. "
            "engine='c' recommandé.",
            level="WARNING",
        )

# ============================================================
# UTILS
# ============================================================


class SkipSymbolError(RuntimeError):
    pass


def clone_args_for_symbol(args: argparse.Namespace, symbol: str, out_dir: Optional[str] = None) -> argparse.Namespace:
    data = vars(args).copy()
    data["symbol"] = symbol.upper()
    if out_dir is not None:
        data["out_dir"] = out_dir
    return argparse.Namespace(**data)


def _extract_exchange_ts(info: Dict[str, object], *keys: str) -> Optional[pd.Timestamp]:
    for key in keys:
        value = info.get(key)
        if value in (None, "", 0, "0"):
            continue
        try:
            ts = pd.to_datetime(int(value), unit="ms", utc=True)
            if pd.notna(ts):
                return ts
        except Exception:
            try:
                ts = pd.to_datetime(value, utc=True)
                if pd.notna(ts):
                    return ts
            except Exception:
                pass
    return None


def assess_symbol_period_status(
    exchange_info: Dict[str, object],
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
) -> Dict[str, object]:
    status = "active"
    onboard_dt = _extract_exchange_ts(exchange_info, "onboard_date", "onboarddate")
    delist_dt = _extract_exchange_ts(exchange_info, "delivery_date", "deliverydate", "delist_time", "delisttime")
    contract_type = str(exchange_info.get("contract_type", "")).upper()
    if contract_type == "PERPETUAL" and delist_dt is not None and delist_dt.year >= 2090:
        delist_dt = None

    if onboard_dt is not None and onboard_dt > end_dt:
        status = "future_listing"
    elif delist_dt is not None and delist_dt < start_dt:
        status = "delisted_before_period"
    elif onboard_dt is not None and onboard_dt > start_dt and delist_dt is not None and delist_dt < end_dt:
        status = "partial_listing_and_delisting"
    elif onboard_dt is not None and onboard_dt > start_dt:
        status = "partial_listing"
    elif delist_dt is not None and delist_dt < end_dt:
        status = "partial_delisting"

    return {
        "symbol_status": status,
        "onboard_date_utc": None if onboard_dt is None else onboard_dt.isoformat(),
        "delist_time_utc": None if delist_dt is None else delist_dt.isoformat(),
        "status_now": str(exchange_info.get("status", "") or exchange_info.get("contract_status", "")).upper(),
        "contract_type": contract_type,
        "period_partial": int(status in {"partial_listing", "partial_delisting", "partial_listing_and_delisting"}),
    }

def _run_timestamp() -> str:
    """Timestamp du run, calculé à l'appel (pas à l'import)."""
    return pd.Timestamp.now(tz="UTC").isoformat()


def make_cache_key(args: argparse.Namespace) -> str:
    raw = (
        f"{args.market}|{args.symbol.upper()}|{args.freq}|"
        f"{args.start}|{args.end}|{args.metrics_merge}|"
        f"{args.max_metrics_staleness}"
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _col_is_downcast_safe(col_name: str) -> bool:
    """Retourne False si la colonne ne doit pas être downcastée."""
    name_lower = col_name.lower()
    return not any(pat in name_lower for pat in DOWNCAST_EXCLUDE_PATTERNS)


def maybe_downcast(
    df: pd.DataFrame, enabled: bool,
) -> pd.DataFrame:
    if not enabled:
        return df
    for col in df.columns:
        if not _col_is_downcast_safe(col):
            continue
        if pd.api.types.is_float_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], downcast="float")
        elif pd.api.types.is_integer_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], downcast="integer")
    return df


def rolling_zscore(
    s: pd.Series,
    window: int,
    min_periods: Optional[int] = None,
    std_epsilon: float = 1e-8,
    fill_low_var_with: float = 0.0,
) -> pd.Series:
    """
    Z-score rolling robuste.
    Si la variance locale est quasi nulle, renvoie fill_low_var_with (0.0 par défaut).
    Évite les z-scores explosifs sur séries quasi-constantes.
    """
    s = pd.to_numeric(s, errors="coerce")
    mp = min_periods if min_periods is not None else max(5, window // 5)

    mu = s.rolling(window, min_periods=mp).mean()
    sd = s.rolling(window, min_periods=mp).std(ddof=0)

    z = (s - mu) / sd
    low_var_mask = sd.isna() | (sd < std_epsilon)
    z = z.mask(low_var_mask, fill_low_var_with)
    z = z.clip(-10, 10)  # bonus anti-outliers pour scalers/tree models
    return z


def pick_first_existing(
    columns: List[str], candidates: List[str],
) -> Optional[str]:
    colset = set(columns)
    for c in candidates:
        if c in colset:
            return c
    return None


def parse_binance_datetime(series: pd.Series) -> pd.Series:
    s_num = pd.to_numeric(series, errors="coerce")
    if s_num.notna().any():
        median_val = s_num.dropna().median()
        if median_val > 1e14:
            return pd.to_datetime(
                s_num, unit="us", errors="coerce", utc=True,
            )
        if median_val > 1e11:
            return pd.to_datetime(
                s_num, unit="ms", errors="coerce", utc=True,
            )
        if median_val > 1e9:
            return pd.to_datetime(
                s_num, unit="s", errors="coerce", utc=True,
            )
    s_str = series.astype("string").str.strip()
    iso = pd.to_datetime(
        s_str, format="ISO8601", errors="coerce", utc=True,
    )
    if iso.notna().any():
        return iso
    return pd.to_datetime(s_str, errors="coerce", utc=True)


def safe_to_numeric(
    values,
    errors: str = "coerce",
    downcast: Optional[str] = None,
) -> pd.Series:
    """
    Conversion numérique robuste pour séries/object/NA pandas.
    Gère proprement pd.NA / NAType, booléens, strings Binance et valeurs vides.
    """
    if values is None:
        return pd.Series(dtype="float64")
    s = values if isinstance(values, pd.Series) else pd.Series(values)
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors=errors, downcast=downcast)
    if pd.api.types.is_bool_dtype(s):
        return pd.to_numeric(s.astype("Int8"), errors=errors, downcast=downcast)
    s2 = s.astype("string")
    s2 = s2.replace({
        "<NA>": pd.NA,
        "NA": pd.NA,
        "NaN": pd.NA,
        "nan": pd.NA,
        "None": pd.NA,
        "null": pd.NA,
        "": pd.NA,
    })
    return pd.to_numeric(s2, errors=errors, downcast=downcast)


def normalize_bool_series(series: pd.Series) -> pd.Series:
    """
    Normalisation robuste de booléens Binance/CSV/API.
    Retourne une série pandas nullable boolean.
    """
    if series is None:
        return pd.Series(dtype="boolean")
    s = series if isinstance(series, pd.Series) else pd.Series(series)
    if pd.api.types.is_bool_dtype(s):
        return s.astype("boolean")

    mapper = {
        "true": True, "false": False,
        "1": True, "0": False,
        "t": True, "f": False,
        "yes": True, "no": False,
        "y": True, "n": False,
        "buyer": True, "seller": False,
        "buy": True, "sell": False,
    }

    def _map_one(x):
        if x is pd.NA or pd.isna(x):
            return pd.NA
        if isinstance(x, (bool, np.bool_)):
            return bool(x)
        if isinstance(x, (int, np.integer, float, np.floating)) and not pd.isna(x):
            if float(x) == 1.0:
                return True
            if float(x) == 0.0:
                return False
        return mapper.get(str(x).strip().lower(), pd.NA)

    return s.map(_map_one).astype("boolean")


def debug_frame_snapshot(name: str, df: Optional[pd.DataFrame], max_cols: int = 30) -> None:
    if df is None:
        log(f"[SNAPSHOT] {name}: None", level="DEBUG")
        return
    if not isinstance(df, pd.DataFrame):
        log(f"[SNAPSHOT] {name}: not_a_dataframe={type(df).__name__}", level="DEBUG")
        return
    cols = list(df.columns[:max_cols])
    dtype_map = {c: str(df[c].dtype) for c in cols}
    na_ratio = {}
    for c in cols:
        try:
            na_ratio[c] = round(float(df[c].isna().mean()), 4)
        except Exception:
            na_ratio[c] = None
    log(
        f"[SNAPSHOT] {name} shape={df.shape} cols={cols} dtypes={dtype_map} na_ratio={na_ratio}",
        level="DEBUG",
    )


def _supports_dtype_backend() -> bool:
    try:
        import inspect
        return "dtype_backend" in inspect.signature(
            pd.read_csv
        ).parameters
    except Exception:
        return False


def count_csv_fields(
    path: Path, max_lines: int = 3,
) -> Optional[int]:
    try:
        import gzip
        opener = (
            gzip.open if path.suffix.lower() == ".gz" else open
        )
        with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
            for _ in range(max_lines):
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if line:
                    return len(line.split(","))
    except Exception:
        return None
    return None


def read_first_non_empty_line(path: Path) -> str:
    try:
        import gzip
        opener = (
            gzip.open if path.suffix.lower() == ".gz" else open
        )
        with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line:
                    return line
    except Exception:
        return ""
    return ""


def file_looks_headered(
    path: Path, candidates: List[str],
) -> bool:
    line = read_first_non_empty_line(path)
    if not line:
        return False
    parts = [
        p.strip().strip('"').strip("'") for p in line.split(",")
    ]
    part_set = {p for p in parts if p}
    cand_set = set(candidates)
    return any(p in cand_set for p in part_set)


def _ncols_to_headerless_metrics_schema(
    ncols: Optional[int],
) -> Optional[dict]:
    mapping = {
        4: HEADERLESS_METRICS_COLUMNS_4,
        5: HEADERLESS_METRICS_COLUMNS_5,
        8: HEADERLESS_METRICS_COLUMNS_8,
        9: HEADERLESS_METRICS_COLUMNS_9,
    }
    names = mapping.get(ncols)
    if names is None:
        return None
    return {"header": None, "names": names}


def sha256_file(
    path: Path, chunk_size: int = 1024 * 1024,
) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def fetch_remote_checksum(
    session: requests.Session,
    url: str,
    timeout: int = REQUEST_TIMEOUT,
) -> Optional[str]:
    try:
        r = session.get(url + ".CHECKSUM", timeout=timeout)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        line = r.text.strip().splitlines()[0].strip()
        if not line:
            return None
        return line.split()[0].strip().lower()
    except Exception:
        return None


def normalize_colname(name: str) -> str:
    s = str(name).strip()
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s).strip("_").lower()
    return s


def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {c: normalize_colname(c) for c in df.columns}
    if len(set(renamed.values())) != len(renamed):
        seen: Dict[str, int] = {}
        dedup = {}
        for old, new in renamed.items():
            k = seen.get(new, 0)
            seen[new] = k + 1
            dedup[old] = new if k == 0 else f"{new}__dup{k}"
        renamed = dedup
    return df.rename(columns=renamed)


def validate_schema_contract(
    columns: List[str],
    data_type: str,
    source_name: str,
) -> Dict[str, object]:
    contract = SCHEMA_CONTRACTS.get(data_type)
    if contract is None:
        return {
            "data_type": data_type,
            "source": source_name,
            "columns": list(columns),
            "note": "no contract defined",
        }
    cols = list(columns)
    time_col = pick_first_existing(cols, contract["time"])
    matches = []
    missing_groups = []
    for group in contract["required_any"]:
        matched = pick_first_existing(cols, group)
        matches.append(matched)
        if matched is None:
            missing_groups.append(group)
    info = {
        "data_type": data_type,
        "source": source_name,
        "columns": cols,
        "time_col": time_col,
        "matched_required": [m for m in matches if m is not None],
        "missing_groups": missing_groups,
    }
    if time_col is None:
        raise ValueError(
            f"Schéma {data_type} invalide pour {source_name}: "
            f"colonne temps absente. Colonnes lues: {cols}"
        )
    if not info["matched_required"]:
        raise ValueError(
            f"Schéma {data_type} invalide pour {source_name}: "
            f"aucune colonne essentielle reconnue. "
            f"Colonnes lues: {cols}"
        )
    return info


def validate_partial_content(
    df: pd.DataFrame, data_type: str, source_name: str,
) -> None:
    if df is None or df.empty:
        raise ValueError(
            f"Partial {data_type} vide pour {source_name}"
        )
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(
            f"Partial {data_type} sans DatetimeIndex "
            f"pour {source_name}"
        )
    if df.index.tz is None:
        raise ValueError(
            f"Partial {data_type} non timezone-aware "
            f"pour {source_name}"
        )
    if not df.index.is_monotonic_increasing:
        raise ValueError(
            f"Partial {data_type} non trié pour {source_name}"
        )
    dup = int(df.index.duplicated(keep=False).sum())
    if dup > 0:
        raise ValueError(
            f"Partial {data_type} avec {dup} timestamps "
            f"dupliqués pour {source_name}"
        )

    critical_map = {
        "metrics": [
            "sum_open_interest", "count_long_short_ratio",
            "count_long_account", "count_short_account",
        ],
        "funding": ["funding_rate"],
        "trade": [
            "open", "high", "low", "close",
            "volume", "trade_count",
        ],
    }
    quasi_constant_guard_cols = {
        "open", "high", "low", "close",
        "volume", "trade_count", "sum_open_interest",
    }
    for col in critical_map.get(data_type, []):
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        non_na = s.dropna()
        non_na_ratio = float(s.notna().mean()) if len(s) else 0.0
        if non_na_ratio < 0.05:
            raise ValueError(
                f"Partial {data_type} colonne critique quasi "
                f"vide: {col} ({non_na_ratio:.2%} non-NaN) "
                f"dans {source_name}"
            )
        nunique = int(non_na.nunique(dropna=True)) if len(non_na) else 0
        if (
            col in quasi_constant_guard_cols
            and len(non_na) >= 50
            and nunique <= 1
        ):
            raise ValueError(
                f"Partial {data_type} colonne critique quasi "
                f"constante: {col} dans {source_name}"
            )
        nonneg_cols = {
            "sum_open_interest", "trade_count",
            "volume", "notional",
        }
        if col in nonneg_cols:
            neg = int((s < 0).fillna(False).sum())
            if neg > 0:
                raise ValueError(
                    f"Partial {data_type} colonne {col} "
                    f"contient {neg} valeurs négatives "
                    f"dans {source_name}"
                )


def build_source_manifest(
    paths: List[Path], max_entries: int = 200,
) -> Dict[str, object]:
    items = []
    for p in sorted(paths)[:max_entries]:
        try:
            st = p.stat()
        except OSError:
            continue
        fingerprint = hashlib.sha256(
            f"{p.name}|{st.st_size}|{int(st.st_mtime)}".encode()
        ).hexdigest()[:16]
        items.append({
            "name": p.name,
            "size": int(st.st_size),
            "mtime": int(st.st_mtime),
            "fingerprint": fingerprint,
        })
    return {
        "count": len(paths),
        "sampled": len(items),
        "files": items,
    }


def compute_non_null_stats(
    df: pd.DataFrame, cols: List[str],
) -> Dict[str, Dict[str, float]]:
    stats: Dict[str, Dict[str, float]] = {}
    if df is None or df.empty:
        return stats
    for col in cols:
        if col in df.columns:
            s = df[col]
            stats[col] = {
                "non_null_ratio": round(float(s.notna().mean()), 6),
                "null_count": int(s.isna().sum()),
            }
    return stats


def build_component_status(
    *,
    requested: bool,
    downloaded: bool,
    integrated: bool,
    reason: str = "",
) -> Dict[str, object]:
    return {
        "requested": int(bool(requested)),
        "downloaded": int(bool(downloaded)),
        "integrated": int(bool(integrated)),
        "reason": str(reason or ""),
    }


def normalize_batch_result(result: object, symbol_hint: str = "") -> Dict[str, object]:
    """Normalise un résultat batch pour éviter les None / types inattendus."""
    if isinstance(result, dict):
        out = dict(result)
        out.setdefault("symbol", symbol_hint or str(out.get("symbol", "")))
        out.setdefault("status", "ok")
        out.setdefault("run_dir", "")
        out.setdefault("report_json", "")
        out.setdefault("rows_final", None)
        out.setdefault("symbol_status", None)
        out.setdefault("export_status", out.get("status", "ok"))
        return out
    return {
        "symbol": symbol_hint,
        "status": "error",
        "run_dir": "",
        "report_json": "",
        "rows_final": None,
        "symbol_status": None,
        "export_status": "error",
        "error": f"unexpected_result_type:{type(result).__name__}",
    }


def annotate_component_matrix(
    report: Dict[str, object],
    *,
    market: str,
    include_oi_5m_api: bool,
    include_liquidations: bool,
    trades_df: Optional[pd.DataFrame],
    metrics_df: Optional[pd.DataFrame],
    funding_df: Optional[pd.DataFrame],
    oi_api_df: Optional[pd.DataFrame],
    liquidation_df: Optional[pd.DataFrame],
) -> None:
    trades_ok = trades_df is not None and not trades_df.empty
    metrics_ok = metrics_df is not None and not metrics_df.empty
    funding_ok = funding_df is not None and not funding_df.empty
    oi_ok = oi_api_df is not None and not oi_api_df.empty
    liq_ok = liquidation_df is not None and not liquidation_df.empty

    report["components_requested"] = {
        "trades": 1,
        "metrics": int(market != "spot"),
        "funding": int(market != "spot"),
        "oi_api_5m": int(bool(include_oi_5m_api)),
        "liquidations": int(bool(include_liquidations)),
    }
    report["components_integrated"] = {
        "trades": int(trades_ok),
        "metrics": int(metrics_ok),
        "funding": int(funding_ok),
        "oi_api_5m": int(oi_ok),
        "liquidations": int(liq_ok),
    }
    report["component_status"] = {
        "trades": build_component_status(True, trades_ok, trades_ok, "" if trades_ok else "trades_missing_or_empty"),
        "metrics": build_component_status(market != "spot", metrics_ok, metrics_ok, "" if metrics_ok or market == "spot" else "metrics_missing_or_empty"),
        "funding": build_component_status(market != "spot", funding_ok, funding_ok, "" if funding_ok or market == "spot" else "funding_missing_or_empty"),
        "oi_api_5m": build_component_status(bool(include_oi_5m_api), oi_ok, oi_ok, "" if oi_ok or not include_oi_5m_api else "oi_api_missing_or_failed"),
        "liquidations": build_component_status(bool(include_liquidations), liq_ok, liq_ok, "" if liq_ok or not include_liquidations else "liquidations_missing_or_empty"),
    }

def infer_trade_notional_bucket_edges(symbol_hint: str) -> List[float]:
    s = str(symbol_hint or "").upper()
    multiplier = 1.0
    if any(x in s for x in ("BTC", "ETH")):
        multiplier = 10.0
    elif any(x in s for x in ("BNB", "SOL")):
        multiplier = 3.0
    return [
        0.0,
        100.0 * multiplier,
        1_000.0 * multiplier,
        10_000.0 * multiplier,
        100_000.0 * multiplier,
        np.inf,
    ]


def derive_trade_notional_bucket_edges(
    values: pd.Series, symbol_hint: str,
) -> List[float]:
    fallback = infer_trade_notional_bucket_edges(symbol_hint)
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype="float64")
    arr = arr[np.isfinite(arr)]
    arr = arr[arr > 0]
    if arr.size < 200:
        return fallback

    try:
        q = np.quantile(arr, [0.0, 0.50, 0.80, 0.95, 0.995])
        if not np.isfinite(q).all():
            return fallback
        edges = [0.0]
        prev = 0.0
        min_step = max(float(np.median(arr)) * 1e-6, 1e-9)
        for x in q[1:]:
            xv = float(max(x, prev + min_step))
            edges.append(xv)
            prev = xv
        edges.append(np.inf)
        if len(edges) != 6:
            return fallback
        return edges
    except Exception:
        return fallback


def infer_trade_bucket_edges_from_sample(
    csv_path: Path,
    chunksize: int = 250_000,
    target_sample_size: int = 200_000,
    max_chunks: int = 8,
) -> dict:
    """
    Infère les seuils small/medium/large/whale à partir d'un échantillon
    multi-chunks pour éviter le biais du premier chunk.
    """
    notionals = []
    total = 0

    for i, chunk in enumerate(read_csv_smart(csv_path, chunksize=chunksize)):
        if i >= max_chunks:
            break

        chunk = canonicalize_columns(chunk)

        price_col = pick_first_existing(chunk.columns, ["price", "p"])
        qty_col = pick_first_existing(chunk.columns, ["qty", "quantity", "q"])

        if price_col is None or qty_col is None:
            continue

        px = pd.to_numeric(chunk[price_col], errors="coerce")
        qty = pd.to_numeric(chunk[qty_col], errors="coerce")
        notional = (px * qty).replace([np.inf, -np.inf], np.nan).dropna()

        if not notional.empty:
            notionals.append(notional)
            total += len(notional)

        if total >= target_sample_size:
            break

    if not notionals:
        return {
            "small": 100,
            "medium": 1_000,
            "large": 10_000,
            "whale": 100_000,
            "source": "fallback_default",
        }

    all_notionals = pd.concat(notionals, ignore_index=True)

    return {
        "small": float(all_notionals.quantile(0.50)),
        "medium": float(all_notionals.quantile(0.80)),
        "large": float(all_notionals.quantile(0.95)),
        "whale": float(all_notionals.quantile(0.995)),
        "source": "sampled_multichunk",
        "sample_size": int(len(all_notionals)),
    }


def _apply_flow_state_fill(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()

    flow_cols = []
    for col in out.columns:
        lc = col.lower()
        if col in FLOW_FILL_EXACT_COLS:
            flow_cols.append(col)
            continue
        if lc.startswith("liq_") or lc.startswith("long_liq_") or lc.startswith("short_liq_"):
            flow_cols.append(col)
            continue

    for col in dict.fromkeys(flow_cols):
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    state_cols = [c for c in out.columns if c in STATE_FFILL_EXACT_COLS]
    if state_cols:
        out[state_cols] = out[state_cols].ffill()

    if "buy_sell_imbalance" in out.columns:
        out["buy_sell_imbalance"] = pd.to_numeric(
            out["buy_sell_imbalance"], errors="coerce"
        ).fillna(0.0)
    return out


def infer_dropna_subset(df: pd.DataFrame) -> List[str]:
    if df is None or df.empty:
        return []

    core_market_cols = [
        "open", "high", "low", "close",
        "volume", "trade_count",
    ]
    subset = [c for c in core_market_cols if c in df.columns]

    optional_state_cols = [
        "sum_open_interest",
        "count_long_short_ratio",
        "funding_rate",
        "oi_api_5m",
    ]
    for col in optional_state_cols:
        if col not in df.columns:
            continue
        coverage = float(df[col].notna().mean()) if len(df) else 0.0
        if coverage >= 0.98:
            subset.append(col)

    return list(dict.fromkeys(subset))


def compute_merge_diagnostics(
    merged: pd.DataFrame,
    aux_cols: List[str],
    aux_time_col: Optional[str],
    mode: str,
    tolerance: Optional[str],
) -> Dict[str, object]:
    diag: Dict[str, object] = {
        "mode": mode,
        "tolerance": tolerance,
        "rows_total": int(len(merged)),
        "aux_columns": aux_cols,
        "non_null": compute_non_null_stats(merged, aux_cols),
    }
    if aux_cols:
        mask = pd.Series(False, index=merged.index)
        for c in aux_cols:
            if c in merged.columns:
                mask = mask | merged[c].notna()
        diag["rows_with_any_aux"] = int(mask.sum())
        diag["rows_without_aux"] = int((~mask).sum())

    if isinstance(merged.index, pd.DatetimeIndex):
        idx_ts = pd.to_datetime(
            merged.index, utc=True,
        ).to_series(index=merged.index)
        lag_candidates = []
        for candidate in [
            aux_time_col,
            "metrics_source_timestamp",
            "funding_source_timestamp",
            "aux_timestamp",
        ]:
            if candidate and candidate in merged.columns:
                lag_candidates.append(candidate)
        lag_candidates = list(dict.fromkeys(lag_candidates))

        lag_hours: Dict[str, Dict[str, float]] = {}
        for candidate in lag_candidates:
            aux_ts = pd.to_datetime(
                merged[candidate], utc=True, errors="coerce",
            )
            lag = (
                (idx_ts - aux_ts).dt.total_seconds() / 3600.0
            )
            lag = lag.replace([np.inf, -np.inf], np.nan).dropna()
            if not lag.empty:
                lag_hours[candidate] = {
                    "max": round(float(lag.max()), 4),
                    "median": round(float(lag.median()), 4),
                    "p95": round(float(lag.quantile(0.95)), 4),
                }
        if lag_hours:
            diag["lag_hours"] = lag_hours
            if aux_time_col in lag_hours:
                diag["lag_hours_primary"] = lag_hours[aux_time_col]
    return diag


def infer_csv_schema(path: Path) -> Optional[dict]:
    name = path.name.lower()

    if "aggtrades" in name:
        return {
            "header": None,
            "names": HEADERLESS_TRADE_COLUMNS,
        }

    if "fundingrate" in name:
        header_candidates = (
            FUNDING_TIME_CANDIDATES
            + FUNDING_RATE_CANDIDATES
            + METRICS_SYMBOL_CANDIDATES
            + ["mark_price", "markPrice"]
        )
        if file_looks_headered(path, header_candidates):
            return None
        ncols = count_csv_fields(path)
        if ncols == 3:
            return {
                "header": None,
                "names": HEADERLESS_FUNDING_COLUMNS_3,
            }
        if ncols == 4:
            return {
                "header": None,
                "names": HEADERLESS_FUNDING_COLUMNS_4,
            }
        return None

    metric_tokens = (
        "metrics", "openinterest", "toptrader", "longshortratio",
    )
    if any(tok in name for tok in metric_tokens):
        header_candidates = (
            METRICS_TIME_CANDIDATES
            + METRICS_SYMBOL_CANDIDATES
            + METRICS_OI_CANDIDATES
            + METRICS_LS_RATIO_CANDIDATES
            + METRICS_LA_RATIO_CANDIDATES
            + METRICS_SA_RATIO_CANDIDATES
            + ["sum_open_interest_value", "sumOpenInterestValue"]
        )
        if file_looks_headered(path, header_candidates):
            return None
        return _ncols_to_headerless_metrics_schema(
            count_csv_fields(path)
        )

    if "liquidationsnapshot" in name:
        header_candidates = (
            LIQUIDATION_TIME_CANDIDATES
            + LIQUIDATION_SIDE_CANDIDATES
            + LIQUIDATION_QTY_CANDIDATES
            + LIQUIDATION_PRICE_CANDIDATES
            + METRICS_SYMBOL_CANDIDATES
            + ["event_time", "trade_time", "average_price"]
        )
        if file_looks_headered(path, header_candidates):
            return None
        ncols = count_csv_fields(path)
        if ncols == 11:
            return {"header": None, "names": HEADERLESS_LIQUIDATION_COLUMNS_11}
        if ncols == 12:
            return {"header": None, "names": HEADERLESS_LIQUIDATION_COLUMNS_12}
        return None

    return None


def build_read_csv_kwargs(
    path: Path,
    engine: str = "c",
    chunksize: Optional[int] = None,
    usecols=None,
    nrows: Optional[int] = None,
) -> dict:
    safe_engine = engine
    if engine == "pyarrow" and chunksize is not None:
        safe_engine = "c"
    kwargs: dict = {"engine": safe_engine, "low_memory": False}
    if usecols is not None:
        kwargs["usecols"] = usecols
    if chunksize is not None:
        kwargs["chunksize"] = chunksize
    if nrows is not None:
        kwargs["nrows"] = nrows

    schema = infer_csv_schema(path)
    if schema is not None:
        kwargs.update(schema)
        names = schema.get("names", [])
        if names == HEADERLESS_TRADE_COLUMNS:
            kwargs["dtype"] = {c: "string" for c in names}
        elif names in (
            HEADERLESS_METRICS_COLUMNS_8,
            HEADERLESS_METRICS_COLUMNS_9,
            HEADERLESS_FUNDING_COLUMNS_3,
            HEADERLESS_FUNDING_COLUMNS_4,
            HEADERLESS_LIQUIDATION_COLUMNS_11,
            HEADERLESS_LIQUIDATION_COLUMNS_12,
        ):
            kwargs["dtype"] = {c: "string" for c in names}

    if _supports_dtype_backend():
        kwargs["dtype_backend"] = "numpy_nullable"
    return kwargs


def read_csv_smart(
    path: Path,
    usecols=None,
    chunksize: Optional[int] = None,
    engine: str = "c",
):
    kwargs = build_read_csv_kwargs(
        path=path, engine=engine,
        chunksize=chunksize, usecols=usecols,
    )
    try:
        return pd.read_csv(path, **kwargs)
    except TypeError:
        kwargs.pop("dtype_backend", None)
        return pd.read_csv(path, **kwargs)


def read_csv_sample(
    path: Path, nrows: int = 64, engine: str = "c",
) -> pd.DataFrame:
    kwargs = build_read_csv_kwargs(
        path=path, engine=engine, nrows=nrows,
    )
    try:
        return pd.read_csv(path, **kwargs)
    except TypeError:
        kwargs.pop("dtype_backend", None)
        return pd.read_csv(path, **kwargs)


def ensure_utc_timestamp(ts_like) -> pd.Timestamp:
    ts = pd.Timestamp(ts_like)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def combine_auxiliary_frames(
    metrics_df: pd.DataFrame,
    funding_df: pd.DataFrame,
    oi_api_df: Optional[pd.DataFrame],
    liquidation_df: Optional[pd.DataFrame],
    downcast_float32: bool,
) -> pd.DataFrame:
    frames = []
    for frame in (metrics_df, funding_df, oi_api_df, liquidation_df):
        if frame is not None and not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    if len(frames) == 1:
        return maybe_downcast(frames[0].sort_index(), downcast_float32)
    aux_df = pd.concat(frames, axis=1, join="outer").sort_index()
    aux_df = aux_df.loc[:, ~aux_df.columns.duplicated(keep="last")]
    return maybe_downcast(aux_df, downcast_float32)


def validate_post_merge_types(df: pd.DataFrame) -> pd.DataFrame:
    numeric_candidates = [
        "open", "high", "low", "close", "volume",
        "signed_vol", "buy_vol", "sell_vol",
        "notional", "trade_count", "vwap",
        "buy_sell_imbalance", "buy_sell_ratio",
        "hl_range", "oc_return", "cvd",
        "sum_open_interest", "count_long_short_ratio",
        "count_long_account", "count_short_account",
        "oi_change_abs", "oi_change_pct",
        "ls_ratio_change", "ls_ratio_pct_change",
        "funding_rate", "funding_rate_change",
        "funding_rate_abs", "funding_spike_flag",
        "avg_trade_size", "trade_size_std",
        "avg_trade_notional", "trade_notional_std",
        "large_trade_count", "large_trade_count_ratio",
        "large_trade_notional_ratio",
        "whale_trade_count_ratio",
        "whale_trade_notional_ratio",
        "trade_size_hhi", "trade_size_entropy",
        "trade_size_whale_pressure",
        "vol_parkinson", "vol_garman_klass",
        "vpin_proxy_50", "vpin_proxy_100",
        "kyle_lambda_24", "kyle_lambda_72",
        "oi_api_5m", "oi_api_5m_value",
        "oi_api_5m_change_abs", "oi_api_5m_change_pct",
        "liquidations_count", "long_liq_count", "short_liq_count",
        "total_liq_qty", "long_liq_qty", "short_liq_qty",
        "total_liq_notional", "long_liq_notional", "short_liq_notional",
        "liq_long_share", "liq_short_share",
        "cvd_delta_1", "cvd_delta_1d", "cvd_ema_fast", "cvd_ema_slow",
        "cvd_z_1d", "cvd_divergence_1d",
        "data_quality_flag", "data_gap_suspect",
        "oi_stale_flag", "funding_stale_flag",
        "target_triple_barrier",
    ] + [
        f"trade_bucket_{b}_{s}"
        for b in TRADE_NOTIONAL_BUCKET_NAMES
        for s in ("count", "notional")
    ]
    for col in numeric_candidates:
        if col in df.columns and not pd.api.types.is_numeric_dtype(
            df[col]
        ):
            df[col] = safe_to_numeric(df[col], errors="coerce")
    return df


def assign_feature(
    df: pd.DataFrame, name: str, values,
) -> None:
    df[name] = values


def validate_required_ohlc(
    df: pd.DataFrame, cols: List[str],
) -> bool:
    return all(c in df.columns for c in cols)


# ============================================================
# TRADE BUCKET FEATURES
# ============================================================

def compute_trade_bucket_features(
    df: pd.DataFrame,
) -> pd.DataFrame:
    count_cols = [
        f"trade_bucket_{b}_count"
        for b in TRADE_NOTIONAL_BUCKET_NAMES
        if f"trade_bucket_{b}_count" in df.columns
    ]
    notional_cols = [
        f"trade_bucket_{b}_notional"
        for b in TRADE_NOTIONAL_BUCKET_NAMES
        if f"trade_bucket_{b}_notional" in df.columns
    ]

    if {"trade_count", "volume"}.issubset(df.columns):
        df["avg_trade_size"] = safe_divide(
            df["volume"], df["trade_count"], zero_policy="nan"
        )

    if {
        "trade_count", "qty_sq", "avg_trade_size",
    }.issubset(df.columns):
        denom = df["trade_count"].replace(0, np.nan)
        qty_var = (
            safe_divide(df["qty_sq"], df["trade_count"], zero_policy="nan")
            - (df["avg_trade_size"] ** 2)
        )
        df["trade_size_std"] = np.sqrt(qty_var.clip(lower=0))

    if {"trade_count", "notional"}.issubset(df.columns):
        df["avg_trade_notional"] = safe_divide(
            df["notional"], df["trade_count"], zero_policy="nan"
        )

    if {
        "trade_count", "notional_sq", "avg_trade_notional",
    }.issubset(df.columns):
        denom = df["trade_count"].replace(0, np.nan)
        notional_var = (
            safe_divide(df["notional_sq"], df["trade_count"], zero_policy="nan")
            - (df["avg_trade_notional"] ** 2)
        )
        df["trade_notional_std"] = np.sqrt(
            notional_var.clip(lower=0)
        )

    if count_cols:
        count_block = (
            df[count_cols]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )
        count_total = count_block.sum(axis=1).replace(0, np.nan)
        shares = count_block.div(count_total, axis=0)
        df["trade_size_hhi"] = shares.pow(2).sum(axis=1)
        log_shares = np.log(shares.where(shares > 0))
        entropy = -(shares.where(shares > 0) * log_shares).sum(
            axis=1
        )
        df["trade_size_entropy"] = entropy

        large_cc = (
            "trade_bucket_large_count"
            if "trade_bucket_large_count" in count_cols
            else None
        )
        whale_cc = (
            "trade_bucket_whale_count"
            if "trade_bucket_whale_count" in count_cols
            else None
        )
        big_cols = [c for c in [large_cc, whale_cc] if c]
        if big_cols:
            df["large_trade_count"] = (
                count_block[big_cols].sum(axis=1)
            )
            df["large_trade_count_ratio"] = (
                df["large_trade_count"] / count_total
            )
        if large_cc:
            df["large_only_trade_count"] = (
                count_block[large_cc]
            )
            df["large_only_trade_count_ratio"] = (
                count_block[large_cc] / count_total
            )
        if whale_cc:
            df["whale_trade_count_ratio"] = (
                count_block[whale_cc] / count_total
            )

    if notional_cols:
        notion_block = (
            df[notional_cols]
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )
        notion_total = notion_block.sum(axis=1).replace(
            0, np.nan,
        )
        large_nc = (
            "trade_bucket_large_notional"
            if "trade_bucket_large_notional" in notional_cols
            else None
        )
        whale_nc = (
            "trade_bucket_whale_notional"
            if "trade_bucket_whale_notional" in notional_cols
            else None
        )
        big_n_cols = [c for c in [large_nc, whale_nc] if c]
        if big_n_cols:
            df["large_trade_notional_ratio"] = (
                notion_block[big_n_cols].sum(axis=1)
                / notion_total
            )
        if large_nc:
            df["large_only_trade_notional_ratio"] = (
                notion_block[large_nc] / notion_total
            )
        if whale_nc:
            df["whale_trade_notional_ratio"] = (
                notion_block[whale_nc] / notion_total
            )
        if (
            "large_only_trade_notional_ratio" in df.columns
            and "whale_trade_notional_ratio" in df.columns
        ):
            df["trade_size_whale_pressure"] = (
                df["whale_trade_notional_ratio"]
                - df["large_only_trade_notional_ratio"]
            )
        elif (
            "large_trade_notional_ratio" in df.columns
            and "whale_trade_notional_ratio" in df.columns
        ):
            df["trade_size_whale_pressure"] = (
                df["whale_trade_notional_ratio"]
                - df["large_trade_notional_ratio"]
            )

    return df


# ============================================================
# VOLATILITY FEATURES
# ============================================================

def add_range_volatility_features(
    df: pd.DataFrame, w1d: int, w3d: int,
) -> pd.DataFrame:
    if not {"open", "high", "low", "close"}.issubset(df.columns):
        return df

    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    open_ = pd.to_numeric(df["open"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")

    valid_hl = (high > 0) & (low > 0)
    log_hl = pd.Series(np.nan, index=df.index, dtype="float64")
    log_hl.loc[valid_hl] = np.log(
        high.loc[valid_hl] / low.loc[valid_hl]
    )
    parkinson_var = (log_hl ** 2) / (4.0 * np.log(2.0))
    df["vol_parkinson"] = np.sqrt(parkinson_var.clip(lower=0))

    valid_oc = (open_ > 0) & (close > 0)
    log_oc = pd.Series(np.nan, index=df.index, dtype="float64")
    log_oc.loc[valid_oc] = np.log(
        close.loc[valid_oc] / open_.loc[valid_oc]
    )
    gk_var = (
        0.5 * (log_hl ** 2)
        - ((2.0 * np.log(2.0)) - 1.0) * (log_oc ** 2)
    )
    df["vol_garman_klass"] = np.sqrt(gk_var.clip(lower=0))

    for base_col in ("vol_parkinson", "vol_garman_klass"):
        if base_col in df.columns:
            s = df[base_col]
            df[f"{base_col}_{w1d}p"] = s.rolling(
                w1d, min_periods=w1d,
            ).mean()
            df[f"{base_col}_{w3d}p"] = s.rolling(
                w3d, min_periods=w3d,
            ).mean()
            df[f"{base_col}_z_{w1d}p"] = rolling_zscore(s, w1d)

    return df


def compute_block_rolling_stats(
    df: pd.DataFrame,
    columns: List[str],
    w1d: int,
    w3d: int,
    batch_size: int = 4,
) -> None:
    cols = [c for c in columns if c in df.columns]
    if not cols:
        return
    bs = max(1, batch_size)
    for i in range(0, len(cols), bs):
        batch = cols[i : i + bs]
        block = df[batch].apply(pd.to_numeric, errors="coerce")
        m1 = block.rolling(w1d, min_periods=w1d).mean()
        s1 = block.rolling(w1d, min_periods=w1d).std()
        m3 = block.rolling(w3d, min_periods=w3d).mean()
        s3 = block.rolling(w3d, min_periods=w3d).std()

        z1 = pd.DataFrame(index=block.index, columns=batch, dtype="float64")
        z3 = pd.DataFrame(index=block.index, columns=batch, dtype="float64")
        for c in batch:
            z1[c] = rolling_zscore(block[c], w1d, min_periods=w1d)
            z3[c] = rolling_zscore(block[c], w3d, min_periods=w3d)

        for c in batch:
            assign_feature(df, f"{c}_z_{w1d}p", z1[c])
            assign_feature(df, f"{c}_z_{w3d}p", z3[c])
            assign_feature(df, f"{c}_ma_{w1d}p", m1[c])
            assign_feature(df, f"{c}_std_{w1d}p", s1[c])
        del block, m1, s1, m3, s3, z1, z3


# ============================================================
# TIME GAPS / WINDOW HELPERS
# ============================================================

def compute_time_gaps(
    index: pd.DatetimeIndex, freq: str,
) -> Dict[str, object]:
    if len(index) < 2:
        return {
            "expected_freq": freq,
            "missing_bars": 0,
            "coverage_ratio": 1.0,
        }
    full_range = pd.date_range(
        index.min(), index.max(), freq=freq, tz=index.tz,
    )
    missing = full_range.difference(index)
    cov = (
        float(len(index) / len(full_range))
        if len(full_range) else np.nan
    )
    return {
        "expected_freq": freq,
        "missing_bars": int(len(missing)),
        "coverage_ratio": cov,
    }


def infer_feature_windows(freq: str) -> Dict[str, int]:
    bars_per_day = {
        "1min": 1440, "5min": 288, "15min": 96,
        "1h": 24, "4h": 6, "1d": 1,
    }[freq]
    return {
        "1d": bars_per_day,
        "3d": bars_per_day * 3,
        "7d": bars_per_day * 7,
    }


# ============================================================
# DATE HELPERS
# ============================================================

def month_start(ts: pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    return pd.Timestamp(
        year=ts.year, month=ts.month, day=1, tz=ts.tz,
    )


def month_end(ts: pd.Timestamp) -> pd.Timestamp:
    return month_start(ts) + pd.offsets.MonthEnd(1)


def iter_month_starts(
    start_day: pd.Timestamp, end_day: pd.Timestamp,
) -> Iterable[pd.Timestamp]:
    cur = month_start(start_day)
    last = month_start(end_day)
    while cur <= last:
        yield cur
        cur = cur + pd.offsets.MonthBegin(1)


def iter_days(
    start_day: pd.Timestamp, end_day: pd.Timestamp,
) -> Iterable[pd.Timestamp]:
    cur = start_day
    while cur <= end_day:
        yield cur
        cur += pd.Timedelta(days=1)


def build_download_plan(
    start_day: pd.Timestamp, end_day: pd.Timestamp,
) -> Tuple[List[pd.Timestamp], List[pd.Timestamp]]:
    monthly = list(iter_month_starts(start_day, end_day))
    daily: List[pd.Timestamp] = []
    today = pd.Timestamp.now(tz="UTC").normalize()
    current_month = month_start(today)
    if (
        start_day <= today <= end_day
        and current_month in monthly
    ):
        monthly.remove(current_month)
        d0 = max(start_day, current_month)
        d1 = min(end_day, month_end(current_month))
        daily.extend(list(iter_days(d0, d1)))
    return monthly, daily


def make_url(
    base_url: str,
    data_type: str,
    symbol: str,
    period_kind: str,
    ts: pd.Timestamp,
) -> str:
    symbol = symbol.upper()
    if period_kind == "daily":
        stamp = ts.strftime("%Y-%m-%d")
        return (
            f"{base_url}/daily/{data_type}/{symbol}/"
            f"{symbol}-{data_type}-{stamp}.zip"
        )
    stamp = ts.strftime("%Y-%m")
    return (
        f"{base_url}/monthly/{data_type}/{symbol}/"
        f"{symbol}-{data_type}-{stamp}.zip"
    )


def detect_supported_types(
    market: str,
) -> Tuple[str, Optional[str], Optional[str]]:
    if market == "spot":
        return "aggTrades", None, None
    return "aggTrades", "metrics", "fundingRate"


def build_rest_base_url(market: str) -> str:
    if market == "futures_um":
        return "https://fapi.binance.com"
    if market == "futures_cm":
        return "https://dapi.binance.com"
    raise ValueError("OI 5m API uniquement pour futures_um/futures_cm")


def fetch_symbol_exchange_info(
    symbol: str,
    market: str,
) -> Dict[str, object]:
    if market not in {"futures_um", "futures_cm"}:
        return {}
    session = get_thread_session()
    endpoint = f"{build_rest_base_url(market)}/{'fapi' if market == 'futures_um' else 'dapi'}/v1/exchangeInfo"
    r = api_get(session, endpoint, timeout=REQUEST_TIMEOUT, market=market)
    r.raise_for_status()
    data = r.json()
    symbol = symbol.upper()
    for row in data.get("symbols", []):
        if str(row.get("symbol", "")).upper() == symbol:
            return canonicalize_columns(pd.DataFrame([row])).iloc[0].to_dict()
    return {}


def fetch_funding_rate_info(
    symbol: str,
    market: str,
) -> Dict[str, object]:
    if market != "futures_um":
        return {}
    session = get_thread_session()
    endpoint = f"{build_rest_base_url(market)}/fapi/v1/fundingInfo"
    try:
        r = api_get(session, endpoint, params={"symbol": symbol.upper()}, timeout=REQUEST_TIMEOUT, market=market)
        if r.status_code == 404:
            return {}
        r.raise_for_status()
        data = r.json()
        rows = data if isinstance(data, list) else [data]
        for row in rows:
            if str(row.get("symbol", "")).upper() == symbol.upper():
                out = {normalize_colname(k): v for k, v in row.items()}
                try:
                    if "funding_interval_hours" in out and out["funding_interval_hours"] is not None:
                        out["funding_interval_hours"] = int(out["funding_interval_hours"])
                except Exception:
                    pass
                return out
    except Exception as e:
        log(f"FundingInfo indisponible pour {symbol}: {e}", level="WARNING")
    return {}


def precheck_symbol_or_raise(symbol: str, market: str) -> Dict[str, object]:
    info = fetch_symbol_exchange_info(symbol, market)
    if not info:
        raise ValueError(
            f"Symbole {symbol.upper()} introuvable sur {market} via exchangeInfo"
        )
    status = str(info.get("status", "")).upper()
    contract_status = str(info.get("contract_status", "")).upper()
    if status not in {"TRADING", "SETTLING"} and contract_status not in {"TRADING", "PERPETUAL", "SETTLING"}:
        raise ValueError(
            f"Symbole {symbol.upper()} non tradable sur {market}: status={status or 'NA'} contract_status={contract_status or 'NA'}"
        )
    return info


def fetch_open_interest_hist(
    symbol: str,
    market: str,
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    period: str = "5m",
    limit: int = 500,
) -> pd.DataFrame:
    if market not in {"futures_um", "futures_cm"}:
        return pd.DataFrame()
    session = get_thread_session()
    base = build_rest_base_url(market)
    endpoint = (
        f"{base}/futures/data/openInterestHist"
    )
    rows = []
    cur = ensure_utc_timestamp(start_dt)
    end_dt = ensure_utc_timestamp(end_dt)
    one_ms = pd.Timedelta(milliseconds=1)
    while cur <= end_dt:
        params = {
            "symbol": symbol.upper(),
            "period": period,
            "limit": int(limit),
            "startTime": int(cur.timestamp() * 1000),
            "endTime": int(end_dt.timestamp() * 1000),
        }
        r = api_get(session, endpoint, params=params, timeout=REQUEST_TIMEOUT, market=market)
        if r.status_code == 404:
            break
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        rows.extend(data)
        last_ts = pd.to_datetime(
            max(int(x.get("timestamp", 0)) for x in data),
            unit="ms", utc=True,
        )
        nxt = last_ts + pd.Timedelta(milliseconds=1)
        if nxt <= cur:
            break
        cur = nxt
        if len(data) < limit:
            break
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df = canonicalize_columns(df)
    if "timestamp" not in df.columns:
        return pd.DataFrame()

    df["timestamp"] = parse_binance_datetime(df["timestamp"])
    for c in ["sum_open_interest", "sum_open_interest_value"]:
        if c in df.columns:
            df[c] = safe_to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
    df = df.drop_duplicates(subset=["timestamp"], keep="last")
    df = df[(df["timestamp"] >= start_dt) & (df["timestamp"] <= end_dt)]
    if df.empty:
        return pd.DataFrame()

    df = df.set_index("timestamp").sort_index()
    out = pd.DataFrame(index=df.index)
    if "sum_open_interest" in df.columns:
        out["oi_api_5m"] = df["sum_open_interest"]
        out["oi_api_5m_change_abs"] = out["oi_api_5m"].diff()
        out["oi_api_5m_change_pct"] = out["oi_api_5m"].pct_change(fill_method=None)
    if "sum_open_interest_value" in df.columns:
        out["oi_api_5m_value"] = df["sum_open_interest_value"]

    out["oi_api_source_timestamp"] = out.index
    out["oi_api_source_period"] = period
    return out


def fetch_open_interest_hist_multi_period(
    symbol: str,
    market: str,
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    periods: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    periods = periods or ["5m", "15m", "30m", "1h", "4h", "1d"]
    stats: Dict[str, object] = {"attempts": []}

    for period in periods:
        try:
            df = fetch_open_interest_hist(
                symbol=symbol,
                market=market,
                start_dt=start_dt,
                end_dt=end_dt,
                period=period,
            )
            stats["attempts"].append({"period": period, "rows": int(len(df))})
            if df is not None and not df.empty:
                stats["selected_period"] = period
                stats["rows_raw"] = int(len(df))
                return df, stats
        except Exception as exc:
            stats["attempts"].append({"period": period, "rows": 0, "error": str(exc)})

    stats["selected_period"] = None
    stats["rows_raw"] = 0
    return pd.DataFrame(), stats


def build_oi_from_metrics_fallback(metrics_df: pd.DataFrame) -> pd.DataFrame:
    if metrics_df is None or metrics_df.empty:
        return pd.DataFrame()

    out = pd.DataFrame(index=metrics_df.index)

    if "sum_open_interest" in metrics_df.columns:
        out["oi_api_5m"] = safe_to_numeric(metrics_df["sum_open_interest"], errors="coerce")
    elif "open_interest" in metrics_df.columns:
        out["oi_api_5m"] = safe_to_numeric(metrics_df["open_interest"], errors="coerce")

    if "sum_open_interest_value" in metrics_df.columns:
        out["oi_api_5m_value"] = safe_to_numeric(metrics_df["sum_open_interest_value"], errors="coerce")

    if out.empty:
        return pd.DataFrame()

    if "oi_api_5m" in out.columns:
        out["oi_api_5m_change_abs"] = out["oi_api_5m"].diff()
        out["oi_api_5m_change_pct"] = out["oi_api_5m"].pct_change(fill_method=None)

    out["oi_api_source_timestamp"] = out.index
    out["oi_api_source_period"] = "metrics_fallback"
    return out.sort_index()


def resample_oi_api_frame(
    oi_df: pd.DataFrame,
    freq: str,
    downcast_float32: bool,
) -> pd.DataFrame:
    if oi_df is None or oi_df.empty:
        return pd.DataFrame()
    grouped = oi_df.resample(freq).last().sort_index()
    if "oi_api_5m" in grouped.columns:
        grouped["oi_api_5m_change_abs"] = grouped["oi_api_5m"].diff()
        grouped["oi_api_5m_change_pct"] = grouped["oi_api_5m"].pct_change(fill_method=None)
    return maybe_downcast(grouped, downcast_float32)


# ============================================================
# SAFE DIVISION UTILITY (PATCH 6)
# ============================================================

def safe_divide(
    num: pd.Series,
    den: pd.Series,
    zero_policy: str = "nan",  # "nan", "zero", "ffill"
) -> pd.Series:
    """
    Division sécurisée configurable pour éviter la propagation massive de NaN
    sur assets peu liquides (volume/trade_count = 0).
    """
    num = pd.to_numeric(num, errors="coerce")
    den = pd.to_numeric(den, errors="coerce")

    out = pd.Series(np.nan, index=num.index, dtype="float64")
    valid = den.notna() & (den != 0)
    out.loc[valid] = num.loc[valid] / den.loc[valid]

    if zero_policy == "zero":
        zero_mask = den.eq(0) & den.notna()
        out.loc[zero_mask] = 0.0
    elif zero_policy == "ffill":
        zero_mask = den.eq(0) & den.notna()
        out.loc[zero_mask] = np.nan
        out = out.ffill()

    return out


# ============================================================
# HTTP / DOWNLOAD
# ============================================================

_API_RATE_STATE = {"next_ts": 0.0}
_API_RATE_LOCK = threading.Lock()
_API_RATE_DB_ENV = "BINANCE_API_RATE_LIMIT_DB"
_API_RATE_DB_OWNER_ENV = "BINANCE_API_RATE_LIMIT_DB_OWNER_PID"


def _default_rate_limit_db_path() -> str:
    return str(Path(tempfile.gettempdir()) / "binance_api_rate_limit.sqlite3")


def _cleanup_rate_limiter_files(db_path: Optional[str], force: bool = False) -> None:
    if not db_path:
        return
    base = Path(db_path)
    candidates = [base, Path(str(base) + "-wal"), Path(str(base) + "-shm")]
    for p in candidates:
        try:
            if p.exists():
                p.unlink()
        except Exception:
            if force:
                continue


def cleanup_cross_process_rate_limiter(db_path: Optional[str] = None, force: bool = False) -> None:
    db_path = str(db_path or os.environ.get(_API_RATE_DB_ENV) or "")
    if not db_path:
        return
    owner_pid = str(os.environ.get(_API_RATE_DB_OWNER_ENV, "") or "")
    if not force and owner_pid and owner_pid != str(os.getpid()):
        return
    _cleanup_rate_limiter_files(db_path, force=force)


def _sqlite_connect_rate_limiter(db_path: str) -> sqlite3.Connection:
    timeout_s = float(os.environ.get("BINANCE_API_RATE_SQLITE_TIMEOUT", "60.0") or 60.0)
    timeout_s = max(1.0, timeout_s)
    conn = sqlite3.connect(db_path, timeout=timeout_s, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout = {int(timeout_s * 1000)}")
    return conn


def initialize_cross_process_rate_limiter(db_path: Optional[str] = None) -> str:
    db_path = str(db_path or os.environ.get(_API_RATE_DB_ENV) or _default_rate_limit_db_path())
    os.environ[_API_RATE_DB_ENV] = db_path
    os.environ.setdefault(_API_RATE_DB_OWNER_ENV, str(os.getpid()))
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = _sqlite_connect_rate_limiter(db_path)
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS rate_state (id INTEGER PRIMARY KEY CHECK (id = 1), next_ts REAL NOT NULL)")
        conn.execute("INSERT OR IGNORE INTO rate_state (id, next_ts) VALUES (1, 0.0)")
        row = conn.execute("SELECT next_ts FROM rate_state WHERE id = 1").fetchone()
        now = time.monotonic()
        next_ts = float(row[0]) if row else 0.0
        if (not np.isfinite(next_ts)) or next_ts < 0 or next_ts > (now + 3600.0):
            conn.execute("UPDATE rate_state SET next_ts = 0.0 WHERE id = 1")
        conn.commit()
    finally:
        conn.close()
    return db_path


_API_RATE_REDIS_ENV = "BINANCE_API_RATE_REDIS_URL"


def _api_rate_limit_wait_redis(max_per_min: float, jitter: float = 0.0) -> bool:
    redis_url = os.environ.get(_API_RATE_REDIS_ENV)
    if not redis_url:
        return False
    max_per_min = float(max_per_min or 0.0)
    if max_per_min <= 0:
        return True
    spacing = 60.0 / max_per_min
    extra = random.random() * float(jitter or 0.0)
    attempts = 0
    client = None
    while True:
        pipe = None
        try:
            if client is None:
                import redis
                client = redis.Redis.from_url(redis_url)
            now = time.monotonic()
            pipe = client.pipeline()
            pipe.watch("binance_api_rate_limit")
            current = pipe.get("binance_api_rate_limit")
            next_ts = float(current) if current is not None else 0.0
            if (not np.isfinite(next_ts)) or next_ts < 0 or next_ts > (now + 3600.0):
                next_ts = 0.0
            scheduled = max(now, next_ts)
            wait = max(0.0, scheduled - now)
            pipe.multi()
            pipe.set("binance_api_rate_limit", scheduled + spacing + extra, ex=120)
            pipe.execute()
            break
        except Exception:
            attempts += 1
            if attempts >= 8:
                raise
            time.sleep(min(0.05 * (2 ** (attempts - 1)), 1.0) + random.random() * 0.05)
        finally:
            if pipe is not None:
                try:
                    pipe.reset()
                except Exception:
                    pass
    if wait > 0:
        time.sleep(wait)
    if extra > 0:
        time.sleep(extra)
    return True


def _api_rate_limit_wait_cross_process(max_per_min: float, jitter: float = 0.0) -> bool:
    db_path = os.environ.get(_API_RATE_DB_ENV)
    if not db_path:
        return False
    max_per_min = float(max_per_min or 0.0)
    if max_per_min <= 0:
        return True
    spacing = 60.0 / max_per_min
    extra = random.random() * float(jitter or 0.0)
    attempts = 0
    while True:
        conn = None
        wait = 0.0
        try:
            conn = _sqlite_connect_rate_limiter(db_path)
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT next_ts FROM rate_state WHERE id = 1").fetchone()
            now = time.monotonic()
            next_ts = float(row[0]) if row else 0.0
            if (not np.isfinite(next_ts)) or next_ts < 0 or next_ts > (now + 3600.0):
                next_ts = 0.0
            scheduled = max(now, next_ts)
            wait = max(0.0, scheduled - now)
            conn.execute("UPDATE rate_state SET next_ts = ? WHERE id = 1", (scheduled + spacing + extra,))
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "locked" not in msg and "busy" not in msg:
                raise
            attempts += 1
            if attempts >= 8:
                raise
            time.sleep(min(0.05 * (2 ** (attempts - 1)), 1.0) + random.random() * 0.05)
        finally:
            if conn is not None:
                conn.close()
    if wait > 0:
        time.sleep(wait)
    if extra > 0:
        time.sleep(extra)
    return True


atexit.register(cleanup_cross_process_rate_limiter)


def api_rate_limit_wait(max_per_min: float, jitter: float = 0.0) -> None:
    backend = str(getattr(_SESSION_LOCAL, "api_rate_backend", "sqlite") or "sqlite").lower()
    if backend == "noop":
        return
    if backend == "sqlite" and _api_rate_limit_wait_cross_process(max_per_min=max_per_min, jitter=jitter):
        return
    if backend == "redis" and _api_rate_limit_wait_redis(max_per_min=max_per_min, jitter=jitter):
        return
    max_per_min = float(max_per_min or 0.0)
    if max_per_min <= 0:
        return
    spacing = 60.0 / max_per_min
    with _API_RATE_LOCK:
        now = time.monotonic()
        scheduled = max(now, float(_API_RATE_STATE.get("next_ts", 0.0)))
        wait = max(0.0, scheduled - now)
        extra = random.random() * float(jitter or 0.0)
        _API_RATE_STATE["next_ts"] = scheduled + spacing + extra
    if wait > 0:
        time.sleep(wait)
    if extra > 0:
        time.sleep(extra)


def api_get(
    session: requests.Session,
    url: str,
    *,
    params: Optional[Dict[str, object]] = None,
    timeout: int = REQUEST_TIMEOUT,
    market: Optional[str] = None,
    max_retries: int = 6,
    backoff_base: float = 1.6,
) -> requests.Response:
    try:
        max_per_min = float(getattr(_SESSION_LOCAL, "api_max_per_min", 60.0))
    except Exception:
        max_per_min = 60.0
    try:
        jitter = float(getattr(_SESSION_LOCAL, "api_rate_limit_jitter", 0.10))
    except Exception:
        jitter = 0.10

    last_exc = None
    for attempt in range(max_retries + 1):
        api_rate_limit_wait(max_per_min=max_per_min, jitter=jitter)
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code < 400:
                return resp
            if resp.status_code == 404:
                return resp
            if resp.status_code not in {418, 429, 500, 502, 503, 504}:
                resp.raise_for_status()
            wait_s = float(backoff_base) ** attempt
            log(
                f"HTTP {resp.status_code} sur API Binance url={url} params={params} retry={attempt + 1}/{max_retries} wait={wait_s:.2f}s",
                level="WARNING",
            )
            time.sleep(wait_s)
        except requests.RequestException as exc:
            last_exc = exc
            wait_s = float(backoff_base) ** attempt
            log(
                f"Erreur réseau API Binance url={url} params={params} retry={attempt + 1}/{max_retries} wait={wait_s:.2f}s err={exc}",
                level="WARNING",
            )
            time.sleep(wait_s)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"api_get failed url={url} params={params}")


def build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry, pool_connections=32, pool_maxsize=32,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": f"QuantDatasetBuilder{PIPELINE_VERSION}/1.0",
    })
    return session


_SESSION_LOCAL = threading.local()


def get_thread_session() -> requests.Session:
    session = getattr(_SESSION_LOCAL, "session", None)
    if session is None:
        session = build_http_session()
        _SESSION_LOCAL.session = session
    return session


def configure_thread_api_limits(args: argparse.Namespace) -> None:
    base_limit = float(getattr(args, "api_max_per_min", 60.0))
    jitter = float(getattr(args, "api_rate_limit_jitter", 0.10))
    backend = str(getattr(args, "api_rate_backend", "sqlite") or "sqlite").lower()
    sqlite_timeout = float(getattr(args, "api_rate_sqlite_timeout", 60.0) or 60.0)
    os.environ["BINANCE_API_RATE_SQLITE_TIMEOUT"] = str(max(1.0, sqlite_timeout))
    eff_limit = base_limit
    try:
        workers = int(getattr(args, "parallel_workers", 0) or 0)
        if workers <= 0:
            workers = min(max(1, int(getattr(args, "batch_size", 1))), max(1, (os.cpu_count() or 1)))
        if bool(getattr(args, "parallel", False)) and getattr(args, "parallel_backend", "process") == "process":
            if backend == "sqlite":
                initialize_cross_process_rate_limiter(getattr(args, "api_rate_limit_db", None))
                eff_limit = base_limit
            elif backend == "redis":
                os.environ[_API_RATE_REDIS_ENV] = str(getattr(args, "redis_url", os.environ.get(_API_RATE_REDIS_ENV, "")) or os.environ.get(_API_RATE_REDIS_ENV, ""))
                eff_limit = base_limit
            elif backend == "noop":
                eff_limit = base_limit
                log(
                    "Rate limiter backend=noop: aucune limitation centralisée active.",
                    level="WARNING",
                )
            else:
                eff_limit = max(1.0, base_limit / max(1, workers))
                log(
                    "Rate limiter backend=shared_memory: quota réparti entre workers sans coordination disque globale.",
                    level="WARNING",
                )
        elif bool(getattr(args, "parallel", False)) and workers > 1:
            eff_limit = max(1.0, base_limit / max(1, workers))
    except Exception:
        eff_limit = base_limit
    _SESSION_LOCAL.api_max_per_min = eff_limit
    _SESSION_LOCAL.api_rate_limit_jitter = jitter
    _SESSION_LOCAL.api_rate_backend = backend


def download_file(
    session: requests.Session,
    url: str,
    dest: Path,
    timeout: int = REQUEST_TIMEOUT,
) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with session.get(url, stream=True, timeout=timeout) as r:
            if r.status_code == 404:
                return False
            r.raise_for_status()
            expected_size = pd.to_numeric(
                r.headers.get("Content-Length"), errors="coerce",
            )
            bytes_written = 0
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(
                    chunk_size=1024 * 1024,
                ):
                    if not chunk:
                        continue
                    f.write(chunk)
                    bytes_written += len(chunk)
            if bytes_written <= 0:
                _cleanup_paths(dest, tmp)
                return False
            if (
                pd.notna(expected_size)
                and int(expected_size) > 0
                and bytes_written != int(expected_size)
            ):
                log(
                    f"Téléchargement incomplet {url} "
                    f"({bytes_written}/{int(expected_size)})",
                    level="WARNING",
                )
                _cleanup_paths(dest, tmp)
                return False
            remote_cs = fetch_remote_checksum(
                session, url, timeout=timeout,
            )
            if remote_cs:
                local_cs = sha256_file(tmp)
                if local_cs.lower() != remote_cs.lower():
                    log(
                        f"Checksum SHA256 invalide {url}",
                        level="WARNING",
                    )
                    _cleanup_paths(dest, tmp)
                    return False
            tmp.replace(dest)
            return True
    except requests.RequestException as e:
        log(
            f"Erreur téléchargement {url}: {e}",
            level="WARNING",
        )
        _cleanup_paths(dest, tmp)
        return False
    except Exception:
        _cleanup_paths(dest, tmp)
        raise


def _cleanup_paths(*paths: Path) -> None:
    for p in paths:
        try:
            if p.exists():
                p.unlink()
        except OSError:
            pass


def extract_zip(
    zip_path: Path, extract_dir: Path,
) -> List[Path]:
    extract_dir.mkdir(parents=True, exist_ok=True)
    out: List[Path] = []
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = [
                m for m in zf.namelist()
                if m.lower().endswith(".csv")
            ]
            if not members:
                log(
                    f"ZIP sans CSV: {zip_path}",
                    level="WARNING",
                )
                return []
            for member in members:
                target = extract_dir / Path(member).name
                with zf.open(member) as src, open(
                    target, "wb",
                ) as dst:
                    shutil.copyfileobj(src, dst)
                out.append(target)
    except zipfile.BadZipFile:
        log(f"ZIP corrompue: {zip_path}", level="WARNING")
        return []
    return out


def download_extract_one(
    url: str,
    zip_path: Path,
    extract_dir: Path,
    force_redownload: bool,
) -> Tuple[str, List[Path]]:
    session = get_thread_session()
    if zip_path.exists() and not force_redownload:
        csvs = extract_zip(zip_path, extract_dir)
        if csvs:
            return "existing", csvs
        log(
            f"ZIP corrompu, retéléchargement: {zip_path}",
            level="WARNING",
        )
        _cleanup_paths(zip_path)
    ok = download_file(session, url, zip_path)
    if not ok:
        return "missing", []
    csvs = (
        extract_zip(zip_path, extract_dir)
        if zip_path.exists() else []
    )
    if not csvs:
        return "missing", []
    return "downloaded", csvs


def fallback_daily_for_month(
    base_url: str,
    data_type: str,
    symbol: str,
    ms: pd.Timestamp,
    start_day: pd.Timestamp,
    end_day: pd.Timestamp,
    cache_dir: Path,
    force_redownload: bool,
    max_workers: int,
) -> Tuple[List[Path], Dict[str, int]]:
    csvs: List[Path] = []
    stats = {"downloaded": 0, "existing": 0, "missing": 0}
    first_day = max(ms, start_day)
    last_day = min(month_end(ms), end_day)
    days = list(iter_days(first_day, last_day))

    futures = {}
    eff = max(1, min(max_workers, 4))
    with ThreadPoolExecutor(max_workers=eff) as ex:
        for day in days:
            stamp = day.strftime("%Y-%m-%d")
            url = make_url(
                base_url, data_type, symbol, "daily", day,
            )
            zp = (
                cache_dir / data_type / "daily" / symbol
                / f"{symbol}-{data_type}-{stamp}.zip"
            )
            ed = (
                cache_dir / "extracted" / data_type
                / "daily" / symbol / stamp
            )
            fut = ex.submit(
                download_extract_one,
                url, zp, ed, force_redownload,
            )
            futures[fut] = stamp
        for fut in tqdm(
            as_completed(futures),
            total=len(futures),
            desc=f"Fallback daily {data_type}",
        ):
            stamp = futures[fut]
            try:
                status, paths = fut.result()
                stats[status] += 1
                csvs.extend(paths)
            except Exception as e:
                log(
                    f"Erreur fallback daily {data_type} "
                    f"{symbol} {stamp}: {e}",
                    level="WARNING",
                )
                stats["missing"] += 1
    return sorted(csvs), stats


def fetch_periods_for_type(
    base_url: str,
    data_type: str,
    symbol: str,
    start_day: pd.Timestamp,
    end_day: pd.Timestamp,
    cache_dir: Path,
    force_redownload: bool,
    max_workers: int,
) -> Tuple[List[Path], Dict[str, object]]:
    monthly_parts, daily_parts = build_download_plan(
        start_day, end_day,
    )
    csvs: List[Path] = []
    stats: Dict[str, object] = {
        "monthly_requested": len(monthly_parts),
        "monthly_downloaded": 0,
        "monthly_existing": 0,
        "monthly_missing": 0,
        "daily_requested": len(daily_parts),
        "daily_downloaded": 0,
        "daily_existing": 0,
        "daily_missing": 0,
    }

    # ── Monthly ─────────────────────────────────────────────
    futures = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for ms in monthly_parts:
            stamp = ms.strftime("%Y-%m")
            url = make_url(
                base_url, data_type, symbol, "monthly", ms,
            )
            zp = (
                cache_dir / data_type / "monthly" / symbol
                / f"{symbol}-{data_type}-{stamp}.zip"
            )
            ed = (
                cache_dir / "extracted" / data_type
                / "monthly" / symbol / stamp
            )
            fut = ex.submit(
                download_extract_one,
                url, zp, ed, force_redownload,
            )
            futures[fut] = (ms, stamp)

        for fut in tqdm(
            as_completed(futures),
            total=len(futures),
            desc=f"Monthly {data_type}",
        ):
            ms, stamp = futures[fut]
            try:
                status, paths = fut.result()
            except Exception as e:
                log(
                    f"Erreur monthly {data_type} {symbol} "
                    f"{stamp}: {e}",
                    level="WARNING",
                )
                status, paths = "missing", []
            if status == "missing":
                stats["monthly_missing"] = (
                    int(stats["monthly_missing"]) + 1
                )
                log(
                    f"Monthly manquant {data_type} {symbol} "
                    f"{stamp} → fallback daily",
                    level="WARNING",
                )
                dc, ds = fallback_daily_for_month(
                    base_url, data_type, symbol, ms,
                    start_day, end_day, cache_dir,
                    force_redownload, max_workers,
                )
                csvs.extend(dc)
                for k in ("downloaded", "existing", "missing"):
                    stats[f"daily_{k}"] = (
                        int(stats[f"daily_{k}"]) + ds[k]
                    )
            else:
                stats[f"monthly_{status}"] = (
                    int(stats[f"monthly_{status}"]) + 1
                )
                csvs.extend(paths)

    # ── Daily ───────────────────────────────────────────────
    if daily_parts:
        futures = {}
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for day in daily_parts:
                stamp = day.strftime("%Y-%m-%d")
                url = make_url(
                    base_url, data_type, symbol, "daily", day,
                )
                zp = (
                    cache_dir / data_type / "daily" / symbol
                    / f"{symbol}-{data_type}-{stamp}.zip"
                )
                ed = (
                    cache_dir / "extracted" / data_type
                    / "daily" / symbol / stamp
                )
                fut = ex.submit(
                    download_extract_one,
                    url, zp, ed, force_redownload,
                )
                futures[fut] = stamp
            for fut in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f"Daily {data_type}",
            ):
                stamp = futures[fut]
                try:
                    status, paths = fut.result()
                    stats[f"daily_{status}"] = (
                        int(stats[f"daily_{status}"]) + 1
                    )
                    csvs.extend(paths)
                except Exception as e:
                    log(
                        f"Erreur daily {data_type} {symbol} "
                        f"{stamp}: {e}",
                        level="WARNING",
                    )
                    stats["daily_missing"] = (
                        int(stats["daily_missing"]) + 1
                    )

    return sorted(csvs), stats


# ============================================================
# CSV → PARTIAL PARQUET
# ============================================================

def inspect_trade_columns(
    csv_file: Path,
) -> Dict[str, str]:
    sample = read_csv_sample(csv_file, nrows=50)
    cols = list(sample.columns)
    time_col = pick_first_existing(
        cols, TRADES_TIME_CANDIDATES,
    )
    qty_col = pick_first_existing(
        cols, TRADES_QTY_CANDIDATES,
    )
    price_col = pick_first_existing(
        cols, TRADES_PRICE_CANDIDATES,
    )
    side_col = pick_first_existing(
        cols, TRADES_SIDEFLAG_CANDIDATES,
    )
    if any(c is None for c in (
        time_col, qty_col, price_col, side_col,
    )):
        raise ValueError(
            f"Colonnes trades introuvables dans "
            f"{csv_file.name}: {cols}"
        )
    return {
        "time_col": time_col,
        "qty_col": qty_col,
        "price_col": price_col,
        "side_col": side_col,
    }


def inspect_metrics_columns(
    csv_file: Path,
) -> Dict[str, Optional[str]]:
    raw_sample = read_csv_sample(csv_file, nrows=50)
    raw_cols = list(raw_sample.columns)

    sample = canonicalize_columns(raw_sample.copy())
    cols = list(sample.columns)
    schema_info = validate_schema_contract(
        cols, "metrics", csv_file.name,
    )
    time_col = pick_first_existing(
        cols, METRICS_TIME_CANDIDATES,
    )
    oi_col = pick_first_existing(
        cols, METRICS_OI_CANDIDATES,
    )
    ls_col = pick_first_existing(
        cols, METRICS_LS_RATIO_CANDIDATES,
    )
    la_col = pick_first_existing(
        cols, METRICS_LA_RATIO_CANDIDATES,
    )
    sa_col = pick_first_existing(
        cols, METRICS_SA_RATIO_CANDIDATES,
    )
    symbol_col = pick_first_existing(
        cols, METRICS_SYMBOL_CANDIDATES,
    )

    raw_to_canon = {
        raw: normalize_colname(raw) for raw in raw_cols
    }
    canon_to_raw = {v: k for k, v in raw_to_canon.items()}

    return {
        "time_col": time_col,
        "oi_col": oi_col,
        "ls_col": ls_col,
        "la_col": la_col,
        "sa_col": sa_col,
        "symbol_col": symbol_col,
        "raw_time_col": canon_to_raw.get(time_col),
        "raw_oi_col": canon_to_raw.get(oi_col) if oi_col else None,
        "raw_ls_col": canon_to_raw.get(ls_col) if ls_col else None,
        "raw_la_col": canon_to_raw.get(la_col) if la_col else None,
        "raw_sa_col": canon_to_raw.get(sa_col) if sa_col else None,
        "raw_symbol_col": canon_to_raw.get(symbol_col) if symbol_col else None,
        "columns": cols,
        "raw_columns": raw_cols,
        "schema_info": schema_info,
    }


def inspect_funding_columns(
    csv_file: Path,
) -> Dict[str, Optional[str]]:
    raw_sample = read_csv_sample(csv_file, nrows=50)
    raw_cols = list(raw_sample.columns)

    sample = canonicalize_columns(raw_sample.copy())
    cols = list(sample.columns)
    schema_info = validate_schema_contract(
        cols, "funding", csv_file.name,
    )
    time_col = pick_first_existing(
        cols, FUNDING_TIME_CANDIDATES,
    )
    rate_col = pick_first_existing(
        cols, FUNDING_RATE_CANDIDATES,
    )
    if time_col is None or rate_col is None:
        raise ValueError(
            f"Colonnes funding introuvables dans "
            f"{csv_file.name}: {cols}"
        )

    raw_to_canon = {
        raw: normalize_colname(raw) for raw in raw_cols
    }
    canon_to_raw = {v: k for k, v in raw_to_canon.items()}

    return {
        "time_col": time_col,
        "rate_col": rate_col,
        "raw_time_col": canon_to_raw.get(time_col),
        "raw_rate_col": canon_to_raw.get(rate_col),
        "columns": cols,
        "raw_columns": raw_cols,
        "schema_info": schema_info,
    }


def partial_name(prefix: str, csv_path: Path) -> str:
    h = hashlib.sha256(
        str(csv_path.resolve()).encode("utf-8")
    ).hexdigest()[:16]
    return f"{prefix}_{csv_path.stem}_{h}.parquet"


def _build_trade_agg_dict(
    price_col: str, qty_col: str,
) -> dict:
    """Dictionnaire d'agrégation pour le resample trades."""
    agg = {
        "open": (price_col, "first"),
        "open_ts": ("_trade_ts", "first"),
        "high": (price_col, "max"),
        "low": (price_col, "min"),
        "close": (price_col, "last"),
        "close_ts": ("_trade_ts", "last"),
        "volume": (qty_col, "sum"),
        "signed_vol": ("signed_vol", "sum"),
        "buy_vol": ("buy_vol", "sum"),
        "sell_vol": ("sell_vol", "sum"),
        "notional": ("notional", "sum"),
        "qty_sq": ("qty_sq", "sum"),
        "notional_sq": ("notional_sq", "sum"),
        "trade_count": ("trade_count", "sum"),
    }
    for bucket in TRADE_NOTIONAL_BUCKET_NAMES:
        cn = f"trade_bucket_{bucket}_count"
        nn = f"trade_bucket_{bucket}_notional"
        agg[cn] = (cn, "sum")
        agg[nn] = (nn, "sum")
    return agg


def _build_trade_combine_agg_map() -> dict:
    """Mapping pour combiner des partiels trade."""
    agg = {
        "open": "first",
        "open_ts": "min",
        "high": "max",
        "low": "min",
        "close": "last",
        "close_ts": "max",
        "volume": "sum",
        "signed_vol": "sum",
        "buy_vol": "sum",
        "sell_vol": "sum",
        "notional": "sum",
        "qty_sq": "sum",
        "notional_sq": "sum",
        "trade_count": "sum",
    }
    for bucket in TRADE_NOTIONAL_BUCKET_NAMES:
        agg[f"trade_bucket_{bucket}_count"] = "sum"
        agg[f"trade_bucket_{bucket}_notional"] = "sum"
    return agg


def write_trade_partial(
    csv_path: Path,
    partial_dir: Path,
    freq: str,
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    chunksize: int,
    engine: str,
    force_reprocess: bool,
    downcast_float32: bool,
) -> Optional[Path]:
    out_path = partial_dir / partial_name("trade", csv_path)
    if out_path.exists() and not force_reprocess:
        return out_path

    meta = inspect_trade_columns(csv_path)
    tc = meta["time_col"]
    qc = meta["qty_col"]
    pc = meta["price_col"]
    sc = meta["side_col"]
    usecols = [tc, qc, pc, sc]

    # PATCH 3 : inférence multi-chunk des buckets (anti-biais premier chunk)
    bucket_info = infer_trade_bucket_edges_from_sample(
        csv_path, chunksize=chunksize
    )
    bucket_edges = [
        0.0,
        bucket_info["small"],
        bucket_info["medium"],
        bucket_info["large"],
        bucket_info["whale"],
        np.inf,
    ]
    bucket_bins = np.asarray(bucket_edges[1:-1], dtype="float64")
    log(
        f"Trade buckets inférés depuis échantillon : {bucket_info.get('source')} "
        f"({bucket_info.get('sample_size', 0)} trades)",
        level="DEBUG",
    )

    chunks_agg = []
    reader = read_csv_smart(
        csv_path, usecols=usecols,
        chunksize=chunksize, engine=engine,
    )
    agg_dict = _build_trade_agg_dict(pc, qc)

    for chunk in reader:
        chunk[tc] = parse_binance_datetime(chunk[tc])
        chunk[qc] = pd.to_numeric(chunk[qc], errors="coerce")
        chunk[pc] = pd.to_numeric(chunk[pc], errors="coerce")
        chunk[sc] = normalize_bool_series(chunk[sc])
        chunk = chunk.dropna(subset=[tc, qc, pc, sc])
        if chunk.empty:
            continue
        chunk = chunk[chunk[sc].isin([0, 1])].copy()
        if chunk.empty:
            continue
        chunk[sc] = chunk[sc].astype(np.int8).astype(bool)
        chunk = chunk[
            (chunk[tc] >= start_dt) & (chunk[tc] <= end_dt)
        ]
        if chunk.empty:
            continue
        chunk = chunk.sort_values(tc)

        # Colonnes dérivées
        chunk["signed_vol"] = np.where(
            chunk[sc], -chunk[qc], chunk[qc],
        )
        chunk["buy_vol"] = np.where(
            chunk[sc], 0.0, chunk[qc],
        )
        chunk["sell_vol"] = np.where(
            chunk[sc], chunk[qc], 0.0,
        )
        chunk["notional"] = chunk[pc] * chunk[qc]
        chunk["qty_sq"] = chunk[qc] ** 2
        chunk["notional_sq"] = chunk["notional"] ** 2
        chunk["trade_count"] = 1

        notional_vals = np.clip(
            chunk["notional"].fillna(0.0).to_numpy(
                dtype="float64", copy=False,
            ),
            a_min=0.0, a_max=None,
        )
        bucket_ids = np.digitize(
            notional_vals, bucket_bins, right=False,
        )
        for i, bucket in enumerate(TRADE_NOTIONAL_BUCKET_NAMES):
            mask = bucket_ids == i
            cn = f"trade_bucket_{bucket}_count"
            nn = f"trade_bucket_{bucket}_notional"
            chunk[cn] = mask.astype(np.int8)
            chunk[nn] = np.where(
                mask, chunk["notional"], 0.0,
            )

        chunk["_trade_ts"] = chunk[tc]
        agg = (
            chunk.set_index(tc)
            .resample(freq, label="right", closed="right")  # PATCH 5 : end-of-bar
            .agg(**agg_dict)
        )
        chunks_agg.append(agg)

    if not chunks_agg:
        return None

    combine_map = _build_trade_combine_agg_map()
    df = (
        pd.concat(chunks_agg)
        .groupby(level=0)
        .agg(combine_map)
        .sort_index()
    )
    df["vwap"] = safe_divide(df["notional"], df["volume"], "nan")
    bvol_tot = df["buy_vol"] + df["sell_vol"]
    df["buy_sell_imbalance"] = safe_divide(
        df["buy_vol"] - df["sell_vol"], bvol_tot, "nan"
    )
    df["buy_sell_ratio"] = safe_divide(
        df["buy_vol"], df["sell_vol"], "nan"
    )
    df["hl_range"] = np.where(
        df["low"] > 0,
        (df["high"] - df["low"]) / df["low"],
        np.nan,
    )
    df["oc_return"] = np.where(
        df["open"] > 0,
        (df["close"] - df["open"]) / df["open"],
        np.nan,
    )
    df = maybe_downcast(df, downcast_float32)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path)
    return out_path


def write_metric_partial(
    csv_path: Path,
    partial_dir: Path,
    freq: str,
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    engine: str,
    force_reprocess: bool,
    downcast_float32: bool,
) -> Optional[Path]:
    out_path = partial_dir / partial_name("metric", csv_path)
    if out_path.exists() and not force_reprocess:
        return out_path

    meta = inspect_metrics_columns(csv_path)
    tc = meta["time_col"]
    raw_usecols = [
        c for c in [
            meta["raw_time_col"],
            meta["raw_oi_col"],
            meta["raw_ls_col"],
            meta["raw_la_col"],
            meta["raw_sa_col"],
        ] if c is not None
    ]
    raw_usecols = list(dict.fromkeys(raw_usecols))
    if not raw_usecols:
        raise ValueError(
            f"Aucune colonne metric reconnue dans "
            f"{csv_path.name}. Colonnes: {meta['columns']}"
        )

    df = read_csv_smart(
        csv_path,
        usecols=raw_usecols,
        chunksize=None,
        engine=engine,
    )
    df = canonicalize_columns(df)

    usable_cols = [
        c for c in [
            tc,
            meta["oi_col"],
            meta["ls_col"],
            meta["la_col"],
            meta["sa_col"],
        ] if c is not None and c in df.columns
    ]
    if tc is None or tc not in usable_cols:
        raise ValueError(
            f"Colonne temps metric introuvable dans {csv_path.name}. "
            f"Colonnes: {list(df.columns)}"
        )

    df = df[usable_cols].copy()
    df[tc] = parse_binance_datetime(df[tc])
    for c in usable_cols:
        if c != tc:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=[tc])
    df = df[(df[tc] >= start_dt) & (df[tc] <= end_dt)]
    if df.empty:
        return None

    rename_map = {}
    mapping = [
        (meta["oi_col"], "sum_open_interest"),
        (meta["ls_col"], "count_long_short_ratio"),
        (meta["la_col"], "count_long_account"),
        (meta["sa_col"], "count_short_account"),
    ]
    for src, dst in mapping:
        if src is not None and src in df.columns:
            rename_map[src] = dst
    if not rename_map:
        raise ValueError(
            f"Aucune colonne metric exploitable dans "
            f"{csv_path.name}. Colonnes: {list(df.columns)}"
        )

    df = df.rename(columns=rename_map)
    df["timestamp"] = df[tc].dt.floor(freq) + pd.to_timedelta(freq)  # PATCH 5 : end-of-bar
    keep = list(rename_map.values())
    grouped = df.groupby("timestamp")
    out = grouped[keep].last().sort_index()
    out["metrics_source_timestamp"] = (
        grouped[tc].max().sort_index()
    )
    for col in REQUIRED_METRIC_OUTPUT_COLS:
        if col not in out.columns:
            out[col] = np.nan
    if "sum_open_interest" in out.columns:
        out["oi_change_abs"] = out["sum_open_interest"].diff()
        out["oi_change_pct"] = out[
            "sum_open_interest"
        ].pct_change(fill_method=None)
    if "count_long_short_ratio" in out.columns:
        out["ls_ratio_change"] = out[
            "count_long_short_ratio"
        ].diff()
        out["ls_ratio_pct_change"] = out[
            "count_long_short_ratio"
        ].pct_change(fill_method=None)

    validate_partial_content(out, "metrics", csv_path.name)
    out = maybe_downcast(out, downcast_float32)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path)
    return out_path


def write_funding_partial(
    csv_path: Path,
    partial_dir: Path,
    freq: str,
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    engine: str,
    force_reprocess: bool,
    downcast_float32: bool,
) -> Optional[Path]:
    out_path = partial_dir / partial_name("funding", csv_path)
    if out_path.exists() and not force_reprocess:
        return out_path

    meta = inspect_funding_columns(csv_path)
    tc = meta["time_col"]
    rc = meta["rate_col"]
    raw_tc = meta["raw_time_col"]
    raw_rc = meta["raw_rate_col"]

    if raw_tc is None or raw_rc is None:
        raise ValueError(
            f"Impossible de remapper les colonnes funding brutes "
            f"pour {csv_path.name}"
        )

    df = read_csv_smart(
        csv_path,
        usecols=[raw_tc, raw_rc],
        chunksize=None,
        engine=engine,
    )
    df = canonicalize_columns(df)

    if tc is None or rc is None or tc not in df.columns or rc not in df.columns:
        raise ValueError(
            f"Colonnes funding absentes après canonicalisation "
            f"dans {csv_path.name}: {list(df.columns)}"
        )

    df[tc] = parse_binance_datetime(df[tc])
    df[rc] = pd.to_numeric(df[rc], errors="coerce")
    df = df.dropna(subset=[tc])
    df = df[(df[tc] >= start_dt) & (df[tc] <= end_dt)]
    if df.empty:
        return None

    df = df.rename(columns={rc: "funding_rate"})
    df["timestamp"] = df[tc].dt.floor(freq) + pd.to_timedelta(freq)  # PATCH 5 : end-of-bar
    grouped = df.groupby("timestamp")
    out = grouped[["funding_rate"]].last().sort_index()
    out["funding_source_timestamp"] = (
        grouped[tc].max().sort_index()
    )
    out["funding_rate_change"] = out["funding_rate"].diff()
    out["funding_rate_abs"] = out["funding_rate"].abs()

    validate_partial_content(out, "funding", csv_path.name)
    out = maybe_downcast(out, downcast_float32)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path)
    return out_path


# ============================================================
# COMBINE PARTIALS
# ============================================================

def _safe_read_parquet(path: Path) -> Optional[pd.DataFrame]:
    try:
        return pd.read_parquet(path)
    except Exception as e:
        log(
            f"Partiel ignoré (illisible): {path.name} ({e})",
            level="WARNING",
        )
        return None


def inspect_liquidation_columns(
    csv_file: Path,
) -> Dict[str, Optional[str]]:
    raw_sample = read_csv_sample(csv_file, nrows=50)
    raw_cols = list(raw_sample.columns)
    canon_sample = canonicalize_columns(raw_sample.copy())
    cols = list(canon_sample.columns)

    time_col = pick_first_existing(cols, LIQUIDATION_TIME_CANDIDATES)
    side_col = pick_first_existing(cols, LIQUIDATION_SIDE_CANDIDATES)
    qty_col = pick_first_existing(cols, LIQUIDATION_QTY_CANDIDATES)
    price_col = pick_first_existing(cols, LIQUIDATION_PRICE_CANDIDATES)
    if time_col is None or side_col is None or qty_col is None:
        raise ValueError(
            f"Colonnes liquidation introuvables dans {csv_file.name}: {cols}"
        )

    raw_to_canon = {raw: normalize_colname(raw) for raw in raw_cols}
    canon_to_raw = {v: k for k, v in raw_to_canon.items()}
    return {
        "time_col": time_col,
        "side_col": side_col,
        "qty_col": qty_col,
        "price_col": price_col,
        "raw_time_col": canon_to_raw.get(time_col),
        "raw_side_col": canon_to_raw.get(side_col),
        "raw_qty_col": canon_to_raw.get(qty_col),
        "raw_price_col": canon_to_raw.get(price_col) if price_col else None,
        "columns": cols,
    }


def write_liquidation_partial(
    csv_path: Path,
    partial_dir: Path,
    freq: str,
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    engine: str,
    force_reprocess: bool,
    downcast_float32: bool,
) -> Optional[Path]:
    out_path = partial_dir / partial_name("liquidation", csv_path)
    if out_path.exists() and not force_reprocess:
        return out_path

    meta = inspect_liquidation_columns(csv_path)
    usecols = [
        c for c in [
            meta.get("raw_time_col"), meta.get("raw_side_col"),
            meta.get("raw_qty_col"), meta.get("raw_price_col"),
        ] if c is not None
    ]
    df = read_csv_smart(csv_path, usecols=usecols, chunksize=None, engine=engine)
    df = canonicalize_columns(df)

    tc = meta["time_col"]
    sc = meta["side_col"]
    qc = meta["qty_col"]
    pc = meta.get("price_col")
    if tc not in df.columns or sc not in df.columns or qc not in df.columns:
        raise ValueError(f"Colonnes liquidation absentes après canonicalisation: {csv_path.name}")

    df[tc] = parse_binance_datetime(df[tc])
    df[sc] = df[sc].astype("string").str.upper()
    df[qc] = pd.to_numeric(df[qc], errors="coerce")
    if pc and pc in df.columns:
        df[pc] = pd.to_numeric(df[pc], errors="coerce")
    else:
        df["average_price"] = np.nan
        pc = "average_price"

    df = df.dropna(subset=[tc, sc, qc])
    df = df[(df[tc] >= start_dt) & (df[tc] <= end_dt)]
    if df.empty:
        return None

    df["is_long_liq"] = (df[sc] == "SELL")
    df["is_short_liq"] = (df[sc] == "BUY")
    df["long_liq_qty"] = np.where(df["is_long_liq"], df[qc], 0.0)
    df["short_liq_qty"] = np.where(df["is_short_liq"], df[qc], 0.0)
    df["total_liq_qty"] = df[qc]
    df["price_used"] = pd.to_numeric(df[pc], errors="coerce")
    df["long_liq_notional"] = df["long_liq_qty"] * df["price_used"]
    df["short_liq_notional"] = df["short_liq_qty"] * df["price_used"]
    df["total_liq_notional"] = df["total_liq_qty"] * df["price_used"]
    df["liquidations_count"] = 1
    df["long_liq_count"] = df["is_long_liq"].astype(int)
    df["short_liq_count"] = df["is_short_liq"].astype(int)
    df["timestamp"] = df[tc].dt.floor(freq) + pd.to_timedelta(freq)  # PATCH 5 : end-of-bar

    out = df.groupby("timestamp")[
        [
            "liquidations_count", "long_liq_count", "short_liq_count",
            "total_liq_qty", "long_liq_qty", "short_liq_qty",
            "total_liq_notional", "long_liq_notional", "short_liq_notional",
        ]
    ].sum().sort_index()
    out["liquidation_source_timestamp"] = df.groupby("timestamp")[tc].max().sort_index()
    denom = out["total_liq_qty"].replace(0, np.nan)
    out["liq_long_share"] = out["long_liq_qty"] / denom
    out["liq_short_share"] = out["short_liq_qty"] / denom
    out = maybe_downcast(out, downcast_float32)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path)
    return out_path


def load_and_combine_liquidation_partials(
    partials: List[Path], downcast_float32: bool,
) -> pd.DataFrame:
    if not partials:
        return pd.DataFrame()
    frames: List[pd.DataFrame] = []
    valid_count = 0
    total_count = len(partials)
    for p in sorted(partials):
        chunk = _safe_read_parquet(p)
        if chunk is None or chunk.empty:
            continue
        valid_count += 1
        frames.append(chunk.sort_index())
    _validate_partial_read_health(valid_count, total_count, "liquidation")
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, axis=0).sort_index().groupby(level=0).sum(min_count=1).sort_index()
    if "liquidation_source_timestamp" in df.columns:
        src = pd.concat([f[["liquidation_source_timestamp"]] for f in frames if "liquidation_source_timestamp" in f.columns], axis=0).sort_index()
        df["liquidation_source_timestamp"] = src.groupby(level=0).max().sort_index()
    liq_total = pd.to_numeric(df.get("total_liq_qty", np.nan), errors="coerce")
    if isinstance(liq_total, pd.Series):
        denom = liq_total.replace(0, np.nan)
        if "long_liq_qty" in df.columns:
            df["liq_long_share"] = df["long_liq_qty"] / denom
        if "short_liq_qty" in df.columns:
            df["liq_short_share"] = df["short_liq_qty"] / denom
        if {"long_liq_qty", "short_liq_qty"}.issubset(df.columns):
            df["liq_imbalance"] = np.where(
                liq_total > 0,
                (df["long_liq_qty"] - df["short_liq_qty"]) / liq_total,
                0.0,
            )
    return maybe_downcast(df, downcast_float32)


def _validate_partial_read_health(
    valid_count: int, total_count: int, label: str,
) -> None:
    if total_count <= 0:
        return
    if valid_count == 0:
        raise ValueError(
            f"Aucun partial {label} valide "
            f"({valid_count}/{total_count})"
        )
    if valid_count < max(1, int(np.ceil(total_count * 0.5))):
        raise ValueError(
            f"Trop de partials {label} corrompus: "
            f"{valid_count}/{total_count}"
        )


def _recompute_trade_bar_features(
    df: pd.DataFrame,
) -> pd.DataFrame:
    if df.empty:
        return df
    if {"notional", "volume"}.issubset(df.columns):
        df["vwap"] = safe_divide(df["notional"], df["volume"], "nan")
    if {"buy_vol", "sell_vol"}.issubset(df.columns):
        tot = df["buy_vol"] + df["sell_vol"]
        df["buy_sell_imbalance"] = safe_divide(
            df["buy_vol"] - df["sell_vol"], tot, "nan"
        )
        df["buy_sell_ratio"] = safe_divide(
            df["buy_vol"], df["sell_vol"], "nan"
        )
    if {"high", "low"}.issubset(df.columns):
        df["hl_range"] = np.where(
            df["low"] > 0,
            (df["high"] - df["low"]) / df["low"],
            np.nan,
        )
    if {"open", "close"}.issubset(df.columns):
        df["oc_return"] = np.where(
            df["open"] > 0,
            (df["close"] - df["open"]) / df["open"],
            np.nan,
        )
    return df


def load_and_combine_trade_partials(
    partials: List[Path], downcast_float32: bool,
) -> pd.DataFrame:
    if not partials:
        raise ValueError("Aucun partiel trade disponible")

    agg_map = _build_trade_combine_agg_map()
    valid_count = 0
    total_count = len(partials)
    combined: Optional[pd.DataFrame] = None

    for p in sorted(partials):
        chunk = _safe_read_parquet(p)
        if chunk is None or chunk.empty:
            continue
        valid_count += 1
        keep = [c for c in agg_map if c in chunk.columns]
        chunk = chunk[keep].sort_index(kind="stable")
        if combined is None or combined.empty:
            combined = chunk
            continue

        merged = pd.concat([combined, chunk], axis=0).sort_index(kind="stable")
        available_agg = {k: v for k, v in agg_map.items() if k in merged.columns}
        combined = merged.groupby(level=0, sort=True).agg(available_agg).sort_index()
        del merged, chunk

    _validate_partial_read_health(
        valid_count, total_count, "trade",
    )
    if combined is None or combined.empty:
        raise ValueError("Aucun partiel trade valide")

    df = combined
    del combined

    df = _recompute_trade_bar_features(df)
    df["signed_vol"] = pd.to_numeric(
        df["signed_vol"], errors="coerce"
    ).astype(np.float64)
    if (
        isinstance(df.index, pd.DatetimeIndex)
        and len(df.index) > 1
        and (df.index.max() - df.index.min()) > pd.Timedelta(days=365 * 5)
    ):
        df["cvd"] = (
            df.groupby(pd.Grouper(freq="365D"))["signed_vol"]
            .cumsum()
            .astype(np.float64)
        )
    else:
        df["cvd"] = df["signed_vol"].cumsum().astype(np.float64)

    bucket_feats = [
        "avg_trade_size", "trade_size_std",
        "avg_trade_notional", "trade_notional_std",
        "trade_size_hhi", "trade_size_entropy",
        "large_trade_count_ratio",
        "large_trade_notional_ratio",
    ]
    if any(c not in df.columns for c in bucket_feats):
        df = compute_trade_bucket_features(df)

    df = df.drop(
        columns=["qty_sq", "notional_sq", "open_ts", "close_ts"],
        errors="ignore",
    )
    return maybe_downcast(df, downcast_float32)


def load_and_combine_metric_partials(
    partials: List[Path], downcast_float32: bool,
) -> pd.DataFrame:
    if not partials:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    valid_count = 0
    total_count = len(partials)
    for p in sorted(partials):
        chunk = _safe_read_parquet(p)
        if chunk is None or chunk.empty:
            continue
        valid_count += 1
        frames.append(chunk.sort_index())

    _validate_partial_read_health(
        valid_count, total_count, "metric",
    )
    if not frames:
        return pd.DataFrame()

    df = (
        pd.concat(frames, axis=0)
        .sort_index()
        .groupby(level=0)
        .last()
        .sort_index()
    )
    del frames

    for col in REQUIRED_METRIC_OUTPUT_COLS:
        if col not in df.columns:
            df[col] = np.nan
    if "sum_open_interest" in df.columns:
        df["oi_change_abs"] = df["sum_open_interest"].diff()
        df["oi_change_pct"] = df[
            "sum_open_interest"
        ].pct_change(fill_method=None)
    if "count_long_short_ratio" in df.columns:
        df["ls_ratio_change"] = df[
            "count_long_short_ratio"
        ].diff()
        df["ls_ratio_pct_change"] = df[
            "count_long_short_ratio"
        ].pct_change(fill_method=None)

    validate_partial_content(df, "metrics", "combined_metrics")
    return maybe_downcast(df, downcast_float32)


def load_and_combine_funding_partials(
    partials: List[Path], downcast_float32: bool,
) -> pd.DataFrame:
    if not partials:
        return pd.DataFrame()

    frames: List[pd.DataFrame] = []
    valid_count = 0
    total_count = len(partials)
    for p in sorted(partials):
        chunk = _safe_read_parquet(p)
        if chunk is None or chunk.empty:
            continue
        valid_count += 1
        frames.append(chunk.sort_index())

    _validate_partial_read_health(
        valid_count, total_count, "funding",
    )
    if not frames:
        return pd.DataFrame()

    df = (
        pd.concat(frames, axis=0)
        .sort_index()
        .groupby(level=0)
        .last()
        .sort_index()
    )
    del frames

    if "funding_rate" in df.columns:
        df["funding_rate_change"] = df["funding_rate"].diff()
        df["funding_rate_abs"] = df["funding_rate"].abs()

    validate_partial_content(
        df, "funding", "combined_funding",
    )
    return maybe_downcast(df, downcast_float32)


# ============================================================
# PATCH HELPER — safe_merge_asof (ajouté pour le patch 1)
# ============================================================

def safe_merge_asof(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str,
    suffix: str,
    tolerance: pd.Timedelta,
) -> pd.DataFrame:
    """Helper ajouté pour le patch microstructure (patch 1).
    Gère index DatetimeIndex → reset + merge_asof + suffix."""
    left = left.copy()
    right = right.copy()

    if isinstance(left.index, pd.DatetimeIndex):
        left = left.reset_index().rename(columns={left.index.name or "index": on})
    if isinstance(right.index, pd.DatetimeIndex):
        right = right.reset_index().rename(columns={right.index.name or "index": on})

    merged = pd.merge_asof(
        left.sort_values(on),
        right.sort_values(on),
        on=on,
        tolerance=tolerance,
        direction="backward",
    )
    merged = merged.set_index(on).sort_index()

    right_cols = [c for c in right.columns if c != on]
    rename_map = {c: f"{c}{suffix}" for c in right_cols}
    merged = merged.rename(columns=rename_map)
    return merged


# ============================================================
# MERGE / FEATURES
# ============================================================

def resample_funding_adaptive(
    funding_df: pd.DataFrame,
    freq: str,
    regime_series: Optional[pd.Series] = None,
) -> pd.DataFrame:
    """
    Resample funding en respectant les changements de régime
    (1h vs 4h/mixed) pour éviter les duplications ou trous artificiels.
    Retourne un DataFrame avec colonne timestamp prêt pour le merge.
    """
    if funding_df is None or funding_df.empty:
        return pd.DataFrame()

    df = funding_df.copy()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
        df = df.set_index("timestamp")
    elif isinstance(df.index, pd.DatetimeIndex):
        df = df.sort_index()
    else:
        return pd.DataFrame()

    if regime_series is not None:
        rs = regime_series.copy()
        if not isinstance(rs.index, pd.DatetimeIndex):
            if "timestamp" in funding_df.columns:
                rs.index = pd.to_datetime(funding_df["timestamp"], utc=True, errors="coerce")
            else:
                rs.index = df.index
        rs = rs.reindex(df.index).astype(str)
        df["_funding_regime_input"] = rs

    regime_col = "_funding_regime_input" if "_funding_regime_input" in df.columns else (
        "funding_regime" if "funding_regime" in df.columns else None
    )

    if regime_col is None:
        out = df.resample(freq, label="right", closed="right").last().ffill()  # PATCH 5
        out = out[~out.index.duplicated(keep="last")]
        return out.reset_index()

    regime_seg = df[regime_col].astype(str).ne(df[regime_col].astype(str).shift()).cumsum()

    resampled_parts: List[pd.DataFrame] = []
    for _, group in df.groupby(regime_seg, sort=False):
        if group.empty:
            continue
        if len(group) < 2:
            resampled_parts.append(group)
            continue
        res = group.resample(freq, label="right", closed="right").last().ffill()  # PATCH 5
        resampled_parts.append(res)

    if not resampled_parts:
        return pd.DataFrame()

    combined = pd.concat(resampled_parts, axis=0).sort_index()
    combined = combined[~combined.index.duplicated(keep="last")]
    combined = combined.drop(columns=["_funding_regime_input"], errors="ignore")
    return combined.reset_index()


def merge_trades_and_metrics(
    trades_df: pd.DataFrame,
    metrics_df: pd.DataFrame,
    mode: str,
    max_metrics_staleness: Optional[str],
    freq: str,
) -> pd.DataFrame:
    if metrics_df is None or metrics_df.empty:
        out = trades_df.copy()
        out.attrs["merge_diagnostics"] = {
            "mode": mode,
            "tolerance": max_metrics_staleness,
            "rows_total": int(len(out)),
            "rows_with_any_aux": 0,
            "rows_without_aux": int(len(out)),
            "aux_columns": [],
            "non_null": {},
        }
        return out

    aux_cols = [
        c for c in metrics_df.columns
        if c not in {
            "metrics_source_timestamp",
            "funding_source_timestamp",
        }
    ]
    overlap = [
        c for c in metrics_df.columns
        if c in trades_df.columns
    ]
    right_df = metrics_df.drop(columns=overlap, errors="ignore")

    aux_time_col = None
    for candidate in (
        "metrics_source_timestamp",
        "funding_source_timestamp",
    ):
        if candidate in right_df.columns:
            aux_time_col = candidate
            break

    if mode == "inner":
        merged = (
            trades_df.join(right_df, how="inner").sort_index()
        )
        merged = _apply_flow_state_fill(merged)
        merged = validate_post_merge_types(merged)
        merged.attrs["merge_diagnostics"] = (
            compute_merge_diagnostics(
                merged, aux_cols, aux_time_col, mode, None,
            )
        )
        return merged

    if mode == "left_ffill":
        aligned = right_df.resample(freq).last().sort_index()
        aligned = _apply_flow_state_fill(aligned)
        merged = (
            trades_df.join(aligned, how="left").sort_index()
        )
        merged = _apply_flow_state_fill(merged)
        merged = validate_post_merge_types(merged)
        merged.attrs["merge_diagnostics"] = (
            compute_merge_diagnostics(
                merged, aux_cols, aux_time_col, mode, None,
            )
        )
        return merged

    if mode == "left":
        merged = (
            trades_df.join(right_df, how="left").sort_index()
        )
        merged = _apply_flow_state_fill(merged)
        merged = validate_post_merge_types(merged)
        merged.attrs["merge_diagnostics"] = (
            compute_merge_diagnostics(
                merged, aux_cols, aux_time_col, mode, None,
            )
        )
        return merged

    # ── mode == "asof" ──────────────────────────────────────
    left = (
        trades_df.sort_index()
        .reset_index()
        .rename(columns={
            trades_df.index.name or "index": "timestamp",
        })
    )
    right = (
        right_df.sort_index()
        .reset_index()
        .rename(columns={
            right_df.index.name or "index": "aux_timestamp",
        })
    )
    ov2 = [
        c for c in right.columns
        if c != "aux_timestamp" and c in left.columns
    ]
    if ov2:
        right = right.drop(columns=ov2, errors="ignore")

    left["timestamp"] = pd.to_datetime(
        left["timestamp"], utc=True,
    ).astype("datetime64[ns, UTC]")
    right["aux_timestamp"] = pd.to_datetime(
        right["aux_timestamp"], utc=True,
    ).astype("datetime64[ns, UTC]")

    tolerance = pd.Timedelta(
        max_metrics_staleness or DEFAULT_MAX_METRICS_STALENESS
    )
    merged = pd.merge_asof(
        left.sort_values("timestamp"),
        right.sort_values("aux_timestamp"),
        left_on="timestamp",
        right_on="aux_timestamp",
        direction="backward",
        tolerance=tolerance,
        allow_exact_matches=True,
    )
    merged = merged.set_index("timestamp").sort_index()
    merged = _apply_flow_state_fill(merged)
    merged = validate_post_merge_types(merged)
    merged.attrs["merge_diagnostics"] = (
        compute_merge_diagnostics(
            merged, aux_cols, "aux_timestamp",
            mode, str(tolerance),
        )
    )
    return merged


def add_return_and_vol_features(
    out: pd.DataFrame, w1d: int, w7d: int,
) -> None:
    if "close" in out.columns and out["close"].notna().any():
        assign_feature(
            out, "ret_1p",
            out["close"].pct_change(fill_method=None),
        )
        assign_feature(
            out, f"ret_{w1d}p",
            out["close"].pct_change(w1d, fill_method=None),
        )
        assign_feature(
            out, f"ret_{w7d}p",
            out["close"].pct_change(w7d, fill_method=None),
        )

    if "ret_1p" in out.columns:
        ret1 = out["ret_1p"]
        assign_feature(
            out, f"vol_{w1d}p",
            ret1.rolling(w1d, min_periods=w1d).std(),
        )
        assign_feature(
            out, f"vol_{w7d}p",
            ret1.rolling(w7d, min_periods=w7d).std(),
        )
        assign_feature(
            out, f"realized_vol_{w1d}p",
            ret1.rolling(w1d, min_periods=w1d).std()
            * np.sqrt(w1d),
        )


def add_true_range_and_candles(
    out: pd.DataFrame, w1d: int,
) -> None:
    if {"high", "low", "close"}.issubset(out.columns):
        prev_close = out["close"].shift(1)
        tr1 = out["high"] - out["low"]
        tr2 = (out["high"] - prev_close).abs()
        tr3 = (out["low"] - prev_close).abs()
        assign_feature(
            out, "tr",
            np.maximum.reduce([
                tr1.values, tr2.values, tr3.values,
            ]),
        )
        assign_feature(
            out, f"atr_{w1d}p",
            pd.Series(out["tr"], index=out.index).rolling(
                w1d, min_periods=w1d,
            ).mean(),
        )

    if validate_required_ohlc(
        out, ["open", "high", "low", "close"],
    ):
        assign_feature(
            out, "body", out["close"] - out["open"],
        )
        assign_feature(
            out, "body_pct",
            safe_divide(out["body"], out["open"]),
        )
        assign_feature(
            out, "upper_wick",
            out["high"] - np.maximum(out["open"], out["close"]),
        )
        assign_feature(
            out, "lower_wick",
            np.minimum(out["open"], out["close"]) - out["low"],
        )
        assign_feature(
            out, f"momentum_{w1d}p",
            safe_divide(out["close"].diff(w1d), out["close"].shift(w1d)),
        )


def add_microstructure_rolls(
    out: pd.DataFrame, w1d: int, w3d: int,
) -> None:
    compute_block_rolling_stats(
        out,
        [
            "volume", "signed_vol", "sum_open_interest",
            "count_long_short_ratio", "oi_change_pct",
            "funding_rate", "avg_trade_size",
            "avg_trade_notional", "trade_size_hhi",
            "trade_size_entropy", "large_trade_count_ratio",
            "large_trade_notional_ratio",
            "whale_trade_count_ratio",
            "whale_trade_notional_ratio",
            "vol_parkinson", "vol_garman_klass",
        ],
        w1d, w3d, batch_size=4,
    )


def add_lag_features(out: pd.DataFrame, w1d: int) -> None:
    lag_cols = [
        "close", "volume", "signed_vol", "cvd",
        "sum_open_interest", "count_long_short_ratio",
        "ret_1p", "funding_rate", "avg_trade_size",
        "avg_trade_notional", "trade_size_hhi",
        "large_trade_notional_ratio",
        "whale_trade_notional_ratio",
        "vol_parkinson", "vol_garman_klass",
    ]
    for c in lag_cols:
        if c in out.columns:
            assign_feature(
                out, f"{c}_lag_1p", out[c].shift(1),
            )
            assign_feature(
                out, f"{c}_lag_2p", out[c].shift(2),
            )
            assign_feature(
                out, f"{c}_lag_1d", out[c].shift(w1d),
            )


def add_interaction_features(out: pd.DataFrame) -> None:
    if "funding_rate_change" in out.columns:
        assign_feature(
            out, "funding_rate_change_lag_1p",
            out["funding_rate_change"].shift(1),
        )
        assign_feature(
            out, "funding_rate_change_abs",
            out["funding_rate_change"].abs(),
        )

    pairs = [
        ("oi_change_pct", "signed_vol", "oi_x_signed_vol"),
        (
            "count_long_short_ratio", "oi_change_pct",
            "ls_x_oi_change",
        ),
        ("funding_rate", "ret_1p", "funding_x_ret_1p"),
        ("funding_rate", "sum_open_interest", "funding_x_oi"),
    ]
    for a, b, name in pairs:
        if a in out.columns and b in out.columns:
            assign_feature(out, name, out[a] * out[b])


def add_cvd_features(
    df: pd.DataFrame, w1d: int, w3d: int,
) -> None:
    if "cvd" not in df.columns:
        return
    df["cvd_delta_1"] = df["cvd"].diff()
    df["cvd_delta_1d"] = df["cvd"].diff(w1d)
    df["cvd_ema_fast"] = df["cvd"].ewm(span=max(3, w1d // 4), adjust=False).mean()
    df["cvd_ema_slow"] = df["cvd"].ewm(span=max(6, w1d), adjust=False).mean()
    roll_mean = df["cvd"].rolling(w1d, min_periods=w1d).mean()
    roll_std = df["cvd"].rolling(w1d, min_periods=w1d).std(ddof=0)
    df["cvd_z_1d"] = (df["cvd"] - roll_mean) / roll_std.replace(0, np.nan)
    if "close" in df.columns:
        price_ret = pd.to_numeric(df["close"], errors="coerce").pct_change(w1d)
        cvd_ret = pd.to_numeric(df["cvd"], errors="coerce").diff(w1d)
        df["cvd_divergence_1d"] = cvd_ret - price_ret


def annotate_funding_regime(
    funding_df: pd.DataFrame,
    funding_info: Optional[Dict[str, object]],
    downcast_float32: bool = False,
) -> pd.DataFrame:
    if funding_df is None or funding_df.empty:
        return funding_df
    out = funding_df.copy()
    observed_hours = pd.Series(np.nan, index=out.index, dtype="float64")
    if "funding_source_timestamp" in out.columns:
        ts = pd.to_datetime(out["funding_source_timestamp"], utc=True, errors="coerce")
        diffs = ts.diff().dt.total_seconds().div(3600.0)
        observed_hours = diffs.astype("float64")
        out["funding_observed_interval_hours"] = observed_hours
    else:
        out["funding_observed_interval_hours"] = observed_hours

    api_interval = None
    try:
        api_interval = int((funding_info or {}).get("funding_interval_hours"))
    except Exception:
        api_interval = None
    out["funding_interval_hours_api"] = (
        pd.Series(float(api_interval), index=out.index, dtype="float64")
        if api_interval is not None else pd.Series(np.nan, index=out.index, dtype="float64")
    )

    regime = pd.Series("unknown", index=out.index, dtype="object")
    rounded = observed_hours.round(0)
    regime.loc[rounded.eq(1)] = "1h"
    regime.loc[rounded.eq(4)] = "4h"
    other_mask = rounded.notna() & ~rounded.isin([1.0, 4.0])
    regime.loc[other_mask] = "mixed"
    if api_interval is not None:
        regime.loc[regime.eq("unknown")] = f"api_{api_interval}h"
    if rounded.dropna().nunique() > 1:
        regime.loc[observed_hours.notna()] = "mixed"
    out["funding_regime"] = regime
    regime_change = regime.ne(regime.shift(1)) & regime.notna() & regime.shift(1).notna()
    out["funding_regime_change"] = regime_change.astype("int8")
    return maybe_downcast(out, downcast_float32)


def add_state_staleness_flags(
    df: pd.DataFrame, w1d: int,
    funding_spike_abs_threshold: float = 0.001,
    funding_interval_hours: Optional[int] = None,
    funding_spike_rel_multiplier: float = 5.0,
    inplace: bool = True,
) -> pd.DataFrame:
    out = df if inplace else df.copy()
    stale_window = max(3, int(w1d))

    def _stale_flag(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce")
        no_change = s.diff().abs().le(1e-12) & s.notna() & s.shift(1).notna()
        return (
            no_change.rolling(stale_window, min_periods=stale_window).sum()
            >= stale_window
        ).astype("int8")

    if "sum_open_interest" in out.columns:
        out["oi_stale_flag"] = _stale_flag(out["sum_open_interest"])
    else:
        out["oi_stale_flag"] = pd.Series(0, index=out.index, dtype="int8")

    if "funding_rate" in out.columns:
        funding_rate = pd.to_numeric(out["funding_rate"], errors="coerce")
        out["funding_stale_flag"] = _stale_flag(funding_rate)
        context_window = max(8, int(w1d))
        if funding_interval_hours and funding_interval_hours > 0:
            context_window = max(context_window, int(max(4, funding_interval_hours)))
        minp = max(4, context_window // 3)
        roll_med = funding_rate.rolling(context_window, min_periods=minp).median()
        roll_mad = (funding_rate - roll_med).abs().rolling(context_window, min_periods=minp).median()
        contextual_spike = (funding_rate - roll_med).abs() > (
            float(funding_spike_rel_multiplier) * roll_mad.replace(0, np.nan)
        )
        mild_thr = float(funding_spike_abs_threshold)
        strong_thr = max(mild_thr * 3.0, 0.003)
        extreme_thr = max(mild_thr * 10.0, 0.01)
        abs_rate = funding_rate.abs()
        out["funding_spike_mild_flag"] = ((abs_rate >= mild_thr) | contextual_spike).fillna(False).astype("int8")
        out["funding_spike_strong_flag"] = (abs_rate >= strong_thr).fillna(False).astype("int8")
        out["funding_spike_extreme_flag"] = (abs_rate >= extreme_thr).fillna(False).astype("int8")
        out["funding_spike_pos_flag"] = (funding_rate >= mild_thr).fillna(False).astype("int8")
        out["funding_spike_neg_flag"] = (funding_rate <= -mild_thr).fillna(False).astype("int8")
        mild_bool = out["funding_spike_mild_flag"].astype(bool)
        grp = mild_bool.ne(mild_bool.shift()).cumsum()
        duration = mild_bool.groupby(grp).cumsum().where(mild_bool, 0)
        out["funding_spike_duration"] = duration.astype("int32")
        out["funding_spike_flag"] = out["funding_spike_mild_flag"].astype("int8")
    else:
        out["funding_stale_flag"] = pd.Series(0, index=out.index, dtype="int8")
        out["funding_spike_flag"] = pd.Series(0, index=out.index, dtype="int8")
        out["funding_spike_mild_flag"] = pd.Series(0, index=out.index, dtype="int8")
        out["funding_spike_strong_flag"] = pd.Series(0, index=out.index, dtype="int8")
        out["funding_spike_extreme_flag"] = pd.Series(0, index=out.index, dtype="int8")
        out["funding_spike_pos_flag"] = pd.Series(0, index=out.index, dtype="int8")
        out["funding_spike_neg_flag"] = pd.Series(0, index=out.index, dtype="int8")
        out["funding_spike_duration"] = pd.Series(0, index=out.index, dtype="int32")
    return out

def add_data_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
    out = df
    if "trade_count" in out.columns:
        tc = pd.to_numeric(out["trade_count"], errors="coerce").fillna(0.0)
        out["data_quality_flag"] = (tc > 0).astype("int8")
        if "close" in out.columns:
            close = pd.to_numeric(out["close"], errors="coerce")
            out["data_gap_suspect"] = (
                (tc <= 0)
                & close.notna()
                & close.ne(close.shift(1))
            ).astype("int8")
        else:
            out["data_gap_suspect"] = (tc <= 0).astype("int8")
    else:
        out["data_quality_flag"] = pd.Series(1, index=out.index, dtype="int8")
        out["data_gap_suspect"] = pd.Series(0, index=out.index, dtype="int8")
    return out


def add_quant_features(
    df: pd.DataFrame, freq: str, inplace: bool = False,
) -> pd.DataFrame:
    out = df if inplace else df.copy()
    w = infer_feature_windows(freq)
    w1d, w3d, w7d = w["1d"], w["3d"], w["7d"]

    add_return_and_vol_features(out, w1d=w1d, w7d=w7d)
    add_true_range_and_candles(out, w1d=w1d)
    add_range_volatility_features(out, w1d=w1d, w3d=w3d)

    if "volume" in out.columns:
        assign_feature(
            out, f"vp_q25_{w1d}p",
            out["volume"].rolling(
                w1d, min_periods=w1d,
            ).quantile(0.25),
        )

    add_microstructure_rolls(out, w1d=w1d, w3d=w3d)
    add_cvd_features(out, w1d=w1d, w3d=w3d)
    add_lag_features(out, w1d=w1d)
    add_interaction_features(out)

    out.replace([np.inf, -np.inf], np.nan, inplace=True)

    # ── Alertes features quasi-vides ────────────────────────
    sparse_alerts = []
    for col in out.columns:
        s = out[col]
        if not pd.api.types.is_numeric_dtype(s):
            continue
        ratio = float(s.notna().mean())
        if ratio < 0.01 and len(s) >= 10:
            sparse_alerts.append(
                f"Feature '{col}' quasi vide "
                f"({ratio:.2%} non-NaN)"
            )
    if sparse_alerts:
        out.attrs["sparse_feature_alerts"] = sparse_alerts
        for alert in sparse_alerts[:5]:
            log(alert, level="WARNING")
        if len(sparse_alerts) > 5:
            log(
                f"... et {len(sparse_alerts) - 5} autres "
                f"features quasi-vides",
                level="WARNING",
            )

    funding_spike_abs_threshold = float(
        out.attrs.get("funding_spike_abs_threshold", 0.001)
    )
    funding_spike_rel_multiplier = float(
        out.attrs.get("funding_spike_rel_multiplier", 5.0)
    )
    funding_interval_hours = out.attrs.get("funding_interval_hours")
    out = add_state_staleness_flags(
        out, w1d=w1d,
        funding_spike_abs_threshold=funding_spike_abs_threshold,
        funding_interval_hours=(
            int(funding_interval_hours)
            if funding_interval_hours not in [None, "", np.nan] and pd.notna(funding_interval_hours)
            else None
        ),
        funding_spike_rel_multiplier=funding_spike_rel_multiplier,
    )
    out = add_data_quality_flags(out)
    return out


# ============================================================
# REPORT / CLEANUP
# ============================================================

def build_data_dictionary(
    df: pd.DataFrame,
) -> List[Dict[str, object]]:
    dictionary: List[Dict[str, object]] = []
    for col in df.columns:
        s = df[col]
        entry: Dict[str, object] = {
            "name": col,
            "dtype": str(s.dtype),
            "non_null": int(s.notna().sum()),
            "nulls": int(s.isna().sum()),
        }
        if pd.api.types.is_numeric_dtype(s):
            vals = pd.to_numeric(s, errors="coerce")
            if vals.notna().any():
                entry.update({
                    "min": float(vals.min()),
                    "max": float(vals.max()),
                    "mean": float(vals.mean()),
                })
        dictionary.append(entry)
    return dictionary


def cleanup_extracted_cache(
    cache_dir: Path, symbol: str,
) -> int:
    base = cache_dir / "extracted"
    deleted = 0
    if not base.exists():
        return 0
    symbol = symbol.upper()
    for dtype_dir in base.iterdir():
        if not dtype_dir.is_dir():
            continue
        for period_dir in dtype_dir.iterdir():
            if not period_dir.is_dir():
                continue
            target = period_dir / symbol
            if target.is_dir():
                shutil.rmtree(target, ignore_errors=True)
                deleted += 1
    return deleted


def run_post_build_validation(
    df: pd.DataFrame, freq: str,
) -> List[str]:
    violations: List[str] = []
    if df is None:
        return ["CRITICAL: dataset final absent"]
    if df.empty:
        return ["CRITICAL: dataset final vide"]
    if not isinstance(df.index, pd.DatetimeIndex):
        return [
            "CRITICAL: l'index final n'est pas un DatetimeIndex"
        ]
    if df.index.tz is None:
        violations.append(
            "CRITICAL: l'index final n'est pas "
            "timezone-aware (UTC attendu)"
        )
    if not df.index.is_monotonic_increasing:
        violations.append(
            "CRITICAL: l'index final n'est pas trié "
            "par ordre croissant"
        )
    dup_count = int(df.index.duplicated(keep=False).sum())
    if dup_count > 0:
        violations.append(
            f"CRITICAL: {dup_count} timestamp(s) dupliqué(s)"
        )

    gaps = compute_time_gaps(df.index, freq)
    if gaps.get("missing_bars", 0) > 0:
        violations.append(
            f"WARNING: {gaps['missing_bars']} barre(s) "
            f"manquante(s) ({freq})"
        )

    required_ohlc = ["open", "high", "low", "close"]
    missing_req = [
        c for c in required_ohlc if c not in df.columns
    ]
    if missing_req:
        violations.append(
            f"CRITICAL: colonnes OHLC manquantes: "
            f"{missing_req}"
        )
    else:
        o = pd.to_numeric(df["open"], errors="coerce")
        h = pd.to_numeric(df["high"], errors="coerce")
        lo = pd.to_numeric(df["low"], errors="coerce")
        cl = pd.to_numeric(df["close"], errors="coerce")
        bad = (
            (h < lo) | (h < o) | (h < cl) | (lo > o) | (lo > cl)
        ).fillna(False)
        n_bad = int(bad.sum())
        if n_bad > 0:
            violations.append(
                f"CRITICAL: {n_bad} ligne(s) OHLC invalide(s)"
            )

    nonneg = [
        "volume", "trade_count", "notional",
        "buy_vol", "sell_vol",
    ]
    for col in nonneg:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            n_neg = int((s < 0).fillna(False).sum())
            if n_neg > 0:
                violations.append(
                    f"CRITICAL: {n_neg} valeur(s) "
                    f"négative(s) dans {col}"
                )

    return violations




def sanitize_export_dataset(
    df: pd.DataFrame,
    allow_target_columns_on_export: bool = False,
    keep_cvd_raw_on_export: bool = False,
) -> pd.DataFrame:
    if df is None:
        return df
    out = df.copy()
    drop_cols = []
    if not allow_target_columns_on_export:
        drop_cols.extend([
            c for c in out.columns
            if isinstance(c, str) and c.startswith("target_")
        ])
    if not keep_cvd_raw_on_export and "cvd" in out.columns:
        drop_cols.append("cvd")
    if drop_cols:
        out = out.drop(columns=list(dict.fromkeys(drop_cols)), errors="ignore")
    return out



def infer_series_event_frequency(
    series: Optional[pd.Series],
) -> Dict[str, object]:
    if series is None:
        return {}
    ts = pd.to_datetime(series, utc=True, errors="coerce").dropna().sort_values()
    if len(ts) < 2:
        return {
            "n_timestamps": int(len(ts)),
            "median_hours": None,
            "mode_hours": None,
            "unique_step_hours": [],
        }
    steps = ts.diff().dropna().dt.total_seconds() / 3600.0
    steps = steps.replace([np.inf, -np.inf], np.nan).dropna()
    if steps.empty:
        return {
            "n_timestamps": int(len(ts)),
            "median_hours": None,
            "mode_hours": None,
            "unique_step_hours": [],
        }
    rounded = steps.round(6)
    mode_hours = None
    try:
        mode_vals = rounded.mode(dropna=True)
        if not mode_vals.empty:
            mode_hours = float(mode_vals.iloc[0])
    except Exception:
        mode_hours = None
    uniq = sorted({round(float(x), 6) for x in rounded.unique()})
    return {
        "n_timestamps": int(len(ts)),
        "median_hours": round(float(rounded.median()), 6),
        "mode_hours": mode_hours,
        "unique_step_hours": uniq[:12],
    }

def build_report(
    args: argparse.Namespace,
    cache_key: str,
    run_ts: str,
    trade_dl_stats: Dict[str, object],
    metric_dl_stats: Dict[str, object],
    funding_dl_stats: Dict[str, object],
    oi_api_stats: Optional[Dict[str, object]],
    liquidation_dl_stats: Optional[Dict[str, object]],
    exchange_info: Optional[Dict[str, object]],
    funding_info: Optional[Dict[str, object]],
    trades_df: pd.DataFrame,
    metrics_df: pd.DataFrame,
    funding_df: pd.DataFrame,
    oi_api_df: Optional[pd.DataFrame],
    liquidation_df: Optional[pd.DataFrame],
    final_df: pd.DataFrame,
    trade_partial_count: int,
    metric_partial_count: int,
    funding_partial_count: int,
    liquidation_partial_count: int = 0,
    trade_files: Optional[List[Path]] = None,
    metric_files: Optional[List[Path]] = None,
    funding_files: Optional[List[Path]] = None,
    liquidation_files: Optional[List[Path]] = None,
) -> Dict[str, object]:
    trades_qc = (
        compute_time_gaps(trades_df.index, args.freq)
        if not trades_df.empty else {}
    )
    metrics_qc = (
        compute_time_gaps(metrics_df.index, args.freq)
        if metrics_df is not None and not metrics_df.empty
        else {}
    )
    funding_qc = (
        compute_time_gaps(funding_df.index, args.freq)
        if funding_df is not None and not funding_df.empty
        else {}
    )
    oi_api_qc = (
        compute_time_gaps(oi_api_df.index, args.freq)
        if oi_api_df is not None and not oi_api_df.empty else {}
    )
    liquidation_qc = (
        compute_time_gaps(liquidation_df.index, args.freq)
        if liquidation_df is not None and not liquidation_df.empty else {}
    )
    final_qc = (
        compute_time_gaps(final_df.index, args.freq)
        if not final_df.empty else {}
    )
    merge_diag = (
        final_df.attrs.get("merge_diagnostics", {})
        if final_df is not None else {}
    )
    sparse_alerts = (
        final_df.attrs.get("sparse_feature_alerts", [])
        if final_df is not None else []
    )

    report: Dict[str, object] = {
        "pipeline_version": PIPELINE_VERSION,
        "run_timestamp_utc": run_ts,
        "cache_key": cache_key,
        "market": args.market,
        "symbol": args.symbol.upper(),
        "freq": args.freq,
        "start": args.start,
        "end": args.end,
        "engine_used": args.engine,
        "chunksize": int(args.chunksize),
        "batch_size": int(args.batch_size),
        "metrics_merge": args.metrics_merge,
        "max_metrics_staleness": args.max_metrics_staleness,
        "keep_cvd_raw_on_export": bool(args.keep_cvd_raw_on_export),
        "funding_spike_abs_threshold": float(args.funding_spike_abs_threshold),
        "funding_spike_rel_multiplier": float(args.funding_spike_rel_multiplier),
        "api_max_per_min": float(args.api_max_per_min),
        "api_rate_limit_jitter": float(args.api_rate_limit_jitter),
        "parallel_backend": getattr(args, "parallel_backend", "process"),
        "skip_partial_listing": bool(getattr(args, "skip_partial_listing", False)),
        "downcast_float32": bool(args.downcast_float32),
        "download_stats": {
            "aggTrades": trade_dl_stats,
            "metrics": metric_dl_stats,
            "fundingRate": funding_dl_stats,
            "openInterestApi5m": oi_api_stats or {},
            "liquidationSnapshot": liquidation_dl_stats or {},
        },
        "exchange_info_precheck": exchange_info or {},
        "funding_rate_info_api": funding_info or {},
        "partial_counts": {
            "trade_partials": trade_partial_count,
            "metric_partials": metric_partial_count,
            "funding_partials": funding_partial_count,
            "liquidation_partials": liquidation_partial_count,
        },
        "trades_qc": trades_qc,
        "metrics_qc": metrics_qc,
        "funding_qc": funding_qc,
        "oi_api_qc": oi_api_qc,
        "liquidation_qc": liquidation_qc,
        "final_qc": final_qc,
        "data_dictionary": build_data_dictionary(final_df),
        "merge_diagnostics": merge_diag,
        "source_file_hashes": {
            "aggTrades": build_source_manifest(
                trade_files or [],
            ),
            "metrics": build_source_manifest(
                metric_files or [],
            ),
            "fundingRate": build_source_manifest(
                funding_files or [],
            ),
            "liquidationSnapshot": build_source_manifest(
                liquidation_files or [],
            ),
        },
        "final_types": (
            {c: str(dt) for c, dt in final_df.dtypes.items()}
            if final_df is not None else {}
        ),
        "merge_qc": {
            "rows_trades": int(len(trades_df)),
            "rows_metrics": (
                int(len(metrics_df))
                if metrics_df is not None else 0
            ),
            "rows_funding": (
                int(len(funding_df))
                if funding_df is not None else 0
            ),
            "rows_oi_api": (
                int(len(oi_api_df)) if oi_api_df is not None else 0
            ),
            "rows_liquidation": (
                int(len(liquidation_df)) if liquidation_df is not None else 0
            ),
            "rows_final": int(len(final_df)),
            "coverage_ratio": (
                float(len(final_df) / len(trades_df))
                if trades_df is not None and len(trades_df) > 0 else None
            ),
            "first_timestamp": (
                None if final_df.empty
                else str(final_df.index.min())
            ),
            "last_timestamp": (
                None if final_df.empty
                else str(final_df.index.max())
            ),
            "columns": list(final_df.columns),
        },
        "feature_nan_rates": compute_non_null_stats(
            final_df,
            [
                "sum_open_interest",
                "count_long_short_ratio",
                "funding_rate", "oi_change_pct",
                "ls_ratio_pct_change",
                "funding_rate_change", "ret_1p",
                "vol_parkinson", "vol_garman_klass",
                "oi_api_5m", "oi_api_5m_change_pct",
                "liquidations_count", "total_liq_notional",
                "liq_imbalance",
                "ofi_imbalance_ratio_50", "ofi_imbalance_ratio_100",
                "amihud_illiquidity_24", "amihud_illiquidity_72",
                "cvd_z_1d", "cvd_divergence_1d",
                "data_quality_flag", "data_gap_suspect",
                "oi_stale_flag", "funding_stale_flag",
                "funding_spike_flag",
                "funding_spike_mild_flag", "funding_spike_strong_flag",
                "funding_spike_extreme_flag", "funding_spike_duration",
                "funding_spike_pos_flag", "funding_spike_neg_flag",
                "funding_regime_change", "funding_observed_interval_hours",
                "target_triple_barrier",
            ],
        ),
        "sparse_feature_alerts": sparse_alerts,
    }

    report["funding_spike_count"] = int(
        pd.to_numeric(final_df.get("funding_spike_flag"), errors="coerce").fillna(0).sum()
    ) if "funding_spike_flag" in final_df.columns else 0

    report["funding_source_frequency"] = infer_series_event_frequency(
        funding_df["funding_source_timestamp"]
        if funding_df is not None and "funding_source_timestamp" in funding_df.columns
        else None
    )
    observed_mode = report["funding_source_frequency"].get("mode_hours")
    api_interval = None
    try:
        api_interval = int((funding_info or {}).get("funding_interval_hours"))
    except Exception:
        api_interval = None
    observed_unique_hours = []
    if funding_df is not None and not funding_df.empty and "funding_observed_interval_hours" in funding_df.columns:
        observed_unique_hours = sorted({
            float(x) for x in pd.to_numeric(funding_df["funding_observed_interval_hours"], errors="coerce").dropna().round(3).tolist()
        })
    report["funding_frequency_awareness"] = {
        "api_interval_hours": api_interval,
        "observed_mode_hours": observed_mode,
        "observed_unique_hours": observed_unique_hours,
        "funding_regime_counts": (
            funding_df["funding_regime"].value_counts(dropna=False).to_dict()
            if funding_df is not None and not funding_df.empty and "funding_regime" in funding_df.columns
            else {}
        ),
        "possible_mismatch": bool(
            api_interval is not None and observed_mode is not None
            and abs(float(api_interval) - float(observed_mode)) > 0.25
        ),
        "mixed_observed_regime": bool(len(observed_unique_hours) > 1),
    }

    report["feature_usage_notes"] = {
        "cvd": (
            "Le cvd brut est un cumul local à la fenêtre extraite. "
            "Pour le ML/backtest, privilégier cvd_delta_* ou cvd_z_1d "
            "plutôt que le niveau absolu. L'export final l'exclut par défaut."
        ),
        "ofi_imbalance_ratio": (
            "ofi_imbalance_ratio_* est un proxy temporel de déséquilibre de flux (OFI), "
            "pas un VPIN académique à volume buckets constants. Une version "
            "future volume-buckets devra être calculée avant agrégation temporelle."
        ),
        "target_triple_barrier": (
            "target_triple_barrier est un label offline avec look-ahead assumé. "
            "Mode ohlc_intrabar_conservative (détection high/low). "
            "En cas de double touche TP/SL dans une même bougie, la convention prudente "
            "retient SL en premier pour éviter un biais optimiste. "
            "Ne jamais l'utiliser comme feature live/backtest. Exclure toute "
            "colonne target_* du chargeur de modèle si le dataset est réutilisé."
        ),
        "timestamp_semantics": (
            "La ligne horodatée t correspond à une fin de barre (end-of-bar). "
            "Les agrégats trades, metrics et funding sont alignés causalement sur la clôture "
            "de l'intervalle. Pour le ML, utiliser la ligne t pour prédire t+1 ou plus, "
            "pas pour simuler une décision prise au début de la même barre."
        ),
        "stale_flags": (
            "oi_stale_flag et funding_stale_flag signalent des périodes "
            "où ces séries restent inchangées sur une fenêtre glissante "
            "d'environ 1 jour de barres."
        ),
        "funding_spike_flag": (
            "funding_spike_flag correspond au niveau mild. Le pipeline ajoute aussi "
            "funding_spike_strong_flag, funding_spike_extreme_flag, funding_spike_pos_flag, "
            "funding_spike_neg_flag et funding_spike_duration."
        ),
        "funding_regime": (
            "funding_regime synthétise la cadence funding observée (1h/4h/mixed) et "
            "funding_regime_change marque les changements de régime détectés."
        ),
        "liquidation_side_convention": (
            "Les agrégats de liquidations reposent sur la convention Binance Vision "
            "observée dans les snapshots. Vérifier la sémantique de side si Binance "
            "fait évoluer son schéma."
        ),
    }
    report["timestamp_semantics"] = {
        "dataset_index": "end_of_bar",
        "trades_alignment": "end_of_bar",
        "metrics_alignment": "end_of_bar",
        "funding_alignment": "end_of_bar",
        "recommended_prediction_usage": "use_bar_t_to_predict_bar_t_plus_1_or_later",
    }
    report["triple_barrier_label_mode"] = "ohlc_intrabar_conservative"

    integrity = []
    freq_awareness = report.get("funding_frequency_awareness", {})
    if freq_awareness.get("possible_mismatch"):
        integrity.append(
            "WARNING: possible mismatch entre la fréquence funding observée et funding_interval_hours API"
        )
    for name, qc in [
        ("trades", trades_qc), ("metrics", metrics_qc),
        ("funding", funding_qc), ("final", final_qc),
    ]:
        cov = qc.get("coverage_ratio")
        if cov is not None and pd.notna(cov) and cov < 0.98:
            integrity.append(
                f"Couverture {name} faible: {cov:.2%}"
            )
    for col in [
        "sum_open_interest", "count_long_short_ratio",
        "funding_rate",
    ]:
        if col in final_df.columns:
            s = pd.to_numeric(final_df[col], errors="coerce")
            if s.notna().mean() < 0.05:
                integrity.append(
                    f"Colonne {col} quasi vide "
                    f"dans le dataset final"
                )
            elif s.nunique(dropna=True) <= 1 and len(s) >= 10:
                integrity.append(
                    f"Colonne {col} quasi constante "
                    f"dans le dataset final"
                )
    report["data_integrity"] = integrity
    return report


# ============================================================
# PATCH 1 — MICROSTRUCTURE / ORDER-FLOW (ajouté tel quel)
# ============================================================



def _infer_large_trade_rolling_window(freq: str) -> int:
    freq = str(freq or "").lower()
    mapping = {
        "1min": 5000,
        "5min": 3000,
        "15min": 2000,
        "1h": 1500,
        "4h": 1000,
        "1d": 500,
    }
    return int(mapping.get(freq, 2000))


def _past_only_large_trade_threshold(notional: pd.Series, freq: str, q: float) -> pd.Series:
    notional = pd.to_numeric(notional, errors="coerce").astype(float)
    q = float(q)
    window = max(50, _infer_large_trade_rolling_window(freq))
    min_periods = min(window, 50)
    shifted = notional.shift(1)
    roll_q = shifted.rolling(window=window, min_periods=min_periods).quantile(q)
    expand_q = shifted.expanding(min_periods=2).quantile(q)
    return roll_q.combine_first(expand_q)


def build_nan_coverage_report(
    df_before_dropna: pd.DataFrame,
    df_after_dropna: pd.DataFrame,
    dropna_subset: Optional[List[str]] = None,
) -> Dict[str, object]:
    report: Dict[str, object] = {
        "rows_before_dropna": int(len(df_before_dropna)),
        "rows_after_dropna": int(len(df_after_dropna)),
        "rows_dropped_by_final_dropna": int(max(0, len(df_before_dropna) - len(df_after_dropna))),
        "pct_rows_dropped_by_final_dropna": float(
            100.0 * max(0, len(df_before_dropna) - len(df_after_dropna)) / max(1, len(df_before_dropna))
        ),
        "dropna_subset": list(dropna_subset or []),
        "feature_nan_stats": [],
    }
    if df_before_dropna is None or df_before_dropna.empty:
        return report

    rows = int(len(df_before_dropna))
    first_valid_map: Dict[str, Optional[str]] = {}
    feature_stats: List[Dict[str, object]] = []
    for col in df_before_dropna.columns:
        s = df_before_dropna[col]
        nan_count = int(s.isna().sum())
        first_valid = getattr(s.first_valid_index(), "isoformat", lambda: None)()
        stat = {
            "feature": str(col),
            "nan_count": nan_count,
            "nan_pct": float(100.0 * nan_count / max(1, rows)),
            "non_na_count": int(rows - nan_count),
            "first_valid_index": first_valid,
        }
        feature_stats.append(stat)
        first_valid_map[str(col)] = first_valid
    feature_stats.sort(key=lambda x: (-x["nan_pct"], x["feature"]))
    report["feature_nan_stats"] = feature_stats
    report["top_20_most_sparse_features"] = feature_stats[:20]
    report["features_with_any_nan"] = int(sum(1 for x in feature_stats if x["nan_count"] > 0))
    report["features_complete_no_nan"] = int(sum(1 for x in feature_stats if x["nan_count"] == 0))
    return report


def export_nan_coverage_report(
    run_dir: Path,
    symbol: str,
    df_before_dropna: pd.DataFrame,
    df_after_dropna: pd.DataFrame,
    dropna_subset: Optional[List[str]] = None,
) -> Tuple[Path, Path, Dict[str, object]]:
    report = build_nan_coverage_report(df_before_dropna, df_after_dropna, dropna_subset=dropna_subset)
    json_path = run_dir / f"{symbol}_nan_report.json"
    csv_path = run_dir / f"{symbol}_nan_report.csv"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    pd.DataFrame(report.get("feature_nan_stats", [])).to_csv(csv_path, index=False, encoding="utf-8")
    return json_path, csv_path, report

ML_NORMALIZATION_EXCLUDE_EXACT = {
    "symbol", "market", "target_triple_barrier", "timestamp",
}
ML_NORMALIZATION_EXCLUDE_PREFIXES = ("target_",)

def _eligible_ml_normalization_columns(df: pd.DataFrame) -> List[str]:
    cols: List[str] = []
    for c in df.columns:
        if c in ML_NORMALIZATION_EXCLUDE_EXACT:
            continue
        if any(str(c).startswith(pref) for pref in ML_NORMALIZATION_EXCLUDE_PREFIXES):
            continue
        if not pd.api.types.is_numeric_dtype(df[c]):
            continue
        cols.append(c)
    return cols

def build_ml_ready_normalized_dataset(
    df: pd.DataFrame,
    mode: str = "rolling_zscore",
    window: int = 252,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    if df is None or df.empty:
        return pd.DataFrame(index=getattr(df, "index", None)), {
            "enabled": False,
            "normalized_columns": 0,
            "mode": mode,
            "window": int(max(2, window or 2)),
        }

    window = int(max(2, window or 2))
    mode = str(mode or "rolling_zscore").lower()
    out = df.copy(deep=False)
    eligible = _eligible_ml_normalization_columns(out)
    added_cols: List[str] = []

    for c in eligible:
        s = pd.to_numeric(out[c], errors="coerce").astype(float)
        shifted = s.shift(1)
        if mode == "expanding_zscore":
            mean = shifted.expanding(min_periods=20).mean()
            std = shifted.expanding(min_periods=20).std(ddof=0)
        else:
            mean = shifted.rolling(window=window, min_periods=min(20, window)).mean()
            std = shifted.rolling(window=window, min_periods=min(20, window)).std(ddof=0)
        std = std.replace(0.0, np.nan)
        z = (s - mean) / std
        out[f"{c}_ml_z"] = z.astype(np.float32)
        added_cols.append(f"{c}_ml_z")

    meta = {
        "enabled": True,
        "mode": mode,
        "window": window,
        "normalized_source_columns": len(eligible),
        "normalized_columns": len(added_cols),
        "normalized_columns_preview": added_cols[:25],
    }
    return out, meta


def build_microstructure_features_from_trades(
    trades_df: pd.DataFrame,
    freq: str,
    large_trade_quantile: float = 0.90,
) -> pd.DataFrame:
    """
    Reconstruit des features de microstructure à partir des aggTrades.
    Compatible avec un pipeline monolithique V7.x.

    Hypothèses attendues dans trades_df :
    - index datetime OU colonne 'timestamp'
    - colonnes 'price', 'qty', 'is_buyer_maker'
    """
    if trades_df is None or trades_df.empty:
        return pd.DataFrame()

    df = trades_df.copy(deep=False)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp").set_index("timestamp")
    else:
        if not isinstance(df.index, pd.DatetimeIndex):
            return pd.DataFrame()
        df = df.sort_index()

    if "price" not in df.columns or "qty" not in df.columns or "is_buyer_maker" not in df.columns:
        return pd.DataFrame()

    df["price"] = safe_to_numeric(df["price"], errors="coerce")
    df["qty"] = safe_to_numeric(df["qty"], errors="coerce")
    df["is_buyer_maker"] = normalize_bool_series(df["is_buyer_maker"])

    df = df.dropna(subset=["price", "qty"])
    if df.empty:
        return pd.DataFrame()

    df["notional"] = df["price"] * df["qty"]

    # Sens agressif : buyer maker=True => sell agressif
    df["trade_sign"] = np.where(df["is_buyer_maker"] == True, -1.0, 1.0)
    df["signed_qty"] = df["qty"] * df["trade_sign"]
    df["signed_notional"] = df["notional"] * df["trade_sign"]

    # Gros trades définis uniquement à partir du passé (anti look-ahead)
    past_threshold = _past_only_large_trade_threshold(df["notional"], freq=freq, q=large_trade_quantile)
    large_mask = past_threshold.notna() & (df["notional"] >= past_threshold)
    df["is_large_trade"] = large_mask.astype("float64")
    df["large_trade_notional"] = df["notional"].where(large_mask, 0.0)
    df["large_signed_notional"] = df["signed_notional"].where(large_mask, 0.0)

    rule = freq

    out = df.resample(rule).agg(
        trade_count=("qty", "size"),
        volume=("qty", "sum"),
        notional=("notional", "sum"),
        signed_qty=("signed_qty", "sum"),
        signed_notional=("signed_notional", "sum"),
        avg_trade_size=("qty", "mean"),
        median_trade_size=("qty", "median"),
        avg_trade_notional=("notional", "mean"),
        large_trade_count=("is_large_trade", "sum"),
        large_trade_notional=("large_trade_notional", "sum"),
        large_signed_notional=("large_signed_notional", "sum"),
    )

    out["buy_aggr_qty"] = df["qty"].where(df["trade_sign"] > 0, 0.0).resample(rule).sum()
    out["sell_aggr_qty"] = df["qty"].where(df["trade_sign"] < 0, 0.0).resample(rule).sum()

    out["buy_aggr_notional"] = df["notional"].where(df["trade_sign"] > 0, 0.0).resample(rule).sum()
    out["sell_aggr_notional"] = df["notional"].where(df["trade_sign"] < 0, 0.0).resample(rule).sum()

    out["trade_intensity"] = out["trade_count"]
    out["flow_imbalance_ratio"] = safe_divide(
        out["buy_aggr_qty"] - out["sell_aggr_qty"],
        out["buy_aggr_qty"] + out["sell_aggr_qty"],
    )

    out["notional_imbalance_ratio"] = safe_divide(
        out["buy_aggr_notional"] - out["sell_aggr_notional"],
        out["buy_aggr_notional"] + out["sell_aggr_notional"],
    )

    out["large_trade_ratio"] = safe_divide(out["large_trade_count"], out["trade_count"])

    out["cvd"] = out["signed_qty"].cumsum()
    out["cvd_notional"] = out["signed_notional"].cumsum()
    out["cvd_change"] = out["cvd"].diff()
    out["cvd_notional_change"] = out["cvd_notional"].diff()

    roll = 24
    out["cvd_zscore_24"] = (
        (out["cvd_change"] - out["cvd_change"].rolling(roll, min_periods=5).mean())
        / out["cvd_change"].rolling(roll, min_periods=5).std()
    )

    out["trade_intensity_z_24"] = (
        (out["trade_intensity"] - out["trade_intensity"].rolling(roll, min_periods=5).mean())
        / out["trade_intensity"].rolling(roll, min_periods=5).std()
    )

    out["ofi_proxy"] = out["signed_notional"]
    out["ofi_proxy_z_24"] = (
        (out["ofi_proxy"] - out["ofi_proxy"].rolling(roll, min_periods=5).mean())
        / out["ofi_proxy"].rolling(roll, min_periods=5).std()
    )

    return out


# ============================================================
# PATCH 2 — OPEN INTEREST ENRICHED / RÉGIMES OI (ajouté tel quel)
# ============================================================

def build_oi_regime_features(
    df: pd.DataFrame,
    oi_col: str = "open_interest",
    price_col: str = "close",
    roll: int = 24,
) -> pd.DataFrame:
    """
    Features OI enrichies + régimes simples.
    Attend un DataFrame déjà resamplé à la fréquence de travail.
    """
    if df is None or df.empty:
        return pd.DataFrame(index=getattr(df, "index", None))

    out = pd.DataFrame(index=df.index)

    if oi_col not in df.columns:
        return out

    oi = safe_to_numeric(df[oi_col], errors="coerce")
    out["oi"] = oi
    out["oi_delta"] = oi.diff()
    out["oi_delta_pct"] = oi.pct_change(fill_method=None)
    out["oi_acceleration"] = out["oi_delta"].diff()

    out["oi_z_24"] = (
        (oi - oi.rolling(roll, min_periods=5).mean())
        / oi.rolling(roll, min_periods=5).std()
    )

    out["oi_delta_z_24"] = (
        (out["oi_delta"] - out["oi_delta"].rolling(roll, min_periods=5).mean())
        / out["oi_delta"].rolling(roll, min_periods=5).std()
    )

    if price_col in df.columns:
        px = safe_to_numeric(df[price_col], errors="coerce")
        out["price_ret_1"] = px.pct_change(fill_method=None)

        price_up = out["price_ret_1"] > 0
        price_down = out["price_ret_1"] < 0
        oi_up = out["oi_delta"] > 0
        oi_down = out["oi_delta"] < 0

        out["oi_build_up_long"] = (price_up & oi_up).astype("float64")
        out["oi_build_up_short"] = (price_down & oi_up).astype("float64")
        out["oi_short_covering"] = (price_up & oi_down).astype("float64")
        out["oi_long_unwind"] = (price_down & oi_down).astype("float64")

        out["price_oi_divergence"] = np.where(
            out["price_ret_1"].notna() & out["oi_delta_pct"].notna(),
            np.sign(out["price_ret_1"]) * np.sign(out["oi_delta_pct"]),
            np.nan,
        )

    out["oi_shock"] = (
        out["oi_delta_z_24"].abs() >= 2.0
    ).astype("float64")

    if "oi_api_source_period" in df.columns:
        out["oi_source_is_fallback"] = (df["oi_api_source_period"].astype("string") == "metrics_fallback").astype("float64")

    return out


# ============================================================
# PATCH 3 — RÉGIMES DE MARCHÉ (ajouté tel quel)
# ============================================================

def build_market_regime_features(
    df: pd.DataFrame,
    price_col: str = "close",
    high_col: str = "high",
    low_col: str = "low",
    volume_col: str = "qty",
    roll_fast: int = 12,
    roll_slow: int = 48,
    roll_vol: int = 24,
) -> pd.DataFrame:
    """
    Détection simple de régimes de marché.
    Compatible V7.x, sans dépendance externe.

    PATCH V7.8:
    - remplace les z-scores calculés à la main par rolling_zscore()
    - évite les divisions instables sur variance quasi nulle
    - conserve des features plus propres pour le ML
    """
    if df is None or df.empty or price_col not in df.columns:
        return pd.DataFrame(index=getattr(df, "index", None))

    out = pd.DataFrame(index=df.index)

    close = safe_to_numeric(df[price_col], errors="coerce")
    ret1 = close.pct_change(fill_method=None)

    out["ret_1"] = ret1
    out["ma_fast"] = close.rolling(roll_fast, min_periods=3).mean()
    out["ma_slow"] = close.rolling(roll_slow, min_periods=5).mean()

    out["trend_strength"] = np.where(
        out["ma_slow"].abs() > 0,
        (out["ma_fast"] - out["ma_slow"]) / out["ma_slow"],
        np.nan,
    )

    out["volatility_24"] = ret1.rolling(roll_vol, min_periods=5).std()
    out["ret_z_24"] = rolling_zscore(ret1, roll_vol, min_periods=5)

    if high_col in df.columns and low_col in df.columns:
        high = safe_to_numeric(df[high_col], errors="coerce")
        low = safe_to_numeric(df[low_col], errors="coerce")
        prev_close = close.shift(1)

        tr = pd.concat(
            [
                (high - low).abs(),
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)

        out["atr_24"] = tr.rolling(roll_vol, min_periods=5).mean()
        out["atr_norm"] = np.where(close.abs() > 0, out["atr_24"] / close, np.nan)
        out["atr_norm_z_24"] = rolling_zscore(out["atr_norm"], roll_vol, min_periods=5)

    if volume_col in df.columns:
        vol = safe_to_numeric(df[volume_col], errors="coerce")
        out["volume_z_24"] = rolling_zscore(vol, roll_vol, min_periods=5)

    # Régimes simples
    out["regime_trend"] = (
        out["trend_strength"].abs() > 0.003
    ).astype("float64")

    out["regime_mean_revert"] = (
        (out["trend_strength"].abs() <= 0.003) &
        (out["ret_z_24"].abs() >= 1.0)
    ).astype("float64")

    out["regime_high_vol"] = (
        out["volatility_24"] >
        out["volatility_24"].rolling(roll_slow, min_periods=10).median()
    ).astype("float64")

    out["regime_low_vol"] = (
        out["volatility_24"] <=
        out["volatility_24"].rolling(roll_slow, min_periods=10).median()
    ).astype("float64")

    out["regime_shock"] = (
        out["ret_z_24"].abs() >= 2.5
    ).astype("float64")

    # Code synthétique de régime
    # 0 = neutre
    # 1 = trend
    # 2 = mean revert
    # 3 = high vol
    # 4 = shock
    regime_code = np.zeros(len(out), dtype="float64")
    regime_code = np.where(out["regime_trend"] == 1.0, 1.0, regime_code)
    regime_code = np.where(out["regime_mean_revert"] == 1.0, 2.0, regime_code)
    regime_code = np.where(out["regime_high_vol"] == 1.0, 3.0, regime_code)
    regime_code = np.where(out["regime_shock"] == 1.0, 4.0, regime_code)
    out["market_regime_code"] = regime_code

    return out


# ============================================================
# AMIHUD ILLIQUIDITY (PATCH 1 — remplace le faux Kyle Lambda)
# ============================================================

def compute_amihud_illiquidity(
    close: pd.Series,
    volume: pd.Series,
    windows=(20, 50),
) -> pd.DataFrame:
    """
    Proxy d'illiquidité type Amihud : |return| / volume
    Remplace l'ancien compute_kyle_lambda (qui était en réalité un proxy Amihud).
    """
    out = pd.DataFrame(index=close.index)

    ret = np.log(close).diff()
    vol = pd.to_numeric(volume, errors="coerce").replace(0, np.nan)

    base = ret.abs() / vol

    for w in windows:
        out[f"amihud_illiquidity_{w}"] = base.rolling(w, min_periods=max(5, w // 5)).mean()

    return out


# ============================================================
# MAIN PIPELINE (intégration des patches sans toucher à la logique existante)
# ============================================================

def _run_pipeline(args: argparse.Namespace) -> Dict[str, object]:
    configure_thread_api_limits(args)
    run_ts = _run_timestamp()

    symbol = args.symbol.upper()
    base_url = BASE_URLS[args.market]
    cache_key = make_cache_key(args)
    exchange_info = precheck_symbol_or_raise(symbol, args.market)
    funding_info = fetch_funding_rate_info(symbol, args.market)

    start_day = ensure_utc_timestamp(args.start).normalize()
    end_day = ensure_utc_timestamp(args.end).normalize()
    start_dt = start_day
    end_dt = (
        end_day
        + pd.Timedelta(days=1)
        - pd.Timedelta(milliseconds=1)
    )
    symbol_period_info = assess_symbol_period_status(exchange_info, start_dt, end_dt)
    if symbol_period_info.get("symbol_status") in {"future_listing", "delisted", "delisted_before_period"}:
        msg = (
            f"Symbole {symbol} incompatible avec la période demandée: "
            f"{symbol_period_info.get('symbol_status')}"
        )
        if args.skip_delisted:
            raise SkipSymbolError(msg)
        raise ValueError(msg)
    if symbol_period_info.get("symbol_status") in {"partial", "partial_listing", "partial_delisting", "partial_listing_and_delisting"}:
        msg = (
            f"Symbole {symbol} partiel sur la période demandée "
            f"(listing/delisting/settlement détecté)"
        )
        if args.skip_delisted:
            raise SkipSymbolError(msg)
        log(msg, level="WARNING")

    run_dir = (
        Path(args.out_dir)
        / f"{symbol}_{args.freq}_{args.start}_{args.end}"
          f"_{cache_key}"
    )
    cache_dir = Path(args.cache_dir) / cache_key
    partial_trade_dir = run_dir / "partials" / "trades"
    partial_metric_dir = run_dir / "partials" / "metrics"
    partial_funding_dir = run_dir / "partials" / "funding"
    partial_liquidation_dir = run_dir / "partials" / "liquidations"
    for d in (
        run_dir, cache_dir,
        partial_trade_dir, partial_metric_dir,
        partial_funding_dir, partial_liquidation_dir,
    ):
        d.mkdir(parents=True, exist_ok=True)

    if bool(getattr(args, "log_per_symbol", False)):
        symbol_log_path = Path(args.out_dir) / "logs" / f"{symbol}.log"
        setup_logger(symbol_log_path)
        log(f"Logging fichier symbole activé: {symbol_log_path}")

    csv_suffix = ".csv.gz" if args.gzip_csv else ".csv"
    exp_csv = (
        run_dir
        / f"{symbol}_dataset_quant_{args.freq}{csv_suffix}"
    )
    exp_parquet = (
        run_dir
        / f"{symbol}_dataset_quant_{args.freq}.parquet"
    )
    exp_report = run_dir / f"{symbol}_report.json"
    if (
        exp_csv.exists()
        and exp_parquet.exists()
        and exp_report.exists()
        and not args.force_reprocess
        and not args.force_redownload
    ):
        log("=" * 52)
        log(f"Binance Vision → Dataset Quant {PIPELINE_VERSION}")
        log("=" * 52)
        log(f"Résultat existant: {run_dir}")
        log(
            "Utilise --force_reprocess ou --force_redownload "
            "pour forcer."
        )
        log("=" * 52)
        return {
            "symbol": symbol,
            "status": "cached",
            "run_dir": str(run_dir),
            "report_json": str(exp_report),
            "rows_final": None,
            "symbol_status": symbol_period_info.get("symbol_status"),
            "export_status": "cached",
        }

    log("=" * 52)
    log(f"Binance Vision → Dataset Quant {PIPELINE_VERSION}")
    log("=" * 52)
    log(f"Symbol      : {symbol}")
    log(f"Marché      : {args.market}")
    log(f"Granularité : {args.freq}")
    log(f"Début       : {args.start}")
    log(f"Fin         : {args.end}")
    log(f"Sortie      : {run_dir}")
    log(f"Cache       : {cache_dir}")
    log(f"Workers DL  : {args.max_download_workers}")

    trade_type, metric_type, funding_type = (
        detect_supported_types(args.market)
    )
    liquidation_type = (
        "liquidationSnapshot"
        if args.include_liquidations and args.market != "spot"
        else None
    )

    # ── Téléchargement ──────────────────────────────────────
    trade_files, trade_dl_stats = fetch_periods_for_type(
        base_url, trade_type, symbol,
        start_day, end_day, cache_dir,
        args.force_redownload, args.max_download_workers,
    )
    if not trade_files:
        raise ValueError(
            "Aucun fichier aggTrades sur la plage demandée"
        )

    metric_files: List[Path] = []
    metric_dl_stats: Dict[str, object] = {}
    if metric_type is not None:
        metric_files, metric_dl_stats = fetch_periods_for_type(
            base_url, metric_type, symbol,
            start_day, end_day, cache_dir,
            args.force_redownload, args.max_download_workers,
        )
    else:
        log("Spot: pas de metrics Vision.", level="WARNING")

    funding_files: List[Path] = []
    funding_dl_stats: Dict[str, object] = {}
    if funding_type is not None:
        funding_files, funding_dl_stats = (
            fetch_periods_for_type(
                base_url, funding_type, symbol,
                start_day, end_day, cache_dir,
                args.force_redownload, args.max_download_workers,
            )
        )
    else:
        log("Spot: pas de funding Vision.", level="WARNING")

    liquidation_files: List[Path] = []
    liquidation_dl_stats: Dict[str, object] = {}
    if liquidation_type is not None:
        liquidation_files, liquidation_dl_stats = fetch_periods_for_type(
            base_url, liquidation_type, symbol,
            start_day, end_day, cache_dir,
            args.force_redownload, args.max_download_workers,
        )

    # ── Prétraitement partiels ──────────────────────────────
    trade_partials = []
    for f in tqdm(sorted(trade_files), desc="Trades"):
        try:
            p = write_trade_partial(
                f, partial_trade_dir, args.freq,
                start_dt, end_dt, args.chunksize,
                args.engine, args.force_reprocess,
                args.downcast_float32,
            )
            if p is not None:
                trade_partials.append(p)
        except Exception as e:
            log(
                f"Erreur trades {f.name}: {e}",
                level="WARNING",
            )

    metric_partials = []
    first_metric_error = None
    for f in tqdm(sorted(metric_files), desc="Metrics"):
        try:
            p = write_metric_partial(
                f, partial_metric_dir, args.freq,
                start_dt, end_dt, args.engine,
                args.force_reprocess, args.downcast_float32,
            )
            if p is not None:
                metric_partials.append(p)
        except Exception as e:
            if first_metric_error is None:
                first_metric_error = (f.name, str(e))
            log(
                f"Erreur metrics {f.name}: {e}",
                level="WARNING",
            )

    funding_partials = []
    first_funding_error = None
    for f in tqdm(sorted(funding_files), desc="Funding"):
        try:
            p = write_funding_partial(
                f, partial_funding_dir, args.freq,
                start_dt, end_dt, args.engine,
                args.force_reprocess, args.downcast_float32,
            )
            if p is not None:
                funding_partials.append(p)
        except Exception as e:
            if first_funding_error is None:
                first_funding_error = (f.name, str(e))
            log(
                f"Erreur funding {f.name}: {e}",
                level="WARNING",
            )

    if (
        metric_files
        and not metric_partials
        and not list(partial_metric_dir.glob("*.parquet"))
    ):
        detail = (
            f"{first_metric_error[0]} → "
            f"{first_metric_error[1]}"
            if first_metric_error
            else "aucun partial produit"
        )
        raise ValueError(
            f"Tous les metrics ont échoué. Premier: {detail}"
        )

    if (
        funding_files
        and not funding_partials
        and not list(partial_funding_dir.glob("*.parquet"))
    ):
        detail = (
            f"{first_funding_error[0]} → "
            f"{first_funding_error[1]}"
            if first_funding_error
            else "aucun partial produit"
        )
        raise ValueError(
            f"Tous les funding ont échoué. Premier: {detail}"
        )

    liquidation_partials = []
    for f in tqdm(sorted(liquidation_files), desc="Liquidations"):
        try:
            p = write_liquidation_partial(
                f, partial_liquidation_dir, args.freq,
                start_dt, end_dt, args.engine,
                args.force_reprocess, args.downcast_float32,
            )
            if p is not None:
                liquidation_partials.append(p)
        except Exception as e:
            log(f"Erreur liquidation {f.name}: {e}", level="WARNING")

    # ── Combine partiels ────────────────────────────────────
    tp = (
        sorted(set(trade_partials))
        if trade_partials
        else sorted(partial_trade_dir.glob("*.parquet"))
    )
    mp = (
        sorted(set(metric_partials))
        if metric_partials
        else sorted(partial_metric_dir.glob("*.parquet"))
    )
    fp = (
        sorted(set(funding_partials))
        if funding_partials
        else sorted(partial_funding_dir.glob("*.parquet"))
    )
    lp = (
        sorted(set(liquidation_partials))
        if liquidation_partials
        else sorted(partial_liquidation_dir.glob("*.parquet"))
    )

    trades_df = load_and_combine_trade_partials(
        tp, args.downcast_float32,
    )
    metrics_df = load_and_combine_metric_partials(
        mp, args.downcast_float32,
    )
    funding_df = load_and_combine_funding_partials(
        fp, args.downcast_float32,
    )
    funding_df = annotate_funding_regime(
        funding_df, funding_info, args.downcast_float32,
    )
    if funding_df is not None and not funding_df.empty:
        funding_df = resample_funding_adaptive(
            funding_df,
            args.freq,
            funding_df["funding_regime"]
            if "funding_regime" in funding_df.columns
            else None,
        )
        if not funding_df.empty and "timestamp" in funding_df.columns:
            funding_df = funding_df.set_index("timestamp").sort_index()
        funding_df = maybe_downcast(
            funding_df, args.downcast_float32,
        )
    liquidation_df = load_and_combine_liquidation_partials(
        lp, args.downcast_float32,
    )
    oi_api_raw_df = pd.DataFrame()
    oi_api_df = pd.DataFrame()
    oi_api_stats: Dict[str, object] = {"enabled": bool(args.include_oi_5m_api)}

    if args.include_oi_5m_api and args.market != "spot":
        try:
            oi_api_raw_df, oi_api_stats = fetch_open_interest_hist_multi_period(
                symbol=symbol,
                market=args.market,
                start_dt=start_dt,
                end_dt=end_dt,
                periods=["5m", "15m", "30m", "1h", "4h", "1d"],
            )

            if oi_api_raw_df is None or oi_api_raw_df.empty:
                fallback_oi = build_oi_from_metrics_fallback(metrics_df)
                if fallback_oi is not None and not fallback_oi.empty:
                    oi_api_raw_df = fallback_oi
                    oi_api_stats["fallback_from_metrics"] = True
                    oi_api_stats["selected_period"] = "metrics_fallback"
                    oi_api_stats["rows_raw"] = int(len(oi_api_raw_df))

            oi_api_df = resample_oi_api_frame(
                oi_api_raw_df,
                args.freq,
                args.downcast_float32,
            )
            oi_api_stats.update({
                "enabled": True,
                "rows_resampled": int(len(oi_api_df)),
                "integrated": int(not oi_api_df.empty),
            })

        except Exception as e:
            oi_api_stats = {"enabled": True, "integrated": 0, "error": str(e)}
            log(f"Erreur OI API multi-period: {e}", level="WARNING")

    debug_frame_snapshot("trades_df", trades_df)
    debug_frame_snapshot("metrics_df", metrics_df)
    debug_frame_snapshot("funding_df", funding_df)
    debug_frame_snapshot("liquidation_df", liquidation_df)

    aux_df = combine_auxiliary_frames(
        metrics_df, funding_df, oi_api_df, liquidation_df,
        args.downcast_float32,
    )

    debug_frame_snapshot("oi_api_raw_df", oi_api_raw_df)
    debug_frame_snapshot("oi_api_df", oi_api_df)
    debug_frame_snapshot("aux_df", aux_df)

    # Vérification colonnes critiques
    for col in REQUIRED_METRIC_OUTPUT_COLS:
        if (
            col in metrics_df.columns
            and pd.to_numeric(
                metrics_df[col], errors="coerce",
            ).notna().mean() < 0.05
        ):
            raise ValueError(
                f"Colonne metric critique quasi vide "
                f"après combine: {col}"
            )

    # ── PATCH 1 INTÉGRATION (après trades_df + avant merge final) ──
    trades_raw_df = trades_df
    micro_df = build_microstructure_features_from_trades(trades_raw_df, args.freq)
    debug_frame_snapshot("micro_df", micro_df)

    if micro_df is not None and not micro_df.empty:
        aux_df = safe_merge_asof(
            aux_df,
            micro_df,
            on="timestamp",
            suffix="_micro",
            tolerance=pd.Timedelta("7D"),
        )

    # ── Merge ───────────────────────────────────────────────
    final_df = merge_trades_and_metrics(
        trades_df, aux_df, args.metrics_merge,
        args.max_metrics_staleness, args.freq,
    )
    merge_diag = final_df.attrs.get("merge_diagnostics", {})
    if merge_diag:
        log(
            f"Merge: mode={merge_diag.get('mode')} "
            f"rows_with_aux="
            f"{merge_diag.get('rows_with_any_aux')} "
            f"rows_without="
            f"{merge_diag.get('rows_without_aux')}"
        )

    # ── PATCH 2 INTÉGRATION (après merge OI dans final_df) ──
    oi_regime_df = build_oi_regime_features(
        final_df,
        oi_col="oi_api_5m" if "oi_api_5m" in final_df.columns else "sum_open_interest",
        price_col="close",
        roll=24,
    )
    debug_frame_snapshot("oi_regime_df", oi_regime_df)

    for c in oi_regime_df.columns:
        if c not in final_df.columns:
            final_df[c] = oi_regime_df[c]

    # ── Features ────────────────────────────────────────────
    final_df.attrs["funding_spike_abs_threshold"] = float(args.funding_spike_abs_threshold)
    final_df.attrs["funding_spike_rel_multiplier"] = float(args.funding_spike_rel_multiplier)
    if funding_info:
        final_df.attrs["funding_interval_hours"] = funding_info.get("funding_interval_hours")
    final_df = add_quant_features(final_df, args.freq)
    final_df = add_v62_microstructure_features(
        final_df,
        include_labels=args.include_offline_labels,
    )

    # ── PATCH 3 INTÉGRATION (juste avant export) ──
    market_regime_df = build_market_regime_features(
        final_df,
        price_col="close",
        high_col="high",
        low_col="low",
        volume_col="volume",  # colonne existante dans trades_df
        roll_fast=12,
        roll_slow=48,
        roll_vol=24,
    )
    debug_frame_snapshot("market_regime_df", market_regime_df)

    for c in market_regime_df.columns:
        if c not in final_df.columns:
            final_df[c] = market_regime_df[c]

    final_df = maybe_downcast(final_df, args.downcast_float32)

    debug_frame_snapshot("final_df_pre_export", final_df)
    final_df_before_dropna = final_df.copy(deep=False)
    dropna_subset: List[str] = []

    if args.dropna_final and not final_df.empty:
        before = len(final_df)
        dropna_subset = infer_dropna_subset(final_df)
        if dropna_subset:
            final_df = final_df.dropna(subset=dropna_subset).copy()
            log(
                "Dropna final intelligent "
                f"(subset={dropna_subset}): {before} → {len(final_df)}"
            )
        else:
            log(
                "Dropna final ignoré: aucune colonne critique disponible",
                level="WARNING",
            )

    # ── Validation ──────────────────────────────────────────
    violations = run_post_build_validation(
        final_df, args.freq,
    )
    for msg in violations:
        level = (
            "ERROR" if msg.startswith("CRITICAL")
            else "WARNING"
        )
        log(msg, level=level)
    if any(v.startswith("CRITICAL") for v in violations):
        log("Invariants CRITICAL détectés — export autorisé en mode dégradé", level="WARNING")
        report["post_build_validation_degraded_export"] = True
        report["export_status"] = "degraded"

    target_cols_present = [
        c for c in final_df.columns
        if isinstance(c, str) and c.startswith("target_")
    ]
    if target_cols_present and not args.allow_target_columns_on_export:
        log(
            "Target columns detected and automatically excluded from export "
            f"to prevent look-ahead leakage: {target_cols_present}",
            level="WARNING",
        )
    if args.allow_target_columns_on_export and target_cols_present:
        log(
            "⚠️  Target columns export enabled. These columns use future "
            "information (look-ahead) and must NEVER be used in backtesting "
            "or live trading.",
            level="WARNING",
        )

    export_final_df = sanitize_export_dataset(
        final_df,
        allow_target_columns_on_export=args.allow_target_columns_on_export,
        keep_cvd_raw_on_export=args.keep_cvd_raw_on_export,
    )

    ml_ready_df = None
    ml_ready_meta = {"enabled": False, "mode": None, "window": None, "normalized_columns": 0}
    if bool(getattr(args, "emit_ml_ready_normalized", False)):
        try:
            ml_ready_df, ml_ready_meta = build_ml_ready_normalized_dataset(
                export_final_df,
                mode=getattr(args, "ml_normalization_mode", "rolling_zscore"),
                window=int(getattr(args, "ml_normalization_window", 252) or 252),
            )
            log(
                f"Export ML-ready normalisé préparé: {ml_ready_meta.get('normalized_columns', 0)} colonnes normalisées",
            )
        except Exception as e:
            ml_ready_df = None
            ml_ready_meta = {"enabled": False, "error": str(e), "mode": getattr(args, "ml_normalization_mode", None), "window": getattr(args, "ml_normalization_window", None), "normalized_columns": 0}
            log(f"Export ML-ready non généré: {e}", level="WARNING")

    # ── Report ──────────────────────────────────────────────
    exchange_info = {**(exchange_info or {}), **symbol_period_info}
    report = build_report(
        args=args,
        cache_key=cache_key,
        run_ts=run_ts,
        trade_dl_stats=trade_dl_stats,
        metric_dl_stats=metric_dl_stats,
        funding_dl_stats=funding_dl_stats,
        oi_api_stats=oi_api_stats,
        liquidation_dl_stats=liquidation_dl_stats,
        exchange_info=exchange_info,
        funding_info=funding_info,
        trades_df=trades_df,
        metrics_df=metrics_df,
        funding_df=funding_df,
        oi_api_df=oi_api_df,
        liquidation_df=liquidation_df,
        final_df=final_df,
        trade_partial_count=len(
            list(partial_trade_dir.glob("*.parquet"))
        ),
        metric_partial_count=len(
            list(partial_metric_dir.glob("*.parquet"))
        ),
        funding_partial_count=len(
            list(partial_funding_dir.glob("*.parquet"))
        ),
        liquidation_partial_count=len(
            list(partial_liquidation_dir.glob("*.parquet"))
        ),
        trade_files=trade_files,
        metric_files=metric_files,
        funding_files=funding_files,
        liquidation_files=liquidation_files,
    )
    report["export_target_columns_excluded"] = not bool(
        args.allow_target_columns_on_export
    )
    report["nan_report_enabled"] = bool(getattr(args, "nan_report", False))
    report["nan_report_json"] = str(nan_report_json) if nan_report_json else None
    report["nan_report_csv"] = str(nan_report_csv) if nan_report_csv else None
    report["log_per_symbol"] = bool(getattr(args, "log_per_symbol", False))
    report["dropna_final_subset"] = dropna_subset
    if isinstance(nan_report, dict):
        report["nan_summary"] = {
            "rows_before_dropna": nan_report.get("rows_before_dropna"),
            "rows_after_dropna": nan_report.get("rows_after_dropna"),
            "rows_dropped_by_final_dropna": nan_report.get("rows_dropped_by_final_dropna"),
            "pct_rows_dropped_by_final_dropna": nan_report.get("pct_rows_dropped_by_final_dropna"),
            "features_with_any_nan": nan_report.get("features_with_any_nan"),
        }
    report["export_keep_cvd_raw"] = bool(args.keep_cvd_raw_on_export)
    report["target_columns_present"] = target_cols_present
    report["target_columns_exported"] = bool(args.allow_target_columns_on_export)
    report["api_rate_backend"] = str(getattr(args, "api_rate_backend", "sqlite"))
    report["api_rate_sqlite_timeout"] = float(getattr(args, "api_rate_sqlite_timeout", 60.0))
    report["emit_ml_ready_normalized"] = bool(getattr(args, "emit_ml_ready_normalized", False))
    report["ml_ready_normalization"] = ml_ready_meta
    for msg in report.get("data_integrity", []):
        log(msg, level="WARNING")

    # ── Export ──────────────────────────────────────────────
    compression = "gzip" if args.gzip_csv else None
    trades_csv = (
        run_dir / f"{symbol}_trades_{args.freq}{csv_suffix}"
    )
    metrics_csv = (
        run_dir / f"{symbol}_metrics_{args.freq}{csv_suffix}"
    )
    funding_csv = (
        run_dir / f"{symbol}_funding_{args.freq}{csv_suffix}"
    )
    final_csv = (
        run_dir
        / f"{symbol}_dataset_quant_{args.freq}{csv_suffix}"
    )
    ml_ready_csv = (
        run_dir
        / f"{symbol}_dataset_quant_{args.freq}_ml_ready{csv_suffix}"
    )
    report_json = run_dir / f"{symbol}_report.json"
    dd_json = run_dir / f"{symbol}_data_dictionary.json"

    nan_report_json = None
    nan_report_csv = None
    nan_report = None
    if bool(getattr(args, "nan_report", False)):
        try:
            nan_report_json, nan_report_csv, nan_report = export_nan_coverage_report(
                run_dir=run_dir,
                symbol=symbol,
                df_before_dropna=final_df_before_dropna,
                df_after_dropna=final_df,
                dropna_subset=dropna_subset,
            )
            log(f"NaN report exporté: {nan_report_json}")
        except Exception as e:
            log(f"NaN report non exporté: {e}", level="WARNING")

    trades_df.to_csv(trades_csv, compression=compression)
    metrics_df.to_csv(metrics_csv, compression=compression)
    funding_df.to_csv(funding_csv, compression=compression)
    export_final_df.to_csv(final_csv, compression=compression)
    if ml_ready_df is not None and not ml_ready_df.empty:
        ml_ready_df.to_csv(ml_ready_csv, compression=compression)
    trades_df.to_parquet(
        run_dir / f"{symbol}_trades_{args.freq}.parquet"
    )
    metrics_df.to_parquet(
        run_dir / f"{symbol}_metrics_{args.freq}.parquet"
    )
    funding_df.to_parquet(
        run_dir / f"{symbol}_funding_{args.freq}.parquet"
    )
    export_final_df.to_parquet(
        run_dir
        / f"{symbol}_dataset_quant_{args.freq}.parquet"
    )
    if ml_ready_df is not None and not ml_ready_df.empty:
        ml_ready_df.to_parquet(
            run_dir
            / f"{symbol}_dataset_quant_{args.freq}_ml_ready.parquet"
        )
    with open(report_json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    with open(dd_json, "w", encoding="utf-8") as f:
        json.dump(
            report.get("data_dictionary", []),
            f, indent=2, ensure_ascii=False,
        )

    manifest = {
        "pipeline_version": PIPELINE_VERSION,
        "run_timestamp_utc": run_ts,
        "cache_key": cache_key,
        "symbol": symbol,
        "market": args.market,
        "freq": args.freq,
        "start": args.start,
        "end": args.end,
        "trade_partials": len(
            list(partial_trade_dir.glob("*.parquet"))
        ),
        "metric_partials": len(
            list(partial_metric_dir.glob("*.parquet"))
        ),
        "funding_partials": len(
            list(partial_funding_dir.glob("*.parquet"))
        ),
        "final_rows": int(len(export_final_df)),
        "target_columns_excluded_on_export": not bool(
            args.allow_target_columns_on_export
        ),
        "api_rate_backend": str(getattr(args, "api_rate_backend", "sqlite")),
        "api_rate_sqlite_timeout": float(getattr(args, "api_rate_sqlite_timeout", 60.0)),
        "emit_ml_ready_normalized": bool(getattr(args, "emit_ml_ready_normalized", False)),
        "ml_ready_normalization": ml_ready_meta,
    }
    with open(
        run_dir / "manifest.json", "w", encoding="utf-8",
    ) as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    if not args.keep_extracted:
        deleted = cleanup_extracted_cache(cache_dir, symbol)
        log(
            f"Nettoyage cache extracted: {deleted} dossier(s) "
            f"supprimé(s)"
        )

    log("-" * 52)
    log(f"Trades CSV : {trades_csv}")
    log(f"Metrics CSV: {metrics_csv}")
    log(f"Funding CSV: {funding_csv}")
    log(f"Final CSV  : {final_csv}")
    if ml_ready_df is not None and not ml_ready_df.empty:
        log(f"ML-ready CSV: {ml_ready_csv}")
    log(f"Report JSON: {report_json}")
    log(f"Data dict  : {dd_json}")
    log(f"Shape finale: {final_df.shape}")
    if not final_df.empty:
        log(
            f"Période: {final_df.index.min()} → "
            f"{final_df.index.max()}"
        )
    log("=" * 52)
    return {
        "symbol": symbol,
        "status": "ok",
        "run_dir": str(run_dir),
        "report_json": str(report_json),
        "nan_report_json": str(nan_report_json) if nan_report_json else None,
        "nan_report_csv": str(nan_report_csv) if nan_report_csv else None,
        "symbol_log": str((Path(args.out_dir) / "logs" / f"{symbol}.log")) if bool(getattr(args, "log_per_symbol", False)) else None,
        "rows_final": int(len(export_final_df)),
        "ml_ready_rows_final": int(len(ml_ready_df)) if isinstance(ml_ready_df, pd.DataFrame) else None,
        "ml_ready_csv": str(ml_ready_csv) if isinstance(ml_ready_df, pd.DataFrame) and not ml_ready_df.empty else None,
        "symbol_status": symbol_period_info.get("symbol_status"),
    }


def save_batch_checkpoint(batch_root: Path, batch_idx: int, results_so_far: List[Dict[str, object]]) -> Path:
    checkpoint_path = batch_root / f"checkpoint_batch_{batch_idx}.json"
    tmp_path = checkpoint_path.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(results_so_far, f, indent=2, ensure_ascii=False)
    tmp_path.replace(checkpoint_path)
    log(f"Checkpoint {batch_idx} sauvegardé ({len(results_so_far)} résultats): {checkpoint_path}")
    return checkpoint_path


def load_existing_batch_results(batch_root: Path) -> List[Dict[str, object]]:
    results: List[Dict[str, object]] = []
    for cp in sorted(batch_root.glob("checkpoint_batch_*.json")):
        try:
            data = json.loads(cp.read_text(encoding="utf-8"))
            if isinstance(data, list):
                results = data
        except Exception as e:
            log(f"Checkpoint ignoré {cp.name}: {e}", level="WARNING")
    return results


def add_batch_global_validation(summary: Dict[str, object], results: List[Dict[str, object]]) -> Dict[str, object]:
    if not results:
        return summary

    coverage_ratios: List[float] = []
    rows_counts: List[float] = []
    funding_spike_counts: List[int] = []
    ok_count = skipped_count = error_count = 0

    for r in results:
        r = normalize_batch_result(r)
        status = r.get("status")
        if status in {"ok", "cached"}:
            ok_count += 1
        elif status == "skipped":
            skipped_count += 1
        elif status == "error":
            error_count += 1

        if status not in {"ok", "cached"}:
            continue

        report_path = r.get("report_json")
        if not report_path:
            run_dir = r.get("run_dir")
            symbol = r.get("symbol")
            if run_dir and symbol:
                report_path = str(Path(run_dir) / f"{symbol}_report.json")
        if not report_path:
            continue
        try:
            with open(report_path, "r", encoding="utf-8") as f:
                rep = json.load(f)
            qc = rep.get("merge_qc", {})
            cov = qc.get("coverage_ratio")
            if cov is not None:
                coverage_ratios.append(float(cov))
            rows = qc.get("rows_final")
            if rows is not None:
                rows_counts.append(float(rows))
            funding_spike_counts.append(int(rep.get("funding_spike_count", 0) or 0))
        except Exception as e:
            log(f"Erreur lecture report {report_path}: {e}", level="WARNING")
            continue

    summary["global_validation"] = {
        "symbols_ok_or_cached": ok_count,
        "symbols_skipped": skipped_count,
        "symbols_error": error_count,
        "mean_coverage_ratio": float(np.nanmean(coverage_ratios)) if coverage_ratios else None,
        "median_coverage_ratio": float(np.nanmedian(coverage_ratios)) if coverage_ratios else None,
        "min_coverage_ratio": float(np.nanmin(coverage_ratios)) if coverage_ratios else None,
        "symbols_with_low_coverage_lt_0_80": int(sum(x < 0.80 for x in coverage_ratios if pd.notna(x))),
        "mean_rows_final": float(np.nanmean(rows_counts)) if rows_counts else None,
        "symbols_with_zero_rows": int(sum(x == 0 for x in rows_counts)),
        "total_funding_spike_events": int(sum(funding_spike_counts)) if funding_spike_counts else 0,
    }
    return summary


def run_multi_symbol_pipeline(args: argparse.Namespace) -> Dict[str, object]:
    symbols = list(getattr(args, "symbols_resolved", []) or [])
    if not symbols:
        raise ValueError("Aucun symbole résolu pour le mode batch")
    multi = len(symbols) > 1
    batch_root = Path(args.out_dir)
    if multi:
        batch_root = batch_root / f"batch_{pd.Timestamp.now(tz='UTC').strftime('%Y%m%dT%H%M%SZ')}"
        batch_root.mkdir(parents=True, exist_ok=True)

    results: List[Dict[str, object]] = load_existing_batch_results(batch_root) if batch_root.exists() else []
    done_symbols = {
        str(r.get("symbol", "")).upper()
        for r in results
        if r.get("status") in {"ok", "cached"}
    }
    if done_symbols:
        log(f"Reprise batch: {len(done_symbols)} symbole(s) déjà traités seront ignorés")

    bs = max(1, int(args.batch_size))
    pending = [s for s in symbols if s.upper() not in done_symbols]
    total = len(pending)
    for start_i in range(0, total, bs):
        batch_symbols = pending[start_i:start_i + bs]
        batch_idx = start_i // bs + 1
        log(f"Batch {batch_idx}: {batch_symbols}")
        if args.parallel and len(batch_symbols) > 1:
            max_workers = args.parallel_workers or min(len(batch_symbols), bs, max(1, (os.cpu_count() or 1)))
            max_workers = max(1, max_workers)
            if getattr(args, "parallel_backend", "process") == "process":
                db_path = initialize_cross_process_rate_limiter(getattr(args, "api_rate_limit_db", None))
                setattr(args, "api_rate_limit_db", db_path)
                log(f"Rate limiter cross-process activé: {db_path}")
            executor_cls = ProcessPoolExecutor if getattr(args, "parallel_backend", "process") == "process" else ThreadPoolExecutor
            with executor_cls(max_workers=max_workers) as ex:
                fut_map = {
                    ex.submit(_run_pipeline, clone_args_for_symbol(args, sym, str(batch_root))): sym
                    for sym in batch_symbols
                }
                for fut in as_completed(fut_map):
                    sym = fut_map[fut]
                    try:
                        res = fut.result()
                        results.append(normalize_batch_result(res, sym))
                    except SkipSymbolError as e:
                        log(f"Symbole ignoré {sym}: {e}", level="WARNING")
                        results.append({"symbol": sym, "status": "skipped", "reason": str(e)})
                    except Exception as e:
                        log(f"Échec symbole {sym}: {e}", level="ERROR")
                        results.append({"symbol": sym, "status": "error", "reason": str(e)})
                    finally:
                        gc.collect()
        else:
            for sym in batch_symbols:
                try:
                    res = _run_pipeline(clone_args_for_symbol(args, sym, str(batch_root)))
                    results.append(normalize_batch_result(res, sym))
                except SkipSymbolError as e:
                    log(f"Symbole ignoré {sym}: {e}", level="WARNING")
                    results.append({"symbol": sym, "status": "skipped", "reason": str(e)})
                except Exception as e:
                    log(f"Échec symbole {sym}: {e}", level="ERROR")
                    results.append({"symbol": sym, "status": "error", "reason": str(e)})
                finally:
                    gc.collect()
        if multi:
            save_batch_checkpoint(batch_root, batch_idx, results)

    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "run_timestamp_utc": _run_timestamp(),
        "market": args.market,
        "freq": args.freq,
        "start": args.start,
        "end": args.end,
        "symbols_requested": symbols,
        "symbols_pending_after_resume": pending,
        "batch_size": int(args.batch_size),
        "parallel": bool(args.parallel),
        "parallel_workers": int(args.parallel_workers or 0),
        "skip_delisted": bool(args.skip_delisted),
        "results": results,
        "counts": {
            "ok": sum(1 for r in results if r.get("status") in {"ok", "cached"}),
            "cached": sum(1 for r in results if r.get("status") == "cached"),
            "skipped": sum(1 for r in results if r.get("status") == "skipped"),
            "error": sum(1 for r in results if r.get("status") == "error"),
        },
    }
    summary = add_batch_global_validation(summary, results)
    if multi:
        summary_path = batch_root / "batch_report.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        log(f"Batch report: {summary_path}")
    return summary


def main() -> None:
    setup_logger()
    args = parse_args()
    validate_args(args)
    try:
        run_multi_symbol_pipeline(args)
    except KeyboardInterrupt:
        log("Interruption utilisateur", level="WARNING")
        cleanup_cross_process_rate_limiter(getattr(args, "api_rate_limit_db", None), force=True)
        raise SystemExit(130)
    except ValueError as e:
        log(f"Erreur: {e}", level="ERROR")
        cleanup_cross_process_rate_limiter(getattr(args, "api_rate_limit_db", None), force=True)
        raise SystemExit(1)
    except Exception as e:
        log(f"Erreur inattendue: {e}", level="ERROR")
        cleanup_cross_process_rate_limiter(getattr(args, "api_rate_limit_db", None), force=True)
        raise SystemExit(1)
    finally:
        cleanup_cross_process_rate_limiter(getattr(args, "api_rate_limit_db", None), force=False)


# ============================================================
# V62 EXTENSIONS — ADVANCED MICROSTRUCTURE & LABELING
# ============================================================

# build_volume_bars / build_imbalance_bars retirées en V62.4
# car non branchées au pipeline principal et source de dette technique.
def compute_vpin(df: pd.DataFrame, window: int = 50) -> pd.Series:
    if not {"buy_vol", "sell_vol"}.issubset(df.columns):
        return pd.Series(index=df.index, dtype="float64")
    buy = pd.to_numeric(df["buy_vol"], errors="coerce")
    sell = pd.to_numeric(df["sell_vol"], errors="coerce")
    imbalance = (buy - sell).abs()
    total = (buy + sell).replace(0, np.nan)
    vpin = (
        imbalance.rolling(window, min_periods=window).sum()
        / total.rolling(window, min_periods=window).sum()
    )
    return vpin.astype("float64")


def triple_barrier_label_ohlc(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    horizon: int = 24,
    pt: float = 0.03,
    sl: float = 0.02,
) -> pd.Series:
    """
    Label triple barrier robuste utilisant high/low intrabar.
    +1 si TP touché avant SL
    -1 si SL touché avant TP
     0 sinon à l'horizon
    (PATCH V7.9.3 — fin de série blindée, horizon réel par ligne)
    """
    close = pd.to_numeric(close, errors="coerce")
    high = pd.to_numeric(high, errors="coerce")
    low = pd.to_numeric(low, errors="coerce")

    n = len(close)
    labels = np.full(n, np.nan, dtype=float)
    if n == 0:
        return pd.Series(labels, index=close.index, name="target_triple_barrier")

    horizon = int(max(1, horizon))
    close_arr = close.to_numpy(dtype=float, copy=False)
    high_arr = high.to_numpy(dtype=float, copy=False)
    low_arr = low.to_numpy(dtype=float, copy=False)

    try:
        from numpy.lib.stride_tricks import sliding_window_view
    except Exception:
        sliding_window_view = None

    def _fallback_loop() -> pd.Series:
        for i in range(n):
            p0 = close_arr[i]
            if not np.isfinite(p0):
                continue
            actual_horizon = min(horizon, n - i - 1)
            if actual_horizon <= 0:
                labels[i] = 0.0
                continue
            up_barrier = p0 * (1.0 + pt)
            dn_barrier = p0 * (1.0 - sl)
            end = i + actual_horizon
            hit_label = 0.0
            for j in range(i + 1, end + 1):
                h = high_arr[j]
                l = low_arr[j]
                hit_tp = np.isfinite(h) and h >= up_barrier
                hit_sl = np.isfinite(l) and l <= dn_barrier
                if hit_tp and hit_sl:
                    hit_label = -1.0
                    break
                elif hit_tp:
                    hit_label = 1.0
                    break
                elif hit_sl:
                    hit_label = -1.0
                    break
            if hit_label == 0.0 and np.isfinite(close_arr[end]):
                final_ret = close_arr[end] / p0 - 1.0
                hit_label = 1.0 if final_ret > 0 else (-1.0 if final_ret < 0 else 0.0)
            labels[i] = hit_label
        return pd.Series(labels, index=close.index, name="target_triple_barrier")

    if sliding_window_view is None or n <= 1:
        return _fallback_loop()

    max_fw = min(horizon, max(1, n - 1))
    if max_fw <= 0:
        labels[:] = 0.0
        return pd.Series(labels, index=close.index, name="target_triple_barrier")

    pad = np.full(max_fw, np.nan, dtype=float)
    high_pad = np.concatenate([high_arr, pad])
    low_pad = np.concatenate([low_arr, pad])
    close_pad = np.concatenate([close_arr, pad])

    high_fw = sliding_window_view(high_pad[1:], max_fw)
    low_fw = sliding_window_view(low_pad[1:], max_fw)
    close_fw = sliding_window_view(close_pad[1:], max_fw)

    block_size = 50000
    for start in range(0, n, block_size):
        end = min(n, start + block_size)
        p0 = close_arr[start:end]
        valid_p0 = np.isfinite(p0)
        if not valid_p0.any():
            continue

        actual_horizons = np.minimum(horizon, n - 1 - np.arange(start, end)).astype(int)
        tradable = valid_p0 & (actual_horizons > 0)
        if not tradable.any():
            labels[start:end] = np.where(valid_p0, 0.0, np.nan)
            continue

        high_block = high_fw[start:end]
        low_block = low_fw[start:end]
        close_block = close_fw[start:end]
        col_idx = np.arange(max_fw, dtype=int)[None, :]
        valid_future_mask = col_idx < actual_horizons[:, None]

        up_barrier = p0 * (1.0 + pt)
        dn_barrier = p0 * (1.0 - sl)

        hit_tp = valid_future_mask & np.isfinite(high_block) & (high_block >= up_barrier[:, None])
        hit_sl = valid_future_mask & np.isfinite(low_block) & (low_block <= dn_barrier[:, None])

        any_tp = hit_tp.any(axis=1)
        any_sl = hit_sl.any(axis=1)
        tp_idx = np.where(any_tp, hit_tp.argmax(axis=1), max_fw)
        sl_idx = np.where(any_sl, hit_sl.argmax(axis=1), max_fw)

        block_labels = np.zeros(end - start, dtype=float)
        block_labels[(any_tp) & (tp_idx < sl_idx)] = 1.0
        block_labels[(any_sl) & (sl_idx <= tp_idx)] = -1.0

        unresolved = tradable & ~(any_tp | any_sl)
        if unresolved.any():
            last_idx = np.maximum(actual_horizons - 1, 0)
            row_idx = np.arange(end - start)
            last_close = close_block[row_idx, last_idx]
            final_ret = np.full(end - start, np.nan, dtype=float)
            final_ret[unresolved] = safe_divide(
                pd.Series(last_close[unresolved]),
                pd.Series(p0[unresolved])
            ).to_numpy(dtype=float, copy=False) - 1.0
            block_labels[unresolved & np.isfinite(final_ret) & (final_ret > 0)] = 1.0
            block_labels[unresolved & np.isfinite(final_ret) & (final_ret < 0)] = -1.0

        block_labels[valid_p0 & (actual_horizons <= 0)] = 0.0
        labels[start:end] = np.where(valid_p0, block_labels, np.nan)

    return pd.Series(labels, index=close.index, name="target_triple_barrier")


def add_v62_microstructure_features(
    df: pd.DataFrame,
    include_labels: bool = False,
) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy(deep=False)

    # PATCH 6 : nomenclature + PATCH 1 : Amihud
    df["ofi_imbalance_ratio_50"] = compute_vpin(df, 50)
    df["ofi_imbalance_ratio_100"] = compute_vpin(df, 100)

    amihud_df = compute_amihud_illiquidity(df["close"], df["volume"], windows=(24, 72))
    df["amihud_illiquidity_24"] = amihud_df["amihud_illiquidity_24"]
    df["amihud_illiquidity_72"] = amihud_df["amihud_illiquidity_72"]

    if include_labels and {"close", "high", "low"}.issubset(df.columns):
        df["target_triple_barrier"] = triple_barrier_label_ohlc(
            df["close"], df["high"], df["low"]
        )

    return df


if __name__ == "__main__":
    main()