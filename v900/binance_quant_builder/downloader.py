from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .rate_limiter import RateLimiter
from .utils.time import ensure_utc_timestamp


PACKAGE_VERSION = "9.1.2"
BASE_URL_BY_MARKET = {
    "futures_um": "https://fapi.binance.com/fapi/v1/aggTrades",
    "spot": "https://api.binance.com/api/v3/aggTrades",
}
FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
OPEN_INTEREST_HIST_URL = "https://fapi.binance.com/futures/data/openInterestHist"
LIQUIDATIONS_URL = "https://fapi.binance.com/fapi/v1/allForceOrders"

FUTURES_AGGTRADES_MAX_WINDOW_MS = 55 * 60 * 1000
SPOT_AGGTRADES_MAX_WINDOW_MS = 24 * 60 * 60 * 1000
AGGTRADES_REQUEST_WEIGHT = {"futures_um": 20, "spot": 4}


def build_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": f"binance-quant-builder/{PACKAGE_VERSION}"})
    retries = Retry(total=5, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "HEAD"])
    adapter = HTTPAdapter(max_retries=retries, pool_connections=16, pool_maxsize=16)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def api_get_with_retry(
    session: requests.Session,
    url: str,
    rate_limiter: Optional[RateLimiter] = None,
    timeout: int = 60,
    max_attempts: int = 5,
    params: dict | None = None,
    request_weight: int = 1,
):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        if rate_limiter is not None:
            try:
                rate_limiter.acquire(weight=request_weight)
            except TypeError:
                rate_limiter.acquire()
        try:
            resp = session.get(url, timeout=timeout, params=params)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt >= max_attempts:
                break
            time.sleep(min(2 ** attempt, 10))
    raise RuntimeError(f"Echec GET {url}: {last_exc}")


def _to_ms(value: str) -> int:
    ts = datetime.fromisoformat(str(value))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    return int(ts.timestamp() * 1000)


def _aggtrades_window_ms(market: str) -> int:
    if market == "futures_um":
        return FUTURES_AGGTRADES_MAX_WINDOW_MS
    return SPOT_AGGTRADES_MAX_WINDOW_MS


def _aggtrades_request_weight(market: str) -> int:
    return int(AGGTRADES_REQUEST_WEIGHT.get(market, 1))


def _chunk_index_from_offset(start_ms: int, chunk_start: int, window_ms: int) -> int:
    if window_ms <= 0:
        return 0
    return max(0, int((chunk_start - start_ms) // window_ms))


def _read_trade_time(item: dict, fallback: int) -> int:
    try:
        return int(item.get("T", item.get("time", fallback)))
    except Exception:
        return int(fallback)


def _read_agg_trade_id(item: dict) -> int | None:
    value = item.get("a")
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _empty_aggtrade_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "timestamp", "is_buyer_maker", "is_best_match"])


def _build_aggtrades_diagnostics(
    *,
    df: pd.DataFrame,
    start_ms: int,
    end_ms: int,
    market: str,
    window_ms: int,
    request_weight: int,
    chunk_reports: list[dict[str, Any]],
) -> dict[str, Any]:
    requested_seconds = max((end_ms - start_ms) / 1000.0, 0.0)
    diagnostics: dict[str, Any] = {
        "market": market,
        "request_weight": int(request_weight),
        "requested_start": pd.to_datetime(start_ms, unit="ms", utc=True).isoformat(),
        "requested_end": pd.to_datetime(end_ms, unit="ms", utc=True).isoformat(),
        "requested_seconds": float(requested_seconds),
        "window_ms": int(window_ms),
        "windows_requested": int(math.ceil(max(end_ms - start_ms, 0) / float(window_ms))) if window_ms > 0 else 1,
        "windows": chunk_reports,
        "rows": int(len(df)),
    }
    if df.empty:
        diagnostics.update(
            {
                "duplicate_agg_trade_ids": 0,
                "non_empty_windows": int(sum(1 for c in chunk_reports if c.get("rows", 0) > 0)),
                "largest_trade_gap_ms": None,
                "coverage_ratio": 0.0,
                "warnings": ["empty_dataset"],
            }
        )
        return diagnostics

    ts = ensure_utc_timestamp(df["timestamp"]).dropna().sort_values()
    duplicate_count = int(df["agg_trade_id"].duplicated(keep=False).sum()) if "agg_trade_id" in df.columns else 0
    observed_start = ts.iloc[0]
    observed_end = ts.iloc[-1]
    observed_seconds = max((min(observed_end, pd.to_datetime(end_ms, unit="ms", utc=True)) - max(observed_start, pd.to_datetime(start_ms, unit="ms", utc=True))).total_seconds(), 0.0)
    coverage_ratio = 1.0 if requested_seconds == 0 else max(0.0, min(1.0, observed_seconds / requested_seconds))
    largest_gap_ms = None
    if len(ts) >= 2:
        diffs = ts.diff().dropna().dt.total_seconds() * 1000.0
        if not diffs.empty:
            largest_gap_ms = float(diffs.max())

    warnings: list[str] = []
    if duplicate_count > 0:
        warnings.append("duplicate_agg_trade_ids")
    if coverage_ratio < 0.80:
        warnings.append("low_observed_coverage")
    if any(c.get("warning") for c in chunk_reports):
        warnings.append("chunk_warnings_present")

    diagnostics.update(
        {
            "observed_start": observed_start.isoformat(),
            "observed_end": observed_end.isoformat(),
            "non_empty_windows": int(sum(1 for c in chunk_reports if c.get("rows", 0) > 0)),
            "duplicate_agg_trade_ids": duplicate_count,
            "largest_trade_gap_ms": largest_gap_ms,
            "coverage_ratio": float(coverage_ratio),
            "warnings": warnings or None,
        }
    )
    return diagnostics


def _aggtrade_window_fetch(
    *,
    url: str,
    symbol: str,
    market: str,
    session: requests.Session,
    rate_limiter: Optional[RateLimiter],
    timeout: int,
    limit: int,
    start_ms: int,
    end_ms: int,
    chunk_index: int,
) -> tuple[list[dict], dict[str, Any]]:
    hard_limit = max(1, min(int(limit), 1000))
    request_weight = _aggtrades_request_weight(market)
    rows: list[dict] = []
    requests_count = 0
    pagination_mode = "time"
    stop_reason = "empty"
    stagnant_rounds = 0
    max_seen_trade_id = None

    params = {"symbol": symbol, "startTime": int(start_ms), "endTime": int(end_ms), "limit": hard_limit}
    resp = api_get_with_retry(
        session=session,
        url=url,
        rate_limiter=rate_limiter,
        timeout=timeout,
        params=params,
        request_weight=request_weight,
    )
    requests_count += 1
    payload = resp.json()
    if not payload:
        report = {
            "chunk_index": int(chunk_index),
            "start": pd.to_datetime(start_ms, unit="ms", utc=True).isoformat(),
            "end": pd.to_datetime(end_ms, unit="ms", utc=True).isoformat(),
            "rows": 0,
            "requests": requests_count,
            "pagination_mode": pagination_mode,
            "request_weight": int(request_weight),
            "stop_reason": stop_reason,
            "warning": None,
        }
        return rows, report

    for item in payload:
        trade_time = _read_trade_time(item, start_ms)
        if start_ms <= trade_time < end_ms:
            rows.append(item)
        agg_id = _read_agg_trade_id(item)
        if agg_id is not None:
            max_seen_trade_id = agg_id if max_seen_trade_id is None else max(max_seen_trade_id, agg_id)

    if len(payload) < hard_limit or max_seen_trade_id is None:
        stop_reason = "time_window_exhausted"
    else:
        pagination_mode = "time_then_from_id"
        while True:
            params = {"symbol": symbol, "fromId": int(max_seen_trade_id + 1), "limit": hard_limit}
            resp = api_get_with_retry(
                session=session,
                url=url,
                rate_limiter=rate_limiter,
                timeout=timeout,
                params=params,
                request_weight=request_weight,
            )
            requests_count += 1
            payload = resp.json()
            if not payload:
                stop_reason = "from_id_empty"
                break

            progressed = False
            reached_window_end = False
            filtered_count = 0
            for item in payload:
                agg_id = _read_agg_trade_id(item)
                if agg_id is not None and agg_id > max_seen_trade_id:
                    max_seen_trade_id = agg_id
                    progressed = True
                trade_time = _read_trade_time(item, end_ms)
                if trade_time >= end_ms:
                    reached_window_end = True
                    continue
                if trade_time < start_ms:
                    continue
                rows.append(item)
                filtered_count += 1

            if reached_window_end:
                stop_reason = "window_end_reached_via_from_id"
                break
            if len(payload) < hard_limit:
                stop_reason = "from_id_exhausted"
                break
            if not progressed:
                stagnant_rounds += 1
            else:
                stagnant_rounds = 0
            if stagnant_rounds >= 2:
                raise RuntimeError("Pagination aggTrades bloquée: le curseur fromId n'avance plus.")
            if filtered_count == 0 and not reached_window_end:
                stop_reason = "from_id_out_of_range"
                break

    timestamps = [_read_trade_time(item, start_ms) for item in rows]
    report = {
        "chunk_index": int(chunk_index),
        "start": pd.to_datetime(start_ms, unit="ms", utc=True).isoformat(),
        "end": pd.to_datetime(end_ms, unit="ms", utc=True).isoformat(),
        "rows": int(len(rows)),
        "requests": requests_count,
        "pagination_mode": pagination_mode,
        "request_weight": int(request_weight),
        "first_trade_time": pd.to_datetime(min(timestamps), unit="ms", utc=True).isoformat() if timestamps else None,
        "last_trade_time": pd.to_datetime(max(timestamps), unit="ms", utc=True).isoformat() if timestamps else None,
        "stop_reason": stop_reason,
        "warning": "dense_window_multi_page" if requests_count > 3 else None,
    }
    return rows, report


def fetch_aggtrades_dataframe(
    symbol: str,
    market: str,
    start: str,
    end: str,
    session: requests.Session,
    rate_limiter: Optional[RateLimiter] = None,
    timeout: int = 60,
    limit: int = 1000,
    return_diagnostics: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, dict[str, Any]]:
    url = BASE_URL_BY_MARKET.get(market)
    if not url:
        raise ValueError(f"Marché non supporté pour le downloader natif: {market}")

    start_ms = _to_ms(start)
    end_ms = _to_ms(end)
    rows: list[dict] = []
    chunk_reports: list[dict[str, Any]] = []
    window_ms = _aggtrades_window_ms(market)
    request_weight = _aggtrades_request_weight(market)
    chunk_start = start_ms

    while chunk_start < end_ms:
        chunk_end = min(chunk_start + window_ms, end_ms)
        chunk_rows, chunk_report = _aggtrade_window_fetch(
            url=url,
            symbol=symbol,
            market=market,
            session=session,
            rate_limiter=rate_limiter,
            timeout=timeout,
            limit=limit,
            start_ms=chunk_start,
            end_ms=chunk_end,
            chunk_index=_chunk_index_from_offset(start_ms, chunk_start, window_ms),
        )
        rows.extend(chunk_rows)
        chunk_reports.append(chunk_report)
        chunk_start = chunk_end

    normalized: list[dict] = []
    for item in rows:
        trade_time = _read_trade_time(item, start_ms)
        if trade_time < start_ms or trade_time >= end_ms:
            continue
        normalized.append(
            {
                "agg_trade_id": item.get("a"),
                "price": item.get("p"),
                "quantity": item.get("q"),
                "first_trade_id": item.get("f"),
                "last_trade_id": item.get("l"),
                "timestamp": pd.to_datetime(trade_time, unit="ms", utc=True),
                "is_buyer_maker": item.get("m"),
                "is_best_match": item.get("M"),
            }
        )

    if not normalized:
        df = _empty_aggtrade_frame()
    else:
        df = pd.DataFrame(normalized)
        df = df.sort_values(["timestamp", "agg_trade_id"]).drop_duplicates(subset=["agg_trade_id"], keep="last").reset_index(drop=True)
        df["timestamp"] = ensure_utc_timestamp(df["timestamp"])

    diagnostics = _build_aggtrades_diagnostics(
        df=df,
        start_ms=start_ms,
        end_ms=end_ms,
        market=market,
        window_ms=window_ms,
        request_weight=request_weight,
        chunk_reports=chunk_reports,
    )
    if return_diagnostics:
        return df, diagnostics
    return df


def _parse_payload_to_rows(payload) -> list[dict]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        return [payload]
    return []


def _paginate_timeboxed_rows(
    *,
    url: str,
    session: requests.Session,
    start_ms: int,
    end_ms: int,
    limit: int,
    timeout: int,
    rate_limiter: Optional[RateLimiter],
    base_params: dict | None = None,
    time_field_candidates: tuple[str, ...] = ("timestamp", "time", "fundingTime", "updateTime"),
    request_weight: int = 1,
) -> list[dict]:
    rows: list[dict] = []
    cursor = int(start_ms)
    hard_limit = max(1, int(limit))
    extra = dict(base_params or {})

    while cursor < end_ms:
        params = {**extra, "startTime": cursor, "endTime": end_ms, "limit": hard_limit}
        resp = api_get_with_retry(session=session, url=url, rate_limiter=rate_limiter, timeout=timeout, params=params, request_weight=request_weight)
        batch = _parse_payload_to_rows(resp.json())
        if not batch:
            break

        max_seen = cursor
        progressed = False
        for item in batch:
            if not isinstance(item, dict):
                continue
            item_time = None
            for field in time_field_candidates:
                value = item.get(field)
                if value is not None:
                    try:
                        item_time = int(value)
                    except Exception:
                        item_time = None
                    break
            if item_time is not None:
                if item_time < start_ms:
                    continue
                if item_time >= end_ms:
                    continue
                max_seen = max(max_seen, item_time)
                progressed = True
            rows.append(item)

        if len(batch) < hard_limit:
            break
        next_cursor = max_seen + 1 if progressed else cursor + 1
        if next_cursor <= cursor:
            break
        cursor = next_cursor

    return rows


def fetch_funding_rate_dataframe(symbol: str, start: str, end: str, session: requests.Session, rate_limiter: Optional[RateLimiter] = None, timeout: int = 60, limit: int = 1000) -> pd.DataFrame:
    rows = _paginate_timeboxed_rows(
        url=FUNDING_URL,
        session=session,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        limit=min(int(limit), 1000),
        timeout=timeout,
        rate_limiter=rate_limiter,
        base_params={"symbol": symbol},
        time_field_candidates=("fundingTime", "time", "timestamp"),
        request_weight=1,
    )
    if not rows:
        return pd.DataFrame(columns=["timestamp", "funding_rate", "mark_price"])
    df = pd.DataFrame(rows)
    if "fundingTime" in df.columns:
        df["timestamp"] = pd.to_datetime(pd.to_numeric(df["fundingTime"], errors="coerce"), unit="ms", utc=True)
    elif "time" in df.columns:
        df["timestamp"] = pd.to_datetime(pd.to_numeric(df["time"], errors="coerce"), unit="ms", utc=True)
    else:
        df["timestamp"] = ensure_utc_timestamp(df.get("timestamp"))
    df["funding_rate"] = pd.to_numeric(df.get("fundingRate"), errors="coerce")
    df["mark_price"] = pd.to_numeric(df.get("markPrice"), errors="coerce")
    return df[["timestamp", "funding_rate", "mark_price"]].dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)


def fetch_open_interest_hist_dataframe(symbol: str, start: str, end: str, period: str, session: requests.Session, rate_limiter: Optional[RateLimiter] = None, timeout: int = 60, limit: int = 500) -> pd.DataFrame:
    rows = _paginate_timeboxed_rows(
        url=OPEN_INTEREST_HIST_URL,
        session=session,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        limit=min(int(limit), 500),
        timeout=timeout,
        rate_limiter=rate_limiter,
        base_params={"symbol": symbol, "period": period},
        time_field_candidates=("timestamp", "time"),
        request_weight=1,
    )
    if not rows:
        return pd.DataFrame(columns=["timestamp", "sum_open_interest", "sum_open_interest_value", "cmc_circulating_supply", "count_toptrader_long_short_ratio"])
    df = pd.DataFrame(rows)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(pd.to_numeric(df["timestamp"], errors="coerce"), unit="ms", utc=True)
    else:
        df["timestamp"] = ensure_utc_timestamp(df.get("time"))
    df["sum_open_interest"] = pd.to_numeric(df.get("sumOpenInterest"), errors="coerce")
    df["sum_open_interest_value"] = pd.to_numeric(df.get("sumOpenInterestValue"), errors="coerce")
    df["cmc_circulating_supply"] = pd.to_numeric(df.get("CMCCirculatingSupply"), errors="coerce")
    ratio_col = None
    for candidate in ("countToptraderLongShortRatio", "count_toptrader_long_short_ratio"):
        if candidate in df.columns:
            ratio_col = candidate
            break
    df["count_toptrader_long_short_ratio"] = pd.to_numeric(df.get(ratio_col) if ratio_col else None, errors="coerce")
    keep_cols = ["timestamp", "sum_open_interest", "sum_open_interest_value", "cmc_circulating_supply", "count_toptrader_long_short_ratio"]
    return df[keep_cols].dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)


def fetch_liquidations_dataframe(symbol: str, start: str, end: str, session: requests.Session, rate_limiter: Optional[RateLimiter] = None, timeout: int = 60, limit: int = 100) -> pd.DataFrame:
    rows = _paginate_timeboxed_rows(
        url=LIQUIDATIONS_URL,
        session=session,
        start_ms=_to_ms(start),
        end_ms=_to_ms(end),
        limit=min(int(limit), 100),
        timeout=timeout,
        rate_limiter=rate_limiter,
        base_params={"symbol": symbol},
        time_field_candidates=("time", "updateTime", "timestamp"),
        request_weight=1,
    )
    if not rows:
        return pd.DataFrame(columns=["timestamp", "liquidation_side", "liquidation_price", "liquidation_qty", "liquidation_notional"])
    df = pd.DataFrame(rows)
    if "time" in df.columns:
        df["timestamp"] = pd.to_datetime(pd.to_numeric(df["time"], errors="coerce"), unit="ms", utc=True)
    elif "updateTime" in df.columns:
        df["timestamp"] = pd.to_datetime(pd.to_numeric(df["updateTime"], errors="coerce"), unit="ms", utc=True)
    else:
        df["timestamp"] = ensure_utc_timestamp(df.get("timestamp"))
    order_col = "o" if "o" in df.columns else None
    if order_col:
        df["liquidation_side"] = df[order_col].apply(lambda x: x.get("S") if isinstance(x, dict) else None)
        df["liquidation_price"] = pd.to_numeric(df[order_col].apply(lambda x: x.get("ap") if isinstance(x, dict) else None), errors="coerce")
        df["liquidation_qty"] = pd.to_numeric(df[order_col].apply(lambda x: x.get("z") if isinstance(x, dict) else None), errors="coerce")
    else:
        df["liquidation_side"] = df.get("side")
        df["liquidation_price"] = pd.to_numeric(df.get("avgPrice"), errors="coerce")
        df["liquidation_qty"] = pd.to_numeric(df.get("executedQty"), errors="coerce")
    df["liquidation_notional"] = df["liquidation_price"] * df["liquidation_qty"]
    return df[["timestamp", "liquidation_side", "liquidation_price", "liquidation_qty", "liquidation_notional"]].dropna(subset=["timestamp"]).sort_values("timestamp").drop_duplicates(subset=["timestamp", "liquidation_side", "liquidation_price", "liquidation_qty"], keep="last").reset_index(drop=True)


def save_dataframe(df: pd.DataFrame, path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.suffix.lower() == ".parquet":
        df.to_parquet(target, index=False)
    else:
        df.to_csv(target, index=False)
    return target
