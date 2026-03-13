from __future__ import annotations

import json
from dataclasses import dataclass

from binance_quant_builder.downloader import fetch_aggtrades_dataframe


@dataclass
class FakeResponse:
    payload: list[dict]

    def json(self):
        return self.payload

    def raise_for_status(self):
        return None


class DenseWindowSession:
    def __init__(self):
        self.calls = []

    def get(self, url, timeout=60, params=None):
        params = dict(params or {})
        self.calls.append({"url": url, **params})
        if "fromId" in params:
            from_id = int(params["fromId"])
            if from_id == 3:
                return FakeResponse([
                    {"a": 3, "p": "102", "q": "1", "f": 3, "l": 3, "T": 2_000, "m": False, "M": True},
                    {"a": 4, "p": "103", "q": "1", "f": 4, "l": 4, "T": 3_000, "m": True, "M": True},
                ])
            return FakeResponse([])
        return FakeResponse([
            {"a": 1, "p": "100", "q": "1", "f": 1, "l": 1, "T": 0, "m": False, "M": True},
            {"a": 2, "p": "101", "q": "1", "f": 2, "l": 2, "T": 1_000, "m": True, "M": True},
        ])


class RecordingLimiter:
    def __init__(self):
        self.weights = []

    def acquire(self, weight=1):
        self.weights.append(int(weight))


def test_fetch_aggtrades_dataframe_uses_from_id_for_dense_windows():
    session = DenseWindowSession()
    limiter = RecordingLimiter()
    df, diag = fetch_aggtrades_dataframe(
        symbol="BTCUSDT",
        market="futures_um",
        start="1970-01-01T00:00:00+00:00",
        end="1970-01-01T00:00:04+00:00",
        session=session,
        rate_limiter=limiter,
        limit=2,
        return_diagnostics=True,
    )
    assert df["agg_trade_id"].tolist() == [1, 2, 3, 4]
    assert any("fromId" in call for call in session.calls)
    assert diag["windows"][0]["pagination_mode"] == "time_then_from_id"
    assert limiter.weights and all(w == 20 for w in limiter.weights)


def test_fetch_aggtrades_dataframe_returns_diagnostics_payload():
    session = DenseWindowSession()
    df, diag = fetch_aggtrades_dataframe(
        symbol="BTCUSDT",
        market="spot",
        start="1970-01-01T00:00:00+00:00",
        end="1970-01-01T00:00:04+00:00",
        session=session,
        limit=10,
        return_diagnostics=True,
    )
    assert len(df) == 2
    assert diag["request_weight"] == 4
    assert diag["rows"] == 2
    assert diag["windows"][0]["requests"] == 1
    json.dumps(diag)
