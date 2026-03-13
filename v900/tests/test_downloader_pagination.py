from __future__ import annotations

from dataclasses import dataclass

from binance_quant_builder.downloader import fetch_aggtrades_dataframe, fetch_funding_rate_dataframe


@dataclass
class FakeResponse:
    payload: list[dict]

    def json(self):
        return self.payload

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, timeout=60, params=None):
        self.calls.append({"url": url, **dict(params or {})})
        start = params["startTime"]
        end = params["endTime"]
        if "fundingRate" in url:
            if start == 0:
                return FakeResponse([
                    {"fundingTime": 0, "fundingRate": "0.001", "markPrice": "100"},
                    {"fundingTime": 1, "fundingRate": "0.002", "markPrice": "101"},
                ])
            if start == 2:
                return FakeResponse([
                    {"fundingTime": 2, "fundingRate": "0.003", "markPrice": "102"},
                ])
            return FakeResponse([])
        if "aggTrades" in url:
            payload = []
            if start <= 0 < end:
                payload.append({"a": 1, "p": "100", "q": "1", "f": 1, "l": 1, "T": 0, "m": False, "M": True})
            if start <= 3_300_000 < end:
                payload.append({"a": 2, "p": "101", "q": "1.5", "f": 2, "l": 2, "T": 3_300_000, "m": True, "M": True})
            return FakeResponse(payload)
        return FakeResponse([])


def test_fetch_funding_rate_dataframe_paginates():
    session = FakeSession()
    df = fetch_funding_rate_dataframe(
        symbol="BTCUSDT",
        start="1970-01-01T00:00:00+00:00",
        end="1970-01-01T00:00:00.003+00:00",
        session=session,
        limit=2,
    )
    assert len([c for c in session.calls if "fundingRate" in c["url"]]) == 2
    assert len(df) == 3
    assert df["funding_rate"].tolist() == [0.001, 0.002, 0.003]


def test_fetch_aggtrades_dataframe_uses_timeboxed_windows_for_futures():
    session = FakeSession()
    df = fetch_aggtrades_dataframe(
        symbol="BTCUSDT",
        market="futures_um",
        start="1970-01-01T00:00:00+00:00",
        end="1970-01-01T01:10:00+00:00",
        session=session,
        limit=1000,
    )
    agg_calls = [c for c in session.calls if "aggTrades" in c["url"]]
    assert len(agg_calls) == 2
    assert agg_calls[0]["endTime"] <= 3_300_000
    assert agg_calls[1]["startTime"] == agg_calls[0]["endTime"]
    assert df["agg_trade_id"].tolist() == [1, 2]
