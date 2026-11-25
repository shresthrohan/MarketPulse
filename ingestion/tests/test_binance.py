"""Binance adapter without network, using the Finnhub tests' fake websocket and asyncio.sleep recorder."""

import json
import logging
import uuid

import pytest
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

from ingestion.providers import binance as bn
from ingestion.providers.session import SessionBook
from ingestion.tests.test_finnhub import FakeWS, collect

# Captured from the live combined stream (q/V/Q were elided; the provider does not read them).
SAMPLE = ('{"stream":"btcusdt@kline_1s","data":{"e":"kline","E":1795582486077,"s":"BTCUSDT","k":{'
          '"t":1795582485000,"T":1795582485999,"s":"BTCUSDT","i":"1s","f":6909495408,"L":6909495412,'
          '"o":"59102.29000000","c":"59102.30000000","h":"59102.30000000","l":"59102.29000000",'
          '"v":"0.00012000","n":5,"x":true,"q":"7.09227600","V":"0.00008000","Q":"4.72818400","B":"0"}}}')
URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1s/ethusdt@kline_1s"
MIDNIGHT = 1795651200000  # 2026-11-26T00:00:00Z in ms


def kline(t, o, h, lo, c, v="1.5", n=3, x=True, **extra):
    k = {"t": t, "T": t + 999, "s": "BTCUSDT", "i": "1s", "o": str(o), "h": str(h), "l": str(lo), "c": str(c),
         "v": str(v), "n": n, "x": x, **extra}
    return json.dumps({"stream": "btcusdt@kline_1s", "data": {"e": "kline", "E": t + 1077, "s": "BTCUSDT", "k": k}})


def provider(*sessions):
    """Each connect() takes the next session (frames, or an exception the handshake raises); the last repeats."""
    conns, calls = [], []

    def connect(url, **kw):
        calls.append((url, kw))
        conns.append(FakeWS(sessions[min(len(conns), len(sessions) - 1)]))
        return conns[-1]

    return bn.BinanceProvider(["BTCUSDT", "ETHUSDT"], connect=connect), conns, calls


def test_real_kline_becomes_a_live_event_with_a_stable_id(monkeypatch):
    e = bn.parse_frame(SAMPLE, SessionBook())
    assert e == {"event_id": e["event_id"], "symbol": "BTCUSDT", "timestamp": "2026-11-25T04:54:45.999+00:00",
                 "price": 59102.3, "volume": 0.00012, "open": 59102.29, "high": 59102.3, "low": 59102.29,
                 "close": 59102.3, "source": "binance", "data_mode": "live", "schema_version": 1}
    assert uuid.UUID(e["event_id"]).version == 5 and isinstance(e["volume"], float)
    runs = []
    for _ in range(2):  # two providers (= a restart or reconnect) map the same kline to the same event
        p, _, _ = provider([SAMPLE])
        runs.append(collect(p, monkeypatch, items=1).items)
        assert p.status == "streaming"
    assert runs == [[e], [e]]


def test_empty_and_unclosed_seconds_are_skipped():
    book = SessionBook()
    assert bn.parse_frame(kline(MIDNIGHT, 100, 100, 100, 100, v="0.00000000", n=0, f=-1), book) is None
    assert bn.parse_frame(kline(MIDNIGHT, 101, 105, 99, 104, x=False), book) is None
    e = bn.parse_frame(kline(MIDNIGHT, 101, 103, 100, 102), book)
    assert (e["open"], e["high"], e["low"], e["price"]) == (101, 103, 100, 102)  # skipped ones left no trace


def test_session_ohlc_spans_the_utc_day_and_rolls_over_at_midnight():
    book = SessionBook()
    frames = [kline(MIDNIGHT - 2000, 100, 101, 99.5, 100.5), kline(MIDNIGHT - 1000, 100.5, 102, 100, 101),
              kline(MIDNIGHT, 101.2, 101.5, 100.8, 101.4), kline(MIDNIGHT + 1000, 101.4, 101.6, 101, 101.1)]
    events = [bn.parse_frame(f, book) for f in frames]
    assert [(e["timestamp"], e["open"], e["high"], e["low"], e["close"]) for e in events] == [
        ("2026-11-25T23:59:58.999+00:00", 100, 101, 99.5, 100.5),
        ("2026-11-25T23:59:59.999+00:00", 100, 102, 99.5, 101),
        ("2026-11-26T00:00:00.999+00:00", 101.2, 101.5, 100.8, 101.4),  # new UTC day opens at that kline's o
        ("2026-11-26T00:00:01.999+00:00", 101.2, 101.6, 100.8, 101.1)]


def test_malformed_frames_are_forwarded_as_bytes_and_the_connection_is_kept(monkeypatch):
    no_close = json.loads(kline(MIDNIGHT, 1, 1, 1, 1))
    del no_close["data"]["k"]["c"]
    bad = [b"\x00garbage", '"a string"', '{"stream":"btcusdt@kline_1s"}', json.dumps(no_close),
           kline(MIDNIGHT, 100, 101, 0, 100)]  # a zero low would poison the session
    frames = [kline(MIDNIGHT, 100, 101, 99, 100.5), *bad, kline(MIDNIGHT + 1000, 100.5, 102, 100, 101)]
    p, conns, _ = provider(frames)
    run = collect(p, monkeypatch, sleeps=1)
    assert len(conns) == 1  # no reconnect before the session ended
    assert run.items[1:-1] == [f if isinstance(f, bytes) else f.encode() for f in bad]
    assert [(e["price"], e["low"]) for e in (run.items[0], run.items[-1])] == [(100.5, 99), (101, 99)]


def test_connection_failure_backs_off_and_reconnects_to_the_combined_stream(monkeypatch):
    refused = OSError("connection refused")
    p, _, calls = provider(refused, refused, [kline(MIDNIGHT, 100, 101, 99, 100.5)], refused)
    run = collect(p, monkeypatch, sleeps=4)
    assert run.statuses == ["reconnecting (OSError)"] * 2 + ["reconnecting (ConnectionError)", "reconnecting (OSError)"]
    assert 0.5 <= run.delays[0] <= 1 and 1 <= run.delays[1] <= 2
    assert 0.5 <= run.delays[2] <= 1  # reset by the successful connection
    assert [e["price"] for e in run.items] == [100.5]
    assert [url for url, _ in calls] == [URL] * 4
    kw = calls[0][1]
    assert (kw["open_timeout"], kw["ping_interval"], kw["max_size"]) == (15, 20, 2**20)
    assert not kw["logger"].isEnabledFor(logging.DEBUG)
    assert bn.stream_url("wss://data-stream.binance.vision/", ["SOLUSDT"]) == (
        "wss://data-stream.binance.vision/stream?streams=solusdt@kline_1s")


@pytest.mark.parametrize("code", [451, 403])
def test_geo_blocked_handshake_reports_blocked_with_max_backoff(monkeypatch, code):
    p, _, _ = provider(InvalidStatus(Response(code, "Unavailable For Legal Reasons", Headers())))
    run = collect(p, monkeypatch, sleeps=2)
    assert run.delays == [60, 60] and run.statuses == [f"blocked (HTTP {code}): set BINANCE_WS_URL"] * 2
