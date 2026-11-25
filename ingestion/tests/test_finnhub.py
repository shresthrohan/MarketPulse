"""Finnhub adapters without network: httpx.MockTransport for REST, a fake websocket for WS.

asyncio.sleep in the provider module is replaced by a recorder, so every backoff/poll wait is
observable and instant; a run ends after a fixed number of sleeps or messages.
"""

import asyncio
import json
import logging
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

from ingestion.providers import finnhub as fh

KEY = "sekr1t-test-key"
T = 1767623400  # 2026-01-05T14:30:00Z
CONFIG = Path(__file__).resolve().parents[2] / "config" / "instruments.json"


class _Stop(Exception):
    pass


def collect(provider, monkeypatch, sleeps=None, items=None):
    """Drive provider.stream() until it has slept `sleeps` times or yielded `items` messages."""
    run = SimpleNamespace(items=[], delays=[], statuses=[])

    async def fake_sleep(delay):
        run.delays.append(delay)
        run.statuses.append(provider.status)
        if len(run.delays) == sleeps:
            raise _Stop

    monkeypatch.setattr(fh.asyncio, "sleep", fake_sleep)

    async def main():
        stream = provider.stream()
        try:
            async with asyncio.timeout(5):
                async for message in stream:
                    run.items.append(message)
                    if len(run.items) == items:
                        break
        except _Stop:
            pass
        finally:
            await stream.aclose()

    asyncio.run(main())
    return run


# --- REST ------------------------------------------------------------------------------------


def quote(c, t=T, **extra):
    return httpx.Response(200, json={"c": c, "h": c + 1, "l": c - 1, "o": c, "t": t, **extra})


def responder(script, seen=None):
    """script[symbol] = list of responses/exceptions served in order; the last one repeats."""
    def handler(request):
        if seen is not None:
            seen.append(request)
        queue = script[request.url.params["symbol"]]
        r = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(r, type) and issubclass(r, Exception):
            raise r("simulated failure", request=request)
        return httpx.Response(r.status_code, headers=r.headers, content=r.content)  # fresh copy per request
    return handler


def rest(script, symbols, seen=None):
    return fh.FinnhubRestProvider(KEY, symbols, 30, transport=httpx.MockTransport(responder(script, seen)))


def test_rest_quotes_become_delayed_events_and_unchanged_quotes_are_skipped(monkeypatch):
    script = {"AAPL": [quote(200.0), quote(200.0), quote(201.0)], "MSFT": [quote(410.0)], "NODATA": [quote(0, t=0)]}
    run = collect(rest(script, ["AAPL", "MSFT", "NODATA"]), monkeypatch, sleeps=3)
    assert [(e["symbol"], e["price"]) for e in run.items] == [("AAPL", 200.0), ("MSFT", 410.0), ("AAPL", 201.0)]
    assert run.delays == [30, 30, 30] and run.statuses == ["polling"] * 3
    e = run.items[0]
    assert set(e) == {"event_id", "symbol", "timestamp", "price", "volume", "open", "high", "low", "close",
                      "source", "data_mode", "schema_version"}
    assert e["data_mode"] == "delayed" and e["source"] == "finnhub" and e["timestamp"].startswith("2026-01-05T14:30:00")
    again = collect(rest({"AAPL": [quote(200.0)]}, ["AAPL"]), monkeypatch, sleeps=1)
    assert again.items[0]["event_id"] == e["event_id"]


def test_rest_token_is_sent_as_header_never_in_url(monkeypatch):
    seen = []
    collect(rest({"AAPL": [quote(200.0)]}, ["AAPL"], seen), monkeypatch, sleeps=2)
    assert seen and all(r.headers["X-Finnhub-Token"] == KEY and KEY not in str(r.url) for r in seen)


def test_rest_unauthorized_waits_max_backoff_logs_once_and_never_leaks_key(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    p = rest({"AAPL": [httpx.Response(401, json={"error": "Invalid API key."})]}, ["AAPL"])
    run = collect(p, monkeypatch, sleeps=3)
    assert run.items == [] and run.delays == [60, 60, 60] and p.status == "unauthorized"
    assert len([r for r in caplog.records if r.name == fh.__name__]) == 1
    assert KEY not in caplog.text and KEY not in p.status


@pytest.mark.parametrize(("retry_after", "expected"), [("7", 7), ("9999", 60), ("0", 1), (None, 60)])
def test_rest_rate_limit_honours_clamped_retry_after(monkeypatch, retry_after, expected):
    headers = {"Retry-After": retry_after} if retry_after is not None else {}
    run = collect(rest({"AAPL": [httpx.Response(429, headers=headers), quote(200.0)]}, ["AAPL"]), monkeypatch, sleeps=2)
    assert run.delays[0] == expected and run.statuses == ["rate_limited", "polling"]
    assert [e["symbol"] for e in run.items] == ["AAPL"]


@pytest.mark.parametrize("failure", [httpx.ConnectError, httpx.ReadTimeout, httpx.Response(503)],
                         ids=["network", "timeout", "server_error"])
def test_rest_transient_failures_back_off_then_recover(monkeypatch, caplog, failure):
    caplog.set_level(logging.DEBUG)
    script = {"AAPL": [failure, failure, quote(200.0), failure, quote(201.0)]}
    run = collect(rest(script, ["AAPL"]), monkeypatch, sleeps=5)
    name = "HTTPStatusError" if isinstance(failure, httpx.Response) else failure.__name__
    assert run.statuses == [f"retrying ({name})", f"retrying ({name})", "polling", f"retrying ({name})", "polling"]
    assert 0.5 <= run.delays[0] <= 1 and 1 <= run.delays[1] <= 2  # jittered exponential
    assert 0.5 <= run.delays[3] <= 1  # reset by the successful poll
    assert [e["price"] for e in run.items] == [200.0, 201.0]
    assert KEY not in caplog.text


def test_rest_bad_bodies_are_forwarded_as_bytes_and_later_symbols_still_arrive(monkeypatch):
    script = {
        "JUNK": [httpx.Response(200, content=b"<html>oops")],
        "LIST": [httpx.Response(200, content=b"[1, 2]")],
        "PART": [httpx.Response(200, content=b'{"c": 1.0, "t": 1767623400}')],
        "AAPL": [quote(200.0)],
    }
    run = collect(rest(script, ["JUNK", "LIST", "PART", "AAPL"]), monkeypatch, sleeps=1)
    assert run.items[:3] == [b"<html>oops", b"[1, 2]", b'{"c": 1.0, "t": 1767623400}']
    assert run.items[3]["symbol"] == "AAPL" and len(run.items) == 4
    assert run.delays == [30] and run.statuses == ["polling"]  # same poll, no error backoff


# --- WebSocket -------------------------------------------------------------------------------


class FakeWS:
    def __init__(self, session):
        self.session = session  # list of frames, or an exception the handshake raises
        self.sent = []

    async def __aenter__(self):
        if isinstance(self.session, BaseException):
            raise self.session
        return self

    async def __aexit__(self, *exc):
        return False

    async def send(self, data):
        self.sent.append(json.loads(data))

    async def __aiter__(self):
        for frame in self.session:
            yield frame


def ws_provider(*sessions, symbols=("AAPL",)):
    """Each connect() takes the next session; the last one repeats."""
    conns, kwargs = [], []

    def connect(url, **kw):
        assert url == f"wss://ws.finnhub.io?token={KEY}"
        kwargs.append(kw)
        conns.append(FakeWS(sessions[min(len(conns), len(sessions) - 1)]))
        return conns[-1]

    p = fh.FinnhubWebSocketProvider(KEY, list(symbols), connect=connect)
    return p, conns, kwargs


def trades(*rows):
    return json.dumps({"type": "trade", "data": [{"s": s, "p": p, "t": t, "v": v} for s, p, t, v in rows]})


def subscribed(conn):
    return [m["symbol"] for m in conn.sent if m["type"] == "subscribe"]


def test_ws_trades_become_live_events_with_session_ohlc(monkeypatch):
    frames = [trades(("AAPL", 200.5, T * 1000, 10), ("AAPL", 199.0, T * 1000 + 1, 5)), '{"type":"ping"}']
    p, conns, kwargs = ws_provider(frames, symbols=("AAPL", "MSFT"))
    run = collect(p, monkeypatch, sleeps=1)
    assert [(e["price"], e["open"], e["high"], e["low"]) for e in run.items] == [
        (200.5, 200.5, 200.5, 200.5), (199.0, 200.5, 200.5, 199.0)]
    assert all(e["data_mode"] == "live" and e["source"] == "finnhub" for e in run.items)
    assert subscribed(conns[0]) == ["AAPL", "MSFT"]
    assert not kwargs[0]["logger"].isEnabledFor(logging.DEBUG)  # websockets logs ?token= at DEBUG


def test_ws_malformed_frames_are_forwarded_and_the_connection_is_kept(monkeypatch):
    frames = [b"\x00garbage", trades(("AAPL", 200.0, T * 1000, 1)), '"a string"',
              '{"type":"trade","data":{"s":"AAPL"}}', '{"type":"trade","data":[{"s":"AAPL"}]}',
              trades(("AAPL", 201.0, T * 1000 + 1, 1))]
    p, conns, _ = ws_provider(frames)
    run = collect(p, monkeypatch, sleeps=1)
    assert len(conns) == 1  # no reconnect before the session ended
    assert [m if isinstance(m, bytes) else m["price"] for m in run.items] == [
        b"\x00garbage", 200.0, b'"a string"', b'{"type":"trade","data":{"s":"AAPL"}}',
        b'{"type":"trade","data":[{"s":"AAPL"}]}', 201.0]


def test_ws_error_frame_sets_redacted_truncated_status(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    msg = f"bad token {KEY} " + "x" * 300
    p, _, _ = ws_provider([json.dumps({"type": "error", "msg": msg}), trades(("AAPL", 200.0, T * 1000, 1))])
    run = collect(p, monkeypatch, items=1)
    assert run.items[0]["price"] == 200.0
    assert p.status.startswith("provider error: bad token ***") and len(p.status) <= len("provider error: ") + 100
    assert KEY not in caplog.text


def test_ws_connect_failure_backs_off_and_resubscribes_everything(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    refused = OSError(f"connect to wss://ws.finnhub.io?token={KEY} refused")
    p, conns, _ = ws_provider(refused, refused, [trades(("MSFT", 410.0, T * 1000, 1))], refused, symbols=("AAPL", "MSFT"))
    run = collect(p, monkeypatch, sleeps=4)
    assert run.statuses == ["reconnecting (OSError)"] * 2 + ["reconnecting (ConnectionError)", "reconnecting (OSError)"]
    assert 0.5 <= run.delays[0] <= 1 and 1 <= run.delays[1] <= 2
    assert 0.5 <= run.delays[2] <= 1  # reset once subscribed
    assert subscribed(conns[2]) == ["AAPL", "MSFT"] and [e["symbol"] for e in run.items] == ["MSFT"]
    assert KEY not in caplog.text and "token=***" in caplog.text


def test_ws_handshake_401_is_unauthorized_with_max_backoff(monkeypatch, caplog):
    caplog.set_level(logging.DEBUG)
    p, _, _ = ws_provider(InvalidStatus(Response(401, "Unauthorized", Headers())))
    run = collect(p, monkeypatch, sleeps=2)
    assert run.delays == [60, 60] and p.status == "unauthorized"
    assert len([r for r in caplog.records if r.name == fh.__name__]) == 1
    assert KEY not in caplog.text


# --- wiring ----------------------------------------------------------------------------------


@pytest.mark.parametrize(("kind", "key", "expected"), [
    ("finnhub_rest", "", "SimulatorProvider"),
    ("finnhub_ws", "", "SimulatorProvider"),
    ("finnhub_rest", KEY, "FinnhubRestProvider"),
    ("finnhub_ws", KEY, "FinnhubWebSocketProvider"),
])
def test_build_provider_needs_a_key_for_finnhub(monkeypatch, kind, key, expected):
    from ingestion import main

    monkeypatch.delenv("PROVIDERS", raising=False)  # the container sets it; this test drives the legacy PROVIDER
    monkeypatch.setenv("PROVIDER", kind)
    monkeypatch.setenv("FINNHUB_API_KEY", key)
    monkeypatch.setenv("INSTRUMENTS_FILE", str(CONFIG))
    monkeypatch.delenv("FINNHUB_POLL_INTERVAL_S", raising=False)
    assert type(main.build_provider({})).__name__ == expected


def test_ws_deeply_nested_frame_is_forwarded_not_fatal():
    provider = fh.FinnhubWebSocketProvider(KEY, ["AAPL"], connect=lambda *a, **k: None)
    out = provider._frame("[" * 100_000 + "]" * 100_000, fh.SessionBook())
    assert len(out) == 1 and isinstance(out[0], bytes)
