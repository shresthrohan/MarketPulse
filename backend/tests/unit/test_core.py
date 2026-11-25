from app.core.runtime import RateLimiter, RequestStats, percentile
from app.services.market import movers, overview, screen, sort_rows
from app.services.ws import ConnectionManager, parse_command


def test_rate_limiter_blocks_after_limit_and_recovers_after_window():
    rl = RateLimiter(limit=3, window_s=10)
    assert [rl.check("a", t) for t in (0, 1, 2)] == [None, None, None]
    assert rl.check("a", 3) == 7
    assert rl.check("b", 3) is None  # other clients unaffected
    assert rl.check("a", 10.5) is None


def test_rate_limiter_memory_is_bounded():
    rl = RateLimiter(limit=1, window_s=60, max_clients=100)
    for i in range(1000):
        rl.check(str(i), 0)
    assert len(rl._hits) == 100


def test_percentile_nearest_rank():
    assert percentile([1, 2, 3, 4], 50) == 2 and percentile([1, 2, 3, 4], 99) == 4 and percentile([], 50) is None


def test_request_stats_window_is_bounded():
    s = RequestStats(maxlen=10)
    for i in range(50):
        s.record("GET /x", float(i))
    out = s.summary()
    assert out["requests"] == 10 and out["total_requests"] == 50 and out["p50_ms"] == 44


def row(sym, change_pct=None, volume=None, asset="equity"):
    q = None if change_pct is None else {"change_pct": change_pct, "volume": volume, "data_mode": "simulated",
                                         "source": "simulator", "processed_at": "2026-01-01T00:00:00+00:00"}
    return {"symbol": sym, "company_name": sym.lower() + " inc", "exchange": "NYSE", "asset_type": asset, "quote": q}


ROWS = [row("A", 1.5, 10), row("B", -2.0, 30), row("C"), row("D", 0.0, 20, "etf")]


def test_sort_puts_missing_quotes_last_in_both_directions():
    assert [r["symbol"] for r in sort_rows(ROWS, "change_pct", True)] == ["A", "D", "B", "C"]
    assert [r["symbol"] for r in sort_rows(ROWS, "change_pct", False)] == ["B", "D", "A", "C"]


def test_movers_exclude_instruments_without_quotes():
    assert [r["symbol"] for r in movers(ROWS, "gainers", 10)] == ["A", "D", "B"]
    assert [r["symbol"] for r in movers(ROWS, "active", 2)] == ["B", "D"]


def test_screen_filters_and_paginates():
    out = screen(ROWS, search="b", asset_type=None, exchange=None, sort="symbol", order="asc", page=1, page_size=10)
    assert [r["symbol"] for r in out["items"]] == ["B"] and out["total"] == 1
    out = screen(ROWS, search=None, asset_type="equity", exchange="nyse", sort="symbol", order="desc", page=2, page_size=2)
    assert [r["symbol"] for r in out["items"]] == ["A"] and out["total"] == 3


def test_overview_breadth_and_groups():
    o = overview(ROWS)
    assert o["breadth"] == {"advancers": 1, "decliners": 1, "unchanged": 1}
    assert [r["symbol"] for r in o["indices"]] == ["D"] and o["quotes_available"] == 3 and o["data_modes"] == ["simulated"]


def test_parse_command_validates_shape():
    assert parse_command('{"action":"subscribe","symbols":["aapl"]}') == ("subscribe", ["AAPL"])
    assert isinstance(parse_command("nope"), str)
    assert isinstance(parse_command('{"action":"subscribe","symbols":[]}'), str)
    assert isinstance(parse_command('{"action":"drop","symbols":["A"]}'), str)


class _FakeWS:
    pass


def test_fanout_respects_subscriptions_and_flags_slow_clients():
    import asyncio

    async def run():
        m = ConnectionManager(max_connections=2, queue_size=1)
        a, b = m.register(_FakeWS()), m.register(_FakeWS())
        assert m.register(_FakeWS()) is None  # capacity enforced
        a.symbols.add("*")
        b.symbols.add("MSFT")
        m.fanout('{"symbol":"AAPL"}')
        assert a.queue.qsize() == 1 and b.queue.qsize() == 0
        m.fanout('{"symbol":"AAPL"}')  # a's queue (size 1) is full
        assert a.overflowed.is_set() and m.dropped_clients == 1 and not b.overflowed.is_set()

    asyncio.run(run())


def test_websocket_origin_check():
    from app.services.ws import origin_allowed

    allowed = ["http://localhost:8080"]
    assert origin_allowed(None, "localhost:8000", allowed)  # non-browser client
    assert origin_allowed("http://localhost:8080", "backend:8000", allowed)  # configured origin
    assert origin_allowed("http://127.0.0.1:8080", "127.0.0.1:8080", allowed)  # same origin via nginx
    assert not origin_allowed("https://evil.example", "localhost:8000", allowed)
    assert not origin_allowed("http://localhost:8080.evil.example", "localhost:8000", allowed)


def test_cache_status_flags_stale_and_missing_quotes():
    from datetime import UTC, datetime, timedelta

    from app.services.market import cache_status

    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    fresh = {"A": {"processed_at": (now - timedelta(seconds=2)).isoformat()}}
    old = {"A": {"processed_at": (now - timedelta(minutes=5)).isoformat()}}
    assert cache_status(fresh, True, now) == {"available": True, "quotes": 1, "last_update": fresh["A"]["processed_at"],
                                              "age_s": 2.0, "stale": False}
    assert cache_status(old, True, now)["stale"] is True
    assert cache_status({}, False, now) == {"available": False, "quotes": 0, "last_update": None, "age_s": None, "stale": True}


def test_analytics_refuse_to_mix_simulated_and_real_instruments():
    import pytest
    from fastapi import HTTPException

    from app.api.deps import same_data_source
    from app.models.tables import Instrument

    sim, live, delayed, unknown = (Instrument(symbol=s, data_mode=m) for s, m in
                                   (("AAPL", "simulated"), ("BTCUSDT", "live"), ("MSFT", "delayed"), ("NEW", None)))
    same_data_source([sim, unknown])
    same_data_source([live, delayed])  # both real market data
    with pytest.raises(HTTPException) as exc:
        same_data_source([sim, live])
    assert exc.value.status_code == 422 and "AAPL" in exc.value.detail and "BTCUSDT" in exc.value.detail


def test_one_client_cannot_take_every_websocket_slot():
    import asyncio

    async def run():
        m = ConnectionManager(max_connections=10, queue_size=4, max_per_client=2)
        a1, a2 = m.register(_FakeWS(), "1.2.3.4"), m.register(_FakeWS(), "1.2.3.4")
        assert a1 and a2 and m.register(_FakeWS(), "1.2.3.4") is None  # per-client cap
        assert m.register(_FakeWS(), "5.6.7.8") is not None  # others unaffected
        m.unregister(a1)
        m.unregister(a1)  # idempotent: a double unregister must not free a slot twice
        assert m.register(_FakeWS(), "1.2.3.4") is not None and m.register(_FakeWS(), "1.2.3.4") is None

    asyncio.run(run())
