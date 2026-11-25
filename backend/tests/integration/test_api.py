import csv
import io
import threading
import uuid

import pytest

STATUSES = {"up", "degraded", "down"}


def get(client, url, status=200):
    r = client.get(url)
    assert r.status_code == status, (url, r.status_code, r.text[:300])
    return r.json()


def test_health_reports_real_component_status(client):
    h = get(client, "/api/health")
    assert set(h["components"]) == {"database", "redis", "kafka", "ingestion", "processor", "websocket"}
    assert h["components"]["database"]["status"] == "up" and h["components"]["redis"]["status"] == "up"
    assert get(client, "/api/health/database")["pool"]["size"] >= 1
    k = get(client, "/api/health/kafka")
    assert k["status"] == "up" and k["missing_topics"] == [] and set(k["topics"]) >= {"market.raw", "market.errors"}
    s = get(client, "/api/health/system")
    assert s["process"]["rss_bytes"] > 0 and s["host"]["cpu_count"] >= 1 and "p95_ms" in s["api"]


def test_instruments_screener_sorts_filters_and_paginates(client):
    out = get(client, "/api/instruments?page_size=100")
    assert out["total"] >= 20 and len(out["items"]) == out["total"]
    syms = [i["symbol"] for i in out["items"]]
    assert syms == sorted(syms)
    assert [i["symbol"] for i in get(client, "/api/instruments?search=apple")["items"]] == ["AAPL"]
    etfs = get(client, "/api/instruments?asset_type=etf&page_size=100")["items"]
    assert etfs and all(i["asset_type"] == "etf" for i in etfs)
    page2 = get(client, "/api/instruments?page=2&page_size=5")
    assert [i["symbol"] for i in page2["items"]] == syms[5:10]
    get(client, "/api/instruments?sort=nope", 422)
    get(client, "/api/instruments/NOPE", 404)


@pytest.mark.parametrize("rng,interval", [("1D", "1m"), ("1W", "15m"), ("1M", "1d"), ("1Y", "1d")])
def test_history_returns_consistent_ohlcv_bars_from_the_database(client, rng, interval):
    out = get(client, f"/api/instruments/aapl/history?range={rng}")
    assert out["symbol"] == "AAPL" and out["interval"] == interval and out["bars"]
    times = [b["time"] for b in out["bars"]]
    assert times == sorted(times) and len(set(times)) == len(times)
    for b in out["bars"]:
        assert b["low"] <= min(b["open"], b["close"]) <= max(b["open"], b["close"]) <= b["high"] and b["volume"] >= 0
    if rng == "1Y":
        assert 300 <= len(out["bars"]) <= 366


def test_statistics_are_computed_from_history(client):
    s = get(client, "/api/instruments/MSFT/statistics")
    assert s["observations"] >= 300 and s["low_52w"] <= s["last_price"] * 1.5
    assert s["volatility_30d"] > 0 and s["max_drawdown_1y"]["value"] <= 0 and set(s["returns"]) == {"1W", "1M", "3M", "1Y"}


def test_market_overview_and_movers(client):
    o = get(client, "/api/market/overview")
    assert o["instruments_tracked"] >= 20 and all(i["asset_type"] == "etf" for i in o["indices"])
    g = [i["quote"]["change_pct"] for i in get(client, "/api/market/gainers?limit=50")["items"]]
    assert g == sorted(g, reverse=True)
    lo = [i["quote"]["change_pct"] for i in get(client, "/api/market/losers?limit=50")["items"]]
    assert lo == sorted(lo)


def test_analytics_endpoints(client):
    syms = "AAPL,MSFT,SPY,QQQ"
    r = get(client, f"/api/analytics/returns?symbols={syms}&range=3M&method=log")
    assert set(r["series"]) == set(syms.split(",")) and r["summary"]["AAPL"]["observations"] > 50
    c = get(client, f"/api/analytics/correlation?symbols={syms}&range=1Y")
    assert c["symbols"] == syms.split(",")
    for i in range(4):
        assert c["matrix"][i][i] == pytest.approx(1.0)
        for j in range(4):
            assert c["matrix"][i][j] == pytest.approx(c["matrix"][j][i])
    assert c["matrix"][2][3] > 0.5  # SPY/QQQ share the market factor
    v = get(client, f"/api/analytics/volatility?symbols={syms}&window=20")
    assert all(x > 0 for x in v["latest"].values())
    d = get(client, "/api/analytics/drawdown?symbols=TSLA&range=1Y")
    assert all(p["value"] <= 0 for p in d["series"]["TSLA"]) and d["max_drawdown"]["TSLA"]["value"] < 0
    cmp_ = get(client, f"/api/analytics/comparison?symbols={syms}")
    assert all(s[0]["value"] == pytest.approx(100) for s in cmp_["series"].values())
    ma = get(client, "/api/analytics/moving-averages?symbol=AAPL&range=3M&sma=20&ema=50")
    assert ma["sma"] and ma["ema"] and ma["sma"][0]["time"] >= ma["close"][0]["time"]
    vol = get(client, "/api/analytics/volume?symbols=AAPL&range=1M")
    assert vol["series"]["AAPL"] and vol["average"]["AAPL"]
    get(client, "/api/analytics/returns?symbols=" + ",".join(["AAPL"] * 1 + [f"X{i}" for i in range(11)]), 422)
    get(client, "/api/analytics/returns?symbols=AAPL,NOPE", 404)


def test_watchlist_lifecycle_persists_in_postgres(client):
    name = f"it-{uuid.uuid4().hex[:8]}"
    r = client.post("/api/watchlists", json={"name": name})
    assert r.status_code == 201
    wid = r.json()["id"]
    assert client.post("/api/watchlists", json={"name": name}).status_code == 409
    assert client.post("/api/watchlists", json={"name": "  "}).status_code == 422
    assert client.put(f"/api/watchlists/{wid}", json={"name": name + "-r"}).json()["name"] == name + "-r"
    r = client.post(f"/api/watchlists/{wid}/instruments", json={"symbol": "nvda"})
    assert r.status_code == 201 and [i["symbol"] for i in r.json()["instruments"]] == ["NVDA"]
    assert client.post(f"/api/watchlists/{wid}/instruments", json={"symbol": "NVDA"}).status_code == 409
    assert client.post(f"/api/watchlists/{wid}/instruments", json={"symbol": "NOPE"}).status_code == 404
    listed = {w["id"]: w for w in get(client, "/api/watchlists")["items"]}
    assert listed[wid]["instruments"][0]["symbol"] == "NVDA"
    assert client.delete(f"/api/watchlists/{wid}/instruments/NVDA").status_code == 204
    assert client.delete(f"/api/watchlists/{wid}/instruments/NVDA").status_code == 404
    assert client.delete(f"/api/watchlists/{wid}").status_code == 204
    assert client.delete(f"/api/watchlists/{wid}").status_code == 404


def test_data_explorer_filters_paginates_and_exports_csv(client):
    page = get(client, "/api/market/history?symbol=AAPL&page_size=5")
    assert page["items"] and all(e["symbol"] == "AAPL" for e in page["items"])
    ts = [e["event_timestamp"] for e in page["items"]]
    assert ts == sorted(ts, reverse=True)
    one = page["items"][0]
    assert get(client, f"/api/market/history?search={one['event_id']}")["items"][0]["event_id"] == one["event_id"]
    assert get(client, f"/api/market/history?search={one['event_id'][:8]}")["items"]
    assert all(e["price"] >= 100 for e in get(client, "/api/market/history?min_price=100&page_size=50")["items"])
    get(client, "/api/market/history?search=';drop", 422)
    r = client.get("/api/market/history?symbol=AAPL&format=csv")
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/csv")
    rows = list(csv.DictReader(io.StringIO(r.text)))
    assert rows and rows[0]["symbol"] == "AAPL" and "processing_status" in rows[0]


def test_pipeline_monitoring_reflects_real_processing(client):
    o = get(client, "/api/pipeline/overview")
    assert o["events_processed"] > 0 and o["last_successful_event_at"] is not None
    dq = o["data_quality"]
    assert dq["total"] >= dq["valid"] and dq["rejected"] == dq["invalid"] + dq["duplicate"]
    svcs = get(client, "/api/pipeline/services")["items"]
    assert {s["key"] for s in svcs} == {"ingestion", "kafka", "processor", "postgres", "redis", "api", "websocket"}
    assert all(s["status"] in STATUSES for s in svcs)
    assert {s["key"]: s["status"] for s in svcs}["processor"] == "up"
    m = get(client, "/api/pipeline/metrics?minutes=30&bucket_s=60")
    assert m["series"]["events_per_second"]
    t = get(client, "/api/pipeline/throughput?minutes=30")
    assert t["processed"] and all(p["value"] >= 0 for p in t["received"])
    for view in ("all", "success", "failed", "retried"):
        rows = get(client, f"/api/pipeline/events?view={view}&page_size=20")["items"]
        if view == "success":
            assert rows and all(r["severity"] == "info" for r in rows)
        if view == "failed":
            assert all(r["processing_status"] == "rejected" for r in rows)
        if view == "retried":
            assert all(r["processing_status"] == "retried" for r in rows)
    get(client, "/api/pipeline/errors?page_size=10")


def _recv_with_timeout(ws, timeout=15.0):
    box: dict = {}

    def run():
        try:
            box["msg"] = ws.receive_json()
        except Exception as exc:  # surfaced to the test below
            box["err"] = exc

    t = threading.Thread(target=run, daemon=True)
    t.start()
    t.join(timeout)
    if "err" in box:
        raise box["err"]
    assert "msg" in box, "no websocket message within timeout"
    return box["msg"]


def test_websocket_delivers_live_quotes_for_subscribed_symbols_only(client):
    with client.websocket_connect("/ws/market") as ws:
        ws.send_text("not json")
        assert _recv_with_timeout(ws)["type"] == "error"
        ws.send_json({"action": "subscribe", "symbols": ["SPY"]})
        assert _recv_with_timeout(ws) == {"type": "subscribed", "symbols": ["SPY"]}
        quotes = []
        for _ in range(20):
            m = _recv_with_timeout(ws)
            assert m["symbol"] == "SPY" and m["type"] in ("quote", "bar")
            if m["type"] == "quote":
                quotes.append(m)
            if len(quotes) == 2:
                break
        assert len(quotes) == 2 and quotes[0]["data_mode"] in ("simulated", "live", "delayed")
        assert quotes[1]["processed_at"] >= quotes[0]["processed_at"]


def test_cors_rejects_unknown_origins(client):
    r = client.options("/api/health", headers={"Origin": "https://evil.example", "Access-Control-Request-Method": "GET"})
    assert "access-control-allow-origin" not in {k.lower() for k in r.headers}


def test_websocket_rejects_foreign_origins_and_accepts_the_app_origin(client):
    from starlette.websockets import WebSocketDisconnect

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect("/ws/market", headers={"origin": "https://evil.example"}):
            pass
    assert exc.value.code == 1008
    with client.websocket_connect("/ws/market", headers={"origin": "http://localhost:8080"}) as ws:
        ws.send_json({"action": "subscribe", "symbols": ["SPY"]})
        assert _recv_with_timeout(ws)["type"] == "subscribed"


def test_websocket_connections_are_released_after_disconnect(client):
    import time

    def connections():
        return get(client, "/api/health/system")["websocket"]["connections"]

    def settle(predicate, timeout=5.0):
        # Unregistering happens in the connection's own task, shortly after the close.
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            n = connections()
            if predicate(n):
                time.sleep(0.3)
                if connections() == n:
                    return n
            time.sleep(0.1)
        return connections()

    before = settle(lambda n: True)  # sockets from earlier tests may still be closing
    with client.websocket_connect("/ws/market") as ws:
        ws.send_json({"action": "subscribe", "symbols": ["*"]})
        _recv_with_timeout(ws)
        assert settle(lambda n: n == before + 1) == before + 1
    assert settle(lambda n: n == before) == before


def test_cross_site_form_posts_cannot_create_watchlists(client):
    # A browser form can send text/plain without a CORS preflight; FastAPI must not parse it as JSON.
    r = client.post("/api/watchlists", content='{"name": "csrf"}', headers={"Content-Type": "text/plain"})
    assert r.status_code == 422
    assert all(w["name"] != "csrf" for w in get(client, "/api/watchlists")["items"])


def test_api_timestamps_are_utc(client):
    item = get(client, "/api/market/history?page_size=1")["items"][0]
    assert item["event_timestamp"].endswith("+00:00") and item["processed_at"].endswith("+00:00")
    err = get(client, "/api/pipeline/events?view=all&page_size=5")["items"]
    assert all(r["timestamp"].endswith("+00:00") for r in err)


def test_overview_reports_cache_freshness(client):
    cache = get(client, "/api/market/overview")["cache"]
    assert cache["available"] is True and cache["quotes"] >= 20 and cache["stale"] is False and cache["age_s"] < 60


def test_kafka_offsets_reconcile(client):
    k = get(client, "/api/health/kafka")
    assert k["raw_end_offsets"] >= k["raw_committed_offsets"] >= 0
    assert k["raw_end_offsets"] - k["raw_committed_offsets"] == k["consumer_lag"]


def test_live_and_simulated_instruments_are_labelled_and_never_mixed_in_analytics(client):
    rows = get(client, "/api/instruments?page_size=100")["items"]
    live = [r for r in rows if r["data_mode"] == "live"]
    if not live:
        pytest.skip("no live provider configured (CI runs the simulator only)")
    sim = next(r for r in rows if r["data_mode"] == "simulated")
    assert all(r["quote"] is None or r["quote"]["data_mode"] == r["data_mode"] for r in rows)
    assert set(get(client, "/api/market/overview")["data_modes"]) >= {"live", "simulated"}
    r = client.get(f"/api/analytics/correlation?symbols={sim['symbol']},{live[0]['symbol']}")
    assert r.status_code == 422 and "simulated" in r.json()["detail"] and live[0]["symbol"] in r.json()["detail"]
    ok = get(client, f"/api/analytics/returns?symbols={live[0]['symbol']}&range=1M")
    assert ok["summary"][live[0]["symbol"]]["observations"] >= 20  # real Binance history, not simulated
