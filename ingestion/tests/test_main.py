"""Provider selection, heartbeat aggregation and the pump supervisor, without Kafka or Redis."""

import asyncio
import json
import logging
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from ingestion import main
from ingestion.producers.kafka_producer import ProducerStats

CONFIG = Path(__file__).resolve().parents[2] / "config"


class _Stop(Exception):
    pass


def prov(name, data_mode, status):
    return SimpleNamespace(name=name, data_mode=data_mode, status=status)


# --- provider selection ----------------------------------------------------------------------


@pytest.mark.parametrize(("raw", "has_key", "expected"), [
    ("", False, ["simulator"]),
    ("binance", False, ["binance"]),
    (" binance, simulator ,binance,", False, ["binance", "simulator"]),  # whitespace and duplicates
    ("finnhub_ws", True, ["finnhub_ws"]),
    ("finnhub_rest,binance", True, ["finnhub_rest", "binance"]),
    ("binance,finnhub_ws", False, ["binance", "simulator"]),  # no key: the simulator stands in for equities
    ("finnhub_rest,finnhub_rest", False, ["simulator"]),
])
def test_resolve_providers(raw, has_key, expected):
    assert main.resolve_providers(raw, has_key) == expected


@pytest.mark.parametrize("has_key", [True, False])  # a bad list is bad whether or not the key is set
@pytest.mark.parametrize(("raw", "message"), [
    ("binance,kraken", "unknown provider(s) kraken"),
    ("simulator,finnhub_ws", "finnhub replaces the simulator for equities"),
    ("finnhub_rest, simulator", "finnhub replaces the simulator for equities"),
    ("finnhub_ws,finnhub_rest", "both stream the same equities"),
])
def test_resolve_providers_rejects_bad_lists(raw, message, has_key):
    with pytest.raises(SystemExit, match=re.escape(message)):
        main.resolve_providers(raw, has_key)


def test_finnhub_without_a_key_warns(caplog):
    caplog.set_level(logging.WARNING)
    main.resolve_providers("finnhub_ws", False)
    assert "FINNHUB_API_KEY" in caplog.text


@pytest.mark.parametrize(("env", "expected"), [
    ({}, ["simulator"]),
    ({"PROVIDER": "finnhub_ws"}, ["finnhub_ws"]),  # legacy single PROVIDER
    ({"PROVIDERS": "", "PROVIDER": "finnhub_rest"}, ["finnhub_rest"]),  # compose passes an unset var as ""
    ({"PROVIDERS": "binance,simulator", "PROVIDER": "finnhub_ws"}, ["binance", "simulator"]),
])
def test_provider_names_fall_back_to_the_legacy_provider(monkeypatch, env, expected):
    monkeypatch.delenv("PROVIDERS", raising=False)
    monkeypatch.delenv("PROVIDER", raising=False)
    monkeypatch.setenv("FINNHUB_API_KEY", "k")
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    assert main.provider_names() == expected


def test_binance_reads_its_own_instrument_file_and_url(monkeypatch):
    monkeypatch.setenv("BINANCE_INSTRUMENTS_FILE", str(CONFIG / "binance.json"))
    monkeypatch.setenv("BINANCE_WS_URL", "wss://data-stream.binance.vision")
    p = main.build_provider({}, "binance")
    assert p._url == ("wss://data-stream.binance.vision/stream?streams="
                      "btcusdt@kline_1s/ethusdt@kline_1s/solusdt@kline_1s/bnbusdt@kline_1s/xrpusdt@kline_1s")


# --- heartbeat -------------------------------------------------------------------------------


def test_provider_fields_aggregate_status_and_modes():
    sim, bn, rest = prov("simulator", "simulated", "streaming"), prov("binance", "live", "streaming"), \
        prov("finnhub_rest", "delayed", "polling")
    assert main.provider_fields([sim, bn]) == {
        "status": "streaming", "provider": "simulator, binance", "data_mode": "simulated, live",
        "providers": [{"name": "simulator", "data_mode": "simulated", "status": "streaming"},
                      {"name": "binance", "data_mode": "live", "status": "streaming"}]}
    assert main.provider_fields([rest, bn])["status"] == "streaming"  # polling is healthy too
    assert main.provider_fields([bn, prov("finnhub_ws", "live", "streaming")])["data_mode"] == "live"
    bn.status = "reconnecting (OSError)"
    assert main.provider_fields([sim, bn])["status"] == "degraded: binance reconnecting (OSError)"
    rest.status = "unauthorized"
    assert main.provider_fields([rest, bn])["status"] == "degraded: finnhub_rest unauthorized; binance reconnecting (OSError)"


def test_heartbeat_keeps_the_keys_the_backend_reads(monkeypatch):
    written = []

    class Redis:
        async def set(self, key, value, ex):
            written.append(json.loads(value))

    async def fake_sleep(delay):  # one beat, then stop
        if written:
            raise _Stop

    monkeypatch.setattr(main.asyncio, "sleep", fake_sleep)
    providers = [prov("simulator", "simulated", "streaming"), prov("binance", "live", "starting")]
    with pytest.raises(_Stop):
        asyncio.run(main.heartbeat(Redis(), providers, SimpleNamespace(stats=ProducerStats(), poll=lambda: None)))
    (beat,) = written
    assert set(beat) == {"service", "status", "provider", "data_mode", "started_at", "heartbeat_at", "produced",
                         "delivered", "delivery_failed", "last_delivered_at", "events_per_second", "cpu_percent",
                         "rss_bytes", "providers"}
    assert (beat["status"], beat["provider"]) == ("degraded: binance starting", "simulator, binance")


# --- supervisor ------------------------------------------------------------------------------


def test_run_pumps_every_provider_and_exits_non_zero_when_one_stops(monkeypatch):
    sent = []

    class Redis:
        async def hgetall(self, key):
            return {}

        async def aclose(self):
            pass

    class Producer:
        stats = ProducerStats()

        def send(self, message):
            sent.append(message)

        def poll(self):
            pass

        def flush(self, timeout_s):
            return 0

    class Provider:
        data_mode, status = "live", "streaming"

        def __init__(self, name, hang):
            self.name, self._hang = name, hang

        async def stream(self):
            yield {"symbol": self.name}
            if self._hang:
                await asyncio.Event().wait()

    fakes = {"a": Provider("a", hang=True), "b": Provider("b", hang=False)}
    monkeypatch.setattr(main.aioredis, "from_url", lambda *a, **kw: Redis())
    monkeypatch.setattr(main, "EventProducer", lambda *a: Producer())
    monkeypatch.setattr(main, "provider_names", lambda: list(fakes))
    monkeypatch.setattr(main, "build_provider", lambda prices, name: fakes[name])
    with pytest.raises(SystemExit, match="b pump ended unexpectedly"):
        asyncio.run(main.run())
    assert sent == [{"symbol": "a"}, {"symbol": "b"}]
