"""Ingestion service: providers -> Kafka `market.raw`. Heartbeat -> Redis."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
import time
from pathlib import Path
from datetime import UTC, datetime

import psutil
import redis.asyncio as aioredis
from redis.exceptions import RedisError

from ingestion.logging_setup import setup_logging
from ingestion.producers.kafka_producer import EventProducer
from ingestion.providers.base import MarketDataProvider
from ingestion.providers.binance import DEFAULT_URL as BINANCE_URL
from ingestion.providers.binance import BinanceProvider
from ingestion.providers.finnhub import FinnhubRestProvider, FinnhubWebSocketProvider
from ingestion.providers.simulator import SimulatorProvider
from ingestion.simulator.exchange import SimulatedExchange
from ingestion.simulator.model import load_instruments

log = logging.getLogger("ingestion")
HEALTH_FILE = Path("/tmp/healthy")
HEARTBEAT_KEY = f"mp:svc:ingestion:{socket.gethostname()}"
QUOTES_KEY = "mp:quotes"
KNOWN_PROVIDERS = ("simulator", "binance", "finnhub_ws", "finnhub_rest")


def env(name: str, default: str) -> str:
    return os.environ.get(name) or default  # empty counts as unset: compose passes ${VAR:-} as ""


def resolve_providers(raw: str, has_finnhub_key: bool) -> list[str]:
    """PROVIDERS value ("binance, simulator") -> de-duplicated provider names. Pure; bad config exits."""
    names = list(dict.fromkeys(n for n in map(str.strip, raw.split(",")) if n)) or ["simulator"]
    unknown = [n for n in names if n not in KNOWN_PROVIDERS]
    if unknown:
        raise SystemExit(f"unknown provider(s) {', '.join(unknown)}; choose from {', '.join(KNOWN_PROVIDERS)}")
    finnhub = [n for n in names if n.startswith("finnhub")]
    if finnhub and "simulator" in names:
        raise SystemExit(f"PROVIDERS lists simulator and {finnhub[0]}: finnhub replaces the simulator for equities, "
                         "so list only one of them")
    if len(finnhub) > 1:
        raise SystemExit(f"PROVIDERS lists {' and '.join(finnhub)}: both stream the same equities, so list only one")
    if finnhub and not has_finnhub_key:
        log.warning("%s needs FINNHUB_API_KEY, which is empty; using the simulator for equities instead", finnhub[0])
        names[names.index(finnhub[0])] = "simulator"
    return names


def provider_names() -> list[str]:
    """PROVIDERS, else the legacy single PROVIDER, else the simulator."""
    return resolve_providers(env("PROVIDERS", env("PROVIDER", "")), bool(os.environ.get("FINNHUB_API_KEY")))


async def initial_prices(redis: aioredis.Redis) -> dict[str, float]:
    """Resume the walk from the last cached price so restarts don't jump the chart."""
    try:
        raw = await asyncio.wait_for(redis.hgetall(QUOTES_KEY), timeout=3)
    except (RedisError, OSError, TimeoutError) as exc:
        log.warning("could not read last prices from redis, starting from base prices: %s", exc)
        return {}
    prices = {}
    for symbol, value in raw.items():
        try:
            prices[symbol] = float(json.loads(value)["price"])
        except (ValueError, KeyError, TypeError):
            continue
    return prices


def build_provider(prices: dict[str, float], name: str | None = None) -> MarketDataProvider:
    """One provider; `name` defaults to the first configured one (the single-PROVIDER setup)."""
    name = name or provider_names()[0]
    if name == "binance":
        with open(env("BINANCE_INSTRUMENTS_FILE", "/srv/config/binance.json"), encoding="utf-8") as f:
            symbols = [row["symbol"] for row in json.load(f)]
        return BinanceProvider(symbols, env("BINANCE_WS_URL", BINANCE_URL))
    specs = load_instruments(env("INSTRUMENTS_FILE", "/srv/config/instruments.json"))
    api_key = os.environ.get("FINNHUB_API_KEY", "")
    symbols = [s.symbol for s in specs]
    if name == "finnhub_ws":
        return FinnhubWebSocketProvider(api_key, symbols)
    if name == "finnhub_rest":
        return FinnhubRestProvider(api_key, symbols, float(env("FINNHUB_POLL_INTERVAL_S", "30")))
    if name != "simulator":
        raise ValueError(f"no builder for provider {name!r}")
    interval_s = int(env("SIM_INTERVAL_MS", "500")) / 1000
    seed = os.environ.get("SIM_SEED")
    exchange = SimulatedExchange(
        specs,
        seed=int(seed) if seed else None,
        speed=float(env("SIM_SPEED", "1")),
        interval_s=interval_s,
        tick_probability=float(env("SIM_TICK_PROBABILITY", "0.6")),
        fault_rate=float(env("SIM_FAULT_RATE", "0.005")),
        initial_prices=prices,
    )
    return SimulatorProvider(exchange, interval_s)


def provider_fields(providers: list[MarketDataProvider]) -> dict:
    """Heartbeat fields about the providers. Pure. Overall "streaming" only while every provider is healthy."""
    bad = [f"{p.name} {p.status}" for p in providers if not p.status.startswith(("streaming", "polling"))]
    return {
        "status": f"degraded: {'; '.join(bad)}" if bad else "streaming",
        "provider": ", ".join(p.name for p in providers),
        "data_mode": ", ".join(dict.fromkeys(p.data_mode for p in providers)),
        "providers": [{"name": p.name, "data_mode": p.data_mode, "status": p.status} for p in providers],
    }


async def heartbeat(redis: aioredis.Redis, providers: list[MarketDataProvider], producer: EventProducer) -> None:
    proc = psutil.Process()
    proc.cpu_percent(None)
    last_produced, last_t = 0, time.monotonic()
    while True:
        await asyncio.sleep(2)
        HEALTH_FILE.touch()  # container healthcheck: the event loop is alive
        producer.poll()
        now = time.monotonic()
        s = producer.stats
        eps = (s.produced - last_produced) / (now - last_t)
        last_produced, last_t = s.produced, now
        beat = {
            "service": "ingestion",
            **provider_fields(providers),
            "started_at": datetime.fromtimestamp(s.started_at, UTC).isoformat(),
            "heartbeat_at": datetime.now(UTC).isoformat(),
            "produced": s.produced,
            "delivered": s.delivered,
            "delivery_failed": s.delivery_failed,
            "last_delivered_at": s.last_delivered_at,
            "events_per_second": round(eps, 2),
            "cpu_percent": proc.cpu_percent(None),
            "rss_bytes": proc.memory_info().rss,
        }
        try:
            await asyncio.wait_for(redis.set(HEARTBEAT_KEY, json.dumps(beat), ex=10), timeout=2)
        except (RedisError, OSError, TimeoutError) as exc:
            log.warning("heartbeat write failed: %s", exc)


async def run() -> None:
    names = provider_names()  # a bad PROVIDERS exits here, before anything connects
    redis = aioredis.from_url(env("REDIS_URL", "redis://redis:6379/0"), decode_responses=True, socket_timeout=3)
    producer = EventProducer(env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"), env("RAW_TOPIC", "market.raw"))
    prices = await initial_prices(redis)
    providers = [build_provider(prices, name) for name in names]
    log.info("ingestion started", extra={"fields": {"providers": names}})

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows dev runs
            pass

    async def pump(provider: MarketDataProvider) -> None:
        async for message in provider.stream():
            producer.send(message)

    tasks = [asyncio.create_task(pump(p), name=f"{p.name} pump") for p in providers]
    tasks.append(asyncio.create_task(heartbeat(redis, providers, producer), name="heartbeat"))
    stopper = asyncio.create_task(stop.wait(), name="stop")
    done, _ = await asyncio.wait([*tasks, stopper], return_when=asyncio.FIRST_COMPLETED)
    for t in [*tasks, stopper]:
        t.cancel()
    await asyncio.gather(*tasks, stopper, return_exceptions=True)
    left = producer.flush(10)
    log.info("ingestion stopped", extra={"fields": {"unflushed": left, **producer_counts(producer)}})
    await redis.aclose()
    for t in done:  # any task ending on its own (crashed or not) is a non-zero exit, so Docker restarts us
        if t is stopper:
            continue
        if not t.cancelled() and t.exception() is not None:
            raise t.exception()
        raise SystemExit(f"{t.get_name()} ended unexpectedly")


def producer_counts(producer: EventProducer) -> dict:
    s = producer.stats
    return {"produced": s.produced, "delivered": s.delivered, "delivery_failed": s.delivery_failed}


if __name__ == "__main__":
    setup_logging()
    asyncio.run(run())
