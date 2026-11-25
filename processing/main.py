"""Processing service entry point: consume loop + periodic metrics, heartbeat, retention."""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psutil
from confluent_kafka import Consumer, Producer
from redis import Redis
from redis.exceptions import RedisError

from processing.cache import QuoteCache
from processing.consumers.processor import StreamProcessor
from processing.logging_setup import setup_logging
from processing.repository import TRANSIENT_DB_ERRORS, Store
from processing.validators.event_validator import Rules

log = logging.getLogger("processing")
SERVICE = "processor"
HEALTH_FILE = Path("/tmp/healthy")


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def retention_policies() -> list[tuple[str, str, int]]:
    return [
        ("market_events", "event_timestamp < %s", int(env("EVENT_RETENTION_DAYS", "30"))),
        ("processing_errors", '"timestamp" < %s', int(env("ERROR_RETENTION_DAYS", "30"))),
        ("pipeline_metrics", '"timestamp" < %s', int(env("METRIC_RETENTION_DAYS", "7"))),
        ("historical_aggregates", "\"interval\" = '1m' AND \"timestamp\" < %s", int(env("BAR_1M_RETENTION_DAYS", "90"))),
    ]


def wait_for_instruments(store: Store) -> list[tuple[str, int, str | None]]:
    for attempt in range(30):
        try:
            instruments = store.load_instruments()
            if instruments:
                return instruments
            log.warning("instruments table is empty; waiting for the seed step")
        except TRANSIENT_DB_ERRORS as exc:
            log.warning("database not ready (attempt %d): %s", attempt + 1, exc)
        time.sleep(2)
    raise SystemExit("no instruments available after 60s")


def main() -> None:
    setup_logging()
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topics = {
        "raw": env("RAW_TOPIC", "market.raw"),
        "validated": env("VALIDATED_TOPIC", "market.validated"),
        "processed": env("PROCESSED_TOPIC", "market.processed"),
        "errors": env("ERRORS_TOPIC", "market.errors"),
    }
    store = Store(env("DATABASE_URL", "postgresql://marketpulse@postgres:5432/marketpulse"))
    instruments = wait_for_instruments(store)
    symbols = {sym: iid for sym, iid, _ in instruments}
    redis = Redis.from_url(env("REDIS_URL", "redis://redis:6379/0"), socket_timeout=2, socket_connect_timeout=2)
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": env("CONSUMER_GROUP", "marketpulse-processor"),
            "enable.auto.commit": False,  # offsets advance only after the DB commit
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 45000,
        }
    )
    producer = Producer(
        {"bootstrap.servers": bootstrap, "client.id": "marketpulse-processor", "acks": "all",
         "enable.idempotence": True, "linger.ms": 5, "compression.type": "lz4"}
    )
    max_future_skew = timedelta(seconds=float(env("MAX_FUTURE_SKEW_S", "30")))
    max_age = timedelta(hours=float(env("MAX_EVENT_AGE_H", "24")))

    def rules_for(rows: list[tuple[str, int, str | None]]) -> Rules:
        return Rules(frozenset(sym for sym, _, _ in rows), max_future_skew, max_age,
                     {sym: mode for sym, _, mode in rows if mode})

    rules = rules_for(instruments)
    processor = StreamProcessor(consumer, producer, store, QuoteCache(redis), topics, rules, symbols,
                                int(env("ROLLING_WINDOW", "20")))
    processor.rebuild_cache()  # repopulate quotes if Redis lost them while we were down
    batch_max = int(env("BATCH_MAX", "500"))
    # consume() returns early only when batch_max messages are buffered, so this bounds
    # the added latency at low rates while still batching under load.
    consume_timeout = float(env("CONSUME_TIMEOUT_S", "0.05"))
    metrics_every = float(env("METRICS_INTERVAL_S", "5"))
    retention_every = float(env("RETENTION_INTERVAL_S", "3600"))

    stopping = False

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer.subscribe([topics["raw"]])
    host = socket.gethostname()
    proc = psutil.Process()
    proc.cpu_percent(None)
    started_at = datetime.now(UTC).isoformat()
    last_metrics = last_symbols = time.monotonic()
    last_retention = 0.0
    log.info("processor started", extra={"fields": {"symbols": len(symbols), "topics": topics}})
    try:
        while not stopping:
            processor.handle(consumer.consume(num_messages=batch_max, timeout=consume_timeout))
            now = time.monotonic()
            if now - last_symbols > 60:
                last_symbols = now
                try:
                    rows = store.load_instruments()
                    processor.symbol_ids = {sym: iid for sym, iid, _ in rows}
                    processor.rules = rules_for(rows)
                except TRANSIENT_DB_ERRORS as exc:
                    log.warning("symbol refresh failed: %s", exc)
            if now - last_metrics >= metrics_every:
                last_metrics = now
                HEALTH_FILE.touch()  # container healthcheck: the consume loop is alive
                processor.rebuild_cache()  # no-op unless Redis lost quotes
                lag = processor.consumer_lag()
                values = processor.metrics.snapshot(lag)
                try:
                    store.write_metrics(SERVICE, values)
                    processor.metrics.reset_interval()
                except TRANSIENT_DB_ERRORS as exc:
                    log.warning("metrics flush failed, will include in next flush: %s", exc)
                beat = {
                    "service": SERVICE, "status": "running", "host": host, "started_at": started_at,
                    "heartbeat_at": datetime.now(UTC).isoformat(), "last_event_at": processor.metrics.last_event_at,
                    "consumer_lag": lag, "totals": dict(processor.metrics.totals),
                    "cpu_percent": proc.cpu_percent(None), "rss_bytes": proc.memory_info().rss,
                }
                try:
                    redis.set(f"mp:svc:{SERVICE}:{host}", json.dumps(beat), ex=15)
                except (RedisError, OSError) as exc:
                    log.warning("heartbeat write failed: %s", exc)
            if now - last_retention >= retention_every:
                last_retention = now
                try:
                    log.info("retention run", extra={"fields": {"deleted": store.apply_retention(retention_policies())}})
                except TRANSIENT_DB_ERRORS as exc:
                    log.warning("retention run failed: %s", exc)
    finally:
        log.info("processor stopping", extra={"fields": {"totals": dict(processor.metrics.totals)}})
        consumer.close()
        producer.flush(10)
        store.close()


if __name__ == "__main__":
    main()
