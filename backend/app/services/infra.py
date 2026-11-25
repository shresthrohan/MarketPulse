"""Real checks against each dependency. Every check has a caller-set deadline."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import settings
from app.database.redis import HEARTBEAT_PATTERN, redis_client
from app.database.session import engine

log = logging.getLogger(__name__)
CHECK_TIMEOUT_S = 3.0


def _ms(t0: float) -> float:
    return round((time.perf_counter() - t0) * 1000, 2)


async def check_database() -> dict:
    t0 = time.perf_counter()
    pool = engine.pool
    # overflow() is negative while the pool has not yet opened all `size` connections.
    stats = {"size": pool.size(), "checked_out": pool.checkedout(), "overflow": max(0, pool.overflow())}
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_S):
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        return {"status": "up", "latency_ms": _ms(t0), "detail": "SELECT 1 ok", "pool": stats}
    except (SQLAlchemyError, OSError, TimeoutError) as exc:
        return {"status": "down", "latency_ms": None, "detail": f"{type(exc).__name__}: {exc}"[:300], "pool": stats}


async def check_redis() -> dict:
    t0 = time.perf_counter()
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_S):
            await redis_client.ping()
            latency = _ms(t0)
            info = await redis_client.info()
        return {"status": "up", "latency_ms": latency, "detail": f"redis {info.get('redis_version')}",
                "used_memory_bytes": info.get("used_memory"), "connected_clients": info.get("connected_clients")}
    except (RedisError, OSError, TimeoutError) as exc:
        return {"status": "down", "latency_ms": None, "detail": f"{type(exc).__name__}: {exc}"[:300],
                "used_memory_bytes": None, "connected_clients": None}


async def heartbeats() -> list[dict]:
    """Service heartbeats written by ingestion/processor (TTL'd keys, so absence == not running)."""
    out = []
    try:
        async with asyncio.timeout(CHECK_TIMEOUT_S):
            keys = [k async for k in redis_client.scan_iter(match=HEARTBEAT_PATTERN, count=100)]
            values = await redis_client.mget(keys) if keys else []
    except (RedisError, OSError, TimeoutError) as exc:
        log.warning("heartbeat read failed: %s", exc)
        return []
    now = datetime.now(UTC)
    for value in values:
        if not value:
            continue
        try:
            beat = json.loads(value)
            age = (now - datetime.fromisoformat(beat["heartbeat_at"])).total_seconds()
            started = datetime.fromisoformat(beat["started_at"])
        except (ValueError, KeyError, TypeError):
            continue
        beat["age_s"] = round(age, 2)
        beat["uptime_s"] = round((now - started).total_seconds(), 1)
        beat["fresh"] = age <= settings.heartbeat_stale_s
        out.append(beat)
    return out


class KafkaInspector:
    """Broker metadata + consumer-group lag, read without joining the group.

    confluent-kafka calls block, so they run in a worker thread with their own
    timeouts (an abandoned thread finishes within those). One check in flight at a
    time; results are cached briefly so dashboards polling many endpoints cost one check.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._alock = asyncio.Lock()
        self._admin: AdminClient | None = None
        self._consumer: Consumer | None = None
        self._cached: tuple[float, dict] | None = None

    def _clients(self) -> tuple[AdminClient, Consumer]:
        if self._admin is None or self._consumer is None:
            conf = {"bootstrap.servers": settings.kafka_bootstrap_servers, "socket.timeout.ms": 3000}
            self._admin = AdminClient(conf)
            self._consumer = Consumer({**conf, "group.id": settings.consumer_group, "enable.auto.commit": False})
        return self._admin, self._consumer

    def _check(self) -> dict:
        with self._lock:
            t0 = time.perf_counter()
            topics_wanted = settings.topic_list
            try:
                admin, consumer = self._clients()
                md = admin.list_topics(timeout=CHECK_TIMEOUT_S)
                latency = _ms(t0)
                topics = {t: len(md.topics[t].partitions) for t in topics_wanted if t in md.topics}
                missing = [t for t in topics_wanted if t not in md.topics]
                lag = end_total = committed_total = None
                raw = topics_wanted[0]
                if raw in md.topics:
                    tps = [TopicPartition(raw, p) for p in md.topics[raw].partitions]
                    lag = end_total = committed_total = 0
                    for tp in consumer.committed(tps, timeout=CHECK_TIMEOUT_S):
                        low, high = consumer.get_watermark_offsets(tp, timeout=CHECK_TIMEOUT_S, cached=False)
                        position = tp.offset if tp.offset >= 0 else low  # nothing committed yet: start of log
                        end_total += high
                        committed_total += position
                        lag += max(0, high - position)
            except KafkaException as exc:
                return {"status": "down", "latency_ms": None, "detail": str(exc)[:300], "brokers": 0,
                        "topics": {}, "missing_topics": topics_wanted, "consumer_lag": None}
            status = "up" if md.brokers and not missing else "degraded"
            detail = f"{len(md.brokers)} broker(s), {len(topics)}/{len(topics_wanted)} topics"
            return {"status": status, "latency_ms": latency, "detail": detail, "brokers": len(md.brokers),
                    "topics": topics, "missing_topics": missing, "consumer_lag": lag,
                    # Reconciliation on market.raw: end offsets = committed (processed) + lag (pending).
                    "raw_end_offsets": end_total, "raw_committed_offsets": committed_total}

    async def check(self) -> dict:
        async with self._alock:
            if self._cached and time.monotonic() - self._cached[0] < 2.0:
                return self._cached[1]
            try:
                result = await asyncio.wait_for(asyncio.to_thread(self._check), timeout=4 * CHECK_TIMEOUT_S)
            except TimeoutError:
                result = {"status": "down", "latency_ms": None, "detail": "kafka check timed out", "brokers": 0,
                          "topics": {}, "missing_topics": settings.topic_list, "consumer_lag": None}
            self._cached = (time.monotonic(), result)
            return result


kafka_inspector = KafkaInspector()
