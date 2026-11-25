"""Pipeline monitoring: component status from real checks, metrics from the processor's rows."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.runtime import runtime
from app.repositories import market_data
from app.repositories import pipeline as repo
from app.services import infra
from app.services.ws import manager

COUNTERS = ["events_received", "events_valid", "events_invalid", "events_duplicate", "events_retried"]
GAUGES = ["avg_latency_ms", "max_latency_ms", "db_write_ms", "consumer_lag"]


def _beat_status(beats: list[dict], service: str) -> tuple[str, dict | None]:
    mine = [b for b in beats if b.get("service") == service]
    fresh = [b for b in mine if b["fresh"]]
    if not mine:
        return "down", None
    newest = max(mine, key=lambda b: b["heartbeat_at"])
    if not fresh:
        return "down", newest
    if service == "ingestion" and not str(newest.get("status", "")).startswith(("streaming", "polling")):
        return "degraded", newest
    return "up", newest


async def services() -> dict:
    db, redis, kafka, beats = await asyncio.gather(
        infra.check_database(), infra.check_redis(), infra.kafka_inspector.check(), infra.heartbeats()
    )
    ing_status, ing = _beat_status(beats, "ingestion")
    proc_status, proc = _beat_status(beats, "processor")
    ws = manager.stats()
    api = runtime.request_stats.summary()
    items = [
        {"key": "ingestion", "name": "Data ingestion", "status": ing_status, "latency_ms": None,
         "detail": f"{ing['provider']} ({ing['data_mode']}): {ing['status']}" if ing else "no heartbeat",
         "metrics": {k: ing.get(k) for k in ("events_per_second", "produced", "delivered", "delivery_failed")} if ing else {},
         "last_seen": ing["heartbeat_at"] if ing else None},
        {"key": "kafka", "name": "Kafka broker", "status": kafka["status"], "latency_ms": kafka["latency_ms"],
         "detail": kafka["detail"], "metrics": {"brokers": kafka["brokers"], "consumer_lag": kafka["consumer_lag"],
                                                 "topics": len(kafka["topics"])}, "last_seen": None},
        {"key": "processor", "name": "Stream processor", "status": proc_status, "latency_ms": None,
         "detail": f"{sum(b['service'] == 'processor' and b['fresh'] for b in beats)} instance(s) running" if proc else "no heartbeat",
         "metrics": {"consumer_lag": proc.get("consumer_lag"), "last_event_at": proc.get("last_event_at"),
                     **{k: v for k, v in (proc.get("totals") or {}).items() if k in COUNTERS}} if proc else {},
         "last_seen": proc["heartbeat_at"] if proc else None},
        {"key": "postgres", "name": "PostgreSQL", "status": db["status"], "latency_ms": db["latency_ms"],
         "detail": db["detail"], "metrics": db["pool"], "last_seen": None},
        {"key": "redis", "name": "Redis", "status": redis["status"], "latency_ms": redis["latency_ms"],
         "detail": redis["detail"], "metrics": {"used_memory_bytes": redis["used_memory_bytes"],
                                                 "connected_clients": redis["connected_clients"]}, "last_seen": None},
        {"key": "api", "name": "FastAPI", "status": "up", "latency_ms": api["p50_ms"],
         "detail": f"{api['requests']} requests in window", "metrics": {"p95_ms": api["p95_ms"], "uptime_s": runtime.uptime_s()},
         "last_seen": datetime.now(UTC).isoformat()},
        {"key": "websocket", "name": "WebSocket service", "status": "up" if ws["listener_connected"] else "down",
         "latency_ms": None, "detail": f"{ws['connections']} client(s); pub/sub {'connected' if ws['listener_connected'] else 'disconnected'}",
         "metrics": ws, "last_seen": None},
    ]
    return {"checked_at": datetime.now(UTC).isoformat(), "items": items}


async def overview(session: AsyncSession) -> dict:
    now = datetime.now(UTC)
    day = await repo.metric_aggregates(session, COUNTERS + GAUGES, now - timedelta(hours=24))
    recent = await repo.metric_aggregates(session, GAUGES, now - timedelta(minutes=5))
    minute = await repo.metric_aggregates(session, ["events_valid"], now - timedelta(seconds=60))
    kafka = await infra.kafka_inspector.check()
    s = {k: int(day.get(k, {}).get("sum", 0)) for k in COUNTERS}
    rejected = s["events_invalid"] + s["events_duplicate"]
    total = s["events_received"]
    return {
        "window_hours": 24,
        "instruments_tracked": len(await market_data.list_instruments(session)),
        "events_received": total,
        "events_processed": s["events_valid"],
        "events_failed": s["events_invalid"],
        "events_duplicate": s["events_duplicate"],
        "events_retried": s["events_retried"],
        "events_per_second": round(minute.get("events_valid", {}).get("sum", 0.0) / 60, 2),
        "avg_latency_ms": _r(recent.get("avg_latency_ms", {}).get("avg")),
        "max_latency_ms": _r(recent.get("max_latency_ms", {}).get("max")),
        "db_write_ms": _r(recent.get("db_write_ms", {}).get("avg")),
        "consumer_lag": kafka["consumer_lag"],
        "last_successful_event_at": await market_data.last_processed_at(session),
        "data_quality": {
            "total": total, "valid": s["events_valid"], "invalid": s["events_invalid"],
            "duplicate": s["events_duplicate"], "rejected": rejected,
            "valid_pct": round(s["events_valid"] / total * 100, 3) if total else None,
        },
        "error_breakdown": await repo.error_breakdown(session, now - timedelta(hours=24)),
    }


def _r(x: float | None) -> float | None:
    return round(x, 3) if x is not None else None


async def _series(session: AsyncSession, minutes: int, bucket_s: int) -> dict[str, dict[int, tuple[float, float]]]:
    since = datetime.now(UTC) - timedelta(minutes=minutes)
    out: dict[str, dict[int, tuple[float, float]]] = {}
    for t, name, total, avg in await repo.metric_series(session, COUNTERS + GAUGES, since, bucket_s):
        out.setdefault(name, {})[int(t.timestamp())] = (total, avg)
    return out


def _pts(d: dict[int, float]) -> list[dict]:
    return [{"time": t, "value": round(v, 4)} for t, v in sorted(d.items())]


async def metrics(session: AsyncSession, minutes: int, bucket_s: int) -> dict:
    s = await _series(session, minutes, bucket_s)
    received = {t: v[0] for t, v in s.get("events_received", {}).items()}
    bad = {t: s.get("events_invalid", {}).get(t, (0, 0))[0] + s.get("events_duplicate", {}).get(t, (0, 0))[0] for t in received}
    return {
        "bucket_s": bucket_s,
        "series": {
            "events_per_second": _pts({t: v[0] / bucket_s for t, v in s.get("events_valid", {}).items()}),
            "avg_latency_ms": _pts({t: v[1] for t, v in s.get("avg_latency_ms", {}).items()}),
            "error_rate": _pts({t: bad[t] / n for t, n in received.items() if n > 0}),
            "consumer_lag": _pts({t: v[1] for t, v in s.get("consumer_lag", {}).items()}),
            "db_write_ms": _pts({t: v[1] for t, v in s.get("db_write_ms", {}).items()}),
        },
    }


async def throughput(session: AsyncSession, minutes: int, bucket_s: int) -> dict:
    s = await _series(session, minutes, bucket_s)

    def rate(*names: str) -> list[dict]:
        acc: dict[int, float] = {}
        for n in names:
            for t, v in s.get(n, {}).items():
                acc[t] = acc.get(t, 0.0) + v[0] / bucket_s
        return _pts(acc)

    return {"bucket_s": bucket_s, "received": rate("events_received"), "processed": rate("events_valid"),
            "failed": rate("events_invalid", "events_duplicate")}
