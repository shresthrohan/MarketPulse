import asyncio
import os
import platform
import threading
from datetime import UTC, datetime
from typing import Literal

import psutil
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.runtime import runtime
from app.database.session import get_session
from app.repositories import pipeline as repo
from app.services import infra, pipeline
from app.services.ws import manager

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])
health = APIRouter(prefix="/api/health", tags=["health"])
VERSION = "1.0.0"
_proc = psutil.Process()


@router.get("/overview")
async def overview(session: AsyncSession = Depends(get_session)):
    return await pipeline.overview(session)


@router.get("/services")
async def services():
    return await pipeline.services()


@router.get("/metrics")
async def metrics(minutes: int = Query(60, ge=5, le=1440), bucket_s: int = Query(60, ge=10, le=3600),
                  session: AsyncSession = Depends(get_session)):
    return await pipeline.metrics(session, minutes, bucket_s)


@router.get("/throughput")
async def throughput(minutes: int = Query(60, ge=5, le=1440), bucket_s: int = Query(60, ge=10, le=3600),
                     session: AsyncSession = Depends(get_session)):
    return await pipeline.throughput(session, minutes, bucket_s)


@router.get("/errors")
async def errors(error_type: str | None = Query(None, max_length=32), symbol: str | None = Query(None, max_length=16),
                 search: str | None = Query(None, max_length=100), page: int = Query(1, ge=1, le=1000),
                 page_size: int = Query(50, ge=1, le=200), session: AsyncSession = Depends(get_session)):
    items = await repo.errors_page(session, error_type, symbol, search, page_size + 1, (page - 1) * page_size)
    return {"items": items[:page_size], "page": page, "page_size": page_size, "has_more": len(items) > page_size}


@router.get("/events")
async def events(view: Literal["all", "success", "failed", "retried"] = "all",
                 search: str | None = Query(None, max_length=100), symbol: str | None = Query(None, max_length=16),
                 severity: Literal["info", "warning", "error"] | None = None, page: int = Query(1, ge=1, le=200),
                 page_size: int = Query(50, ge=1, le=200), session: AsyncSession = Depends(get_session)):
    items = await repo.event_log(session, view, search, symbol, severity, page_size + 1, (page - 1) * page_size)
    return {"items": items[:page_size], "page": page, "page_size": page_size, "has_more": len(items) > page_size}


def _component(c: dict) -> dict:
    return {"status": c["status"], "latency_ms": c.get("latency_ms"), "detail": c.get("detail", "")}


def _with_status(body: dict) -> JSONResponse:
    return JSONResponse(body, status_code=200 if body["status"] != "down" else 503)


@health.get("")
async def health_all():
    db, redis, kafka, beats = await asyncio.gather(
        infra.check_database(), infra.check_redis(), infra.kafka_inspector.check(), infra.heartbeats()
    )
    comps = {"database": _component(db), "redis": _component(redis), "kafka": _component(kafka)}
    for svc in ("ingestion", "processor"):
        mine = [b for b in beats if b.get("service") == svc and b["fresh"]]
        comps[svc] = {"status": "up" if mine else "down", "latency_ms": None,
                      "detail": f"heartbeat {min(b['age_s'] for b in mine)}s ago" if mine else "no fresh heartbeat"}
    ws = manager.stats()
    comps["websocket"] = {"status": "up" if ws["listener_connected"] else "down", "latency_ms": None,
                          "detail": f"{ws['connections']} client(s)"}
    statuses = {c["status"] for c in comps.values()}
    core_down = any(comps[k]["status"] == "down" for k in ("database",))
    overall = "down" if core_down else ("ok" if statuses == {"up"} else "degraded")
    return {"status": overall, "version": VERSION, "uptime_s": runtime.uptime_s(), "components": comps}


@health.get("/database")
async def health_database():
    return _with_status(await infra.check_database())


@health.get("/redis")
async def health_redis():
    return _with_status(await infra.check_redis())


@health.get("/kafka")
async def health_kafka():
    return _with_status(await infra.kafka_inspector.check())


@health.get("/system")
async def health_system():
    vm = psutil.virtual_memory()
    try:
        load = list(os.getloadavg())
    except (AttributeError, OSError):
        load = None
    beats = await infra.heartbeats()
    return {
        "started_at": runtime.started_at.isoformat(),
        "uptime_s": runtime.uptime_s(),
        "python": platform.python_version(),
        "process": {"cpu_percent": _proc.cpu_percent(None), "rss_bytes": _proc.memory_info().rss,
                    "threads": threading.active_count()},
        "host": {"cpu_percent": psutil.cpu_percent(None), "cpu_count": psutil.cpu_count(),
                 "memory_total_bytes": vm.total, "memory_used_bytes": vm.total - vm.available,
                 "memory_percent": vm.percent, "load_avg": load},
        "api": runtime.request_stats.summary(),
        "websocket": manager.stats(),
        "services": [
            {"key": b.get("service"), "host": b.get("host"), "status": "up" if b["fresh"] else "down",
             "cpu_percent": b.get("cpu_percent"), "rss_bytes": b.get("rss_bytes"), "uptime_s": b["uptime_s"],
             "heartbeat_at": b.get("heartbeat_at")}
            for b in beats
        ],
        "checked_at": datetime.now(UTC).isoformat(),
    }
