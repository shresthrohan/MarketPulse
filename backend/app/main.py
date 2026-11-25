"""FastAPI application: REST + WebSocket over PostgreSQL (history) and Redis (latest state)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator

import psutil
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api import analytics, market, monitoring, watchlists
from app.core.config import settings
from app.core.logging import setup_logging
from app.core.runtime import RateLimiter, runtime
from app.database.redis import pubsub_client, redis_client
from app.database.session import engine
from app.services.ws import manager, origin_allowed, parse_command

setup_logging()
log = logging.getLogger("api")
limiter = RateLimiter(settings.rate_limit_per_minute)


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    psutil.cpu_percent(None)  # prime: the first reading is always 0
    manager.start()
    log.info("api started")
    yield
    await manager.stop()
    await redis_client.aclose()
    await pubsub_client.aclose()
    await engine.dispose()


app = FastAPI(title="MarketPulse API", version=monitoring.VERSION, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Content-Type"],
)


@app.middleware("http")
async def limit_and_time(request: Request, call_next):
    if request.url.path.startswith("/api/"):
        key = request.client.host if request.client else "unknown"
        retry_after = limiter.check(key, time.monotonic())
        if retry_after is not None:
            return JSONResponse({"detail": "rate limit exceeded"}, status_code=429,
                                headers={"Retry-After": str(max(1, int(retry_after)))})
    t0 = time.perf_counter()
    response = await call_next(request)
    route = request.scope.get("route")
    if route is not None and request.url.path.startswith("/api/"):
        runtime.request_stats.record(f"{request.method} {route.path}", (time.perf_counter() - t0) * 1000)
    return response


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception) -> JSONResponse:
    # Full detail goes to the log; the client gets no internals (connection strings, SQL).
    log.exception("unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse({"detail": "internal server error"}, status_code=500)


for r in (market.instruments, market.router, analytics.router, watchlists.router, monitoring.router, monitoring.health):
    app.include_router(r)


@app.websocket("/ws/market")
async def ws_market(ws: WebSocket) -> None:
    if not origin_allowed(ws.headers.get("origin"), ws.headers.get("host"), settings.cors_origin_list):
        await ws.close(code=1008)  # before accept => handshake rejected (HTTP 403)
        return
    await ws.accept()
    client = manager.register(ws, ws.client.host if ws.client else "unknown")
    if client is None:
        await ws.close(code=1013, reason="connection limit reached")
        return

    async def sender() -> None:
        while True:
            message = await client.queue.get()
            await ws.send_text(message)
            manager.messages_sent += 1

    async def receiver() -> None:
        while True:
            parsed = parse_command(await ws.receive_text())
            if isinstance(parsed, str):
                await ws.send_json({"type": "error", "message": parsed})
                continue
            action, symbols = parsed
            if action == "subscribe":
                client.symbols.update(symbols)
            else:
                client.symbols.difference_update(symbols)
            await ws.send_json({"type": "subscribed", "symbols": sorted(client.symbols)})

    tasks = [asyncio.create_task(sender()), asyncio.create_task(receiver()), asyncio.create_task(client.overflowed.wait())]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if tasks[2] in done:
            with contextlib.suppress(RuntimeError, WebSocketDisconnect):
                await ws.close(code=1013, reason="client too slow")
        for t in done:
            if t is not tasks[2] and not t.cancelled() and t.exception() is not None:
                exc = t.exception()
                if not isinstance(exc, (WebSocketDisconnect, RuntimeError)):
                    log.warning("websocket error: %r", exc)
    finally:
        # Unregister before any await: if this handler is cancelled while waiting below,
        # code after the await never runs and the slot would leak toward the connection cap.
        manager.unregister(client)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

