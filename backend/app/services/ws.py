"""WebSocket fan-out: one Redis pub/sub listener -> per-client bounded queues."""

from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from fastapi import WebSocket
from redis.exceptions import RedisError

from app.core.config import settings
from app.database.redis import TICKS_CHANNEL, pubsub_client

log = logging.getLogger(__name__)
ALL = "*"


@dataclass(eq=False, slots=True)
class Client:
    ws: WebSocket
    queue: asyncio.Queue
    key: str = ""  # client address (after trusted-proxy resolution); bounds connections per client
    symbols: set[str] = field(default_factory=set)
    overflowed: asyncio.Event = field(default_factory=asyncio.Event)

    def wants(self, symbol: str) -> bool:
        return ALL in self.symbols or symbol in self.symbols


class ConnectionManager:
    def __init__(self, max_connections: int, queue_size: int, max_per_client: int | None = None) -> None:
        self.max_connections = max_connections
        self.max_per_client = max_per_client or max_connections
        self.queue_size = queue_size
        self.clients: set[Client] = set()
        self._per_key: dict[str, int] = {}
        self.messages_sent = 0
        self.dropped_clients = 0
        self.listener_connected = False
        self._task: asyncio.Task | None = None

    def register(self, ws: WebSocket, key: str = "") -> Client | None:
        """None when the server or this client is at capacity (one client must not exhaust the global cap)."""
        if len(self.clients) >= self.max_connections or self._per_key.get(key, 0) >= self.max_per_client:
            return None
        c = Client(ws, asyncio.Queue(maxsize=self.queue_size), key=key)
        self.clients.add(c)
        self._per_key[key] = self._per_key.get(key, 0) + 1
        return c

    def unregister(self, c: Client) -> None:
        if c not in self.clients:
            return
        self.clients.discard(c)
        left = self._per_key.get(c.key, 1) - 1
        if left > 0:
            self._per_key[c.key] = left
        else:
            self._per_key.pop(c.key, None)

    def fanout(self, raw: str) -> None:
        try:
            symbol = json.loads(raw).get("symbol")
        except ValueError:
            return
        if not isinstance(symbol, str):
            return
        for c in list(self.clients):
            if not c.wants(symbol):
                continue
            try:
                c.queue.put_nowait(raw)
            except asyncio.QueueFull:
                # A slow client must not grow memory or delay others: disconnect it.
                if not c.overflowed.is_set():
                    self.dropped_clients += 1
                    c.overflowed.set()

    async def _listen(self) -> None:
        attempt = 0
        while True:
            pubsub = pubsub_client.pubsub()
            try:
                await pubsub.subscribe(TICKS_CHANNEL)
                self.listener_connected = True
                attempt = 0
                async for msg in pubsub.listen():
                    if msg.get("type") == "message":
                        self.fanout(msg["data"])
            except (RedisError, OSError) as exc:
                delay = min(30.0, 0.5 * 2**attempt) * random.uniform(0.5, 1.0)
                attempt += 1
                log.warning("pub/sub listener lost (%s), reconnecting in %.1fs", exc, delay)
                await asyncio.sleep(delay)
            finally:
                self.listener_connected = False
                try:
                    await pubsub.aclose()
                except (RedisError, OSError):
                    pass

    def start(self) -> None:
        self._task = asyncio.create_task(self._listen(), name="ws-listener")
        self._task.add_done_callback(self._on_done)

    def _on_done(self, task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception() is not None:
            log.error("ws listener crashed", exc_info=task.exception())
            self.listener_connected = False

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    @property
    def listener_alive(self) -> bool:
        return self._task is not None and not self._task.done() and self.listener_connected

    def stats(self) -> dict:
        return {"connections": len(self.clients), "messages_sent": self.messages_sent,
                "dropped_clients": self.dropped_clients, "listener_connected": self.listener_alive}


manager = ConnectionManager(settings.ws_max_connections, settings.ws_queue_size, settings.ws_max_per_client)


def parse_command(text: str) -> tuple[str, list[str]] | str:
    """Client command -> (action, symbols) or an error message."""
    try:
        msg = json.loads(text)
    except ValueError:
        return "invalid JSON"
    if not isinstance(msg, dict) or msg.get("action") not in ("subscribe", "unsubscribe"):
        return "expected {\"action\": \"subscribe\"|\"unsubscribe\", \"symbols\": [...]}"
    symbols = msg.get("symbols")
    if not isinstance(symbols, list) or not 1 <= len(symbols) <= 100 or not all(isinstance(s, str) and 0 < len(s) <= 10 for s in symbols):
        return "symbols must be a list of 1-100 strings"
    return msg["action"], [s.upper() for s in symbols]


def origin_allowed(origin: str | None, host: str | None, allowed: list[str]) -> bool:
    """WebSockets are not covered by CORS, so the handshake checks Origin itself.

    No Origin = a non-browser client (CLI, tests); browsers always send one. Same-origin
    (page served by the bundled nginx on this host) and configured CORS origins are accepted.
    """
    if not origin:
        return True
    if origin in allowed:
        return True
    return host is not None and urlsplit(origin).netloc == host
