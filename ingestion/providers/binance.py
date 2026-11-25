"""Binance public market data (no API key): closed 1-second klines, data_mode "live".

One combined stream carries every symbol. Each event is one closed second that had trades:
empty seconds (n == 0) and klines still open (x false) are skipped. Event IDs come from
(symbol, kline open time), so a kline seen again after a reconnect gets the same ID and the
processor's duplicate check drops it.

BINANCE_WS_URL picks the host: wss://stream.binance.com:9443 (default),
wss://data-stream.binance.vision (market data only, same feed), or
wss://stream.binance.us:9443 where binance.com is geo-blocked (HTTP 451).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import websockets
from websockets.exceptions import InvalidStatus

from ingestion.providers.base import Message
from ingestion.providers.finnhub import _MAX_BACKOFF_S, _as_bytes, _backoff, _event_id  # shared policy
from ingestion.providers.session import SessionBook

log = logging.getLogger(__name__)
# websockets logs every frame at DEBUG; keep its logger above that.
_WS_LOG = logging.getLogger(f"{__name__}.ws")
_WS_LOG.setLevel(logging.INFO)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_BLOCKED = (403, 451)  # Binance refuses restricted locations at the handshake
DEFAULT_URL = "wss://stream.binance.com:9443"


def stream_url(base_url: str, symbols: list[str]) -> str:
    return f"{base_url.rstrip('/')}/stream?streams=" + "/".join(f"{s.lower()}@kline_1s" for s in symbols)


def map_kline(k: dict, book: SessionBook) -> dict | None:
    """Kline {"s","t","T"(ms),"o","h","l","c","v","n","x"} -> event; None for an empty or still-open second."""
    if not k["x"] or k["n"] == 0:
        return None
    o, h, lo, c, v = (float(k[f]) for f in "ohlcv")
    if not (all(math.isfinite(x) and x > 0 for x in (o, h, lo, c)) and math.isfinite(v) and v >= 0):
        raise ValueError("bad kline values")  # checked before the book, so one bad kline can't poison the session
    ts = _EPOCH + timedelta(milliseconds=k["T"])  # integer ms, so no float rounding in the timestamp
    s = book.apply_bar(k["s"], o, h, lo, c, ts)
    return {
        "event_id": _event_id("binance", k["s"], k["t"]),
        "symbol": k["s"],
        "timestamp": ts.isoformat(timespec="milliseconds"),
        "price": c,
        "volume": v,  # base-asset quantity, fractional
        "open": s.open,
        "high": s.high,
        "low": s.low,
        "close": c,
        "source": "binance",
        "data_mode": "live",
        "schema_version": 1,
    }


def parse_frame(raw: str | bytes, book: SessionBook) -> Message | None:
    """One combined-stream frame -> event, None (skipped second), or the raw bytes when malformed."""
    try:
        return map_kline(json.loads(raw)["data"]["k"], book)
    except (ValueError, KeyError, TypeError, OverflowError, RecursionError):
        return _as_bytes(raw)


class BinanceProvider:
    name = "binance"
    data_mode = "live"

    def __init__(self, symbols: list[str], base_url: str = DEFAULT_URL, connect=websockets.connect) -> None:
        if not symbols:
            raise ValueError("binance needs at least one symbol")
        self._url = stream_url(base_url, symbols)
        self._connect = connect
        self.status = "starting"

    async def stream(self) -> AsyncIterator[Message]:
        book = SessionBook()
        attempt = 0
        while True:
            try:
                async with self._connect(self._url, open_timeout=15, ping_interval=20, max_size=2**20,
                                         logger=_WS_LOG) as ws:
                    self.status = "streaming"
                    attempt = 0
                    async for raw in ws:
                        message = parse_frame(raw, book)
                        if message is not None:
                            yield message
                raise ConnectionError("closed by server")  # clean close (Binance ends every connection at 24 h)
            except (OSError, EOFError, websockets.WebSocketException) as exc:
                code = exc.response.status_code if isinstance(exc, InvalidStatus) else None
                if code in _BLOCKED:
                    delay, self.status = _MAX_BACKOFF_S, f"blocked (HTTP {code}): set BINANCE_WS_URL"
                    log.error("binance refused the connection (HTTP %d, restricted location?); set BINANCE_WS_URL "
                              "to a reachable host. Retrying in %.0fs", code, delay)
                else:
                    delay = _backoff(attempt)
                    attempt += 1
                    self.status = f"reconnecting ({type(exc).__name__})"
                    log.warning("binance websocket error, reconnecting in %.1fs: %s", delay, exc)
            await asyncio.sleep(delay)
