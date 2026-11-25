"""Finnhub adapters (real market data; needs FINNHUB_API_KEY).

- FinnhubWebSocketProvider: streaming trades (data_mode "live").
- FinnhubRestProvider: polled quotes (data_mode "delayed", up to one poll interval old).

Both map into the same event schema as the simulator. Event IDs are derived from
what the trade *is* (symbol, exchange time, price, size), so a reconnect that
replays trades produces the same IDs and the processor's duplicate check catches them.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import websockets
from websockets.exceptions import InvalidStatus

from ingestion.providers.base import Message
from ingestion.providers.session import SessionBook

log = logging.getLogger(__name__)
# websockets logs the request path (which carries ?token=) at DEBUG; keep its logger above that.
_WS_LOG = logging.getLogger(f"{__name__}.ws")
_WS_LOG.setLevel(logging.INFO)
_NAMESPACE = uuid.UUID("6f1c2b1e-9d1a-4c55-9a43-1f2d3e4b5a60")
_MAX_BACKOFF_S = 60.0
_UNAUTHORIZED = (401, 403)


def _event_id(*parts: object) -> str:
    # Length-prefixed so ("AB", "1") and ("A", "B1") cannot collide.
    key = "|".join(f"{len(str(p))}:{p}" for p in parts)
    return str(uuid.uuid5(_NAMESPACE, key))


def map_trade(trade: dict, book: SessionBook) -> dict:
    """Finnhub trade {"s","p","t"(ms),"v"} -> MarketPulse event."""
    ts = datetime.fromtimestamp(trade["t"] / 1000, UTC)
    price = float(trade["p"])
    s = book.apply(trade["s"], price, ts)
    return {
        "event_id": _event_id("finnhub", trade["s"], trade["t"], trade["p"], trade["v"]),
        "symbol": trade["s"],
        "timestamp": ts.isoformat(timespec="milliseconds"),
        "price": price,
        "volume": int(trade["v"]),
        "open": s.open,
        "high": s.high,
        "low": s.low,
        "close": price,
        "source": "finnhub",
        "data_mode": "live",
        "schema_version": 1,
    }


def map_quote(symbol: str, q: dict) -> dict:
    """Finnhub /quote {"c","h","l","o","t"(s)} -> event. Quotes carry no volume."""
    ts = datetime.fromtimestamp(q["t"], UTC)
    return {
        "event_id": _event_id("finnhub-quote", symbol, q["t"], q["c"]),
        "symbol": symbol,
        "timestamp": ts.isoformat(timespec="milliseconds"),
        "price": q["c"],
        "volume": 0,
        "open": q["o"],
        "high": q["h"],
        "low": q["l"],
        "close": q["c"],
        "source": "finnhub",
        "data_mode": "delayed",
        "schema_version": 1,
    }


def _backoff(attempt: int) -> float:
    return min(_MAX_BACKOFF_S, 1.0 * 2**attempt) * random.uniform(0.5, 1.0)


def _retry_after(value: str | None) -> float:
    try:  # constant-first min/max also maps NaN to 1
        return min(_MAX_BACKOFF_S, max(1.0, float(value)))
    except (TypeError, ValueError):  # missing, or the HTTP-date form: wait the maximum
        return _MAX_BACKOFF_S


def _redact(text: object, secret: str) -> str:
    """str(text) with the API key masked. Everything logged or put in `status` goes through this."""
    text = str(text)
    return text.replace(secret, "***") if secret else text


def _as_bytes(raw: str | bytes) -> bytes:
    return raw.encode() if isinstance(raw, str) else raw


class FinnhubWebSocketProvider:
    name = "finnhub_ws"
    data_mode = "live"

    def __init__(self, api_key: str, symbols: list[str], connect=websockets.connect) -> None:
        self._key = api_key
        self._url = f"wss://ws.finnhub.io?token={api_key}"  # Finnhub's WS only accepts the token here
        self._symbols = symbols
        self._connect = connect
        self.status = "starting"

    async def stream(self) -> AsyncIterator[Message]:
        book = SessionBook()
        attempt = 0
        auth_logged = False
        while True:
            try:
                async with self._connect(self._url, open_timeout=15, ping_interval=20, logger=_WS_LOG) as ws:
                    for symbol in self._symbols:
                        await ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
                    self.status = "streaming"
                    attempt, auth_logged = 0, False
                    async for raw in ws:
                        for message in self._frame(raw, book):
                            yield message
                raise ConnectionError("closed by server")  # clean close: reconnect with backoff too
            except (OSError, EOFError, websockets.WebSocketException) as exc:
                if isinstance(exc, InvalidStatus) and exc.response.status_code in _UNAUTHORIZED:
                    delay, self.status = _MAX_BACKOFF_S, "unauthorized"
                    if not auth_logged:
                        auth_logged = True
                        log.error("finnhub rejected the API key (HTTP %d); retrying every %.0fs",
                                  exc.response.status_code, delay)
                else:
                    delay = _backoff(attempt)
                    attempt += 1
                    self.status = f"reconnecting ({type(exc).__name__})"
                    log.warning("finnhub websocket error, reconnecting in %.1fs: %s", delay, _redact(exc, self._key))
            await asyncio.sleep(delay)

    def _frame(self, raw: str | bytes, book: SessionBook) -> list[Message]:
        """One frame -> messages. Malformed input is forwarded as bytes; the connection is kept."""
        try:
            msg = json.loads(raw)
        except (ValueError, RecursionError):  # deeply nested JSON must not crash the pump
            msg = None
        if not isinstance(msg, dict):
            return [_as_bytes(raw)]
        kind = msg.get("type")
        if kind == "error":
            detail = _redact(msg.get("msg", ""), self._key)[:100]
            self.status = f"provider error: {detail}"
            log.warning("finnhub websocket error message: %s", detail)
            return []
        if kind != "trade":  # ping and anything else carries no data
            return []
        trades = msg.get("data")
        if not isinstance(trades, list):
            return [_as_bytes(raw)]
        out: list[Message] = []
        for trade in trades:
            try:
                out.append(map_trade(trade, book))
            except (KeyError, TypeError, ValueError, OverflowError, OSError):
                out.append(_as_bytes(raw))
        return out


class FinnhubRestProvider:
    name = "finnhub_rest"
    data_mode = "delayed"

    def __init__(self, api_key: str, symbols: list[str], poll_interval_s: float,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        if poll_interval_s < len(symbols):  # free tier: 60 requests/minute
            raise ValueError("poll interval must allow >= 1s per symbol (Finnhub free tier is 60 req/min)")
        self._key = api_key
        self._symbols = symbols
        self._poll_interval_s = poll_interval_s
        self._transport = transport
        self._last: dict[str, tuple] = {}
        self.status = "starting"

    async def stream(self) -> AsyncIterator[Message]:
        attempt = 0
        auth_logged = False
        # Key in a header, never the URL: httpx logs URLs and puts them in HTTPStatusError messages.
        async with httpx.AsyncClient(base_url="https://finnhub.io/api/v1", timeout=10.0, transport=self._transport,
                                     headers={"X-Finnhub-Token": self._key}) as client:
            while True:
                try:
                    for symbol in self._symbols:
                        r = await client.get("/quote", params={"symbol": symbol})
                        r.raise_for_status()
                        message = self._quote(symbol, r.content)
                        if message is not None:
                            yield message
                    self.status = "polling"
                    attempt, auth_logged = 0, False
                    delay = self._poll_interval_s
                except httpx.HTTPError as exc:
                    # ponytail: a failure restarts the poll from the first symbol; per-symbol retry if one symbol 5xxs forever
                    code = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
                    if code in _UNAUTHORIZED:
                        delay, self.status = _MAX_BACKOFF_S, "unauthorized"
                        if not auth_logged:
                            auth_logged = True
                            log.error("finnhub rejected the API key (HTTP %d); retrying every %.0fs", code, delay)
                    elif code == 429:
                        delay, self.status = _retry_after(exc.response.headers.get("Retry-After")), "rate_limited"
                        log.warning("finnhub rate limit hit, retrying in %.0fs", delay)
                    else:
                        delay = _backoff(attempt)
                        attempt += 1
                        self.status = f"retrying ({type(exc).__name__})"
                        log.warning("finnhub quote poll failed, retrying in %.1fs: %s", delay, _redact(exc, self._key))
                await asyncio.sleep(delay)

    def _quote(self, symbol: str, body: bytes) -> Message | None:
        """Body -> event, None (no data / unchanged), or the raw bytes when malformed or missing fields."""
        try:
            q = json.loads(body)
            event = map_quote(symbol, q)  # reads c, h, l, o, t
        except (ValueError, KeyError, TypeError, OverflowError, OSError):
            return body
        if not q["t"] or self._last.get(symbol) == (q["t"], q["c"]):
            return None  # market closed / unchanged: nothing new to report
        self._last[symbol] = (q["t"], q["c"])
        return event
