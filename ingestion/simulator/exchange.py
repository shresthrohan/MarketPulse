"""Simulated exchange: turns the price model into market events.

`step(now)` is pure with respect to the clock (time is passed in), so a given
seed + initial prices + sequence of `now` values always yields the same events.
"""

from __future__ import annotations

import copy
import random
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from ingestion.simulator.model import (
    SECONDS_PER_YEAR,
    InstrumentSpec,
    correlated_shocks,
    gbm_step,
    tick_volume,
)

# Fault kinds injected at SIM_FAULT_RATE so the data-quality layer is exercised
# continuously, not only in tests. Each one maps to a processor rejection rule.
FAULT_KINDS = (
    "negative_price",
    "null_price",
    "missing_field",
    "invalid_ohlc",
    "invalid_volume",
    "unknown_symbol",
    "future_timestamp",
    "duplicate",
    "malformed_json",
)

Message = dict | bytes  # dict = event, bytes = raw payload (malformed on purpose)
_ID_NAMESPACE = uuid.UUID("0d4f7a52-3c1e-4b8e-9a6d-2f5c8e1b7a39")


@dataclass(slots=True)
class _Book:
    price: float  # unrounded internal price; emitted prices are rounded from it
    session_day: date | None = None
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    last_event: dict | None = None


class SimulatedExchange:
    def __init__(
        self,
        specs: list[InstrumentSpec],
        *,
        seed: int | None = None,
        speed: float = 1.0,
        interval_s: float = 0.5,
        tick_probability: float = 0.6,
        fault_rate: float = 0.0,
        initial_prices: dict[str, float] | None = None,
    ) -> None:
        if not specs:
            raise ValueError("at least one instrument is required")
        if not 0 < speed <= 10_000:
            raise ValueError("speed must be in (0, 10000]")
        if not 0.01 <= interval_s <= 60:
            raise ValueError("interval_s must be in [0.01, 60]")
        if not 0 < tick_probability <= 1:
            raise ValueError("tick_probability must be in (0, 1]")
        if not 0 <= fault_rate <= 1:
            raise ValueError("fault_rate must be in [0, 1]")
        self._specs = specs
        self._rng = random.Random(seed)
        self._dt = interval_s * speed / SECONDS_PER_YEAR
        self._tick_probability = tick_probability
        self._fault_rate = fault_rate
        # Event IDs = UUIDv5(run, symbol, timestamp, sequence). A seeded run reproduces its own IDs
        # for the same clock, but a restart (later timestamps) never reuses IDs already stored;
        # drawing IDs from the seeded RNG would repeat them and every post-restart event would be
        # rejected as a duplicate. Unseeded runs add a random nonce so two instances never collide.
        self._run = f"seed:{seed}" if seed is not None else f"run:{uuid.uuid4().hex}"
        self._seq = 0
        prices = initial_prices or {}
        self._books = {s.symbol: _Book(price=prices.get(s.symbol, s.base_price)) for s in specs}

    def _event_id(self, symbol: str, ts: str) -> str:
        self._seq += 1
        return str(uuid.uuid5(_ID_NAMESPACE, f"{self._run}|{len(symbol)}:{symbol}|{ts}|{self._seq}"))

    def step(self, now: datetime) -> list[Message]:
        shocks = correlated_shocks(self._rng, [s.factor_loading for s in self._specs])
        out: list[Message] = []
        for spec, z in zip(self._specs, shocks, strict=True):
            book = self._books[spec.symbol]
            # The walk advances every step, traded or not, so the random stream
            # is consumed identically regardless of which instruments tick.
            book.price = gbm_step(book.price, spec.volatility, self._dt, z)
            traded = self._rng.random() < self._tick_probability
            volume = tick_volume(self._rng, spec.tick_volume)
            faulty = self._rng.random() < self._fault_rate
            fault_kind = self._rng.choice(FAULT_KINDS)
            if not traded:
                continue
            price = round(max(book.price, 0.01), 2)
            if faulty:
                out.append(self._fault(fault_kind, spec, book, price, volume, now))
                continue
            if book.session_day != now.date():
                book.session_day = now.date()
                book.open = book.high = book.low = price
            book.high = max(book.high, price)
            book.low = min(book.low, price)
            event = {
                "event_id": self._event_id(spec.symbol, now.isoformat(timespec="milliseconds")),
                "symbol": spec.symbol,
                "timestamp": now.isoformat(timespec="milliseconds"),
                "price": price,
                "volume": volume,
                "open": book.open,
                "high": book.high,
                "low": book.low,
                "close": price,
                "source": "simulator",
                "data_mode": "simulated",
                "schema_version": 1,
            }
            book.last_event = event
            out.append(event)
        return out

    def _fault(self, kind: str, spec: InstrumentSpec, book: _Book, price: float, volume: int, now: datetime) -> Message:
        base = {
            "event_id": self._event_id(spec.symbol, now.isoformat(timespec="milliseconds")),
            "symbol": spec.symbol,
            "timestamp": now.isoformat(timespec="milliseconds"),
            "price": price,
            "volume": volume,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "source": "simulator",
            "data_mode": "simulated",
            "schema_version": 1,
        }
        match kind:
            case "negative_price":
                base["price"] = base["close"] = -price
            case "null_price":
                base["price"] = None
            case "missing_field":
                del base["volume"]
            case "invalid_ohlc":
                base["high"], base["low"] = price * 0.99, price * 1.01
            case "invalid_volume":
                base["volume"] = -volume
            case "unknown_symbol":
                base["symbol"] = "ZZZZ"
            case "future_timestamp":
                base["timestamp"] = (now + timedelta(hours=1)).isoformat(timespec="milliseconds")
            case "duplicate":
                if book.last_event is not None:
                    return copy.deepcopy(book.last_event)
                base["price"] = None  # nothing to duplicate yet; emit a rejectable event instead
            case "malformed_json":
                return b'{"event_id": "truncated", "symbol": "' + spec.symbol.encode() + b'", "price": '
            case _:
                raise ValueError(f"unknown fault kind {kind}")
        return base
