"""Data-quality rules for raw market events. Pure: no I/O, clock passed in.

Every raw payload becomes exactly one of ValidEvent | Rejected, so nothing is
silently dropped: rejects are persisted and published to `market.errors`.
"""

from __future__ import annotations

import json
import math
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum


class ErrorType(StrEnum):
    MALFORMED_JSON = "malformed_json"
    MISSING_FIELD = "missing_field"
    INVALID_FIELD = "invalid_field"
    NULL_PRICE = "null_price"
    NEGATIVE_PRICE = "negative_price"
    INVALID_PRICE = "invalid_price"
    INVALID_VOLUME = "invalid_volume"
    INVALID_TIMESTAMP = "invalid_timestamp"
    INVALID_OHLC = "invalid_ohlc"
    UNKNOWN_SYMBOL = "unknown_symbol"
    DUPLICATE_EVENT = "duplicate_event"
    DB_REJECTED = "db_rejected"


def severity(error_type: ErrorType) -> str:
    # A duplicate is the pipeline working as designed (replays, provider resends).
    return "warning" if error_type is ErrorType.DUPLICATE_EVENT else "error"


REQUIRED_FIELDS = ("event_id", "symbol", "timestamp", "price", "volume", "open", "high", "low", "close", "source", "data_mode")
DATA_MODES = frozenset({"simulated", "live", "delayed"})
MAX_PRICE = 1e9  # NUMERIC(18,6) holds 1e12; anything near that is a feed bug
MAX_VOLUME = 10**12
MAX_PAYLOAD_BYTES = 16 * 1024  # a real event is ~300 bytes; bounds JSON parse cost
_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
_EPS = 1e-9


@dataclass(frozen=True, slots=True)
class ValidEvent:
    event_id: uuid.UUID
    symbol: str
    timestamp: datetime  # UTC
    price: float
    volume: float  # fractional for crypto (base-asset quantity)
    open: float
    high: float
    low: float
    close: float
    source: str
    data_mode: str
    ingested_at: datetime | None


def _clean(value: str | None, limit: int) -> str | None:
    # PostgreSQL text cannot hold NUL, and column widths are fixed: an unclean
    # reject would fail its own insert and turn into a poison message.
    return None if value is None else value.replace("\x00", "")[:limit]


@dataclass(frozen=True, slots=True)
class Rejected:
    error_type: ErrorType
    message: str
    event_id: str | None
    symbol: str | None
    source: str | None
    raw: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "message", _clean(self.message, 1000))
        object.__setattr__(self, "event_id", _clean(self.event_id, 64))
        object.__setattr__(self, "symbol", _clean(self.symbol, 16))
        object.__setattr__(self, "source", _clean(self.source, 64))
        object.__setattr__(self, "raw", _clean(self.raw, 2048))


@dataclass(frozen=True, slots=True)
class Rules:
    known_symbols: frozenset[str]
    max_future_skew: timedelta = timedelta(seconds=30)  # tolerates host clock drift; still rejects real faults
    max_age: timedelta = timedelta(days=1)
    # symbol -> the instrument's established data mode (instruments.data_mode). An instrument never mixes
    # simulated and real data, so its charts and analytics are never a blend of both.
    instrument_modes: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.max_future_skew < timedelta(0) or self.max_age <= timedelta(0):
            raise ValueError("timestamp tolerances must be positive")


def data_class(mode: str) -> str:
    """'simulated' or 'real' (live and delayed are both real market data)."""
    return "simulated" if mode == "simulated" else "real"


def _number(v: object) -> float | None:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    try:
        return float(v)
    except OverflowError:  # JSON allows integers far beyond float range
        return None


def _parse_ts(v: object) -> datetime | None:
    if not isinstance(v, str):
        return None
    try:
        ts = datetime.fromisoformat(v)
        if ts.tzinfo is None:
            return None  # a naive timestamp has no defined instant
        return ts.astimezone(UTC)
    except (ValueError, OverflowError):  # e.g. year 1 with a positive offset has no UTC equivalent
        return None


def validate(raw: bytes, rules: Rules, now: datetime) -> ValidEvent | Rejected:
    """Total function: must return for every byte string (a raise here would block a partition).
    tests/test_pure.py fuzzes it with random payloads."""
    text = raw[:2048].decode("utf-8", "replace")
    if len(raw) > MAX_PAYLOAD_BYTES:
        return Rejected(ErrorType.MALFORMED_JSON, f"payload exceeds {MAX_PAYLOAD_BYTES} bytes", None, None, None, text)
    try:
        doc = json.loads(raw)
    except (ValueError, RecursionError):  # JSONDecodeError and UnicodeDecodeError are ValueErrors
        return Rejected(ErrorType.MALFORMED_JSON, "payload is not valid JSON", None, None, None, text)
    if not isinstance(doc, dict):
        return Rejected(ErrorType.MALFORMED_JSON, "payload is not a JSON object", None, None, None, text)

    event_id = doc.get("event_id") if isinstance(doc.get("event_id"), str) else None
    symbol = doc.get("symbol").strip().upper() if isinstance(doc.get("symbol"), str) else None
    source = doc.get("source") if isinstance(doc.get("source"), str) else None

    def reject(kind: ErrorType, message: str) -> Rejected:
        return Rejected(kind, message, event_id, symbol, source, text)

    missing = [f for f in REQUIRED_FIELDS if f not in doc]
    if missing:
        return reject(ErrorType.MISSING_FIELD, f"missing fields: {', '.join(missing)}")
    try:
        parsed_id = uuid.UUID(event_id) if event_id else None
    except ValueError:
        parsed_id = None
    if parsed_id is None:
        return reject(ErrorType.INVALID_FIELD, "event_id must be a UUID string")
    if symbol is None or not _SYMBOL_RE.match(symbol):
        return reject(ErrorType.INVALID_FIELD, "symbol must be 1-10 chars, A-Z 0-9 . -")
    if not source or len(source) > 64 or not source.isprintable():
        return reject(ErrorType.INVALID_FIELD, "source must be 1-64 printable chars")
    if not isinstance(doc["data_mode"], str) or doc["data_mode"] not in DATA_MODES:
        return reject(ErrorType.INVALID_FIELD, f"data_mode must be one of {sorted(DATA_MODES)}")

    if doc["price"] is None:
        return reject(ErrorType.NULL_PRICE, "price is null")
    price = _number(doc["price"])
    if price is None:
        return reject(ErrorType.INVALID_PRICE, "price must be a number")
    if not math.isfinite(price) or price == 0 or abs(price) > MAX_PRICE:
        return reject(ErrorType.INVALID_PRICE, f"price {price} is zero, non-finite or out of range")
    if price < 0:
        return reject(ErrorType.NEGATIVE_PRICE, f"price {price} is negative")

    vol = doc["volume"]
    volume = _number(vol)
    if volume is None or not math.isfinite(volume) or not 0 <= volume <= MAX_VOLUME:
        return reject(ErrorType.INVALID_VOLUME, f"volume {vol!r} must be a number in [0, {MAX_VOLUME}]")

    ohlc = {k: _number(doc[k]) for k in ("open", "high", "low", "close")}
    bad = [k for k, v in ohlc.items() if v is None or not math.isfinite(v) or v <= 0 or v > MAX_PRICE]
    if bad:
        return reject(ErrorType.INVALID_OHLC, f"{', '.join(bad)} must be positive numbers")
    o, h, lo, c = ohlc["open"], ohlc["high"], ohlc["low"], ohlc["close"]
    if not (lo <= h + _EPS and lo - _EPS <= o <= h + _EPS and lo - _EPS <= c <= h + _EPS and lo - _EPS <= price <= h + _EPS):
        return reject(ErrorType.INVALID_OHLC, f"violates low <= open/close/price <= high (o={o} h={h} l={lo} c={c} p={price})")

    ts = _parse_ts(doc["timestamp"])
    if ts is None:
        return reject(ErrorType.INVALID_TIMESTAMP, "timestamp must be ISO-8601 with a UTC offset")
    if ts > now + rules.max_future_skew:
        return reject(ErrorType.INVALID_TIMESTAMP, f"timestamp {ts.isoformat()} is in the future")
    if ts < now - rules.max_age:
        return reject(ErrorType.INVALID_TIMESTAMP, f"timestamp {ts.isoformat()} is older than {rules.max_age}")

    if symbol not in rules.known_symbols:
        return reject(ErrorType.UNKNOWN_SYMBOL, f"symbol {symbol} is not a tracked instrument")
    established = rules.instrument_modes.get(symbol)
    if established is not None and data_class(established) != data_class(doc["data_mode"]):
        return reject(ErrorType.INVALID_FIELD,
                      f"{symbol} holds {established} data; {doc['data_mode']} data for it needs a fresh database "
                      "(see docs/deployment.md, switching data sources)")

    return ValidEvent(
        event_id=parsed_id,
        symbol=symbol,
        timestamp=ts,
        price=price,
        volume=volume,
        open=o,
        high=h,
        low=lo,
        close=c,
        source=source,
        data_mode=doc["data_mode"],
        ingested_at=_parse_ts(doc.get("ingested_at")),
    )


def duplicate_of(event: ValidEvent, where: str) -> Rejected:
    return Rejected(
        ErrorType.DUPLICATE_EVENT,
        f"event_id already {where}",
        str(event.event_id),
        event.symbol,
        event.source,
        "",
    )
