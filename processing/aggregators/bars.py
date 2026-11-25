"""Collapse a batch of events into per-bucket OHLCV deltas. Pure.

The database merges each delta into the stored bar (see repository.UPSERT_BARS),
and only for events that were newly inserted, so replays never double-count volume.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from processing.validators.event_validator import ValidEvent

INTERVAL_SECONDS = {"1m": 60, "1d": 86400}


def bucket_start(ts: datetime, interval: str) -> datetime:
    step = INTERVAL_SECONDS[interval]
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(epoch - epoch % step, UTC)


@dataclass(frozen=True, slots=True)
class BarDelta:
    symbol: str
    interval: str
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    first_at: datetime
    last_at: datetime


def aggregate(events: Iterable[ValidEvent]) -> list[BarDelta]:
    bars: dict[tuple[str, str, datetime], BarDelta] = {}
    for e in events:
        for interval in INTERVAL_SECONDS:
            key = (e.symbol, interval, bucket_start(e.timestamp, interval))
            b = bars.get(key)
            if b is None:
                bars[key] = BarDelta(e.symbol, interval, key[2], e.price, e.price, e.price, e.price, e.volume, e.timestamp, e.timestamp)
                continue
            first = e.timestamp < b.first_at
            last = e.timestamp >= b.last_at
            bars[key] = BarDelta(
                e.symbol,
                interval,
                key[2],
                e.price if first else b.open,
                max(b.high, e.price),
                min(b.low, e.price),
                e.price if last else b.close,
                b.volume + e.volume,
                e.timestamp if first else b.first_at,
                e.timestamp if last else b.last_at,
            )
    return list(bars.values())
