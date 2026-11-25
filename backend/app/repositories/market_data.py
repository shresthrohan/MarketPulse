"""SQL for instruments, bars and stored events. All values bound as parameters."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import String, cast, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tables import HistoricalAggregate, Instrument, MarketEvent


async def list_instruments(session: AsyncSession) -> list[Instrument]:
    return list((await session.execute(select(Instrument).order_by(Instrument.symbol))).scalars())


async def get_instrument(session: AsyncSession, symbol: str) -> Instrument | None:
    return (await session.execute(select(Instrument).where(Instrument.symbol == symbol.upper()))).scalar_one_or_none()


async def instruments_by_symbol(session: AsyncSession, symbols: list[str]) -> dict[str, Instrument]:
    rows = await session.execute(select(Instrument).where(Instrument.symbol.in_(symbols)))
    return {i.symbol: i for i in rows.scalars()}


_BUCKETED_BARS = text(
    """
    SELECT date_bin(make_interval(secs => :bucket), "timestamp", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS bucket,
           (array_agg(open ORDER BY "timestamp"))[1] AS open, max(high) AS high, min(low) AS low,
           (array_agg(close ORDER BY "timestamp" DESC))[1] AS close, sum(volume) AS volume
    FROM historical_aggregates
    WHERE instrument_id = :iid AND "interval" = :iv AND "timestamp" >= :start
    GROUP BY 1 ORDER BY 1
    """
)


async def bars(session: AsyncSession, instrument_id: int, interval: str, start: datetime, bucket_s: int | None) -> list[dict]:
    if bucket_s:
        rows = await session.execute(_BUCKETED_BARS, {"bucket": float(bucket_s), "iid": instrument_id, "iv": interval, "start": start})
    else:
        h = HistoricalAggregate
        rows = await session.execute(
            select(h.timestamp.label("bucket"), h.open, h.high, h.low, h.close, h.volume)
            .where(h.instrument_id == instrument_id, h.interval == interval, h.timestamp >= start)
            .order_by(h.timestamp)
        )
    return [
        {"time": int(r.bucket.timestamp()), "open": float(r.open), "high": float(r.high), "low": float(r.low),
         "close": float(r.close), "volume": float(r.volume)}
        for r in rows
    ]


async def daily_bars(session: AsyncSession, instrument_ids: list[int], start: datetime) -> list[tuple[int, datetime, float, float]]:
    h = HistoricalAggregate
    rows = await session.execute(
        select(h.instrument_id, h.timestamp, h.close, h.volume)
        .where(h.instrument_id.in_(instrument_ids), h.interval == "1d", h.timestamp >= start)
        .order_by(h.timestamp)
    )
    return [(r.instrument_id, r.timestamp, float(r.close), float(r.volume)) for r in rows]


@dataclass(frozen=True, slots=True)
class EventFilter:
    instrument_id: int | None = None
    source: str | None = None
    status: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    min_price: float | None = None
    max_price: float | None = None
    search: str | None = None


def _events_select(f: EventFilter):
    e = MarketEvent
    latency = func.extract("epoch", e.processed_at - e.ingested_at) * 1000
    q = select(
        e.event_id, Instrument.symbol, e.event_timestamp, e.price, e.volume, e.open, e.high, e.low, e.close,
        e.source, e.data_mode, e.processing_status, e.attempts, e.ingested_at, e.processed_at, latency.label("latency_ms"),
    ).join(Instrument, Instrument.id == e.instrument_id)
    if f.instrument_id is not None:
        q = q.where(e.instrument_id == f.instrument_id)
    if f.source:
        q = q.where(e.source == f.source)
    if f.status:
        q = q.where(e.processing_status == f.status)
    if f.start:
        q = q.where(e.event_timestamp >= f.start)
    if f.end:
        q = q.where(e.event_timestamp <= f.end)
    if f.min_price is not None:
        q = q.where(e.price >= f.min_price)
    if f.max_price is not None:
        q = q.where(e.price <= f.max_price)
    if f.search:
        try:
            q = q.where(e.event_id == uuid.UUID(f.search))  # full id: index lookup
        except ValueError:
            q = q.where(cast(e.event_id, String).like(f.search.lower().replace("%", "").replace("_", "") + "%"))
    return q.order_by(e.event_timestamp.desc(), e.id.desc())


def _event_dict(r) -> dict:
    return {
        "event_id": str(r.event_id), "symbol": r.symbol, "event_timestamp": r.event_timestamp,
        "price": float(r.price), "volume": float(r.volume), "open": float(r.open), "high": float(r.high),
        "low": float(r.low), "close": float(r.close), "source": r.source, "data_mode": r.data_mode,
        "processing_status": r.processing_status, "attempts": r.attempts, "ingested_at": r.ingested_at,
        "processed_at": r.processed_at, "latency_ms": round(float(r.latency_ms), 3) if r.latency_ms is not None else None,
    }


async def search_events(session: AsyncSession, f: EventFilter, limit: int, offset: int) -> list[dict]:
    rows = await session.execute(_events_select(f).limit(limit).offset(offset))
    return [_event_dict(r) for r in rows]


async def stream_events(session: AsyncSession, f: EventFilter, cap: int) -> AsyncIterator[dict]:
    """Server-side cursor so a CSV export never holds the whole result in memory."""
    result = await session.stream(_events_select(f).limit(cap).execution_options(yield_per=1000))
    async for r in result:
        yield _event_dict(r)


async def last_processed_at(session: AsyncSession) -> datetime | None:
    # Newest row by primary key: an index scan, unlike max(processed_at).
    return (await session.execute(select(MarketEvent.processed_at).order_by(MarketEvent.id.desc()).limit(1))).scalar()
