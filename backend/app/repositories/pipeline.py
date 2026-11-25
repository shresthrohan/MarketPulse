"""SQL for pipeline metrics, processing errors and the unified event log."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import String, cast, func, literal, null, or_, select, text, union_all
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tables import Instrument, MarketEvent, PipelineMetric, ProcessingError

SERVICE = "processor"


async def metric_aggregates(session: AsyncSession, names: list[str], since: datetime) -> dict[str, dict[str, float]]:
    m = PipelineMetric
    rows = await session.execute(
        select(m.metric_name, func.sum(m.metric_value), func.avg(m.metric_value), func.max(m.metric_value))
        .where(m.service_name == SERVICE, m.metric_name.in_(names), m.timestamp >= since)
        .group_by(m.metric_name)
    )
    return {name: {"sum": float(s), "avg": float(a), "max": float(mx)} for name, s, a, mx in rows}


_SERIES = text(
    """
    SELECT date_bin(make_interval(secs => :bucket), "timestamp", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS bucket,
           metric_name, sum(metric_value) AS s, avg(metric_value) AS a
    FROM pipeline_metrics
    WHERE service_name = :service AND "timestamp" >= :since AND metric_name = ANY(:names)
    GROUP BY 1, 2 ORDER BY 1
    """
)


async def metric_series(session: AsyncSession, names: list[str], since: datetime, bucket_s: int) -> list[tuple[datetime, str, float, float]]:
    rows = await session.execute(_SERIES, {"bucket": float(bucket_s), "service": SERVICE, "since": since, "names": names})
    return [(r.bucket, r.metric_name, float(r.s), float(r.a)) for r in rows]


async def error_breakdown(session: AsyncSession, since: datetime) -> list[dict]:
    p = ProcessingError
    rows = await session.execute(
        select(p.error_type, func.count()).where(p.timestamp >= since).group_by(p.error_type).order_by(func.count().desc())
    )
    return [{"error_type": t, "count": int(c)} for t, c in rows]


def _like(term: str) -> str:
    return "%" + term.replace("\\", "").replace("%", "").replace("_", "") + "%"


async def errors_page(session: AsyncSession, error_type: str | None, symbol: str | None, search: str | None,
                      limit: int, offset: int) -> list[dict]:
    p = ProcessingError
    q = select(p)
    if error_type:
        q = q.where(p.error_type == error_type)
    if symbol:
        q = q.where(p.symbol == symbol.upper())
    if search:
        q = q.where(or_(p.event_id.ilike(_like(search)), p.error_message.ilike(_like(search))))
    rows = (await session.execute(q.order_by(p.timestamp.desc(), p.id.desc()).limit(limit).offset(offset))).scalars()
    return [
        {"id": r.id, "event_id": r.event_id, "symbol": r.symbol, "error_type": r.error_type,
         "error_message": r.error_message, "severity": r.severity, "source": r.source,
         "raw_payload": r.raw_payload, "timestamp": r.timestamp}
        for r in rows
    ]


async def event_log(session: AsyncSession, view: str, search: str | None, symbol: str | None,
                    severity: str | None, limit: int, offset: int) -> list[dict]:
    """Stored events and rejected events in one timeline.

    Each branch is ordered and limited on its own index before the merge, so a
    page costs O(limit + offset) rows per branch, not a scan of both tables.
    """
    n = limit + offset
    branches = []
    want_events = view in ("all", "success", "retried") and severity in (None, "info")
    want_errors = view in ("all", "failed") and severity in (None, "warning", "error")
    if want_events:
        e = MarketEvent
        q = select(
            e.event_timestamp.label("ts"), cast(e.event_id, String).label("event_id"), e.source.label("source"),
            Instrument.symbol.label("symbol"), literal("market.tick").label("event_type"),
            literal("info").label("severity"), e.processing_status.label("processing_status"),
            cast(null(), String).label("error_details"),
        ).join(Instrument, Instrument.id == e.instrument_id)
        if view == "retried":
            q = q.where(e.processing_status == "retried")
        if symbol:
            q = q.where(Instrument.symbol == symbol.upper())
        if search:
            q = q.where(cast(e.event_id, String).ilike(_like(search)))
        branches.append(q.order_by(e.event_timestamp.desc()).limit(n).subquery())
    if want_errors:
        p = ProcessingError
        q = select(
            p.timestamp.label("ts"), p.event_id.label("event_id"), p.source.label("source"), p.symbol.label("symbol"),
            ("error." + p.error_type).label("event_type"), p.severity.label("severity"),
            literal("rejected").label("processing_status"), p.error_message.label("error_details"),
        )
        if severity in ("warning", "error"):
            q = q.where(p.severity == severity)
        if symbol:
            q = q.where(p.symbol == symbol.upper())
        if search:
            q = q.where(or_(p.event_id.ilike(_like(search)), p.error_message.ilike(_like(search))))
        branches.append(q.order_by(p.timestamp.desc()).limit(n).subquery())
    if not branches:
        return []
    merged = union_all(*(select(b) for b in branches)).subquery()
    rows = await session.execute(select(merged).order_by(merged.c.ts.desc()).limit(limit).offset(offset))
    return [
        {"timestamp": r.ts, "event_id": r.event_id, "source": r.source, "symbol": r.symbol,
         "event_type": r.event_type, "severity": r.severity, "processing_status": r.processing_status,
         "error_details": r.error_details}
        for r in rows
    ]
