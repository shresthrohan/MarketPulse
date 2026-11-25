"""PostgreSQL access for the processor (psycopg 3, one connection, single thread)."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import psycopg
from psycopg.rows import namedtuple_row

from processing.aggregators.bars import aggregate
from processing.validators.event_validator import (
    Rejected,
    ValidEvent,
    duplicate_of,
    severity,
)

log = logging.getLogger(__name__)

# Connection loss, timeouts, server restarts: a later attempt can succeed.
TRANSIENT_DB_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError)
# Constraint/data violations: the same input fails every time.
PERMANENT_DB_ERRORS = (psycopg.DataError, psycopg.IntegrityError)

INSERT_EVENTS = """
INSERT INTO market_events (event_id, instrument_id, event_timestamp, price, volume, open, high, low, close,
                           source, data_mode, processing_status, attempts, ingested_at)
SELECT * FROM unnest(%s::uuid[], %s::int[], %s::timestamptz[], %s::numeric[], %s::numeric[], %s::numeric[],
                     %s::numeric[], %s::numeric[], %s::numeric[], %s::text[], %s::text[], %s::text[],
                     %s::smallint[], %s::timestamptz[])
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id
"""

# open/close only move when the delta is earlier/later than what is stored, so
# late or out-of-order batches merge correctly.
UPSERT_BARS = """
INSERT INTO historical_aggregates AS h (instrument_id, "interval", "timestamp", open, high, low, close, volume,
                                        first_event_at, last_event_at)
SELECT * FROM unnest(%s::int[], %s::text[], %s::timestamptz[], %s::numeric[], %s::numeric[], %s::numeric[],
                     %s::numeric[], %s::numeric[], %s::timestamptz[], %s::timestamptz[])
ON CONFLICT (instrument_id, "interval", "timestamp") DO UPDATE SET
  open = CASE WHEN EXCLUDED.first_event_at < h.first_event_at THEN EXCLUDED.open ELSE h.open END,
  high = GREATEST(h.high, EXCLUDED.high),
  low = LEAST(h.low, EXCLUDED.low),
  close = CASE WHEN EXCLUDED.last_event_at >= h.last_event_at THEN EXCLUDED.close ELSE h.close END,
  volume = h.volume + EXCLUDED.volume,
  first_event_at = LEAST(h.first_event_at, EXCLUDED.first_event_at),
  last_event_at = GREATEST(h.last_event_at, EXCLUDED.last_event_at)
RETURNING instrument_id, "interval", "timestamp", open, high, low, close, volume
"""

# Latest stored event per instrument (index (instrument_id, event_timestamp)) + that event's UTC-day bar.
LATEST_QUOTES = """
SELECT i.symbol, e.event_id::text AS event_id, e.event_timestamp, e.price::float8 AS price, e.source, e.data_mode,
       e.processed_at, d.open::float8 AS day_open, d.high::float8 AS day_high, d.low::float8 AS day_low,
       d.volume::float8 AS day_volume
FROM instruments i
JOIN LATERAL (SELECT * FROM market_events m WHERE m.instrument_id = i.id
              ORDER BY m.event_timestamp DESC LIMIT 1) e ON true
LEFT JOIN historical_aggregates d
  ON d.instrument_id = i.id AND d."interval" = '1d' AND d."timestamp" = date_trunc('day', e.event_timestamp, 'UTC')
WHERE i.symbol = ANY(%s)
"""

INSERT_ERRORS = """
INSERT INTO processing_errors (event_id, symbol, error_type, error_message, severity, source, raw_payload)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""


@dataclass(frozen=True, slots=True)
class Bar:
    symbol: str
    interval: str
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True, slots=True)
class BatchResult:
    inserted: list[ValidEvent]
    duplicates: list[Rejected]
    bars: list[Bar]
    db_write_ms: float
    refused: list[Rejected] = field(default_factory=list)  # valid events the database refused


# Called inside the open transaction with (inserted, every error row written, bars);
# raising rolls the batch back.
BeforeCommit = Callable[[list[ValidEvent], list[Rejected], list[Bar]], None]


class Store:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: psycopg.Connection | None = None

    def conn(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(
                self._dsn, autocommit=True, connect_timeout=5, options="-c statement_timeout=15000"
            )
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()

    def load_instruments(self) -> list[tuple[str, int, str | None]]:
        """(symbol, id, data_mode) for every instrument."""
        with self.conn().cursor() as cur:
            cur.execute("SELECT symbol, id, data_mode FROM instruments")
            return cur.fetchall()

    def load_symbols(self) -> dict[str, int]:
        with self.conn().cursor() as cur:
            cur.execute("SELECT symbol, id FROM instruments")
            return dict(cur.fetchall())

    def write_batch(
        self,
        valid: Sequence[ValidEvent],
        rejects: Sequence[Rejected],
        symbol_ids: dict[str, int],
        attempt: int,
        before_commit: BeforeCommit,
    ) -> BatchResult:
        """One transaction: events + derived bars + errors commit together or not at all."""
        started = datetime.now(UTC)
        conn = self.conn()
        status = "valid" if attempt == 1 else "retried"
        names = {v: k for k, v in symbol_ids.items()}
        with conn.transaction(), conn.cursor() as cur:
            inserted_ids: set = set()
            if valid:
                cur.execute(
                    INSERT_EVENTS,
                    (
                        [e.event_id for e in valid],
                        [symbol_ids[e.symbol] for e in valid],
                        [e.timestamp for e in valid],
                        [e.price for e in valid],
                        [e.volume for e in valid],
                        [e.open for e in valid],
                        [e.high for e in valid],
                        [e.low for e in valid],
                        [e.close for e in valid],
                        [e.source for e in valid],
                        [e.data_mode for e in valid],
                        [status] * len(valid),
                        [attempt] * len(valid),
                        [e.ingested_at for e in valid],
                    ),
                )
                inserted_ids = {row[0] for row in cur.fetchall()}
            inserted = [e for e in valid if e.event_id in inserted_ids]
            duplicates = [duplicate_of(e, "stored") for e in valid if e.event_id not in inserted_ids]

            bars: list[Bar] = []
            deltas = aggregate(inserted)
            if deltas:
                cur.execute(
                    UPSERT_BARS,
                    (
                        [symbol_ids[d.symbol] for d in deltas],
                        [d.interval for d in deltas],
                        [d.start for d in deltas],
                        [d.open for d in deltas],
                        [d.high for d in deltas],
                        [d.low for d in deltas],
                        [d.close for d in deltas],
                        [d.volume for d in deltas],
                        [d.first_at for d in deltas],
                        [d.last_at for d in deltas],
                    ),
                )
                bars = [
                    Bar(names[i], iv, ts, float(o), float(h), float(lo), float(c), float(v))
                    for i, iv, ts, o, h, lo, c, v in cur.fetchall()
                ]

            errors = [*rejects, *duplicates]
            if errors:
                cur.executemany(
                    INSERT_ERRORS,
                    [
                        (r.event_id, r.symbol, r.error_type.value, r.message, severity(r.error_type), r.source, r.raw or None)
                        for r in errors
                    ],
                )
            before_commit(inserted, errors, bars)
        elapsed = (datetime.now(UTC) - started).total_seconds() * 1000
        return BatchResult(inserted, duplicates, bars, elapsed)

    def latest_quotes(self, symbols: Sequence[str]) -> list:
        with self.conn().cursor(row_factory=namedtuple_row) as cur:
            cur.execute(LATEST_QUOTES, (list(symbols),))
            return cur.fetchall()

    def record_rejects(self, rejects: Sequence[Rejected]) -> None:
        with self.conn().transaction(), self.conn().cursor() as cur:
            cur.executemany(
                INSERT_ERRORS,
                [(r.event_id, r.symbol, r.error_type.value, r.message, severity(r.error_type), r.source, r.raw or None) for r in rejects],
            )

    def write_metrics(self, service: str, values: dict[str, float]) -> None:
        with self.conn().transaction(), self.conn().cursor() as cur:
            cur.executemany(
                "INSERT INTO pipeline_metrics (service_name, metric_name, metric_value) VALUES (%s, %s, %s)",
                [(service, k, float(v)) for k, v in values.items()],
            )

    def apply_retention(self, policies: Sequence[tuple[str, str, int]], batch: int = 10_000, max_rounds: int = 100) -> dict[str, int]:
        """Delete expired rows in bounded batches: short locks per statement, bounded total work per run.

        `policies` is (table, condition with one %s cutoff placeholder, days). Both strings come
        from the fixed list in main.py, never from input. Deletes go by `id` (unique in every
        table), not ctid, because ctid is not unique across TimescaleDB chunks.
        """
        deleted: dict[str, int] = {}
        with self.conn().cursor() as cur:
            for table, condition, days in policies:
                cutoff = datetime.now(UTC) - timedelta(days=days)
                total = 0
                for _ in range(max_rounds):
                    cur.execute(f"DELETE FROM {table} WHERE id IN (SELECT id FROM {table} WHERE {condition} LIMIT %s)", (cutoff, batch))
                    total += cur.rowcount
                    if cur.rowcount < batch:
                        break
                deleted[f"{table}:{condition}"] = total
        return deleted
