"""Stream processor: market.raw -> validate -> PostgreSQL (+ derived topics) -> Redis.

Order per batch (crash between any two steps is safe):
  1. validate (pure)
  2. BEGIN; insert events ON CONFLICT DO NOTHING; upsert bars for newly inserted
     events; insert errors; produce validated/processed/errors and flush; COMMIT
  3. adopt new rolling state
  4. update Redis cache + publish (advisory; failures are logged and counted)
  5. commit Kafka offsets
A crash before 5 replays the batch: inserts conflict, so the replay is recorded
as duplicates and nothing is double-counted. Derived topics are at-least-once.
"""

from __future__ import annotations

import json
import logging
import random
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from redis.exceptions import RedisError

from processing.aggregators.bars import bucket_start
from processing.cache import QuoteCache
from processing.repository import (
    PERMANENT_DB_ERRORS,
    TRANSIENT_DB_ERRORS,
    Bar,
    BatchResult,
    Store,
)
from processing.transformers.enrich import Enriched, Window, enrich
from processing.validators.event_validator import (
    ErrorType,
    Rejected,
    Rules,
    ValidEvent,
    duplicate_of,
    validate,
)

log = logging.getLogger(__name__)

MAX_ATTEMPTS = 6  # backoff 0.5+1+2+4+8 s ~= 15 s, well under max.poll.interval.ms


class KafkaFlushError(Exception):
    """Derived-topic messages were not acknowledged in time; the batch rolls back and retries."""


@dataclass(slots=True)
class Metrics:
    counters: Counter = field(default_factory=Counter)
    totals: Counter = field(default_factory=Counter)
    latency_sum_ms: float = 0.0
    latency_n: int = 0
    latency_max_ms: float = 0.0
    db_ms_sum: float = 0.0
    db_n: int = 0
    last_event_at: str | None = None
    interval_started: float = field(default_factory=lambda: time.monotonic())

    def count(self, name: str, n: int = 1) -> None:
        self.counters[name] += n
        self.totals[name] += n

    def snapshot(self, lag: int | None) -> dict[str, float]:
        # Measured from the last successful flush, so counts carried over a failed flush
        # are not divided by a single interval.
        interval_s = time.monotonic() - self.interval_started
        out: dict[str, float] = {k: float(v) for k, v in self.counters.items()}
        for name in ("events_received", "events_valid", "events_invalid", "events_duplicate", "events_retried"):
            out.setdefault(name, 0.0)
        out["events_per_second"] = out["events_valid"] / interval_s if interval_s > 0 else 0.0
        if self.latency_n:
            out["avg_latency_ms"] = self.latency_sum_ms / self.latency_n
            out["max_latency_ms"] = self.latency_max_ms
        if self.db_n:
            out["db_write_ms"] = self.db_ms_sum / self.db_n
        if lag is not None:
            out["consumer_lag"] = float(lag)
        return out

    def reset_interval(self) -> None:
        self.interval_started = time.monotonic()
        self.counters.clear()
        self.latency_sum_ms = self.latency_max_ms = self.db_ms_sum = 0.0
        self.latency_n = self.db_n = 0


def make_quote(*, symbol: str, price: float, event_id: str, event_ts: datetime, processed_at: datetime,
               source: str, data_mode: str, day: tuple[float, float, float, float],
               tick_change_pct: float | None = None, rolling_avg: float | None = None,
               rolling_volume: float | None = None, rolling_window: int | None = None, origin: str = "stream") -> dict:
    """Quote snapshot; day = (open, high, low, volume) of the event's UTC-day bar from PostgreSQL."""
    day_open, day_high, day_low, day_volume = day
    return {
        "type": "quote", "symbol": symbol, "price": price,
        "open": day_open, "high": day_high, "low": day_low, "volume": day_volume,
        "change": _f(price - day_open),
        "change_pct": _f((price - day_open) / day_open * 100) if day_open else None,
        "tick_change_pct": _f(tick_change_pct) if tick_change_pct is not None else None,
        "rolling_avg": _f(rolling_avg) if rolling_avg is not None else None,
        "rolling_volume": rolling_volume, "rolling_window": rolling_window,
        "event_id": event_id, "event_timestamp": event_ts.isoformat(),
        "event_ts_ms": int(event_ts.timestamp() * 1000),
        "processed_at": processed_at.isoformat(), "source": source, "data_mode": data_mode, "origin": origin,
    }


def _backoff(attempt: int) -> float:
    return min(8.0, 0.5 * 2 ** (attempt - 1)) * random.uniform(0.8, 1.2)


def _f(x: float) -> float:
    return round(x, 6)


class StreamProcessor:
    def __init__(
        self,
        consumer: Consumer,
        producer: Producer,
        store: Store,
        cache: QuoteCache,
        topics: dict[str, str],
        rules: Rules,
        symbol_ids: dict[str, int],
        window: int,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._store = store
        self._cache = cache
        self._topics = topics
        self.rules = rules
        self.symbol_ids = symbol_ids
        self._window = window
        self._state: dict[str, Window] = {}
        self._pending: tuple[dict[str, Window], list[Enriched]] | None = None
        self.metrics = Metrics()

    # ---- decide --------------------------------------------------------------

    def _classify(self, payloads: list[bytes], now: datetime) -> tuple[list[ValidEvent], list[Rejected]]:
        valid: list[ValidEvent] = []
        rejects: list[Rejected] = []
        seen: set = set()
        for raw in payloads:
            try:
                result = validate(raw, self.rules, now)
            except Exception as exc:  # supervision boundary: one bad message must not block the partition
                log.exception("validator raised; rejecting the message")
                self.metrics.count("validator_crashes")
                result = Rejected(ErrorType.INVALID_FIELD, f"unexpected validation error ({type(exc).__name__})",
                                  None, None, None, raw[:2048].decode("utf-8", "replace"))
            if isinstance(result, Rejected):
                rejects.append(result)
            elif result.event_id in seen:
                rejects.append(duplicate_of(result, "seen in this batch"))
            else:
                seen.add(result.event_id)
                valid.append(result)
        return valid, rejects

    # ---- act -----------------------------------------------------------------

    def handle(self, messages: list) -> None:
        payloads: list[bytes] = []
        for m in messages:
            err = m.error()
            if err is not None:
                if err.code() != KafkaError._PARTITION_EOF:
                    log.error("kafka consume error: %s", err)
                    self.metrics.count("kafka_errors")
                continue
            payloads.append(m.value() or b"")
        if not payloads:
            return
        self.metrics.count("events_received", len(payloads))
        valid, rejects = self._classify(payloads, datetime.now(UTC))
        result, attempt = self._write_with_retry(valid, rejects)
        committed_at = datetime.now(UTC)

        new_state, enriched = self._pending or (self._state, [])
        self._state, self._pending = new_state, None

        dup_count = len(result.duplicates) + sum(r.error_type is ErrorType.DUPLICATE_EVENT for r in rejects)
        invalid = [r for r in rejects if r.error_type is not ErrorType.DUPLICATE_EVENT]
        self.metrics.count("events_valid", len(result.inserted))
        self.metrics.count("events_invalid", len(invalid) + len(result.refused))
        self.metrics.count("events_db_rejected", len(result.refused))
        self.metrics.count("events_duplicate", dup_count)
        self.metrics.count("batches")
        if attempt > 1:
            self.metrics.count("events_retried", len(result.inserted))
        self.metrics.db_ms_sum += result.db_write_ms
        self.metrics.db_n += 1
        for e in result.inserted:
            if e.ingested_at is not None:
                ms = (committed_at - e.ingested_at).total_seconds() * 1000
                self.metrics.latency_sum_ms += ms
                self.metrics.latency_n += 1
                self.metrics.latency_max_ms = max(self.metrics.latency_max_ms, ms)
        if result.inserted:
            self.metrics.last_event_at = committed_at.isoformat()

        self._update_cache(enriched, result.bars, committed_at)
        try:
            self._consumer.commit(asynchronous=False)
        except KafkaException as exc:
            # Offsets not stored: the batch is redelivered and recorded as duplicates.
            log.warning("offset commit failed, batch will be replayed: %s", exc)
            self.metrics.count("offset_commit_failures")

    def _write_with_retry(self, valid: list[ValidEvent], rejects: list[Rejected]) -> tuple[BatchResult, int]:
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self._pending = None
            try:
                return self._store.write_batch(valid, rejects, self.symbol_ids, attempt, self._before_commit), attempt
            except (*TRANSIENT_DB_ERRORS, KafkaFlushError) as exc:
                if attempt == MAX_ATTEMPTS:
                    log.error("batch failed after %d attempts; exiting so offsets are not advanced", attempt)
                    raise
                delay = _backoff(attempt)
                self.metrics.count("batch_retries")
                log.warning("transient failure (attempt %d), retrying in %.2fs: %s", attempt, delay, exc)
                time.sleep(delay)
            except PERMANENT_DB_ERRORS as exc:
                log.error("batch rejected by database, isolating poison events: %s", exc)
                return self._write_one_by_one(valid, rejects, attempt), attempt
        raise AssertionError("unreachable")

    def _write_one_by_one(self, valid: list[ValidEvent], rejects: list[Rejected], attempt: int) -> BatchResult:
        """Find which events the database refuses and store the rest. Bounded by batch size."""
        inserted: list[ValidEvent] = []
        duplicates: list[Rejected] = []
        refused: list[Rejected] = []
        bars: list[Bar] = []
        enriched_all: list[Enriched] = []
        start_state = self._state
        ms = 0.0
        for e in valid:
            self._pending = None
            try:
                r = self._store.write_batch([e], [], self.symbol_ids, attempt, self._before_commit)
            except PERMANENT_DB_ERRORS as exc:
                refused.append(Rejected(ErrorType.DB_REJECTED, str(exc)[:500], str(e.event_id), e.symbol, e.source, ""))
                continue
            if self._pending is not None:
                self._state, more = self._pending  # chain rolling state across single-event commits
                enriched_all.extend(more)
            inserted += r.inserted
            duplicates += r.duplicates
            bars += r.bars
            ms += r.db_write_ms
        final_state, self._state = self._state, start_state
        errors = [*rejects, *refused]
        if errors:
            self._store.record_rejects(errors)
            self._publish_rejects(errors)
            self._producer.flush(10)
        self._pending = (final_state, enriched_all)
        return BatchResult(inserted, duplicates, bars, ms, refused)

    def _produce(self, topic: str, key: str | None, value: dict) -> None:
        data = json.dumps(value, separators=(",", ":"), default=str).encode()
        while True:
            try:
                self._producer.produce(topic, value=data, key=key.encode() if key else None)
                return
            except BufferError:
                self._producer.poll(0.5)

    def _publish_rejects(self, rejects: list[Rejected]) -> None:
        now = datetime.now(UTC).isoformat()
        for r in rejects:
            self._produce(
                self._topics["errors"],
                r.symbol,
                {"error_type": r.error_type.value, "message": r.message, "event_id": r.event_id,
                 "symbol": r.symbol, "source": r.source, "raw": r.raw, "detected_at": now},
            )

    def _before_commit(self, inserted: list[ValidEvent], errors: list[Rejected], bars: list[Bar]) -> None:
        """Runs inside the DB transaction. Raising rolls everything back."""
        new_state, enriched = enrich(self._state, inserted, self._window)
        for en in enriched:
            e = en.event
            base = {
                "event_id": str(e.event_id), "symbol": e.symbol, "timestamp": e.timestamp.isoformat(),
                "price": e.price, "volume": e.volume, "open": e.open, "high": e.high, "low": e.low,
                "close": e.close, "source": e.source, "data_mode": e.data_mode,
            }
            self._produce(self._topics["validated"], e.symbol, base)
            self._produce(
                self._topics["processed"],
                e.symbol,
                {**base, "tick_change_pct": en.tick_change_pct, "rolling_avg": en.rolling_avg,
                 "rolling_volume": en.rolling_volume, "rolling_window": en.window_size},
            )
        self._publish_rejects(errors)
        remaining = self._producer.flush(10)
        if remaining:
            raise KafkaFlushError(f"{remaining} derived messages not acknowledged")
        self._pending = (new_state, enriched)

    def _update_cache(self, enriched: list[Enriched], bars: list[Bar], committed_at: datetime) -> None:
        if not enriched:
            return
        # Latest by event time, not arrival order: a late event must not become "the" quote.
        latest: dict[str, Enriched] = {}
        for en in enriched:
            cur = latest.get(en.event.symbol)
            if cur is None or en.event.timestamp >= cur.event.timestamp:
                latest[en.event.symbol] = en
        day = {(b.symbol, b.start): b for b in bars if b.interval == "1d"}
        quotes = []
        for symbol, en in latest.items():
            e = en.event
            d = day.get((symbol, bucket_start(e.timestamp, "1d")))
            quotes.append(make_quote(
                symbol=symbol, price=e.price, event_id=str(e.event_id), event_ts=e.timestamp,
                processed_at=committed_at, source=e.source, data_mode=e.data_mode,
                day=(d.open, d.high, d.low, d.volume) if d else (e.open, e.high, e.low, e.volume),
                tick_change_pct=en.tick_change_pct, rolling_avg=en.rolling_avg,
                rolling_volume=en.rolling_volume, rolling_window=en.window_size,
            ))
        bar_msgs = [
            {"type": "bar", "symbol": b.symbol, "interval": b.interval, "time": int(b.start.timestamp()),
             "open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume}
            for b in bars if b.interval == "1m"
        ]
        try:
            self._cache.write(quotes, bar_msgs)
        except (RedisError, OSError) as exc:
            # Cache is derived from PostgreSQL; the next batch or the rebuild repairs it.
            self.metrics.count("redis_failures")
            log.warning("redis update failed: %s", exc)

    def rebuild_cache(self) -> int:
        """Fill quotes missing from Redis (data loss, FLUSHALL, eviction) from PostgreSQL.

        Conditional writes mean a rebuild racing live traffic never replaces a newer quote.
        Returns the number of quotes written.
        """
        try:
            missing = self._cache.missing(self.symbol_ids)
            if not missing:
                return 0
            rows = self._store.latest_quotes(missing)
            quotes = [
                make_quote(symbol=r.symbol, price=r.price, event_id=r.event_id, event_ts=r.event_timestamp,
                           processed_at=r.processed_at, source=r.source, data_mode=r.data_mode,
                           day=(r.day_open, r.day_high, r.day_low, r.day_volume) if r.day_open is not None
                           else (r.price, r.price, r.price, 0),
                           origin="rebuild")
                for r in rows
            ]
            written = sum(self._cache.write(quotes, publish=False)) if quotes else 0
        except (RedisError, OSError) as exc:
            log.warning("cache rebuild skipped, redis unavailable: %s", exc)
            return 0
        if written:
            self.metrics.count("cache_rebuilt_quotes", written)
            log.info("rebuilt %d quote(s) from PostgreSQL", written)
        return written

    def consumer_lag(self) -> int | None:
        try:
            assignment = self._consumer.assignment()
            if not assignment:
                return None
            lag = 0
            for tp in self._consumer.position(assignment):
                # Cached = high watermark from the latest fetch response. A broker query per
                # partition here blocked the consume loop ~1 s every metrics interval (measured);
                # the API reads authoritative lag from the broker on its own thread.
                low, high = self._consumer.get_watermark_offsets(tp, cached=True)
                if high < 0 or (tp.offset < 0 and low < 0):
                    continue  # nothing fetched for this partition yet
                lag += max(0, high - (tp.offset if tp.offset >= 0 else low))
            return lag
        except KafkaException as exc:
            log.warning("could not compute consumer lag: %s", exc)
            return None
