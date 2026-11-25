"""StreamProcessor failure injection with in-memory fakes (no Kafka, PostgreSQL or Redis).

The fake store mirrors the real transaction contract: rows become visible only if
before_commit returns, and a raise anywhere leaves nothing behind.
"""

import json
import uuid
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from confluent_kafka import KafkaException
from redis.exceptions import ConnectionError as RedisConnectionError

from processing.consumers import processor as proc_mod
from processing.consumers.processor import MAX_ATTEMPTS, StreamProcessor
from processing.repository import BatchResult
from processing.validators.event_validator import ErrorType, Rules, duplicate_of

SYMBOLS = {"AAPL": 1, "MSFT": 2}
TOPICS = {"raw": "market.raw", "validated": "market.validated", "processed": "market.processed", "errors": "market.errors"}


class Msg:
    def __init__(self, value: bytes):
        self._v = value

    def error(self):
        return None

    def value(self):
        return self._v


class FakeConsumer:
    def __init__(self, fail_commit=False):
        self.commits = 0
        self.fail_commit = fail_commit

    def commit(self, asynchronous=False):
        if self.fail_commit:
            raise KafkaException("commit failed")
        self.commits += 1

    def assignment(self):
        return []


class FakeProducer:
    def __init__(self, flush_failures=0):
        self.sent: list[tuple[str, dict]] = []
        self._pending: list[tuple[str, dict]] = []
        self.flush_failures = flush_failures

    def produce(self, topic, value, key=None):
        self._pending.append((topic, json.loads(value)))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        if self.flush_failures:
            self.flush_failures -= 1
            self._pending.clear()  # undelivered
            return 1
        self.sent += self._pending
        self._pending.clear()
        return 0

    def on(self, topic):
        return [v for t, v in self.sent if t == topic]


class FakeStore:
    def __init__(self, failures=(), poison=frozenset()):
        self.rows: dict = {}  # event_id -> (event, status, attempts)
        self.errors: list = []
        self.failures = list(failures)
        self.poison = set(poison)

    def write_batch(self, valid, rejects, symbol_ids, attempt, before_commit):
        if self.failures:
            raise self.failures.pop(0)
        if any(e.event_id in self.poison for e in valid):
            raise psycopg.DataError("numeric field overflow")
        inserted = [e for e in valid if e.event_id not in self.rows]
        dups = [duplicate_of(e, "stored") for e in valid if e.event_id in self.rows]
        errors = [*rejects, *dups]
        before_commit(inserted, errors, [])  # may raise: nothing below happens, like a rollback
        for e in inserted:
            self.rows[e.event_id] = (e, "valid" if attempt == 1 else "retried", attempt)
        self.errors += errors
        return BatchResult(inserted, dups, [], 1.0)

    def record_rejects(self, rejects):
        self.errors += rejects

    def latest_quotes(self, symbols):
        return []


class FakeCache:
    def __init__(self, fail=False):
        self.quotes: dict = {}
        self.fail = fail

    def write(self, quotes, bars=(), publish=True):
        if self.fail:
            raise RedisConnectionError("redis down")
        for q in quotes:
            self.quotes[q["symbol"]] = q
        return [True for _ in quotes]

    def missing(self, symbols):
        return [s for s in symbols if s not in self.quotes]


def event(symbol="AAPL", ts=None, **over):
    ts = ts or datetime.now(UTC)
    e = {"event_id": str(uuid.uuid4()), "symbol": symbol, "timestamp": ts.isoformat(), "price": 100.0, "volume": 10,
         "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0, "source": "test", "data_mode": "simulated"}
    e.update(over)
    return e


def msgs(*docs):
    return [Msg(d if isinstance(d, bytes) else json.dumps(d).encode()) for d in docs]


def make(store=None, producer=None, consumer=None, cache=None):
    p = StreamProcessor(consumer or FakeConsumer(), producer or FakeProducer(), store or FakeStore(),
                        cache or FakeCache(), TOPICS, Rules(frozenset(SYMBOLS)), SYMBOLS, window=3)
    return p


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(proc_mod.time, "sleep", lambda s: None)


def test_counts_reconcile_received_equals_valid_plus_invalid_plus_duplicate():
    store = FakeStore()
    p = make(store=store)
    stored = event()
    p.handle(msgs(stored))
    a, b = event(), event("MSFT")
    batch = msgs(a, b, a, stored, event(price=-1), event(symbol="ZZZZ"), b"{oops")
    p.handle(batch)
    c = p.metrics.totals
    assert c["events_received"] == 8
    assert c["events_valid"] == 3 and c["events_invalid"] == 3 and c["events_duplicate"] == 2
    assert c["events_received"] == c["events_valid"] + c["events_invalid"] + c["events_duplicate"]
    # every rejected payload is traceable to a recorded error with its validation reason
    kinds = sorted(r.error_type.value for r in store.errors)
    assert kinds == sorted(["duplicate_event", "duplicate_event", "negative_price", "unknown_symbol", "malformed_json"])
    assert len(store.rows) == 3  # accepted events stored exactly once


def test_offsets_are_committed_only_after_the_batch_is_stored():
    consumer, store = FakeConsumer(), FakeStore()
    make(store=store, consumer=consumer).handle(msgs(event(), event()))
    assert consumer.commits == 1 and len(store.rows) == 2


def test_transient_db_failure_is_retried_and_events_are_marked_retried():
    store, consumer = FakeStore(failures=[psycopg.OperationalError("connection lost")]), FakeConsumer()
    p = make(store=store, consumer=consumer)
    p.handle(msgs(event(), event()))
    assert [status for _, status, _ in store.rows.values()] == ["retried", "retried"]
    assert {attempts for *_, attempts in store.rows.values()} == {2}
    assert p.metrics.totals["events_retried"] == 2 and consumer.commits == 1


def test_exhausted_retries_raise_without_committing_offsets():
    store = FakeStore(failures=[psycopg.OperationalError("down")] * MAX_ATTEMPTS)
    consumer = FakeConsumer()
    with pytest.raises(psycopg.OperationalError):
        make(store=store, consumer=consumer).handle(msgs(event()))
    assert consumer.commits == 0 and store.rows == {}


def test_kafka_flush_failure_rolls_back_and_the_retry_stores_once():
    producer, store = FakeProducer(flush_failures=1), FakeStore()
    p = make(store=store, producer=producer)
    e = event()
    p.handle(msgs(e))
    assert len(store.rows) == 1 and [m["event_id"] for m in producer.on("market.processed")] == [e["event_id"]]


def test_rolled_back_batch_does_not_advance_rolling_state():
    producer = FakeProducer(flush_failures=1)
    p = make(producer=producer)
    p.handle(msgs(event(price=100.0, open=100.0, high=100.0, low=100.0, close=100.0)))
    p.handle(msgs(event(price=110.0, open=110.0, high=110.0, low=110.0, close=110.0)))
    processed = producer.on("market.processed")
    assert processed[-1]["rolling_window"] == 2  # the failed first attempt was not counted
    assert processed[-1]["tick_change_pct"] == pytest.approx(10.0)


def test_poison_event_is_isolated_dead_lettered_and_does_not_block_the_batch():
    good, bad = event(), event()
    store, producer, consumer = FakeStore(poison={uuid.UUID(bad["event_id"])}), FakeProducer(), FakeConsumer()
    p = make(store=store, producer=producer, consumer=consumer)
    p.handle(msgs(good, bad))
    assert list(store.rows) == [uuid.UUID(good["event_id"])]
    assert [r.error_type for r in store.errors] == [ErrorType.DB_REJECTED]
    assert [m["event_id"] for m in producer.on("market.errors")] == [bad["event_id"]]
    assert consumer.commits == 1 and p.metrics.totals["events_db_rejected"] == 1


def test_redis_outage_does_not_stop_storage_or_offset_commits():
    store, consumer = FakeStore(), FakeConsumer()
    p = make(store=store, consumer=consumer, cache=FakeCache(fail=True))
    p.handle(msgs(event()))
    assert len(store.rows) == 1 and consumer.commits == 1 and p.metrics.totals["redis_failures"] == 1


def test_failed_offset_commit_is_tolerated_and_the_replay_creates_no_duplicates():
    store = FakeStore()
    e = event()
    make(store=store, consumer=FakeConsumer(fail_commit=True)).handle(msgs(e))
    p = make(store=store)  # restarted processor re-reads the uncommitted message
    p.handle(msgs(e))
    assert len(store.rows) == 1 and p.metrics.totals["events_duplicate"] == 1


def test_an_unexpected_validator_exception_rejects_only_that_message(monkeypatch):
    real = proc_mod.validate

    def flaky(raw, rules, now):
        if b"boom" in raw:
            raise RuntimeError("bug")
        return real(raw, rules, now)

    monkeypatch.setattr(proc_mod, "validate", flaky)
    store = FakeStore()
    p = make(store=store)
    p.handle(msgs(event(), event(source="boom")))
    assert len(store.rows) == 1 and p.metrics.totals["validator_crashes"] == 1
    assert store.errors[0].error_type is ErrorType.INVALID_FIELD


def test_quote_follows_event_time_not_arrival_order():
    cache = FakeCache()
    p = make(cache=cache)
    now = datetime.now(UTC)
    newer, late = event(ts=now, price=101.0, high=101.0, close=101.0), event(ts=now - timedelta(minutes=5), price=99.0, low=99.0, close=99.0)
    p.handle(msgs(newer, late))
    assert cache.quotes["AAPL"]["event_id"] == newer["event_id"] and cache.quotes["AAPL"]["price"] == 101.0


def test_metrics_rate_uses_time_since_last_successful_flush(monkeypatch):
    clock = iter([0.0, 30.0])
    monkeypatch.setattr(proc_mod.time, "monotonic", lambda: next(clock))
    p = make()
    p.metrics.count("events_valid", 300)
    assert p.metrics.snapshot(lag=None)["events_per_second"] == pytest.approx(10.0)
