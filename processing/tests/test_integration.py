"""End-to-end through the real stack: Kafka -> running processor -> PostgreSQL / Redis / market.errors.

Run inside the processing image on the compose network while the stack is up:
    docker compose run --rm processing pytest processing/tests/test_integration.py
"""

import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta, timezone

import psycopg
import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from redis import Redis

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DSN = os.environ.get("DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not DSN, reason="needs the compose stack")


@pytest.fixture(scope="module")
def db():
    with psycopg.connect(DSN, autocommit=True) as conn:
        yield conn


@pytest.fixture(scope="module")
def redis():
    return Redis.from_url(os.environ["REDIS_URL"], socket_timeout=3)


def wait_for(fn, timeout=30.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = fn()
        if value:
            return value
        time.sleep(0.25)
    raise AssertionError("condition not met within timeout")


def produce(*payloads: bytes, key: bytes | None = b"AAPL") -> None:
    p = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all"})
    for v in payloads:
        p.produce("market.raw", value=v, key=key)
    assert p.flush(10) == 0


def event(redis, **over) -> dict:
    quote = redis.hget("mp:quotes", "AAPL")
    price = json.loads(quote)["price"] if quote else 225.0
    now = datetime.now(UTC).isoformat()
    e = {"event_id": str(uuid.uuid4()), "symbol": "AAPL", "timestamp": now, "price": price, "volume": 1,
         "open": price, "high": price, "low": price, "close": price, "source": "integration-test",
         "data_mode": "simulated", "ingested_at": now}
    e.update(over)
    return e


def bar_volume(db, ts: str) -> int:
    row = db.execute(
        """SELECT h.volume FROM historical_aggregates h JOIN instruments i ON i.id = h.instrument_id
           WHERE i.symbol = 'AAPL' AND h."interval" = '1m' AND h."timestamp" = date_trunc('minute', %s::timestamptz)""",
        (ts,)).fetchone()
    return row[0] if row else 0


def errors_for(db, event_id):
    return db.execute("SELECT error_type, severity FROM processing_errors WHERE event_id = %s", (event_id,)).fetchall()


def test_valid_event_is_stored_once_and_cached(db, redis):
    e = event(redis)
    produce(json.dumps(e).encode())
    row = wait_for(lambda: db.execute(
        "SELECT processing_status, attempts, source FROM market_events WHERE event_id = %s", (e["event_id"],)).fetchone())
    assert row == ("valid", 1, "integration-test")
    bucket = db.execute(
        """SELECT count(*) FROM historical_aggregates h JOIN instruments i ON i.id = h.instrument_id
           WHERE i.symbol = 'AAPL' AND h."interval" = '1m' AND h."timestamp" = date_trunc('minute', %s::timestamptz)""",
        (e["timestamp"],)).fetchone()[0]
    assert bucket == 1
    assert json.loads(redis.hget("mp:quotes", "AAPL"))["symbol"] == "AAPL"

    # Replaying the same event (e.g. after a crash before the offset commit) must not create a second row.
    produce(json.dumps(e).encode())
    assert wait_for(lambda: ("duplicate_event", "warning") in errors_for(db, e["event_id"]))
    assert db.execute("SELECT count(*) FROM market_events WHERE event_id = %s", (e["event_id"],)).fetchone()[0] == 1


def test_invalid_events_are_recorded_and_dead_lettered_not_dropped(db, redis):
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"it-{uuid.uuid4()}", "enable.auto.commit": False})
    md = c.list_topics("market.errors", timeout=10)
    start = []
    for p in md.topics["market.errors"].partitions:
        _, high = c.get_watermark_offsets(TopicPartition("market.errors", p), timeout=10)
        start.append(TopicPartition("market.errors", p, high))
    c.assign(start)

    neg = event(redis, price=-1.0)
    bad_ohlc = event(redis, high=1.0, low=2.0)
    unknown = event(redis, symbol="ZZZZ")
    marker = uuid.uuid4().hex
    produce(json.dumps(neg).encode(), json.dumps(bad_ohlc).encode(), json.dumps(unknown).encode())
    produce(f'{{"broken": "{marker}"'.encode(), key=None)

    assert wait_for(lambda: errors_for(db, neg["event_id"])) == [("negative_price", "error")]
    assert wait_for(lambda: errors_for(db, bad_ohlc["event_id"])) == [("invalid_ohlc", "error")]
    assert wait_for(lambda: errors_for(db, unknown["event_id"])) == [("unknown_symbol", "error")]
    assert wait_for(lambda: db.execute(
        "SELECT error_type FROM processing_errors WHERE raw_payload LIKE %s", (f"%{marker}%",)).fetchone()) == ("malformed_json",)
    for e in (neg, bad_ohlc, unknown):
        assert db.execute("SELECT count(*) FROM market_events WHERE event_id = %s", (e["event_id"],)).fetchone()[0] == 0

    wanted = {neg["event_id"], bad_ohlc["event_id"], unknown["event_id"]}
    seen: set = set()
    end = time.monotonic() + 30
    while wanted - seen and time.monotonic() < end:
        msg = c.poll(1.0)
        if msg is not None and msg.error() is None:
            seen.add(json.loads(msg.value()).get("event_id"))
    c.close()
    assert wanted <= seen, "rejected events must be published to market.errors"


def test_processor_publishes_metrics(db):
    row = wait_for(lambda: db.execute(
        """SELECT count(*) FROM pipeline_metrics WHERE service_name = 'processor'
           AND metric_name = 'events_received' AND "timestamp" > now() - interval '1 minute'""").fetchone()[0], timeout=15)
    assert row >= 1


def test_replayed_event_does_not_double_count_bar_volume(db, redis):
    # A past minute: live ticks only touch the current minute, so this bucket changes only through us.
    ts = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    e = event(redis, timestamp=ts, volume=7)
    before = bar_volume(db, ts)
    produce(json.dumps(e).encode())
    wait_for(lambda: db.execute("SELECT 1 FROM market_events WHERE event_id = %s", (e["event_id"],)).fetchone())
    assert bar_volume(db, ts) == before + 7
    produce(json.dumps(e).encode())
    wait_for(lambda: ("duplicate_event", "warning") in errors_for(db, e["event_id"]))
    assert bar_volume(db, ts) == before + 7


def test_every_produced_event_is_accounted_for_exactly_once(db, redis):
    """Reconciliation: produced = stored + rejected, with duplicates stored once and logged once."""
    tag = f"recon-{uuid.uuid4().hex[:8]}"
    valid = [event(redis, source=tag) for _ in range(5)]
    invalid = [event(redis, source=tag, price=-5.0), event(redis, source=tag, volume=-1),
               event(redis, source=tag, timestamp="2026-01-01 10:00:00")]
    dupes = valid[:2]
    produce(*(json.dumps(e).encode() for e in valid + invalid))
    produce(*(json.dumps(e).encode() for e in dupes))
    ids = [e["event_id"] for e in valid + invalid]

    def accounted():
        stored = db.execute("SELECT event_id::text FROM market_events WHERE source = %s", (tag,)).fetchall()
        errors = db.execute("SELECT event_id, error_type FROM processing_errors WHERE source = %s", (tag,)).fetchall()
        return (stored, errors) if len(stored) + len(errors) >= len(ids) + len(dupes) else None

    stored, errors = wait_for(accounted)
    stored_ids = [r[0] for r in stored]
    assert sorted(stored_ids) == sorted(e["event_id"] for e in valid)  # each accepted event exactly once
    by_type = sorted((eid, kind) for eid, kind in errors)
    expected = sorted([(invalid[0]["event_id"], "negative_price"), (invalid[1]["event_id"], "invalid_volume"),
                       (invalid[2]["event_id"], "invalid_timestamp")] +
                      [(d["event_id"], "duplicate_event") for d in dupes])
    assert by_type == expected  # every rejection traceable to its validation failure
    assert len(stored) + len(errors) == len(valid) + len(invalid) + len(dupes)  # counts reconcile


def test_timestamps_are_stored_and_returned_in_utc(db, redis):
    local = datetime.now(UTC).astimezone(timezone(timedelta(hours=5, minutes=30)))
    e = event(redis, timestamp=local.isoformat())
    produce(json.dumps(e).encode())
    stored = wait_for(lambda: db.execute(
        "SELECT event_timestamp FROM market_events WHERE event_id = %s", (e["event_id"],)).fetchone())[0]
    assert stored == local and stored.utcoffset() == timedelta(0)
    in_utc = db.execute("SELECT to_char(event_timestamp AT TIME ZONE 'UTC', 'HH24:MI') FROM market_events WHERE event_id = %s",
                        (e["event_id"],)).fetchone()[0]
    assert in_utc == local.astimezone(UTC).strftime("%H:%M")


def test_quote_cache_rejects_older_quotes_and_rebuilds_from_postgres(redis):
    from processing.cache import QuoteCache
    from processing.consumers.processor import StreamProcessor
    from processing.repository import Store
    from processing.validators.event_validator import Rules

    key, channel = f"mp:test:quotes:{uuid.uuid4().hex}", f"mp:test:ch:{uuid.uuid4().hex}"
    cache = QuoteCache(redis, key, channel)
    store = Store(DSN)
    try:
        symbols = dict(store.conn().execute("SELECT symbol, id FROM instruments").fetchall())
        p = StreamProcessor(None, None, store, cache, {}, Rules(frozenset(symbols)), symbols, 3)
        written = p.rebuild_cache()
        latest = dict(store.conn().execute(
            """SELECT DISTINCT ON (i.symbol) i.symbol, e.event_id::text FROM market_events e
               JOIN instruments i ON i.id = e.instrument_id ORDER BY i.symbol, e.event_timestamp DESC""").fetchall())
        cached = {s: json.loads(v) for s, v in redis.hgetall(key).items()}
        assert written == len(latest) and {s.decode(): q["event_id"] for s, q in cached.items()}.keys() == latest.keys()
        assert all(q["origin"] == "rebuild" and q["event_ts_ms"] > 0 for q in cached.values())
        assert p.rebuild_cache() == 0  # nothing missing any more

        q = json.loads(redis.hget(key, "AAPL"))
        older = dict(q, event_id="older", event_ts_ms=q["event_ts_ms"] - 60_000)
        newer = dict(q, event_id="newer", event_ts_ms=q["event_ts_ms"] + 60_000)
        assert cache.write([older]) == [False] and json.loads(redis.hget(key, "AAPL"))["event_id"] == q["event_id"]
        assert cache.write([newer]) == [True] and json.loads(redis.hget(key, "AAPL"))["event_id"] == "newer"
    finally:
        redis.delete(key)
        store.close()
