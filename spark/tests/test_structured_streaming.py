"""Tests for structured_streaming.py on a local SparkSession. No Kafka needed; run with ../run_tests.sh."""

from __future__ import annotations

import json
import os
import uuid

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from structured_streaming import build_bars, parse_events


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    session = (
        SparkSession.builder.master("local[2]")
        .appName("marketpulse-spark-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def event(ts, price, volume, symbol="AAPL", event_id=None):
    """A market.processed message shaped like processing/consumers/processor.py publishes it."""
    return {
        "event_id": event_id or str(uuid.uuid4()), "symbol": symbol, "timestamp": f"2026-11-25T{ts}+00:00",
        "price": price, "volume": volume, "open": price, "high": price, "low": price, "close": price,
        "source": "simulator", "data_mode": "simulated",
        "tick_change_pct": None, "rolling_avg": price, "rolling_volume": volume, "rolling_window": 20,
    }


def without(message, key):
    return {k: v for k, v in message.items() if k != key}


def to_raw(spark, messages):
    """Kafka-like rows with a `value` column; str messages are passed through untouched (malformed input)."""
    return spark.createDataFrame(
        [(m if isinstance(m, str) else json.dumps(m),) for m in messages], "value string"
    )


def rows(bars_df):
    """Bars as dicts with HH:MM:SS window bounds (formatted in the UTC session zone), sorted."""
    out = bars_df.withColumn("window_start", F.date_format("window_start", "HH:mm:ss")).withColumn(
        "window_end", F.date_format("window_end", "HH:mm:ss")
    )
    return sorted((r.asDict() for r in out.collect()), key=lambda r: (r["symbol"], r["window_start"]))


def batch_bars(spark, messages):
    return rows(build_bars(parse_events(to_raw(spark, messages)), streaming=False))


def bar(start, end, trades, vwap, low, high, volume, last, symbol="AAPL"):
    return {"window_start": start, "window_end": end, "symbol": symbol, "vwap": pytest.approx(vwap),
            "trades": trades, "low": low, "high": high, "volume": volume, "last_price": last}


def test_window_boundaries_are_start_inclusive_end_exclusive(spark):
    out = batch_bars(spark, [event("10:00:05", 100.0, 1), event("10:00:59.999", 100.0, 1),
                             event("10:01:00", 100.0, 1)])
    assert [(r["window_start"], r["window_end"], r["trades"]) for r in out] == [
        ("10:00:00", "10:01:00", 2),
        ("10:01:00", "10:02:00", 1),
    ]


def test_vwap_and_bar_fields_with_input_out_of_event_time_order(spark):
    # (100*10 + 101*30 + 99*60) / 100 = 9970 / 100 = 99.7; the last trade by event time is 99 at 10:00:50.
    out = batch_bars(spark, [event("10:00:30", 101.0, 30), event("10:00:50", 99.0, 60),
                             event("10:00:10", 100.0, 10)])
    assert out == [bar("10:00:00", "10:01:00", 3, 99.7, 99.0, 101.0, 100, 99.0)]


def test_zero_total_volume_gives_null_vwap(spark):
    out = batch_bars(spark, [event("10:00:10", 100.0, 0), event("10:00:20", 101.0, 0)])
    assert out == [{**bar("10:00:00", "10:01:00", 2, 0, 100.0, 101.0, 0, 101.0), "vwap": None}]


def test_empty_input_gives_no_bars(spark):
    assert batch_bars(spark, []) == []


def test_republished_event_id_is_counted_once(spark):
    retried = event("10:00:10", 100.0, 10)
    out = batch_bars(spark, [retried, event("10:00:20", 110.0, 30), retried])
    # (100*10 + 110*30) / 40 = 107.5; counting the retry twice would give (2000 + 3300) / 50 = 106.
    assert out == [bar("10:00:00", "10:01:00", 2, 107.5, 100.0, 110.0, 40, 110.0)]


def test_parse_events_drops_invalid_records(spark):
    good = [event("10:00:01", 100.0, 5, event_id="ok"), event("10:00:02", 100.0, 0, event_id="ok-zero-volume")]
    bad = [
        "not json",
        '{"event_id": "truncated", "symbol": "AAPL", "price": 1',
        {**event("10:00:03", 100.0, 5, event_id="null-price"), "price": None},
        event("10:00:03", -1.0, 5, event_id="negative-price"),
        event("10:00:03", 0.0, 5, event_id="zero-price"),
        {**event("10:00:03", 100.0, 5, event_id="null-volume"), "volume": None},
        event("10:00:03", 100.0, -5, event_id="negative-volume"),
        {**event("10:00:03", 100.0, 5, event_id="bad-timestamp"), "timestamp": "yesterday"},
        without(event("10:00:03", 100.0, 5, event_id="missing-timestamp"), "timestamp"),
        without(event("10:00:03", 100.0, 5, event_id="missing-symbol"), "symbol"),
        without(event("10:00:03", 100.0, 5, symbol="MISSING-ID"), "event_id"),
    ]
    kept = parse_events(to_raw(spark, bad + good)).select("event_id", "symbol").collect()
    assert sorted((r.event_id, r.symbol) for r in kept) == [("ok", "AAPL"), ("ok-zero-volume", "AAPL")]


def test_symbols_are_aggregated_independently(spark):
    out = batch_bars(spark, [
        event("10:00:10", 100.0, 10), event("10:00:15", 300.0, 1, symbol="MSFT"),
        event("10:00:20", 102.0, 10), event("10:00:25", 310.0, 3, symbol="MSFT"),
    ])
    assert out == [
        bar("10:00:00", "10:01:00", 2, 101.0, 100.0, 102.0, 20, 102.0),
        bar("10:00:00", "10:01:00", 2, 307.5, 300.0, 310.0, 4, 310.0, symbol="MSFT"),  # (300 + 930) / 4
    ]


def start_stream(spark, tmp_path, name, mode):
    """build_bars over a text file source (one JSON message per line, like Kafka values) into a memory sink."""
    src, staging = tmp_path / "in", tmp_path / "staging"
    src.mkdir()
    staging.mkdir()
    query = (
        build_bars(parse_events(spark.readStream.text(str(src))), streaming=True)
        .writeStream.outputMode(mode).format("memory").queryName(name)
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .start()
    )

    def feed(n, messages):
        """One file = one micro-batch. Renamed into place so the source never sees a half-written file."""
        f = staging / f"batch-{n}.json"
        f.write_text("\n".join(json.dumps(m) for m in messages))
        os.replace(f, src / f.name)
        query.processAllAvailable()

    return query, feed


def test_streaming_ignores_late_events_and_republished_duplicates(spark, tmp_path):
    # Append mode: each window is emitted once, final, after the watermark passes its end (the same mode
    # as the Parquet sink), so the memory table holds exactly the finished bars and nothing to de-version.
    query, feed = start_stream(spark, tmp_path, "bars_append", "append")
    try:
        dup = event("10:04:00", 105.0, 20, event_id="dup")
        feed(1, [event("10:00:10", 100.0, 10), event("10:00:40", 102.0, 10), dup, event("10:05:00", 106.0, 5)])
        # Watermark is now 10:05:00 - 2 min = 10:03:00.
        feed(2, [
            event("10:00:30", 1.0, 1000),  # older than the watermark: must be ignored
            event("10:04:30", 107.0, 20),  # within the watermark: must count
            dup,                           # processor retry republished it: must not count again
        ])
        feed(3, [event("10:10:00", 110.0, 1)])  # watermark -> 10:08, so windows ending by then are emitted
        out = rows(spark.table("bars_append"))
    finally:
        query.stop()
    assert out == [
        bar("10:00:00", "10:01:00", 2, 101.0, 100.0, 102.0, 20, 102.0),
        bar("10:04:00", "10:05:00", 2, 106.0, 105.0, 107.0, 40, 107.0),  # (105*20 + 107*20) / 40
        bar("10:05:00", "10:06:00", 1, 106.0, 106.0, 106.0, 5, 106.0),
    ]


def test_streaming_update_mode_is_supported(spark, tmp_path):
    # main()'s console sink uses update mode; dedup chained before the aggregation must be accepted there too.
    query, feed = start_stream(spark, tmp_path, "bars_update", "update")
    try:
        e = event("10:00:10", 100.0, 10)
        feed(1, [e, e])
        out = rows(spark.table("bars_update"))
    finally:
        query.stop()
    assert out == [bar("10:00:00", "10:01:00", 1, 100.0, 100.0, 100.0, 10, 100.0)]
