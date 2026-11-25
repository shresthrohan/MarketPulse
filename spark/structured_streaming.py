"""Optional advanced mode: 1-minute VWAP bars from `market.processed` with Spark Structured Streaming.

The core pipeline never needs Spark. This job shows the same processed stream consumed by an
independent engine with event-time windows and watermarks. See README.md for how to run it.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Mirrors the message built in processing/consumers/processor.py (_before_commit).
PROCESSED_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),  # ISO-8601 with offset; cast below (lenient, keeps microseconds)
    StructField("price", DoubleType()),
    StructField("volume", DoubleType()),  # fractional for crypto (base-asset quantity)
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("source", StringType()),
    StructField("data_mode", StringType()),
    StructField("tick_change_pct", DoubleType()),
    StructField("rolling_avg", DoubleType()),
    StructField("rolling_volume", DoubleType()),
    StructField("rolling_window", IntegerType()),
])


def parse_events(raw: DataFrame) -> DataFrame:
    """Kafka-shaped rows (JSON in `value`) -> typed events plus `event_time`; drops records a bar can't use."""
    return (
        # Unparseable JSON becomes a null struct, i.e. all-null fields, which the filter below drops.
        raw.select(F.from_json(F.col("value").cast("string"), PROCESSED_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.col("timestamp").cast("timestamp"))  # null if missing or unparseable
        .where(
            F.col("event_time").isNotNull() & F.col("symbol").isNotNull() & F.col("event_id").isNotNull()
            # A comparison with null is not true, so these also drop null price / null volume.
            & (F.col("price") > 0) & (F.col("volume") >= 0)
        )
    )


def build_bars(events: DataFrame, *, streaming: bool) -> DataFrame:
    """1-minute bars per symbol: VWAP, trades, low/high, volume, last price by event time."""
    events = events.withWatermark("event_time", "2 minutes")  # no-op on a batch DataFrame
    # market.processed is at-least-once: a processor retry republishes the same event_id. The streaming
    # form forgets an id once the watermark passes it, so its state stays bounded; it is streaming-only.
    if streaming:
        events = events.dropDuplicatesWithinWatermark(["event_id"])
    else:
        events = events.dropDuplicates(["event_id"])
    return (
        events.groupBy(F.window("event_time", "1 minute"), "symbol")
        .agg(
            F.sum(F.col("price") * F.col("volume")).alias("notional"),
            F.sum("volume").alias("volume"),
            F.count(F.lit(1)).alias("trades"),
            F.min("price").alias("low"),
            F.max("price").alias("high"),
            F.max_by("price", "event_time").alias("last_price"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            F.when(F.col("volume") > 0, F.col("notional") / F.col("volume")).alias("vwap"),
            "trades", "low", "high", "volume", "last_price",
        )
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("marketpulse-vwap-1m")
        .config("spark.sql.session.timeZone", "UTC")
        # 20 symbols: the default 200 shuffle partitions would mean 200 tiny state stores per micro-batch.
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
        .option("subscribe", os.environ.get("PROCESSED_TOPIC", "market.processed"))
        .option("startingOffsets", "latest")
        # Topic retention (24 h) can delete offsets an old checkpoint still points at; skip ahead instead of failing.
        .option("failOnDataLoss", "false")
        .load()
    )
    bars = build_bars(parse_events(raw), streaming=True)

    checkpoint = os.environ.get("SPARK_CHECKPOINT_PATH", "/tmp/marketpulse-spark/checkpoints")
    # Console shows bars as they fill ("update"); the file sink only supports "append", which writes
    # each window once, after the watermark passes its end (window end + 2 minutes).
    (bars.writeStream.outputMode("update").format("console")
     .option("truncate", "false").option("numRows", 100)
     .option("checkpointLocation", f"{checkpoint}/console")
     .trigger(processingTime="10 seconds").start())
    if output := os.environ.get("SPARK_OUTPUT_PATH"):
        (bars.writeStream.outputMode("append").format("parquet")
         .option("path", output)
         .option("checkpointLocation", f"{checkpoint}/parquet")
         .trigger(processingTime="30 seconds").start())
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
