# Spark Structured Streaming (optional)

`structured_streaming.py` reads the processor's `market.processed` topic and builds 1-minute
bars per symbol with event-time windows: VWAP (`sum(price*volume) / sum(volume)`, null when the
window's volume is 0), trade count, min/max price, total volume and last price.

The basic system does not use Spark. Nothing else depends on this job, and it only reads Kafka.

## Run

The service lives under the compose profile `spark`, so a plain `docker compose up` never starts it.

```sh
docker compose up -d                       # the normal stack
docker compose --profile spark up spark    # add the Spark job (Ctrl-C to stop)
```

It uses the stock `apache/spark:3.5.3-python3` image and fetches the Kafka connector at start-up:

```sh
/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/marketpulse/structured_streaming.py
```

`spark.jars.ivy` is set because the image's `spark` user has no home directory, and `--packages`
fails without a writable Ivy cache. The connector (about 15 MB with its dependencies) is
downloaded again each time the container is recreated.

Output appears every 10 s in the service log:

```
+-------------------+-------------------+------+------------------+------+------+------+------+----------+
|window_start       |window_end         |symbol|vwap              |trades|low   |high  |volume|last_price|
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka brokers |
| `PROCESSED_TOPIC` | `market.processed` | input topic |
| `SPARK_CHECKPOINT_PATH` | `/tmp/marketpulse-spark/checkpoints` | streaming state and offsets (`/console`, `/parquet` subdirectories) |
| `SPARK_OUTPUT_PATH` | unset | if set, finalized bars are also written as Parquet here |
| `SPARK_SHUFFLE_PARTITIONS` | `4` | shuffle partitions; the Spark default of 200 is wasteful for 20 symbols |

The default checkpoint is inside the container, so a recreated container starts again from the
latest offset. To keep state across restarts, point `SPARK_CHECKPOINT_PATH` (and `SPARK_OUTPUT_PATH`)
at a mounted directory that the `spark` user (uid 185) can write.

## Semantics

- Event time is the event's `timestamp`. The watermark is 2 minutes, so events up to 2 minutes
  late still count, and older ones are dropped.
- The console sink uses `update` mode, so each micro-batch shows the bars that changed. The Parquet
  sink uses `append` mode (the only mode file sinks support), so each window is written once, after
  the watermark passes its end (window end + 2 minutes).
- `startingOffsets` is `latest`: the job aggregates what arrives after it starts, and does not
  backfill. `failOnDataLoss` is off because the topic keeps 24 h of data and can delete offsets an
  old checkpoint still points to.
- `market.processed` is at-least-once (see `processing/consumers/processor.py`), so a processor
  retry can publish the same event again. The job drops repeats by `event_id` with
  `dropDuplicatesWithinWatermark` (Spark 3.5 and later). An id is kept in state until the watermark
  passes its event time plus 2 minutes, so state stays bounded. A copy that arrives later than that
  falls in a window that is already closed, and it is dropped as late anyway.
- Records that cannot form a bar are dropped before aggregation. These are unparseable JSON, a
  missing or unparseable `timestamp`, a missing `symbol` or `event_id`, a null or non-positive
  price, and a null or negative volume. A volume of 0 is kept, and a window whose total volume is 0
  has a null VWAP.

## Tests

The job's logic lives in two functions that work on ordinary DataFrames. `parse_events` turns
Kafka-style rows, which hold JSON in `value`, into typed events and drops invalid records.
`build_bars` adds the watermark, removes duplicates and aggregates. `main()` only connects them to
the Kafka source and the sinks. The tests in `tests/` use a local SparkSession and need no Kafka.
Run them from the repository root:

```sh
docker run --rm -v "$PWD/spark:/opt/marketpulse" -w /opt/marketpulse apache/spark:3.5.3-python3 sh run_tests.sh
```

In Git Bash on Windows, put `MSYS_NO_PATHCONV=1` in front of the command and use a Windows-style
path such as `-v "F:/path/to/MarketPulse/spark:/opt/marketpulse"`. `run_tests.sh` installs pytest
into `/tmp` inside the container, because the image has neither pytest nor a home directory, and
then runs pytest. Arguments are passed through, for example `sh run_tests.sh -k streaming`. A run
takes about 30 seconds.

Batch tests call `build_bars(..., streaming=False)` and check values worked out by hand. They cover
window boundaries (a window includes its start and excludes its end), VWAP, trade count, low, high,
volume and last price when input arrives out of order, a null VWAP for zero volume, empty input, a
republished `event_id` counted once, symbols aggregated separately, and every kind of invalid record
that `parse_events` drops. Two streaming tests feed JSON files through a file source into a memory
sink, one file per micro-batch. The append-mode test checks the final bars after three batches. In
those bars, an event older than the watermark is ignored, an event inside the watermark is counted,
and a duplicate that arrives in a later micro-batch is counted once. The update-mode test checks
that the dedup-then-aggregate plan is accepted in the mode the console sink uses.
