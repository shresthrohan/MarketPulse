# Streaming pipeline

## Topics

| Topic | Partitions | Retention | Key | Producer | Content |
|---|---|---|---|---|---|
| `market.raw` | 6 | 24 h | symbol | ingestion | provider events as received (malformed payloads included, unkeyed) |
| `market.validated` | 6 | 24 h | symbol | processor | events that passed validation and were newly stored, normalised (UTC, upper-case symbol) |
| `market.processed` | 6 | 24 h | symbol | processor | validated + `tick_change_pct`, `rolling_avg`, `rolling_volume`, `rolling_window` |
| `market.errors` | 3 | 7 d | symbol | processor | dead letters: every rejected or duplicate event with `error_type`, `message`, `raw` |

Created idempotently by `infrastructure/kafka/create-topics.sh` (`kafka-init` service). Auto-creation is off.
Keying by symbol keeps each instrument's events in one partition, so per-instrument order is preserved end to end;
up to 6 processor instances can share the load through the consumer group `marketpulse-processor`.

## Event schema (v1)

```json
{
  "event_id": "uuid",              // unique per event; providers derive it from content where possible
  "symbol": "AAPL",                // must exist in instruments
  "timestamp": "ISO-8601 with offset",
  "price": 225.50,                 // > 0
  "volume": 1200,                  // >= 0; fractional for crypto (0.0012 BTC)
  "open": 224.00, "high": 226.00, "low": 223.50, "close": 225.50,   // session values, low <= o,c,price <= high
  "source": "simulator",           // provider name: simulator | binance | finnhub
  "data_mode": "simulated",        // simulated | live | delayed
  "schema_version": 1,
  "ingested_at": "ISO-8601"        // added by the ingestion producer
}
```

Data provenance: every instrument has a recorded `data_mode` (`instruments.data_mode`, set once by the seed).
The validator rejects an event whose kind (simulated vs. real; live and delayed are both real) differs from its
instrument's, so no instrument ever mixes simulated and real data.

## Ingestion (`ingestion/`)

- `providers/base.py` — the provider contract: `stream()` yields events (dicts) or raw bytes.
- `providers/simulator.py` + `simulator/` — one-factor geometric Brownian motion
  (`S(t+dt) = S(t)·exp((μ − σ²/2)dt + σ√dt·z)`, μ = 0) with correlated shocks
  (`z_i = β_i·z_market + √(1−β_i²)·ε_i`), lognormal trade sizes, per-symbol UTC session OHLC.
  Deterministic for a given `SIM_SEED`, initial prices and clock. Restarts resume from the last cached price.
  `SIM_FAULT_RATE` (default 0.5%) deliberately emits bad events — negative/null price, missing field, bad OHLC,
  bad volume, unknown symbol, future timestamp, duplicate ID, truncated JSON — so the quality layer is always exercised.
- `providers/binance.py` — **live crypto, no credentials**: Binance public combined stream of 1-second klines
  (`<pair>@kline_1s`) for the pairs in `config/binance.json`. Only closed seconds with at least one trade become
  events (bounded rate: at most one event per pair per second, instead of hundreds of raw trades). Event =
  price = close of that second, volume = base-asset quantity traded in it (fractional), UTC-day session
  OHLC, `source` `binance`, `data_mode` `live`; `event_id` = UUIDv5 of (pair, kline open time), so a reconnect
  replay deduplicates. Malformed frames are forwarded as bytes (recorded, not dropped); reconnects use jittered
  backoff; a geo-block (HTTP 451/403) is reported in the status. Hosts are configurable (`BINANCE_WS_URL`;
  binance.com refuses US addresses, use binance.us there).
- `providers/finnhub.py` — real US equities: WebSocket trades (`finnhub_ws`, data_mode `live`) or polled quotes
  (`finnhub_rest`, data_mode `delayed`). Needs `FINNHUB_API_KEY` (sent as a header, redacted from logs); without
  it the service logs a warning and uses the simulator. Finnhub replaces the simulator for equities; the two
  cannot run together.
- `PROVIDERS` (comma-separated) chooses what runs; each provider is its own pump task into the same producer,
  and one ending or crashing exits the service so Docker restarts it. The heartbeat lists every provider's
  status; overall status is `streaming` only when all are healthy.
- `producers/kafka_producer.py` — `acks=all`, idempotent producer, lz4, 5 ms linger, 30 s message timeout.
  Delivery failures are counted and reported in the heartbeat.
- Heartbeat every 2 s to Redis `mp:svc:ingestion:<host>` (TTL 10 s): provider status, produced/delivered/failed
  counts, events/s, CPU, RSS.

Adding a provider: implement `stream()` in a new module, add its name to `KNOWN_PROVIDERS` and one branch to
`ingestion/main.py: build_provider`.

## Processing (`processing/`)

Consumer group `marketpulse-processor`: `enable.auto.commit=false`, `auto.offset.reset=earliest`,
`max.poll.interval.ms=300000`, `session.timeout.ms=45000`. Up to 6 instances share the 6 partitions.

Per batch (≤ `BATCH_MAX` = 500 messages, `CONSUME_TIMEOUT_S` = 0.05 s):

1. **Validate** (`validators/event_validator.py`, pure). Each payload becomes `ValidEvent` or `Rejected`:
   size ≤ 16 KB, JSON object, required fields, UUID `event_id`, symbol format, printable `source`, `data_mode`,
   price (null → `null_price`, negative → `negative_price`, zero/non-finite/huge → `invalid_price`),
   volume a number ≥ 0 (fractional allowed), OHLC positive and `low ≤ open, close, price ≤ high`, timestamp ISO-8601 *with offset*,
   not more than `MAX_FUTURE_SKEW_S` (30 s) ahead or `MAX_EVENT_AGE_H` (24 h) behind, symbol tracked, data kind matching the instrument's recorded `data_mode`.
   Duplicate IDs inside the batch are rejected here.
2. **Store** (`repository.py: write_batch`, one transaction):
   insert events `ON CONFLICT (event_id) DO NOTHING RETURNING event_id` → newly inserted vs. already stored
   (`duplicate_event`); upsert 1m and 1d bars from newly inserted events only (`aggregators/bars.py` pre-aggregates
   the batch); insert every reject and duplicate into `processing_errors`; then, still inside the transaction,
   publish to `market.validated`, `market.processed` and `market.errors` and wait for broker acks. Any failure
   rolls the whole batch back.
3. **Adopt rolling state** (`transformers/enrich.py`, pure, returns a new state) only after the commit.
4. **Cache** — per symbol, the latest event *by event time* in the batch becomes the quote (day OHLCV from the
   committed 1d bar). It is written by an atomic Lua set-if-newer on `event_ts_ms` and published on `mp:ticks`, together
   with the full current 1m bar. Failures are logged and counted (`redis_failures`). Every 5 s the processor also
   restores any symbol missing from `mp:quotes` from PostgreSQL (`origin: "rebuild"`), so Redis data loss heals even
   for instruments that are not trading.
5. **Commit offsets** synchronously.

### Delivery guarantees

- Storage: **at-least-once delivery, idempotent writes** — `UNIQUE(event_id)` + bars derived only from newly
  inserted rows, in one transaction, so each `event_id` is stored once. This is not exactly-once processing: a crash
  anywhere before step 5 replays the batch, and replayed events are processed again and recorded as `duplicate_event`
  warnings (replayed invalid events are logged again under their error type). Verified live by the offset-rewind drill
  (1,200 replayed messages → 0 new rows).
- Derived topics are **at-least-once**: a crash after publishing but before the commit republishes on retry.
  Consumers of `market.processed` must deduplicate by `event_id` if exact counts matter (the Spark job does).
- Ordering: per instrument (partition key). Late events still merge correctly into bars (open/close by event time).

### Retries and dead letters

| Failure | Handling |
|---|---|
| Invalid event | `processing_errors` + `market.errors`; offset advances |
| Duplicate event | same, severity `warning` |
| Validator raises unexpectedly on a payload | that payload is rejected (`invalid_field`, counted as `validator_crashes`); the batch continues |
| Transient DB / Kafka error (`OperationalError`, `InterfaceError`, flush timeout) | whole batch retried up to 6 attempts with jittered exponential backoff (0.5→8 s); events stored after a retry get `processing_status='retried'` |
| Still failing after 6 attempts | process exits non-zero **without** committing offsets; Docker restarts it and it resumes from the last committed offset |
| Constraint/data error (`DataError`, `IntegrityError`) | batch retried one event at a time; events the DB refuses are dead-lettered as `db_rejected`, the rest are stored |
| Redis down | quotes/WebSocket stall, storage continues; counted as `redis_failures` |
| Redis data lost | missing quotes rebuilt from PostgreSQL within one metrics interval |

`market.errors` is a sink: nothing in the system consumes it, so a dead letter is never retried automatically and
cannot loop. Rejected events are also queryable in `processing_errors` (API: `/api/pipeline/errors`, Event Logs page).

### Reconciliation

- Per batch, `events_received = events_valid + events_invalid + events_duplicate` (unit-tested with fakes).
- Per event, every produced `event_id` is either stored once or present in `processing_errors` with its reason
  (integration test `test_every_produced_event_is_accounted_for_exactly_once`; the benchmark reports the same
  accounting for 20,000 events).
- Per topic, `raw_end_offsets = raw_committed_offsets + consumer_lag` (`/api/health/kafka`). There are no per-symbol
  sequence numbers, so a gap is detected as a count mismatch (producer `delivery_failed`, offsets, lag), not as a
  missing sequence number.

### Metrics

Written to `pipeline_metrics` every 5 s (see `docs/database.md`), heartbeat to Redis `mp:svc:processor:<host>`
(TTL 15 s) with lifetime totals and consumer lag. Consumer lag = Σ partitions (high watermark − position);
the API independently reads committed offsets vs. high watermarks, so lag stays truthful when the processor is down.

## Optional: Spark Structured Streaming (`spark/`)

`docker compose --profile spark up spark` runs `spark/structured_streaming.py`: reads `market.processed`, drops
invalid records, deduplicates by `event_id` within a 2-minute watermark (the topic is at-least-once), and builds
1-minute tumbling windows per symbol with VWAP, trade count, min/max/last price and volume to a console sink (optional
Parquet sink via `SPARK_OUTPUT_PATH`). Not part of the core path. Tests: `spark/tests` (9, run in the Spark image via
`spark/run_tests.sh`, also in CI) cover window boundaries, VWAP against hand calculations, empty input, duplicates,
invalid records and late events in a real streaming query. The Kafka source itself was verified by a manual run.
