# Architecture

```
 Market data sources            ┌──────────────── docker compose network ────────────────────────────────┐
 ─────────────────────          │                                                                         │
 Simulator (default)  ──┐       │  ingestion ──► Kafka market.raw ──► processing ──► PostgreSQL/Timescale │
 Binance (crypto,live)──┼──────►│  (providers,      (6 partitions,     (validate,     events, bars,        │
 Finnhub (optional)   ──┘       │   producer,        key = symbol)      store, bars,   errors, metrics,     │
                                │   heartbeat)                          dead-letter)   watchlists           │
                                │                    market.validated ◄───┤                 │               │
                                │                    market.processed ◄───┤ ──► Redis ──────┤               │
                                │                    market.errors    ◄───┘    quotes,      │               │
                                │                         │                    heartbeats,  │               │
                                │              (optional) Spark VWAP           mp:ticks     │               │
                                │                                                 │         │               │
                                │                                   FastAPI ◄─────┴─────────┘               │
                                │                                   REST /api/*  +  WS /ws/market           │
                                │                                        │                                  │
                                │                          nginx + React terminal (:8080)                   │
                                └─────────────────────────────────────────────────────────────────────────┘
```

## Components

| Component | Responsibility | Owns | Does not |
|---|---|---|---|
| `ingestion/` | Pull/stream from a provider, stamp `ingested_at`, publish to `market.raw` keyed by symbol, heartbeat | provider adapters, simulator | touch the database or validate |
| Kafka (KRaft, single broker) | Durable ordered log between services; consumer-group offsets | 4 topics | — |
| `processing/` | Validate, deduplicate, store events + bars + errors atomically, publish derived topics, refresh Redis, record metrics, retention | all writes to `market_events`, `historical_aggregates`, `processing_errors`, `pipeline_metrics` | serve reads |
| PostgreSQL + TimescaleDB | Source of truth for history, watchlists, metrics, errors | schema (Alembic) | — |
| Redis | Latest quote per symbol, service heartbeats, tick pub/sub | derived state only | hold anything that cannot be rebuilt |
| `backend/` (FastAPI) | REST + WebSocket, analytics (pandas/NumPy), health checks; routers → services → repositories | watchlist writes, analytics | write market data |
| `frontend/` | Institutional terminal UI; one WebSocket per tab, REST via TanStack Query | presentation | compute rankings, bars or analytics |
| `scripts/` | `seed_data.py` (instruments + 1 year of continuous simulated history), `benchmark.py` | — | — |

## Event lifecycle

1. **Provider → ingestion.** Each configured provider (simulator, Binance, Finnhub; `PROVIDERS`) yields event dicts, or raw bytes it could not
   parse. The producer adds `ingested_at` and publishes to `market.raw`, keyed by symbol (`acks=all`, idempotent
   producer). Delivery failures are counted in the ingestion heartbeat.
2. **Kafka.** Six partitions; the key pins each instrument to one partition, so per-instrument order is the offset order.
3. **Processor consume.** Consumer group `marketpulse-processor`, `enable.auto.commit=false`,
   `auto.offset.reset=earliest`, batches of up to 500 messages, `consume()` timeout 50 ms.
4. **Validate** (pure, total). Every payload becomes `ValidEvent` or `Rejected(error_type)`. Same-batch duplicate IDs are rejected here.
5. **Store** (one PostgreSQL transaction): insert events `ON CONFLICT (event_id) DO NOTHING RETURNING`, upsert 1m/1d bars
   from the newly inserted events only, insert every reject and duplicate into `processing_errors`, publish to
   `market.validated` / `market.processed` / `market.errors` and wait for broker acks, then COMMIT. Transient failures
   retry the whole batch (≤ 6 attempts); permanent ones isolate the poison event (`db_rejected`).
6. **Cache.** The quote per symbol is the latest event *by event time*, with day OHLCV from the committed 1d bar, written
   to Redis by an atomic set-if-newer and published on `mp:ticks` along with 1-minute bar updates.
7. **Commit offsets** synchronously. A crash before this point replays the batch; storage is unaffected (I1, I2).
8. **Serve.** FastAPI reads history from PostgreSQL and latest state from Redis; one pub/sub listener fans `mp:ticks`
   out to WebSocket clients through per-client bounded queues.

## Deduplication key and ordering

- **Key:** `event_id` (UUID). Uniqueness is enforced by PostgreSQL (`uq_market_events_event_id`), never by process
  memory, so it holds across restarts and processor instances. Providers derive IDs from what the event *is*:
  - Finnhub: UUIDv5 of the trade's content. Binance: UUIDv5 of (pair, 1-second kline open time).
  - Simulator: UUIDv5 of run, symbol, timestamp and sequence, so a provider resend or a replay carries the same ID.
- **Order:** per instrument, by Kafka offset (partition key = symbol). No global order is promised or needed.
  Event time is used where it matters semantically: bar open/close merge by `first_event_at`/`last_event_at`, and the
  cached quote only moves forward in event time. Rolling windows (`rolling_avg`, `tick_change_pct`) follow processing
  order and are labelled as such.

## Delivery guarantees

| Path | Model | Mechanism |
|---|---|---|
| Provider → Kafka | at-least-once (producer retries are deduplicated by the idempotent producer) | `acks=all`, `enable.idempotence` |
| Kafka → PostgreSQL | at-least-once delivery, idempotent storage: one row per `event_id` | unique key + bars from inserted rows, in one transaction; offsets committed after |
| Processor → derived topics | at-least-once (duplicates after retry/replay) | published inside the transaction, before COMMIT |
| Processor → Redis | best effort, self-healing | conditional writes; rebuild from PostgreSQL |
| Redis → WebSocket clients | at-most-once, conflated (latest quote per batch) | pub/sub; after a reconnect the UI catches up from its periodic REST queries and the next quotes |

Exactly-once processing is **not** claimed. A replayed event is processed again and recorded as a `duplicate_event`
warning, and a replayed invalid event is logged again under its error type (the error log is per delivery).

**Crash after the DB write, before the offset commit:** the batch is redelivered. Inserts conflict, so no rows and no
bar volume are added. Each replayed event is logged as `duplicate_event` (also published to `market.errors`);
`market.validated`/`market.processed` receive nothing new for those events (they were published before the original commit).
Verified by `test_failed_offset_commit_is_tolerated_and_the_replay_creates_no_duplicates` and the
`processor_kill` / `offset_rewind_replay` drills.

**Poison messages:** malformed or rule-breaking payloads are rejected, not retried. A validator bug cannot block a
partition (supervision boundary per message). A payload the database refuses is isolated by single-event retries and
dead-lettered as `db_rejected`. Only infrastructure failures (DB or broker unavailable) hold a partition, and only
until the dependency returns. `market.errors` has no consumer in this system, so nothing ever re-reads or retries it.

## Redis strategy

PostgreSQL is the source of truth; Redis holds derived state only:

| Key | Content | Lifetime |
|---|---|---|
| `mp:quotes` (hash) | latest quote per symbol (`event_ts_ms`, `origin` = `stream` or `rebuild`) | persistent (AOF); never expired, staleness reported instead |
| `mp:svc:<service>:<host>` | heartbeat JSON | TTL 10–15 s: absence = service down |
| `mp:ticks` (channel) | quote + 1m bar updates | pub/sub, not stored |

- **Unavailable:** storage continues (Redis is after the commit). The API answers 200 with `quote: null` and
  `cache.available: false`, health shows Redis down, and the processor counts `redis_failures`.
- **Restarted with data:** AOF restores quotes.
- **Restarted empty (or FLUSHALL):** the processor detects missing symbols and rebuilds them from PostgreSQL within one
  metrics interval (5 s). Ingestion resumes the simulated walk from those prices.
- **Stale data:** `GET /api/market/overview` → `cache.age_s` and `cache.stale` (> 60 s since the last processed update).

## Migrations

Alembic, schema of record in `backend/alembic/versions`. The one-shot `migrate` service runs `alembic upgrade head`
then the idempotent seed before any service that needs the schema starts (`depends_on: service_completed_successfully`).
TimescaleDB is enabled only if the extension is available. `alembic check` in CI fails on any drift between the ORM
models and the migrations. New schema changes go in a new revision; existing revisions are never edited.

## Design audit (premises checked before writing I/O code)

| Requirement as stated | Problem if taken literally | What was built |
|---|---|---|
| "Store valid events, cache latest prices, route invalid events" (DB + Redis + Kafka) | Three writes with no shared transaction: a crash between them leaves them disagreeing forever | DB writes (events, bars, errors) in **one** transaction; derived Kafka topics published inside it before commit (at-least-once); Redis is derived and repaired by the next batch; offsets committed last |
| "Detect duplicate event IDs" | An in-memory set forgets on restart and differs per instance | `UNIQUE(event_id)` + `ON CONFLICT DO NOTHING RETURNING` decides; bars only from returned rows |
| "Aggregate volume", "rolling averages" | Re-processing a batch after a crash double-counts | Bars updated in the same transaction from newly inserted rows only; rolling state is a pure value adopted after commit |
| "Retry handling" | Unbounded retry hides an outage; retrying a bad record forever blocks the partition | Transient errors: 6 bounded, jittered attempts then exit without committing offsets (visible as a restart + down heartbeat). Permanent errors: isolate the poison event, dead-letter it, store the rest |
| "Handle malformed records without crashing" | A reject that itself violates a column width or contains NUL fails its own insert → poison loop | Reject fields are sanitised and truncated at construction; payloads > 16 KB rejected before parsing |
| "Pipeline can restart without duplicating history" | Seeded history and live data from different random paths jump at the seam | Backfill generated *backwards* from the simulator's start price; restarts resume from the cached last price |
| "TimescaleDB where supported" for all time-series | Hypertable unique indexes must include time → dedup key would become `(event_id, time)` | Bars are a hypertable; `market_events` stays a plain table with a global `UNIQUE(event_id)` |
| "Real-time updates, don't poll per price" | Fan-out per tick to N slow browsers grows memory without bound | One pub/sub listener, per-client bounded queue (256); a client that falls behind is closed with 1013 and reconnects |
| "Health statuses must be real" | A status read from the service itself says "up" while it is hung | Heartbeats with TTL (absence = down); DB/Redis/Kafka checked on request with deadlines; lag read from the broker, not from the processor |
| Event timestamps as ordering | Wall clocks differ between producers | Ordering is Kafka offset order per partition; bar open/close merge by event time with `first/last_event_at` |

**Contract notes** (behaviour a reader might not assume):

- Derived topics (`market.validated`, `market.processed`, `market.errors`) are at-least-once, not exactly-once.
- A replayed event is logged as a `duplicate_event` warning (it is one); after a crash, a few such warnings are expected.
- Quotes on the WebSocket are conflated: at most one per symbol per processed batch.
- Day change (`change_pct`) is versus the UTC-day open (the simulated venue trades 24/7).

## Invariants and where each is enforced

| # | Invariant | Enforced by |
|---|---|---|
| I1 | An event ID is stored at most once | `uq_market_events_event_id` + `ON CONFLICT DO NOTHING` (`processing/repository.py: INSERT_EVENTS`) |
| I2 | Bars reflect each stored event exactly once | `write_batch`: `aggregate(inserted)` inside the same transaction as the insert |
| I3 | Every consumed payload ends as a stored event or a recorded reject | `validate` is total (fuzz-tested) and `StreamProcessor._classify` rejects any unexpected validator exception per message; rejects are inserted in the same transaction; offsets are committed only after commit |
| I4 | Offsets never pass an uncommitted batch | `enable.auto.commit=false`; `consumer.commit()` is the last step of `handle()`; exhausted retries raise before it |
| I5 | Rolling windows never count a rolled-back batch | `enrich` is pure; `_pending` state adopted only after `write_batch` returns |
| I6 | Stored prices are positive and OHLC-consistent | validator + `CHECK` constraints (`ck_market_events_*`) |
| I7 | A reject can always be persisted | `Rejected.__post_init__` sanitises NUL and truncates to column widths |
| I8 | Every queue/buffer is bounded | WS client queue (`WS_QUEUE_SIZE`), connection cap (`WS_MAX_CONNECTIONS`), rate-limiter map (10k keys), request-stats window (2,000), batch size (500), payload size (16 KB), producer queue (librdkafka) |
| I9 | Every external wait has a deadline | DB `statement_timeout` 15 s + pool timeout 10 s; Redis request-path socket timeouts 2–3 s (the pub/sub listener has no read timeout because idle channels are normal; it uses 30 s health checks instead); Kafka admin/lag calls in the API 3 s each, 12 s overall (the processor's in-loop lag uses cached watermarks and never blocks); derived-topic flush 10 s; Finnhub HTTP 10 s; WS open 15 s |
| I10 | Health never reports a dead service as up | TTL heartbeats + on-request checks (`backend/app/services/infra.py`) |
| I11 | The cached quote never moves backwards in event time | latest-by-event-time per batch + Lua set-if-newer (`processing/cache.py`) |
| I12 | A WebSocket slot is always released | `manager.unregister` runs first in the handler's `finally`, before any await |
| I13 | An instrument never mixes simulated and real data; simulated data is never shown unlabelled | `instruments.data_mode` set once by the seed (`COALESCE`); the validator rejects events of the other kind; analytics refuse to combine kinds (`same_data_source`); the UI tags every row SIM/LIVE when both kinds exist |

## Failure modes

| Failure | What happens | Invariant kept |
|---|---|---|
| Processor killed mid-batch (before DB commit) | Transaction rolls back; batch redelivered | I1–I5 |
| Killed after DB commit, before offset commit | Batch redelivered; inserts conflict → duplicate warnings; bars unchanged | I1, I2 |
| Killed after publishing derived topics, before commit | Derived messages duplicated on retry (documented at-least-once) | I1–I3 |
| PostgreSQL down | Batch retried 6× (~15 s) then process exits; Docker restarts; ingestion keeps filling Kafka; lag grows and is shown on the monitor | I3, I4 |
| PostgreSQL slow | `statement_timeout` 15 s → treated as transient | I9 |
| Kafka down | Ingestion buffers up to librdkafka limits then counts delivery failures (heartbeat); processor waits; health shows Kafka down | I8 |
| Redis down | Storage continues; quotes/WS stall; heartbeats missing → services shown down; API returns quotes `null` and `cache.available=false` | I3 |
| Redis loses its data | Processor rebuilds missing quotes from PostgreSQL within 5 s; simulator resumes from them | I11 |
| Late / out-of-order event | Stored and merged into the right bars; does not replace a newer quote | I2, I11 |
| Validator bug on one payload | That payload is rejected (`validator_crashes` counted); the partition keeps moving | I3 |
| Poison event (DB refuses it) | One-by-one isolation; `db_rejected` dead letter; rest stored | I3, I7 |
| Malformed / hostile payload | Rejected with type; size-capped before parsing | I3, I7 |
| Duplicate submission | `duplicate_event` warning, no second row | I1 |
| Slow WebSocket client | Its queue fills → closed with 1013; others unaffected | I8 |
| Second processor instance | Consumer group splits partitions; dedup and bar merges are in the DB, so overlap during rebalance is safe | I1, I2 |
| Graceful shutdown (SIGTERM) | Loop finishes the current batch, closes consumer, flushes producer | I4 |

## Deliberately not built

- Schema registry / Avro: one schema version, validated by code at the trust boundary.
- Generic retry decorator, plugin registry, DI container: one retry site, one provider switch.
- Authentication/roles: local single-user app (per the brief). CORS is restricted and the API binds to 127.0.0.1.
- Exactly-once Kafka transactions: storage is already idempotent; derived topics are documented at-least-once.
- Distributed rate limiting: per-process limiter is enough for one backend instance.

## Verification

See README "Testing" for commands and counts. Each safety invariant was checked by seeding a fault and confirming
a test fails (then reverting):

| Seeded fault | Caught by |
|---|---|
| Remove `ON CONFLICT (event_id) DO NOTHING` | `test_valid_event_is_stored_once_and_cached`, `test_replayed_event_does_not_double_count_bar_volume` |
| Aggregate bars from all valid events instead of newly inserted ones | `test_replayed_event_does_not_double_count_bar_volume` |
| Drop the OHLC relationship check | `test_each_data_quality_rule_rejects_with_its_error_type[invalid_ohlc]` |
| Accept naive timestamps | same test, `invalid_timestamp` case |
| Accept unknown symbols | same test, `unknown_symbol` case |
| `enrich` mutates caller state | `test_enrichment_computes_tick_change_and_rolling_window_without_mutating_input` |
| Bar close taken from earliest event | `test_bars_take_open_from_earliest_close_from_latest_and_sum_volume` |
| Rate limiter never records hits / unbounded key map | `test_rate_limiter_blocks_after_limit_and_recovers_after_window`, `test_rate_limiter_memory_is_bounded` |
| WS fan-out ignores subscriptions / never drops slow clients | `test_fanout_respects_subscriptions_and_flags_slow_clients` |
| Volatility with population std (ddof=0) | `test_rolling_volatility_is_sample_std_annualised` |
| Drawdown against global max | `test_drawdown_recovery_date_is_first_close_back_at_peak` |
| Missing quotes sorted first | `test_sort_puts_missing_quotes_last_in_both_directions` |

Audit round (see [audit.md](audit.md)):

| Seeded fault | Caught by |
|---|---|
| `data_mode` type check removed / float overflow not caught | `test_validator_never_raises_on_arbitrary_input` |
| No supervision boundary around `validate` | `test_an_unexpected_validator_exception_rejects_only_that_message` |
| Quote = last arrival instead of latest event time | `test_quote_follows_event_time_not_arrival_order` |
| Unconditional Redis write | `test_quote_cache_rejects_older_quotes_and_rebuilds_from_postgres` |
| Simulator IDs from the seeded RNG | `test_a_restarted_seeded_run_never_reuses_event_ids` |
| Metrics rate over a fixed interval | `test_metrics_rate_uses_time_since_last_successful_flush` |
| WebSocket origin check disabled | `test_websocket_origin_check` |
| Stale flag never set | `test_cache_status_flags_stale_and_missing_quotes` |
| Finnhub key in the query string / whole poll restarted on one bad quote | `test_rest_token_is_sent_as_header_never_in_url` (+4), `test_rest_bad_bodies_are_forwarded_as_bytes_and_later_symbols_still_arrive` |
| Spark: no dedup / VWAP as a mean / wider watermark | 3, 3 and 1 Spark tests respectively |
| WS unregister after the await | `test_websocket_connections_are_released_after_disconnect` failed 2/8 runs (fixed: 0/14) |

All 14 initial seeded faults and every audit-round fault above were caught. Live failure drills:
[failure-drills.md](failure-drills.md).
