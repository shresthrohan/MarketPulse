# Database design

PostgreSQL 16 with the TimescaleDB 2.17 extension (`timescale/timescaledb:2.17.2-pg16`) is the source of truth.
Redis holds only derived, re-creatable state (latest quotes, heartbeats, the tick channel).

Schema of record: `backend/alembic/versions/` (`0001_initial`, `0002_multi_source`: crypto asset type,
`instruments.data_mode`, fractional volume). ORM models: `backend/app/models/tables.py`.
`alembic check` (run in CI) fails if the two drift apart.

## Tables

### `instruments`

| Column | Type | Notes |
|---|---|---|
| id | serial PK | |
| symbol | varchar(10) | `UNIQUE`, `CHECK symbol ~ '^[A-Z][A-Z0-9.\-]{0,9}$'` |
| company_name | varchar(200) | |
| exchange | varchar(32) | |
| asset_type | varchar(16) | `CHECK IN ('equity','etf','crypto')` |
| data_mode | varchar(16) | where its data comes from: `simulated` / `live` / `delayed`; set once by the seed (`COALESCE`), never flipped; the processor rejects events of the other kind |
| currency | varchar(8) | `USD`, `USDT`, ... |
| created_at | timestamptz | default `now()` |

Seeded from `config/instruments.json` (simulated or Finnhub equities/ETFs) and, when `binance` is enabled,
`config/binance.json` (crypto pairs) by `scripts/seed_data.py` (upsert, safe on every start). Crypto history is
real (Binance public klines: 1d for a year, 1m for 7 days); equities get generated history only when the
simulator feeds them.

### `market_events` — every stored (valid) event

| Column | Type | Notes |
|---|---|---|
| id | bigserial PK | |
| event_id | uuid | `UNIQUE` — the deduplication point |
| instrument_id | int FK → instruments (RESTRICT) | |
| event_timestamp | timestamptz | event time, normalised to UTC |
| price, open, high, low, close | numeric(18,6) | `CHECK price > 0`, `CHECK low <= high AND open/close/low > 0` |
| volume | numeric(28,8) | `CHECK volume >= 0`; fractional for crypto |
| source | varchar(64) | provider name (`simulator`, `binance`, `finnhub`, `benchmark`, ...) |
| data_mode | varchar(16) | `CHECK IN ('simulated','live','delayed')` |
| processing_status | varchar(16) | `CHECK IN ('valid','retried')` — `retried` = stored after ≥1 transient failure |
| attempts | smallint | `CHECK attempts >= 1` |
| ingested_at | timestamptz | stamped by the ingestion producer |
| processed_at | timestamptz | default `now()` (start of the storing transaction) |

Indexes: `uq_market_events_event_id`, `(instrument_id, event_timestamp)`, `(event_timestamp)`,
`(processing_status, event_timestamp)`. The composite index also serves lookups by `instrument_id` alone,
so there is no separate single-column index.

Not a hypertable on purpose: TimescaleDB requires every unique index on a hypertable to include the time
column, which would weaken deduplication from `event_id` to `(event_id, event_timestamp)`.

Rejected events are **not** stored here (they may lack a price or a known instrument); they go to `processing_errors`.

### `historical_aggregates` — OHLCV bars (TimescaleDB hypertable)

| Column | Type | Notes |
|---|---|---|
| id | bigserial | part of the PK |
| instrument_id | int FK → instruments | |
| interval | varchar(4) | `CHECK IN ('1m','1d')` |
| timestamp | timestamptz | bucket start (UTC) |
| open, high, low, close | numeric(18,6) | `CHECK low <= high AND low > 0` |
| volume | numeric(28,8) | `CHECK volume >= 0`; fractional for crypto |
| first_event_at, last_event_at | timestamptz | earliest/latest event merged into the bar |

PK `(id, timestamp)` and `UNIQUE (instrument_id, interval, timestamp)` both include the time column as the
hypertable requires. Chunk interval 30 days. Created by the migration only when the extension is available,
so plain PostgreSQL works unchanged.

Bars are maintained by the processor in the same transaction as the events that change them
(`processing/repository.py: UPSERT_BARS`): `open` only moves to an earlier event, `close` only to a later one,
`high/low` are `GREATEST/LEAST`, `volume` is additive — and only for events that were newly inserted, so a replayed
batch never double-counts. Coarser views (15-minute buckets for the 1W chart) are computed at query time with
`date_bin`.

### `watchlists` / `watchlist_instruments`

`watchlists(id, name UNIQUE CHECK char_length(btrim(name)) BETWEEN 1 AND 64, created_at)`;
`watchlist_instruments(id, watchlist_id FK CASCADE, instrument_id FK CASCADE, added_at, UNIQUE(watchlist_id, instrument_id))`.
The unique constraints (not application code) decide races between concurrent requests; the API maps the
violation to HTTP 409.

### `pipeline_metrics`

`(id, service_name, metric_name, metric_value double precision, timestamp)`; indexes
`(service_name, metric_name, timestamp)` and `(timestamp)`. The processor writes one row per metric every
`METRICS_INTERVAL_S` (5 s). Counters (`events_received`, `events_valid`, `events_invalid`, `events_duplicate`,
`events_retried`, `events_db_rejected`, `batches`, ...) are per-interval deltas, so a window total is a `SUM`;
gauges (`avg_latency_ms`, `max_latency_ms`, `db_write_ms`, `consumer_lag`, `events_per_second`) are averaged.

### `processing_errors`

`(id, event_id, symbol, error_type, error_message, severity, source, raw_payload, timestamp)`.
`error_type` is `CHECK`ed against the closed set in `processing/validators/event_validator.py: ErrorType`;
`severity` is `warning` (duplicates) or `error`. Indexes `(timestamp)`, `(error_type, timestamp)`, `(event_id)`.
`raw_payload` keeps the first 2 KB of the original message for investigation.

## Time handling

Every timestamp column is `timestamptz`. The validator rejects timestamps without a UTC offset and normalises the
rest to UTC; the API serialises timestamps as ISO-8601 with `+00:00` and chart times as Unix seconds. Bars and
sessions use UTC days. Covered by `test_timestamps_are_stored_and_returned_in_utc` and `test_api_timestamps_are_utc`.

## Cache rebuild query

`processing/repository.py: LATEST_QUOTES` returns, per instrument, the latest stored event (lateral lookup on
`(instrument_id, event_timestamp)`) and that event's UTC-day bar. The processor uses it to restore quotes Redis has lost.

## Query patterns

| Use | Query | Index |
|---|---|---|
| 1D chart | 1m bars for one instrument, last 24 h | `uq_historical_aggregates_instrument_id_interval_timestamp` + chunk exclusion |
| 1W chart | same, `date_bin('15 min')` | same |
| 1M–1Y chart, analytics | 1d bars for ≤10 instruments | same |
| Data Explorer | events for an instrument, newest first, `LIMIT/OFFSET` | `(instrument_id, event_timestamp)` |
| Event log | events ∪ errors, each branch ordered + limited on its own index before the merge | `(event_timestamp)`, `(timestamp)` |
| Last successful event | newest row by PK (`ORDER BY id DESC LIMIT 1`) | PK |

Measured plans and timings: `docs/benchmark.md`.

## Retention

The processor runs a retention pass every `RETENTION_INTERVAL_S` (default 1 h), deleting in batches of 10,000
(at most 100 batches per table per pass) so locks stay short:

| Data | Default | Variable |
|---|---|---|
| `market_events` | 30 days | `EVENT_RETENTION_DAYS` |
| `processing_errors` | 30 days | `ERROR_RETENTION_DAYS` |
| `pipeline_metrics` | 7 days | `METRIC_RETENTION_DAYS` |
| 1m bars | 90 days | `BAR_1M_RETENTION_DAYS` |
| 1d bars | kept | — |

Kafka topics keep 24 h (`market.errors` 7 days), well inside the event retention, so a replay can never
re-insert an event that retention already deleted.

## Connections and transactions

- API: SQLAlchemy async engine over asyncpg, pool 10 + 10 overflow, `pool_pre_ping`, 15 s server-side
  `statement_timeout`. Repositories (`backend/app/repositories`) hold all SQL; routes never touch the database.
- Processor: one psycopg 3 connection, `autocommit` with explicit `conn.transaction()` blocks; one transaction per
  batch covers events + bars + error rows.
- All values are bound parameters. The only interpolated identifiers are the fixed retention table names in
  `processing/main.py`.
