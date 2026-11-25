# MarketPulse API

Base URL: `http://localhost:8000` (direct) or `http://localhost:8080` (through the nginx frontend).
Interactive OpenAPI docs: `/docs`.

Conventions:

- Timestamps in JSON bodies are ISO-8601 UTC strings. Chart points use `time` = Unix seconds (int).
- Errors: `{"detail": "..."}` with 4xx/5xx status. 422 = invalid parameters. 429 = rate limited (`Retry-After` header).
- `Point` = `{"time": int, "value": float}`. NaN/inf are never sent; missing values are omitted from series or `null` in scalars.
- Paginated lists: `page` (1-based), `page_size`; responses include `page`, `page_size` and either `total` or `has_more`.
- Symbols are case-insensitive in paths/queries and returned upper-case.

## Objects

`Quote` (latest snapshot from Redis, written by the processor after each committed batch):

```json
{"type": "quote", "symbol": "AAPL", "price": 225.5, "open": 224.0, "high": 226.0, "low": 223.5, "volume": 1834200,
 "change": 1.5, "change_pct": 0.6696, "tick_change_pct": 0.0133, "rolling_avg": 225.41,
 "rolling_volume": 18342, "rolling_window": 20, "event_id": "uuid",
 "event_timestamp": "2026-11-25T14:30:00.500+00:00", "event_ts_ms": 1795617000500,
 "processed_at": "2026-11-25T14:30:00.540+00:00", "source": "simulator", "data_mode": "simulated", "origin": "stream"}
```

`open/high/low/volume` are the current UTC-day bar from PostgreSQL (`historical_aggregates`, interval `1d`).
`change` / `change_pct` are versus that day's open. `tick_change_pct` is versus the previous processed price. `origin` is `stream` (written by the processor for a new
event) or `rebuild` (restored from PostgreSQL after Redis lost data; rolling fields are then `null`). The cached quote
only moves forward in `event_ts_ms`, so a late event never replaces a newer quote. All timestamps are UTC.

`InstrumentRow`:

```json
{"id": 1, "symbol": "AAPL", "company_name": "Apple Inc.", "exchange": "NASDAQ", "asset_type": "equity",
 "currency": "USD", "data_mode": "simulated", "quote": Quote | null}
```

`asset_type` is `equity`, `etf` or `crypto`. `data_mode` is where the instrument's data comes from (`simulated`,
`live`, `delayed`; `null` until established) and never changes. `quote` is `null` until the pipeline has processed an
event for the instrument. Volumes are numbers and may be fractional (crypto base-asset quantities).

## Instruments

| Method | Path | Query | Response |
|---|---|---|---|
| GET | `/api/instruments` | `search` (symbol/company substring), `asset_type` (`equity`\|`etf`), `exchange`, `sort` (`symbol`\|`company_name`\|`price`\|`change`\|`change_pct`\|`volume`\|`updated`), `order` (`asc`\|`desc`), `page`, `page_size` (≤100, default 25) | `{"items": InstrumentRow[], "page", "page_size", "total"}` |
| GET | `/api/instruments/{symbol}` | | `InstrumentRow` (404 if unknown) |
| GET | `/api/instruments/{symbol}/history` | `range` = `1D`\|`1W`\|`1M`\|`3M`\|`1Y` (default `1D`) | `{"symbol", "range", "interval", "bars": [{"time", "open", "high", "low", "close", "volume"}]}` |
| GET | `/api/instruments/{symbol}/statistics` | | see below |

History ranges: `1D` = 1-minute bars for the last 24h (`interval: "1m"`); `1W` = 1-minute bars bucketed to 15 minutes (`"15m"`); `1M`/`3M`/`1Y` = daily bars (`"1d"`) for the last 30/91/365 days.

Statistics response (from daily closes over the last 365 days plus the live quote):

```json
{"symbol": "AAPL", "as_of": "iso", "last_price": 225.5, "observations": 365,
 "high_52w": 240.1, "low_52w": 180.2, "avg_volume_30d": 2.1e6, "volatility_30d": 0.27,
 "returns": {"1W": 0.012, "1M": -0.03, "3M": 0.05, "1Y": 0.11},
 "max_drawdown_1y": {"value": -0.18, "peak_at": "iso", "trough_at": "iso", "recovered_at": "iso" | null} | null,
 "sma_20": 223.1, "sma_50": 221.0, "ema_20": 223.4}
```

Any field may be `null` when history is too short.

## Market

| Method | Path | Query | Response |
|---|---|---|---|
| GET | `/api/market/overview` | | see below |
| GET | `/api/market/gainers` | `limit` (1–50, default 10) | `{"items": InstrumentRow[]}` by `change_pct` desc |
| GET | `/api/market/losers` | `limit` | `{"items": InstrumentRow[]}` by `change_pct` asc |
| GET | `/api/market/active` | `limit` | `{"items": InstrumentRow[]}` by day `volume` desc |
| GET | `/api/market/history` | `symbol`, `source`, `status` (`valid`\|`retried`), `start`, `end` (ISO), `min_price`, `max_price`, `search` (event_id prefix), `page`, `page_size` (≤200, default 50), `format` (`json`\|`csv`) | events page or CSV download |

Overview:

```json
{"data_modes": ["simulated"], "sources": ["simulator"], "last_update": "iso" | null,
 "instruments_tracked": 20, "quotes_available": 20,
 "indices": InstrumentRow[], "majors": InstrumentRow[],
 "gainers": InstrumentRow[], "losers": InstrumentRow[], "active": InstrumentRow[],
 "breadth": {"advancers": 12, "decliners": 7, "unchanged": 1},
 "cache": {"available": true, "quotes": 20, "last_update": "iso", "age_s": 0.4, "stale": false}}
```

`cache` describes the quote cache: `available` = Redis reachable, `age_s` = seconds since the newest processed quote,
`stale` = no quote or older than 60 s (quotes are never expired, only labelled). `indices` = ETF instruments (index trackers). `majors` = all equities. `gainers`/`losers`/`active` = top 5.

Market history (Data Explorer) item:

```json
{"event_id": "uuid", "symbol": "AAPL", "event_timestamp": "iso", "price": 225.5, "volume": 1200,
 "open": 224.0, "high": 226.0, "low": 223.5, "close": 225.5, "source": "simulator",
 "data_mode": "simulated", "processing_status": "valid", "attempts": 1,
 "ingested_at": "iso" | null, "processed_at": "iso", "latency_ms": 41.2 | null}
```

JSON response: `{"items": [...], "page", "page_size", "has_more"}`, newest first. CSV: same columns, `Content-Disposition: attachment`, capped at 50,000 rows.

## Analytics

All take `symbols` (comma-separated, 1–10) except moving averages (`symbol`). Combining simulated and live
instruments in one request is refused with 422 (the statistics would be meaningless). They also take and `range` = `1M`\|`3M`\|`6M`\|`1Y` (default `3M`). They use daily closes (interval `1d`). Methods: see `app/services/analytics.py` and the README "Analytics methods" section.

| Path | Extra query | Response |
|---|---|---|
| `/api/analytics/returns` | `method` = `simple`\|`log` | `{"method", "range", "series": {SYM: Point[]}, "summary": {SYM: {"total_return", "mean_return", "observations"}}}` |
| `/api/analytics/volatility` | `window` (5–120, default 20) | `{"window", "annualization": 365, "series": {SYM: Point[]}, "latest": {SYM: float\|null}, "full_period": {SYM: float\|null}}` |
| `/api/analytics/correlation` | `method` | `{"symbols": [..], "method", "matrix": (float\|null)[][], "observations": int[][]}` |
| `/api/analytics/drawdown` | | `{"series": {SYM: Point[]}, "max_drawdown": {SYM: {"value", "peak_at", "trough_at", "recovered_at"} \| null}}` |
| `/api/analytics/comparison` | | `{"base": 100, "series": {SYM: Point[]}, "total_return": {SYM: float\|null}}` |
| `/api/analytics/moving-averages` | `symbol`, `sma` (2–200, default 20), `ema` (2–200, default 50) | `{"symbol", "sma_window", "ema_span", "close": Point[], "sma": Point[], "ema": Point[]}` |
| `/api/analytics/volume` | `window` (default 20) | `{"window", "series": {SYM: Point[]}, "average": {SYM: Point[]}}` |

## Watchlists

| Method | Path | Body | Response |
|---|---|---|---|
| GET | `/api/watchlists` | | `{"items": Watchlist[]}` |
| POST | `/api/watchlists` | `{"name": str (1–64)}` | 201 `Watchlist`; 409 name exists |
| PUT | `/api/watchlists/{id}` | `{"name"}` | `Watchlist`; 404; 409 |
| DELETE | `/api/watchlists/{id}` | | 204; 404 |
| POST | `/api/watchlists/{id}/instruments` | `{"symbol"}` | 201 `Watchlist`; 404 unknown watchlist/symbol; 409 already present |
| DELETE | `/api/watchlists/{id}/instruments/{symbol}` | | 204; 404 |

`Watchlist` = `{"id", "name", "created_at", "instruments": InstrumentRow[]}`.

## Pipeline monitoring

| Path | Query | Response |
|---|---|---|
| `/api/pipeline/overview` | | see below |
| `/api/pipeline/services` | | `{"checked_at", "items": Service[]}` |
| `/api/pipeline/metrics` | `minutes` (5–1440, default 60), `bucket_s` (10–3600, default 60) | `{"bucket_s", "series": {"events_per_second", "avg_latency_ms", "error_rate", "consumer_lag", "db_write_ms": Point[]}}` |
| `/api/pipeline/throughput` | `minutes`, `bucket_s` | `{"bucket_s", "received": Point[], "processed": Point[], "failed": Point[]}` (events/second per bucket) |
| `/api/pipeline/errors` | `error_type`, `symbol`, `search` (event_id/message substring), `page`, `page_size` | `{"items": [{"id", "event_id", "symbol", "error_type", "error_message", "severity", "source", "raw_payload", "timestamp"}], "page", "page_size", "has_more"}` |
| `/api/pipeline/events` | `view` = `all`\|`success`\|`failed`\|`retried`, `search`, `symbol`, `severity`, `page`, `page_size` | `{"items": [{"timestamp", "event_id", "source", "symbol", "event_type", "severity", "processing_status", "error_details"}], "page", "page_size", "has_more"}` |

Overview (24h window, sums of processor metrics; live values where noted):

```json
{"window_hours": 24, "instruments_tracked": 20,
 "events_received": 1000, "events_processed": 990, "events_failed": 6, "events_duplicate": 4, "events_retried": 0,
 "events_per_second": 23.9, "avg_latency_ms": 38.2, "max_latency_ms": 120.0, "db_write_ms": 6.1,
 "consumer_lag": 0, "last_successful_event_at": "iso" | null,
 "data_quality": {"total": 1000, "valid": 990, "invalid": 6, "duplicate": 4, "rejected": 10, "valid_pct": 99.0},
 "error_breakdown": [{"error_type": "negative_price", "count": 2}]}
```

`events_per_second` = processed events over the last 60 s; `avg_latency_ms` / `db_write_ms` = last 5 minutes;
`consumer_lag` = live from Kafka (committed offset vs. high watermark), `null` if Kafka is unreachable.

`Service`:

```json
{"key": "ingestion" | "kafka" | "processor" | "postgres" | "redis" | "api" | "websocket",
 "name": "Data ingestion", "status": "up" | "degraded" | "down", "latency_ms": 1.2 | null,
 "detail": "human readable", "metrics": {"name": number | string | null}, "last_seen": "iso" | null}
```

Statuses are computed on every request from real checks (heartbeat freshness in Redis, `SELECT 1`, `PING`, Kafka metadata). Nothing is hard-coded.

Event log rows: `event_type` is `market.tick` for stored events and `error.<error_type>` for rejected ones; `severity` is `info` (stored), `warning` (duplicates) or `error`; `processing_status` is `valid`, `retried` or `rejected`.

## Health

| Path | Response (HTTP 503 when the component is down, except `/api/health` which always answers 200 with a status field) |
|---|---|
| `/api/health` | `{"status": "ok"\|"degraded"\|"down", "version", "uptime_s", "components": {"database", "redis", "kafka", "ingestion", "processor", "websocket": {"status", "latency_ms", "detail"}}}` |
| `/api/health/database` | `{"status", "latency_ms", "detail", "pool": {"size", "checked_out", "overflow"}}` |
| `/api/health/redis` | `{"status", "latency_ms", "detail", "used_memory_bytes", "connected_clients"}` |
| `/api/health/kafka` | `{"status", "latency_ms", "detail", "brokers", "topics": {name: partitions}, "missing_topics": [], "consumer_lag", "raw_end_offsets", "raw_committed_offsets"}` — reconciliation on `market.raw`: end offsets = committed (processed) + lag (pending) |
| `/api/health/system` | `{"started_at", "uptime_s", "process": {"cpu_percent", "rss_bytes", "threads"}, "host": {"cpu_percent", "cpu_count", "memory_total_bytes", "memory_used_bytes", "memory_percent", "load_avg"}, "api": {"requests", "p50_ms", "p95_ms", "p99_ms", "routes": [{"route", "count", "p50_ms", "p95_ms"}]}, "websocket": {"connections", "messages_sent", "dropped_clients", "listener_connected"}, "services": [{"key", "host", "status", "cpu_percent", "rss_bytes", "uptime_s", "heartbeat_at"}]}` |

`host` figures are what the backend container can see (cgroup-limited Docker VM, not necessarily the physical machine).
API latency percentiles cover the last 2,000 requests of this backend process.

## WebSocket `/ws/market`

Client → server:

```json
{"action": "subscribe", "symbols": ["AAPL", "MSFT"]}
{"action": "subscribe", "symbols": ["*"]}
{"action": "unsubscribe", "symbols": ["AAPL"]}
```

Server → client:

```json
{"type": "subscribed", "symbols": ["AAPL"]}
{"type": "quote", ...Quote}
{"type": "bar", "symbol": "AAPL", "interval": "1m", "time": 1764081000, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 10}
{"type": "error", "message": "..."}
```

Quotes are conflated per processed batch (at most one per symbol per batch). `bar` messages carry the full
current state of the 1-minute bar after the batch, so `series.update(bar)` is correct without client-side aggregation.
The server closes with code 1013 when at capacity or when a client cannot keep up; clients should reconnect with backoff.

Origin policy: WebSockets are not covered by CORS, so the handshake is rejected (close 1008, HTTP 403) when a browser
sends an `Origin` that is neither in `CORS_ORIGINS` nor the same origin as the request. Clients that send no `Origin`
(scripts, tests) are accepted. The stream is read-only market data; there is no authentication by design.
