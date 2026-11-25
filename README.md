# MarketPulse

[![ci](https://github.com/shresthrohan/MarketPulse/actions/workflows/ci.yml/badge.svg)](https://github.com/shresthrohan/MarketPulse/actions/workflows/ci.yml)

Real-time financial market data infrastructure: a provider-agnostic ingestion service streams market events into
Kafka, a stream processor validates, deduplicates and stores them in PostgreSQL/TimescaleDB, Redis serves the latest
state, and a FastAPI backend exposes REST + WebSocket APIs to an institutional-style React terminal with analytics,
data-quality monitoring and pipeline observability.

> **Data honesty.** Two kinds of data, never mixed and always labelled:
> - **Simulated** US equities/ETFs from a built-in exchange simulator (geometric Brownian motion with a market
>   factor). Ticker symbols are real; these prices are not market prices. Labelled SIMULATED / SIM.
> - **Live** crypto pairs (BTC, ETH, SOL, BNB, XRP vs USDT) from Binance's public market-data streams, with a
>   year of real history from Binance klines. No API key needed. Labelled LIVE.
>
> Every event carries `source` and `data_mode`; every instrument records its data source once
> (`instruments.data_mode`) and never mixes kinds; analytics refuse to combine simulated and live series.
> Optional: real US equities via Finnhub with your own key (replaces the simulator for equities).

```
provider ─► ingestion ─► Kafka market.raw ─► processing ─┬─► PostgreSQL/TimescaleDB (events, bars, errors, metrics)
                                                         ├─► Redis (latest quotes, heartbeats, tick pub/sub)
                                                         └─► Kafka market.validated / market.processed / market.errors
PostgreSQL + Redis + Kafka ─► FastAPI (REST /api, WS /ws/market) ─► nginx + React terminal
```

Details: [architecture](docs/architecture.md) · [pipeline](docs/pipeline.md) · [database](docs/database.md) ·
[API](docs/api.md) · [deployment](docs/deployment.md) · [benchmark](docs/benchmark.md) ·
[failure drills](docs/failure-drills.md) · [audit](docs/audit.md)

## Quick start

Requirements: Docker with Compose v2 (Docker Desktop 4.x or Docker Engine 24+), ~4 GB free RAM for the VM.

```bash
cp .env.example .env          # then change POSTGRES_PASSWORD
docker compose up -d --build  # first build takes a few minutes
```

Open **http://localhost:8080** (terminal) · http://localhost:8000/docs (OpenAPI).

On start-up `migrate` applies the Alembic migration and seeds 20 instruments plus one year of continuous simulated
history (daily bars, and 1-minute bars for the last 7 days) that ends exactly where the live simulator begins.
Live events start flowing within seconds.

```bash
docker compose ps                         # all long-running services Up; migrate / kafka-init Exited (0)
docker compose logs -f processing         # structured JSON logs
docker compose down                       # stop (data kept in volumes)
docker compose down -v                    # stop and delete all data
```

Data providers are chosen with `PROVIDERS` in `.env` (default `simulator,binance`; no keys needed). Binance
needs outbound internet and refuses US addresses (use the binance.us hosts, see `.env.example`). Real US equities:
`PROVIDERS=finnhub_ws,binance` (or `finnhub_rest`) plus `FINNHUB_API_KEY`, on a fresh database
([switching data sources](docs/deployment.md#switching-data-sources)). Keys stay server-side; the frontend never
sees them.

Production deployment (single host, Docker Compose, Caddy HTTPS, CI/CD gate): [docs/deployment.md](docs/deployment.md).

Optional Spark mode: `docker compose --profile spark up spark` (see [spark/README.md](spark/README.md)).

### Local development without Docker for the UI

```bash
cd frontend && npm install && npm run dev   # http://localhost:5173, proxies /api and /ws to localhost:8000
```

## Services

| Service | Image / code | Port | Notes |
|---|---|---|---|
| postgres | timescale/timescaledb:2.17.2-pg16 | internal | volume `pgdata` |
| redis | redis:7.4-alpine (AOF) | internal | volume `redisdata` |
| kafka | apache/kafka:3.8.1 (KRaft) | internal | 4 topics from `kafka-init` |
| migrate | `backend/` image | — | `alembic upgrade head` + `scripts/seed_data.py`, one-shot |
| ingestion | `ingestion/` | — | provider → `market.raw` |
| processing | `processing/` | — | validation, storage, derived topics, cache, metrics, retention |
| backend | `backend/` | 127.0.0.1:8000 | FastAPI |
| frontend | `frontend/` + nginx | 127.0.0.1:8080 | proxies `/api`, `/ws` |
| spark | apache/spark:3.5.3-python3 | — | profile `spark` only |

Configuration: every variable is listed and explained in [.env.example](.env.example).

## The terminal

| Page | What it shows (all from the API) |
|---|---|
| Overview | pipeline counters, index ETFs, majors, gainers/losers/most active, candlestick + volume chart with live 1-minute bars, recent events and live quote stream |
| Markets | server-side sorted/filtered/paginated screener with live prices and add-to-watchlist |
| Instrument | OHLCV chart (1D 1W 1M 3M 1Y), SMA/EMA overlays, statistics (52-week range, volatility, returns, max drawdown), latest stored events |
| Analytics | returns (simple/log), rolling volatility, moving averages, correlation matrix, drawdowns, volume trends, normalised comparison |
| Watchlists | create / rename / delete, add / remove instruments, live prices (persisted in PostgreSQL) |
| Data Explorer | filterable, paginated stored events with CSV export |
| Pipeline Monitor | component graph with real statuses, throughput / latency / error-rate / consumer-lag charts, data-quality breakdown, recent pipeline events |
| System Health | availability and latency of database, Redis, Kafka, WebSocket; API percentiles; process, host and per-service CPU/memory |
| Event Logs | stored and rejected events in one timeline: all / successful / failed / retried |

## Analytics methods

Implemented in `backend/app/services/analytics.py` (pandas/NumPy, unit-tested against hand-computed values),
using daily closes.

| Metric | Definition |
|---|---|
| Simple return | R(t) = (P(t) − P(t−1)) / P(t−1) |
| Log return | r(t) = ln(P(t) / P(t−1)) |
| SMA(n) | mean of the last n closes; undefined until n observations exist |
| EMA(n) | EMA(t) = α·P(t) + (1−α)·EMA(t−1), α = 2/(n+1), seeded with the first close; undefined for the first n−1 points |
| Rolling volatility | sample standard deviation (ddof = 1) of the last n log returns × √365 |
| Correlation | Pearson correlation of returns, pairwise over dates both instruments have; `null` below 10 overlapping observations |
| Drawdown | DD(t) = P(t) / max(P(0..t)) − 1; maximum drawdown = min DD, with peak, trough and recovery dates |
| Comparison | P(t) / P(0) × 100 |

Annualisation uses 365 periods because the simulated venue trades every calendar day; use 252 for exchange-calendar
data (`PERIODS_PER_YEAR`). Missing data is never filled: returns are computed on each instrument's own consecutive
observations, and cross-instrument statistics use only overlapping dates.

## Data quality

Every raw payload ends as exactly one of: stored event (`valid`, or `retried` if stored after a transient failure)
or recorded reject (`processing_errors` + `market.errors`). Rules: malformed JSON, missing fields, invalid fields,
null / negative / invalid price, invalid volume, invalid or out-of-window timestamp, OHLC relationship violations,
unknown symbol, duplicate event ID, database refusal. The simulator injects such faults at `SIM_FAULT_RATE` (0.5%)
so the quality layer and the error views always have real data. Details: [docs/pipeline.md](docs/pipeline.md).

Delivery model, stated precisely: Kafka → PostgreSQL is **at-least-once delivery with idempotent storage** (one row per `event_id`; bars derived only from newly inserted rows in the same transaction). Derived topics are at-least-once. Exactly-once processing is not claimed. Replays are recorded as `duplicate_event` warnings. Deduplication key: `event_id`. Order: per instrument (Kafka partition key = symbol).

## Testing

| Suite | Where it runs | Command |
|---|---|---|
| Ingestion unit (simulator, Finnhub adapters with mocked HTTP/WebSocket) | any Python 3.12 | `pytest ingestion/tests` |
| Processing unit (validation + fuzzing, enrichment, bars, processor failure injection with fakes) | any | `pytest processing/tests/test_pure.py processing/tests/test_processor.py` |
| Backend unit (analytics, screening, rate limiter, WS fan-out and origin policy, cache freshness) | any | `cd backend && pytest tests/unit` |
| Seed generator | any | `pytest scripts/test_seed.py` |
| Spark job (windows, VWAP, duplicates, invalid records, late events) | Spark image | `docker run --rm -v "$PWD/spark:/opt/marketpulse" -w /opt/marketpulse apache/spark:3.5.3-python3 sh run_tests.sh` |
| Backend integration (REST, WebSocket, DB, Redis, Kafka) | live stack | `docker compose exec backend pytest tests/integration` |
| Pipeline end-to-end (Kafka → processor → DB/Redis/DLQ, replay, reconciliation, UTC, cache rebuild) | live stack | `docker compose run --rm processing pytest processing/tests/test_integration.py` |
| Failure drills (Redis outage/data loss, processor SIGKILL, offset rewind, PostgreSQL/backend/Kafka restarts) | host + live stack | `python scripts/failure_drills.py` |
| Backup + restore check | host + live stack | `scripts/backup.sh && scripts/restore.sh backups/<dump>` |
| Production configuration (HTTPS, WSS, exposure, runtime images) | CI job `production-config` | see [deployment](docs/deployment.md#cicd) |
| Schema drift | live stack | `docker compose exec backend alembic check` |
| Frontend (Vitest) | Node 22 | `cd frontend && npm test` |

Unit suites without Docker: `pip install -r ingestion/requirements.txt -r processing/requirements.txt -r backend/requirements.txt`
then `PYTHONPATH=.:backend pytest ingestion/tests processing/tests/test_pure.py processing/tests/test_processor.py backend/tests/unit scripts/test_seed.py`.
CI (`.github/workflows/ci.yml`) runs the unit suites, lint, the Spark tests and the frontend checks, then the whole
stack with the integration suites, failure drills and a backup/restore check, and boots the production overlay
behind HTTPS. A deploy job exists but runs only when a server is configured (none is, by choice).

## Benchmark

```bash
docker compose run --rm -v ./docs:/srv/docs processing python /srv/scripts/benchmark.py --events 20000 --output /srv/docs/benchmark.md
```

Measures ingestion throughput, processing throughput, end-to-end latency, API response times and query plans on
the running stack. Benchmark events are real stored events labelled `source=benchmark`. Results: [docs/benchmark.md](docs/benchmark.md).

## Security

- Secrets only from environment (`.env`, git-ignored); no credentials in code or in the frontend. The Finnhub key is
  sent in a header (never in a URL) and redacted from every log line and status string (tested).
- PostgreSQL, Redis and Kafka are not published to the host; backend and frontend bind to 127.0.0.1 only.
- CORS restricted to `CORS_ORIGINS`; methods and headers allow-listed. WebSocket handshakes from foreign browser
  origins are rejected (CORS does not cover WebSockets).
- Writes require a JSON body, so a cross-site HTML form cannot create or change data (tested).
- Per-client rate limit (`RATE_LIMIT_PER_MINUTE`, default 1200) on `/api/*`, 429 with `Retry-After`.
- Input validation on every parameter and body (Pydantic, enumerated values, bounded lengths and page sizes);
  all SQL uses bound parameters; the Data Explorer only exposes fixed, paginated queries.
- Unhandled errors return a generic 500; details (with traceback) go to the structured log only.
- nginx adds `X-Content-Type-Options`, `X-Frame-Options: DENY` and `Referrer-Policy`.
- The Python service containers run as a non-root user.
- Audit findings by severity: [docs/audit.md](docs/audit.md).

## Known limitations

- **Single node.** One Kafka broker (replication factor 1), one PostgreSQL, one Redis: no high availability.
- **No authentication** (local single-user application, as specified). Redis and Kafka are unauthenticated on the
  internal Docker network; health endpoints include internal host names in error details.
- **Simulated equities.** The simulated venue trades 24/7 with UTC-day sessions; day change is versus the UTC-day open.
- **Binance data has gaps across restarts.** Seconds traded while ingestion is down are not backfilled (history
  is backfilled only for pairs with none). binance.com refuses US addresses (configurable hosts).
- **Not deployed publicly.** The production configuration is verified in CI and locally; no server is provisioned.
- **At-least-once derived topics.** `market.validated/processed/errors` can contain duplicates after a retry or replay.
- **Replays are visible.** A replayed event is logged again (`duplicate_event`, or its original error type if it was
  invalid), so error counts include redeliveries.
- **No per-symbol sequence numbers.** Lost events are detected by count/offset reconciliation (producer
  `delivery_failed`, `raw_end_offsets = raw_committed_offsets + lag`), not by gap detection.
- **Rolling windows follow processing order**, not event time; bars and quotes use event time.
- **Latency figures** come from one Docker VM with a shared clock; `processed_at` is the storing transaction's start.
- **Rate limiting is per process and per peer address**; behind the bundled nginx all browser traffic shares one key.
- **Screener filtering/sorting runs in the API process** over all instruments (fine for hundreds, not tens of thousands).
- **Consumer-lag history** is the processor's own gauge (from cached watermarks); the live lag shown on the monitor
  is read from the broker.
- **Finnhub adapters** are tested with mocked HTTP/WebSocket responses, never against the live service (no key in CI).
  A REST symbol that keeps failing delays the symbols after it in the same poll.
- **Spark mode**: transformations and watermark behaviour are tested; the Kafka source was only verified manually.
- **Failure drills** need the docker CLI on the host and briefly disrupt the running stack. For about two minutes after
  a broker or database restart, isolated 1–2 s batch stalls were observed before latency returned to normal.

## Test results

Final verification on 2026-11-25 (Windows 11, Docker Desktop, WSL2 VM with 22 vCPU / 7.5 GB), with the simulator
and the live Binance feed running (`PROVIDERS=simulator,binance`):

| Suite | Result |
|---|---|
| `alembic check` (migrations 0001 + 0002, upgrade of a populated database, downgrade/upgrade round trip) | no drift |
| Backend unit + integration (`tests/`) | 47 passed |
| Processing unit + end-to-end (`processing/tests`) | 50 passed |
| Ingestion unit (simulator, Binance and Finnhub with mocked sockets/HTTP, provider configuration) | 65 passed |
| Seed generator + Binance kline mapping | 8 passed |
| Spark job | 9 passed |
| Frontend `tsc --noEmit` / Vitest / `vite build` | clean / 37 passed / built |
| Ruff (E, F, W, B, UP) on all Python | clean |
| Failure drills (with 20 simulated + 5 live instruments) | 7/7 passed |
| Seeded-fault checks | every seeded fault caught ([table](docs/architecture.md#verification)) |
| Production configuration from a clean clone (HTTPS, WSS, exposure, persistence, crash recovery, backup/restore, replay, offline UI) | all passed ([details](docs/deployment.md#verified)) |

CI runs the same suites on every push (badge above).

### Benchmark summary

From [docs/benchmark.md](docs/benchmark.md): one run on the final build, settled stack, simulator and Binance feed
running; the report records the machine, the conditions and every method.

| Metric | Measured |
|---|---|
| Ingestion throughput (producer → Kafka, acks=all), 20,000-event burst | 67,956 events/s |
| Processing throughput (Kafka → validated → PostgreSQL + Redis + derived topics) | 5,920 events/s wall-clock (6,437 over the processed_at span) |
| Event accounting | 20,000 produced = 20,000 stored, 0 rejected, 0 retried |
| Steady-state end-to-end latency (live traffic, 27 events/s, 1,603 events) | p50 33 ms · p95 56 ms · p99 70 ms · max 75 ms |
| API p50 / p95 per endpoint | 5–31 ms / 6–62 ms, 0 errors |
| Key queries (EXPLAIN ANALYZE) | 0.1–1.9 ms, index / chunk scans |

The burst also reports backlog-drain latency (every event of the burst queued at once), a different quantity from the
steady-state latency above. Figures depend on the build and host load and are not comparable across runs taken under
different conditions.
