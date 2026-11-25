# Post-implementation technical audit

Date: 2026-11-25. Scope: correctness, reliability, fault tolerance, security and documentation of the existing
system. No features were added and the frontend was not redesigned. Every fix below is backed by a test or a
recorded verification. "Seeded fault" means the fix was reverted on purpose, a test was confirmed to fail, and the
fix was restored.

## Findings

### High

| # | Finding | Impact | Fix | Evidence |
|---|---|---|---|---|
| H1 | The validator raised on some malformed payloads: a non-string `data_mode` (`TypeError: unhashable`), integers beyond float range (`OverflowError`), timestamps with no UTC equivalent such as `0001-01-01T00:00:00+05:00` (`OverflowError`). Reproduced all three. | The processor crashed before committing offsets, restarted, re-read the same message and crashed again. One bad message blocked its partition indefinitely. | Validator made total (each case returns a typed `Rejected`), plus a per-message supervision boundary in `StreamProcessor._classify`: an unexpected exception rejects only that message (logged, counted as `validator_crashes`). | Fuzz test (4,000 mutated events + 1,000 random byte strings) `test_validator_never_raises_on_arbitrary_input`; `test_an_unexpected_validator_exception_rejects_only_that_message`. Seeded faults caught. |
| H2 | With `SIM_SEED` set, simulator event IDs came from the seeded RNG. Reproduced: after a restart, 603/603 IDs repeated. | After any restart every new event was rejected as `duplicate_event`, and nothing was stored. | IDs are now `UUIDv5(run, symbol, timestamp, sequence)`. A seed still reproduces a run exactly; a restart (later timestamps) never reuses IDs. Unseeded runs add a random nonce. | `test_a_restarted_seeded_run_never_reuses_event_ids`, `test_unseeded_instances_never_share_event_ids`; seeded fault caught. |
| H3 | The Finnhub REST adapter sent the API key as a URL query parameter. httpx error messages and request logs include the URL. | The secret would be written to container logs on any HTTP error. | Key moved to the `X-Finnhub-Token` header; key redacted from all logged text and status strings (the WebSocket URL must carry it); the websockets logger is pinned above DEBUG because it logs the request path. | `test_rest_token_is_sent_as_header_never_in_url` and `caplog` assertions in every failure test; seeded fault made 5 tests fail. |

### Medium

| # | Finding | Fix | Evidence |
|---|---|---|---|
| M1 | Steady-state latency tail. The processor computed consumer lag with 6 blocking broker queries inside the consume loop, which stalled it for about 1 s every 5 s. Measured per-event p95 1,063 ms (p50 42 ms); the slow batches were exactly 5 s apart. | In-loop lag uses the high watermark cached from fetch responses. The API still reads authoritative lag from the broker on its own thread. | After the fix, on a settled stack: p50 30 ms, p95 54 ms, p99 65 ms, max 67 ms ([benchmark.md](benchmark.md)). |
| M2 | A late (out-of-order) event could replace a newer Redis quote: the quote was the last event *by arrival*, written with an unconditional `HSET`. | The latest event per symbol is chosen by event time within a batch, and Redis writes go through an atomic Lua set-if-newer on `event_ts_ms`. | `test_quote_follows_event_time_not_arrival_order`, `test_quote_cache_rejects_older_quotes_and_rebuilds_from_postgres`; seeded faults caught. |
| M3 | No cache rebuild. After Redis lost data, a quote came back only when that symbol traded again (never, for a closed live market), and an ingestion restart fell back to configured base prices, causing a price jump. | The processor rebuilds missing quotes from PostgreSQL (latest stored event plus its UTC-day bar) at start-up and every metrics interval, through the same conditional write. | Integration test above; drill `redis_data_loss`: FLUSHALL with ingestion stopped gave 20/20 quotes back, each equal to the latest stored event, and the simulator resumed within 0.002 % of the last price. |
| M4 | No freshness signal. A stopped pipeline left old quotes looking current. | `GET /api/market/overview` returns `cache: {available, quotes, last_update, age_s, stale}`. Quotes carry `event_ts_ms` and `origin` (`stream` or `rebuild`). Quotes are never expired, only labelled. | `test_cache_status_flags_stale_and_missing_quotes`, `test_overview_reports_cache_freshness`; seeded fault caught. |
| M5 | WebSocket cleanup could leak a connection slot: the handler unregistered the client *after* an `await` in `finally`, so a cancellation there skipped it. Leaked slots count toward the 200-connection cap. | Unregister happens first, before any await. | A/B under identical conditions: old ordering failed `test_websocket_connections_are_released_after_disconnect` in 2/8 runs, new ordering in 0/14. |
| M6 | Finnhub adapter robustness: one malformed WebSocket frame tore down the connection; one malformed REST quote restarted the whole poll; 401 was retried at full speed; `Retry-After` was ignored; a clean server close reconnected with no backoff. | Per-frame and per-symbol handling: malformed data is forwarded as bytes so the processor records it. 401/403 → `unauthorized` and 60 s backoff; 429 → `Retry-After` clamped to [1, 60] s; jittered backoff on every reconnect. | `ingestion/tests/test_finnhub.py`: 20 mocked cases, no network or key needed. |
| M7 | The Spark job double-counted republished events in VWAP (`market.processed` is at-least-once) and kept records with null volume or non-positive price. | `parse_events` filters invalid records, and `build_bars` deduplicates by `event_id` within the watermark (`dropDuplicatesWithinWatermark`). | `spark/tests`: 9 tests, including a streaming lateness test. Seeded faults (no dedup, VWAP as a mean, wider watermark) were all caught. |

### Low

| # | Finding | Fix | Evidence |
|---|---|---|---|
| L1 | The WebSocket endpoint accepted any `Origin` (CORS does not apply to WebSockets), so any website could read the live stream from a local browser. Read-only public data. | Handshake rejected (1008/HTTP 403) unless Origin is absent (non-browser), configured in `CORS_ORIGINS`, or same-origin. | `test_websocket_origin_check`, `test_websocket_rejects_foreign_origins_and_accepts_the_app_origin`; seeded fault caught. |
| L2 | events/s was overstated after a failed metrics flush (carried-over counts divided by one interval). | Rate measured from the last successful flush. | `test_metrics_rate_uses_time_since_last_successful_flush`; seeded fault caught. |
| L3 | nginx sent no security headers. | `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer` on every location. | Verified with `curl -I`. |
| L4 | Default `CORS_ORIGINS` lacked the 127.0.0.1 forms of the app URLs. | Added. | Config. |
| L5 | Offset reconciliation was not exposed. | `/api/health/kafka` adds `raw_end_offsets` and `raw_committed_offsets` (end = committed + lag). | `test_kafka_offsets_reconcile`. |
| L6 | README compared benchmark runs taken under different host load as if they were equivalent. | Removed. Each benchmark report records its own environment and run conditions. | README. |

Checked and found sound (no change): bound SQL parameters everywhere (LIKE input sanitised; the only interpolated
identifiers are fixed retention table names); JSON bodies required for writes (a cross-site `text/plain` form post is
rejected, `test_cross_site_form_posts_cannot_create_watchlists`); generic 500s without internals; secrets only from
`.env` (git-ignored); PostgreSQL, Redis and Kafka not published to the host; backend and UI bound to 127.0.0.1;
non-root Python containers; idempotent Kafka producers; manual offset commits after the DB commit.

## Documentation discrepancies found and corrected

- `pipeline.md` said "the next batch repairs the cache". That was true only for symbols that trade again; the rebuild is now documented.
- `architecture.md` failure table did not cover Redis data loss, and its I9 deadline row did not mention the in-loop lag query that turned out to stall processing.
- `api.md`: the Quote object lacked `event_ts_ms`/`origin`; the overview lacked `cache`; correlation responses include `method`; `/api/health/kafka` gained the reconciliation fields; the WebSocket origin policy was undocumented.
- README presented burst (backlog-drain) latency as the headline latency figure, with no steady-state latency measured; both are now reported separately.
- Test counts and the seeded-fault table were updated.

## Tests added

| Suite | Before | After | New coverage |
|---|---|---|---|
| Ingestion | 11 | 33 | Finnhub adapters (mocked HTTP/WS: credentials, rate limits, timeouts, malformed data, reconnect, key redaction), seeded-restart IDs |
| Processing unit | 26 | 39 | Validator fuzzing; processor failure injection with fakes (transient retry → `retried`, exhausted retries leave offsets uncommitted, Kafka flush failure rolls back, poison isolation, Redis outage, offset-commit failure + replay, rolling state after rollback, metric reconciliation, quote ordering) |
| Processing integration | 4 | 7 | Event-level reconciliation, UTC storage, Redis conditional writes + rebuild from PostgreSQL |
| Backend | 36 | 44 | WS origin policy, WS slot release, cross-site form post, UTC serialisation, cache freshness, Kafka offset reconciliation |
| Spark | 0 | 9 | Windows, VWAP vs. hand calculation, empty input, duplicates, invalid records, late events (streaming) |
| Seed / frontend | 6 / 37 | 6 / 37 | unchanged |
| Failure drills (`scripts/failure_drills.py`) | 0 | 7 | Redis outage, Redis data loss, processor SIGKILL, offset rewind replay, PostgreSQL restart, backend recreate, Kafka restart |

## Delivery model (as implemented, stated precisely)

- **Kafka → PostgreSQL:** at-least-once delivery with idempotent storage. `UNIQUE(event_id)` + `ON CONFLICT DO
  NOTHING`, and bars derived only from newly inserted rows, all in one transaction. The effect on stored data is
  exactly once per `event_id`. This is not exactly-once *processing*: a replayed event is processed again and
  recorded as a `duplicate_event` warning.
- **Derived topics** (`market.validated`, `market.processed`, `market.errors`): at-least-once; duplicates are possible after a retry or replay.
- **Error log:** append-only per delivery. Replaying an invalid event records it again under its original error type (in the final drill run, 1,200 rewound messages gave 1,198 duplicate warnings plus 2 re-rejected invalid events, and 0 new rows).
- **Exactly-once is not claimed** anywhere.

## Readiness

Fit to demonstrate as a portfolio project. The full stack starts from one documented command; every suite passes
from a clean rebuild (results in the README); the delivery guarantees are stated precisely and checked by tests
plus seven live failure drills; benchmark figures are measured, reproducible and recorded with their conditions.
The remaining limits are listed in the README "Known limitations". The main ones a reviewer will ask about are the
single-node infrastructure, at-least-once derived topics, and the absence of authentication by design.
