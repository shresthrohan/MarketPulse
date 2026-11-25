# MarketPulse benchmark

Measured 2026-11-25T01:35:38+00:00 with `scripts/benchmark.py --events 20000`. Every figure below comes from this run.

Run conditions (recorded by hand): final build after the technical audit; stack settled — no component restarted
in the preceding 2 minutes and maximum live end-to-end latency 71 ms in that window. Results taken right after
failure drills (broker/database restarts) are not comparable and were discarded.

## Environment

| | |
|---|---|
| timestamp_utc | 2026-11-25T01:35:38+00:00 |
| events | 20000 |
| cpu_count_logical | 22 |
| cpu_count_physical | 11 |
| memory_total_gib | 7.5 |
| platform | Linux-6.18.33.2-microsoft-standard-WSL2-x86_64-with-glibc2.41 |
| python | 3.12.16 |
| confluent_kafka | 2.6.1 |
| librdkafka | 2.6.1 |
| psycopg | 3.2.3 |
| redis_py | 5.2.1 |
| postgresql | 16.6 |
| timescaledb | 2.17.2 |
| kafka_brokers | 1 |
| raw_topic_partitions | 6 |
| instruments | 20 |

CPU and memory are what the benchmark container sees (on Docker Desktop: the Linux VM, shared by every service).

## Ingestion throughput (producer -> Kafka `market.raw`)

| events | elapsed s | events/s | delivered | failed | undelivered |
|---|---|---|---|---|---|
| 20000 | 0.242 | **82530.7** | 20000 | 0 | 0 |

How measured: one confluent-kafka Producer configured like the ingestion service (acks=all, idempotence, linger.ms=5, lz4), one Python thread, key = symbol, round-robin over the instruments table. Time from the first `produce()` to `flush()` returning with every message acknowledged; includes per-event UUID generation and JSON encoding. Prices: 20/20 symbols priced from Redis quotes, rest from config base_price.

## Processing throughput and end-to-end latency

| arrived | batches | processed_at span s | events/s (span) | wall s | events/s (wall) |
|---|---|---|---|---|---|
| 20000/20000 | 40 | 1.991 | **10043.0** | 2.065 | **9686.2** |

| latency ms | p50 | p95 | p99 | max |
|---|---|---|---|---|
| processed_at - ingested_at | 898.76 | 1683.63 | 1768.61 | 1773.78 |

How measured: the script polls `market_events` every 0.25 s until every benchmark event_id is stored (timeout 300.0 s). Span throughput = arrived / (max - min `processed_at`); `processed_at` is each batch's transaction start (`now()`), so the span leaves out the last batch's own write time and reads slightly high. Wall throughput = arrived / (first produce -> the poll that saw the last row), so it includes Kafka transit, consumer polling and up to one poll interval of observation delay. Latency = `processed_at` - `ingested_at` per event: `ingested_at` is stamped by this script just before `produce()`, `processed_at` by PostgreSQL at transaction start, so commit time, Redis update and offset commit are excluded. All events are sent as one burst, so this is backlog-drain latency under load, not idle steady-state latency. Both clocks are the same host clock. Other (non-benchmark) events the processor stored in the same `processed_at` window, i.e. competing live traffic: 0.

Accounting of the benchmark events: 20000 produced, 20000 stored (0 of them after a transient retry), rejected by type: none; stored + rejected (excluding duplicate warnings) = 20000/20000.

## Steady-state latency (live simulator traffic)

| window s | events | events/s | p50 ms | p95 ms | p99 ms | max ms |
|---|---|---|---|---|---|---|
| 60 | 1467 | 24.45 | 29.99 | 54.41 | 64.61 | 66.91 |

How measured: per-event `processed_at - ingested_at` of every non-benchmark event stored in the 60 s before the burst started (the normal simulator load), same clocks as above. This, not the burst figure, is the latency a user sees during normal operation.

## API response time

| endpoint | p50 ms | p95 ms | p99 ms | mean ms | non-200 |
|---|---|---|---|---|---|
| `/api/health` | 2.42 | 3.36 | 4.60 | 2.56 | 0  |
| `/api/market/overview` | 3.88 | 4.25 | 4.38 | 3.91 | 0  |
| `/api/instruments` | 3.38 | 3.86 | 4.21 | 3.42 | 0  |
| `/api/instruments/AAPL/history?range=1D` | 24.82 | 26.36 | 49.80 | 25.81 | 0  |
| `/api/instruments/AAPL/history?range=1Y` | 9.41 | 10.21 | 11.24 | 9.44 | 0  |
| `/api/instruments/AAPL/statistics` | 11.70 | 15.09 | 16.78 | 12.30 | 0  |
| `/api/analytics/correlation?symbols=AAPL,MSFT,NVDA,SPY,QQQ&range=1Y` | 20.05 | 24.98 | 57.52 | 21.95 | 0  |
| `/api/pipeline/overview` | 6.36 | 7.59 | 7.98 | 6.51 | 0  |
| `/api/market/history?page_size=50` | 5.25 | 6.53 | 6.74 | 5.43 | 0  |

How measured: sequential `urllib` GETs to `http://backend:8000` from inside the compose network, after the processing backlog drained. 3 untimed warm-up calls, then 40 timed calls per endpoint (new TCP connection each, full body read). Non-200 responses are counted and kept in the timings; 429 means the backend rate limit was hit.

## Database query performance

| query | execution ms (median) | planning ms | rows | buffers hit/read | plan nodes |
|---|---|---|---|---|---|
| 1D chart: 1m bars, one instrument, last 24 h | **0.32** | 0.196 | 1440 | 36/0 | Custom Scan x1, Index Scan x1 |
| 1Y daily closes, 10 instruments | **1.871** | 0.661 | 3650 | 373/0 | Index Scan x13, Sort x1, Custom Scan x1 |
| Data Explorer: one instrument, newest 50 events | **0.101** | 0.113 | 50 | 11/0 | Limit x1, Incremental Sort x1, Nested Loop x1, Index Scan x1, Materialize x1, Seq Scan x1 |
| Latest 50 events, all instruments | **0.131** | 0.155 | 50 | 44/0 | Index Scan x2, Limit x1, Incremental Sort x1, Nested Loop x1, Memoize x1 |
| Processing errors, last 24 h, by type | **0.159** | 0.091 | 9 | 22/0 | Sort x1, Aggregate x1, Index Only Scan x1 |

How measured: `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` of SQL with the same shape as the backend's queries, 1 discarded warm-up run then 5 runs; the median server-side Execution Time is reported (excludes planning and network). Plans below are from the last run.

**1D chart: 1m bars, one instrument, last 24 h**

```
Custom Scan (ChunkAppend) on historical_aggregates (rows=1440, loops=1)
  Index Scan using 1_3_uq_historical_aggregates_instrument_id_interval_timestamp on _hyper_1_1_chunk (rows=1440, loops=1)
```

**1Y daily closes, 10 instruments**

```
Sort (rows=3650, loops=1)
  Custom Scan (ChunkAppend) on historical_aggregates (rows=3650, loops=1)
    Index Scan using 2_6_uq_historical_aggregates_instrument_id_interval_timestamp on _hyper_1_2_chunk (rows=120, loops=1)
    Index Scan using 3_9_uq_historical_aggregates_instrument_id_interval_timestamp on _hyper_1_3_chunk (rows=300, loops=1)
    Index Scan using 4_12_uq_historical_aggregates_instrument_id_interval_timestamp on _hyper_1_4_chunk (rows=300, loops=1)
    Index Scan using 5_15_uq_historical_aggregates_instrument_id_interval_timestamp on _hyper_1_5_chunk (rows=300, loops=1)
    Index Scan using 6_18_uq_historical_aggregates_instrument_id_interval_timestamp on _hyper_1_6_chunk (rows=300, loops=1)
    Index Scan using 7_21_uq_historical_aggregates_instrument_id_interval_timestamp on _hyper_1_7_chunk (rows=300, loops=1)
    Index Scan using 8_24_uq_historical_aggregates_instrument_id_interval_timestamp on _hyper_1_8_chunk (rows=300, loops=1)
    Index Scan using 9_27_uq_historical_aggregates_instrument_id_interval_timestamp on _hyper_1_9_chunk (rows=300, loops=1)
    Index Scan using 10_30_uq_historical_aggregates_instrument_id_interval_timestamp on _hyper_1_10_chunk (rows=300, loops=1)
    Index Scan using 11_33_uq_historical_aggregates_instrument_id_interval_timestamp on _hyper_1_11_chunk (rows=300, loops=1)
... 3 more nodes
```

**Data Explorer: one instrument, newest 50 events**

```
Limit (rows=50, loops=1)
  Incremental Sort (rows=50, loops=1)
    Nested Loop (rows=51, loops=1)
      Index Scan using ix_market_events_instrument_id_event_timestamp on market_events (rows=51, loops=1)
      Materialize (rows=1, loops=51)
        Seq Scan on instruments (rows=1, loops=1)
```

**Latest 50 events, all instruments**

```
Limit (rows=50, loops=1)
  Incremental Sort (rows=50, loops=1)
    Nested Loop (rows=59, loops=1)
      Index Scan using ix_market_events_event_timestamp on market_events (rows=59, loops=1)
      Memoize (rows=1, loops=59)
        Index Scan using pk_instruments on instruments (rows=1, loops=20)
```

**Processing errors, last 24 h, by type**

```
Sort (rows=9, loops=1)
  Aggregate (rows=9, loops=1)
    Index Only Scan using ix_processing_errors_error_type_timestamp on processing_errors (rows=1263, loops=1)
```
