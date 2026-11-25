"""Benchmark a running MarketPulse stack. Every number in the report is measured in this run.

docker compose run --rm -v ./docs:/srv/docs processing \
    python /srv/scripts/benchmark.py --events 20000 --output /srv/docs/benchmark.md

Order: produce a burst to market.raw -> wait until the processor has stored it -> time the
API (after the backlog has drained, so the burst does not distort it) -> EXPLAIN ANALYZE.
Benchmark events are real, valid events (source "benchmark", priced at the current quote);
they stay in market_events like any other processed event.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import psutil
import psycopg
import redis
from confluent_kafka import KafkaException, Producer, libversion
from confluent_kafka import version as kafka_client_version

QUOTES_KEY = "mp:quotes"  # processing.consumers.processor.QUOTES_KEY
POLL_S = 0.25
ENDPOINTS = [
    "/api/health",
    "/api/market/overview",
    "/api/instruments",
    "/api/instruments/AAPL/history?range=1D",
    "/api/instruments/AAPL/history?range=1Y",
    "/api/instruments/AAPL/statistics",
    "/api/analytics/correlation?symbols=AAPL,MSFT,NVDA,SPY,QQQ&range=1Y",
    "/api/pipeline/overview",
    "/api/market/history?page_size=50",
]
EVENT_COLUMNS = """e.event_id, i.symbol, e.event_timestamp, e.price, e.volume, e.open, e.high, e.low, e.close, e.source,
       e.data_mode, e.processing_status, e.attempts, e.ingested_at, e.processed_at,
       extract(epoch FROM e.processed_at - e.ingested_at) * 1000 AS latency_ms"""
# Same SQL shape as backend/app/repositories (market_data.py, pipeline.py).
QUERIES = [
    ("1D chart: 1m bars, one instrument, last 24 h",
     """SELECT "timestamp", open, high, low, close, volume FROM historical_aggregates
        WHERE instrument_id = %(iid)s AND "interval" = '1m' AND "timestamp" >= now() - interval '24 hours'
        ORDER BY "timestamp" """),
    ("1Y daily closes, 10 instruments",
     """SELECT instrument_id, "timestamp", close, volume FROM historical_aggregates
        WHERE instrument_id = ANY(%(iids)s::int[]) AND "interval" = '1d' AND "timestamp" >= now() - interval '365 days'
        ORDER BY "timestamp" """),
    ("Data Explorer: one instrument, newest 50 events",
     f"""SELECT {EVENT_COLUMNS} FROM market_events e JOIN instruments i ON i.id = e.instrument_id
        WHERE e.instrument_id = %(iid)s ORDER BY e.event_timestamp DESC, e.id DESC LIMIT 50"""),
    ("Latest 50 events, all instruments",
     f"""SELECT {EVENT_COLUMNS} FROM market_events e JOIN instruments i ON i.id = e.instrument_id
        ORDER BY e.event_timestamp DESC, e.id DESC LIMIT 50"""),
    ("Processing errors, last 24 h, by type",
     """SELECT error_type, count(*) FROM processing_errors WHERE "timestamp" >= now() - interval '24 hours'
        GROUP BY error_type ORDER BY count(*) DESC"""),
]


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def dist(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    q = statistics.quantiles(values, n=100, method="inclusive") if len(values) > 1 else values * 99
    return {k: round(v, 3) for k, v in
            {"p50": q[49], "p95": q[94], "p99": q[98], "max": max(values), "mean": statistics.fmean(values)}.items()}


# ---- ingestion ------------------------------------------------------------------


def current_prices(symbols: list[str], fallback: dict[str, float]) -> tuple[dict[str, float], str]:
    """Price each symbol at its live quote so benchmark ticks do not move the charts."""
    try:
        r = redis.Redis.from_url(env("REDIS_URL", "redis://redis:6379/0"), socket_timeout=3, socket_connect_timeout=3)
        raw = r.hmget(QUOTES_KEY, symbols)
    except (redis.RedisError, OSError) as exc:
        print(f"warning: redis unavailable ({exc}); using config base prices", file=sys.stderr)
        raw = [None] * len(symbols)
    prices, from_redis = {}, 0
    for sym, value in zip(symbols, raw, strict=True):
        price = json.loads(value).get("price") if value else None
        from_redis += price is not None
        prices[sym] = float(price) if price is not None else fallback[sym]
    return prices, f"{from_redis}/{len(symbols)} symbols priced from Redis quotes, rest from config base_price"


def produce(producer: Producer, topic: str, symbols: list[str], prices: dict[str, float], n: int) -> dict:
    failures: list[str] = []

    def on_delivery(err, _msg) -> None:
        if err is not None:
            failures.append(str(err))

    ids: list[uuid.UUID] = []
    t0 = time.perf_counter()
    for i in range(n):
        sym = symbols[i % len(symbols)]
        p = prices[sym]
        now = datetime.now(UTC)
        eid = uuid.uuid4()
        ids.append(eid)
        value = json.dumps(
            {"event_id": str(eid), "symbol": sym, "timestamp": now.isoformat(timespec="milliseconds"), "price": p,
             "volume": 1, "open": p, "high": p, "low": p, "close": p, "source": "benchmark", "data_mode": "simulated",
             "schema_version": 1, "ingested_at": now.isoformat(timespec="microseconds")},
            separators=(",", ":"),
        ).encode()
        while True:
            try:
                producer.produce(topic, value=value, key=sym.encode(), on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
    deadline = time.monotonic() + 60
    remaining = producer.flush(1.0)
    while remaining and time.monotonic() < deadline:  # short flushes keep Ctrl-C responsive
        remaining = producer.flush(1.0)
    elapsed = time.perf_counter() - t0
    return {
        "ids": ids, "t0": t0, "elapsed_s": round(elapsed, 3), "events_per_s": round(n / elapsed, 1),
        "delivered": n - len(failures) - remaining, "delivery_failed": len(failures), "undelivered_after_60s": remaining,
        "first_error": failures[0] if failures else None,
    }


# ---- processing -----------------------------------------------------------------


def wait_processed(conn: psycopg.Connection, ids: list[uuid.UUID], timeout_s: float) -> tuple[list[tuple], float | None]:
    pending = set(ids)
    rows: list[tuple] = []
    last_seen = None
    deadline = time.perf_counter() + timeout_s
    while pending and time.perf_counter() < deadline:
        batch = list(pending)
        with conn.cursor() as cur:
            for i in range(0, len(batch), 5000):
                cur.execute("SELECT event_id, ingested_at, processed_at, processing_status FROM market_events WHERE event_id = ANY(%s::uuid[])",
                            (batch[i:i + 5000],))
                found = cur.fetchall()
                rows += found
                pending.difference_update(r[0] for r in found)
                if found:
                    last_seen = time.perf_counter()
        if pending:
            time.sleep(POLL_S)
    return rows, last_seen


def processing_stats(rows: list[tuple], n: int, t0: float, last_seen: float | None) -> dict:
    processed = [r[2] for r in rows]
    span = (max(processed) - min(processed)).total_seconds() if rows else 0.0
    latencies = [(r[2] - r[1]).total_seconds() * 1000 for r in rows if r[1] is not None]
    return {
        "arrived": len(rows), "expected": n,
        "batches_observed": len(set(processed)),
        "processed_at_span_s": round(span, 3),
        "events_per_s_span": round(len(rows) / span, 1) if span > 0 else None,
        "wall_s": round(last_seen - t0, 3) if last_seen else None,
        "events_per_s_wall": round(len(rows) / (last_seen - t0), 1) if last_seen else None,
        "latency_ms": dist(latencies),
        "stored_retried": sum(1 for r in rows if r[3] == "retried"),
    }


def steady_state(conn: psycopg.Connection, window_s: int = 60) -> dict:
    """Per-event latency of live (non-benchmark) traffic in the window just before the burst."""
    rows = conn.execute(
        """SELECT EXTRACT(EPOCH FROM processed_at - ingested_at) * 1000 FROM market_events
           WHERE event_timestamp >= now() - make_interval(secs => %s) * 2 AND processed_at >= now() - make_interval(secs => %s)
             AND source <> 'benchmark' AND ingested_at IS NOT NULL""", (window_s, window_s)).fetchall()
    values = [float(r[0]) for r in rows]
    return {"window_s": window_s, "events": len(values), "events_per_s": round(len(values) / window_s, 2),
            "latency_ms": dist(values)}


def rejected_benchmark_events(conn: psycopg.Connection, ids: list[uuid.UUID]) -> dict[str, int]:
    """Benchmark events the processor rejected, by error type; with the stored ones they must cover every produced id."""
    out: dict[str, int] = {}
    with conn.cursor() as cur:
        for i in range(0, len(ids), 5000):
            cur.execute("SELECT error_type, count(*) FROM processing_errors WHERE event_id = ANY(%s) GROUP BY 1",
                        ([str(x) for x in ids[i:i + 5000]],))
            for kind, n in cur.fetchall():
                out[kind] = out.get(kind, 0) + n
    return out


# ---- API ------------------------------------------------------------------------


def get(url: str) -> tuple[int | str, float]:
    t = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            resp.read()
            status: int | str = resp.status
    except urllib.error.HTTPError as exc:
        exc.read()
        status = exc.code
    except (urllib.error.URLError, OSError) as exc:
        status = f"error: {getattr(exc, 'reason', exc)}"
    return status, (time.perf_counter() - t) * 1000


def bench_api(base: str, n: int, warmup: int = 3) -> dict:
    status, _ = get(base + "/api/health")
    if not isinstance(status, int):
        return {"error": f"backend unreachable at {base}: {status}"}
    out = {}
    for path in ENDPOINTS:
        for _ in range(warmup):
            get(base + path)
        times, statuses = [], Counter()
        for _ in range(n):
            status, ms = get(base + path)
            statuses[str(status)] += 1
            if isinstance(status, int):
                times.append(ms)
        out[path] = {"latency_ms": dist(times), "requests": n,
                     "non_200": sum(c for s, c in statuses.items() if s != "200"), "statuses": dict(statuses)}
    return out


# ---- database -------------------------------------------------------------------


def plan_lines(node: dict, depth: int = 0) -> list[str]:
    label = node["Node Type"]
    if "Custom Plan Provider" in node:  # e.g. TimescaleDB ChunkAppend
        label += f" ({node['Custom Plan Provider']})"
    if "Index Name" in node:
        label += f" using {node['Index Name']}"
    if "Relation Name" in node:
        label += f" on {node['Relation Name']}"
    label += f" (rows={node.get('Actual Rows')}, loops={node.get('Actual Loops')})"
    return ["  " * depth + label] + [line for child in node.get("Plans", []) for line in plan_lines(child, depth + 1)]


def node_types(node: dict) -> Counter:
    c = Counter([node["Node Type"]])
    for child in node.get("Plans", []):
        c += node_types(child)
    return c


def bench_queries(conn: psycopg.Connection, params: dict, runs: int = 5) -> dict:
    out = {}
    with conn.cursor() as cur:
        for name, sql in QUERIES:
            explains = []
            for _ in range(runs + 1):  # first run warms the cache and is discarded
                cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, params)
                doc = cur.fetchone()[0]
                explains.append(doc[0] if isinstance(doc, list) else json.loads(doc)[0])
            last = explains[-1]
            plan = last["Plan"]
            out[name] = {
                "execution_ms_median": round(statistics.median(e["Execution Time"] for e in explains[1:]), 3),
                "planning_ms_median": round(statistics.median(e["Planning Time"] for e in explains[1:]), 3),
                "rows": plan.get("Actual Rows"),
                "shared_hit_blocks": plan.get("Shared Hit Blocks"), "shared_read_blocks": plan.get("Shared Read Blocks"),
                "node_types": dict(node_types(plan).most_common()),
                "plan": plan_lines(plan),
            }
    return out


# ---- report ---------------------------------------------------------------------


def fmt(d: dict | None, keys=("p50", "p95", "p99", "max")) -> list[str]:
    return [f"{d[k]:.2f}" if d else "n/a" for k in keys]


def render(s: dict) -> str:
    e, ing, pr, st = s["environment"], s["ingestion"], s["processing"], s["steady_state"]
    lines = [
        "# MarketPulse benchmark",
        "",
        f"Measured {e['timestamp_utc']} with `scripts/benchmark.py --events {e['events']}`. Every figure below comes from this run.",
        "",
        "## Environment",
        "",
        "| | |", "|---|---|",
        *[f"| {k} | {v} |" for k, v in e.items()],
        "",
        "CPU and memory are what the benchmark container sees (on Docker Desktop: the Linux VM, shared by every service).",
        "",
        "## Ingestion throughput (producer -> Kafka `market.raw`)",
        "",
        "| events | elapsed s | events/s | delivered | failed | undelivered |", "|---|---|---|---|---|---|",
        f"| {e['events']} | {ing['elapsed_s']} | **{ing['events_per_s']}** | {ing['delivered']} | {ing['delivery_failed']} | {ing['undelivered_after_60s']} |",
        "",
        "How measured: one confluent-kafka Producer configured like the ingestion service (acks=all, idempotence, "
        "linger.ms=5, lz4), one Python thread, key = symbol, round-robin over the instruments table. Time from the first "
        "`produce()` to `flush()` returning with every message acknowledged; includes per-event UUID generation and JSON "
        f"encoding. Prices: {s['price_source']}.",
        "",
        "## Processing throughput and end-to-end latency",
        "",
        "| arrived | batches | processed_at span s | events/s (span) | wall s | events/s (wall) |", "|---|---|---|---|---|---|",
        f"| {pr['arrived']}/{pr['expected']} | {pr['batches_observed']} | {pr['processed_at_span_s']} | "
        f"**{pr['events_per_s_span']}** | {pr['wall_s']} | **{pr['events_per_s_wall']}** |",
        "",
        "| latency ms | p50 | p95 | p99 | max |", "|---|---|---|---|---|",
        "| processed_at - ingested_at | " + " | ".join(fmt(pr["latency_ms"])) + " |",
        "",
        "How measured: the script polls `market_events` every 0.25 s until every benchmark event_id is stored "
        f"(timeout {s['timeout_s']} s). Span throughput = arrived / (max - min `processed_at`); `processed_at` is each "
        "batch's transaction start (`now()`), so the span leaves out the last batch's own write time and reads slightly "
        "high. Wall throughput = arrived / (first produce -> the poll that saw the last row), so it includes Kafka "
        "transit, consumer polling and up to one poll interval of observation delay. Latency = `processed_at` - "
        "`ingested_at` per event: `ingested_at` is stamped by this script just before `produce()`, `processed_at` by "
        "PostgreSQL at transaction start, so commit time, Redis update and offset commit are excluded. All events are "
        "sent as one burst, so this is backlog-drain latency under load, not idle steady-state latency. Both clocks "
        "are the same host clock. Other (non-benchmark) events the processor stored in the same `processed_at` window, "
        f"i.e. competing live traffic: {pr.get('other_events_in_window', 'n/a')}.",
        "",
        "Accounting of the benchmark events: "
        f"{pr['expected']} produced, {pr['arrived']} stored ({pr['stored_retried']} of them after a transient retry), "
        f"rejected by type: {pr['rejected'] or 'none'}; stored + rejected (excluding duplicate warnings) = "
        f"{pr['accounted']}/{pr['expected']}.",
        "",
        "## Steady-state latency (live simulator traffic)",
        "",
        "| window s | events | events/s | p50 ms | p95 ms | p99 ms | max ms |", "|---|---|---|---|---|---|---|",
        f"| {st['window_s']} | {st['events']} | {st['events_per_s']} | " + " | ".join(fmt(st["latency_ms"])) + " |",
        "",
        "How measured: per-event `processed_at - ingested_at` of every non-benchmark event stored in the "
        f"{st['window_s']} s before the burst started (the normal simulator load), same clocks as above. "
        "This, not the burst figure, is the latency a user sees during normal operation.",
        "",
        "## API response time",
        "",
    ]
    api = s["api"]
    if "error" in api:
        lines += [f"Not measured: {api['error']}.", ""]
    else:
        lines += ["| endpoint | p50 ms | p95 ms | p99 ms | mean ms | non-200 |", "|---|---|---|---|---|---|"]
        for path, r in api.items():
            lines.append(f"| `{path}` | " + " | ".join(fmt(r["latency_ms"], ("p50", "p95", "p99", "mean")))
                         + f" | {r['non_200']} {'' if not r['non_200'] else r['statuses']} |")
        lines.append("")
    lines += [
        f"How measured: sequential `urllib` GETs to `{s['api_base']}` from inside the compose network, after the "
        f"processing backlog drained. 3 untimed warm-up calls, then {s['api_requests']} timed calls per endpoint "
        "(new TCP connection each, full body read). Non-200 responses are counted and kept in the timings; 429 means "
        "the backend rate limit was hit.",
        "",
        "## Database query performance",
        "",
        "| query | execution ms (median) | planning ms | rows | buffers hit/read | plan nodes |", "|---|---|---|---|---|---|",
    ]
    for name, q in s["queries"].items():
        nodes = ", ".join(f"{k} x{v}" for k, v in q["node_types"].items())
        lines.append(f"| {name} | **{q['execution_ms_median']}** | {q['planning_ms_median']} | {q['rows']} | "
                     f"{q['shared_hit_blocks']}/{q['shared_read_blocks']} | {nodes} |")
    lines += [
        "",
        "How measured: `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` of SQL with the same shape as the backend's queries, "
        "1 discarded warm-up run then 5 runs; the median server-side Execution Time is reported (excludes planning and "
        "network). Plans below are from the last run.",
        "",
    ]
    for name, q in s["queries"].items():
        body = q["plan"][:12] + ([f"... {len(q['plan']) - 12} more nodes"] if len(q["plan"]) > 12 else [])
        lines += [f"**{name}**", "", "```", *body, "```", ""]
    return "\n".join(lines)


# ---- main -----------------------------------------------------------------------


def run(args: argparse.Namespace) -> dict:
    dsn = env("DATABASE_URL", "postgresql://marketpulse@postgres:5432/marketpulse").replace("+asyncpg", "")
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = env("RAW_TOPIC", "market.raw")
    config_path = env("INSTRUMENTS_FILE", str(Path(__file__).resolve().parents[1] / "config" / "instruments.json"))
    base_prices = {row["symbol"]: float(row["base_price"]) for row in json.loads(Path(config_path).read_text("utf-8"))}

    try:
        conn = psycopg.connect(dsn, autocommit=True, connect_timeout=5)
    except psycopg.OperationalError as exc:
        sys.exit(f"database unreachable at {urlsplit(dsn).hostname}: {exc}")
    with conn:
        ids_by_symbol = dict(conn.execute("SELECT symbol, id FROM instruments ORDER BY symbol").fetchall())
        if not ids_by_symbol:
            sys.exit("instruments table is empty; run the migrate/seed step first")
        symbols = list(ids_by_symbol)
        pg_version = conn.execute("SHOW server_version").fetchone()[0]
        ts_row = conn.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").fetchone()

        producer = Producer({
            "bootstrap.servers": bootstrap, "client.id": "marketpulse-benchmark", "acks": "all",
            "enable.idempotence": True, "linger.ms": 5, "compression.type": "lz4", "message.timeout.ms": 30000,
        })
        try:
            metadata = producer.list_topics(topic, timeout=10)
        except KafkaException as exc:
            sys.exit(f"kafka unreachable at {bootstrap}: {exc}")
        if topic not in metadata.topics or metadata.topics[topic].error is not None:
            sys.exit(f"topic {topic} is not available on {bootstrap} (topic auto-create is off; is kafka-init done?)")
        partitions = len(metadata.topics[topic].partitions)

        steady = steady_state(conn)
        prices, price_source = current_prices(symbols, base_prices)
        print(f"producing {args.events} events to {topic} ...", file=sys.stderr)
        ingestion = produce(producer, topic, symbols, prices, args.events)
        ids, t0 = ingestion.pop("ids"), ingestion.pop("t0")
        print(f"  {ingestion['events_per_s']} events/s; waiting for the processor ...", file=sys.stderr)
        rows, last_seen = wait_processed(conn, ids, args.timeout)
        processing = processing_stats(rows, args.events, t0, last_seen)
        if rows:
            lo, hi = min(r[2] for r in rows), max(r[2] for r in rows)
            # The event_timestamp bound lets the index do the work; live events are stamped seconds before processing.
            processing["other_events_in_window"] = conn.execute(
                """SELECT count(*) FROM market_events WHERE event_timestamp >= %s - interval '5 minutes'
                   AND processed_at BETWEEN %s AND %s AND source <> 'benchmark'""", (lo, lo, hi)).fetchone()[0]
        processing["rejected"] = rejected_benchmark_events(conn, ids)
        processing["accounted"] = processing["arrived"] + sum(
            n for kind, n in processing["rejected"].items() if kind != "duplicate_event")
        if processing["arrived"] < args.events:
            print(f"warning: only {processing['arrived']}/{args.events} events stored within {args.timeout}s", file=sys.stderr)

        print("timing API endpoints ...", file=sys.stderr)
        api = bench_api(args.api.rstrip("/"), args.api_requests)
        print("explaining queries ...", file=sys.stderr)
        queries = bench_queries(conn, {"iid": ids_by_symbol.get("AAPL", ids_by_symbol[symbols[0]]),
                                       "iids": list(ids_by_symbol.values())[:10]})

    return {
        "environment": {
            "timestamp_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            "events": args.events,
            "cpu_count_logical": os.cpu_count(),
            "cpu_count_physical": psutil.cpu_count(logical=False),
            "memory_total_gib": round(psutil.virtual_memory().total / 2**30, 1),
            "platform": platform.platform(),
            "python": platform.python_version(),
            "confluent_kafka": kafka_client_version()[0],
            "librdkafka": libversion()[0],
            "psycopg": psycopg.__version__,
            "redis_py": redis.__version__,
            "postgresql": pg_version,
            "timescaledb": ts_row[0] if ts_row else "not installed",
            "kafka_brokers": len(metadata.brokers),
            "raw_topic_partitions": partitions,
            "instruments": len(symbols),
        },
        "price_source": price_source,
        "timeout_s": args.timeout,
        "api_base": args.api,
        "api_requests": args.api_requests,
        "ingestion": ingestion,
        "processing": processing,
        "steady_state": steady,
        "api": api,
        "queries": queries,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--events", type=int, default=20000)
    parser.add_argument("--output", help="Markdown report path")
    parser.add_argument("--api", default=env("API_BASE_URL", "http://backend:8000"))
    parser.add_argument("--api-requests", type=int, default=40, help="timed requests per endpoint")
    parser.add_argument("--timeout", type=float, default=300.0, help="seconds to wait for the processor")
    args = parser.parse_args()
    if args.events < 1 or args.api_requests < 1:
        parser.error("--events and --api-requests must be >= 1")
    try:
        summary = run(args)
    except KeyboardInterrupt:
        sys.exit("interrupted")
    no_plans = {name: {k: v for k, v in q.items() if k != "plan"} for name, q in summary["queries"].items()}
    print(json.dumps(summary | {"queries": no_plans}, indent=2, default=str))
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(render(summary), encoding="utf-8")
        print(f"report written to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
