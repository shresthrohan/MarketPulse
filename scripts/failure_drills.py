"""Failure drills against the running compose stack. Run from the repo root on the host:

    python scripts/failure_drills.py [--output docs/failure-drills.md]

Needs only Python 3.10+ and the docker CLI. Each drill breaks one real component (stop, kill -9,
data loss, offset rewind), then asserts that storage stays correct and the system recovers by
itself. Every drill restores the services it touched, even when it fails. Exit code 1 if any drill fails.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

API = "http://localhost:8000"
GROUP = "marketpulse-processor"


def sh(*args: str, check: bool = True) -> str:
    r = subprocess.run(["docker", "compose", *args], capture_output=True, text=True, timeout=180)
    if check and r.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(args)} failed: {r.stderr.strip()[-300:]}")
    return r.stdout.strip()


def env_value(name: str, default: str) -> str:
    try:
        for line in Path(".env").read_text(encoding="utf-8").splitlines():
            if line.startswith(f"{name}="):
                return line.split("=", 1)[1].strip() or default
    except OSError:
        pass
    return default


PG_USER = env_value("POSTGRES_USER", "marketpulse")
PG_DB = env_value("POSTGRES_DB", "marketpulse")


def sql(query: str) -> str:
    return sh("exec", "-T", "postgres", "psql", "-U", PG_USER, "-d", PG_DB, "-tAc", query)


def api(path: str) -> tuple[int, dict | None]:
    try:
        with urllib.request.urlopen(API + path, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except ValueError:
            return e.code, None
    except (urllib.error.URLError, OSError, ValueError):
        return 0, None


def wait(cond: Callable[[], object], timeout: float, what: str) -> object:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            value = cond()
        except (RuntimeError, subprocess.TimeoutExpired, TypeError, KeyError, ValueError):  # not ready yet
            value = None
        if value:
            return value
        time.sleep(1)
    raise AssertionError(f"timed out after {timeout:.0f}s waiting for: {what}")


def container_state(service: str) -> tuple[int, bool]:
    cid = sh("ps", "-q", service)
    out = subprocess.run(["docker", "inspect", "-f", "{{.RestartCount}} {{.State.Running}}", cid],
                         capture_output=True, text=True, check=True).stdout.split()
    return int(out[0]), out[1] == "true"


def stored() -> int:
    return int(sql("SELECT count(*) FROM market_events"))


def duplicate_ids() -> int:
    return int(sql("SELECT count(*) - count(DISTINCT event_id) FROM market_events"))


def lag() -> int | None:
    status, body = api("/api/health/kafka")
    return body.get("consumer_lag") if status == 200 and body else None


def instruments() -> int:
    return int(sql("SELECT count(*) FROM instruments"))


def flowing(before: int, timeout: float = 60) -> int:
    return wait(lambda: (n := stored()) > before and n, timeout, "new events stored")


# ---- drills ------------------------------------------------------------------------------


def drill_redis_outage() -> str:
    sh("stop", "redis")
    try:
        time.sleep(3)
        status, ov = api("/api/market/overview")
        assert status == 200 and ov["cache"]["available"] is False, f"overview during outage: {status} {ov and ov.get('cache')}"
        assert api("/api/health/redis")[0] == 503, "redis health should be 503 while down"
        before = stored()
        flowing(before, 30)  # PostgreSQL keeps receiving events: Redis is not on the storage path
        assert container_state("processing")[1], "processor must keep running without Redis"
    finally:
        sh("start", "redis")
    wait(lambda: (o := api("/api/market/overview")[1]) and o["cache"]["available"] and not o["cache"]["stale"]
         and o["cache"]["quotes"] == instruments(), 60, "fresh quotes for every instrument")
    return "API served 200 with cache.available=false and storage continued during the outage; quotes fresh again after restart"


def drill_redis_data_loss() -> str:
    sh("stop", "ingestion")  # no live ticks, so any quote that comes back was rebuilt from PostgreSQL
    try:
        time.sleep(2)
        sh("exec", "-T", "redis", "redis-cli", "FLUSHALL")
        n = instruments()

        latest = dict(line.split("|") for line in sql(
            "SELECT DISTINCT ON (i.symbol) i.symbol, e.event_id FROM market_events e JOIN instruments i "
            "ON i.id = e.instrument_id ORDER BY i.symbol, e.event_timestamp DESC").splitlines())

        def rebuilt():
            rows = api("/api/instruments?page_size=100")[1]["items"]
            quotes = {r["symbol"]: r["quote"] for r in rows if r["quote"]}
            return len(quotes) == n and quotes

        quotes = wait(rebuilt, 30, "every quote back in the cache")
        wrong = [s for s, q in quotes.items() if q["event_id"] != latest.get(s)]
        assert not wrong, f"cached quote is not the latest stored event for {wrong}"
        origins = sorted({q.get("origin") for q in quotes.values()})
        assert "rebuild" in origins, f"no quote came from the PostgreSQL rebuild (origins {origins})"
        spy = quotes["SPY"]["price"]
    finally:
        sh("start", "ingestion")

    def resumed():
        q = api("/api/instruments/SPY")[1]["quote"]
        return q and q.get("origin") == "stream" and q

    q = wait(resumed, 30, "live quotes after ingestion restart")
    jump = abs(q["price"] - spy) / spy
    assert jump < 0.01, f"simulator restarted {jump:.2%} away from the last price (continuity lost)"
    rebuilt_n = sum(q.get("origin") == "rebuild" for q in quotes.values())
    return (f"{n}/{n} quotes back after FLUSHALL ({rebuilt_n} rebuilt from PostgreSQL), each equal to the latest stored "
            f"event; simulator resumed within {jump:.3%} of the last price")


def drill_processor_kill() -> str:
    before = stored()
    restarts, _ = container_state("processing")
    sh("kill", "-s", "SIGKILL", "processing")
    time.sleep(6)  # events pile up in Kafka meanwhile
    backlog = lag()
    sh("start", "processing")
    wait(lambda: (lg := lag()) is not None and lg < 50, 90, "consumer lag drained")
    after = flowing(before, 30)
    assert duplicate_ids() == 0, "duplicate event_id rows after crash recovery"
    return f"killed with SIGKILL; backlog {backlog} drained after restart; +{after - before} rows, 0 duplicate ids"


def drill_offset_rewind_replay() -> str:
    sh("stop", "processing")
    try:
        cutoff = sql("SELECT now()")  # database clock, not the host's
        before = int(sql(f"SELECT count(*) FROM market_events WHERE processed_at < '{cutoff}'"))
        dups_before = int(sql("SELECT count(*) FROM processing_errors WHERE error_type = 'duplicate_event'"))
        out = sh("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092",
                 "--group", GROUP, "--topic", "market.raw", "--reset-offsets", "--shift-by", "-200", "--execute")
        partitions = sum(1 for line in out.splitlines() if line.split()[:2] == [GROUP, "market.raw"])
        rewound = (partitions or 6) * 200
    finally:
        sh("start", "processing")
    wait(lambda: (lg := lag()) is not None and lg < 50, 120, "replay drained")
    time.sleep(3)
    after = int(sql(f"SELECT count(*) FROM market_events WHERE processed_at < '{cutoff}'"))
    dups = int(sql("SELECT count(*) FROM processing_errors WHERE error_type = 'duplicate_event'")) - dups_before
    assert after == before, f"rows stored before the rewind changed: {before} -> {after}"
    assert duplicate_ids() == 0, "duplicate event_id rows after replay"
    assert dups >= 0.9 * rewound, f"expected ~{rewound} replayed duplicates, saw {dups}"
    return f"rewound {rewound} offsets; replay produced {dups} duplicate warnings and 0 new rows for already-stored events"


def drill_postgres_restart() -> str:
    restarts, _ = container_state("processing")
    before = stored()
    sh("restart", "postgres")
    wait(lambda: api("/api/health/database")[0] == 200, 60, "API database health back")
    flowing(before, 60)
    r2, running = container_state("processing")
    assert running and r2 == restarts, f"processor restarted ({restarts} -> {r2}); it should retry, not crash"
    retried = sql("SELECT count(*) FROM market_events WHERE processing_status = 'retried'")
    return f"processor retried through the outage without restarting; API recovered; {retried} event(s) stored as retried in total"


def ws_roundtrip() -> bool:
    script = ("const ws=new WebSocket('ws://frontend/ws/market');ws.onopen=()=>ws.send(JSON.stringify({action:'subscribe',"
              "symbols:['*']}));ws.onmessage=e=>{if(JSON.parse(e.data).type==='quote')process.exit(0)};"
              "setTimeout(()=>process.exit(1),15000)")
    r = subprocess.run(["docker", "run", "--rm", "--network", "marketpulse_default", "node:22-alpine", "node", "-e", script],
                       capture_output=True, timeout=60)
    return r.returncode == 0


def drill_backend_restart() -> str:
    sh("up", "-d", "--force-recreate", "--no-deps", "backend")  # new container, possibly a new IP
    wait(lambda: api("/api/health")[0] == 200, 60, "backend healthy")
    assert ws_roundtrip(), "no live quote over WebSocket through nginx after backend recreate"
    return "backend recreated; REST healthy and a live quote received over WebSocket through nginx"


def drill_kafka_restart() -> str:
    before = stored()
    sh("restart", "kafka")
    wait(lambda: (s := api("/api/health/kafka"))[0] == 200 and s[1]["status"] == "up", 120, "kafka up")
    flowing(before, 120)
    for svc in ("ingestion", "processing"):
        assert container_state(svc)[1], f"{svc} not running after kafka restart"
    return "broker restarted; ingestion and processor reconnected by themselves and events flow again"


DRILLS = [drill_redis_outage, drill_redis_data_loss, drill_processor_kill, drill_offset_rewind_replay,
          drill_postgres_restart, drill_backend_restart, drill_kafka_restart]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", help="write a Markdown report here")
    parser.add_argument("--only", help="comma-separated drill names (without the drill_ prefix)")
    args = parser.parse_args()
    chosen = [d for d in DRILLS if not args.only or d.__name__.removeprefix("drill_") in args.only.split(",")]
    results = []
    for drill in chosen:
        name = drill.__name__.removeprefix("drill_")
        t0 = time.monotonic()
        try:
            detail, ok = drill(), True
        except Exception as exc:  # report and continue with the next drill
            detail, ok = f"{type(exc).__name__}: {exc}", False
        results.append((name, ok, round(time.monotonic() - t0, 1), detail))
        print(f"{'PASS' if ok else 'FAIL'} {name} ({results[-1][2]}s): {detail}", flush=True)
    if args.output:
        lines = [
            "# Failure drills", "",
            f"Run {datetime.now(UTC).isoformat(timespec='seconds')} with `python scripts/failure_drills.py` against the "
            "local compose stack. Each drill breaks one real component and asserts recovery.", "",
            "| drill | result | seconds | evidence |", "|---|---|---|---|",
            *[f"| {n} | {'PASS' if ok else 'FAIL'} | {s} | {d.replace('|', '/')} |" for n, ok, s, d in results], "",
        ]
        Path(args.output).write_text("\n".join(lines), encoding="utf-8")
    sys.exit(0 if all(ok for _, ok, _, _ in results) else 1)


if __name__ == "__main__":
    main()
