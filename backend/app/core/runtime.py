"""Process-local runtime state: start time, request latency window, rate limiter."""

from __future__ import annotations

import math
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from datetime import UTC, datetime


def percentile(sorted_values: list[float], p: float) -> float | None:
    """Nearest-rank percentile of an ascending list."""
    if not sorted_values:
        return None
    k = max(0, math.ceil(p / 100 * len(sorted_values)) - 1)
    return round(sorted_values[k], 2)


class RequestStats:
    """Latency of the last `maxlen` API requests (bounded; per process)."""

    def __init__(self, maxlen: int = 2000) -> None:
        self._samples: deque[tuple[str, float]] = deque(maxlen=maxlen)
        self.total = 0

    def record(self, route: str, ms: float) -> None:
        self._samples.append((route, ms))
        self.total += 1

    def summary(self) -> dict:
        samples = list(self._samples)
        all_ms = sorted(ms for _, ms in samples)
        by_route: dict[str, list[float]] = {}
        for route, ms in samples:
            by_route.setdefault(route, []).append(ms)
        routes = [
            {"route": r, "count": len(v), "p50_ms": percentile(sorted(v), 50), "p95_ms": percentile(sorted(v), 95)}
            for r, v in by_route.items()
        ]
        routes.sort(key=lambda x: x["count"], reverse=True)
        return {"requests": len(samples), "total_requests": self.total, "p50_ms": percentile(all_ms, 50),
                "p95_ms": percentile(all_ms, 95), "p99_ms": percentile(all_ms, 99), "routes": routes[:20]}


class RateLimiter:
    """Sliding-window log per client key. Bounded: at most `max_clients` keys, oldest evicted.

    ponytail: per-process and keyed by peer address; behind one reverse proxy all users share a
    key. Move to Redis + X-Forwarded-For from a trusted proxy for multi-user deployments.
    """

    def __init__(self, limit: int, window_s: float = 60.0, max_clients: int = 10_000) -> None:
        if limit < 1 or window_s <= 0:
            raise ValueError("limit and window must be positive")
        self.limit, self.window_s, self.max_clients = limit, window_s, max_clients
        self._hits: OrderedDict[str, deque[float]] = OrderedDict()

    def check(self, key: str, now: float) -> float | None:
        """None if allowed (and recorded), else seconds until the next request is allowed."""
        hits = self._hits.get(key)
        if hits is None:
            hits = self._hits[key] = deque(maxlen=self.limit)
            if len(self._hits) > self.max_clients:
                self._hits.popitem(last=False)
        self._hits.move_to_end(key)
        if len(hits) == self.limit and now - hits[0] < self.window_s:
            return self.window_s - (now - hits[0])
        hits.append(now)
        return None


@dataclass
class Runtime:
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    request_stats: RequestStats = field(default_factory=RequestStats)
    _t0: float = field(default_factory=time.monotonic)

    def uptime_s(self) -> float:
        return round(time.monotonic() - self._t0, 1)


runtime = Runtime()
