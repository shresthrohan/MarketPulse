"""Per-symbol rolling enrichment. Pure: returns a new state instead of mutating.

The processor only adopts the returned state after the database commit, so a
rolled-back and retried batch is never counted twice in the rolling windows.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from processing.validators.event_validator import ValidEvent


@dataclass(frozen=True, slots=True)
class Window:
    prices: tuple[float, ...]
    volumes: tuple[float, ...]


@dataclass(frozen=True, slots=True)
class Enriched:
    event: ValidEvent
    tick_change_pct: float | None  # vs previous processed price of the same symbol
    rolling_avg: float  # SMA of the last `window_size` prices
    rolling_volume: float  # summed volume of the last `window_size` events
    window_size: int


def enrich(state: Mapping[str, Window], events: Sequence[ValidEvent], window: int) -> tuple[dict[str, Window], list[Enriched]]:
    if window < 1:
        raise ValueError("window must be >= 1")
    new_state = dict(state)
    out: list[Enriched] = []
    # Input order is Kafka offset order, which is per-symbol event order.
    for e in events:
        prev = new_state.get(e.symbol)
        prices = ((prev.prices if prev else ()) + (e.price,))[-window:]
        volumes = ((prev.volumes if prev else ()) + (e.volume,))[-window:]
        last = prev.prices[-1] if prev else None
        new_state[e.symbol] = Window(prices, volumes)
        out.append(
            Enriched(
                event=e,
                tick_change_pct=(e.price - last) / last * 100 if last else None,
                rolling_avg=sum(prices) / len(prices),
                rolling_volume=sum(volumes),
                window_size=len(prices),
            )
        )
    return new_state, out
