"""Provider contract. Add a provider by implementing `stream()` and registering it in main.build_provider."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

# dict = parsed event in the documented schema; bytes = payload forwarded as-is
# so the processor can record it as malformed instead of it vanishing here.
Message = dict | bytes


class MarketDataProvider(Protocol):
    name: str
    data_mode: str  # "simulated" | "live" | "delayed"
    status: str  # human-readable connection state, reported in the heartbeat

    def stream(self) -> AsyncIterator[Message]: ...
