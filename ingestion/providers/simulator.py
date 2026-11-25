from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime

from ingestion.providers.base import Message
from ingestion.simulator.exchange import SimulatedExchange


class SimulatorProvider:
    name = "simulator"
    data_mode = "simulated"

    def __init__(self, exchange: SimulatedExchange, interval_s: float) -> None:
        self._exchange = exchange
        self._interval_s = interval_s
        self.status = "starting"

    async def stream(self) -> AsyncIterator[Message]:
        self.status = "streaming"
        next_tick = time.monotonic()
        while True:
            for message in self._exchange.step(datetime.now(UTC)):
                yield message
            next_tick += self._interval_s
            delay = next_tick - time.monotonic()
            if delay < 0:  # fell behind (e.g. broker back-pressure): resync, don't burst
                next_tick = time.monotonic()
                delay = 0
            await asyncio.sleep(delay)
