"""Per-symbol UTC-day session open/high/low, built from what a live feed has observed."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(slots=True)
class _Session:
    day: date
    open: float
    high: float
    low: float


class SessionBook:
    """Tracks per-symbol session open/high/low from observed trades or bars (UTC day)."""

    def __init__(self) -> None:
        self._sessions: dict[str, _Session] = {}

    def apply(self, symbol: str, price: float, ts: datetime) -> _Session:
        return self.apply_bar(symbol, price, price, price, price, ts)

    def apply_bar(self, symbol: str, open: float, high: float, low: float, close: float, ts: datetime) -> _Session:
        """The day's first bar sets the session open. High/low take every bar price, so
        low <= open, close <= high holds even for a bar that is itself inconsistent."""
        s = self._sessions.get(symbol)
        if s is None or s.day != ts.date():
            s = _Session(ts.date(), open, open, open)
            self._sessions[symbol] = s
        s.high = max(s.high, open, high, low, close)
        s.low = min(s.low, open, high, low, close)
        return s
