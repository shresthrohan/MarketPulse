"""Historical bars, per-instrument statistics and analytics inputs (DB -> pandas)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tables import Instrument
from app.repositories import market_data
from app.services import analytics as a

# range -> (stored interval, lookback days, bucket seconds or None, reported interval)
HISTORY_RANGES = {
    "1D": ("1m", 1, None, "1m"),
    "1W": ("1m", 7, 900, "15m"),
    "1M": ("1d", 30, None, "1d"),
    "3M": ("1d", 91, None, "1d"),
    "1Y": ("1d", 365, None, "1d"),
}
ANALYTICS_DAYS = {"1M": 30, "3M": 91, "6M": 182, "1Y": 365}


def _start(days: int, interval: str, now: datetime) -> datetime:
    start = now - timedelta(days=days)
    if interval == "1d":  # whole days: N daily bars including today's partial one
        start = (start + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return start


async def history(session: AsyncSession, inst: Instrument, range_: str) -> dict:
    interval, days, bucket, label = HISTORY_RANGES[range_]
    bars = await market_data.bars(session, inst.id, interval, _start(days, interval, datetime.now(UTC)), bucket)
    return {"symbol": inst.symbol, "range": range_, "interval": label, "bars": bars}


async def daily_frames(session: AsyncSession, instruments: list[Instrument], days: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(closes, volumes): one column per symbol, UTC daily index, NaN where an instrument has no bar."""
    by_id = {i.id: i.symbol for i in instruments}
    rows = await market_data.daily_bars(session, list(by_id), _start(days, "1d", datetime.now(UTC)))
    if not rows:
        empty = pd.DataFrame(columns=[i.symbol for i in instruments], dtype=float)
        return empty, empty
    df = pd.DataFrame(rows, columns=["iid", "t", "close", "volume"])
    df["symbol"] = df["iid"].map(by_id)
    df["t"] = pd.to_datetime(df["t"], utc=True)
    closes = df.pivot(index="t", columns="symbol", values="close").reindex(columns=list(by_id.values()))
    volumes = df.pivot(index="t", columns="symbol", values="volume").reindex(columns=list(by_id.values())).astype(float)
    return closes.sort_index(), volumes.sort_index()


def _iso(ts: pd.Timestamp | None) -> str | None:
    return ts.isoformat() if ts is not None else None


def drawdown_dict(prices: pd.Series) -> dict | None:
    dd = a.max_drawdown(prices)
    if dd is None:
        return None
    return {"value": dd.value, "peak_at": _iso(dd.peak_at), "trough_at": _iso(dd.trough_at), "recovered_at": _iso(dd.recovered_at)}


def statistics(symbol: str, closes: pd.Series, volumes: pd.Series, quote: dict | None) -> dict:
    """Pure: everything derived from the daily series + live quote."""
    closes = closes.dropna()
    last = quote["price"] if quote else (float(closes.iloc[-1]) if len(closes) else None)

    def ret(days: int) -> float | None:
        if last is None or closes.empty:
            return None
        cutoff = closes.index[-1] - pd.Timedelta(days=days)
        past = closes[closes.index <= cutoff]
        return float(last / past.iloc[-1] - 1) if len(past) else None

    def latest(s: pd.Series) -> float | None:
        s = s.dropna()
        return a.clean(float(s.iloc[-1])) if len(s) else None

    log_r = a.log_returns(closes)
    return {
        "symbol": symbol,
        "as_of": datetime.now(UTC).isoformat(),
        "last_price": last,
        "observations": int(len(closes)),
        "high_52w": a.clean(float(closes.max())) if len(closes) else None,
        "low_52w": a.clean(float(closes.min())) if len(closes) else None,
        "avg_volume_30d": a.clean(float(volumes.dropna().tail(30).mean())) if volumes.notna().any() else None,
        "volatility_30d": a.annualized_volatility(log_r.tail(30)),
        "returns": {"1W": ret(7), "1M": ret(30), "3M": ret(91), "1Y": ret(365)},
        "max_drawdown_1y": drawdown_dict(closes),
        "sma_20": latest(a.sma(closes, 20)),
        "sma_50": latest(a.sma(closes, 50)),
        "ema_20": latest(a.ema(closes, 20)),
    }
