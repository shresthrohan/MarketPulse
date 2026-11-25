"""Financial calculations (pure: pandas in, pandas/floats out). Methods are documented in docs/api.md.

All inputs are price Series indexed by UTC timestamp, sorted ascending. Missing
observations are handled per series: returns are computed on each instrument's
own consecutive observations, and cross-instrument statistics use only dates
where every instrument involved has a value (pairwise for correlation).
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

# The simulated venue trades every calendar day; use 252 for exchange-calendar data.
PERIODS_PER_YEAR = 365
MIN_CORRELATION_OBS = 10


def simple_returns(prices: pd.Series) -> pd.Series:
    """R(t) = (P(t) - P(t-1)) / P(t-1)"""
    p = prices.dropna()
    return (p / p.shift(1) - 1).dropna()


def log_returns(prices: pd.Series) -> pd.Series:
    """r(t) = ln(P(t) / P(t-1))"""
    p = prices.dropna()
    return np.log(p / p.shift(1)).dropna()


def returns(prices: pd.Series, method: str) -> pd.Series:
    if method == "simple":
        return simple_returns(prices)
    if method == "log":
        return log_returns(prices)
    raise ValueError(f"unknown return method {method!r}")


def sma(prices: pd.Series, window: int) -> pd.Series:
    """Arithmetic mean of the last `window` prices; undefined until `window` observations exist."""
    return prices.rolling(window, min_periods=window).mean()


def ema(prices: pd.Series, span: int) -> pd.Series:
    """EMA(t) = a*P(t) + (1-a)*EMA(t-1), a = 2/(span+1), seeded with P(0); undefined for the first span-1 points."""
    return prices.ewm(span=span, adjust=False, min_periods=span).mean()


def rolling_volatility(rets: pd.Series, window: int, annualize: bool = True) -> pd.Series:
    """Sample standard deviation (ddof=1) of the last `window` returns, x sqrt(periods/year) if annualised."""
    vol = rets.rolling(window, min_periods=window).std(ddof=1)
    return vol * math.sqrt(PERIODS_PER_YEAR) if annualize else vol


def annualized_volatility(rets: pd.Series) -> float | None:
    if len(rets) < 2:
        return None
    return float(rets.std(ddof=1) * math.sqrt(PERIODS_PER_YEAR))


def correlation_matrix(prices: pd.DataFrame, method: str = "simple") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pearson correlation of returns, pairwise over dates both instruments traded.

    Returns (correlation, observation counts). Pairs with fewer than
    MIN_CORRELATION_OBS overlapping returns are NaN rather than a noisy number.
    """
    rets = pd.DataFrame({c: returns(prices[c], method) for c in prices.columns})
    corr = rets.corr(method="pearson", min_periods=MIN_CORRELATION_OBS)
    present = rets.notna().astype(int)
    counts = present.T @ present
    return corr, counts


def drawdown_series(prices: pd.Series) -> pd.Series:
    """DD(t) = P(t) / max(P(0..t)) - 1 (0 at a new high, negative below it)."""
    p = prices.dropna()
    return p / p.cummax() - 1


@dataclass(frozen=True, slots=True)
class MaxDrawdown:
    value: float  # e.g. -0.25 for a 25% peak-to-trough decline
    peak_at: pd.Timestamp
    trough_at: pd.Timestamp
    recovered_at: pd.Timestamp | None


def max_drawdown(prices: pd.Series) -> MaxDrawdown | None:
    p = prices.dropna()
    if len(p) < 2:
        return None
    dd = p / p.cummax() - 1
    trough = dd.idxmin()
    peak = p.loc[:trough].idxmax()
    after = p.loc[trough:]
    recovered = after[after >= p.loc[peak]]
    return MaxDrawdown(float(dd.min()), peak, trough, recovered.index[0] if len(recovered) else None)


def normalized_performance(prices: pd.Series, base: float = 100.0) -> pd.Series:
    p = prices.dropna()
    return p / p.iloc[0] * base if len(p) else p


def total_return(prices: pd.Series) -> float | None:
    p = prices.dropna()
    return float(p.iloc[-1] / p.iloc[0] - 1) if len(p) >= 2 else None


def to_points(s: pd.Series) -> list[dict]:
    """Series -> [{"time": unix seconds, "value": float}] without NaN/inf (not valid JSON)."""
    s = s.replace([np.inf, -np.inf], np.nan).dropna()
    return [{"time": int(ts.timestamp()), "value": float(v)} for ts, v in s.items()]


def clean(x: float | None) -> float | None:
    return None if x is None or not math.isfinite(x) else float(x)
