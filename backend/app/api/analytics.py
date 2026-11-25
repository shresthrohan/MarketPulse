from typing import Literal

import pandas as pd
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import instrument_or_404, instruments_or_404, same_data_source
from app.database.session import get_session
from app.schemas.requests import AnalyticsRange, SymbolList
from app.services import analytics as a
from app.services.history import ANALYTICS_DAYS, daily_frames, drawdown_dict

router = APIRouter(prefix="/api/analytics", tags=["analytics"])
Method = Literal["simple", "log"]


async def _closes(session: AsyncSession, symbols: str, range_: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    insts = await instruments_or_404(session, symbols)
    same_data_source(insts)
    return await daily_frames(session, insts, ANALYTICS_DAYS[range_])


@router.get("/returns")
async def returns(symbols: SymbolList, range: AnalyticsRange = "3M", method: Method = "simple",
                  session: AsyncSession = Depends(get_session)):
    closes, _ = await _closes(session, symbols, range)
    series, summary = {}, {}
    for sym in closes.columns:
        r = a.returns(closes[sym], method)
        series[sym] = a.to_points(r)
        summary[sym] = {"total_return": a.clean(a.total_return(closes[sym])),
                        "mean_return": a.clean(float(r.mean())) if len(r) else None, "observations": int(len(r))}
    return {"method": method, "range": range, "series": series, "summary": summary}


@router.get("/volatility")
async def volatility(symbols: SymbolList, range: AnalyticsRange = "3M", window: int = Query(20, ge=5, le=120),
                     session: AsyncSession = Depends(get_session)):
    closes, _ = await _closes(session, symbols, range)
    series, latest, full = {}, {}, {}
    for sym in closes.columns:
        r = a.log_returns(closes[sym])
        vol = a.rolling_volatility(r, window)
        series[sym] = a.to_points(vol)
        latest[sym] = a.clean(float(vol.dropna().iloc[-1])) if vol.notna().any() else None
        full[sym] = a.clean(a.annualized_volatility(r))
    return {"window": window, "annualization": a.PERIODS_PER_YEAR, "series": series, "latest": latest, "full_period": full}


@router.get("/correlation")
async def correlation(symbols: SymbolList, range: AnalyticsRange = "3M", method: Method = "simple",
                      session: AsyncSession = Depends(get_session)):
    closes, _ = await _closes(session, symbols, range)
    corr, counts = a.correlation_matrix(closes, method)
    syms = list(closes.columns)
    matrix = [[a.clean(float(corr.loc[x, y])) if x in corr.index and y in corr.columns else None for y in syms] for x in syms]
    obs = [[int(counts.loc[x, y]) if x in counts.index and y in counts.columns else 0 for y in syms] for x in syms]
    return {"symbols": syms, "method": method, "matrix": matrix, "observations": obs}


@router.get("/drawdown")
async def drawdown(symbols: SymbolList, range: AnalyticsRange = "3M", session: AsyncSession = Depends(get_session)):
    closes, _ = await _closes(session, symbols, range)
    return {
        "series": {s: a.to_points(a.drawdown_series(closes[s])) for s in closes.columns},
        "max_drawdown": {s: drawdown_dict(closes[s]) for s in closes.columns},
    }


@router.get("/comparison")
async def comparison(symbols: SymbolList, range: AnalyticsRange = "3M", session: AsyncSession = Depends(get_session)):
    closes, _ = await _closes(session, symbols, range)
    return {
        "base": 100,
        "series": {s: a.to_points(a.normalized_performance(closes[s])) for s in closes.columns},
        "total_return": {s: a.clean(a.total_return(closes[s])) for s in closes.columns},
    }


@router.get("/moving-averages")
async def moving_averages(symbol: str = Query(..., max_length=10), range: AnalyticsRange = "3M",
                          sma: int = Query(20, ge=2, le=200), ema: int = Query(50, ge=2, le=200),
                          session: AsyncSession = Depends(get_session)):
    inst = await instrument_or_404(session, symbol)
    # Load extra history so the averages are defined from the first visible day.
    closes, _ = await daily_frames(session, [inst], ANALYTICS_DAYS[range] + max(sma, ema))
    px = closes[inst.symbol].dropna()
    cutoff = px.index.max() - pd.Timedelta(days=ANALYTICS_DAYS[range] - 1) if len(px) else None

    def visible(s: pd.Series) -> list[dict]:
        return a.to_points(s[s.index >= cutoff]) if cutoff is not None else []

    return {"symbol": inst.symbol, "sma_window": sma, "ema_span": ema, "close": visible(px),
            "sma": visible(a.sma(px, sma)), "ema": visible(a.ema(px, ema))}


@router.get("/volume")
async def volume(symbols: SymbolList, range: AnalyticsRange = "3M", window: int = Query(20, ge=2, le=120),
                 session: AsyncSession = Depends(get_session)):
    _, volumes = await _closes(session, symbols, range)
    return {
        "window": window,
        "series": {s: a.to_points(volumes[s]) for s in volumes.columns},
        "average": {s: a.to_points(a.sma(volumes[s].dropna(), window)) for s in volumes.columns},
    }
