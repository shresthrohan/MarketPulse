import csv
import io
from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import instrument_or_404
from app.database.session import SessionLocal, get_session
from app.repositories import market_data
from app.repositories.market_data import EventFilter
from app.schemas.requests import HistoryRange
from app.services import history as history_service
from app.services import market

instruments = APIRouter(prefix="/api/instruments", tags=["instruments"])
router = APIRouter(prefix="/api/market", tags=["market"])

SortKey = Literal["symbol", "company_name", "price", "change", "change_pct", "volume", "updated"]
CSV_CAP = 50_000
CSV_COLUMNS = ["event_id", "symbol", "event_timestamp", "price", "volume", "open", "high", "low", "close", "source",
               "data_mode", "processing_status", "attempts", "ingested_at", "processed_at", "latency_ms"]


@instruments.get("")
async def list_instruments(
    search: str | None = Query(None, max_length=64),
    asset_type: Literal["equity", "etf"] | None = None,
    exchange: str | None = Query(None, max_length=32),
    sort: SortKey = "symbol",
    order: Literal["asc", "desc"] = "asc",
    page: int = Query(1, ge=1, le=10_000),
    page_size: int = Query(25, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
):
    rows = await market.instrument_rows(session)
    return market.screen(rows, search=search, asset_type=asset_type, exchange=exchange, sort=sort, order=order,
                         page=page, page_size=page_size)


@instruments.get("/{symbol}")
async def get_instrument(symbol: str, session: AsyncSession = Depends(get_session)):
    inst = await instrument_or_404(session, symbol)
    return market.row(inst, (await market.get_quotes()).get(inst.symbol))


@instruments.get("/{symbol}/history")
async def instrument_history(symbol: str, range: HistoryRange = "1D", session: AsyncSession = Depends(get_session)):
    return await history_service.history(session, await instrument_or_404(session, symbol), range)


@instruments.get("/{symbol}/statistics")
async def instrument_statistics(symbol: str, session: AsyncSession = Depends(get_session)):
    inst = await instrument_or_404(session, symbol)
    closes, volumes = await history_service.daily_frames(session, [inst], 365)
    quote = (await market.get_quotes()).get(inst.symbol)
    return history_service.statistics(inst.symbol, closes[inst.symbol], volumes[inst.symbol], quote)


@router.get("/overview")
async def overview(session: AsyncSession = Depends(get_session)):
    quotes, available = await market.load_quotes()
    out = market.overview(await market.instrument_rows(session, quotes))
    out["cache"] = market.cache_status(quotes, available, datetime.now(UTC))
    return out


async def _movers(kind: str, limit: int, session: AsyncSession) -> dict:
    return {"items": market.movers(await market.instrument_rows(session), kind, limit)}


@router.get("/gainers")
async def gainers(limit: int = Query(10, ge=1, le=50), session: AsyncSession = Depends(get_session)):
    return await _movers("gainers", limit, session)


@router.get("/losers")
async def losers(limit: int = Query(10, ge=1, le=50), session: AsyncSession = Depends(get_session)):
    return await _movers("losers", limit, session)


@router.get("/active")
async def active(limit: int = Query(10, ge=1, le=50), session: AsyncSession = Depends(get_session)):
    return await _movers("active", limit, session)


@router.get("/history")
async def market_history(
    symbol: str | None = Query(None, max_length=10),
    source: str | None = Query(None, max_length=64),
    status: Literal["valid", "retried"] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    min_price: float | None = Query(None, ge=0),
    max_price: float | None = Query(None, ge=0),
    search: str | None = Query(None, max_length=36, pattern=r"^[0-9a-fA-F\-]+$"),
    page: int = Query(1, ge=1, le=1000),
    page_size: int = Query(50, ge=1, le=200),
    format: Literal["json", "csv"] = "json",
    session: AsyncSession = Depends(get_session),
):
    inst_id = (await instrument_or_404(session, symbol)).id if symbol else None
    f = EventFilter(inst_id, source, status, start, end, min_price, max_price, search)
    if format == "csv":
        async def rows():
            # Own session: request-scoped dependencies are closed before a streamed body is sent.
            async with SessionLocal() as stream_session:
                buf = io.StringIO()
                writer = csv.DictWriter(buf, CSV_COLUMNS)
                writer.writeheader()
                async for r in market_data.stream_events(stream_session, f, CSV_CAP):
                    writer.writerow(r)
                    if buf.tell() > 64_000:
                        yield buf.getvalue()
                        buf.seek(0)
                        buf.truncate()
                yield buf.getvalue()

        return StreamingResponse(rows(), media_type="text/csv",
                                 headers={"Content-Disposition": 'attachment; filename="market_events.csv"'})
    items = await market_data.search_events(session, f, page_size + 1, (page - 1) * page_size)
    return {"items": items[:page_size], "page": page, "page_size": page_size, "has_more": len(items) > page_size}
