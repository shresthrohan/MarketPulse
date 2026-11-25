from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import instrument_or_404
from app.core.config import settings
from app.database.session import get_session
from app.models.tables import Watchlist
from app.repositories import market_data
from app.repositories import watchlists as repo
from app.schemas.requests import WatchlistName, WatchlistSymbol
from app.services import market

router = APIRouter(prefix="/api/watchlists", tags=["watchlists"])


async def _rows(session: AsyncSession, symbols: list[str]) -> list[dict]:
    quotes = await market.get_quotes()
    insts = await market_data.instruments_by_symbol(session, symbols)
    return [market.row(insts[s], quotes.get(s)) for s in symbols if s in insts]


async def _out(session: AsyncSession, w: Watchlist, symbols: list[str] | None = None) -> dict:
    symbols = await repo.symbols_of(session, w.id) if symbols is None else symbols
    return {"id": w.id, "name": w.name, "created_at": w.created_at, "instruments": await _rows(session, symbols)}


async def _get_or_404(session: AsyncSession, watchlist_id: int) -> Watchlist:
    w = await repo.get(session, watchlist_id)
    if w is None:
        raise HTTPException(404, "watchlist not found")
    return w


@router.get("")
async def list_watchlists(session: AsyncSession = Depends(get_session)):
    return {"items": [await _out(session, w, syms) for w, syms in await repo.list_with_symbols(session)]}


@router.post("", status_code=201)
async def create_watchlist(body: WatchlistName, session: AsyncSession = Depends(get_session)):
    # Watchlists are shared and unauthenticated: bound them so a public deployment cannot be filled up.
    if await repo.count(session) >= settings.watchlist_max:
        raise HTTPException(409, f"watchlist limit reached ({settings.watchlist_max}); delete one first")
    try:
        w = await repo.create(session, body.name)
    except repo.Conflict:
        raise HTTPException(409, f"a watchlist named {body.name!r} already exists") from None
    return await _out(session, w, [])


@router.put("/{watchlist_id}")
async def rename_watchlist(watchlist_id: int, body: WatchlistName, session: AsyncSession = Depends(get_session)):
    w = await _get_or_404(session, watchlist_id)
    try:
        w = await repo.rename(session, w, body.name)
    except repo.Conflict:
        raise HTTPException(409, f"a watchlist named {body.name!r} already exists") from None
    return await _out(session, w)


@router.delete("/{watchlist_id}", status_code=204)
async def delete_watchlist(watchlist_id: int, session: AsyncSession = Depends(get_session)):
    await repo.remove(session, await _get_or_404(session, watchlist_id))
    return Response(status_code=204)


@router.post("/{watchlist_id}/instruments", status_code=201)
async def add_instrument(watchlist_id: int, body: WatchlistSymbol, session: AsyncSession = Depends(get_session)):
    w = await _get_or_404(session, watchlist_id)
    inst = await instrument_or_404(session, body.symbol)
    symbol = inst.symbol  # read before commit: a rollback expires ORM attributes
    try:
        await repo.add_instrument(session, w.id, inst.id)
    except repo.Conflict:
        raise HTTPException(409, f"{symbol} is already in this watchlist") from None
    return await _out(session, w)


@router.delete("/{watchlist_id}/instruments/{symbol}", status_code=204)
async def remove_instrument(watchlist_id: int, symbol: str, session: AsyncSession = Depends(get_session)):
    w = await _get_or_404(session, watchlist_id)
    inst = await instrument_or_404(session, symbol)
    if not await repo.remove_instrument(session, w.id, inst.id):
        raise HTTPException(404, f"{inst.symbol} is not in this watchlist")
    return Response(status_code=204)
