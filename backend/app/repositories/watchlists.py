from __future__ import annotations

from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tables import Instrument, Watchlist, WatchlistInstrument


class Conflict(Exception):
    pass


async def list_with_symbols(session: AsyncSession) -> list[tuple[Watchlist, list[str]]]:
    lists = list((await session.execute(select(Watchlist).order_by(Watchlist.created_at, Watchlist.id))).scalars())
    rows = await session.execute(
        select(WatchlistInstrument.watchlist_id, Instrument.symbol)
        .join(Instrument, Instrument.id == WatchlistInstrument.instrument_id)
        .order_by(WatchlistInstrument.added_at, WatchlistInstrument.id)
    )
    members: dict[int, list[str]] = {}
    for wid, symbol in rows:
        members.setdefault(wid, []).append(symbol)
    return [(w, members.get(w.id, [])) for w in lists]


async def count(session: AsyncSession) -> int:
    return (await session.execute(select(func.count()).select_from(Watchlist))).scalar_one()


async def get(session: AsyncSession, watchlist_id: int) -> Watchlist | None:
    return await session.get(Watchlist, watchlist_id)


async def symbols_of(session: AsyncSession, watchlist_id: int) -> list[str]:
    rows = await session.execute(
        select(Instrument.symbol)
        .join(WatchlistInstrument, WatchlistInstrument.instrument_id == Instrument.id)
        .where(WatchlistInstrument.watchlist_id == watchlist_id)
        .order_by(WatchlistInstrument.added_at, WatchlistInstrument.id)
    )
    return list(rows.scalars())


async def _commit(session: AsyncSession) -> None:
    # The UNIQUE constraints decide races between two concurrent requests.
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise Conflict from exc


async def create(session: AsyncSession, name: str) -> Watchlist:
    w = Watchlist(name=name)
    session.add(w)
    await _commit(session)
    await session.refresh(w)
    return w


async def rename(session: AsyncSession, w: Watchlist, name: str) -> Watchlist:
    w.name = name
    await _commit(session)
    return w


async def remove(session: AsyncSession, w: Watchlist) -> None:
    await session.delete(w)
    await session.commit()


async def add_instrument(session: AsyncSession, watchlist_id: int, instrument_id: int) -> None:
    session.add(WatchlistInstrument(watchlist_id=watchlist_id, instrument_id=instrument_id))
    await _commit(session)


async def remove_instrument(session: AsyncSession, watchlist_id: int, instrument_id: int) -> bool:
    result = await session.execute(
        delete(WatchlistInstrument).where(
            WatchlistInstrument.watchlist_id == watchlist_id, WatchlistInstrument.instrument_id == instrument_id
        )
    )
    await session.commit()
    return result.rowcount > 0
