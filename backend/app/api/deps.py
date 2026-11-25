from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tables import Instrument
from app.repositories import market_data


async def instrument_or_404(session: AsyncSession, symbol: str) -> Instrument:
    inst = await market_data.get_instrument(session, symbol)
    if inst is None:
        raise HTTPException(404, f"unknown symbol {symbol.upper()}")
    return inst


def same_data_source(instruments: list[Instrument]) -> None:
    """Cross-instrument statistics over simulated and real series would be meaningless, so refuse them."""
    kinds = {"simulated" if i.data_mode == "simulated" else "real" for i in instruments if i.data_mode}
    if len(kinds) > 1:
        sim = [i.symbol for i in instruments if i.data_mode == "simulated"]
        real = [i.symbol for i in instruments if i.data_mode and i.data_mode != "simulated"]
        raise HTTPException(422, f"cannot combine simulated ({', '.join(sim)}) and live ({', '.join(real)}) instruments "
                                 "in one analysis; select instruments with the same data source")


async def instruments_or_404(session: AsyncSession, symbols_csv: str, max_n: int = 10) -> list[Instrument]:
    symbols = list(dict.fromkeys(s.strip().upper() for s in symbols_csv.split(",") if s.strip()))
    if not 1 <= len(symbols) <= max_n:
        raise HTTPException(422, f"provide 1-{max_n} symbols")
    found = await market_data.instruments_by_symbol(session, symbols)
    missing = [s for s in symbols if s not in found]
    if missing:
        raise HTTPException(404, f"unknown symbol(s): {', '.join(missing)}")
    return [found[s] for s in symbols]
