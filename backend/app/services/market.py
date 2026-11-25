"""Instrument rows = PostgreSQL instrument + latest Redis quote. Ranking/screening here, once."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any

from redis.exceptions import RedisError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.redis import QUOTES_KEY, redis_client
from app.models.tables import Instrument
from app.repositories import market_data

log = logging.getLogger(__name__)


STALE_AFTER_S = 60.0  # a quote older than this is flagged stale (prices are never expired, only labelled)


async def load_quotes() -> tuple[dict[str, dict], bool]:
    """(latest quotes, cache reachable). Redis down => no quotes; PostgreSQL history is unaffected."""
    try:
        raw = await redis_client.hgetall(QUOTES_KEY)
    except (RedisError, OSError) as exc:
        log.warning("quote cache unavailable: %s", exc)
        return {}, False
    out = {}
    for symbol, value in raw.items():
        try:
            out[symbol] = json.loads(value)
        except ValueError:
            log.warning("unparseable quote for %s", symbol)
    return out, True


async def get_quotes() -> dict[str, dict]:
    return (await load_quotes())[0]


def cache_status(quotes: dict[str, dict], available: bool, now: datetime) -> dict:
    """Freshness of the quote cache, so a stopped pipeline shows as stale data rather than live prices."""
    last = max((q["processed_at"] for q in quotes.values() if q.get("processed_at")), default=None)
    age = (now - datetime.fromisoformat(last)).total_seconds() if last else None
    return {"available": available, "quotes": len(quotes), "last_update": last,
            "age_s": round(age, 3) if age is not None else None,
            "stale": age is None or age > STALE_AFTER_S}


def row(inst: Instrument, quote: dict | None) -> dict:
    return {
        "id": inst.id, "symbol": inst.symbol, "company_name": inst.company_name, "exchange": inst.exchange,
        "asset_type": inst.asset_type, "currency": inst.currency, "data_mode": inst.data_mode, "quote": quote,
    }


async def instrument_rows(session: AsyncSession, quotes: dict[str, dict] | None = None) -> list[dict]:
    quotes = await get_quotes() if quotes is None else quotes
    return [row(i, quotes.get(i.symbol)) for i in await market_data.list_instruments(session)]


def _q(field: str) -> Callable[[dict], Any]:
    return lambda r: (r["quote"] or {}).get(field)


SORT_KEYS: dict[str, Callable[[dict], Any]] = {
    "symbol": lambda r: r["symbol"],
    "company_name": lambda r: r["company_name"].lower(),
    "price": _q("price"),
    "change": _q("change"),
    "change_pct": _q("change_pct"),
    "volume": _q("volume"),
    "updated": _q("processed_at"),
}


def sort_rows(rows: list[dict], key: str, descending: bool) -> list[dict]:
    """Missing values always sort last, whichever the direction."""
    k = SORT_KEYS[key]
    present = [r for r in rows if k(r) is not None]
    missing = [r for r in rows if k(r) is None]
    return sorted(present, key=k, reverse=descending) + missing


def screen(rows: list[dict], *, search: str | None, asset_type: str | None, exchange: str | None,
           sort: str, order: str, page: int, page_size: int) -> dict:
    # ponytail: filtering in Python is fine for a few hundred instruments; move to SQL + a
    # quote table if the universe grows to thousands.
    if search:
        s = search.lower()
        rows = [r for r in rows if s in r["symbol"].lower() or s in r["company_name"].lower()]
    if asset_type:
        rows = [r for r in rows if r["asset_type"] == asset_type]
    if exchange:
        rows = [r for r in rows if r["exchange"].lower() == exchange.lower()]
    rows = sort_rows(rows, sort, order == "desc")
    start = (page - 1) * page_size
    return {"items": rows[start:start + page_size], "page": page, "page_size": page_size, "total": len(rows)}


def movers(rows: list[dict], kind: str, limit: int) -> list[dict]:
    if kind == "gainers":
        ranked = sort_rows(rows, "change_pct", True)
    elif kind == "losers":
        ranked = sort_rows(rows, "change_pct", False)
    elif kind == "active":
        ranked = sort_rows(rows, "volume", True)
    else:
        raise ValueError(kind)
    key = SORT_KEYS["volume" if kind == "active" else "change_pct"]
    return [r for r in ranked if key(r) is not None][:limit]


def overview(rows: list[dict]) -> dict:
    quotes = [r["quote"] for r in rows if r["quote"]]
    changes = [q.get("change_pct") for q in quotes if q.get("change_pct") is not None]
    return {
        "data_modes": sorted({q["data_mode"] for q in quotes if q.get("data_mode")}),
        "sources": sorted({q["source"] for q in quotes if q.get("source")}),
        "last_update": max((q["processed_at"] for q in quotes if q.get("processed_at")), default=None),
        "instruments_tracked": len(rows),
        "quotes_available": len(quotes),
        "indices": [r for r in rows if r["asset_type"] == "etf"],
        "majors": [r for r in rows if r["asset_type"] == "equity"],
        "gainers": movers(rows, "gainers", 5),
        "losers": movers(rows, "losers", 5),
        "active": movers(rows, "active", 5),
        "breadth": {
            "advancers": sum(c > 0 for c in changes),
            "decliners": sum(c < 0 for c in changes),
            "unchanged": sum(c == 0 for c in changes),
        },
    }
