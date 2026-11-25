"""Seed instruments and their history.

Run by the `migrate` service after `alembic upgrade head`, on every `docker compose up`:
instruments are upserted every time; history is backfilled only for instruments that have none.

- Simulated equities/ETFs (config/instruments.json): a deterministic year generated backwards from
  each base_price at "now" (the live simulator starts there, so the seam has no price jump).
  Only when the simulator feeds them; with Finnhub they get no simulated history at all.
- Binance crypto pairs (config/binance.json, when PROVIDERS includes binance): real history from
  Binance's public klines (1d for a year, 1m for 7 days). Never simulated.

instruments.data_mode is set once (COALESCE) and never flipped, so an instrument never mixes
simulated and real data; the processor rejects events of the other kind.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import groupby
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

from ingestion.simulator.model import (
    SECONDS_PER_YEAR,
    InstrumentSpec,
    correlated_shocks,
    gbm_step_back,
    load_instruments,
)

log = logging.getLogger("seed")

INTRADAY_DAYS = 7  # 6 full UTC days + today so far, in 1-minute bars
TOTAL_DAYS = 365  # daily bars per instrument, including today's partial one
MINUTE_SUBSTEPS = 6  # 10 s each
DAY_SUBSTEPS = 24  # hourly
TICKS_PER_MINUTE = 72  # live sim: 0.5 s steps * 0.6 tick probability


@dataclass(frozen=True, slots=True)
class Bar:
    symbol: str
    interval: str
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    first_at: datetime
    last_at: datetime


def _px(x: float) -> float:
    # Rounding is monotonic, so rounding o/h/l/c separately keeps low <= o,c <= high.
    return max(round(x, 2), 0.01)


def _walk_back(rng: random.Random, specs: list[InstrumentSpec], prices: list[float], steps: int, dt: float) -> list[list[float]]:
    """Each instrument's path from `prices` going `steps` steps back in time (latest first)."""
    loadings = [s.factor_loading for s in specs]
    paths = [[p] for p in prices]
    for _ in range(steps):
        # One shock draw per time step across all instruments keeps cross-correlation realistic.
        for path, spec, z in zip(paths, specs, correlated_shocks(rng, loadings), strict=True):
            path.append(gbm_step_back(path[-1], spec.volatility, dt, z))
    return paths


def _bar(symbol: str, interval: str, start: datetime, path: list[float], volume: int, length: timedelta) -> Bar:
    return Bar(symbol, interval, start, _px(path[-1]), _px(max(path)), _px(min(path)), _px(path[0]), volume, start, start + length)


def _daily_from_minutes(minutes: list[Bar]) -> list[Bar]:
    out = []
    for _, group in groupby(minutes, key=lambda b: b.start.date()):
        g = list(group)
        day = g[0].start.replace(hour=0, minute=0)
        out.append(Bar(g[0].symbol, "1d", day, g[0].open, max(b.high for b in g), min(b.low for b in g), g[-1].close,
                       sum(b.volume for b in g), g[0].first_at, g[-1].last_at))
    return out


def generate(specs: list[InstrumentSpec], now: datetime, rng: random.Random) -> tuple[dict[str, list[Bar]], dict[str, list[Bar]]]:
    """(1m bars, 1d bars) per symbol, oldest first. Pure: clock and randomness are passed in."""
    anchor = now.astimezone(UTC).replace(second=0, microsecond=0)
    start_1m = anchor.replace(hour=0, minute=0) - timedelta(days=INTRADAY_DAYS - 1)
    n_minutes = (anchor - start_1m) // timedelta(minutes=1)

    prices = [s.base_price for s in specs]
    minute: dict[str, list[Bar]] = {s.symbol: [] for s in specs}
    for m in range(n_minutes - 1, -1, -1):
        start = start_1m + timedelta(minutes=m)
        paths = _walk_back(rng, specs, prices, MINUTE_SUBSTEPS, 10 / SECONDS_PER_YEAR)
        for spec, path in zip(specs, paths, strict=True):
            volume = int(spec.tick_volume * TICKS_PER_MINUTE * rng.lognormvariate(0, 0.35))
            minute[spec.symbol].append(_bar(spec.symbol, "1m", start, path, volume, timedelta(seconds=59)))
        prices = [p[-1] for p in paths]
    for bars in minute.values():
        bars.reverse()

    daily = {sym: _daily_from_minutes(bars) for sym, bars in minute.items()}
    older: dict[str, list[Bar]] = {s.symbol: [] for s in specs}
    # 24/7 venue: each earlier day closes where the following day opens.
    for d in range(1, TOTAL_DAYS - len(daily[specs[0].symbol]) + 1):
        day = start_1m - timedelta(days=d)
        paths = _walk_back(rng, specs, prices, DAY_SUBSTEPS, 3600 / SECONDS_PER_YEAR)
        for spec, path in zip(specs, paths, strict=True):
            volume = int(spec.tick_volume * TICKS_PER_MINUTE * 1440 * rng.lognormvariate(0, 0.25))
            older[spec.symbol].append(_bar(spec.symbol, "1d", day, path, volume, timedelta(seconds=86399)))
        prices = [p[-1] for p in paths]
    return minute, {sym: older[sym][::-1] + daily[sym] for sym in daily}


UPSERT_INSTRUMENT = """
INSERT INTO instruments (symbol, company_name, exchange, asset_type, currency, data_mode) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (symbol) DO UPDATE SET company_name = EXCLUDED.company_name, exchange = EXCLUDED.exchange,
                                   asset_type = EXCLUDED.asset_type, currency = EXCLUDED.currency,
                                   data_mode = COALESCE(instruments.data_mode, EXCLUDED.data_mode)
"""
HAS_BARS = """SELECT i.symbol FROM instruments i WHERE i.symbol = ANY($1)
              AND EXISTS (SELECT 1 FROM historical_aggregates h WHERE h.instrument_id = i.id)"""
KLINE_TIMEOUT_S = 10


def parse_providers(raw: str) -> set[str]:
    return {p.strip() for p in raw.split(",") if p.strip()}


def equities_mode(providers: set[str], has_finnhub_key: bool) -> str:
    """Mirrors ingestion's provider rules: finnhub without a key falls back to the simulator."""
    if has_finnhub_key and "finnhub_ws" in providers:
        return "live"
    if has_finnhub_key and "finnhub_rest" in providers:
        return "delayed"
    return "simulated"


def klines_to_bars(symbol: str, interval: str, rows: list[list]) -> list[Bar]:
    """Binance kline rows [open_ms, o, h, l, c, volume, close_ms, ...] -> bars. Pure."""
    out = []
    for r in rows:
        start = datetime.fromtimestamp(r[0] / 1000, UTC)
        out.append(Bar(symbol, interval, start, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]),
                       start, datetime.fromtimestamp(r[6] / 1000, UTC)))
    return out


def fetch_klines(base: str, symbol: str, interval: str, start_ms: int, end_ms: int) -> list[list]:
    """Public klines (no key), paged 1000 at a time. Raises on network errors."""
    rows: list[list] = []
    cursor = start_ms
    while cursor < end_ms:
        url = f"{base}/api/v3/klines?symbol={symbol}&interval={interval}&startTime={cursor}&endTime={end_ms}&limit=1000"
        with urlopen(url, timeout=KLINE_TIMEOUT_S) as r:  # fixed https base URL from configuration
            page = json.loads(r.read())
        if not page:
            break
        rows += page
        cursor = page[-1][0] + 1
    return rows


def binance_history(base: str, symbols: list[str], now: datetime) -> dict[str, list[Bar]]:
    """Real 1d (a year) and 1m (7 UTC days) bars per symbol; a symbol that fails is skipped (logged)."""
    end_ms = int(now.timestamp() * 1000)
    day0 = now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    out: dict[str, list[Bar]] = {}
    for symbol in symbols:
        try:
            daily = fetch_klines(base, symbol, "1d", int((day0 - timedelta(days=TOTAL_DAYS - 1)).timestamp() * 1000), end_ms)
            minute = fetch_klines(base, symbol, "1m", int((day0 - timedelta(days=INTRADAY_DAYS - 1)).timestamp() * 1000), end_ms)
        except (URLError, OSError, ValueError) as exc:
            log.warning("binance history for %s unavailable, starting without it: %s", symbol, exc)
            continue
        out[symbol] = klines_to_bars(symbol, "1d", daily) + klines_to_bars(symbol, "1m", minute)
    return out


BAR_COLUMNS = ["instrument_id", "interval", "timestamp", "open", "high", "low", "close", "volume", "first_event_at", "last_event_at"]


def _records(ids: dict[str, int], bars: list[Bar]) -> list[tuple]:
    return [(ids[b.symbol], b.interval, b.start, Decimal(str(b.open)), Decimal(str(b.high)), Decimal(str(b.low)),
             Decimal(str(b.close)), Decimal(str(b.volume)), b.first_at, b.last_at) for b in bars]


async def seed(dsn: str, specs: list[InstrumentSpec], crypto: list[dict], equity_mode: str, seed_value: int,
               binance_rest: str) -> None:
    import asyncpg  # imported here so the pure generator (and its tests) need no database driver

    started = time.monotonic()
    conn = await asyncpg.connect(dsn)
    try:
        # Network fetches happen before the transaction so it stays short; the insert re-checks under the lock.
        crypto_symbols = [c["symbol"] for c in crypto]
        missing = set(crypto_symbols) - {r["symbol"] for r in await conn.fetch(HAS_BARS, crypto_symbols)}
        real = binance_history(binance_rest, sorted(missing), datetime.now(UTC)) if missing else {}
        async with conn.transaction():
            # Serialises concurrent `compose up`s: the second waits, then sees the seeded table.
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext('marketpulse_seed'))")
            await conn.executemany(UPSERT_INSTRUMENT, [(s.symbol, s.company_name, s.exchange, s.asset_type, s.currency,
                                                        equity_mode) for s in specs])
            await conn.executemany(UPSERT_INSTRUMENT, [(c["symbol"], c["company_name"], c["exchange"], c["asset_type"],
                                                        c["currency"], "live") for c in crypto])
            log.info("instruments upserted: %d equities/ETFs (%s), %d binance", len(specs), equity_mode, len(crypto))
            ids = dict(await conn.fetch("SELECT symbol, id FROM instruments"))
            modes = dict(await conn.fetch("SELECT symbol, data_mode FROM instruments WHERE symbol = ANY($1)",
                                          [s.symbol for s in specs]))
            counts: dict[str, int] = {}
            equities_seeded = await conn.fetch(HAS_BARS, [s.symbol for s in specs])
            if not equities_seeded and set(modes.values()) == {"simulated"}:
                minute, daily = generate(specs, datetime.now(UTC), random.Random(seed_value))
                for interval, by_symbol in (("1m", minute), ("1d", daily)):
                    records = _records(ids, [b for bars in by_symbol.values() for b in bars])
                    await conn.copy_records_to_table("historical_aggregates", records=records, columns=BAR_COLUMNS)
                    counts[f"simulated {interval}"] = len(records)
            still_missing = set(crypto_symbols) - {r["symbol"] for r in await conn.fetch(HAS_BARS, crypto_symbols)}
            bars = [b for sym, sym_bars in real.items() if sym in still_missing for b in sym_bars]
            if bars:
                await conn.copy_records_to_table("historical_aggregates", records=_records(ids, bars), columns=BAR_COLUMNS)
                counts["binance klines"] = len(bars)
        log.info("seeded historical_aggregates: %s rows in %.1fs", counts or "nothing new", time.monotonic() - started)
    finally:
        await conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    dsn = os.environ.get("DATABASE_URL", "postgresql://marketpulse@postgres:5432/marketpulse").replace("+asyncpg", "")
    config = Path(__file__).resolve().parents[1] / "config"
    path = os.environ.get("INSTRUMENTS_FILE", str(config / "instruments.json"))
    providers = parse_providers(os.environ.get("PROVIDERS") or os.environ.get("PROVIDER") or "simulator")
    crypto_path = os.environ.get("BINANCE_INSTRUMENTS_FILE", str(config / "binance.json"))
    crypto = json.loads(Path(crypto_path).read_text(encoding="utf-8")) if "binance" in providers else []
    mode = equities_mode(providers, bool(os.environ.get("FINNHUB_API_KEY")))
    asyncio.run(seed(dsn, load_instruments(path), crypto, mode, int(os.environ.get("SEED_RANDOM_SEED", "42")),
                     os.environ.get("BINANCE_REST_URL", "https://api.binance.com").rstrip("/")))


if __name__ == "__main__":
    main()  # an exception exits non-zero, which stops `compose up` before services start
