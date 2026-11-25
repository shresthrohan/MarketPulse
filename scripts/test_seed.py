"""Pure checks of the backfill generator; no database needed."""

import random
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path

from seed_data import INTRADAY_DAYS, TOTAL_DAYS, generate

from ingestion.simulator.model import load_instruments

SPECS = load_instruments(Path(__file__).resolve().parents[1] / "config" / "instruments.json")
NOW = datetime(2026, 11, 25, 14, 37, 21, 500000, tzinfo=UTC)
ANCHOR = datetime(2026, 11, 25, 14, 37, tzinfo=UTC)
MINUTE, DAILY = generate(SPECS, NOW, random.Random(42))


def test_history_ends_at_base_price_at_anchor():
    for s in SPECS:
        last = MINUTE[s.symbol][-1]
        assert last.start == ANCHOR - timedelta(minutes=1)
        assert last.close == round(s.base_price, 2)
        assert DAILY[s.symbol][-1].close == last.close


def test_shapes():
    first = datetime(2026, 11, 19, tzinfo=UTC)
    for s in SPECS:
        assert len(MINUTE[s.symbol]) == (ANCHOR - first) // timedelta(minutes=1)
        assert MINUTE[s.symbol][0].start == first
        days = DAILY[s.symbol]
        assert len(days) == TOTAL_DAYS
        assert [d.start for d in days] == [datetime(2026, 11, 25, tzinfo=UTC) - timedelta(days=i) for i in range(TOTAL_DAYS)][::-1]


def test_ohlc_consistent():
    for bars in (*MINUTE.values(), *DAILY.values()):
        for b in bars:
            assert 0.01 <= b.low <= min(b.open, b.close) <= max(b.open, b.close) <= b.high, b
            assert b.volume > 0 and b.first_at <= b.last_at


def test_recent_daily_bars_aggregate_minutes():
    for s in SPECS:
        for day in DAILY[s.symbol][-INTRADAY_DAYS:]:
            ms = [m for m in MINUTE[s.symbol] if m.start.date() == day.start.date()]
            assert (day.open, day.high, day.low, day.close, day.volume, day.first_at, day.last_at) == (
                ms[0].open, max(m.high for m in ms), min(m.low for m in ms), ms[-1].close,
                sum(m.volume for m in ms), ms[0].first_at, ms[-1].last_at,
            )


def test_continuity():
    for bars in (*MINUTE.values(), *DAILY.values()):
        for prev, nxt in pairwise(bars):
            assert prev.close == nxt.open


def test_deterministic():
    assert generate(SPECS, NOW, random.Random(42)) == (MINUTE, DAILY)
    assert generate(SPECS, NOW, random.Random(7)) != (MINUTE, DAILY)


def test_binance_klines_become_real_bars():
    from seed_data import klines_to_bars

    row = [1795478400000, "61270.92", "61682.01", "59260.00", "59832.00", "19707.39868", 1795564799999, "0", 1, "0", "0", "0"]
    (bar,) = klines_to_bars("BTCUSDT", "1d", [row])
    assert bar.start == datetime(2026, 11, 24, tzinfo=UTC) and bar.last_at.date() == bar.start.date()
    assert (bar.open, bar.high, bar.low, bar.close, bar.volume) == (61270.92, 61682.01, 59260.0, 59832.0, 19707.39868)


def test_equities_get_simulated_history_only_when_the_simulator_feeds_them():
    from seed_data import equities_mode, parse_providers

    assert equities_mode(parse_providers("simulator, binance"), has_finnhub_key=False) == "simulated"
    assert equities_mode(parse_providers("finnhub_ws,binance"), has_finnhub_key=True) == "live"
    assert equities_mode(parse_providers("finnhub_rest"), has_finnhub_key=True) == "delayed"
    assert equities_mode(parse_providers("finnhub_ws"), has_finnhub_key=False) == "simulated"  # falls back like ingestion
