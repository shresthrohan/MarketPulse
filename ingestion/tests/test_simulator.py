import json
import statistics
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path

from ingestion.producers.kafka_producer import encode
from ingestion.providers.finnhub import SessionBook, map_quote, map_trade
from ingestion.simulator.exchange import SimulatedExchange
from ingestion.simulator.model import gbm_step, gbm_step_back, load_instruments

SPECS = load_instruments(Path(__file__).resolve().parents[2] / "config" / "instruments.json")
T0 = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)


def run(seed, steps=200, **kw):
    ex = SimulatedExchange(SPECS, seed=seed, **kw)
    return [m for i in range(steps) for m in ex.step(T0 + timedelta(seconds=i / 2))]


def test_same_seed_produces_identical_events():
    assert run(7, fault_rate=0.05) == run(7, fault_rate=0.05)


def test_different_seeds_diverge():
    assert run(1) != run(2)


def test_a_restarted_seeded_run_never_reuses_event_ids():
    # Same seed, later clock (= a restart): prices repeat by design, IDs must not.
    first = {e["event_id"] for e in run(7) if isinstance(e, dict)}
    ex = SimulatedExchange(SPECS, seed=7)
    later = T0 + timedelta(hours=5)
    second = {e["event_id"] for i in range(200) for e in ex.step(later + timedelta(seconds=i / 2)) if isinstance(e, dict)}
    assert first and second and not first & second


def test_unseeded_instances_never_share_event_ids():
    a, b = SimulatedExchange(SPECS), SimulatedExchange(SPECS)
    ids_a = {e["event_id"] for e in a.step(T0) if isinstance(e, dict)}
    ids_b = {e["event_id"] for e in b.step(T0) if isinstance(e, dict)}
    assert not ids_a & ids_b


def test_clean_events_have_consistent_ohlc_and_positive_values():
    events = run(3, steps=500)
    assert events and all(isinstance(e, dict) for e in events)
    for e in events:
        assert e["price"] > 0 and e["volume"] > 0
        assert e["low"] <= e["open"] <= e["high"]
        assert e["low"] <= e["close"] == e["price"] <= e["high"]
        assert e["data_mode"] == "simulated" and e["source"] == "simulator"
    assert len({e["event_id"] for e in events}) == len(events)


def test_faults_are_injected_at_roughly_the_configured_rate():
    events = run(11, steps=2000, fault_rate=0.1)
    bad = [e for e in events if isinstance(e, bytes) or e.get("price") is None or e.get("symbol") == "ZZZZ"
           or "volume" not in e or e["price"] < 0 or e.get("volume", 0) < 0 or e["high"] < e["low"]]
    assert 0.02 < len(bad) / len(events) < 0.2


def test_session_rolls_over_at_utc_midnight():
    ex = SimulatedExchange(SPECS[:1], seed=5, tick_probability=1.0, speed=10_000)
    day1 = [ex.step(datetime(2026, 1, 5, 23, 59, s, tzinfo=UTC))[0] for s in range(50, 60)]
    day2 = ex.step(datetime(2026, 1, 6, 0, 0, 1, tzinfo=UTC))[0]
    assert all(e["open"] == day1[0]["price"] for e in day1)
    assert day2["open"] == day2["price"] and day2["high"] == day2["low"] == day2["price"]


def test_etfs_are_more_correlated_with_each_other_than_with_low_loading_stocks():
    ex = SimulatedExchange(SPECS, seed=9, tick_probability=1.0, speed=3600)
    series = {s: [] for s in ("SPY", "QQQ", "JNJ")}
    for i in range(3000):
        for e in ex.step(T0 + timedelta(seconds=i)):
            if e["symbol"] in series:
                series[e["symbol"]].append(e["price"])
    rets = {s: [b / a - 1 for a, b in pairwise(p)] for s, p in series.items()}
    assert statistics.correlation(rets["SPY"], rets["QQQ"]) > statistics.correlation(rets["SPY"], rets["JNJ"]) + 0.3


def test_backward_step_inverts_forward_step():
    assert abs(gbm_step_back(gbm_step(100.0, 0.3, 0.01, 1.2), 0.3, 0.01, 1.2) - 100.0) < 1e-9


def test_initial_prices_resume_the_walk():
    ex = SimulatedExchange(SPECS[:1], seed=1, tick_probability=1.0, initial_prices={SPECS[0].symbol: 999.0})
    assert abs(ex.step(T0)[0]["price"] - 999.0) < 5


def test_encode_keys_by_symbol_and_stamps_ingestion_time():
    key, value = encode({"symbol": "AAPL", "price": 1}, T0)
    assert key == b"AAPL" and json.loads(value)["ingested_at"].startswith("2026-01-05T14:30:00")
    assert encode(b"garbage", T0) == (None, b"garbage")


def test_finnhub_trade_mapping_tracks_session_and_ids_are_stable():
    book = SessionBook()
    t = {"s": "AAPL", "p": 200.5, "t": 1767623400000, "v": 10}
    e1 = map_trade(t, book)
    e2 = map_trade({"s": "AAPL", "p": 199.0, "t": 1767623401000, "v": 5}, book)
    assert e1["data_mode"] == "live" and e2["low"] == 199.0 and e2["high"] == 200.5 and e2["open"] == 200.5
    assert map_trade(t, SessionBook())["event_id"] == e1["event_id"]


def test_finnhub_quote_mapping():
    e = map_quote("MSFT", {"c": 410.0, "h": 412.0, "l": 405.0, "o": 406.0, "t": 1767623400})
    assert e["data_mode"] == "delayed" and e["volume"] == 0 and e["low"] <= e["price"] <= e["high"]
