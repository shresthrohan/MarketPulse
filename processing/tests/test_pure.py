import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from processing.aggregators.bars import aggregate, bucket_start
from processing.transformers.enrich import enrich
from processing.validators.event_validator import (
    ErrorType,
    Rejected,
    Rules,
    ValidEvent,
    validate,
)

NOW = datetime(2026, 3, 2, 15, 0, 0, tzinfo=UTC)
RULES = Rules(known_symbols=frozenset({"AAPL", "MSFT"}))


def event(**over):
    e = {
        "event_id": str(uuid.uuid4()), "symbol": "AAPL", "timestamp": NOW.isoformat(), "price": 225.5,
        "volume": 1200, "open": 224.0, "high": 226.0, "low": 223.5, "close": 225.5,
        "source": "simulator", "data_mode": "simulated",
    }
    e.update(over)
    return e


def check(doc) -> ValidEvent | Rejected:
    raw = doc if isinstance(doc, bytes) else json.dumps(doc).encode()
    return validate(raw, RULES, NOW)


def test_a_well_formed_event_is_accepted_and_normalised():
    r = check(event(symbol=" aapl ", timestamp="2026-03-02T16:00:00+01:00"))
    assert isinstance(r, ValidEvent)
    assert r.symbol == "AAPL" and r.timestamp == NOW and r.timestamp.tzinfo == UTC


@pytest.mark.parametrize(
    ("doc", "kind"),
    [
        (b"{not json", ErrorType.MALFORMED_JSON),
        (b"[1,2]", ErrorType.MALFORMED_JSON),
        (b"[" * 100_000, ErrorType.MALFORMED_JSON),
        ({k: v for k, v in event().items() if k != "volume"}, ErrorType.MISSING_FIELD),
        (event(event_id="not-a-uuid"), ErrorType.INVALID_FIELD),
        (event(data_mode="live-ish"), ErrorType.INVALID_FIELD),
        (event(price=None), ErrorType.NULL_PRICE),
        (event(price=-5, close=-5), ErrorType.NEGATIVE_PRICE),
        (event(price=0), ErrorType.INVALID_PRICE),
        (event(price="225.5"), ErrorType.INVALID_PRICE),
        (event(price=True), ErrorType.INVALID_PRICE),
        (event(volume=-1), ErrorType.INVALID_VOLUME),
        (event(volume="10"), ErrorType.INVALID_VOLUME),
        (event(volume=float("nan")), ErrorType.INVALID_VOLUME),
        (event(high=223.0, low=226.0), ErrorType.INVALID_OHLC),
        (event(price=230.0), ErrorType.INVALID_OHLC),
        (event(timestamp="2026-03-02 15:00:00"), ErrorType.INVALID_TIMESTAMP),
        (event(timestamp="yesterday"), ErrorType.INVALID_TIMESTAMP),
        (event(timestamp=(NOW + timedelta(minutes=1)).isoformat()), ErrorType.INVALID_TIMESTAMP),
        (event(timestamp=(NOW + timedelta(hours=1)).isoformat()), ErrorType.INVALID_TIMESTAMP),
        (event(timestamp=(NOW - timedelta(days=2)).isoformat()), ErrorType.INVALID_TIMESTAMP),
        (event(symbol="ZZZZ"), ErrorType.UNKNOWN_SYMBOL),
    ],
)
def test_each_data_quality_rule_rejects_with_its_error_type(doc, kind):
    r = check(doc)
    assert isinstance(r, Rejected) and r.error_type is kind


def test_rejects_keep_identity_and_raw_payload_for_the_error_log():
    doc = event(price=None)
    r = check(doc)
    assert r.event_id == doc["event_id"] and r.symbol == "AAPL" and '"price": null' in r.raw


def test_simulator_fault_events_are_all_rejected_and_clean_ones_accepted():
    from pathlib import Path

    from ingestion.simulator.exchange import SimulatedExchange
    from ingestion.simulator.model import load_instruments

    specs = load_instruments(Path(__file__).resolve().parents[2] / "config" / "instruments.json")
    rules = Rules(known_symbols=frozenset(s.symbol for s in specs))
    ex = SimulatedExchange(specs, seed=3, fault_rate=0.2)
    seen: set = set()
    accepted = rejected = 0
    for i in range(300):
        for m in ex.step(NOW + timedelta(seconds=i / 10)):
            raw = m if isinstance(m, bytes) else json.dumps(m).encode()
            r = validate(raw, rules, NOW + timedelta(seconds=i / 10))
            if isinstance(r, ValidEvent) and r.event_id not in seen:
                seen.add(r.event_id)
                accepted += 1
            else:
                rejected += 1
    assert accepted > 1000 and rejected > 100


def test_enrichment_computes_tick_change_and_rolling_window_without_mutating_input():
    a = [ValidEvent(uuid.uuid4(), "AAPL", NOW, p, 10, p, p, p, p, "s", "simulated", None) for p in (100.0, 110.0, 99.0)]
    state0: dict = {}
    s1, out = enrich(state0, a, window=2)
    assert state0 == {}
    assert out[0].tick_change_pct is None and out[1].tick_change_pct == pytest.approx(10.0)
    assert out[2].rolling_avg == pytest.approx((110 + 99) / 2) and out[2].rolling_volume == 20 and out[2].window_size == 2
    _, again = enrich(s1, a[:1], window=2)
    assert again[0].tick_change_pct == pytest.approx((100 - 99) / 99 * 100)


def test_bars_take_open_from_earliest_close_from_latest_and_sum_volume():
    t = NOW.replace(second=10)
    evs = [
        ValidEvent(uuid.uuid4(), "AAPL", t + timedelta(seconds=20), 102.0, 5, 1, 999, 1, 1, "s", "simulated", None),
        ValidEvent(uuid.uuid4(), "AAPL", t, 100.0, 3, 1, 999, 1, 1, "s", "simulated", None),
        ValidEvent(uuid.uuid4(), "AAPL", t + timedelta(seconds=10), 105.0, 2, 1, 999, 1, 1, "s", "simulated", None),
    ]
    bars = {b.interval: b for b in aggregate(evs)}
    m = bars["1m"]
    assert (m.open, m.high, m.low, m.close, m.volume) == (100.0, 105.0, 100.0, 102.0, 10)
    assert m.start == NOW.replace(second=0) and bars["1d"].start == NOW.replace(hour=0, minute=0, second=0)


def test_bucket_start_floors_to_interval():
    assert bucket_start(datetime(2026, 1, 1, 12, 34, 56, tzinfo=UTC), "1m") == datetime(2026, 1, 1, 12, 34, tzinfo=UTC)


def _random_value(rng, depth=0):
    choices = [
        lambda: None, lambda: True, lambda: rng.randint(-10**6, 10**6), lambda: 10 ** rng.randint(300, 600),
        lambda: rng.uniform(-1e12, 1e12), lambda: float("nan"), lambda: float("inf"), lambda: "",
        lambda: "".join(chr(rng.randint(0, 0x2FFF)) for _ in range(rng.randint(1, 20))),
        lambda: rng.choice(["0001-01-01T00:00:00+05:00", "9999-12-31T23:59:59-05:00", "2026-13-40T99:00:00Z",
                            "2026-03-02T15:00:00", NOW.isoformat(), "simulated", "AAPL", str(uuid.uuid4())]),
    ]
    if depth < 3:
        choices += [lambda: [_random_value(rng, depth + 1) for _ in range(rng.randint(0, 3))],
                    lambda: {str(rng.randint(0, 9)): _random_value(rng, depth + 1) for _ in range(rng.randint(0, 3))}]
    return rng.choice(choices)()


def test_validator_never_raises_on_arbitrary_input():
    """Fuzz: any field may hold any JSON type, or the payload may be arbitrary bytes.

    A raise here would crash the processor on the same uncommitted message forever.
    """
    import random

    rng = random.Random(1234)
    base = event()
    for _ in range(4000):
        doc = dict(base)
        for key in rng.sample(list(doc), rng.randint(1, 4)):
            doc[key] = _random_value(rng)
        try:
            raw = json.dumps(doc).encode()
        except (ValueError, OverflowError):
            continue
        assert isinstance(validate(raw, RULES, NOW), (ValidEvent, Rejected))
    for _ in range(1000):
        raw = bytes(rng.randint(0, 255) for _ in range(rng.randint(0, 200)))
        assert isinstance(validate(raw, RULES, NOW), Rejected)
    huge = json.dumps(event()).replace('"price": 225.5', '"price": ' + "9" * 400).encode()
    assert validate(huge, RULES, NOW).error_type is ErrorType.INVALID_PRICE


def test_fractional_volume_is_accepted_for_crypto_quantities():
    r = check(event(volume=0.00012))
    assert isinstance(r, ValidEvent) and r.volume == pytest.approx(0.00012)


def test_an_instrument_never_mixes_simulated_and_real_data():
    rules = Rules(known_symbols=frozenset({"AAPL", "BTCUSDT"}), instrument_modes={"AAPL": "simulated", "BTCUSDT": "live"})
    sim_aapl = validate(json.dumps(event()).encode(), rules, NOW)
    live_aapl = validate(json.dumps(event(data_mode="live")).encode(), rules, NOW)
    delayed_btc = validate(json.dumps(event(symbol="BTCUSDT", data_mode="delayed")).encode(), rules, NOW)
    sim_btc = validate(json.dumps(event(symbol="BTCUSDT")).encode(), rules, NOW)
    assert isinstance(sim_aapl, ValidEvent) and isinstance(delayed_btc, ValidEvent)  # live and delayed are both real
    assert isinstance(live_aapl, Rejected) and live_aapl.error_type is ErrorType.INVALID_FIELD and "fresh database" in live_aapl.message
    assert isinstance(sim_btc, Rejected)
