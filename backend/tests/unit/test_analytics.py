import math

import numpy as np
import pandas as pd
import pytest
from app.services import analytics as a


def series(values, start="2026-01-01"):
    idx = pd.date_range(start, periods=len(values), freq="D", tz="UTC")
    return pd.Series(values, index=idx, dtype=float)


def test_simple_returns_match_definition():
    assert a.simple_returns(series([100, 110, 99])).tolist() == pytest.approx([0.10, -0.10])


def test_log_returns_match_definition():
    assert a.log_returns(series([100, 110, 99])).tolist() == pytest.approx([math.log(1.1), math.log(0.9)])


def test_returns_skip_missing_prices_instead_of_inventing_them():
    r = a.simple_returns(series([100, np.nan, 121]))
    assert r.tolist() == pytest.approx([0.21]) and len(r) == 1


def test_sma_is_undefined_until_window_filled():
    out = a.sma(series([1, 2, 3, 4]), 2)
    assert math.isnan(out.iloc[0]) and out.iloc[1:].tolist() == pytest.approx([1.5, 2.5, 3.5])


def test_ema_uses_recursive_definition():
    # a = 2/(3+1) = 0.5: 1 -> 1.5 -> 2.25 ; first span-1 points undefined
    out = a.ema(series([1, 2, 3]), 3)
    assert math.isnan(out.iloc[1]) and out.iloc[2] == pytest.approx(2.25)


def test_rolling_volatility_is_sample_std_annualised():
    r = series([0.01, -0.01, 0.02, 0.0])
    out = a.rolling_volatility(r, 3, annualize=True)
    expected = np.std([-0.01, 0.02, 0.0], ddof=1) * math.sqrt(a.PERIODS_PER_YEAR)
    assert out.iloc[-1] == pytest.approx(expected) and math.isnan(out.iloc[1])


def test_max_drawdown_finds_largest_peak_to_trough():
    dd = a.max_drawdown(series([100, 120, 90, 130, 65, 70]))
    assert dd.value == pytest.approx(-0.5)
    assert dd.peak_at == pd.Timestamp("2026-01-04", tz="UTC") and dd.trough_at == pd.Timestamp("2026-01-05", tz="UTC")
    assert dd.recovered_at is None


def test_drawdown_recovery_date_is_first_close_back_at_peak():
    dd = a.max_drawdown(series([100, 80, 90, 101]))
    assert dd.value == pytest.approx(-0.2) and dd.recovered_at == pd.Timestamp("2026-01-04", tz="UTC")


def test_correlation_of_scaled_and_inverted_returns():
    r = np.random.default_rng(1).normal(0, 0.01, 59)
    a_px = 100 * np.cumprod(np.r_[1.0, 1 + r])
    c_px = 100 * np.cumprod(np.r_[1.0, 1 - r])  # returns exactly opposite to A
    df = pd.DataFrame({"A": series(a_px), "B": series(a_px * 3), "C": series(c_px)})
    corr, counts = a.correlation_matrix(df)
    assert corr.loc["A", "B"] == pytest.approx(1.0)
    assert corr.loc["A", "C"] == pytest.approx(-1.0, abs=1e-6)
    assert counts.loc["A", "A"] == 59


def test_correlation_is_nan_when_overlap_is_too_short():
    df = pd.DataFrame({"A": series(range(1, 31)), "B": series([np.nan] * 25 + [1, 2, 3, 4, 5])})
    corr, _ = a.correlation_matrix(df)
    assert math.isnan(corr.loc["A", "B"])


def test_normalized_performance_starts_at_base():
    assert a.normalized_performance(series([50, 75])).tolist() == pytest.approx([100, 150])


def test_points_drop_nan_and_inf():
    pts = a.to_points(series([1.0, np.nan, np.inf, 2.0]))
    assert [p["value"] for p in pts] == [1.0, 2.0]
