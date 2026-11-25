"""Price model shared by the live simulator and the historical backfill.

Standard library only, so the seed script can import it without Kafka deps.

One-factor geometric Brownian motion: each step draws one market shock and one
idiosyncratic shock per instrument. `factor_loading` is the correlation of an
instrument's shocks with the market shock, so ETFs move together and single
stocks partly follow them, the way real equities do.
"""

from __future__ import annotations

import json
import math
import random
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

# The simulated venue trades 24/7, so a "year" is a calendar year of seconds.
# Analytics annualise with 365 periods for the same reason.
SECONDS_PER_YEAR = 365 * 24 * 3600


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    symbol: str
    company_name: str
    exchange: str
    asset_type: str
    currency: str
    base_price: float
    volatility: float  # annualised sigma
    factor_loading: float  # correlation with the market factor, 0..1
    tick_volume: int  # median shares per simulated trade

    def __post_init__(self) -> None:
        if self.base_price <= 0:
            raise ValueError(f"{self.symbol}: base_price must be > 0")
        if not 0 < self.volatility < 5:
            raise ValueError(f"{self.symbol}: volatility must be in (0, 5)")
        if not 0 <= self.factor_loading <= 1:
            raise ValueError(f"{self.symbol}: factor_loading must be in [0, 1]")
        if self.tick_volume <= 0:
            raise ValueError(f"{self.symbol}: tick_volume must be > 0")


def load_instruments(path: str | Path) -> list[InstrumentSpec]:
    rows = json.loads(Path(path).read_text(encoding="utf-8"))
    return [InstrumentSpec(**row) for row in rows]


def correlated_shocks(rng: random.Random, loadings: Sequence[float]) -> list[float]:
    """Standard-normal shocks whose pairwise correlation is loading_i * loading_j."""
    z_market = rng.gauss(0.0, 1.0)
    return [b * z_market + math.sqrt(1.0 - b * b) * rng.gauss(0.0, 1.0) for b in loadings]


def gbm_step(price: float, sigma: float, dt_years: float, z: float, drift: float = 0.0) -> float:
    """S(t+dt) = S(t) * exp((mu - sigma^2/2) dt + sigma sqrt(dt) z)."""
    return price * math.exp((drift - 0.5 * sigma * sigma) * dt_years + sigma * math.sqrt(dt_years) * z)


def gbm_step_back(price: float, sigma: float, dt_years: float, z: float, drift: float = 0.0) -> float:
    """Inverse of gbm_step: the earlier price that gbm_step would carry to `price`.

    Used by the backfill to generate history that ends exactly at the live
    simulator's starting price, so charts have no jump at the seam.
    """
    return price / math.exp((drift - 0.5 * sigma * sigma) * dt_years + sigma * math.sqrt(dt_years) * z)


def tick_volume(rng: random.Random, median: int) -> int:
    return max(1, int(rng.lognormvariate(math.log(median), 0.6)))
