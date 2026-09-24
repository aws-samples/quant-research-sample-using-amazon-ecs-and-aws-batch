"""Pin the per-basket capital cap in aggregate.daily_returns.

Capital model: on a day with N traded events, each basket gets
min(1/N, MAX_BASKET_WEIGHT) of the cell's capital, so the day's portfolio
return is mean_pnl * min(1, cap*N); uncommitted capital idles at 0.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import aggregate


def _ok(rows):
    return pd.DataFrame(rows, columns=[
        "trade_day", "construction", "universe", "pnl"])


@pytest.fixture(autouse=True)
def restore_cap():
    old = aggregate.MAX_BASKET_WEIGHT
    yield
    aggregate.MAX_BASKET_WEIGHT = old


def test_single_event_day_scaled_by_cap():
    aggregate.MAX_BASKET_WEIGHT = 0.02
    ok = _ok([("2026-06-10", "c", "u", -0.10)])          # CASY-style -10% day
    mat = aggregate.daily_returns(ok)
    # one event: basket gets 2% of capital -> day return -0.10 * 0.02
    assert mat.loc["2026-06-10", "c|u"] == pytest.approx(-0.002)


def test_many_events_uncapped_equal_split():
    aggregate.MAX_BASKET_WEIGHT = 0.02
    # 60 events: 1/60 < 2% cap -> plain mean * min(1, .02*60) = mean * 1.0? no:
    # min(1, 0.02*60) = min(1, 1.2) = 1.0 -> fully invested, day return = mean
    ok = _ok([("2026-06-10", "c", "u", 0.01)] * 60)
    mat = aggregate.daily_returns(ok)
    assert mat.loc["2026-06-10", "c|u"] == pytest.approx(0.01)


def test_partial_deployment():
    aggregate.MAX_BASKET_WEIGHT = 0.02
    # 10 events at cap 2% -> 20% deployed -> day return = mean * 0.2
    ok = _ok([("2026-06-10", "c", "u", 0.05)] * 10)
    mat = aggregate.daily_returns(ok)
    assert mat.loc["2026-06-10", "c|u"] == pytest.approx(0.05 * 0.2)


def test_cap_one_reproduces_legacy_mean():
    aggregate.MAX_BASKET_WEIGHT = 1.0
    ok = _ok([("2026-06-10", "c", "u", 0.04),
              ("2026-06-10", "c", "u", -0.02)])
    mat = aggregate.daily_returns(ok)
    assert mat.loc["2026-06-10", "c|u"] == pytest.approx(0.01)


def test_no_event_day_zero_and_cells_independent():
    aggregate.MAX_BASKET_WEIGHT = 0.02
    ok = _ok([("2026-06-10", "c1", "u", -0.10),
              ("2026-06-11", "c2", "u", 0.10)])
    mat = aggregate.daily_returns(ok)
    assert mat.loc["2026-06-11", "c1|u"] == 0.0          # idle day stays 0
    assert mat.loc["2026-06-10", "c2|u"] == 0.0
    assert mat.loc["2026-06-11", "c2|u"] == pytest.approx(0.002)


def test_bootstrap_scale_matches_daily_returns():
    aggregate.MAX_BASKET_WEIGHT = 0.02
    ok = _ok([("2026-06-10", "c", "u", -0.10)])
    days = ["2026-06-10"]
    lo, hi = aggregate.bootstrap_sharpe_ci(ok, days, n=10)
    # single constant event: every bootstrap draw = -0.002 daily series;
    # sharpe of a constant is nan -> CI is nan, but must not raise
    import math
    assert math.isnan(lo) and math.isnan(hi)
