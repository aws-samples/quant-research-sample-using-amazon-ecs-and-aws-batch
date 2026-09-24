"""Pin the SPY overlay (aggregate.apply_spy_overlay + --spy-weight).

Portfolio model: long SPY with weight w at all times, strategy traded on top
(daily-rebalanced overlay): r_port = w * r_spy + r_strategy per trade day.
w=0 must be a bit-exact identity — the artifact set of a --spy-weight 0 run
reconciles against a run without the flag.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import aggregate


def _daily(days, cells_values):
    """Daily-returns matrix like aggregate.daily_returns produces."""
    mat = pd.DataFrame(cells_values, index=days)
    mat.index.name = "trade_day"
    return mat


def _spy(days, buy_hold):
    return pd.DataFrame({"buy_hold": buy_hold, "open_close": 0.0}, index=days)


DAYS = ["2026-06-10", "2026-06-11", "2026-06-12"]


def test_weight_zero_is_identity():
    daily = _daily(DAYS, {"c|u": [0.01, -0.02, 0.0]})
    spy = _spy(DAYS, [0.005, 0.007, -0.003])
    out = aggregate.apply_spy_overlay(daily, spy, 0.0)
    pd.testing.assert_frame_equal(out, daily)          # bit-exact, not approx


def test_weight_zero_without_spy_data_is_identity():
    daily = _daily(DAYS, {"c|u": [0.01, -0.02, 0.0]})
    out = aggregate.apply_spy_overlay(daily, None, 0.0)
    pd.testing.assert_frame_equal(out, daily)


def test_weight_one_adds_spy_daily():
    daily = _daily(DAYS, {"a|x": [0.01, -0.02, 0.0],
                          "b|y": [0.00, 0.03, -0.01]})
    spy = _spy(DAYS, [0.005, 0.007, -0.003])
    out = aggregate.apply_spy_overlay(daily, spy, 1.0)
    for cell in daily.columns:
        np.testing.assert_allclose(
            out[cell].to_numpy(),
            daily[cell].to_numpy() + np.array([0.005, 0.007, -0.003]))


def test_fractional_weight_scales():
    daily = _daily(DAYS, {"c|u": [0.0, 0.0, 0.0]})
    spy = _spy(DAYS, [0.01, 0.01, 0.01])
    out = aggregate.apply_spy_overlay(daily, spy, 0.5)
    np.testing.assert_allclose(out["c|u"].to_numpy(), [0.005, 0.005, 0.005])


def test_missing_spy_days_fill_flat():
    daily = _daily(DAYS, {"c|u": [0.01, 0.01, 0.01]})
    spy = _spy(DAYS[:2], [0.005, 0.007])               # last day missing
    out = aggregate.apply_spy_overlay(daily, spy, 1.0)
    np.testing.assert_allclose(out["c|u"].to_numpy(), [0.015, 0.017, 0.01])


def test_nonzero_weight_without_spy_data_raises():
    daily = _daily(DAYS, {"c|u": [0.01, -0.02, 0.0]})
    with pytest.raises(SystemExit):
        aggregate.apply_spy_overlay(daily, None, 1.0)


def test_output_base_suffix():
    assert aggregate.spy_weight_suffix(0.0) == ""
    assert aggregate.spy_weight_suffix(1.0) == "_spyw1"
    assert aggregate.spy_weight_suffix(0.5) == "_spyw0.5"
    assert aggregate.spy_weight_suffix(2.0) == "_spyw2"


# ----------------------------------------------------- strategy leverage

def test_leverage_default_is_identity():
    daily = _daily(DAYS, {"c|u": [0.01, -0.02, 0.0]})
    out = aggregate.apply_spy_overlay(daily, None, 0.0, lev=1.0)
    pd.testing.assert_frame_equal(out, daily)              # bit-exact


def test_leverage_scales_strategy():
    daily = _daily(DAYS, {"c|u": [0.01, -0.02, 0.0]})
    spy = _spy(DAYS, [0.005, 0.007, -0.003])
    out = aggregate.apply_spy_overlay(daily, spy, 1.0, lev=2.0)
    np.testing.assert_allclose(
        out["c|u"].to_numpy(),
        2.0 * np.array([0.01, -0.02, 0.0]) + np.array([0.005, 0.007, -0.003]))


def test_leverage_without_spy():
    daily = _daily(DAYS, {"c|u": [0.01, -0.02, 0.0]})
    out = aggregate.apply_spy_overlay(daily, None, 0.0, lev=2.0)
    np.testing.assert_allclose(out["c|u"].to_numpy(), [0.02, -0.04, 0.0])


def test_leverage_suffix():
    assert aggregate.spy_weight_suffix(1.0, 1.0) == "_spyw1"
    assert aggregate.spy_weight_suffix(1.0, 2.0) == "_spyw1_lev2"
    assert aggregate.spy_weight_suffix(0.0, 2.0) == "_lev2"
    assert aggregate.spy_weight_suffix(0.0, 1.0) == ""
    assert aggregate.spy_weight_suffix(0.5, 1.5) == "_spyw0.5_lev1.5"
