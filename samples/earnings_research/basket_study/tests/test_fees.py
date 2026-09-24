"""Per-share commission transform on the ok shard frame (fees.py).

fee per $1 of reporter notional = sides x rate x shares_per_dollar; every
P&L column (all 18 marks) is reduced by the same amount because the share
count does not depend on the exit price."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import fees  # noqa: E402


def _ok():
    return pd.DataFrame({
        "event_symbol": ["A", "B"], "trade_day": ["2020-01-02", "2020-01-02"],
        "status": ["ok", "ok"],
        "pnl": [0.010, -0.020], "pnl_0931": [0.001, -0.002], "pnl_1530": [0.009, -0.019],
        "hedge_gross": [1.0, 2.0],
        # A: $10 reporter + one $20 peer at w=1  -> 0.1 + 0.05 = 0.15 shares per $
        # B: $2 reporter + one $1 peer at w=2    -> 0.5 + 2.0  = 2.5 shares per $
        "shares_per_dollar": [0.15, 2.5],
    })


class TestApply:
    def test_reduces_every_pnl_column_by_round_trip_fee(self):
        out = fees.apply_per_share_fee(_ok(), 0.005)
        fee = 2 * 0.005 * np.array([0.15, 2.5])           # 0.0015, 0.025
        assert np.allclose(out["fee_per_dollar"], fee)
        for c in ["pnl", "pnl_0931", "pnl_1530"]:
            assert np.allclose(out[c], _ok()[c] - fee)

    def test_zero_rate_is_identity_and_no_copy_side_effects(self):
        src = _ok()
        out = fees.apply_per_share_fee(src, 0.0)
        pd.testing.assert_frame_equal(out, src)
        assert "fee_per_dollar" not in out

    def test_input_frame_untouched(self):
        src = _ok(); before = src.copy()
        fees.apply_per_share_fee(src, 0.005)
        pd.testing.assert_frame_equal(src, before)

    def test_missing_shares_column_raises(self):
        with pytest.raises(ValueError, match="shares_per_dollar"):
            fees.apply_per_share_fee(_ok().drop(columns="shares_per_dollar"), 0.005)

    def test_one_dollar_stock_fee_in_bps(self):
        # reporter-only basket on a $1 stock: 1 share per $ -> 50 bps per side, 100 round trip
        df = _ok().iloc[:1].assign(shares_per_dollar=1.0)
        out = fees.apply_per_share_fee(df, 0.005)
        assert out["fee_per_dollar"].iloc[0] * 1e4 == pytest.approx(100.0)

    def test_sides_parameter(self):
        out = fees.apply_per_share_fee(_ok(), 0.005, sides=1)
        assert np.allclose(out["fee_per_dollar"], 0.005 * np.array([0.15, 2.5]))


class TestSuffix:
    def test_suffix_in_mills(self):
        assert fees.fee_suffix(0.005) == "_fee5m"
        assert fees.fee_suffix(0.0035) == "_fee3.5m"
        assert fees.fee_suffix(0.0) == ""
        assert fees.fee_suffix(None) == ""

    def test_label(self):
        assert fees.fee_label(0.005) == "$0.005 per share"
