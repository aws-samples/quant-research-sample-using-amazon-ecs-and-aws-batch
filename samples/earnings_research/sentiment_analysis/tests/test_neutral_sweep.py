"""Tests for the neutral-band sweep (pure reconstruction, no S3)."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from neutral_sweep import sweep_frames


def _fixture():
    idx = pd.Index([1, 2, 3, 4], name="event_id")
    long_pnl = pd.Series([0.01, -0.02, 0.03, -0.04], index=idx)
    trade_day = pd.Series(["2024-01-02", "2024-01-03", "2024-01-04",
                           "2024-01-05"], index=idx)
    scores = pd.Series([3.0, -2.0, 1.0, np.nan], index=idx)
    signs = pd.DataFrame({0: [1, 1, 1, 1], 1: [-1, -1, -1, -1]}, index=idx)
    return long_pnl, trade_day, scores, signs


def test_neutral_band_excludes_inclusive():
    long_pnl, trade_day, scores, signs = _fixture()
    cell, _ = sweep_frames(long_pnl, trade_day, scores, signs, neutral=1.0)
    # event 3 (score 1.0) skips, event 4 (NaN) skips -> 2 traded, 1 long
    assert cell["n_traded"] == 2
    assert cell["n_long"] == 1


def test_direction_applies_sign_of_score():
    long_pnl, trade_day, scores, signs = _fixture()
    cell, _ = sweep_frames(long_pnl, trade_day, scores, signs, neutral=1.0)
    # long event 1: +0.01; short event 2: -(-0.02)=+0.02 -> both wins
    assert cell["hit_rate"] == 1.0


def test_null_restricted_to_traded_subset():
    long_pnl, trade_day, scores, signs = _fixture()
    _, null_sharpes = sweep_frames(long_pnl, trade_day, scores, signs,
                                   neutral=1.0)
    # 2 seeds, both computable on the 2-event subset
    assert len(null_sharpes) == 2


def test_tight_band_degenerates_to_nan():
    long_pnl, trade_day, scores, signs = _fixture()
    cell, null_sharpes = sweep_frames(long_pnl, trade_day, scores, signs,
                                      neutral=4.0)
    assert cell["n_traded"] == 0
    assert np.isnan(cell["sharpe_annualized"])
    assert len(null_sharpes) == 0
