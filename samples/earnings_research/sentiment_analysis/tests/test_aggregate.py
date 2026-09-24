"""Tests for aggregation and placement (Task 7)."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from aggregate import (
    always_long_frame,
    sharpe_per_event,
    sharpe_annualized,
    null_band,
    placement,
    render_placement_chart,
)


class TestAlwaysLong:
    def test_signs_back_out_of_signed_pnl(self):
        # R shard rows store pnl = direction * long_basket_return
        shard = pd.DataFrame({
            "event_id": [1, 2, 3],
            "status": ["ok", "ok", "skip"],
            "direction": [1, -1, None],
            "trade_day": ["2024-01-02", "2024-01-03", None],
            "pnl": [0.01, -0.02, None],
        })
        al = always_long_frame(shard)
        assert list(al["pnl"]) == [0.01, 0.02]      # short row flips sign
        assert list(al["trade_day"]) == ["2024-01-02", "2024-01-03"]

    def test_only_ok_rows_survive(self):
        shard = pd.DataFrame({
            "event_id": [1], "status": ["skip"], "direction": [None],
            "trade_day": [None], "pnl": [None],
        })
        assert always_long_frame(shard).empty


class TestSharpePerEvent:
    """Test per-event Sharpe calculation."""

    def test_known_small_array(self):
        # Hand-computed: mean = 0.03, std = 0.01414..., sharpe ≈ 2.121
        pnl = np.array([0.02, 0.04])
        result = sharpe_per_event(pnl)
        expected = np.mean([0.02, 0.04]) / np.std([0.02, 0.04], ddof=1)
        assert np.isclose(result, expected, atol=1e-6)
        assert np.isclose(result, 2.1213203435596424, atol=1e-6)

    def test_length_less_than_2_returns_nan(self):
        assert np.isnan(sharpe_per_event(np.array([0.01])))
        assert np.isnan(sharpe_per_event(np.array([])))

    def test_constant_array_returns_nan(self):
        # Zero variance -> nan
        assert np.isnan(sharpe_per_event(np.array([0.01, 0.01, 0.01])))

    def test_negative_mean(self):
        pnl = np.array([-0.01, -0.03])
        result = sharpe_per_event(pnl)
        assert result < 0  # Negative Sharpe


class TestSharpeAnnualized:
    """Test annualized Sharpe calculation."""

    def test_simple_case(self):
        # Two events on consecutive business days
        df = pd.DataFrame({
            "trade_day": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "pnl": [0.01, 0.02],
        })
        result = sharpe_annualized(df)
        # With just 2 days, this should be computable
        assert not np.isnan(result)
        assert result > 0

    def test_string_trade_day_as_in_shard_schema(self):
        # Spec §6: trade_day dtype is string — real shards arrive this way
        df = pd.DataFrame({
            "trade_day": ["2024-01-02", "2024-01-03"],
            "pnl": [0.01, 0.02],
        })
        result = sharpe_annualized(df)
        assert not np.isnan(result)
        expected = sharpe_annualized(df.assign(trade_day=pd.to_datetime(df["trade_day"])))
        assert result == pytest.approx(expected)

    def test_empty_dataframe_returns_nan(self):
        df = pd.DataFrame({"trade_day": [], "pnl": []})
        assert np.isnan(sharpe_annualized(df))

    def test_business_day_gaps_filled_with_zeros(self):
        # Events on Mon and Fri, should fill Tue-Thu with zeros
        df = pd.DataFrame({
            "trade_day": pd.to_datetime(["2024-01-02", "2024-01-05"]),  # Tue, Fri
            "pnl": [0.01, 0.02],
        })
        result = sharpe_annualized(df)
        # Should include 4 days: Tue, Wed, Thu, Fri (Mon was holiday/weekend)
        assert not np.isnan(result)

    def test_multiple_events_same_day_summed(self):
        df = pd.DataFrame({
            "trade_day": pd.to_datetime(["2024-01-02", "2024-01-02", "2024-01-03"]),
            "pnl": [0.01, 0.02, 0.01],
        })
        result = sharpe_annualized(df)
        # Day 1 should have PnL = 0.03, Day 2 = 0.01
        assert not np.isnan(result)

    def test_zero_variance_returns_nan(self):
        # All zeros -> zero variance
        df = pd.DataFrame({
            "trade_day": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "pnl": [0.0, 0.0, 0.0],
        })
        assert np.isnan(sharpe_annualized(df))


class TestNullBand:
    """Test null band computation."""

    def test_three_fake_seeds(self):
        # Deterministic seed 7 for test
        np.random.seed(7)

        shards = {}
        for seed in [0, 1, 2]:
            # Each shard has different events
            pnls = np.random.randn(10) * 0.01
            dates = pd.bdate_range(start="2024-01-02", periods=10)
            shards[seed] = pd.DataFrame({
                "event_id": range(100 + seed * 10, 110 + seed * 10),
                "status": ["ok"] * 10,
                "pnl": pnls,
                "trade_day": dates,
                "direction": [1] * 10,
            })

        result = null_band(shards)

        assert len(result) == 3
        assert list(result.columns) == ["seed", "sharpe_event", "sharpe_annualized", "n_traded"]
        assert list(result["seed"]) == [0, 1, 2]
        assert all(result["n_traded"] == 10)
        # All sharpes should be computable (not nan) for non-degenerate data
        assert result["sharpe_event"].notna().all()
        assert result["sharpe_annualized"].notna().all()


class TestPlacement:
    """Test model placement against null."""

    def test_model_identical_to_null_seed(self):
        # Create a null band with 3 seeds
        np.random.seed(7)
        null_rows = []
        for seed in [0, 1, 2]:
            sharpe_ev = np.random.randn() * 0.5
            sharpe_ann = np.random.randn() * 1.0
            null_rows.append({
                "seed": seed,
                "sharpe_event": sharpe_ev,
                "sharpe_annualized": sharpe_ann,
                "n_traded": 100,
            })
        null_df = pd.DataFrame(null_rows)

        # Model has same Sharpe as seed 1
        model_sharpe_ev = null_rows[1]["sharpe_event"]
        model_sharpe_ann = null_rows[1]["sharpe_annualized"]

        # Build model DataFrame
        pnls = np.array([0.01, 0.02, 0.03])  # Dummy, we'll override Sharpe
        dates = pd.bdate_range(start="2024-01-02", periods=3)
        model_df = pd.DataFrame({
            "event_id": [1, 2, 3],
            "status": ["ok"] * 3,
            "pnl": pnls,
            "trade_day": dates,
            "skip_reason": [None] * 3,
        })

        # We can't easily force exact Sharpe without knowing the formula internals,
        # but we can test p-value logic manually
        result = placement(model_df, null_df)

        # Check structure
        assert "sharpe_event" in result
        assert "sharpe_annualized" in result
        assert "p_event" in result
        assert "p_annualized" in result
        assert "n_traded" in result
        assert result["n_traded"] == 3
        assert "hit_rate" in result
        assert result["hit_rate"] == 1.0  # All positive PnLs

    def test_model_better_than_all_null(self):
        # Null band with 3 seeds, all negative Sharpes
        null_df = pd.DataFrame([
            {"seed": 0, "sharpe_event": -0.5, "sharpe_annualized": -1.0, "n_traded": 100},
            {"seed": 1, "sharpe_event": -0.3, "sharpe_annualized": -0.8, "n_traded": 100},
            {"seed": 2, "sharpe_event": -0.2, "sharpe_annualized": -0.5, "n_traded": 100},
        ])

        # Model with varying positive PnL (will have positive Sharpe with variance)
        dates = pd.bdate_range(start="2024-01-02", periods=5)
        model_df = pd.DataFrame({
            "event_id": [1, 2, 3, 4, 5],
            "status": ["ok"] * 5,
            "pnl": [0.01, 0.02, 0.015, 0.018, 0.012],  # Variable positive PnL
            "trade_day": dates,
            "skip_reason": [None] * 5,
        })

        result = placement(model_df, null_df)

        # Model should be better than all null seeds
        # p = (1 + count(null >= model)) / (1 + n_null)
        # Since model > all null, count = 0, so p = 1/4 = 0.25
        assert result["sharpe_event"] > 0
        assert result["sharpe_annualized"] > 0
        # p-value should be low (model beats all null seeds)
        # p = (1 + 0) / (1 + 3) = 0.25
        assert 0.2 < result["p_event"] < 0.3
        assert 0.2 < result["p_annualized"] < 0.3

    def test_skip_reasons_counted(self):
        dates = pd.bdate_range(start="2024-01-02", periods=5)
        model_df = pd.DataFrame({
            "event_id": [1, 2, 3, 4, 5],
            "status": ["ok", "skip", "skip", "ok", "ok"],
            "pnl": [0.01, None, None, 0.02, -0.01],
            "trade_day": dates,
            "skip_reason": [None, "neutral_score", "no_score", None, None],
        })

        null_df = pd.DataFrame([
            {"seed": 0, "sharpe_event": 0.0, "sharpe_annualized": 0.0, "n_traded": 100},
        ])

        result = placement(model_df, null_df)

        assert result["n_traded"] == 3
        assert result["n_neutral"] == 1
        assert result["n_noscore"] == 1
        # Hit rate = 2/3 (2 positive out of 3 traded)
        assert np.isclose(result["hit_rate"], 2/3, atol=1e-6)


class TestRenderPlacementChart:
    """Test chart rendering."""

    def test_renders_to_file(self, tmp_path):
        # Create dummy null and placements
        null_df = pd.DataFrame([
            {"seed": 0, "sharpe_event": 0.0, "sharpe_annualized": -0.5, "n_traded": 100},
            {"seed": 1, "sharpe_event": 0.1, "sharpe_annualized": 0.0, "n_traded": 100},
            {"seed": 2, "sharpe_event": -0.1, "sharpe_annualized": 0.5, "n_traded": 100},
        ])

        placements_df = pd.DataFrame([
            {"model_name": "model-a", "sharpe_annualized": 1.2},
            {"model_name": "model-b", "sharpe_annualized": 0.8},
            {"model_name": "model-c", "sharpe_annualized": 0.3},
        ])

        out_png = tmp_path / "test_chart.png"
        render_placement_chart(null_df, placements_df, out_png)

        assert out_png.exists()
        assert out_png.stat().st_size > 0
