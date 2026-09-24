"""Paired Sharpe machinery, the sign test, and the coded §6.3 promotion rule
on synthetic data with known answers."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aggregate_sentiment import (add_adjacent_agreement, build_cells,
                                candidates, paired_sharpe_test,
                                percentile_grid, promote, sign_test_pvalue)


class TestPairedSharpe:
    """The three statistical properties, asserted against the CORRECT
    Jobson-Korkie/Memmel variance: V = (1/n)[2 - 2rho + 0.5(sr1^2+sr2^2)
    - 0.5*sr1*sr2*(1+rho^2)]."""

    def test_identical_series_delta_zero_p_one(self):
        rng = np.random.default_rng(7)
        x = rng.normal(0.001, 0.01, 400)
        sb, sc, delta, p = paired_sharpe_test(x, x.copy())
        assert delta == 0.0 and p == 1.0

    def test_flipping_only_losers_improves_sharpe(self):
        rng = np.random.default_rng(7)
        base = rng.normal(0.001, 0.01, 400)
        cond = base.copy()
        worst = np.argsort(base)[:40]
        cond[worst] = -cond[worst]            # flip the 40 worst events
        sb, sc, delta, p = paired_sharpe_test(base, cond)
        assert delta > 0 and p < 0.05

    def test_random_flips_not_significant(self):
        rng = np.random.default_rng(7)
        base = rng.normal(0.001, 0.01, 400)
        cond = base.copy()
        flip = rng.choice(400, 40, replace=False)
        cond[flip] = -cond[flip]
        _, _, _, p = paired_sharpe_test(base, cond)
        assert p > 0.05

    def test_memmel_variance_coefficient_is_half(self):
        """Pins the corrected coefficient: recompute the p-value by hand with
        the 0.5 factor on the cross term and require an exact match (the
        pre-fix formula omitted it, inflating the variance and deflating z)."""
        from scipy.stats import norm
        rng = np.random.default_rng(3)
        base = rng.normal(0.001, 0.01, 250)
        cond = base.copy()
        cond[np.argsort(base)[:30]] *= -1
        sb, sc, delta, p = paired_sharpe_test(base, cond)
        n = len(base)
        rho = float(np.corrcoef(base, cond)[0, 1])
        var = (1.0 / n) * (2 - 2 * rho + 0.5 * (sb**2 + sc**2)
                           - 0.5 * sb * sc * (1 + rho**2))
        expect = float(2 * (1 - norm.cdf(abs((sc - sb) / np.sqrt(var)))))
        assert p == pytest.approx(expect, rel=1e-12)
        # and the (wrong) un-halved variant would give a strictly larger p
        var_wrong = (1.0 / n) * (2 - 2 * rho + 0.5 * (sb**2 + sc**2)
                                 - sb * sc * (1 + rho**2))
        p_wrong = float(2 * (1 - norm.cdf(abs((sc - sb) / np.sqrt(var_wrong)))))
        assert p != pytest.approx(p_wrong, rel=1e-9)

    def test_zero_dispersion_returns_all_nan(self):
        assert all(np.isnan(v) for v in paired_sharpe_test(np.zeros(20),
                                                           np.arange(20.0)))
        assert all(np.isnan(v) for v in paired_sharpe_test(np.arange(20.0),
                                                           np.ones(20)))

    def test_nonpositive_variance_yields_nan_pvalue_not_one(self):
        """A p-value of 1.0 reads as 'tested, no effect'; NaN reads as
        'not testable'. The degenerate branch must say NaN."""
        import aggregate_sentiment as agg
        base = np.array([0.01, 0.02, 0.03, 0.04])
        cond = np.array([0.02, 0.01, 0.04, 0.03])
        real = agg.np.corrcoef

        def fake_corrcoef(a, b):
            return np.array([[1.0, 50.0], [50.0, 1.0]])   # forces var < 0

        agg.np.corrcoef = fake_corrcoef
        try:
            with pytest.warns(RuntimeWarning):
                sb, sc, delta, p = paired_sharpe_test(base, cond)
        finally:
            agg.np.corrcoef = real
        assert np.isnan(p) and p != 1.0


class TestSignTest:
    def test_flipping_known_losers_is_significant(self):
        rng = np.random.default_rng(11)
        base = rng.normal(0.001, 0.01, 400)
        cond = base.copy()
        worst = np.argsort(base)[:40]
        cond[worst] = -cond[worst]        # every changed event improves
        assert sign_test_pvalue(base, cond) < 0.05

    def test_random_flips_not_significant(self):
        rng = np.random.default_rng(11)
        base = rng.normal(0.001, 0.01, 400)
        cond = base.copy()
        flip = rng.choice(400, 60, replace=False)
        cond[flip] = -cond[flip]
        assert sign_test_pvalue(base, cond) > 0.05

    def test_ties_dropped_and_all_ties_gives_nan(self):
        x = np.array([0.01, -0.02, 0.03])
        assert np.isnan(sign_test_pvalue(x, x.copy()))
        # only the two changed events count, not the 98 identical ones
        base = np.concatenate([np.full(98, 0.01), [-0.05, -0.06]])
        cond = base.copy(); cond[-2:] *= -1
        assert sign_test_pvalue(base, cond) == pytest.approx(0.5)


class TestAdjacentAgreement:
    def _cells(self, deltas):
        return pd.DataFrame({"model": ["m"] * 4, "band": ["2", "3", "4", "5"],
                             "sharpe_delta": deltas,
                             "construction": ["ew"] * 4, "universe": ["all"] * 4})

    def test_agreeing_neighbour_is_true(self):
        out = add_adjacent_agreement(self._cells([0.1, 0.2, -0.3, -0.4]))
        got = dict(zip(out["band"], out["adjacent_agrees"]))
        assert got["2"] and got["3"]        # 2<->3 both positive
        assert got["4"] and got["5"]        # 4<->5 both negative

    def test_lone_sign_is_false(self):
        out = add_adjacent_agreement(self._cells([0.1, -0.2, -0.3, -0.4]))
        got = dict(zip(out["band"], out["adjacent_agrees"]))
        assert got["2"] is False or not got["2"]   # band 2's only neighbour (3) disagrees
        assert got["3"] and got["4"] and got["5"]

    def test_neighbours_clamped_to_studied_bands(self):
        """Band 2 has no band-1 neighbour and band 5 no band-6: a single
        positive cell at an edge can never self-confirm."""
        out = add_adjacent_agreement(self._cells([0.1, -0.2, -0.3, 0.4]))
        got = dict(zip(out["band"], out["adjacent_agrees"]))
        assert not got["2"] and not got["5"]

    def test_agreement_does_not_cross_models(self):
        cells = pd.DataFrame({
            "model": ["a", "b"], "band": ["2", "3"], "sharpe_delta": [0.1, 0.2],
            "construction": ["ew"] * 2, "universe": ["all"] * 2})
        out = add_adjacent_agreement(cells)
        assert not out["adjacent_agrees"].any()

    def test_agreement_does_not_cross_cells(self):
        cells = pd.DataFrame({
            "model": ["a", "a"], "band": ["2", "3"], "sharpe_delta": [0.1, 0.2],
            "construction": ["ew", "ridge"], "universe": ["all", "all"]})
        out = add_adjacent_agreement(cells)
        assert not out["adjacent_agrees"].any()


def _cells_frame(**over):
    """One unflagged cell that passes every §6.3 criterion, plus its agreeing
    neighbour; `over` overrides fields of the cell under test (row 0)."""
    rows = [{"model": "m", "band": "3", "n_flipped": 50, "flagged": False,
             "sharpe_delta": 0.4, "lw_pvalue": 0.01, "sign_pvalue": 0.01,
             "construction": "ew", "universe": "all"},
            {"model": "m", "band": "4", "n_flipped": 50, "flagged": False,
             "sharpe_delta": 0.3, "lw_pvalue": 0.01, "sign_pvalue": 0.01,
             "construction": "ew", "universe": "all"}]
    rows[0].update(over)
    return add_adjacent_agreement(pd.DataFrame(rows))


class TestPromotion:
    def test_all_criteria_met_is_promoted(self):
        prom = promote(_cells_frame())
        assert "3" in set(prom["band"])

    @pytest.mark.parametrize("override", [
        {"flagged": True, "n_flipped": 5},      # below the n_flipped floor
        {"sharpe_delta": -0.4},                 # wrong direction
        {"lw_pvalue": 0.20},                    # LW test fails
        {"sign_pvalue": 0.20},                  # sign test fails
    ])
    def test_each_missing_criterion_blocks_promotion(self, override):
        cells = _cells_frame(**override)
        assert "3" not in set(promote(cells)["band"])

    def test_lone_significant_band_not_promoted_but_is_a_candidate(self):
        """Band 3 clears LW + sign + delta + n_flipped, but its only positive
        neighbour is gone -> candidate, never a finding."""
        cells = _cells_frame()
        cells.loc[1, "sharpe_delta"] = -0.3
        cells = add_adjacent_agreement(cells.drop(columns=["adjacent_agrees"]))
        assert "3" not in set(promote(cells)["band"])
        assert "3" in set(candidates(cells)["band"])

    def test_candidates_and_promoted_are_disjoint(self):
        cells = _cells_frame()
        assert not (set(promote(cells).index) & set(candidates(cells).index))

    def test_nan_pvalue_never_promotes(self):
        cells = _cells_frame(lw_pvalue=float("nan"))
        assert len(promote(cells)) == 0 or "3" not in set(promote(cells)["band"])


class TestBuildCells:
    def _shards(self, n_flip=10):
        base = pd.DataFrame({
            "event_id": range(100), "construction": "ew", "universe": "all",
            "status": "ok", "pnl": np.linspace(-0.02, 0.02, 100)})
        m = pd.concat([base.assign(flip_band=str(b),
                                   direction_rule=np.where(base.event_id < n_flip,
                                                           "momentum_flip", "fade"),
                                   pnl=np.where(base.event_id < n_flip, -base.pnl, base.pnl))
                       for b in (2, 3, 4, 5)], ignore_index=True)
        return base, {"model-x": m}

    def test_cells_shape_and_columns(self):
        base, shards = self._shards()
        cells = build_cells(base, shards, "ew", "all")
        assert len(cells) == 4                       # 1 model x 4 bands
        for c in ("model", "band", "n_flipped", "sharpe_base",
                  "sharpe_cond", "sharpe_delta", "lw_pvalue", "sign_pvalue",
                  "adjacent_agrees", "flagged"):
            assert c in cells.columns

    def test_anecdotal_flag_below_20_flips(self):
        base, shards = self._shards()
        cells = build_cells(base, shards, "ew", "all")
        assert cells["flagged"].all()                # 10 flips < 20 floor

    def test_flagged_cells_are_never_promoted(self):
        base, shards = self._shards()
        cells = build_cells(base, shards, "ew", "all")
        assert len(promote(cells)) == 0

    def test_percentile_grid_carries_base_and_cond_percentiles(self):
        base, shards = self._shards(n_flip=30)
        grid = percentile_grid(build_cells(base, shards, "ew", "all"))
        for q in (5, 25, 50, 75, 95):
            assert f"base_p{q}_pct" in grid.columns
            assert f"cond_p{q}_pct" in grid.columns

    def test_duplicate_event_ids_in_model_shard_raise(self):
        """A stale legacy model=<m>.parquet next to the new .part=* files
        duplicates every event; silently proceeding corrupts every statistic."""
        base, shards = self._shards()
        dup = pd.concat([shards["model-x"], shards["model-x"]], ignore_index=True)
        with pytest.raises(ValueError, match="duplicate event_ids"):
            build_cells(base, {"model-x": dup}, "ew", "all")

    def test_duplicate_event_ids_in_baseline_raise(self):
        base, shards = self._shards()
        with pytest.raises(ValueError, match="duplicate event_ids"):
            build_cells(pd.concat([base, base], ignore_index=True), shards,
                        "ew", "all")
