"""Tests for validation gates (Task 6)."""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from validate import gate1_signs, gate2_scores, gate3_pairing


def _row(eid, direction, pnl, status="ok", skip=None, seed=None, score=None):
    return dict(event_id=eid, direction=direction, pnl=pnl, status=status,
                skip_reason=skip, seed=seed, sentiment_score=score)


class TestGate1:
    def test_clean_pass(self):
        signs = {(1, 0): 1, (1, 1): -1}
        shards = {0: pd.DataFrame([_row(1, 1, 0.02, seed=0)]),
                  1: pd.DataFrame([_row(1, -1, -0.02, seed=1)])}
        assert gate1_signs(shards, signs).empty

    def test_direction_mismatch_caught(self):
        signs = {(1, 0): -1}
        shards = {0: pd.DataFrame([_row(1, 1, 0.02, seed=0)])}
        assert len(gate1_signs(shards, signs)) == 1

    def test_magnitude_drift_caught(self):
        signs = {(1, 0): 1, (1, 1): -1}
        shards = {0: pd.DataFrame([_row(1, 1, 0.02, seed=0)]),
                  1: pd.DataFrame([_row(1, -1, -0.03, seed=1)])}
        assert len(gate1_signs(shards, signs)) >= 1

    def test_nan_pnl_on_ok_row_caught(self):
        """CRITICAL FIX 1: NaN pnl on traded row must be flagged."""
        signs = {(1, 0): 1}
        shards = {0: pd.DataFrame([_row(1, 1, None, seed=0)])}  # NaN pnl
        problems = gate1_signs(shards, signs)
        assert len(problems) == 1
        assert problems.iloc[0]["issue"] == "nan_pnl_on_traded_row"

    def test_cross_seed_status_divergence_caught(self):
        """CRITICAL FIX 2: Event traded in one seed but skipped in another must be flagged."""
        signs = {(1, 0): 1, (1, 1): -1}
        shards = {
            0: pd.DataFrame([_row(1, 1, 0.02, seed=0, status="ok")]),
            1: pd.DataFrame([_row(1, 0, None, seed=1, status="skip", skip="rth_release")])
        }
        problems = gate1_signs(shards, signs)
        assert len(problems) >= 1
        assert any(p["issue"] == "cross_seed_status_divergence" for _, p in problems.iterrows())


class TestGate2:
    def test_clean(self):
        s = pd.DataFrame([_row(1, 1, 0.02, score=3.0),
                          _row(2, 0, None, status="skip", skip="neutral_score", score=0.5),
                          _row(3, 0, None, status="skip", skip="no_score", score=None)])
        assert gate2_scores(s, {1: 3.0, 2: 0.5, 3: None}, 1.0).empty

    def test_wrong_direction_caught(self):
        s = pd.DataFrame([_row(1, -1, 0.02, score=3.0)])
        assert len(gate2_scores(s, {1: 3.0}, 1.0)) == 1

    def test_traded_neutral_caught(self):
        s = pd.DataFrame([_row(1, 1, 0.02, score=0.5)])
        assert len(gate2_scores(s, {1: 0.5}, 1.0)) == 1


class TestGate3:
    def test_row_conservation(self):
        a = pd.DataFrame([_row(1, 1, 0.1), _row(2, 1, 0.1)])
        b = pd.DataFrame([_row(1, 1, 0.1)])          # missing event 2
        assert len(gate3_pairing([a, b], expected_n_events=2)) >= 1

    def test_missing_seed_shard_caught(self):
        """CRITICAL FIX 3: Missing seed 63 must be flagged."""
        # Create shards 0..62 (missing 63)
        shards_r = {i: pd.DataFrame([_row(1, 1, 0.02, seed=i)]) for i in range(63)}
        all_shards = list(shards_r.values())
        problems = gate3_pairing(all_shards, shards_R=shards_r, expected_n_seeds=64, expected_n_events=1)
        assert len(problems) >= 1
        # Check for either count mismatch or missing seeds issue
        issues = set(problems["issue"])
        assert "arm_r_shard_count_mismatch" in issues or "arm_r_missing_seeds" in issues

    def test_arm_skip_is_not_a_pairing_violation(self):
        """An event arm-skipped in S (neutral/no_score) never reaches the data
        checks, so it must not count as a viable-set mismatch vs Arm R."""
        r = pd.DataFrame([_row(1, 1, 0.02, seed=0), _row(2, -1, -0.01, seed=0)])
        s = pd.DataFrame([_row(1, 1, 0.02, score=3.0),
                          _row(2, 0, None, status="skip", skip="neutral_score", score=0.5)])
        problems = gate3_pairing([r, s], expected_n_events=2)
        if not problems.empty:
            assert "viable_set_mismatch" not in set(problems["issue"])

    def test_data_viability_divergence_still_caught(self):
        """An event traded in R but DATA-skipped in S (same panels, same code)
        is a genuine pairing violation and must be flagged."""
        r = pd.DataFrame([_row(1, 1, 0.02, seed=0), _row(2, -1, -0.01, seed=0)])
        s = pd.DataFrame([_row(1, 1, 0.02, score=3.0),
                          _row(2, 0, None, status="skip", skip="reporter_no_open_bar", score=2.0)])
        problems = gate3_pairing([r, s], expected_n_events=2)
        assert "viable_set_mismatch" in set(problems["issue"])

    def test_missing_model_shard_caught(self):
        """CRITICAL FIX 3: Missing one model must be flagged."""
        expected_models = ["model-a", "model-b", "model-c"]
        # Only provide 2 models (missing model-c)
        shards_s = [
            pd.DataFrame([_row(1, 1, 0.02, score=2.0)]).assign(sentiment_model="model-a"),
            pd.DataFrame([_row(1, 1, 0.02, score=2.0)]).assign(sentiment_model="model-b"),
        ]
        all_shards = shards_s
        problems = gate3_pairing(all_shards, shards_S=shards_s, expected_models=expected_models, expected_n_events=1)
        assert len(problems) >= 1
        issues = set(problems["issue"])
        assert "arm_s_shard_count_mismatch" in issues or "arm_s_missing_models" in issues
