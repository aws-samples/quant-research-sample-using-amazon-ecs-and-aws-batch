"""Golden-fixture checks for the sentiment thread-through.

GM event 1203531964 (fixture): fade direction is LONG (+1). A flipping
ctx must produce exactly -P&L per cell; a non-flipping ctx must byte-match
the no-ctx run; no-ctx runs must not even carry the new columns.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _fixtures import require  # noqa: E402

from evaluate import evaluate_event, event_table, CSV_COLUMNS

FIXTURE = Path(__file__).parent / "fixtures" / "panel_1203531964_subset.parquet"
GM_EVENT = 1203531964


@pytest.fixture(scope="module")
def panel():
    return pd.read_parquet(require(FIXTURE))


def _ok_pnl(rows):
    return {(r["construction"], r["universe"]): r["pnl"]
            for r in rows if r.get("status") == "ok"}


class TestLegacyPathUnchanged:
    def test_no_ctx_output_has_no_sentiment_columns(self, panel):
        t = event_table(panel)
        assert list(t.columns) == CSV_COLUMNS
        assert "sentiment_model" not in t.columns

    def test_no_ctx_rows_equal_pre_change_golden(self, panel):
        # phase-3 golden tests already pin the P&L values; here we pin that
        # evaluate_event(panel) == evaluate_event(panel, None)
        assert evaluate_event(panel) == evaluate_event(panel, None)


class TestNonFlippingCtx:
    def test_below_band_pnl_identical_to_baseline(self, panel):
        base = _ok_pnl(evaluate_event(panel))
        ctx = {"model": "m", "band": 4, "scores": {GM_EVENT: 2.0},
               "code_version": "dev"}
        out = evaluate_event(panel, ctx)
        assert _ok_pnl(out) == base
        assert all(r["direction_rule"] == "fade" for r in out if r.get("status") == "ok")

    def test_null_score_tagged_fade_no_score(self, panel):
        ctx = {"model": "m", "band": 4, "scores": {GM_EVENT: None},
               "code_version": "dev"}
        out = evaluate_event(panel, ctx)
        assert all(r["direction_rule"] == "fade_no_score"
                   for r in out if r.get("status") == "ok")
        assert _ok_pnl(out) == _ok_pnl(evaluate_event(panel))


class TestFlippingCtx:
    def test_flip_negates_pnl_exactly_per_cell(self, panel):
        base = _ok_pnl(evaluate_event(panel))
        ctx = {"model": "m", "band": 4, "scores": {GM_EVENT: 5.0},
               "code_version": "dev"}
        flipped = _ok_pnl(evaluate_event(panel, ctx))
        assert set(flipped) == set(base)
        for cell, pnl in flipped.items():
            assert pnl == pytest.approx(-base[cell], abs=1e-12)

    def test_flip_rows_tagged_momentum(self, panel):
        ctx = {"model": "m", "band": 4, "scores": {GM_EVENT: -4.0},
               "code_version": "dev"}
        out = evaluate_event(panel, ctx)
        ok = [r for r in out if r.get("status") == "ok"]
        assert ok and all(r["direction_rule"] == "momentum_flip" for r in ok)
        assert all(r["direction"] == -1 for r in ok)   # fade was +1 (LONG)

    def test_score_gap_agree_recorded(self, panel):
        # GM gap is negative; score +5 disagrees in sign
        ctx = {"model": "m", "band": 4, "scores": {GM_EVENT: 5.0},
               "code_version": "dev"}
        ok = [r for r in evaluate_event(panel, ctx) if r.get("status") == "ok"]
        assert all(r["score_gap_agree"] is False for r in ok)


class TestNeutralBandWithSentimentCtx:
    def test_neutral_skip_retains_sentiment_columns_under_ctx(self):
        # Scale prior day to put gap inside ±0.5% band -> neutral_band skip.
        # Even with an extreme score + flipping ctx, the skip happens upstream
        # of the flip, and sentiment columns are still populated on the skip row.
        panel = pd.read_parquet(require(FIXTURE))
        bars_gm = (panel["symbol"] == "GM")
        prev_day = panel["ts"].dt.tz_convert("America/New_York").dt.date.astype(str) == "2026-07-20"
        panel.loc[bars_gm & prev_day, ["open", "high", "low", "close"]] *= 0.9955

        ctx = {"model": "test_model", "band": 2, "scores": {GM_EVENT: 10.0},
               "code_version": "test"}
        out = evaluate_event(panel, ctx)

        # Should be a single skip row (no grid cells)
        assert len(out) == 1
        row = out[0]
        assert row["status"] == "skip"
        assert row["skip_reason"] == "neutral_band"
        # Sentiment columns must be populated even on skip
        assert row["sentiment_model"] == "test_model"
        assert row["flip_band"] == "2"
        assert row["code_version"] == "test"
        # No ok rows
        assert not any(r.get("status") == "ok" for r in out)
