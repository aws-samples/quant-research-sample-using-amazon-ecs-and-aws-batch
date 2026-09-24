"""Golden-event checks: direction plumbs through to P&L sign; schema fixed."""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _fixtures import require  # noqa: E402
from evaluate import COLUMNS, evaluate_event, event_table
from rules.direction import Decision

FIXTURE = Path(__file__).parent / "fixtures" / "panel_1203531964_subset.parquet"


@pytest.fixture(scope="module")
def panel():
    return pd.read_parquet(require(FIXTURE))


def _meta(**kw):
    base = dict(seed=None, sentiment_model=None, neutral_threshold=None,
                code_version="dev", run_id="test")
    return base | kw


def _pnl(rows):
    ok = [r for r in rows if r["status"] == "ok"]
    assert len(ok) == 1, rows      # single construction x single universe
    return ok[0]["pnl"]


class TestDirectionPlumbing:
    def test_long_vs_short_exact_negation(self, panel):
        long_rows = evaluate_event(panel, lambda e: Decision(+1, "random", None),
                                   _meta(seed=0))
        short_rows = evaluate_event(panel, lambda e: Decision(-1, "random", None),
                                    _meta(seed=1))
        assert _pnl(long_rows) == pytest.approx(-_pnl(short_rows), abs=1e-12)

    def test_skip_decision_produces_skip_row(self, panel):
        rows = evaluate_event(panel, lambda e: Decision(0, "sentiment", "neutral_score"),
                              _meta(sentiment_model="m", neutral_threshold=1.0))
        assert all(r["status"] == "skip" and r["skip_reason"] == "neutral_score"
                   for r in rows)

    def test_no_gap_columns_in_output(self, panel):
        t = event_table(panel, lambda e: Decision(+1, "random", None), _meta(seed=0))
        assert "gap" not in t.columns and "signal_return_928_vs_prev_close" not in t.columns
        assert list(t.columns) == COLUMNS

    def test_single_basket_per_event(self, panel):
        t = event_table(panel, lambda e: Decision(+1, "random", None), _meta(seed=0))
        assert (t["construction"] == "ridge_levels_beta_neutral").all()
        assert (t["universe"] == "all").all()
        assert len(t) == 1
