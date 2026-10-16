"""Multi-day holding periods (user ask 2026-10-12): enter at the 9:30 open of
the trade day as today, hold every leg at its entry weight, exit at the RTH
close of trading day N (day 1 = the trade day, so `pnl` is the 1-day hold).
New mark columns pnl_d2..pnl_d5 next to the intraday marks; a mark is NULL
(not 0) when the panel does not carry that session. The 1-day `pnl` must be
untouched (the phase-3 golden tests pin it).

GM fixture: trade day 2026-07-21 (06:30 ET release); the panel carries
sessions 07-22, 07-23, 07-24, 07-27, 07-28 after it, so day 2..5 = 07-22,
07-23, 07-24, 07-27.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import evaluate  # noqa: E402
from evaluate import CSV_COLUMNS, CSV_DTYPES, HOLD_DAYS, HOLD_LABELS, MARK_LABELS, evaluate_event  # noqa: E402
from rules.sessions import consolidate, exit_price, hold_sessions  # noqa: E402
from _fixtures import require  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "panel_1203531964_subset.parquet"
EQ, ALL = "equal_weight_dollar_neutral", "all"


@pytest.fixture(scope="module")
def panel():
    return pd.read_parquet(require(FIXTURE))


@pytest.fixture(scope="module")
def rows(panel):
    return evaluate_event(panel)


def _cell(rows, construction=EQ, universe=ALL):
    return next(r for r in rows if r["construction"] == construction and r["universe"] == universe)


class TestSchema:
    def test_hold_labels(self):
        assert HOLD_DAYS == [2, 3, 4, 5]
        assert HOLD_LABELS == ["pnl_d2", "pnl_d3", "pnl_d4", "pnl_d5"]

    def test_columns_follow_the_intraday_marks(self):
        i = CSV_COLUMNS.index(MARK_LABELS[-1])
        assert CSV_COLUMNS[i + 1:i + 5] == HOLD_LABELS
        assert all(CSV_DTYPES[l] == "Float64" for l in HOLD_LABELS)

    def test_intraday_marks_unchanged(self):
        # the hold marks are NOT intraday marks: MARK_MINUTES/MARK_LABELS stay zipped 1:1
        assert all(not l.startswith("pnl_d") for l in MARK_LABELS)
        assert len(MARK_LABELS) == len(evaluate.MARK_MINUTES)


class TestHoldSessions:
    def test_next_sessions_after_trade_day(self):
        assert hold_sessions("2026-07-21", 5) == ["2026-07-22", "2026-07-23", "2026-07-24", "2026-07-27"]

    def test_skips_weekend_and_holiday(self):
        # 2026-07-03 (Fri) is the Independence Day observance: closed
        assert hold_sessions("2026-07-02", 2) == ["2026-07-06"]
        assert hold_sessions("2026-07-02", 3) == ["2026-07-06", "2026-07-07"]

    def test_one_day_hold_has_no_extra_sessions(self):
        assert hold_sessions("2026-07-21", 1) == []


class TestHoldMarks:
    def test_day2_close_golden(self, rows, panel):
        bars = consolidate(panel)
        r = _cell(rows)
        w = {"GM": 1.0, "F": -1 / 3, "TSLA": -1 / 3, "HON": -1 / 3}
        entry = {"GM": 75.36, "F": 14.15, "TSLA": 371.34, "HON": 227.87}
        expect = sum(w[s] * (exit_price(bars, s, "2026-07-22") / entry[s] - 1) for s in w)
        assert r["pnl_d2"] == pytest.approx(expect, abs=1e-12)

    def test_day5_close_golden(self, rows, panel):
        bars = consolidate(panel)
        r = _cell(rows)
        w = {"GM": 1.0, "F": -1 / 3, "TSLA": -1 / 3, "HON": -1 / 3}
        entry = {"GM": 75.36, "F": 14.15, "TSLA": 371.34, "HON": 227.87}
        expect = sum(w[s] * (exit_price(bars, s, "2026-07-27") / entry[s] - 1) for s in w)
        assert r["pnl_d5"] == pytest.approx(expect, abs=1e-12)

    def test_holds_differ_from_the_one_day_close(self, rows):
        r = _cell(rows)
        vals = [r["pnl"]] + [r[l] for l in HOLD_LABELS]
        assert all(v is not None for v in vals)
        assert len(set(round(v, 9) for v in vals)) == 5

    def test_every_ok_row_carries_all_holds(self, rows):
        for r in rows:
            if r["status"] == "ok":
                assert all(l in r and r[l] is not None for l in HOLD_LABELS), r["construction"]

    def test_outright_hold_is_the_reporter_leg(self, panel):
        rows = evaluate_event(panel, constructions=["reporter_only"])
        r = rows[0]
        bars = consolidate(panel)
        assert r["status"] == "ok"
        assert r["pnl_d3"] == pytest.approx(exit_price(bars, "GM", "2026-07-23") / 75.36 - 1, abs=1e-12)


class TestMissingSessions:
    def test_marks_are_null_when_the_panel_lacks_the_session(self, panel):
        et = panel["ts"].dt.tz_convert("America/New_York").dt.date.astype(str)
        truncated = panel[et <= "2026-07-23"].reset_index(drop=True)      # day 1..3 only
        rows = evaluate_event(truncated)
        r = _cell(rows)
        full = _cell(evaluate_event(panel))
        assert r["pnl"] == pytest.approx(full["pnl"], abs=1e-12)
        assert r["pnl_d2"] == pytest.approx(full["pnl_d2"], abs=1e-12)
        assert r["pnl_d3"] == pytest.approx(full["pnl_d3"], abs=1e-12)
        assert r["pnl_d4"] is None and r["pnl_d5"] is None

    def test_event_table_types_null_holds_as_float(self, panel):
        et = panel["ts"].dt.tz_convert("America/New_York").dt.date.astype(str)
        t = evaluate.event_table(panel[et <= "2026-07-22"].reset_index(drop=True))
        ok = t[t["status"] == "ok"]
        assert str(t["pnl_d5"].dtype) == "Float64"
        assert ok["pnl_d2"].notna().all() and ok["pnl_d3"].isna().all()
