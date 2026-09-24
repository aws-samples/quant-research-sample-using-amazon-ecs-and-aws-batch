"""Pin the SPY benchmark contract: minute-bar -> daily RTH reduction,
alignment to trade days, both return series, and graceful no-op while the
data hasn't been loaded yet."""
import sys
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import benchmark


class FakeS3IO:
    """read_parquet backed by an in-memory frame (None -> raise, simulating
    a missing S3 object)."""
    def __init__(self, df=None):
        self.df = df

    def read_parquet(self, uri):
        if self.df is None:
            raise FileNotFoundError(uri)
        return pl.from_pandas(self.df)


def _minute_bars(day: str, bars: list) -> pd.DataFrame:
    """bars: list of (HH:MM ET, open, close)."""
    rows = []
    for hm, o, c in bars:
        ts = pd.Timestamp(f"{day} {hm}", tz="America/New_York").tz_convert("UTC")
        rows.append({"timestamp": ts, "open": o, "close": c})
    return pd.DataFrame(rows)


# two days of sparse minute bars, incl. pre/after-hours prints that must be
# ignored by the RTH reduction
SPY_MIN = pd.concat([
    _minute_bars("2026-06-08", [
        ("09:15", 599.0, 599.5),   # pre-market — ignored
        ("09:30", 601.0, 601.4),   # RTH open
        ("12:00", 602.0, 602.2),
        ("15:59", 603.9, 604.0),   # RTH close
        ("16:30", 604.5, 604.6),   # after-hours — ignored
    ]),
    _minute_bars("2026-06-09", [
        ("09:30", 606.0, 605.8),
        ("15:59", 598.1, 598.0),
    ]),
], ignore_index=True)

DAYS = ["2026-06-08", "2026-06-09"]


def test_missing_data_returns_none():
    assert benchmark.spy_returns(FakeS3IO(None), DAYS) is None


def test_no_overlap_returns_none():
    assert benchmark.spy_returns(FakeS3IO(SPY_MIN), ["2030-01-02"]) is None


def test_daily_reduction_rth_only():
    daily = benchmark.load_spy_daily(FakeS3IO(SPY_MIN))
    # open = 9:30 bar's open (not the 9:15 pre-market print)
    assert daily.loc["2026-06-08", "open"] == pytest.approx(601.0)
    # close = 15:59 bar's close (not the 16:30 after-hours print)
    assert daily.loc["2026-06-08", "close"] == pytest.approx(604.0)


def test_returns_aligned_and_correct():
    out = benchmark.spy_returns(FakeS3IO(SPY_MIN), DAYS)
    assert list(out.index) == DAYS
    # first covered day has no prior close -> filled flat
    assert out.loc["2026-06-08", "buy_hold"] == 0.0
    assert out.loc["2026-06-09", "buy_hold"] == pytest.approx(598.0 / 604.0 - 1)
    assert out.loc["2026-06-08", "open_close"] == pytest.approx(604.0 / 601.0 - 1)
    assert out.loc["2026-06-09", "open_close"] == pytest.approx(598.0 / 606.0 - 1)


def test_partial_coverage_fills_flat():
    days = DAYS + ["2026-06-10"]                # absent from the bar history
    out = benchmark.spy_returns(FakeS3IO(SPY_MIN), days)
    assert out.loc["2026-06-10", "buy_hold"] == 0.0
    assert out.loc["2026-06-10", "open_close"] == 0.0


def test_sparse_scope_samples_same_trajectory():
    """A scope trading a SUBSET of days must show SPY at the same equity
    level as the full window on shared days (user bug report 2026-08-07:
    per-scope compounding made SPY curves differ across matrices)."""
    three = pd.concat([SPY_MIN, _minute_bars("2026-06-10", [
        ("09:30", 599.0, 599.2), ("15:59", 610.0, 611.0)])], ignore_index=True)
    full_days = ["2026-06-08", "2026-06-09", "2026-06-10"]
    spy = benchmark.spy_returns(FakeS3IO(three), full_days)
    full_curve = benchmark.spy_equity_curve(spy, full_days)
    sparse_curve = benchmark.spy_equity_curve(spy, ["2026-06-08", "2026-06-10"])
    # same trajectory values on the shared days — not recompounded per scope
    assert sparse_curve["2026-06-10"] == pytest.approx(full_curve["2026-06-10"])
    assert sparse_curve["2026-06-08"] == pytest.approx(full_curve["2026-06-08"])


def test_buy_hold_compounds_across_gap_days():
    """buy_hold between consecutive REQUESTED days spans the gap (period
    return), so sparse scopes still track the true SPY path."""
    three = pd.concat([SPY_MIN, _minute_bars("2026-06-10", [
        ("09:30", 599.0, 599.2), ("15:59", 610.0, 611.0)])], ignore_index=True)
    out = benchmark.spy_returns(FakeS3IO(three), ["2026-06-08", "2026-06-10"])
    # 06-10 vs 06-08 close (604 -> 611), skipping 06-09 entirely
    assert out.loc["2026-06-10", "buy_hold"] == pytest.approx(611.0 / 604.0 - 1)


def test_alt_timestamp_column_accepted():
    df = SPY_MIN.rename(columns={"timestamp": "ts_event"})
    daily = benchmark.load_spy_daily(FakeS3IO(df))
    assert daily.loc["2026-06-09", "close"] == pytest.approx(598.0)


def test_charts_accept_none_spy():
    """fig_equity / fig_equity_by_tier must render without SPY."""
    import io
    import aggregate
    ok = pd.DataFrame({
        "trade_day": DAYS * 2,
        "construction": ["equal_weight_dollar_neutral"] * 4,
        "universe": ["all"] * 2 + ["pure play"] * 2,
        "liquidity_tier": ["mid"] * 4,
        "event_id": range(4),
        "pnl": [0.01, -0.02] * 2,
    })
    daily = aggregate.daily_returns(ok)
    aggregate.fig_equity(daily, io.BytesIO(), spy=None)
    aggregate.fig_equity_by_tier(ok, io.BytesIO(), spy=None)


def test_charts_accept_spy_overlay():
    import io
    import aggregate
    ok = pd.DataFrame({
        "trade_day": DAYS,
        "construction": ["equal_weight_dollar_neutral"] * 2,
        "universe": ["all"] * 2,
        "liquidity_tier": ["mid"] * 2,
        "event_id": range(2),
        "pnl": [0.01, -0.02],
    })
    daily = aggregate.daily_returns(ok)
    spy = benchmark.spy_returns(FakeS3IO(SPY_MIN), DAYS)
    aggregate.fig_equity(daily, io.BytesIO(), spy=spy)
    aggregate.fig_equity_by_tier(ok, io.BytesIO(), spy=spy)


def test_cell_matrix_accepts_spy_overlay():
    """fig_cell_matrix renders with and without the SPY curve; every drawn
    panel carries exactly one SPY line (gid='spy') when data is present."""
    import io
    import numpy as np
    import aggregate
    import matplotlib.pyplot as plt

    n = 40
    rng = np.random.default_rng(1)
    ok = pd.DataFrame({
        "trade_day": [DAYS[i % 2] for i in range(n)],
        "construction": [list(aggregate.CONS_COLORS)[i % 4] for i in range(n)],
        "universe": [list(aggregate.UNIV_STYLE)[i % 4] for i in range(n)],
        "liquidity_tier": ["mid"] * n,
        "event_id": range(n),
        "pnl": rng.normal(0, 0.02, n),
        "pnl_0931": rng.normal(0, 0.01, n),
        "pnl_0932": rng.normal(0, 0.01, n),
        "pnl_0933": rng.normal(0, 0.01, n),
        "pnl_0934": rng.normal(0, 0.01, n),
        "pnl_0935": rng.normal(0, 0.01, n),
        "pnl_1000": rng.normal(0, 0.01, n),
        "pnl_1100": rng.normal(0, 0.01, n),
        "pnl_1200": rng.normal(0, 0.01, n),
    })
    spy = benchmark.spy_returns(FakeS3IO(SPY_MIN), DAYS)
    aggregate.fig_cell_matrix(ok, io.BytesIO(), tiers=("mid",), spy=None)
    # capture the figure before it is closed to count SPY lines
    drawn = {}
    orig_savefig = plt.Figure.savefig
    def spy_counter(self, *a, **k):
        drawn["spy_lines"] = sum(1 for ax in self.axes for ln in ax.lines
                                 if ln.get_gid() == "spy")
        drawn["panels"] = sum(1 for ax in self.axes if ax.lines)
        return orig_savefig(self, *a, **k)
    plt.Figure.savefig = spy_counter
    try:
        aggregate.fig_cell_matrix(ok, io.BytesIO(), tiers=("mid",), spy=spy)
    finally:
        plt.Figure.savefig = orig_savefig
    assert drawn["spy_lines"] == drawn["panels"] > 0


def test_date_ticks_adaptive_scale():
    """Tick granularity scales with span: weekly <=~4mo, monthly <=~3y,
    quarterly beyond (user request 2026-08-07: 2y charts unreadable weekly)."""
    import aggregate
    import datetime as dt

    def days(n):
        out, d = [], dt.date(2025, 1, 2)
        while len(out) < n:
            if d.weekday() < 5:
                out.append(str(d))
            d += dt.timedelta(days=1)
        return out

    _, labels, scale = aggregate.date_ticks(days(60))
    assert scale == "weekly" and labels[0].count("-") == 1      # MM-DD

    pos, labels, scale = aggregate.date_ticks(days(393))
    assert scale == "monthly"
    assert labels[0] == "2025-01" and 15 <= len(labels) <= 25   # ~19 labels
    assert pos == sorted(pos)

    _, labels, scale = aggregate.date_ticks(days(1000))
    assert scale == "quarterly" and labels[0] == "2025-Q1"


def test_cell_matrix_accepts_index_list():
    """index=['NDX','SPX'] renders (union scope) — new package member 2026-08-07."""
    import io
    import numpy as np
    import aggregate
    from unittest.mock import patch

    n = 24
    rng = np.random.default_rng(2)
    ok = pd.DataFrame({
        "trade_day": [DAYS[i % 2] for i in range(n)],
        "event_symbol": ["AAPL"] * n,
        "construction": [list(aggregate.CONS_COLORS)[i % 4] for i in range(n)],
        "universe": [list(aggregate.UNIV_STYLE)[i % 4] for i in range(n)],
        "liquidity_tier": ["large"] * n,
        "event_id": range(n),
        "pnl": rng.normal(0, 0.02, n),
        "pnl_0931": rng.normal(0, 0.01, n),
        "pnl_0932": rng.normal(0, 0.01, n),
        "pnl_0933": rng.normal(0, 0.01, n),
        "pnl_0934": rng.normal(0, 0.01, n),
        "pnl_0935": rng.normal(0, 0.01, n),
        "pnl_1000": rng.normal(0, 0.01, n),
        "pnl_1100": rng.normal(0, 0.01, n),
        "pnl_1200": rng.normal(0, 0.01, n),
    })
    # avoid S3: pretend AAPL is a member of both indices
    with patch("index_filter.IndexFilter.is_member", return_value=True):
        aggregate.fig_cell_matrix(ok, io.BytesIO(), index=["NDX", "SPX"],
                                  title_suffix="NDX+SPX (index members)")
