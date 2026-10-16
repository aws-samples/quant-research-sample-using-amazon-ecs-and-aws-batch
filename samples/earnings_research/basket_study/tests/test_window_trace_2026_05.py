"""Regression on the committed data trace of the window 2026-05-01 -> 2026-06-01 (21 trade days):
the producer (portfolio_analysis.build / build_utilization, benchmark.spy_returns, the per-basket cap
aggregate.MAX_BASKET_WEIGHT and the per-share fee fees.apply_per_share_fee) re-run on the trace's
basket rows must reproduce the production daily files sliced from S3 on generation day.

The trace is narrowed to manifest.json `selection`: scopes PX10+ / PX50+ (reporter 9:30 entry price
>= $10 / >= $50, reporter_marks.parquet), construction ridge_levels_beta_neutral, universe pure play,
exits 1100 and close, per-basket caps 2 / 5 / 10 / 15 % and fees none / $0.005 per share — 8 variants
(portfolio_analysis[_capN][_fee5m]). ALL 8 variants x both exits x both scopes are rebuilt from the trace
at max abs diff 0.0 (daily returns) and every variant's utilization likewise (shares_per_dollar is in
the trace, so the fee variants are rebuildable). NOT rebuilt: the stop-loss daily files
(daily_returns_stop*.parquet, stop_sim.py; only portfolio_analysis and portfolio_analysis_fee5m have
them) — they are traced only for the /study10y viewer regression (tools/results_browser/test_server.py).

Fixture: tests/fixtures/window_2026-05/ written by window_trace.py (manifest.json lists every S3
source with its ETag). The module skips if the fixture dir is absent (exported sample).
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

FIX = Path(__file__).resolve().parent / "fixtures" / "window_2026-05"
if not (FIX / "manifest.json").exists():
    pytest.skip(f"window trace fixture not present: {FIX.name} (generate with window_trace.py)",
                allow_module_level=True)

import window_trace as wt  # noqa: E402

MAN = json.loads((FIX / "manifest.json").read_text())
SEL = MAN["selection"]
SCOPES, EXITS = SEL["scopes"], SEL["exits"]
VARIANTS = [(c, f) for c in SEL["caps"] for f in SEL["fees"]]
CELLS = [f"{s}|{c}|{u}" for s in SCOPES for c in SEL["construction"] for u in SEL["universe"]]
VID = lambda v: wt.variant_dir(*v)
MAX_FIXTURE_BYTES = 4 * 1024 * 1024          # hard ceiling; raised from 2 MB for the --pointwise files
                                             # (expected_points.py + leg_marks + event_signal + portfolio_stats, ~2.2 MB total)


@pytest.fixture(scope="module")
def manifest():
    return MAN


@pytest.fixture(scope="module")
def baskets():
    return pd.read_parquet(FIX / "baskets.parquet")


@pytest.fixture(scope="module")
def marks():
    return pd.read_parquet(FIX / "reporter_marks.parquet")


@pytest.fixture(scope="module")
def spy_daily():
    return pd.read_parquet(FIX / "spy_daily.parquet").set_index("date")


@pytest.fixture(scope="module")
def daily_long():
    return pd.read_parquet(FIX / "daily_returns.parquet")


@pytest.fixture(scope="module")
def util_long():
    return pd.read_parquet(FIX / "daily_utilization.parquet")


@pytest.fixture(scope="module")
def prod_close(daily_long):
    return wt.wide_slice(daily_long, "portfolio_analysis", "close").set_index("trade_day")


@pytest.fixture(scope="module")
def rebuilt(baskets, spy_daily, marks):
    """variant (cap, fee) -> (dailies {exit: frame indexed by trade_day}, utilization frame); built once."""
    cache = {}

    def get(variant):
        if variant not in cache:
            dailies, util = wt.rebuild_scope(baskets, spy_daily, SCOPES, EXITS, cap=variant[0], fee=variant[1],
                                             marks=marks, days=MAN["window"]["trade_days"])
            cache[variant] = ({e: d.set_index("trade_day") for e, d in dailies.items()}, util.set_index("trade_day"))
        return cache[variant]
    return get


def test_trace_window_bounds(manifest, baskets):
    w = manifest["window"]
    assert w["start"] == "2026-05-01" and w["end"] == "2026-06-01" and w["n_trade_days"] == 21
    assert baskets["trade_day"].between(w["start"], w["end"]).all()
    # the narrowed cells trade on a subset of the window's 21 study trade days (the rest are 0 rows)
    assert set(baskets["trade_day"].unique()) <= set(w["trade_days"])
    assert set(baskets.loc[baskets["status"] == "ok", "trade_day"].unique()) <= set(w["trade_days"])
    c = manifest["counts"]
    assert baskets["event_id"].nunique() == c["events"]
    assert len(baskets) == c["rows"] and int((baskets["status"] == "ok").sum()) == c["ok_rows"]
    ev = pd.read_csv(FIX / "events.csv")
    assert len(ev) == c["events"] and ev["event_id"].is_unique
    assert int(ev["n_ok_rows"].sum()) == c["ok_rows"]


@pytest.mark.parametrize("variant", VARIANTS, ids=VID)
@pytest.mark.parametrize("exit_code", EXITS)
@pytest.mark.parametrize("scope", SCOPES)
def test_daily_returns_rebuilt_from_trace(rebuilt, daily_long, manifest, scope, exit_code, variant):
    prod = wt.wide_slice(daily_long, VID(variant), exit_code).set_index("trade_day")
    reb = rebuilt(variant)[0][exit_code]
    assert list(reb.index) == list(prod.index) == manifest["window"]["trade_days"]
    for cell in [c for c in CELLS if c.startswith(scope + "|")] + ["spy_buy_hold"]:
        diff = np.abs(reb[cell].to_numpy() - prod[cell].to_numpy()).max()
        assert diff == 0.0, (cell, diff)
        assert (prod[cell] != 0).any(), cell                 # the cell actually traded in the window


@pytest.mark.parametrize("variant", VARIANTS, ids=VID)
@pytest.mark.parametrize("scope", SCOPES)
def test_utilization_rebuilt_from_trace(rebuilt, util_long, scope, variant):
    prod = wt.wide_slice(util_long, VID(variant)).set_index("trade_day")
    reb = rebuilt(variant)[1]
    assert list(reb.index) == list(prod.index)
    for cell in [c for c in CELLS if c.startswith(scope + "|")]:
        diff = np.abs(reb[cell].to_numpy() - prod[cell].to_numpy()).max()
        assert diff == 0.0, (cell, diff)
        assert (prod[cell] > 0).any(), cell


def test_spy_slice_matches_daily_file(spy_daily, prod_close, manifest, monkeypatch):
    import benchmark
    days = manifest["window"]["trade_days"]
    assert spy_daily.index[0] == manifest["window"]["prior_study_trade_day"]     # base close for day 1
    monkeypatch.setattr(benchmark, "load_spy_daily", lambda s3io: spy_daily)
    spy = benchmark.spy_returns(None, days)
    np.testing.assert_allclose(spy["buy_hold"].to_numpy(), prod_close["spy_buy_hold"].to_numpy(), rtol=0, atol=1e-12)
    closes = spy_daily["close"]
    by_hand = closes.loc[days].to_numpy() / closes.shift(1).loc[days].to_numpy() - 1
    np.testing.assert_allclose(by_hand, prod_close["spy_buy_hold"].to_numpy(), rtol=0, atol=1e-12)


# --------------------------------------------------------------- narrowed selection (2026-10-16)
def test_manifest_selection(manifest):
    sel = manifest["selection"]
    assert sel["scopes"] == ["PX10+", "PX50+"]
    assert sel["construction"] == ["ridge_levels_beta_neutral"] and sel["universe"] == ["pure play"]
    assert sel["exits"] == ["1100", "close"] and sel["caps"] == [2, 5, 10, 15] and sel["fees"] == [0, 5]
    assert sel["variants"] == ["portfolio_analysis", "portfolio_analysis_fee5m", "portfolio_analysis_cap5",
                               "portfolio_analysis_cap5_fee5m", "portfolio_analysis_cap10",
                               "portfolio_analysis_cap10_fee5m", "portfolio_analysis_cap15",
                               "portfolio_analysis_cap15_fee5m"]
    assert len(manifest["viewer_queries"]) == 2 * 2 * 4 * 2 == 32
    assert manifest["self_check_rebuild_scopes"] == SCOPES
    assert manifest["self_check_viewer_trace_identical"] == 32
    chk = manifest["self_check_max_abs_diff"]
    assert len(chk) == 8 * 3 and set(chk.values()) == {0.0}, chk


def test_baskets_inside_selection(baskets, marks, manifest):
    import study_config
    assert len(baskets.columns) == 47
    assert set(baskets["construction"]) <= set(SEL["construction"])
    assert set(baskets["universe"]) <= set(SEL["universe"])
    assert baskets["construction"].notna().all() and baskets["universe"].notna().all()
    px = wt.with_reporter_price(baskets, marks)[study_config.PRICE_COL]
    assert px.notna().all() and (px >= 10).all()               # PX10+ is the widest selected scope
    assert set(marks["event_id"]) == set(baskets["event_id"])
    ev = pd.read_csv(FIX / "events.csv")
    assert set(ev["event_id"]) == set(baskets["event_id"])     # events with >= 1 surviving row only
    assert manifest["counts"]["skip_rows"] == int((baskets["status"] != "ok").sum()) > 0   # skip rows kept


def test_slices_restricted_to_selection(daily_long, util_long, manifest):
    stop_long = pd.read_parquet(FIX / "daily_returns_stop.parquet")
    for long in (daily_long, util_long, stop_long):
        assert set(long["variant"]) <= set(SEL["variants"])
        assert set(long["column"]) <= set(CELLS) | {"spy_buy_hold"}
        assert set(long["trade_day"]) == set(manifest["window"]["trade_days"])
    for long in (daily_long, stop_long):
        assert set(long["exit"]) == set(EXITS)
    assert set(daily_long["variant"]) == set(util_long["variant"]) == set(SEL["variants"])
    srcs = [s for s in manifest["sources"] if "variant" in s]
    kinds = {(s["fixture"], s["variant"], s["exit"], s["stop_level"]) for s in srcs}
    assert len(kinds) == len(srcs) == 8 * 2 + 8 + 2 * 2 * 4         # daily + utilization + stop (cap 2 only)
    assert all(s["etag"] and s["rows_kept"] == 21 for s in srcs)


def test_fixture_size():
    total = sum(p.stat().st_size for p in FIX.iterdir() if p.is_file())
    assert total <= MAX_FIXTURE_BYTES, total
