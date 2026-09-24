"""SPY-hedged cells (README decision #8, 2026-09-24) on the GM fixture.

SPY bars for the fixture window come from tests/fixtures/spy_1min_2026-07-17_28.parquet
(Alpaca SIP, extracted from market-data/spy/1min_by_month/). Expectations are
recomputed here independently with numpy: origin-forced beta on the panel's
pre-event RTH minutes, clamp [0.5, 2], P&L = GM leg - w * SPY leg.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _fixtures import require  # noqa: E402

import spy_bars  # noqa: E402
from construction.registry import CONSTRUCTIONS, PSEUDO_UNIVERSE, SPY_HEDGED, needs_spy  # noqa: E402
from construction.ridge import CLAMP_HI, CLAMP_LO  # noqa: E402
from evaluate import evaluate_event  # noqa: E402
from rules.sessions import RTH_END_MIN, RTH_START_MIN, consolidate  # noqa: E402

FIX = Path(__file__).parent / "fixtures"
DAY = "2026-07-21"


@pytest.fixture(scope="module")
def panel():
    p = pd.read_parquet(require(FIX / "panel_1203531964_subset.parquet"))
    return spy_bars.attach_spy(p, pd.read_parquet(require(FIX / "spy_1min_2026-07-17_28.parquet")))


@pytest.fixture(scope="module")
def rows(panel):
    return {r["construction"]: r for r in evaluate_event(panel, constructions=list(SPY_HEDGED))}


def _expected_beta(panel, fit_space):
    bars = consolidate(panel)
    rth = bars[(bars["et_date"] < DAY) & (bars["et_min"] >= RTH_START_MIN) & (bars["et_min"] < RTH_END_MIN)]
    wide = rth[rth["symbol"].isin(["GM", "SPY"])].pivot_table(index="ts", columns="symbol", values="close")
    wide = wide.dropna().astype(float)
    logp = np.log(wide)
    m = logp.diff().dropna() if fit_space == "returns" else logp - logp.iloc[0]
    y, x = m["GM"].to_numpy(), m["SPY"].to_numpy()
    return float(y @ x) / float(x @ x), len(m)


def _spy_leg(panel):
    bars = consolidate(panel)
    b = bars[bars["symbol"] == "SPY"]
    entry = float(b[(b["et_date"] == DAY) & (b["et_min"] == RTH_START_MIN)].iloc[0]["open"])
    rth = b[(b["et_date"] == DAY) & (b["et_min"] >= RTH_START_MIN) & (b["et_min"] < RTH_END_MIN)]
    return entry, float(rth.iloc[-1]["close"])


def test_attach_is_panel_schema_and_idempotent(panel):
    p0 = pd.read_parquet(require(FIX / "panel_1203531964_subset.parquet"))
    assert list(panel.columns) == list(p0.columns)
    spy = panel[panel["symbol"] == "SPY"]
    assert len(spy) == 7220 and (spy["reporter_relationship"] == spy_bars.BENCHMARK_REL).all()
    assert not spy["event_bar"].any() and (spy["event_id"] == 1203531964).all()
    again = spy_bars.attach_spy(panel, pd.read_parquet(require(FIX / "spy_1min_2026-07-17_28.parquet")))
    assert len(again) == len(panel)
    assert spy_bars.months_between(*spy_bars.panel_window(p0)) == ["2026-07"]


def test_registry_wiring():
    assert list(SPY_HEDGED) == ["spy_beta_levels_neutral", "spy_beta_returns_neutral", "spy_dollar_neutral"]
    assert not any(n in CONSTRUCTIONS for n in SPY_HEDGED)
    assert all(PSEUDO_UNIVERSE[n] == "SPY" for n in SPY_HEDGED) and PSEUDO_UNIVERSE["reporter_only"] == "none"
    assert needs_spy(["reporter_only", "spy_dollar_neutral"]) and not needs_spy(["reporter_only"]) and not needs_spy(None)


def test_three_rows_under_spy(rows):
    assert set(rows) == set(SPY_HEDGED)
    for r in rows.values():
        assert (r["universe"], r["direction"]) == ("SPY", 1)
        if r["status"] == "ok":
            assert r["n_peers_theo"] == 1 and r["peers_theo"] == "SPY" and r["peers_actual"] == "SPY"
            assert r["n_ffill_entries"] == 0
        else:   # GM fixture: the two-session LEVELS beta on SPY is negative -> logged skip
            assert r["construction"] == "spy_beta_levels_neutral"
            assert r["skip_reason"] == "construction:non-positive beta"


def test_dollar_neutral_is_one_dollar_of_spy(rows, panel):
    r = rows["spy_dollar_neutral"]
    entry, exit_ = _spy_leg(panel)
    gm = 79.51 / 75.36 - 1
    assert r["hedge_gross"] == 1.0 and r["clamped"] is False
    assert r["pnl"] == pytest.approx(gm - (exit_ / entry - 1), abs=1e-12)
    assert r["shares_per_dollar"] == pytest.approx(1 / 75.36 + 1 / entry, abs=1e-12)
    legs = dict((s, (float(w), float(p))) for s, w, p in (tok.split(":") for tok in r["legs"].split(" ")))
    assert legs["GM"] == (1.0, 75.36) and legs["SPY"] == (-1.0, entry)


@pytest.mark.parametrize("name,space", [("spy_beta_levels_neutral", "levels"),
                                        ("spy_beta_returns_neutral", "returns")])
def test_beta_cells_match_numpy(rows, panel, name, space):
    beta, n = _expected_beta(panel, space)
    assert n >= 30
    r = rows[name]
    if beta <= 0:
        assert r["status"] == "skip" and r["skip_reason"] == "construction:non-positive beta"
        return
    w = min(max(beta, CLAMP_LO), CLAMP_HI)
    assert r["hedge_gross"] == pytest.approx(w, abs=1e-12)
    assert r["clamped"] is (w != beta)
    entry, exit_ = _spy_leg(panel)
    assert r["pnl"] == pytest.approx((79.51 / 75.36 - 1) - w * (exit_ / entry - 1), abs=1e-12)
    assert r["shares_per_dollar"] == pytest.approx(1 / 75.36 + w / entry, abs=1e-12)


def test_spaces_give_different_betas(panel):
    from construction import spy_hedge
    from construction.base import BasketContext
    ctx = BasketContext(bars=consolidate(panel), reporter="GM", peers={}, trade_day=DAY, direction=1)
    bl, _ = spy_hedge.beta_on_spy(ctx, "levels")
    br, _ = spy_hedge.beta_on_spy(ctx, "returns")
    assert bl != br and br > 0
    # GM fixture: levels beta is negative (spurious two-session fit) -> the cell skips
    assert bl < 0


def test_hedged_grid_ignores_spy_rows(panel):
    rows = evaluate_event(panel)
    assert len(rows) == 16
    assert not any("SPY" in (r.get("peers_theo") or "") or "SPY" in (r.get("legs") or "") for r in rows)
    base = evaluate_event(pd.read_parquet(require(FIX / "panel_1203531964_subset.parquet")))
    ok = lambda rs: {(r["construction"], r["universe"]): r.get("pnl") for r in rs}
    assert ok(rows) == ok(base)


def test_no_spy_bars_skips_not_crashes():
    p = pd.read_parquet(require(FIX / "panel_1203531964_subset.parquet"))
    rows = evaluate_event(p, constructions=["spy_dollar_neutral"])
    assert len(rows) == 1 and rows[0]["status"] == "skip" and "no SPY bars" in rows[0]["skip_reason"]
