"""Phase-3 tests: registry completeness + evaluator P&L on the GM fixture.

Hand-computed golden P&L for the fixture (XNAS marks, direction LONG GM):
  GM leg:  +1.0 * (79.51/75.36 - 1)           = +5.5069%
  peers (opposite side, entry -> exit):
    F    14.15 -> 14.25   (+0.7067%)
    TSLA 371.34 -> 378.90 (+2.0359%)
    HON  227.87 -> 229.81 (+0.8514%)
  equal_weight 'all' (w = -1/3 each):
    pnl = 5.5069% - (0.7067+2.0359+0.8514)%/3 = +4.3089%
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _fixtures import require  # noqa: E402

from construction.registry import CONSTRUCTIONS, OUTRIGHT, UNHEDGED_UNIVERSE, select
from evaluate import evaluate_event

FIXTURE = Path(__file__).parent / "fixtures" / "panel_1203531964_subset.parquet"


@pytest.fixture(scope="module")
def rows():
    return evaluate_event(pd.read_parquet(require(FIXTURE)))


class TestRegistry:
    def test_four_constructions(self):
        assert sorted(CONSTRUCTIONS) == ["equal_weight_dollar_neutral",
                                         "ridge_levels_beta_neutral",
                                         "ridge_returns_beta_neutral",
                                         "ridge_returns_dollar_neutral"]

    def test_every_name_states_neutrality(self):
        assert all(n.endswith("_dollar_neutral") or n.endswith("_beta_neutral")
                   for n in CONSTRUCTIONS)


class TestEvaluator:
    def test_grid_is_complete(self, rows):
        ok = [r for r in rows if r["status"] == "ok"]
        # 4 constructions x 4 universes; every cell present (ok or logged skip)
        assert len(rows) == 16
        cells = {(r["construction"], r["universe"]) for r in rows}
        assert len(cells) == 16
        assert len(ok) >= 12   # some single-peer ridge cells may legitimately skip

    def test_direction_is_long(self, rows):
        assert all(r["direction"] == +1 for r in rows if "direction" in r)

    def test_equal_weight_all_pnl_golden(self, rows):
        r = next(r for r in rows if r["construction"] == "equal_weight_dollar_neutral"
                 and r["universe"] == "all")
        gm = 79.51 / 75.36 - 1
        f = 14.25 / 14.15 - 1
        tsla = 378.90 / 371.34 - 1
        hon = 229.81 / 227.87 - 1
        expect = gm - (f + tsla + hon) / 3
        assert r["pnl"] == pytest.approx(expect, abs=1e-12)
        assert r["n_peers_theo"] == 3 and r["n_peers_actual"] == 3
        assert r["n_ffill_entries"] == 0

    def test_single_universe_pnl(self, rows):
        r = next(r for r in rows if r["construction"] == "equal_weight_dollar_neutral"
                 and r["universe"] == "functional")
        expect = (79.51 / 75.36 - 1) - (378.90 / 371.34 - 1)
        assert r["pnl"] == pytest.approx(expect, abs=1e-12)

    def test_ridge_beta_pnl_uses_its_gross(self, rows):
        r = next(r for r in rows if r["construction"] == "ridge_returns_beta_neutral"
                 and r["universe"] == "all")
        assert r["status"] == "ok"
        # hedge gross must be the diagnostics value, not 1.0
        assert 0 < r["hedge_gross"] < 1.0

    def test_ridge_dollar_gross_is_one(self, rows):
        r = next(r for r in rows if r["construction"] == "ridge_returns_dollar_neutral"
                 and r["universe"] == "all")
        assert r["hedge_gross"] == pytest.approx(1.0)

    def test_neutral_band_event_skips_all_cells(self):
        # widen prev close so the gap falls inside the band -> single skip row
        panel = pd.read_parquet(require(FIXTURE))
        bars_gm = (panel["symbol"] == "GM")
        prev_day = panel["ts"].dt.tz_convert("America/New_York").dt.date.astype(str) == "2026-07-20"
        panel.loc[bars_gm & prev_day, ["open", "high", "low", "close"]] *= 0.9955
        out = evaluate_event(panel)
        assert len(out) == 1
        assert out[0]["status"] == "skip" and out[0]["skip_reason"] == "neutral_band"

class TestIntradayMarksAndLiquidity:
    def test_mark_columns_present_and_bounded(self, rows):
        from evaluate import MARK_LABELS
        r = next(r for r in rows if r["construction"] == "equal_weight_dollar_neutral"
                 and r["universe"] == "all")
        for lbl in MARK_LABELS:
            assert lbl in r and r[lbl] is not None
        # marks evolve toward the close; last mark (15:30) should be within
        # the day's plausible band and differ from the open mark
        assert r["pnl_0935"] != r["pnl_1530"]
        assert abs(r["pnl_1530"] - r["pnl"]) < 0.05   # 15:30 near the 15:59 exit

    def test_first_mark_uses_935_prices(self, rows):
        # hand-check: GM 9:35 bar close vs entry, weight +1; peers at 1/3 each
        import pandas as pd
        from pathlib import Path
        from rules.sessions import consolidate
        bars = consolidate(pd.read_parquet(
            Path(__file__).parent / "fixtures" / "panel_1203531964_subset.parquet"))
        def close_at(sym, minute):
            b = bars[(bars["symbol"] == sym) & (bars["et_date"] == "2026-07-21")
                     & (bars["et_min"] <= minute)]
            return float(b.iloc[-1]["close"])
        r = next(r for r in rows if r["construction"] == "equal_weight_dollar_neutral"
                 and r["universe"] == "all")
        expect = (close_at("GM", 575) / 75.36 - 1) \
            - (1/3) * (close_at("F", 575) / 14.15 - 1) \
            - (1/3) * (close_at("TSLA", 575) / 371.34 - 1) \
            - (1/3) * (close_at("HON", 575) / 227.87 - 1)
        assert r["pnl_0935"] == pytest.approx(expect, abs=1e-12)

    def test_liquidity_tier_present(self, rows):
        r = rows[0]
        assert r["liquidity_tier"] in ("micro", "small", "mid", "large")
        assert r["reporter_dollar_vol_day"] > 0
        # GM on earnings day is unambiguously large (>$200M dollar volume)
        assert r["liquidity_tier"] == "large"


class TestSharesPerDollar:
    """Transaction-cost hook (2026-09-23): every ok row carries the share count
    traded per $1 of reporter notional, so a per-share commission is a
    post-hoc transform (fee = rate x sides x shares_per_dollar), plus the
    per-leg detail needed for per-order minimums/caps later."""

    def test_equal_weight_all_shares_golden(self, rows):
        r = next(r for r in rows if r["construction"] == "equal_weight_dollar_neutral"
                 and r["universe"] == "all")
        # |w| / entry per leg: GM 1/75.36, peers 1/3 each at their 9:30 opens
        expect = 1 / 75.36 + (1 / 14.15 + 1 / 371.34 + 1 / 227.87) / 3
        assert r["shares_per_dollar"] == pytest.approx(expect, abs=1e-12)

    def test_legs_string_round_trips(self, rows):
        r = next(r for r in rows if r["construction"] == "equal_weight_dollar_neutral"
                 and r["universe"] == "all")
        legs = dict((s, (float(w), float(p))) for s, w, p in
                    (tok.split(":") for tok in r["legs"].split(" ")))
        assert set(legs) == {"GM", "F", "TSLA", "HON"}
        assert legs["GM"] == (1.0, 75.36)
        assert legs["F"][0] == pytest.approx(-1 / 3, abs=1e-6) and legs["F"][1] == 14.15
        # the string reproduces shares_per_dollar exactly
        assert sum(abs(w) / p for w, p in legs.values()) == pytest.approx(
            r["shares_per_dollar"], abs=1e-9)

    def test_ridge_shares_scale_with_hedge_gross(self, rows):
        eq = next(r for r in rows if r["construction"] == "equal_weight_dollar_neutral"
                  and r["universe"] == "functional")
        # single-peer universe: dollar-neutral ridge has gross 1 -> identical share count
        rd = next(r for r in rows if r["construction"] == "ridge_returns_dollar_neutral"
                  and r["universe"] == "functional")
        if rd["status"] == "ok":
            assert rd["shares_per_dollar"] == pytest.approx(eq["shares_per_dollar"], abs=1e-9)

    def test_schema_carries_new_columns(self):
        from evaluate import CSV_COLUMNS, CSV_DTYPES, event_table
        assert "shares_per_dollar" in CSV_COLUMNS and "legs" in CSV_COLUMNS
        assert CSV_DTYPES["shares_per_dollar"] == "Float64" and CSV_DTYPES["legs"] == "string"
        t = event_table(pd.read_parquet(require(FIXTURE)))
        assert t["shares_per_dollar"].notna().sum() == (t["status"] == "ok").sum()


class TestReporterOnly:
    """Outright reporter trade (README decision #7, 2026-09-24): the GM leg
    alone, +1.0 * (79.51/75.36 - 1) = +5.5069%, one row under universe 'none'."""

    @pytest.fixture(scope="class")
    def outright(self):
        return evaluate_event(pd.read_parquet(require(FIXTURE)), constructions=["reporter_only"])

    def test_registry_keeps_hedged_four_separate(self):
        assert list(OUTRIGHT) == ["reporter_only"]
        assert "reporter_only" not in CONSTRUCTIONS
        assert UNHEDGED_UNIVERSE == "none"
        assert list(select(None)) == list(CONSTRUCTIONS)
        assert list(select(["reporter_only"])) == ["reporter_only"]
        with pytest.raises(KeyError):
            select(["reporter_onlyy"])

    def test_one_row_under_none(self, outright):
        assert len(outright) == 1
        r = outright[0]
        assert (r["construction"], r["universe"], r["status"]) == ("reporter_only", "none", "ok")
        assert r["n_peers_theo"] == 0 and r["n_peers_actual"] == 0 and r["peers_actual"] == ""
        assert r["hedge_gross"] == 0.0 and r["clamped"] is False

    def test_pnl_is_the_reporter_leg(self, outright):
        r = outright[0]
        assert r["direction"] == +1
        assert r["pnl"] == pytest.approx(79.51 / 75.36 - 1, abs=1e-12)
        assert r["shares_per_dollar"] == pytest.approx(1 / 75.36, abs=1e-12)
        assert r["legs"] == "GM:1:75.36"

    def test_marks_match_hedged_reporter_leg(self, outright, rows):
        # hedged eq-weight 'all' row = reporter leg - mean(peer legs); the
        # reporter leg at 9:35 is exactly the outright 9:35 mark
        from rules.sessions import consolidate
        bars = consolidate(pd.read_parquet(require(FIXTURE)))
        b = bars[(bars["symbol"] == "GM") & (bars["et_date"] == "2026-07-21") & (bars["et_min"] <= 575)]
        assert outright[0]["pnl_0935"] == pytest.approx(float(b.iloc[-1]["close"]) / 75.36 - 1, abs=1e-12)

    def test_default_grid_unchanged(self, rows):
        assert len(rows) == 16 and not any(r["construction"] == "reporter_only" for r in rows)

    def test_mixed_selection_orders_and_dedups_universes(self):
        rows = evaluate_event(pd.read_parquet(require(FIXTURE)),
                              constructions=["equal_weight_dollar_neutral", "reporter_only"])
        assert len(rows) == 4 + 1
        assert [r["universe"] for r in rows if r["construction"] == "reporter_only"] == ["none"]
