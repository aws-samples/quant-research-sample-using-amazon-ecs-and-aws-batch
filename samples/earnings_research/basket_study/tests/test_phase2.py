"""Phase-2 tests: constructions on the GM fixture (event 1203531964, XNAS).

Fixture peers: F (pure play), TSLA (functional), HON (correlated).
Direction is +1 (gap -0.76% -> long GM), so all hedge legs must be negative.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _fixtures import require  # noqa: E402

from rules.sessions import consolidate
from construction.base import Basket, BasketContext, universe_symbols
from construction import equal_weight, ridge

FIXTURE = Path(__file__).parent / "fixtures" / "panel_1203531964_subset.parquet"
DAY = "2026-07-21"
PEERS = {"pure play": ["F"], "functional": ["TSLA"], "correlated": ["HON"]}


@pytest.fixture(scope="module")
def ctx():
    bars = consolidate(pd.read_parquet(require(FIXTURE)))
    return BasketContext(bars=bars, reporter="GM", peers=PEERS,
                         trade_day=DAY, direction=+1)


class TestUniverse:
    def test_all_unions_buckets(self):
        assert universe_symbols(PEERS, "all", "GM") == ["F", "HON", "TSLA"]

    def test_single_bucket(self):
        assert universe_symbols(PEERS, "functional", "GM") == ["TSLA"]

    def test_reporter_never_a_peer(self):
        assert "GM" not in universe_symbols({"pure play": ["GM", "F"]}, "all", "GM")

    def test_unknown_universe_raises(self):
        with pytest.raises(ValueError):
            universe_symbols(PEERS, "bogus", "GM")


class TestEqualWeight:
    def test_dollar_neutral_all(self, ctx):
        b = equal_weight.build(ctx, "all")
        assert b.weights["GM"] == 1.0
        hedge = {s: w for s, w in b.weights.items() if s != "GM"}
        assert set(hedge) == {"F", "TSLA", "HON"}
        assert all(w == pytest.approx(-1/3) for w in hedge.values())
        assert sum(hedge.values()) == pytest.approx(-1.0)   # dollar-neutral

    def test_single_peer_universe(self, ctx):
        b = equal_weight.build(ctx, "functional")
        assert b.weights == {"GM": 1.0, "TSLA": -1.0}

    def test_direction_flips_all_signs(self, ctx):
        short_ctx = BasketContext(bars=ctx.bars, reporter="GM", peers=PEERS,
                                  trade_day=DAY, direction=-1)
        b = equal_weight.build(short_ctx, "all")
        assert b.weights["GM"] == -1.0
        assert all(w == pytest.approx(1/3) for s, w in b.weights.items() if s != "GM")

    def test_empty_universe_skips(self, ctx):
        b = equal_weight.build(BasketContext(bars=ctx.bars, reporter="GM",
                                             peers={}, trade_day=DAY, direction=1), "all")
        assert b.weights == {} and "skip" in b.diagnostics


class TestRidge:
    def test_reporter_weight_is_direction(self, ctx):
        b = ridge.build(ctx, "all")
        assert b.weights["GM"] == 1.0

    def test_hedge_legs_all_opposite(self, ctx):
        b = ridge.build(ctx, "all")
        assert all(w < 0 for s, w in b.weights.items() if s != "GM")

    def test_fits_on_pre_event_rth_only(self, ctx):
        # fitting matrix must exclude the trade day and ETH bars: with the
        # trade day removed from the fixture the fit is unchanged
        cut = ctx.bars[ctx.bars["et_date"] < DAY]
        b_cut = ridge.build(BasketContext(bars=cut, reporter="GM", peers=PEERS,
                                          trade_day=DAY, direction=1), "all")
        b_full = ridge.build(ctx, "all")
        assert b_cut.weights == pytest.approx(b_full.weights)

    def test_hedge_gross_within_clamp(self, ctx):
        for univ in ["all", "pure play", "functional", "correlated"]:
            b = ridge.build(ctx, univ)
            if not b.weights:
                continue
            gross = sum(abs(w) for s, w in b.weights.items() if s != "GM")
            assert ridge.CLAMP_LO - 1e-9 <= gross <= ridge.CLAMP_HI + 1e-9
            assert b.diagnostics["hedge_gross"] == pytest.approx(gross)

    def test_clamp_fires_and_is_logged(self, ctx):
        # single-peer universes on this fixture fit gross < 0.5 -> clamp up
        b = ridge.build(ctx, "functional")
        assert b.diagnostics["clamped"] is True
        assert sum(abs(w) for s, w in b.weights.items() if s != "GM") == pytest.approx(0.5)

    def test_insufficient_data_skips(self, ctx):
        thin = ctx.bars[ctx.bars["et_date"] >= DAY]   # no pre-event days left
        b = ridge.build(BasketContext(bars=thin, reporter="GM", peers=PEERS,
                                      trade_day=DAY, direction=1), "all")
        assert b.weights == {} and "skip" in b.diagnostics

    def test_negative_clip_refit_drops_peer(self, ctx):
        # engineer an anti-correlated peer: GM's returns mirrored -> its ridge
        # coefficient is negative -> must be clipped out by refit
        gm = ctx.bars[ctx.bars["symbol"] == "GM"].copy()
        gm["symbol"] = "ANTI"
        # mirror the price path around its own start so returns negate
        first = float(gm.iloc[0]["close"])
        for col in ("open", "close", "high", "low"):
            gm[col] = 2 * first - gm[col].astype(float)
        bars2 = pd.concat([ctx.bars, gm], ignore_index=True)
        peers2 = {"pure play": ["F", "ANTI"], "functional": ["TSLA"], "correlated": ["HON"]}
        b = ridge.build(BasketContext(bars=bars2, reporter="GM", peers=peers2,
                                      trade_day=DAY, direction=1), "all")
        assert "ANTI" in b.diagnostics["dropped_negative"]
        assert "ANTI" not in b.weights