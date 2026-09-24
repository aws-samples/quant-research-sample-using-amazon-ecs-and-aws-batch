"""Basket-admission rule: the coverage floor is applied ONCE, before any
construction sees the peer dict — every construction fits/weights the same
admissible peer set, so grid cells differ only in weighting scheme, never in
peer-admission policy.

`admissible_peers` lives in construction/base.py next to universe_symbols
(the docstring there already declares shared peer filtering belongs in base,
not per-construction). COVERAGE_FLOOR moves to base; ridge re-exports it and
keeps its internal filter as defense-in-depth for direct callers.

Fixture: FCFS event 1203405504 — CPSS at 33% pre-event RTH coverage must be
inadmissible for ALL constructions, not just the ridge fit.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _fixtures import require  # noqa: E402

from rules.sessions import consolidate
from construction import base, equal_weight, ridge
from construction.base import BasketContext, admissible_peers

FIXTURES = Path(__file__).parent / "fixtures"

FCFS_DAY = "2026-07-23"
FCFS_PEERS = {"pure play": ["EZPW", "CPSS"], "functional": ["OMF"],
              "correlated": ["COF"]}

GM_DAY = "2026-07-21"
GM_PEERS = {"pure play": ["F"], "functional": ["TSLA"], "correlated": ["HON"]}


@pytest.fixture(scope="module")
def fcfs_bars():
    return consolidate(pd.read_parquet(
        require(FIXTURES / "panel_1203405504_subset.parquet")))


@pytest.fixture(scope="module")
def gm_bars():
    return consolidate(pd.read_parquet(
        require(FIXTURES / "panel_1203531964_subset.parquet")))


class TestFloorConstantLivesInBase:
    def test_base_owns_the_floor(self):
        assert base.COVERAGE_FLOOR == 0.80

    def test_ridge_reexports_it(self):
        # one source of truth: ridge's floor IS base's floor
        assert ridge.COVERAGE_FLOOR is base.COVERAGE_FLOOR


class TestAdmissiblePeers:
    def test_thin_peer_removed_from_its_bucket(self, fcfs_bars):
        peers, dropped = admissible_peers(fcfs_bars, FCFS_PEERS, FCFS_DAY,
                                          reporter="FCFS")
        assert peers == {"pure play": ["EZPW"], "functional": ["OMF"],
                         "correlated": ["COF"]}

    def test_dropped_coverage_logged(self, fcfs_bars):
        _, dropped = admissible_peers(fcfs_bars, FCFS_PEERS, FCFS_DAY,
                                      reporter="FCFS")
        assert set(dropped) == {"CPSS"}
        assert 0.30 <= dropped["CPSS"] <= 0.36

    def test_all_liquid_is_identity(self, gm_bars):
        peers, dropped = admissible_peers(gm_bars, GM_PEERS, GM_DAY,
                                          reporter="GM")
        assert peers == GM_PEERS
        assert dropped == {}

    def test_reporter_never_dropped(self, fcfs_bars):
        # CPSS as reporter: admission filters PEERS only; the thin reporter
        # is downstream's problem (constructions/marks skip it themselves)
        peers, dropped = admissible_peers(
            fcfs_bars, {"pure play": ["FCFS", "CPSS"]}, FCFS_DAY,
            reporter="CPSS")
        assert "CPSS" not in dropped
        assert peers == {"pure play": ["FCFS"]}

    def test_peer_with_no_bars_dropped_at_zero(self, fcfs_bars):
        # a symbol absent from the panel entirely = coverage 0.0 — admission
        # subsumes equal_weight's old dropped_no_bars check
        peers, dropped = admissible_peers(
            fcfs_bars, {"pure play": ["EZPW", "GHOST"]}, FCFS_DAY,
            reporter="FCFS")
        assert dropped["GHOST"] == 0.0
        assert peers == {"pure play": ["EZPW"]}

    def test_bucket_emptied_not_deleted(self, fcfs_bars):
        # a bucket whose only peer is inadmissible stays present but empty,
        # so universe iteration still yields a clean construction skip
        peers, _ = admissible_peers(fcfs_bars, {"pure play": ["CPSS"]},
                                    FCFS_DAY, reporter="FCFS")
        assert peers == {"pure play": []}


class TestConstructionsSeeSameBasket:
    def test_equal_weight_excludes_thin_peer(self, fcfs_bars):
        peers, _ = admissible_peers(fcfs_bars, FCFS_PEERS, FCFS_DAY,
                                    reporter="FCFS")
        ctx = BasketContext(bars=fcfs_bars, reporter="FCFS", peers=peers,
                            trade_day=FCFS_DAY, direction=-1)
        b = equal_weight.build(ctx, "all")
        assert "CPSS" not in b.weights
        assert set(b.weights) == {"FCFS", "EZPW", "OMF", "COF"}
        hedge = [w for s, w in b.weights.items() if s != "FCFS"]
        assert all(w == pytest.approx(1 / 3) for w in hedge)  # 1/3, not 1/4

    def test_ridge_and_equal_weight_same_admissible_set(self, fcfs_bars):
        peers, _ = admissible_peers(fcfs_bars, FCFS_PEERS, FCFS_DAY,
                                    reporter="FCFS")
        ctx = BasketContext(bars=fcfs_bars, reporter="FCFS", peers=peers,
                            trade_day=FCFS_DAY, direction=-1)
        ew = equal_weight.build(ctx, "all")
        rg = ridge.build(ctx, "all")
        ew_peers = {s for s in ew.weights if s != "FCFS"}
        rg_peers = {s for s in rg.weights if s != "FCFS"} | \
            set(rg.diagnostics["dropped_negative"])
        assert ew_peers == rg_peers          # same admission, differ only in weighting
        assert rg.diagnostics["dropped_low_coverage"] == {}  # nothing left to drop
