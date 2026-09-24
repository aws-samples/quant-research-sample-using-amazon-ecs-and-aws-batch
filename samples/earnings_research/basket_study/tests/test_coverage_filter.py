"""Coverage-floor peer filter in the ridge fitting matrix (README decisions log).

Enforces the research policy declared at capture time (earnings_market_data/
panel_builder.py COVERAGE_WARN = 0.80): peers present in fewer than 80% of the
pre-event RTH union minutes are excluded from the fit BEFORE the inner join,
so one illiquid peer cannot starve the matrix for everyone else.

Fixture: FCFS event 1203405504 (2026-07-23), the event that motivated the fix.
CPSS trades only 33% of pre-event RTH minutes; inner-joining it collapses the
matrix from 780 to 258 minutes. GM golden fixture (all-liquid) must be
bit-identical under the filter.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _fixtures import require  # noqa: E402

from rules.sessions import consolidate
from construction.base import BasketContext
from construction import ridge

FIXTURES = Path(__file__).parent / "fixtures"

FCFS_DAY = "2026-07-23"
FCFS_PEERS = {"pure play": ["EZPW", "CPSS"], "functional": ["OMF"],
              "correlated": ["COF"]}

GM_DAY = "2026-07-21"
GM_PEERS = {"pure play": ["F"], "functional": ["TSLA"], "correlated": ["HON"]}


@pytest.fixture(scope="module")
def fcfs_ctx():
    bars = consolidate(pd.read_parquet(
        require(FIXTURES / "panel_1203405504_subset.parquet")))
    return BasketContext(bars=bars, reporter="FCFS", peers=FCFS_PEERS,
                         trade_day=FCFS_DAY, direction=-1)


@pytest.fixture(scope="module")
def gm_ctx():
    bars = consolidate(pd.read_parquet(
        require(FIXTURES / "panel_1203531964_subset.parquet")))
    return BasketContext(bars=bars, reporter="GM", peers=GM_PEERS,
                         trade_day=GM_DAY, direction=+1)


class TestCoverageFloorConstant:
    def test_shared_with_capture_policy(self):
        # one source of truth: the analysis-time floor IS the capture-time
        # warning threshold (panel_builder.COVERAGE_WARN = 0.80)
        assert ridge.COVERAGE_FLOOR == 0.80


class TestLowCoveragePeerExcluded:
    def test_cpss_dropped_from_fit(self, fcfs_ctx):
        b = ridge.build(fcfs_ctx, "all")
        assert "CPSS" not in b.weights
        assert "CPSS" in b.diagnostics["dropped_low_coverage"]

    def test_dropped_coverage_fraction_logged(self, fcfs_ctx):
        b = ridge.build(fcfs_ctx, "all")
        cov = b.diagnostics["dropped_low_coverage"]["CPSS"]
        assert 0.30 <= cov <= 0.36          # CPSS trades ~33% of union minutes

    def test_matrix_no_longer_starved(self, fcfs_ctx):
        # inner join with CPSS: 258 price minutes -> 257 returns.
        # without it the fit must see the near-full union (>=700 returns).
        b = ridge.build(fcfs_ctx, "all")
        assert b.diagnostics["n_minutes"] >= 700

    def test_healthy_peers_unaffected(self, fcfs_ctx):
        b = ridge.build(fcfs_ctx, "all")
        fitted = set(b.diagnostics["dropped_negative"]) | \
            {s for s in b.weights if s != "FCFS"}
        assert fitted == {"EZPW", "OMF", "COF"}


class TestReporterNeverFiltered:
    def test_thin_reporter_skips_not_fits(self, fcfs_ctx):
        # make the reporter itself low-coverage by relabeling: CPSS as
        # reporter with FCFS et al. as peers -> skip, never a filtered fit
        peers = {"pure play": ["EZPW", "FCFS"], "functional": ["OMF"],
                 "correlated": ["COF"]}
        b = ridge.build(BasketContext(bars=fcfs_ctx.bars, reporter="CPSS",
                                      peers=peers, trade_day=FCFS_DAY,
                                      direction=-1), "all")
        assert b.weights == {} and "skip" in b.diagnostics


class TestSinglePeerUniverse:
    def test_only_peer_low_coverage_skips(self, fcfs_ctx):
        # a universe whose sole peer is under the floor must skip cleanly
        peers = {"pure play": ["CPSS"], "functional": [], "correlated": []}
        b = ridge.build(BasketContext(bars=fcfs_ctx.bars, reporter="FCFS",
                                      peers=peers, trade_day=FCFS_DAY,
                                      direction=-1), "pure play")
        assert b.weights == {} and "skip" in b.diagnostics


class TestGoldenEventUnchanged:
    def test_gm_weights_bit_identical(self, gm_ctx):
        # all-liquid basket: the filter must be a no-op. Expected weights are
        # the pre-filter fit on this fixture (pinned 2026-08-05).
        b = ridge.build(gm_ctx, "all")
        assert b.diagnostics["dropped_low_coverage"] == {}
        # pinned from the pre-filter fit on this fixture (2026-08-05)
        expected = {"GM": 1.0, "F": -0.361879041041,
                    "HON": -0.063621004189, "TSLA": -0.168397594415}
        assert b.weights == pytest.approx(expected)
