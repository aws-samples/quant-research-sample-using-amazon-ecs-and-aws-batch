"""Corr-peer arm (2026-10-11): hedge legs = the reporter's HARDCODED correlation peers
(rules/corr_peersets_top10.csv, pooled top 10 from the full1 RTH correlations) instead of the
LLM peer buckets. Bars come from the S&P 1500 universe day store and are attached at evaluation
time under reporter_relationship / universe "corr" (pattern: spy_bars). The four hedged
constructions run unchanged against that universe; everything else in the study is untouched.

Fixture: tests/fixtures/corr_bars_1203531964.parquet = universe-store rows for GM's 10 peers,
2026-07-17..2026-07-28 (the GM golden panel's window)."""
import sys
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import corr_peers_bars as cpb  # noqa: E402
import spy_bars  # noqa: E402
from construction.base import UNIVERSES, universe_symbols  # noqa: E402
from construction.registry import CONSTRUCTIONS  # noqa: E402
from evaluate import evaluate_event  # noqa: E402
from _fixtures import require  # noqa: E402

FIX = Path(__file__).parent / "fixtures"
GM_PEERS = ["F", "SOLV", "DAN", "SEIC", "BWA", "USLM", "PHIN", "XYZ", "LNC", "MRTN"]


@pytest.fixture(scope="module")
def panel0():
    return pd.read_parquet(require(FIX / "panel_1203531964_subset.parquet"))


@pytest.fixture(scope="module")
def bars():
    return pd.read_parquet(require(FIX / "corr_bars_1203531964.parquet"))


@pytest.fixture(scope="module")
def panel(panel0, bars):
    return cpb.attach_corr_peers(panel0, bars)


def test_peersets_csv_is_the_hardcoded_rule():
    ps = cpb.load_peersets(require(cpb.PEERSETS_CSV))
    assert ps["GM"] == GM_PEERS                        # rank order, 10 names
    assert len(ps) == 3889 and all(len(v) == 10 for v in ps.values())
    assert all(r not in v for r, v in ps.items())      # a reporter is never its own peer
    assert cpb.CORR_REL == "corr" and cpb.CORR_UNIVERSE == "corr"


def test_attach_drops_llm_peers_keeps_schema_and_is_idempotent(panel0, bars, panel):
    assert list(panel.columns) == list(panel0.columns)
    assert set(panel["reporter_relationship"]) == {"primary", cpb.CORR_REL}
    corr = panel[panel["reporter_relationship"] == cpb.CORR_REL]
    assert set(corr["symbol"]) == set(GM_PEERS) and len(corr) == len(bars)
    assert not corr["event_bar"].any() and (corr["event_id"] == 1203531964).all()
    assert str(corr["ts"].dtype) == str(panel0["ts"].dtype)
    prim = panel[panel["reporter_relationship"] == "primary"]
    assert len(prim) == (panel0["reporter_relationship"] == "primary").sum()
    again = cpb.attach_corr_peers(panel, bars)
    assert len(again) == len(panel)


def test_attach_keeps_spy_benchmark_rows(panel0, bars):
    with_spy = spy_bars.attach_spy(panel0, pd.read_parquet(require(FIX / "spy_1min_2026-07-17_28.parquet")))
    both = cpb.attach_corr_peers(with_spy, bars)
    assert set(both["reporter_relationship"]) == {"primary", spy_bars.BENCHMARK_REL, cpb.CORR_REL}


def test_attach_for_panel_without_a_peer_set_leaves_no_hedge_legs(panel0):
    out = cpb.attach_corr_peers_for_panel(panel0, s3io=None, peersets={})
    assert set(out["reporter_relationship"]) == {"primary"}


def test_et_days_cover_the_panel_window(panel0):
    start, end = spy_bars.panel_window(panel0)
    days = cpb.et_days(start, end)
    assert days[0] == "2026-07-17" and days[-1] == "2026-07-28" and len(days) == 12
    assert cpb.day_key("2026-07-17") == "earnings-market-data/ALPACA.SIP/universe/sp1500/date=2026-07-17/bars.parquet"


def test_universe_symbols_accepts_corr_but_the_grid_is_unchanged():
    assert universe_symbols({"corr": ["F", "GM", "DAN"]}, "corr", "GM") == ["DAN", "F"]
    assert "corr" not in UNIVERSES and UNIVERSES == ("all", "pure play", "functional", "correlated")
    with pytest.raises(ValueError):
        universe_symbols({}, "nope", "GM")


def test_corr_cells_one_per_hedged_construction(panel, panel0):
    rows = evaluate_event(panel, universes=[cpb.CORR_UNIVERSE])
    assert len(rows) == len(CONSTRUCTIONS) and {r["construction"] for r in rows} == set(CONSTRUCTIONS)
    assert all(r["universe"] == "corr" and r["direction"] == 1 for r in rows)
    ok = [r for r in rows if r["status"] == "ok"]
    assert ok, [r.get("skip_reason") for r in rows]
    for r in ok:
        theo = r["peers_theo"].split()
        assert 0 < len(theo) <= 10 and set(theo) <= set(GM_PEERS) and "GM" not in theo
        assert set(r["peers_actual"].split()) <= set(theo)
    # the LLM-peer grid on the untouched panel is byte-for-byte what it was (16 cells, 4 universes)
    base = evaluate_event(panel0)
    assert len(base) == 16 and {r["universe"] for r in base} == set(UNIVERSES)


def test_no_peer_set_gives_an_explicit_skip(panel0):
    out = cpb.attach_corr_peers_for_panel(panel0, s3io=None, peersets={})
    rows = evaluate_event(out, universes=[cpb.CORR_UNIVERSE])
    assert len(rows) == len(CONSTRUCTIONS)
    assert all(r["status"] == "skip" and r["skip_reason"] == "no_corr_peers" for r in rows)


def test_covered_event_ids_filters_by_reporter():
    ev = pl.DataFrame({"event_id": [1, 2, 3, 3], "reporter": ["GM", "ZZZZ", "F", "F"]})
    assert cpb.covered_event_ids(ev, {"GM": GM_PEERS, "F": ["GM"]}) == {1, 3}
