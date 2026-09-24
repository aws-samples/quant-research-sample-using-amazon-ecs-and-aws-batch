"""Golden-reference test for the earnings peer-panel event study.

For event 1203024235 (Amazon Q4 2024 ER, released 2025-02-06 21:01 UTC) this
pins the full peer-panel shape the pipeline must reproduce:

  * reporter AMZN + 16 peers, each labeled by reporter_relationship
    (primary | pure play | functional | correlated)
  * every row carries the same event_id (relatable by event)
  * every symbol has exactly one event marker at the fixed 21:01 UTC:
      - organic  : symbols that traded at 21:01 -> their real bar(s) flagged
                   event_bar=True, market data intact
      - synthetic: symbols with no 21:01 print -> one null-filled marker row
                   inserted at 21:01 with event_bar=True
  * no symbol has BOTH an organic and a synthetic marker
  * all event_bar timestamps are exactly the event time (never drift)

All offline against the committed fixture; no network.
"""

from pathlib import Path

import pandas as pd
import pytest
from _fixtures import require

FIXTURE = Path(__file__).parent / "fixtures" / "golden_peer_panel_1203024235.parquet"

EVENT_ID = 1203024235
EVENT_TS = pd.Timestamp("2025-02-06 21:01:00", tz="UTC")
EVENT_NS = int(EVENT_TS.value)

COLUMNS = ["symbol", "ts_event", "rtype", "publisher_id", "instrument_id",
           "open", "high", "low", "close", "volume", "ts",
           "reporter_relationship", "event_id", "event_bar"]

RELATIONSHIPS = {
    "primary": ["AMZN"],
    "pure play": ["CHWY", "EBAY", "ETSY", "MELI", "SHOP", "W"],
    "functional": ["COST", "GOOGL", "MSFT", "NFLX", "TGT", "WMT"],
    "correlated": ["AAPL", "META", "NVDA", "QQQ"],
}
ALL_SYMBOLS = sorted(s for v in RELATIONSHIPS.values() for s in v)

ORGANIC = ["AMZN", "MSFT", "NVDA", "QQQ"]           # traded at 21:01
SYNTHETIC = [s for s in ALL_SYMBOLS if s not in ORGANIC]
# multi-venue real bars at 21:01 give organic symbols >1 marker row
EVENT_BAR_COUNTS = {"AMZN": 3, "MSFT": 2, "QQQ": 2}  # others: 1


@pytest.fixture(scope="module")
def panel():
    require(FIXTURE)
    return pd.read_parquet(require(FIXTURE))


class TestSchema:
    def test_columns_present_and_ordered(self, panel):
        assert list(panel.columns) == COLUMNS

    def test_symbol_universe(self, panel):
        assert sorted(panel["symbol"].unique()) == ALL_SYMBOLS


class TestEventId:
    def test_event_id_on_every_row(self, panel):
        # relatable by event: one id, no nulls, on all rows
        assert panel["event_id"].notna().all()
        assert panel["event_id"].nunique() == 1
        assert int(panel["event_id"].iloc[0]) == EVENT_ID


class TestRelationships:
    def test_each_symbol_single_relationship(self, panel):
        per = panel.groupby("symbol")["reporter_relationship"].nunique()
        assert (per == 1).all()

    @pytest.mark.parametrize("rel,syms", RELATIONSHIPS.items())
    def test_relationship_membership(self, panel, rel, syms):
        got = sorted(panel[panel["reporter_relationship"] == rel]["symbol"].unique())
        assert got == sorted(syms)

    def test_exactly_one_primary(self, panel):
        prim = panel[panel["reporter_relationship"] == "primary"]["symbol"].unique()
        assert list(prim) == ["AMZN"]


class TestEventBar:
    def test_every_symbol_has_event_bar(self, panel):
        marked = set(panel[panel["event_bar"]]["symbol"])
        assert marked == set(ALL_SYMBOLS)

    def test_all_event_bars_at_event_time(self, panel):
        eb = panel[panel["event_bar"]]
        assert (eb["ts_event"] == EVENT_NS).all()
        assert (eb["ts"] == EVENT_TS).all()
        assert eb["ts"].nunique() == 1

    def test_organic_bars_are_real(self, panel):
        # organic symbols: event_bar rows carry real market data (rtype not null)
        eb = panel[panel["event_bar"]]
        for sym in ORGANIC:
            rows = eb[eb["symbol"] == sym]
            assert rows["rtype"].notna().all(), f"{sym} organic bar should be real"
            assert rows["open"].notna().all()

    def test_synthetic_bars_are_null(self, panel):
        # synthetic symbols: single marker row, market fields null
        eb = panel[panel["event_bar"]]
        for sym in SYNTHETIC:
            rows = eb[eb["symbol"] == sym]
            assert len(rows) == 1, f"{sym} should have exactly one synthetic marker"
            assert rows[["rtype", "open", "high", "low", "close", "volume"]].isna().all().all()

    def test_no_symbol_has_both_organic_and_synthetic(self, panel):
        eb = panel[panel["event_bar"]]
        for sym in ALL_SYMBOLS:
            rows = eb[eb["symbol"] == sym]
            has_real = rows["rtype"].notna().any()
            has_synth = rows["rtype"].isna().any()
            assert not (has_real and has_synth), f"{sym} mixes organic + synthetic"

    def test_event_bar_counts(self, panel):
        eb = panel[panel["event_bar"]]
        counts = eb.groupby("symbol").size().to_dict()
        for sym in ALL_SYMBOLS:
            assert counts[sym] == EVENT_BAR_COUNTS.get(sym, 1), \
                f"{sym} event_bar count {counts[sym]}"


class TestReporterReaction:
    def test_amzn_event_bar_shows_the_drop(self, panel):
        # the reporter's real 21:01 bars are the earnings crater
        amzn_eb = panel[(panel["symbol"] == "AMZN") & (panel["event_bar"])]
        assert len(amzn_eb) == 3
        assert amzn_eb["low"].min() < 225.0    # dropped from ~238 pre-release
