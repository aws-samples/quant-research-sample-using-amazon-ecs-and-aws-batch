"""Phase-1 tests pinned on event 1203531964 (GM, released 2026-07-21 10:30 UTC
= 06:30 ET pre-market; trade day 2026-07-21).

Fixture: GM (reporter) + F (pure play) + TSLA (functional) + HON (correlated)
from the XNAS.BASIC production panel (DBEQ.BASIC is retired — its pre-market
coverage was too thin: 3 GM pre-market prints vs XNAS's 208).

Golden marks extracted 2026-08-04 from XNAS.BASIC (single venue, id 93):
  GM:   prev_close=75.86  9:28 bar close=75.28  9:30 open=75.36  exit=79.51
        gap = 75.28/75.86-1 = -0.7646% -> outside +/-0.5% band -> LONG
  TSLA: entry=371.34 exit=378.90 | F: entry=14.15 exit=14.25
  HON:  entry=227.87 exit=229.81
All four symbols have real 9:30 bars and pre-market prints in XNAS, so the
fallback / forward-fill / no-premarket paths are covered synthetically by
mutating the fixture (dropping bars) rather than by a lucky thin symbol.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _fixtures import require  # noqa: E402

from rules.sessions import (consolidate, trade_day_for_event, previous_close,
                            signal_price, entry_price, exit_price)
from rules.direction import fade_the_gap, Signal

FIXTURE = Path(__file__).parent / "fixtures" / "panel_1203531964_subset.parquet"
DAY = "2026-07-21"


@pytest.fixture(scope="module")
def bars():
    return consolidate(pd.read_parquet(require(FIXTURE)))


class TestConsolidate:
    def test_one_row_per_symbol_minute(self, bars):
        assert not bars.duplicated(["symbol", "ts"]).any()

    def test_synthetic_markers_dropped(self, bars):
        assert bars["open"].notna().all()


class TestTradeDayMapping:
    def test_premarket_maps_to_same_day(self):
        assert trade_day_for_event(pd.Timestamp("2026-07-21 10:30:00", tz="UTC")) == DAY

    def test_after_hours_maps_to_next_session(self):
        # 21:00 UTC 7/21 = 17:00 ET -> trade 7/22
        assert trade_day_for_event(pd.Timestamp("2026-07-21 21:00:00", tz="UTC")) == "2026-07-22"

    def test_friday_after_hours_maps_to_monday(self):
        # Fri 7/17 17:00 ET -> Mon 7/20
        assert trade_day_for_event(pd.Timestamp("2026-07-17 21:00:00", tz="UTC")) == "2026-07-20"

    def test_rth_release_excluded(self):
        # 14:00 UTC = 10:00 ET, in-session
        assert trade_day_for_event(pd.Timestamp("2026-07-21 14:00:00", tz="UTC")) is None


class TestMarks:
    def test_previous_close(self, bars):
        assert previous_close(bars, "GM", DAY) == 75.86

    def test_signal_is_exact_928_bar(self, bars):
        assert signal_price(bars, "GM", DAY) == 75.28

    def test_signal_falls_back_to_last_premarket_print(self, bars):
        # drop GM's 9:25-9:28 bars -> the 9:24 close becomes the signal
        cut = bars[~((bars["symbol"] == "GM") & (bars["et_date"] == DAY)
                     & bars["et_min"].between(565, 568))]
        want = cut[(cut["symbol"] == "GM") & (cut["et_date"] == DAY)
                   & (cut["et_min"] < 565)].iloc[-1]["close"]
        assert signal_price(cut, "GM", DAY) == float(want)

    def test_signal_none_when_no_premarket(self, bars):
        cut = bars[~((bars["symbol"] == "GM") & (bars["et_date"] == DAY)
                     & (bars["et_min"] < 570))]
        assert signal_price(cut, "GM", DAY) is None

    def test_reporter_entry_is_930_open(self, bars):
        assert entry_price(bars, "GM", DAY, allow_ffill=False) == 75.36

    def test_peer_entries_at_930_open(self, bars):
        assert entry_price(bars, "TSLA", DAY, allow_ffill=True) == 371.34
        assert entry_price(bars, "F", DAY, allow_ffill=True) == 14.15
        assert entry_price(bars, "HON", DAY, allow_ffill=True) == 227.87

    def test_peer_entry_forward_fills_when_930_missing(self, bars):
        cut = bars[~((bars["symbol"] == "F") & (bars["et_date"] == DAY)
                     & (bars["et_min"] == 570))]
        v = entry_price(cut, "F", DAY, allow_ffill=True)
        prior = cut[(cut["symbol"] == "F")
                    & ((cut["et_date"] < DAY)
                       | ((cut["et_date"] == DAY) & (cut["et_min"] < 570)))]
        assert v == float(prior.iloc[-1]["close"])

    def test_reporter_entry_none_without_930(self, bars):
        cut = bars[~((bars["symbol"] == "GM") & (bars["et_date"] == DAY)
                     & (bars["et_min"] == 570))]
        assert entry_price(cut, "GM", DAY, allow_ffill=False) is None

    def test_exit_prices(self, bars):
        assert exit_price(bars, "GM", DAY) == 79.51
        assert exit_price(bars, "TSLA", DAY) == 378.90
        assert exit_price(bars, "F", DAY) == 14.25
        assert exit_price(bars, "HON", DAY) == 229.81


class TestFadeTheGap:
    def test_gm_event_is_long(self, bars):
        # XNAS signal: gap -0.7646% -> outside the band -> fade = LONG reporter
        sig = fade_the_gap(signal_price(bars, "GM", DAY), previous_close(bars, "GM", DAY))
        assert isinstance(sig, Signal)
        assert sig.direction == +1 and sig.reason == "long"
        assert round(sig.gap, 6) == round(75.28 / 75.86 - 1, 6)

    def test_gap_up_shorts_reporter(self):
        assert fade_the_gap(101.0, 100.0).direction == -1

    def test_gap_down_longs_reporter(self):
        assert fade_the_gap(99.0, 100.0).direction == +1

    def test_band_is_inclusive(self):
        assert fade_the_gap(100.5, 100.0).direction == 0
        assert fade_the_gap(99.5, 100.0).direction == 0
        assert fade_the_gap(100.5001, 100.0).direction == -1

    def test_missing_inputs(self):
        assert fade_the_gap(None, 100.0) is None
        assert fade_the_gap(100.0, None) is None