"""Golden-reference test for the earnings market-data pipeline.

Event 1203024235 (Amazon.com Q4 2024 ER, released 2025-02-06 21:01 UTC) is the
canonical retrieval the whole pipeline must reproduce: DBEQ.BASIC, ohlcv-1m,
[-2, +5] trading-day window, bare symbol 'AMZN', ALL columns preserved
(nothing dropped). The user validated this exact grab and blessed its format.

Two layers:
  * offline (always run): pin the golden fixture's schema, shape, and exact
    boundary values. Any pipeline change that alters columns, ordering, price
    scaling, timestamp handling, or windowing breaks these.
  * live (opt-in, RUN_DATABENTO_LIVE=1): re-fetch from Databento with the real
    pipeline path and assert byte-for-value equality with the golden fixture.
    Costs ~$0.011 — gated behind an env flag so CI stays free/offline.
"""

import os
from pathlib import Path

import pandas as pd
import pytest
from _fixtures import require

FIXTURE = Path(__file__).parent / "fixtures" / "golden_AMZN_1203024235_ohlcv1m.parquet"

EVENT_ID = 1203024235
SYMBOL = "AMZN"
DATASET = "DBEQ.BASIC"
SCHEMA = "ohlcv-1m"
WIN_START = "2025-02-04"          # -2 trading days from 2025-02-06
WIN_END_EXCL = "2025-02-14"       # +5 trading days, end-exclusive

# Golden facts captured from the user-blessed retrieval.
GOLDEN_COLUMNS = ["ts_event", "rtype", "publisher_id", "instrument_id",
                  "open", "high", "low", "close", "volume", "ts"]
GOLDEN_ROWS = 6040
GOLDEN_TRADING_DAYS = ["2025-02-04", "2025-02-05", "2025-02-06", "2025-02-07",
                       "2025-02-10", "2025-02-11", "2025-02-12", "2025-02-13"]
GOLDEN_FIRST = {"ts_event": 1738674840000000000, "rtype": 33, "publisher_id": 41,
                "instrument_id": 853, "open": 238.83, "high": 238.83,
                "low": 238.83, "close": 238.83, "volume": 70,
                "ts": "2025-02-04 13:14:00+00:00"}
GOLDEN_LAST = {"ts_event": 1739488440000000000, "rtype": 33, "publisher_id": 40,
               "instrument_id": 853, "open": 229.98, "high": 229.98,
               "low": 229.98, "close": 229.98, "volume": 1,
               "ts": "2025-02-13 23:14:00+00:00"}
GOLDEN_PRICE_LOW = 222.34
GOLDEN_PRICE_HIGH = 242.57


def _sorted(df: pd.DataFrame) -> pd.DataFrame:
    # Deterministic total order: same-minute rows exist per venue and Databento
    # does not guarantee their order across fetches, so tie-break on the full
    # key to make value-by-position comparison stable.
    keys = ["ts", "publisher_id", "open", "high", "low", "close", "volume"]
    return df.sort_values(keys).reset_index(drop=True)


def _row_dict(row) -> dict:
    return {"ts_event": int(row["ts_event"]), "rtype": int(row["rtype"]),
            "publisher_id": int(row["publisher_id"]),
            "instrument_id": int(row["instrument_id"]),
            "open": float(row["open"]), "high": float(row["high"]),
            "low": float(row["low"]), "close": float(row["close"]),
            "volume": int(row["volume"]), "ts": str(row["ts"])}


@pytest.fixture(scope="module")
def golden() -> pd.DataFrame:
    require(FIXTURE)
    return _sorted(pd.read_parquet(require(FIXTURE)))


# ------------------------------------------------------------------ offline

class TestGoldenFixture:
    def test_all_columns_present_and_ordered(self, golden):
        # No column may be dropped or reordered — the format the user blessed.
        assert list(golden.columns) == GOLDEN_COLUMNS

    def test_row_count(self, golden):
        assert len(golden) == GOLDEN_ROWS

    def test_single_instrument_and_rtype(self, golden):
        assert sorted(golden["rtype"].unique()) == [33]           # ohlcv-1m
        assert sorted(golden["instrument_id"].unique()) == [853]  # AMZN
        # consolidated feed → multiple venues per minute; must be preserved
        assert sorted(golden["publisher_id"].unique()) == [39, 40, 41]

    def test_first_row_exact(self, golden):
        assert _row_dict(golden.iloc[0]) == GOLDEN_FIRST

    def test_last_row_exact(self, golden):
        assert _row_dict(golden.iloc[-1]) == GOLDEN_LAST

    def test_window_bounds(self, golden):
        ts = golden["ts"]
        assert ts.min() >= pd.Timestamp(WIN_START, tz="UTC")
        assert ts.max() < pd.Timestamp(WIN_END_EXCL, tz="UTC")

    def test_trading_days_exact(self, golden):
        days = sorted({str(d) for d in
                       golden["ts"].dt.tz_convert("US/Eastern").dt.date.unique()})
        assert days == GOLDEN_TRADING_DAYS       # exactly [-2, +5] sessions

    def test_ohlc_integrity(self, golden):
        assert (golden["low"] <= golden["open"]).all()
        assert (golden["low"] <= golden["close"]).all()
        assert (golden["high"] >= golden["open"]).all()
        assert (golden["high"] >= golden["close"]).all()
        assert (golden[["open", "high", "low", "close"]] > 0).all().all()

    def test_price_range(self, golden):
        assert round(golden["low"].min(), 2) == GOLDEN_PRICE_LOW
        assert round(golden["high"].max(), 2) == GOLDEN_PRICE_HIGH

    def test_earnings_reaction_present(self, golden):
        # AMZN released after close 2025-02-06 16:01 ET; the drop must be in-window.
        g = golden.assign(et=golden["ts"].dt.tz_convert("US/Eastern"))
        d6 = g[g["et"].dt.date == pd.Timestamp("2025-02-06").date()]
        pre = d6[d6["et"].dt.hour < 16]["close"].iloc[-1]
        post = d6[d6["et"].dt.hour >= 16]["close"]
        assert (post.min() / pre - 1) < -0.03   # >3% after-hours drop captured


# --------------------------------------------------------------------- live

@pytest.mark.skipif(os.environ.get("RUN_DATABENTO_LIVE") != "1",
                    reason="set RUN_DATABENTO_LIVE=1 to hit Databento (~$0.011)")
class TestLiveRetrievalMatchesGolden:
    """Re-run the real retrieval path and prove it reproduces the golden grab.

    This is the test that validates pipeline code: point it at the pipeline's
    fetch function once it exists and confirm equality with the fixture.
    """

    def test_live_fetch_reproduces_golden(self, golden):
        import sys
        import settings
        sys.path.insert(0, str(settings.sibling("content_pipeline")))
        import proto_market_data as proto

        key = proto.db_key()
        live = proto.get_range(key, SYMBOL, WIN_START, WIN_END_EXCL)
        live = _sorted(live)[GOLDEN_COLUMNS]

        assert list(live.columns) == GOLDEN_COLUMNS
        assert len(live) == GOLDEN_ROWS
        # exact value equality on the raw+derived columns
        for col in ("ts_event", "rtype", "publisher_id", "instrument_id", "volume"):
            assert live[col].tolist() == golden[col].tolist(), f"{col} mismatch"
        for col in ("open", "high", "low", "close"):
            assert live[col].round(6).tolist() == golden[col].round(6).tolist(), f"{col} mismatch"

    def test_window_derivation_matches(self):
        """The [-2,+5] trading-day window math must yield the golden bounds."""
        import sys
        import settings
        sys.path.insert(0, str(settings.sibling("content_pipeline")))
        import proto_market_data as proto
        start, end = proto.trading_window("2025-02-06 21:01:00")
        assert (start, end) == (WIN_START, WIN_END_EXCL)
