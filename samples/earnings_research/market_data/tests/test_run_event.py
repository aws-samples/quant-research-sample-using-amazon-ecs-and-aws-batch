"""Unit tests for the single-event shard path (offline).

run_event is one array shard: build one event's basket panel and write it,
idempotently. A failure isolates to this shard (the array child).
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import basket_fetch


ENTRY = {
    "index": 0,
    "date": "2025-02-06",
    "event_id": 1203024235,
    "symbol": "AMZN",
    "entity_proper_name": "Amazon.com, Inc.",
    "ticker_region": "AMZN-US",
    "event_datetime_utc": "2025-02-06 21:01:00",
    "event_minute": "2025-02-06 21:01:00+00:00",
}


def _no_such_key_s3():
    """MagicMock s3 whose get_object raises its own NoSuchKey class."""
    s3 = MagicMock()

    class NoSuchKey(Exception):
        pass

    s3.exceptions.NoSuchKey = NoSuchKey
    s3.get_object.side_effect = NoSuchKey("absent")
    return s3


def _panel_bytes(df: pd.DataFrame) -> dict:
    import io
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    body = MagicMock()
    body.read.return_value = buf.getvalue()
    return {"Body": body}


class TestRunEvent:
    def test_writes_panel_to_event_partition(self):
        s3 = _no_such_key_s3()                       # no existing panel
        cache = MagicMock()
        deriver = MagicMock()
        panel = pd.DataFrame({"symbol": ["AMZN"], "ts": ["x"],
                              "publisher_id": [1], "open": [1.0],
                              "close": [1.0]})

        with patch.object(basket_fetch, "_basket_peers",
                          return_value={"pure_play": ["FOO"]}), \
             patch("panel_builder.build_panel_symbols", return_value=panel):
            res = basket_fetch.run_event(ENTRY, "buck", "pfx",
                                         MagicMock(), cache, deriver, s3)

        assert res["panels_written"] == 1
        assert res["event_id"] == 1203024235
        assert res["basket_size"] == 2               # reporter + 1 peer
        assert res["symbols_fetched"] == 2           # full basket fetched
        key = s3.put_object.call_args.kwargs["Key"]
        assert key == "pfx/panels/date=2025-02-06/event_1203024235.parquet"

    def test_idempotent_skip_when_panel_complete(self):
        s3 = MagicMock()
        # existing panel already has REAL rows for the whole basket
        existing = pd.DataFrame({"symbol": ["AMZN", "FOO"],
                                 "ts": ["x", "x"], "publisher_id": [1, 1],
                                 "open": [1.0, 2.0]})
        s3.get_object.return_value = _panel_bytes(existing)
        with patch.object(basket_fetch, "_basket_peers",
                          return_value={"pure_play": ["FOO"]}), \
             patch("panel_builder.build_panel_symbols") as bps:
            res = basket_fetch.run_event(ENTRY, "buck", "pfx",
                                         MagicMock(), MagicMock(), MagicMock(), s3)
        assert res["skipped_existing"] is True
        bps.assert_not_called()
        s3.put_object.assert_not_called()

    def test_per_symbol_resume_fetches_only_missing(self):
        s3 = MagicMock()
        # existing panel: AMZN real, FOO synthetic-only (open null)
        existing = pd.DataFrame({"symbol": ["AMZN", "FOO"],
                                 "ts": ["x", "x"], "publisher_id": [1, 1],
                                 "open": [1.0, None]})
        s3.get_object.return_value = _panel_bytes(existing)
        fetched = pd.DataFrame({"symbol": ["FOO"], "ts": ["y"],
                                "publisher_id": [1], "open": [3.0]})
        with patch.object(basket_fetch, "_basket_peers",
                          return_value={"pure_play": ["FOO"]}), \
             patch("panel_builder.build_panel_symbols",
                   return_value=fetched) as bps:
            res = basket_fetch.run_event(ENTRY, "buck", "pfx",
                                         MagicMock(), MagicMock(), MagicMock(), s3)
        assert res["symbols_fetched"] == 1           # only FOO refetched
        assert bps.call_args.args[3] == [("FOO", "pure_play")]
        assert res["panels_written"] == 1
        s3.put_object.assert_called_once()

    def test_estimate_only_prices_basket_no_write(self):
        s3 = MagicMock()
        db = MagicMock()
        db.get_cost.return_value = 0.01
        with patch.object(basket_fetch, "_basket_peers",
                          return_value={"pure_play": ["FOO", "BAR"]}):
            res = basket_fetch.run_event(ENTRY, "buck", "pfx",
                                         db, MagicMock(), MagicMock(), s3,
                                         estimate_only=True)
        assert res["basket_size"] == 3               # reporter + 2 peers
        assert res["estimated_cost_usd"] == 0.03
        s3.put_object.assert_not_called()
        s3.head_object.assert_not_called()          # estimate skips existence check
