"""Partial-window fetch: when Databento's available range ends before the
event's +5 session, take whatever is available as long as the window still
covers -2 through at least the +1 session; defer otherwise.

Motivated by the Jul 28-31 2026 stub panels: the [-2,+5] window for those
events ended after XNAS.BASIC's available end, Databento returned 422
data_end_after_available_end, and the client swallowed it as "no data" —
717 panels of synthetic rows, plus 152 FALSE unrecoverable tombstones.

Reference event (AMZN 2025-02-06 21:01 UTC, ER after Thursday's close):
  sessions: -2 = 02-04, anchor = 02-06, +1 = 02-07 (Fri), +5 = 02-13
  full window  = (2025-02-04, 2025-02-14)   # end exclusive
  minimum end  = 2025-02-08                 # +1 session fully covered
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import basket_fetch
from databento_client import DatabentoClient, TransientFetchError
from sharding import clamp_window, trading_window

EVENT_DT = "2025-02-06 21:01:00"


class TestClampWindow:
    def test_full_availability_matches_trading_window(self):
        start, end, clamped = clamp_window(EVENT_DT, "2026-08-05T04:00:00Z")
        assert (start, end) == trading_window(EVENT_DT)
        assert clamped is False

    def test_mid_window_end_clamps(self):
        # available through 02-10 18:00 -> floor to 02-10: sessions -2..+2
        start, end, clamped = clamp_window(EVENT_DT, "2025-02-10T18:00:00Z")
        assert start == "2025-02-04"          # -2 untouched
        assert end == "2025-02-10"
        assert clamped is True

    def test_exactly_plus_one_session_accepted(self):
        # end 02-08 covers the +1 session (Fri 02-07) fully — minimum accepted
        start, end, clamped = clamp_window(EVENT_DT, "2025-02-08T00:00:00Z")
        assert end == "2025-02-08"
        assert clamped is True

    def test_only_event_day_available_defers(self):
        # end 02-07 04:00 floors to 02-07: +1 session NOT covered -> defer
        assert clamp_window(EVENT_DT, "2025-02-07T04:00:00Z") is None

    def test_minus_two_unavailable_defers(self):
        # dataset starts after the -2 session -> the fit window is unusable
        assert clamp_window(EVENT_DT, "2026-08-05T00:00:00Z",
                            available_start="2025-02-05") is None

    def test_unparseable_available_end_fails_open(self):
        # metadata hiccup (None / garbage) -> full window, unclamped
        for bad in (None, MagicMock(), "not-a-date"):
            start, end, clamped = clamp_window(EVENT_DT, bad)
            assert (start, end) == trading_window(EVENT_DT)
            assert clamped is False


def _resp(status, body="", case=None):
    r = MagicMock()
    r.status_code = status
    r.text = body
    if case is not None:
        r.text = json.dumps({"detail": {"case": case}})
    return r


class TestDatabento422Cases:
    def _client(self):
        return DatabentoClient(api_key="test-key")

    def test_record_count_symbology_invalid_is_none(self):
        db = self._client()
        with patch.object(db, "_get", return_value=_resp(
                422, case="symbology_invalid_request")):
            assert db.record_count("AFF", "2025-02-04", "2025-02-14") is None

    def test_record_count_end_after_available_raises(self):
        db = self._client()
        with patch.object(db, "_get", return_value=_resp(
                422, case="data_end_after_available_end")):
            with pytest.raises(TransientFetchError):
                db.record_count("PYPL", "2025-02-04", "2025-02-14")

    def test_record_count_unknown_422_raises(self):
        # any 422 we don't recognize must be LOUD, never silent-empty
        db = self._client()
        with patch.object(db, "_get", return_value=_resp(422, body="{}")):
            with pytest.raises(TransientFetchError):
                db.record_count("PYPL", "2025-02-04", "2025-02-14")

    def test_get_range_symbology_invalid_is_empty(self):
        db = self._client()
        with patch.object(db, "_get", return_value=_resp(
                422, case="symbology_invalid_request")):
            df = db.get_range("AFF", "2025-02-04", "2025-02-14", verify=False)
            assert df.empty

    def test_get_range_end_after_available_raises(self):
        db = self._client()
        with patch.object(db, "_get", return_value=_resp(
                422, case="data_end_after_available_end")):
            with pytest.raises(TransientFetchError):
                db.get_range("PYPL", "2025-02-04", "2025-02-14", verify=False)


# ---- run_event partial/defer/top-up (offline, everything mocked) ----------

ENTRY = {
    "index": 0, "date": "2025-02-06", "event_id": 42, "symbol": "AMZN",
    "entity_proper_name": "Amazon.com, Inc.", "ticker_region": "AMZN-US",
    "event_datetime_utc": EVENT_DT, "event_minute": "2025-02-06 21:01:00+00:00",
}
PANEL_KEY = "pfx/panels/date=2025-02-06/event_42.parquet"
PARTIAL_KEY = "pfx/panels/date=2025-02-06/event_42.partial.json"
UNREC_KEY = "pfx/panels/date=2025-02-06/event_42.unrecoverable.json"


def _s3_with(objects: dict):
    """MagicMock s3 serving `objects` (key -> bytes), NoSuchKey otherwise."""
    import io
    s3 = MagicMock()

    class NoSuchKey(Exception):
        pass

    s3.exceptions.NoSuchKey = NoSuchKey

    def get_object(Bucket, Key):
        if Key in objects:
            body = MagicMock()
            body.read.return_value = objects[Key]
            return {"Body": body}
        raise NoSuchKey(Key)

    s3.get_object.side_effect = get_object
    return s3


def _pq(df):
    import io
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def _db(avail_end):
    db = MagicMock()
    db.available_end.return_value = avail_end
    return db


def _put_keys(s3):
    return [c.kwargs["Key"] for c in s3.put_object.call_args_list]


class TestRunEventPartial:
    def test_clamped_fetch_writes_panel_and_partial_marker(self):
        s3 = _s3_with({})
        fetched = pd.DataFrame({"symbol": ["AMZN"], "ts": ["x"],
                                "publisher_id": [1], "open": [1.0]})
        with patch.object(basket_fetch, "_basket_peers",
                          return_value={"pure_play": ["FOO"]}), \
             patch("panel_builder.build_panel_symbols",
                   return_value=fetched) as bps:
            res = basket_fetch.run_event(ENTRY, "buck", "pfx",
                                         _db("2025-02-10T18:00:00Z"),
                                         MagicMock(), MagicMock(), s3)
        assert res["partial"] is True
        assert res["fetched_end"] == "2025-02-10"
        assert bps.call_args.kwargs["window"] == ("2025-02-04", "2025-02-10")
        keys = _put_keys(s3)
        assert PANEL_KEY in keys and PARTIAL_KEY in keys
        assert UNREC_KEY not in keys        # partial fetch NEVER tombstones

    def test_insufficient_availability_defers_without_writing(self):
        s3 = _s3_with({})
        with patch.object(basket_fetch, "_basket_peers",
                          return_value={"pure_play": ["FOO"]}), \
             patch("panel_builder.build_panel_symbols") as bps:
            res = basket_fetch.run_event(ENTRY, "buck", "pfx",
                                         _db("2025-02-07T04:00:00Z"),
                                         MagicMock(), MagicMock(), s3)
        assert res["deferred"] is True
        bps.assert_not_called()
        s3.put_object.assert_not_called()

    def test_topup_fetches_only_missing_tail_and_clears_marker(self):
        existing = pd.DataFrame({"symbol": ["AMZN", "FOO"], "ts": ["x", "x"],
                                 "publisher_id": [1, 1], "open": [1.0, 2.0]})
        partial = json.dumps({"fetched_end": "2025-02-10",
                              "window_end": "2025-02-14"}).encode()
        s3 = _s3_with({PANEL_KEY: _pq(existing), PARTIAL_KEY: partial})
        tail = pd.DataFrame({"symbol": ["AMZN", "FOO"], "ts": ["y", "y"],
                             "publisher_id": [1, 1], "open": [3.0, 4.0]})
        with patch.object(basket_fetch, "_basket_peers",
                          return_value={"pure_play": ["FOO"]}), \
             patch("panel_builder.build_panel_symbols",
                   return_value=tail) as bps:
            res = basket_fetch.run_event(ENTRY, "buck", "pfx",
                                         _db("2026-08-05T04:00:00Z"),
                                         MagicMock(), MagicMock(), s3)
        # tail range only, no duplicate event markers
        assert bps.call_args.kwargs["window"] == ("2025-02-10", "2025-02-14")
        assert bps.call_args.kwargs["event_marker"] is False
        s3.delete_object.assert_called_once_with(Bucket="buck", Key=PARTIAL_KEY)
        assert res["partial"] is False
        # head-real symbols must NOT be tombstoned by a tail-only fetch
        assert UNREC_KEY not in _put_keys(s3)

    def test_topup_with_no_new_availability_waits(self):
        existing = pd.DataFrame({"symbol": ["AMZN", "FOO"], "ts": ["x", "x"],
                                 "publisher_id": [1, 1], "open": [1.0, 2.0]})
        partial = json.dumps({"fetched_end": "2025-02-10",
                              "window_end": "2025-02-14"}).encode()
        s3 = _s3_with({PANEL_KEY: _pq(existing), PARTIAL_KEY: partial})
        with patch.object(basket_fetch, "_basket_peers",
                          return_value={"pure_play": ["FOO"]}), \
             patch("panel_builder.build_panel_symbols") as bps:
            res = basket_fetch.run_event(ENTRY, "buck", "pfx",
                                         _db("2025-02-10T18:00:00Z"),
                                         MagicMock(), MagicMock(), s3)
        assert res["partial_pending"] is True
        bps.assert_not_called()
        s3.put_object.assert_not_called()

    def test_full_window_unchanged_no_marker(self):
        s3 = _s3_with({})
        fetched = pd.DataFrame({"symbol": ["AMZN", "FOO"], "ts": ["x", "x"],
                                "publisher_id": [1, 1], "open": [1.0, 2.0]})
        with patch.object(basket_fetch, "_basket_peers",
                          return_value={"pure_play": ["FOO"]}), \
             patch("panel_builder.build_panel_symbols",
                   return_value=fetched) as bps:
            res = basket_fetch.run_event(ENTRY, "buck", "pfx",
                                         _db("2026-08-05T04:00:00Z"),
                                         MagicMock(), MagicMock(), s3)
        assert res.get("partial") is False
        assert bps.call_args.kwargs["window"] == trading_window(EVENT_DT)
        assert PARTIAL_KEY not in _put_keys(s3)

    def test_clamped_fetch_never_tombstones_even_when_window_closed(self):
        # window fully in the past (2025 event) AND availability clamped:
        # FOO returns empty -> must stay retryable, not unrecoverable
        s3 = _s3_with({})
        fetched = pd.DataFrame({"symbol": ["AMZN"], "ts": ["x"],
                                "publisher_id": [1], "open": [1.0]})
        with patch.object(basket_fetch, "_basket_peers",
                          return_value={"pure_play": ["FOO"]}), \
             patch("panel_builder.build_panel_symbols", return_value=fetched):
            basket_fetch.run_event(ENTRY, "buck", "pfx",
                                   _db("2025-02-10T18:00:00Z"),
                                   MagicMock(), MagicMock(), s3)
        assert UNREC_KEY not in _put_keys(s3)


class TestHttp206:
    def test_get_range_accepts_206_with_full_body(self):
        # Databento serves some responses as 206 Partial Content with the
        # COMPLETE body (VSXY 2026-05-29..2026-06-10: 206 + all 3022 rows,
        # matching record_count). 206 must be accepted like 200; the
        # existing row-count check still guards truncation.
        db = DatabentoClient(api_key="test-key")
        body = ("ts_event,rtype,publisher_id,instrument_id,"
                "open,high,low,close,volume\n"
                "1780387200000000000,33,93,1,1000000000,1000000000,"
                "1000000000,1000000000,10\n")
        with patch.object(db, "_get", return_value=_resp(206, body=body)):
            df = db.get_range("VSXY", "2026-05-29", "2026-06-10",
                              verify=False)
        assert len(df) == 1
        assert df["close"].iloc[0] == 1.0     # 1e9 fixed-point scaled

    def test_get_range_206_short_body_still_retries(self):
        # 206 with FEWER rows than record_count is a genuine truncation:
        # the count check must reject it, same as under 200
        db = DatabentoClient(api_key="test-key")
        body = ("ts_event,rtype,publisher_id,instrument_id,"
                "open,high,low,close,volume\n"
                "1780387200000000000,33,93,1,1000000000,1000000000,"
                "1000000000,1000000000,10\n")
        with patch.object(db, "record_count", return_value=5), \
             patch.object(db, "_get", return_value=_resp(206, body=body)):
            with pytest.raises(TransientFetchError):
                db.get_range("VSXY", "2026-05-29", "2026-06-10")
