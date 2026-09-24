"""Ticker-rename resolution for partial data availability.

When a symbol's fetched window is missing its HEAD sessions (data starts
after the window start — the VSXY signature: renamed 2026-06-02, so the
[-2,+5] window's first two sessions traded under the old ticker VSCO), ask
Claude (Bedrock, same client as peer derivation) what the previous ticker
was, VERIFY the proposal against Databento's free record_count (old ticker
must have bars in the missing head), then fetch the old ticker's bars and
relabel them to the current symbol so the panel is continuous.

An unverified or unknown proposal changes nothing — the panel keeps its
partial head. Zero-data symbols (CIRC/CEPL class) do NOT trigger: partial
means some data present, per the acceptance rule.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from rename_resolver import RenameResolver, head_gap


def _bedrock_saying(payload: dict):
    br = MagicMock()
    br.converse.return_value = {
        "output": {"message": {"content": [
            {"text": "```json\n" + json.dumps(payload) + "\n```"}]}}}
    return br


def _db(counts: dict):
    """MagicMock db whose record_count returns counts[(symbol)] or 0."""
    db = MagicMock()
    db.record_count.side_effect = lambda s, a, b: counts.get(s, 0)
    return db


class TestHeadGap:
    def test_data_starting_late_yields_gap(self):
        # window 2026-05-29..2026-06-10, first bar on 06-02 -> head gap
        gap = head_gap(first_day="2026-06-02",
                       window=("2026-05-29", "2026-06-10"))
        assert gap == ("2026-05-29", "2026-06-02")

    def test_data_at_window_start_no_gap(self):
        assert head_gap("2026-05-29", ("2026-05-29", "2026-06-10")) is None

    def test_no_data_at_all_no_gap(self):
        # zero-data symbols are NOT the partial case — no trigger
        assert head_gap(None, ("2026-05-29", "2026-06-10")) is None


class TestResolve:
    def test_verified_rename_returned(self):
        br = _bedrock_saying({"previous_ticker": "VSCO",
                              "change_date": "2026-06-02",
                              "confidence": "medium", "reason": "spin-off"})
        db = _db({"VSCO": 916})
        r = RenameResolver(br, db)
        assert r.resolve("VSXY", "Victoria's Secret & Co.",
                         "2026-05-29", "2026-06-02") == "VSCO"

    def test_unverified_proposal_rejected(self):
        # Claude proposes a ticker with NO data in the missing head -> None
        br = _bedrock_saying({"previous_ticker": "WRONG",
                              "change_date": "2026-06-02"})
        db = _db({"WRONG": 0})
        r = RenameResolver(br, db)
        assert r.resolve("VSXY", "Victoria's Secret & Co.",
                         "2026-05-29", "2026-06-02") is None

    def test_unknown_rename_is_none(self):
        br = _bedrock_saying({"previous_ticker": None})
        r = RenameResolver(br, _db({}))
        assert r.resolve("CPSS", "Consumer Portfolio Services",
                         "2026-05-29", "2026-06-02") is None

    def test_llm_error_fails_open(self):
        br = MagicMock()
        br.converse.side_effect = Exception("throttled")
        r = RenameResolver(br, _db({}))
        assert r.resolve("VSXY", "x", "2026-05-29", "2026-06-02") is None

    def test_garbage_response_fails_open(self):
        br = MagicMock()
        br.converse.return_value = {"output": {"message": {"content": [
            {"text": "I am not sure about this one."}]}}}
        r = RenameResolver(br, _db({}))
        assert r.resolve("VSXY", "x", "2026-05-29", "2026-06-02") is None

    def test_verification_uses_missing_head_window(self):
        br = _bedrock_saying({"previous_ticker": "VSCO",
                              "change_date": "2026-06-02"})
        db = _db({"VSCO": 916})
        RenameResolver(br, db).resolve("VSXY", "n", "2026-05-29", "2026-06-02")
        db.record_count.assert_called_once_with("VSCO", "2026-05-29",
                                                "2026-06-02")


class TestFetchHeadAsCurrentSymbol:
    def test_old_ticker_rows_relabeled(self):
        br = _bedrock_saying({"previous_ticker": "VSCO",
                              "change_date": "2026-06-02"})
        db = _db({"VSCO": 916})
        old_rows = pd.DataFrame({
            "ts_event": [1, 2], "rtype": [33, 33], "publisher_id": [1, 1],
            "instrument_id": [7, 7], "open": [80.0, 81.0], "high": [81, 82],
            "low": [79, 80], "close": [80.5, 81.5], "volume": [100, 200],
            "ts": pd.to_datetime(["2026-05-29 13:30", "2026-06-01 13:30"],
                                 utc=True)})
        db.get_range.return_value = old_rows
        r = RenameResolver(br, db)
        out = r.fetch_head("VSXY", "Victoria's Secret & Co.",
                           "2026-05-29", "2026-06-02")
        assert out is not None
        db.get_range.assert_called_once_with("VSCO", "2026-05-29", "2026-06-02")
        assert list(out["symbol"].unique()) == ["VSXY"]   # relabeled
        assert len(out) == 2

    def test_no_rename_returns_none(self):
        br = _bedrock_saying({"previous_ticker": None})
        r = RenameResolver(br, _db({}))
        assert r.fetch_head("CPSS", "n", "2026-05-29", "2026-06-02") is None


# ---- integration into run_event (offline) ---------------------------------

import basket_fetch


class TestRunEventRenameIntegration:
    def test_head_gap_symbol_healed_via_rename(self):
        """A fetched symbol whose bars start after the window start gets its
        head backfilled from the verified previous ticker."""
        import io
        from unittest.mock import MagicMock, patch

        ENTRY = {"index": 0, "date": "2026-06-02", "event_id": 99,
                 "symbol": "VSXY", "entity_proper_name": "Victoria's Secret & Co.",
                 "ticker_region": "VSXY-US",
                 "event_datetime_utc": "2026-06-02 11:00:00",
                 "event_minute": "2026-06-02 11:00:00+00:00"}

        s3 = MagicMock()

        class NoSuchKey(Exception):
            pass

        s3.exceptions.NoSuchKey = NoSuchKey
        s3.get_object.side_effect = NoSuchKey("absent")

        # fetch returns VSXY bars starting 06-02 (head gap vs window start 05-29)
        fetched = pd.DataFrame({
            "symbol": ["VSXY"], "ts_event": [1], "rtype": [33],
            "publisher_id": [1], "instrument_id": [7], "open": [80.0],
            "high": [81.0], "low": [79.0], "close": [80.5], "volume": [100],
            "ts": pd.to_datetime(["2026-06-02 13:30"], utc=True)})
        head = pd.DataFrame({
            "symbol": ["VSXY"], "ts_event": [0], "rtype": [33],
            "publisher_id": [1], "instrument_id": [6], "open": [78.0],
            "high": [79.0], "low": [77.0], "close": [78.5], "volume": [50],
            "ts": pd.to_datetime(["2026-05-29 13:30"], utc=True)})

        db = MagicMock()
        db.available_end.return_value = "2026-08-05T04:00:00Z"
        resolver = MagicMock()
        resolver.fetch_head.return_value = head

        with patch.object(basket_fetch, "_basket_peers", return_value={}), \
             patch("panel_builder.build_panel_symbols", return_value=fetched), \
             patch.object(basket_fetch, "_rename_resolver", return_value=resolver):
            res = basket_fetch.run_event(ENTRY, "buck", "pfx",
                                         db, MagicMock(), MagicMock(), s3)

        resolver.fetch_head.assert_called_once()
        args = resolver.fetch_head.call_args.args
        assert args[0] == "VSXY"
        assert args[2] == "2026-05-29"          # gap start = window start
        assert args[3] == "2026-06-02"          # gap end = first data day
        assert res["rename_healed"] == ["VSXY"]
        # written panel contains both head and tail rows
        buf = s3.put_object.call_args_list[0].kwargs["Body"]
        out = pd.read_parquet(io.BytesIO(buf))
        assert len(out[out.symbol == "VSXY"]) == 2

    def test_no_gap_no_resolver_call(self):
        import io
        from unittest.mock import MagicMock, patch

        ENTRY = {"index": 0, "date": "2025-02-06", "event_id": 42,
                 "symbol": "AMZN", "entity_proper_name": "Amazon.com, Inc.",
                 "ticker_region": "AMZN-US",
                 "event_datetime_utc": "2025-02-06 21:01:00",
                 "event_minute": "2025-02-06 21:01:00+00:00"}
        s3 = MagicMock()

        class NoSuchKey(Exception):
            pass

        s3.exceptions.NoSuchKey = NoSuchKey
        s3.get_object.side_effect = NoSuchKey("absent")
        fetched = pd.DataFrame({
            "symbol": ["AMZN"], "ts_event": [1], "rtype": [33],
            "publisher_id": [1], "instrument_id": [7], "open": [1.0],
            "high": [1.0], "low": [1.0], "close": [1.0], "volume": [1],
            "ts": pd.to_datetime(["2025-02-04 14:30"], utc=True)})
        db = MagicMock()
        db.available_end.return_value = "2026-08-05T04:00:00Z"
        resolver = MagicMock()
        with patch.object(basket_fetch, "_basket_peers", return_value={}), \
             patch("panel_builder.build_panel_symbols", return_value=fetched), \
             patch.object(basket_fetch, "_rename_resolver", return_value=resolver):
            res = basket_fetch.run_event(ENTRY, "buck", "pfx",
                                         db, MagicMock(), MagicMock(), s3)
        resolver.fetch_head.assert_not_called()
        assert res.get("rename_healed", []) == []


class TestHealedRowsCarryPanelSchema:
    def test_head_rows_stamped_like_panel_rows(self):
        """Healed head bars must carry the panel-schema stamps (event_id,
        reporter_relationship, event_bar) copied from the symbol's fetched
        rows — raw get_range output crashes downstream boolean masks."""
        from unittest.mock import MagicMock, patch

        fetched = pd.DataFrame({
            "symbol": ["VSXY"], "ts_event": [1], "rtype": [33],
            "publisher_id": [1], "instrument_id": [7], "open": [80.0],
            "high": [81.0], "low": [79.0], "close": [80.5], "volume": [100],
            "ts": pd.to_datetime(["2026-06-02 13:30"], utc=True),
            "reporter_relationship": ["primary"], "event_id": [99],
            "event_bar": [False]})
        head_raw = pd.DataFrame({
            "symbol": ["VSXY"], "ts_event": [0], "rtype": [33],
            "publisher_id": [1], "instrument_id": [6], "open": [78.0],
            "high": [79.0], "low": [77.0], "close": [78.5], "volume": [50],
            "ts": pd.to_datetime(["2026-05-29 13:30"], utc=True)})
        resolver = MagicMock()
        resolver.fetch_head.return_value = head_raw
        db = MagicMock()
        with patch.object(basket_fetch, "_rename_resolver",
                          return_value=resolver):
            out, healed = basket_fetch._heal_head_gaps(
                fetched, {"VSXY": "n"}, db, MagicMock(), 99, "2026-05-29")
        assert healed == ["VSXY"]
        head_out = out[out["ts"] < pd.Timestamp("2026-06-01", tz="UTC")]
        assert (head_out["reporter_relationship"] == "primary").all()
        assert (head_out["event_id"] == 99).all()
        assert (head_out["event_bar"] == False).all()      # noqa: E712
        assert not out["event_bar"].isna().any()
