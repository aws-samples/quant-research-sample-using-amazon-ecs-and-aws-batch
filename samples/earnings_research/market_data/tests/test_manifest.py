"""Unit tests for the event-sharding manifest (offline).

Sharding unit = one EVENT. build_manifest flattens every ER event across the
range into a deterministic, indexed list; each entry is one array shard.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import manifest
from models import EventRow


def _ev(event_id, symbol, name="X Corp", tr=None):
    return EventRow(
        event_id=event_id,
        event_datetime_utc="2025-02-06 21:01:00",
        symbol=symbol,
        entity_proper_name=name,
        ticker_region=tr or (symbol + "-US" if symbol else None),
        event_minute="2025-02-06 21:01:00+00:00",
    )


class TestBuildManifest:
    def test_flattens_days_in_order_and_reindexes(self):
        # two trading days, events per day; index must be a dense 0..n-1
        by_day = {
            "2025-02-06": [_ev(1203024235, "AMZN"), _ev(999, "EX")],
            "2025-02-07": [_ev(1500, "FOO")],
        }
        with patch.object(manifest, "trading_days",
                          return_value=["2025-02-06", "2025-02-07"]), \
             patch.object(manifest, "events_for_date",
                          side_effect=lambda rs, day: by_day[day]):
            m = manifest.build_manifest(MagicMock(), "2025-02-06", "2025-02-07")

        assert m["event_count"] == 3
        assert m["trading_days"] == 2
        assert [e["index"] for e in m["events"]] == [0, 1, 2]
        assert [e["event_id"] for e in m["events"]] == [1203024235, 999, 1500]
        # date travels with each shard so the child writes to the right partition
        assert [e["date"] for e in m["events"]] == \
            ["2025-02-06", "2025-02-06", "2025-02-07"]
        first = m["events"][0]
        assert first["symbol"] == "AMZN"
        assert first["event_minute"] == "2025-02-06 21:01:00+00:00"

    def test_drops_unmapped_symbol_no_wasted_shard(self):
        by_day = {"2025-02-06": [_ev(1, "AMZN"), _ev(2, None, tr=None)]}
        with patch.object(manifest, "trading_days",
                          return_value=["2025-02-06"]), \
             patch.object(manifest, "events_for_date",
                          side_effect=lambda rs, day: by_day[day]):
            m = manifest.build_manifest(MagicMock(), "2025-02-06", "2025-02-06")
        assert m["event_count"] == 1
        assert m["events"][0]["event_id"] == 1
        assert m["events"][0]["index"] == 0   # dense: the dropped one leaves no hole

    def test_empty_range(self):
        with patch.object(manifest, "trading_days", return_value=["2025-02-06"]), \
             patch.object(manifest, "events_for_date", return_value=[]):
            m = manifest.build_manifest(MagicMock(), "2025-02-06", "2025-02-06")
        assert m["event_count"] == 0
        assert m["events"] == []


class TestManifestKey:
    def test_key_shape(self):
        assert manifest.manifest_key("earnings-market-data", "2025-02-01", "2025-02-28") \
            == "earnings-market-data/manifests/manifest_2025-02-01_2025-02-28.json"


class TestReadWriteRoundTrip:
    def test_write_then_read(self):
        store = {}
        s3 = MagicMock()
        s3.put_object.side_effect = \
            lambda Bucket, Key, Body, **kw: store.__setitem__((Bucket, Key), Body)
        m = {"start": "2025-02-06", "end": "2025-02-06",
             "trading_days": 1, "event_count": 1,
             "events": [{"index": 0, "event_id": 1, "date": "2025-02-06",
                         "symbol": "AMZN"}]}
        key = manifest.write_manifest(s3, "buck", "pfx", m)
        assert key == "pfx/manifests/manifest_2025-02-06_2025-02-06.json"

        body = store[("buck", key)]
        s3.get_object.return_value = {"Body": MagicMock(read=lambda: body)}
        back = manifest.read_manifest(s3, "buck", key)
        assert back == m
