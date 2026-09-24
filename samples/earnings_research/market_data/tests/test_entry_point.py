"""Unit tests for the date-driven Redshift entry point (offline)."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sharding import bare_symbol, day_bounds_utc, event_minute_utc, trading_days
from event_source import events_for_date
from models import EventRow


class TestBareSymbol:
    @pytest.mark.parametrize("tr,expected", [
        ("AMZN-AR", "AMZN"),
        ("AMZN-US", "AMZN"),
        ("CFG.PRI-US", "CFG.PRI"),   # dotted preferred kept as-is pre-region
        ("BRK.B-US", "BRK.B"),
        (None, None),
        ("", None),
    ])
    def test_cases(self, tr, expected):
        assert bare_symbol(tr) == expected


class TestTradingDays:
    def test_feb_2025_session_count(self):
        # Feb 2025: 20 weekdays minus Presidents' Day (2025-02-17) = 19 sessions
        dates = trading_days("2025-02-01", "2025-02-28")
        assert len(dates) == 19
        assert "2025-02-17" not in dates          # Presidents' Day holiday
        assert "2025-02-01" not in dates          # Saturday
        assert dates[0] == "2025-02-03"           # first Monday
        assert dates[-1] == "2025-02-28"

    def test_all_weekdays_no_weekends(self):
        import datetime
        for d in trading_days("2025-02-01", "2025-02-28"):
            assert datetime.date.fromisoformat(d).weekday() < 5


class TestDayBounds:
    def test_half_open_utc(self):
        assert day_bounds_utc("2025-02-06") == ("2025-02-06 00:00:00",
                                                "2025-02-07 00:00:00")


class TestEventMinute:
    def test_floor_to_minute(self):
        assert event_minute_utc("2025-02-06 21:01:00") == "2025-02-06 21:01:00+00:00"

    def test_floors_seconds(self):
        assert event_minute_utc("2025-02-06 21:01:37") == "2025-02-06 21:01:00+00:00"


def _mock_client(rows):
    c = MagicMock()
    c.fetch_all.return_value = iter(rows)
    return c


class TestEventsForDate:
    def test_maps_rows_to_eventrows(self):
        rows = [
            {"event_id": 1203024235, "event_datetime_utc": "2025-02-06 21:01:00",
             "entity_proper_name": "Amazon.com, Inc.", "ticker_region": "AMZN-US"},
            {"event_id": 999, "event_datetime_utc": "2025-02-06 13:30:00",
             "entity_proper_name": "Example Corp", "ticker_region": "EX-US"},
        ]
        out = events_for_date(_mock_client(rows), "2025-02-06")
        assert [e.event_id for e in out] == [1203024235, 999]
        amzn = out[0]
        assert isinstance(amzn, EventRow)
        assert amzn.symbol == "AMZN"                       # region stripped
        assert amzn.event_minute == "2025-02-06 21:01:00+00:00"
        assert amzn.entity_proper_name == "Amazon.com, Inc."

    def test_query_uses_day_bounds(self):
        client = _mock_client([])
        events_for_date(client, "2025-02-06")
        sql = client.fetch_all.call_args[0][0]
        assert "'2025-02-06 00:00:00'" in sql
        assert "'2025-02-07 00:00:00'" in sql
        assert "event_type = 'ER'" in sql

    def test_query_resolves_primary_listing(self):
        # US universe = primary listing on a US exchange (OTC excluded), via
        # the fsym_primary_equity_id -> fsym_regional_id ->
        # fsym_primary_listing_id chain — never an alphabetical pick over
        # ce_sec_entity's fanned-out lines (which yields AMZN-AR / AASNXXX-US).
        client = _mock_client([])
        events_for_date(client, "2025-02-06")
        sql = client.fetch_all.call_args[0][0]
        assert "fsym_primary_equity_id" in sql
        assert "fsym_primary_listing_id" in sql
        assert "fref_listing_exchange in ('NAS', 'NYS', 'ASE', 'PSE')" in sql
        assert "row_number" not in sql
        assert "like '%-US'" not in sql

    def test_query_excludes_projected(self):
        # only CONFIRMED releases — projected events carry placeholder datetimes
        # that get revised (BOTJ moved 07-17 -> 07-30), corrupting the window.
        client = _mock_client([])
        events_for_date(client, "2025-02-06")
        sql = client.fetch_all.call_args[0][0]
        assert "projected = false" in sql

    def test_query_requires_us_listed_common_share(self):
        # Universe = US-listed single-class common stock: reg==prim=='SHARE',
        # NO domicile filter. Drops ADRs (reg 'ADR' != prim 'SHARE'), ETFs, etc.
        # US-listed foreign issuers (AEHL/GDHG) are KEPT on the same basis.
        client = _mock_client([])
        events_for_date(client, "2025-02-06")
        sql = client.fetch_all.call_args[0][0]
        assert "reg.fref_security_type = 'SHARE'" in sql
        assert "prim.fref_security_type = 'SHARE'" in sql
        assert "iso_country" not in sql

    def test_empty_day(self):
        assert events_for_date(_mock_client([]), "2025-02-15") == []


class TestArrayIndexMapping:
    def test_index_maps_to_date(self):
        import main
        args = MagicMock(date=None, start="2025-02-01", end="2025-02-28")
        import os
        os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = "0"
        try:
            assert main._resolve_date(args) == "2025-02-03"   # first session
            os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = "18"
            assert main._resolve_date(args) == "2025-02-28"   # 19th session
        finally:
            del os.environ["AWS_BATCH_JOB_ARRAY_INDEX"]

    def test_index_out_of_range(self):
        import main
        args = MagicMock(date=None, start="2025-02-01", end="2025-02-28")
        import os
        os.environ["AWS_BATCH_JOB_ARRAY_INDEX"] = "99"
        try:
            with pytest.raises(SystemExit):
                main._resolve_date(args)
        finally:
            del os.environ["AWS_BATCH_JOB_ARRAY_INDEX"]
