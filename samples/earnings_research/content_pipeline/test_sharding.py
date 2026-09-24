import pytest
from hypothesis import given
from hypothesis import strategies as st

from sharding import (
    event_date_from_datetime,
    region_from_ticker,
    shard_for_event,
    shard_for_symbol,
)


class TestShardForEvent:
    def test_stable(self):
        assert shard_for_event(1203651714, 200) == shard_for_event(1203651714, 200)

    def test_known_value_pinned(self):
        # Pin the function's output: a change here silently orphans every
        # existing manifest/content object
        assert shard_for_event(1203651714, 200) == 193

    @given(st.integers(min_value=0, max_value=2**31), st.integers(min_value=1, max_value=1000))
    def test_in_range(self, event_id, num_shards):
        assert 0 <= shard_for_event(event_id, num_shards) < num_shards

    def test_roughly_uniform(self):
        counts = [0] * 20
        for event_id in range(1_200_000_000, 1_200_010_000):
            counts[shard_for_event(event_id, 20)] += 1
        expected = 10_000 / 20
        assert all(abs(c - expected) / expected < 0.2 for c in counts)

    def test_invalid_num_shards(self):
        with pytest.raises(ValueError):
            shard_for_event(1, 0)


class TestShardForSymbol:
    def test_stable(self):
        assert shard_for_symbol("000Y0Q-E", 20) == shard_for_symbol("000Y0Q-E", 20)

    def test_same_entity_same_shard(self):
        # every event for one company must co-locate so one worker serializes
        # that host's requests
        s = {shard_for_symbol("ABC123-E", 20) for _ in range(5)}
        assert len(s) == 1

    def test_null_entity_falls_back(self):
        assert shard_for_symbol(None, 20) == 0
        assert shard_for_symbol("", 20) == 0

    @given(st.text(min_size=1, max_size=12), st.integers(min_value=1, max_value=200))
    def test_in_range(self, entity, num_shards):
        assert 0 <= shard_for_symbol(entity, num_shards) < num_shards


class TestRegionFromTicker:
    @pytest.mark.parametrize("ticker,region", [
        ("AAPL-US", "US"),
        ("AAPL34-BR", "BR"),
        ("ATZ.TO-CA", "CA"),
        ("aapl-us", "US"),
        (None, "UNKNOWN"),
        ("", "UNKNOWN"),
        ("NODASH", "UNKNOWN"),
        ("X-TOOLONG", "UNKNOWN"),
        ("X-1A", "UNKNOWN"),
    ])
    def test_cases(self, ticker, region):
        assert region_from_ticker(ticker) == region


class TestEventDate:
    def test_redshift_format(self):
        assert event_date_from_datetime("2026-04-29 20:01:00") == "2026-04-29"

    def test_iso_t_separator(self):
        assert event_date_from_datetime("2026-04-29T20:01:00") == "2026-04-29"

    def test_junk_raises(self):
        with pytest.raises(ValueError):
            event_date_from_datetime("not a date")

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            event_date_from_datetime("")
