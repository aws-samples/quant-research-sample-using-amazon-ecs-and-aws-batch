"""Deterministic sharding and manifest-row derivation helpers.

Pure functions — no AWS, no IO. Shard assignment must be stable across runs
and processes, so it uses sha1 rather than Python's builtin hash() (which is
salted per-process).
"""

import hashlib
from datetime import datetime
from typing import Optional


def shard_for_event(event_id: int, num_shards: int) -> int:
    """Stable shard assignment for an event id."""
    if num_shards < 1:
        raise ValueError(f"num_shards must be >= 1, got {num_shards}")
    digest = hashlib.sha1(str(event_id).encode("ascii")).hexdigest()
    return int(digest, 16) % num_shards


def shard_for_symbol(factset_entity_id: Optional[str], num_shards: int) -> int:
    """Stable shard assignment keyed by company, so every event for one entity
    lands in the same shard — a single worker then processes that company's
    URLs sequentially, never hitting one host from two workers at once.

    Events with no entity id fall back to a fixed bucket (0); they are rare
    and carry no company to serialize on.
    """
    if num_shards < 1:
        raise ValueError(f"num_shards must be >= 1, got {num_shards}")
    if not factset_entity_id:
        return 0
    digest = hashlib.sha1(factset_entity_id.encode("utf-8")).hexdigest()
    return int(digest, 16) % num_shards


def region_from_ticker(ticker_region: Optional[str]) -> str:
    """Region partition value from a FactSet ticker_region like 'AAPL-US'.

    The region is the suffix after the last '-'. Anything unmapped or
    malformed becomes 'UNKNOWN' so the partition key is always present.
    """
    if not ticker_region or "-" not in ticker_region:
        return "UNKNOWN"
    region = ticker_region.rsplit("-", 1)[1].strip().upper()
    # Regions are 2-letter codes; guard against junk suffixes
    if len(region) != 2 or not region.isalpha():
        return "UNKNOWN"
    return region


def event_date_from_datetime(event_datetime_utc: str) -> str:
    """YYYY-MM-DD date partition value from a Redshift timestamp string.

    Redshift returns 'YYYY-MM-DD HH:MM:SS' (sometimes with fractional
    seconds or a 'T' separator). The date is the first 10 chars once the
    format is validated.
    """
    s = (event_datetime_utc or "").strip()
    candidate = s[:10]
    datetime.strptime(candidate, "%Y-%m-%d")  # raises ValueError on junk
    return candidate
