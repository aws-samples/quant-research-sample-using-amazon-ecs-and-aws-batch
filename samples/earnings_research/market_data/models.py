"""Data contracts for the market-data entry point."""

from dataclasses import asdict, dataclass
from typing import Optional


@dataclass
class EventRow:
    """One fetch-ready ER event for a single date.

    symbol is the bare (region-stripped) ticker; event_minute is the
    ohlcv-1m-aligned release minute both downstream fetch and event_bar
    stamping key on.
    """

    event_id: int
    event_datetime_utc: str
    symbol: Optional[str]
    entity_proper_name: Optional[str]
    ticker_region: Optional[str]
    event_minute: str

    def to_dict(self) -> dict:
        return asdict(self)
