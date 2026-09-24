"""Pure helpers for the date-driven market-data entry point.

No AWS, no IO. The unit of parallelism is a single trading date: a range fans
out to trading_days(), and each date is processed independently.
"""

from datetime import date, datetime
from typing import List, Optional

import pandas as pd
import pandas_market_calendars as mcal

_XNYS = mcal.get_calendar("XNYS")


def bare_symbol(ticker_region: Optional[str]) -> Optional[str]:
    """Strip the FactSet region suffix to the plain Databento-resolvable symbol.

    'AMZN-AR' -> 'AMZN'. FactSet maps one event to many regional listings, but
    they all share the same underlying ticker, so the region is dropped. Any
    dotted FactSet notation (preferred/synthetic, e.g. 'CFG.PRI-US') is left as
    the pre-region string for the downstream validator to accept or drop.
    """
    if not ticker_region:
        return None
    sym = ticker_region.rsplit("-", 1)[0].strip()
    return sym or None


def trading_days(start: str, end: str) -> List[str]:
    """XNYS session dates in [start, end] inclusive, as 'YYYY-MM-DD' strings.

    This is the fan-out list: one entry per date the entry point will run on.
    Weekends and exchange holidays are excluded by the calendar.
    """
    sched = _XNYS.schedule(start_date=start, end_date=end)
    return [d.strftime("%Y-%m-%d") for d in sched.index.normalize()]


def day_bounds_utc(day: str) -> tuple[str, str]:
    """[start, end) UTC timestamp strings spanning one calendar day.

    event_datetime_utc is stored UTC; an ER released 21:01 UTC belongs to that
    UTC calendar day. Half-open so the query is `>= start and < end`.
    """
    d = datetime.strptime(day, "%Y-%m-%d").date()
    nxt = (pd.Timestamp(d) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    return f"{day} 00:00:00", f"{nxt} 00:00:00"


def event_minute_utc(event_datetime_utc: str) -> str:
    """Floor an event timestamp to its ohlcv-1m bar minute (UTC ISO)."""
    return pd.Timestamp(event_datetime_utc, tz="UTC").floor("min").strftime(
        "%Y-%m-%d %H:%M:%S+00:00")


_DAYS_BEFORE = 2
_DAYS_AFTER = 5


def trading_window(event_datetime_utc: str) -> tuple[str, str]:
    """[-2, +5] trading-day window around an event, as (start, end_exclusive)
    date strings for Databento get_range. Anchors on the first XNYS session on
    or after the event date. Matches the validated prototype/golden window."""
    start, end = _window_sessions(event_datetime_utc, _DAYS_BEFORE, _DAYS_AFTER)
    # get_range end is exclusive -> +1 day so the +5th session is included
    return start.strftime("%Y-%m-%d"), (end + pd.Timedelta(days=1)).strftime("%Y-%m-%d")


def _window_sessions(event_datetime_utc: str, before: int, after: int):
    """(start_session, end_session) Timestamps for a [-before, +after] window."""
    event_date = pd.Timestamp(event_datetime_utc[:10])
    sched = _XNYS.schedule(
        start_date=event_date - pd.Timedelta(days=20),
        end_date=event_date + pd.Timedelta(days=20),
    )
    sessions = sched.index.normalize()
    future = sessions[sessions >= event_date]
    anchor = future[0] if len(future) else sessions[sessions <= event_date][-1]
    pos = sessions.get_loc(anchor)
    return (sessions[max(0, pos - before)],
            sessions[min(len(sessions) - 1, pos + after)])


def clamp_window(event_datetime_utc: str, available_end,
                 available_start: Optional[str] = None):
    """Clamp the [-2,+5] window to the dataset's available range.

    Partial-fetch policy: when availability ends before the +5 session, take
    whatever exists AS LONG AS the window still fully covers -2 through the
    +1 session; otherwise the event is not yet fetchable — defer it.

    Returns (start, end_exclusive, clamped) or None to defer. An unparseable
    available_end fails open to the full unclamped window (a metadata hiccup
    must not shrink a fetch)."""
    full_start, full_end = trading_window(event_datetime_utc)
    try:
        avail = pd.Timestamp(available_end).tz_localize(None).floor("D")
        if pd.isna(avail):                       # pd.Timestamp(None) -> NaT
            raise ValueError("NaT")
    except Exception:
        return full_start, full_end, False
    if available_start is not None and \
            pd.Timestamp(available_start) > pd.Timestamp(full_start):
        return None                              # -2 session unavailable
    if avail >= pd.Timestamp(full_end):
        return full_start, full_end, False       # full window available
    # minimum acceptable end: the +1 session fully covered (end exclusive)
    _, plus1 = _window_sessions(event_datetime_utc, _DAYS_BEFORE, 1)
    min_end = plus1 + pd.Timedelta(days=1)
    if avail < min_end:
        return None                              # not even +1 covered: defer
    return full_start, avail.strftime("%Y-%m-%d"), True
