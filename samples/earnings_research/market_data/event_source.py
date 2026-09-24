"""Redshift entry point: fetch-ready ER events for a single date.

Answers, for one date, "which US earnings-release events happened, and what
bare symbol does each map to?" — the shardable unit of work. No S3, no peers,
no Databento here; peer derivation and fetch append onto this later.
"""

import logging
from datetime import date
from typing import List

from redshift_client import RedshiftClient
from sharding import bare_symbol, day_bounds_utc, event_minute_utc
from models import EventRow
from universe import reporter_events_sql

logger = logging.getLogger(__name__)

# One row per ER event on the given UTC day, resolved to the reporter's PRIMARY
# common share and filtered to the shared US common-equity universe. The query
# text (resolution joins + universe predicate) lives in universe.py so reporters,
# peers, and reconcile all draw from ONE definition — fix the universe once, it
# fixes everywhere. See universe.py for why domicile + exchange + SHARE are all
# required (AEHL/GDHG foreign direct-listers, ADRs, ETFs).
_EVENTS_SQL = reporter_events_sql()


def events_for_date(client: RedshiftClient, day: str,
                    timeout_s: int = 600) -> List[EventRow]:
    """All fetch-ready US ER events whose event_datetime_utc is on `day`."""
    start, end = day_bounds_utc(day)
    sql = _EVENTS_SQL.format(start=start, end=end)
    rows: List[EventRow] = []
    for r in client.fetch_all(sql, timeout_s=timeout_s):
        dt = r["event_datetime_utc"]
        rows.append(EventRow(
            event_id=int(r["event_id"]),
            event_datetime_utc=dt,
            symbol=bare_symbol(r.get("ticker_region")),
            entity_proper_name=r.get("entity_proper_name"),
            ticker_region=r.get("ticker_region"),
            event_minute=event_minute_utc(dt),
        ))
    logger.info("date %s: %d ER events", day, len(rows))
    return rows
