"""Event-sharding manifest: the step-zero pre-pass artifact.

Sharding unit is a single EVENT (not a date). A short "plan" job enumerates
every fetch-ready ER event across [start, end] into a deterministic, ordered
list and writes it to S3; then one Batch array job of that size runs, each
child selecting manifest.events[AWS_BATCH_JOB_ARRAY_INDEX] and fetching that
one event's basket panel. A Databento failure isolates to its own shard.

Ordering is (trading_day asc, event_id asc) — stable across rebuilds, so the
index->event mapping is reproducible from the same inputs.

Layout:
  s3://<bucket>/<prefix>/manifests/manifest_<start>_<end>.json
"""

import io
import json
import logging
from typing import List, Optional

import boto3

from event_source import events_for_date
from redshift_client import RedshiftClient
from sharding import trading_days

logger = logging.getLogger(__name__)


def manifest_key(prefix: str, start: str, end: str) -> str:
    return f"{prefix}/manifests/manifest_{start}_{end}.json"


def build_manifest(
    rs: RedshiftClient,
    start: str,
    end: str,
    ticker_filter: Optional[List[str]] = None
) -> dict:
    """Enumerate every ER event across [start, end], ordered and indexed.

    One free Redshift query per trading day. Result is the shard list: each
    entry is one array-job shard (one event -> one basket -> one panel).

    Args:
        rs: Redshift client
        start: Start date YYYY-MM-DD
        end: End date YYYY-MM-DD
        ticker_filter: Optional list of tickers to filter to (e.g., ['APA', 'COP']).
                      If None (default), returns all events in date range.
    """
    days = trading_days(start, end)
    events: List[dict] = []

    # Convert ticker_filter to set for O(1) lookups
    ticker_set = set(ticker_filter) if ticker_filter else None

    for day in days:
        rows = events_for_date(rs, day)
        for r in rows:
            if not r.symbol:
                continue  # unmapped ticker -> nothing to fetch, don't burn a shard

            # NEW: Apply ticker filter if provided
            if ticker_set is not None and r.symbol not in ticker_set:
                continue

            events.append({
                "index": len(events),
                "date": day,
                "event_id": r.event_id,
                "symbol": r.symbol,
                "entity_proper_name": r.entity_proper_name,
                "ticker_region": r.ticker_region,
                "event_datetime_utc": r.event_datetime_utc,
                "event_minute": r.event_minute,
            })

    # Update log message to indicate filtering if applied
    if ticker_set:
        logger.info("manifest %s..%s: %d events across %d trading days (filtered to %d tickers)",
                    start, end, len(events), len(days), len(ticker_set))
    else:
        logger.info("manifest %s..%s: %d events across %d trading days",
                    start, end, len(events), len(days))

    return {"start": start, "end": end, "trading_days": len(days),
            "event_count": len(events), "events": events}


def write_manifest(s3, bucket: str, prefix: str, manifest: dict) -> str:
    key = manifest_key(prefix, manifest["start"], manifest["end"])
    body = json.dumps(manifest, indent=2).encode()
    s3.put_object(Bucket=bucket, Key=key, Body=body,
                  ContentType="application/json")
    logger.info("wrote manifest s3://%s/%s (%d events)", bucket, key,
                manifest["event_count"])
    return key


def read_manifest(s3, bucket: str, key: str) -> dict:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())
