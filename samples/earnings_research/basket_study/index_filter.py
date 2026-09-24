"""Index constituent filtering for basket study aggregation.

Filters events by whether the REPORTER (not peers) was in a given index on
the trade day. Reads cached constituent CSVs directly from S3 (written by
scripts/fetch_{nasdaq,sp500}_constituents.py) — no local files, no external
API calls, so it works identically on a laptop and inside the Batch container.

Date resolution: constituent snapshots are cached for specific dates only.
For a trade day with no exact snapshot we use the nearest cached date at or
before it (index composition changes slowly), falling back to the earliest
cached date if the trade day predates the cache.
"""
import io
from typing import Optional

import boto3
import pandas as pd

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")


def _prefixes() -> dict:
    """Index -> S3 prefix map from the single study config, with legacy
    extras kept for ad-hoc use."""
    import study_config
    m = dict(study_config.load()["aggregation"]["index_prefixes"])
    m.setdefault("RUA_ACTUAL", "russell-3000-actual-constituents/")  # IWV ETF, sampled
    return m


INDEX_PREFIXES = _prefixes()


class IndexFilter:
    """Index membership checker backed by S3-cached constituent snapshots."""

    def __init__(self, indices: Optional[list] = None, profile: Optional[str] = None):
        """
        Args:
            indices: List of index codes (e.g., ["NDX", "SPX"]). If None/empty, no filtering.
            profile: AWS profile; None uses the default credential chain (Batch job role).
        """
        self.indices = indices or []
        for idx in self.indices:
            if idx not in INDEX_PREFIXES:
                raise ValueError(f"Unknown index: {idx}. Supported: {list(INDEX_PREFIXES)}")
        sess = boto3.Session(profile_name=profile) if profile else boto3.Session()
        self._s3 = sess.client("s3")
        self._snapshot_dates = {}   # index -> sorted list of cached date strings
        self._members = {}          # (index, snapshot_date) -> set of tickers

    # accepted snapshot filenames under {prefix}date={YYYY-MM-DD}/
    _FILENAMES = ("weights.csv", "holdings.csv")

    def _available_dates(self, index: str) -> list:
        """List snapshot dates cached in S3 for an index. Accepts both the
        index-list layout (date=.../weights.csv) and the SPDR ETF layout
        (fund=X/date=.../holdings.csv)."""
        if index not in self._snapshot_dates:
            found = {}   # date -> filename
            paginator = self._s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=_bucket(), Prefix=INDEX_PREFIXES[index]):
                for obj in page.get("Contents", []):
                    parts = obj["Key"].split("date=")
                    if len(parts) == 2:
                        date, _, fname = parts[1].partition("/")
                        if fname in self._FILENAMES:
                            found[date] = fname
            if not found:
                raise RuntimeError(
                    f"No cached constituents for {index} in s3://{_bucket()}/{INDEX_PREFIXES[index]} "
                    f"— run the matching fetch script first")
            self._snapshot_files = getattr(self, "_snapshot_files", {})
            self._snapshot_files[index] = found
            self._snapshot_dates[index] = sorted(found)
        return self._snapshot_dates[index]

    def _snapshot_for(self, index: str, date: str) -> str:
        """Nearest cached snapshot at or before `date`; else earliest cached."""
        dates = self._available_dates(index)
        at_or_before = [d for d in dates if d <= date]
        return at_or_before[-1] if at_or_before else dates[0]

    def _load_members(self, index: str, snapshot_date: str) -> set:
        key = (index, snapshot_date)
        if key not in self._members:
            fname = getattr(self, "_snapshot_files", {}).get(index, {}).get(
                snapshot_date, "weights.csv")
            s3_key = f"{INDEX_PREFIXES[index]}date={snapshot_date}/{fname}"
            body = self._s3.get_object(Bucket=_bucket(), Key=s3_key)["Body"].read()
            df = pd.read_csv(io.BytesIO(body))
            # normalize share-class separators (BRK.B / BRK-B) to dot form
            tick = df["ticker"].astype(str).str.upper().str.replace("-", ".", regex=False)
            self._members[key] = set(tick)
        return self._members[key]

    def is_member(self, ticker: str, date: str) -> bool:
        """True if ticker was in ANY configured index on date (nearest snapshot);
        True unconditionally when no indices are configured."""
        if not self.indices:
            return True
        t = str(ticker).upper().replace("-", ".")
        return any(t in self._load_members(idx, self._snapshot_for(idx, date))
                   for idx in self.indices)

    def filter_events(self, events: pd.DataFrame,
                      ticker_col: str = "event_symbol",
                      date_col: str = "trade_day") -> pd.DataFrame:
        """Filter events to rows whose ticker was an index member on the row's date."""
        if not self.indices or events.empty:
            return events
        mask = events.apply(
            lambda row: self.is_member(row[ticker_col], row[date_col]), axis=1)
        return events[mask].copy()

    def __repr__(self):
        if not self.indices:
            return "IndexFilter(no filter)"
        return f"IndexFilter({', '.join(self.indices)})"