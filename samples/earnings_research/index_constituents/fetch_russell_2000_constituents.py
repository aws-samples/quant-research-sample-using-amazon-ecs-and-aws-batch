#!/usr/bin/env python
"""Fetch and cache Russell 2000 constituents by date.

Usage:
    scripts/.venv/bin/python scripts/fetch_russell_2000_constituents.py 2026-08-10
    scripts/.venv/bin/python scripts/fetch_russell_2000_constituents.py --list

Source: iShares Russell 2000 ETF (IWM) holdings via BlackRock.
Caches to S3: s3://{bucket}/russell-2000-constituents/date={date}/weights.csv
Updates local catalog: <constituent_cache_dir>/russell_2000_constituents_cache.json

Note: ETF holdings are updated daily but may lag official index rebalances by 1-2 days.
"""
import argparse
import json
import sys
from datetime import datetime, date as dt_date
from pathlib import Path
from io import StringIO

import boto3
import pandas as pd
import requests

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
S3_PREFIX = "russell-2000-constituents"
def _cache_file() -> Path:
    return settings.root() / settings.get("paths", "constituent_cache_dir") / "russell_2000_constituents_cache.json"

# iShares Russell 2000 ETF (IWM)
ISHARES_PRODUCT_ID = "239710"
ISHARES_TICKER = "iwm"
ISHARES_URL = f"https://www.ishares.com/us/products/{ISHARES_PRODUCT_ID}/{ISHARES_TICKER}/latest-holdings.csv"


def fetch_constituents(date: dt_date) -> pd.DataFrame:
    """Fetch Russell 2000 constituents from iShares IWM ETF.

    Note: Returns current holdings regardless of requested date. Historical
    point-in-time accuracy requires caching snapshots on the actual dates.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }

    resp = requests.get(ISHARES_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    lines = resp.text.split('\n')

    # Find header row
    header_idx = None
    for i, line in enumerate(lines):
        if 'Ticker' in line and ('Name' in line or 'Sector' in line):
            header_idx = i
            break

    if header_idx is None:
        raise ValueError("Could not find header row in iShares CSV")

    # Parse CSV from header onward
    df = pd.read_csv(StringIO('\n'.join(lines[header_idx:])))

    # Filter to equities only (exclude cash, other)
    df = df[df['Asset Class'] == 'Equity'].copy()

    # Clean and standardize
    df['Ticker'] = df['Ticker'].str.strip().str.upper()
    df = df.rename(columns={'Name': 'company_name', 'Ticker': 'ticker', 'Weight (%)': 'weight'})

    # Select columns matching our schema
    result = df[['ticker', 'company_name', 'weight']].copy()

    # Remove any duplicate tickers
    result = result.drop_duplicates(subset=['ticker'], keep='first')

    if len(result) < 1800:
        raise ValueError(f"Only found {len(result)} holdings, expected ~2000 for Russell 2000")

    return result


def load_cache() -> dict:
    """Load the local cache manifest."""
    if not _cache_file().exists():
        return {"name": "Russell 2000", "source": "iShares IWM ETF", "dates": {}}
    return json.loads(_cache_file().read_text())


def save_cache(cache: dict):
    """Save the local cache manifest."""
    _cache_file().parent.mkdir(parents=True, exist_ok=True)
    _cache_file().write_text(json.dumps(cache, indent=2) + "\n")


def get_cached_path(date: dt_date) -> tuple[str, str]:
    """Return (s3_key, s3_uri) for a given date."""
    date_str = date.strftime("%Y-%m-%d")
    key = f"{S3_PREFIX}/date={date_str}/weights.csv"
    uri = f"s3://{_bucket()}/{key}"
    return key, uri


def is_cached(date: dt_date, s3) -> bool:
    """Check if data exists in S3."""
    key, _ = get_cached_path(date)
    try:
        s3.head_object(Bucket=_bucket(), Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def cache_constituents(date: dt_date, df: pd.DataFrame, s3, cache: dict, profile: str):
    """Upload to S3 and update local cache."""
    key, uri = get_cached_path(date)
    date_str = date.strftime("%Y-%m-%d")

    # Upload CSV
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    s3.put_object(Bucket=_bucket(), Key=key, Body=csv_bytes, ContentType="text/csv")

    # Update local cache
    cache["dates"][date_str] = {
        "s3_uri": uri,
        "n_constituents": len(df),
        "cached_at": datetime.now().isoformat(),
        "profile": profile,
        "source": "iShares IWM ETF",
        "note": "Represents holdings at fetch time; historical accuracy depends on caching on actual dates"
    }
    save_cache(cache)

    print(f"Cached Russell 2000 {date_str}: {len(df)} constituents -> {uri}")


def list_cached(cache: dict):
    """Print all cached dates."""
    if not cache.get("dates"):
        print("No cached Russell 2000 constituents.")
        return

    print(f"\n{cache['name']} ({cache.get('source', 'unknown source')}):")
    for d in sorted(cache["dates"].keys()):
        info = cache["dates"][d]
        print(f"  {d}: {info['n_constituents']} constituents, {info['s3_uri']}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("date", nargs="?",
                    help="Date (YYYY-MM-DD, defaults to today)")
    ap.add_argument("--list", action="store_true",
                    help="List all cached dates")
    ap.add_argument("--profile", default=None,
                    help="AWS profile (default: settings aws.profile)")
    args = ap.parse_args()
    args.profile = args.profile or settings.get("aws", "profile")

    cache = load_cache()

    if args.list:
        list_cached(cache)
        return 0

    # Parse date
    if args.date:
        fetch_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        fetch_date = dt_date.today()

    session = boto3.Session(profile_name=args.profile)
    s3 = session.client("s3")

    # Check if already cached
    if is_cached(fetch_date, s3):
        _, uri = get_cached_path(fetch_date)
        print(f"Already cached: {uri}")
        # Update cache metadata even if S3 has it
        if fetch_date.strftime("%Y-%m-%d") not in cache.get("dates", {}):
            df = pd.read_csv(uri)
            cache_constituents(fetch_date, df, s3, cache, args.profile)
        return 0

    # Fetch and cache
    print(f"Fetching Russell 2000 constituents for {fetch_date}...")
    print(f"Source: iShares IWM ETF (product {ISHARES_PRODUCT_ID})")
    try:
        df = fetch_constituents(fetch_date)
        cache_constituents(fetch_date, df, s3, cache, args.profile)
        print(f"Success: {len(df)} constituents")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
