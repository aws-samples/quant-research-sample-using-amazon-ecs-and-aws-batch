#!/usr/bin/env python
"""Query Russell index constituents from cached data.

Usage:
    # Check if AAPL is in Russell 1000 on 2026-08-10:
    scripts/.venv/bin/python scripts/query_russell_constituents.py AAPL 2026-08-10 --index RUI

    # Check if MOGA is in Russell 2000:
    scripts/.venv/bin/python scripts/query_russell_constituents.py MOGA 2026-08-10 --index RUT

    # Check Russell 3000 membership (derived):
    scripts/.venv/bin/python scripts/query_russell_constituents.py NVDA 2026-08-10 --index RUA

    # Check Russell 3000 membership (actual IWV):
    scripts/.venv/bin/python scripts/query_russell_constituents.py NVDA 2026-08-10 --index RUA --actual

    # List all constituents for a date:
    scripts/.venv/bin/python scripts/query_russell_constituents.py --list 2026-08-10 --index RUI

Supported indices:
    RUI = Russell 1000
    RUT = Russell 2000
    RUA = Russell 3000 (derived from R1000+R2000, ~2985 tickers)
    RUA --actual = Russell 3000 (IWV ETF, sampled, ~2584 tickers)
"""
import argparse
import sys
from datetime import datetime, date as dt_date
from pathlib import Path

import boto3
import pandas as pd

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")

INDEX_CONFIGS = {
    "RUI": {
        "name": "Russell 1000",
        "prefix": "russell-1000-constituents",
        "cache_file": "russell_1000_constituents_cache.json",
    },
    "RUT": {
        "name": "Russell 2000",
        "prefix": "russell-2000-constituents",
        "cache_file": "russell_2000_constituents_cache.json",
    },
    "RUA": {
        "name": "Russell 3000 (Derived)",
        "prefix": "russell-3000-constituents",
        "cache_file": "russell_3000_constituents_cache.json",
    },
    "RUA_ACTUAL": {
        "name": "Russell 3000 (IWV ETF)",
        "prefix": "russell-3000-actual-constituents",
        "cache_file": "russell_3000_actual_constituents_cache.json",
    },
}


def get_nearest_cached_date(target_date: str, available_dates: list) -> str:
    """Find nearest cached date at or before target; else earliest cached."""
    dates = sorted(available_dates)
    at_or_before = [d for d in dates if d <= target_date]
    return at_or_before[-1] if at_or_before else dates[0]


def load_constituents(index_code: str, date_str: str, profile: str, actual: bool) -> pd.DataFrame:
    """Load cached constituent data for the given index and date."""
    if actual and index_code == "RUA":
        config = INDEX_CONFIGS["RUA_ACTUAL"]
    else:
        config = INDEX_CONFIGS[index_code]

    # Load cache manifest
    cache_path = settings.root() / settings.get("paths", "constituent_cache_dir") / config["cache_file"]
    if not cache_path.exists():
        raise FileNotFoundError(f"Cache file not found: {cache_path}\nRun fetch script first.")

    import json
    cache = json.loads(cache_path.read_text())

    if not cache.get("dates"):
        raise ValueError(f"No cached dates for {config['name']}. Run fetch script first.")

    # Find nearest date
    nearest = get_nearest_cached_date(date_str, list(cache["dates"].keys()))
    if nearest != date_str:
        print(f"Using nearest cached date: {nearest} (requested {date_str})", file=sys.stderr)

    s3_uri = cache["dates"][nearest]["s3_uri"]

    # Load from S3
    df = pd.read_csv(s3_uri, storage_options={"profile": profile} if profile else None)

    return df


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("ticker", nargs="?",
                    help="Ticker symbol to query (omit for --list)")
    ap.add_argument("date",
                    help="Date (YYYY-MM-DD)")
    ap.add_argument("--index", choices=["RUI", "RUT", "RUA"], default="RUI",
                    help="Index to query (default: RUI)")
    ap.add_argument("--actual", action="store_true",
                    help="For RUA only: use actual IWV ETF instead of derived")
    ap.add_argument("--list", action="store_true",
                    help="List all constituents for date")
    ap.add_argument("--profile", default=None,
                    help="AWS profile (default: settings aws.profile)")
    args = ap.parse_args()
    args.profile = args.profile or settings.get("aws", "profile")

    # Validate date
    try:
        dt_date.fromisoformat(args.date)
    except ValueError:
        print(f"Error: Invalid date format: {args.date}. Use YYYY-MM-DD.", file=sys.stderr)
        return 1

    # Load data
    try:
        df = load_constituents(args.index, args.date, args.profile, args.actual)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    index_label = INDEX_CONFIGS.get(
        "RUA_ACTUAL" if args.actual and args.index == "RUA" else args.index
    )["name"]

    if args.list:
        # List all constituents
        print(f"\n{index_label} constituents on {args.date}:")
        print(f"Total: {len(df)} tickers\n")
        print(df[['ticker', 'company_name', 'weight']].to_string(index=False))
        return 0

    if not args.ticker:
        print("Error: ticker required (or use --list)", file=sys.stderr)
        return 1

    # Query specific ticker
    ticker_upper = args.ticker.upper()
    match = df[df['ticker'] == ticker_upper]

    if not match.empty:
        row = match.iloc[0]
        print(f"✅ {ticker_upper} is in {index_label} on {args.date}")
        print(f"   Company: {row['company_name']}")
        print(f"   Weight: {row['weight']:.4f}%")
        return 0
    else:
        print(f"❌ {ticker_upper} is NOT in {index_label} on {args.date}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
