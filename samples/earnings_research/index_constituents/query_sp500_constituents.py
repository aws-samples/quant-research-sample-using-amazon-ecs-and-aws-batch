#!/usr/bin/env python
"""Query S&P 500 constituents by date, fetching on-demand if not cached.

Usage:
    scripts/.venv/bin/python scripts/query_sp500_constituents.py 2026-08-06
    scripts/.venv/bin/python scripts/query_sp500_constituents.py 2026-08-06 --check CPOP

Returns the S&P 500 constituent list for the given date. If not cached, fetches
and caches it automatically.
"""
import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import settings

def _cache_file() -> Path:
    return settings.root() / settings.get("paths", "constituent_cache_dir") / "sp500_constituents_cache.json"


def get_constituents(date: str, profile: Optional[str] = None) -> pd.DataFrame:
    """Get S&P 500 constituents for date, fetching if not cached."""
    profile = profile or settings.get("aws", "profile")
    import json

    # Check cache
    if _cache_file().exists():
        cache = json.loads(_cache_file().read_text())
        if date in cache.get("dates", {}):
            uri = cache["dates"][date]["s3_uri"]
            print(f"Reading from cache: {uri}", file=sys.stderr)
            return pd.read_csv(uri, storage_options={"profile": profile} if profile else None)

    # Not cached - fetch it
    print(f"Not cached, fetching S&P 500 {date}...", file=sys.stderr)
    fetch_script = Path(__file__).parent / "fetch_sp500_constituents.py"
    result = subprocess.run(
        [sys.executable, str(fetch_script), date] + (["--profile", profile] if profile else []),
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"Fetch failed: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    print(result.stdout, file=sys.stderr)

    # Now it should be cached - read it
    cache = json.loads(_cache_file().read_text())
    uri = cache["dates"][date]["s3_uri"]
    return pd.read_csv(uri)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("date", help="Date (YYYY-MM-DD)")
    ap.add_argument("--check", help="Check if a specific ticker is in the list")
    ap.add_argument("--profile", default=None, help="AWS profile (default: settings aws.profile)")
    args = ap.parse_args()
    args.profile = args.profile or settings.get("aws", "profile")

    # Validate date format
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"Invalid date format: {args.date}. Use YYYY-MM-DD", file=sys.stderr)
        return 1

    df = get_constituents(args.date, args.profile)

    if args.check:
        ticker_upper = args.check.upper()
        if ticker_upper in df["ticker"].values:
            print(f"\n✓ {ticker_upper} IS in S&P 500 on {args.date}")
            row = df[df["ticker"] == ticker_upper].iloc[0]
            print(f"  Company: {row['company_name']}")
        else:
            print(f"\n✗ {ticker_upper} is NOT in S&P 500 on {args.date}")
        return 0

    # Print full list
    print(f"\nS&P 500 Constituents on {args.date} ({len(df)} total):\n")
    print(df.to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
