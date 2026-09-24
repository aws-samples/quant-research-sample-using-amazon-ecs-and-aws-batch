#!/usr/bin/env python
"""Fetch and cache S&P 500 constituents by date.

Usage:
    scripts/.venv/bin/python scripts/fetch_sp500_constituents.py 2026-08-06
    scripts/.venv/bin/python scripts/fetch_sp500_constituents.py --list

Caches to S3: s3://{bucket}/sp500-constituents/date={date}/weights.csv
Updates local catalog: <constituent_cache_dir>/sp500_constituents_cache.json

Source: Wikipedia S&P 500 component list (most reliable free source; official S&P
data requires SPDJI subscription). Historical constituents before ~2020 may be
incomplete.
"""
import argparse
import json
import sys
from datetime import datetime, date as dt_date
from pathlib import Path

import boto3
import pandas as pd
import requests
from bs4 import BeautifulSoup

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
S3_PREFIX = "sp500-constituents"
def _cache_file() -> Path:
    return settings.root() / settings.get("paths", "constituent_cache_dir") / "sp500_constituents_cache.json"

# Wikipedia archive URL for historical constituents
# Note: Only provides current list; historical versions require scraping archive.org
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"


def fetch_constituents(date: dt_date) -> pd.DataFrame:
    """Fetch S&P 500 constituents from Wikipedia.

    Note: Wikipedia only provides the current list. For historical dates, we cache
    the list with the requested date, but it represents the composition at fetch time,
    not necessarily the historical composition on that date.
    """
    if date < dt_date(2020, 1, 1):
        print(
            f"Warning: Historical S&P 500 data before 2020 may not be accurate. "
            f"Fetching current list and caching as {date}.",
            file=sys.stderr
        )

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    resp = requests.get(WIKI_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.content, "html.parser")

    # Find the constituents table (id="constituents")
    table = soup.find("table", {"id": "constituents"})
    if not table:
        raise ValueError("Could not find S&P 500 constituents table in Wikipedia")

    # Parse table
    rows = []
    for tr in table.find("tbody").find_all("tr")[1:]:  # Skip header
        cells = tr.find_all("td")
        if len(cells) >= 2:
            ticker = cells[0].text.strip()
            company = cells[1].text.strip()
            rows.append({"ticker": ticker, "company_name": company})

    if len(rows) < 400:  # Sanity check
        raise ValueError(f"Only found {len(rows)} constituents, expected ~500")

    df = pd.DataFrame(rows)
    return df


def load_cache() -> dict:
    """Load the local cache manifest."""
    if not _cache_file().exists():
        return {"name": "S&P 500", "dates": {}}
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
        "source": "wikipedia",
        "note": "Represents composition at fetch time; historical accuracy not guaranteed"
    }
    save_cache(cache)

    print(f"Cached S&P 500 {date_str}: {len(df)} constituents -> {uri}")


def list_cached(cache: dict):
    """Print all cached dates."""
    if not cache.get("dates"):
        print("No cached S&P 500 constituents.")
        return

    print(f"\n{cache['name']}:")
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
    print(f"Fetching S&P 500 constituents for {fetch_date}...")
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
