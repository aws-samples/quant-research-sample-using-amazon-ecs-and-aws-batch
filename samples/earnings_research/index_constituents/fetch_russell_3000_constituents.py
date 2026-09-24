#!/usr/bin/env python
"""Fetch and cache Russell 3000 constituents by date.

TWO METHODS:
1. Derived (default): Union of Russell 1000 + Russell 2000 (most complete, ~2985 tickers)
2. Actual (--actual): Direct download from iShares IWV ETF (sampled, ~2584 tickers)

Usage:
    # Derived (R1000 + R2000):
    scripts/.venv/bin/python scripts/fetch_russell_3000_constituents.py 2026-08-10

    # Actual (IWV ETF):
    scripts/.venv/bin/python scripts/fetch_russell_3000_constituents.py 2026-08-10 --actual

    # List cached:
    scripts/.venv/bin/python scripts/fetch_russell_3000_constituents.py --list

Caches to S3: s3://{bucket}/russell-3000-constituents/date={date}/weights.csv
              s3://{bucket}/russell-3000-actual-constituents/date={date}/weights.csv
Updates local catalog: <constituent_cache_dir>/russell_3000_constituents_cache.json
                      <constituent_cache_dir>/russell_3000_actual_constituents_cache.json

Recommendation: Use derived method for complete membership; IWV is an optimized/sampled ETF.
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
S3_PREFIX_DERIVED = "russell-3000-constituents"
S3_PREFIX_ACTUAL = "russell-3000-actual-constituents"

def _cache_file_derived() -> Path:
    return settings.root() / settings.get("paths", "constituent_cache_dir") / "russell_3000_constituents_cache.json"
def _cache_file_actual() -> Path:
    return settings.root() / settings.get("paths", "constituent_cache_dir") / "russell_3000_actual_constituents_cache.json"

# iShares Russell 3000 ETF (IWV)
ISHARES_PRODUCT_ID = "239714"
ISHARES_TICKER = "iwv"
ISHARES_URL = f"https://www.ishares.com/us/products/{ISHARES_PRODUCT_ID}/{ISHARES_TICKER}/latest-holdings.csv"

# iShares Russell 1000 and 2000 for derived method
IWB_PRODUCT_ID = "239707"
IWM_PRODUCT_ID = "239710"
IWB_URL = f"https://www.ishares.com/us/products/{IWB_PRODUCT_ID}/iwb/latest-holdings.csv"
IWM_URL = f"https://www.ishares.com/us/products/{IWM_PRODUCT_ID}/iwm/latest-holdings.csv"


def fetch_ishares_holdings(url: str, min_holdings: int) -> pd.DataFrame:
    """Common helper to fetch and parse iShares ETF holdings."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }

    resp = requests.get(url, headers=headers, timeout=30)
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

    # Filter to equities only
    df = df[df['Asset Class'] == 'Equity'].copy()

    # Clean and standardize
    df['Ticker'] = df['Ticker'].str.strip().str.upper()
    df = df.rename(columns={'Name': 'company_name', 'Ticker': 'ticker', 'Weight (%)': 'weight'})

    # Select columns
    result = df[['ticker', 'company_name', 'weight']].copy()
    result = result.drop_duplicates(subset=['ticker'], keep='first')

    if len(result) < min_holdings:
        raise ValueError(f"Only found {len(result)} holdings, expected at least {min_holdings}")

    return result


def fetch_constituents_derived(date: dt_date) -> pd.DataFrame:
    """Fetch Russell 3000 constituents by deriving from R1000 + R2000.

    This is the RECOMMENDED method: produces ~2985 tickers (the full index).
    """
    print("  Fetching Russell 1000 (IWB)...")
    r1000 = fetch_ishares_holdings(IWB_URL, min_holdings=900)
    print(f"    ✅ {len(r1000)} constituents")

    print("  Fetching Russell 2000 (IWM)...")
    r2000 = fetch_ishares_holdings(IWM_URL, min_holdings=1800)
    print(f"    ✅ {len(r2000)} constituents")

    # Merge (should be no overlap, but use concat + drop_duplicates to be safe)
    r3000 = pd.concat([r1000, r2000], ignore_index=True)
    r3000 = r3000.drop_duplicates(subset=['ticker'], keep='first')

    # Recalculate weights to sum to 100% (weights from R1000 and R2000 are within their respective indices)
    # For simplicity, just normalize existing weights
    r3000['weight'] = r3000['weight'] / r3000['weight'].sum() * 100.0

    # Verify no overlap
    overlap = set(r1000['ticker']) & set(r2000['ticker'])
    if overlap:
        print(f"  ⚠️  WARNING: {len(overlap)} overlapping tickers between R1000 and R2000: {sorted(list(overlap))[:10]}")

    print(f"  ✅ Derived Russell 3000: {len(r3000)} constituents (R1000 + R2000)")

    return r3000


def fetch_constituents_actual(date: dt_date) -> pd.DataFrame:
    """Fetch Russell 3000 constituents directly from iShares IWV ETF.

    Note: IWV is an optimized/sampled ETF that only holds ~2584 of the ~3000
    index members. Use derived method for complete membership.
    """
    print(f"  Fetching Russell 3000 (IWV) from iShares product {ISHARES_PRODUCT_ID}...")
    df = fetch_ishares_holdings(ISHARES_URL, min_holdings=2400)
    print(f"  ✅ {len(df)} constituents (note: IWV is sampled, not full replication)")

    return df


def load_cache(actual: bool) -> dict:
    """Load the local cache manifest."""
    cache_file = _cache_file_actual() if actual else _cache_file_derived()
    if not cache_file.exists():
        return {
            "name": "Russell 3000",
            "source": "iShares IWV ETF" if actual else "Derived (R1000 + R2000)",
            "dates": {}
        }
    return json.loads(cache_file.read_text())


def save_cache(cache: dict, actual: bool):
    """Save the local cache manifest."""
    cache_file = _cache_file_actual() if actual else _cache_file_derived()
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(cache, indent=2) + "\n")


def get_cached_path(date: dt_date, actual: bool) -> tuple[str, str]:
    """Return (s3_key, s3_uri) for a given date."""
    prefix = S3_PREFIX_ACTUAL if actual else S3_PREFIX_DERIVED
    date_str = date.strftime("%Y-%m-%d")
    key = f"{prefix}/date={date_str}/weights.csv"
    uri = f"s3://{_bucket()}/{key}"
    return key, uri


def is_cached(date: dt_date, s3, actual: bool) -> bool:
    """Check if data exists in S3."""
    key, _ = get_cached_path(date, actual)
    try:
        s3.head_object(Bucket=_bucket(), Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def cache_constituents(date: dt_date, df: pd.DataFrame, s3, cache: dict, profile: str, actual: bool):
    """Upload to S3 and update local cache."""
    key, uri = get_cached_path(date, actual)
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
        "source": cache["source"],
        "note": "IWV is sampled ETF, not full replication" if actual else "Derived from R1000 + R2000 union"
    }
    save_cache(cache, actual)

    method = "actual (IWV)" if actual else "derived (R1000+R2000)"
    print(f"Cached Russell 3000 [{method}] {date_str}: {len(df)} constituents -> {uri}")


def list_cached(cache: dict):
    """Print all cached dates."""
    if not cache.get("dates"):
        print(f"No cached {cache['name']} constituents ({cache.get('source', 'unknown')}).")
        return

    print(f"\n{cache['name']} ({cache.get('source', 'unknown')}):")
    for d in sorted(cache["dates"].keys()):
        info = cache["dates"][d]
        print(f"  {d}: {info['n_constituents']} constituents, {info['s3_uri']}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("date", nargs="?",
                    help="Date (YYYY-MM-DD, defaults to today)")
    ap.add_argument("--actual", action="store_true",
                    help="Use actual IWV ETF holdings instead of derived (R1000+R2000)")
    ap.add_argument("--list", action="store_true",
                    help="List all cached dates")
    ap.add_argument("--profile", default=None,
                    help="AWS profile (default: settings aws.profile)")
    args = ap.parse_args()
    args.profile = args.profile or settings.get("aws", "profile")

    cache = load_cache(args.actual)

    if args.list:
        # List both derived and actual
        if not args.actual:
            cache_derived = load_cache(False)
            cache_actual = load_cache(True)
            list_cached(cache_derived)
            list_cached(cache_actual)
        else:
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
    if is_cached(fetch_date, s3, args.actual):
        _, uri = get_cached_path(fetch_date, args.actual)
        method = "actual (IWV)" if args.actual else "derived (R1000+R2000)"
        print(f"Already cached [{method}]: {uri}")
        # Update cache metadata even if S3 has it
        if fetch_date.strftime("%Y-%m-%d") not in cache.get("dates", {}):
            df = pd.read_csv(uri)
            cache_constituents(fetch_date, df, s3, cache, args.profile, args.actual)
        return 0

    # Fetch and cache
    method = "actual (IWV ETF)" if args.actual else "derived (R1000 + R2000)"
    print(f"Fetching Russell 3000 constituents [{method}] for {fetch_date}...")
    try:
        if args.actual:
            df = fetch_constituents_actual(fetch_date)
        else:
            df = fetch_constituents_derived(fetch_date)

        cache_constituents(fetch_date, df, s3, cache, args.profile, args.actual)
        print(f"Success: {len(df)} constituents")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
