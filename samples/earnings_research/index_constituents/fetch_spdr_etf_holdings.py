#!/usr/bin/env python
"""Fetch and cache SPDR ETF constituents (S&P 1500 composite + 11 Select Sectors).

Usage:
    scripts/.venv/bin/python scripts/fetch_spdr_etf_holdings.py            # all funds
    scripts/.venv/bin/python scripts/fetch_spdr_etf_holdings.py XLK XLE   # subset
    scripts/.venv/bin/python scripts/fetch_spdr_etf_holdings.py --list

Caches to S3: s3://{bucket}/etf-constituents/fund={TICKER}/date={as-of}/holdings.csv
Updates local catalog index: <constituent_cache_dir>/spdr_etf_constituents_cache.json

Source: SSGA daily-holdings XLSX (the ETF provider — authoritative, free, daily).
The as-of date comes from inside the file, NOT the fetch date. Reads S3 directly,
no temp downloads; ≤2 retries per request (workflow preference).
"""
import argparse
import io
import json
import sys
import time
from pathlib import Path

import boto3
import pandas as pd
import requests

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
S3_PREFIX = "etf-constituents"
def _cache_file() -> Path:
    return settings.root() / settings.get("paths", "constituent_cache_dir") / "spdr_etf_constituents_cache.json"

# SSGA daily holdings endpoint; the same path pattern serves every US SPDR.
URL = ("https://www.ssga.com/us/en/individual/library-content/products/"
       "fund-data/etfs/us/holdings-daily-us-en-{ticker}.xlsx")

FUNDS = {
    "SPTM": "S&P 1500 Composite Stock Market",
    "XLC": "Communication Services Select Sector",
    "XLY": "Consumer Discretionary Select Sector",
    "XLP": "Consumer Staples Select Sector",
    "XLE": "Energy Select Sector",
    "XLF": "Financial Select Sector",
    "XLV": "Health Care Select Sector",
    "XLI": "Industrial Select Sector",
    "XLB": "Materials Select Sector",
    "XLRE": "Real Estate Select Sector",
    "XLK": "Technology Select Sector",
    "XLU": "Utilities Select Sector",
}

_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}


def fetch_holdings(ticker: str) -> tuple[pd.DataFrame, str]:
    """(holdings frame, as-of date YYYY-MM-DD) from SSGA's daily XLSX."""
    url = URL.format(ticker=ticker.lower())
    for attempt in range(3):                      # 1 try + ≤2 retries
        r = requests.get(url, headers=_HEADERS, timeout=60,
                         allow_redirects=True)
        if r.status_code == 200:
            break
        time.sleep(2)
    r.raise_for_status()

    raw = pd.read_excel(io.BytesIO(r.content), header=None)
    # Row 2 col 1: "As of DD-Mon-YYYY"; row 4 = column headers; rows 5+ = holdings
    as_of = pd.Timestamp(str(raw.iloc[2, 1]).replace("As of", "").strip()
                         ).strftime("%Y-%m-%d")
    df = raw.iloc[5:].copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in raw.iloc[4]]
    df = df.dropna(subset=["ticker"])
    df = df[df["ticker"].astype(str).str.strip().ne("-")]
    # equity holdings only: index-futures cash-management lines (XAE ENERGY
    # SEP26 etc.) carry no SEDOL — they are not constituents
    if "sedol" in df.columns:
        df = df[df["sedol"].astype(str).str.strip().ne("-")]
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
    df["shares_held"] = pd.to_numeric(df["shares_held"], errors="coerce")
    keep = [c for c in ("name", "ticker", "identifier", "sedol", "weight",
                        "sector", "shares_held", "local_currency")
            if c in df.columns]
    return df[keep].reset_index(drop=True), as_of


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("tickers", nargs="*", help="subset (default: all funds)")
    ap.add_argument("--list", action="store_true", help="show cached snapshots")
    ap.add_argument("--profile", default=None, help="AWS profile (default: settings aws.profile)")
    args = ap.parse_args()
    args.profile = args.profile or settings.get("aws", "profile")

    cache = (json.loads(_cache_file().read_text()) if _cache_file().exists()
             else {"name": "SPDR ETF constituents (SPTM + Select Sectors)",
                   "source": "SSGA daily-holdings XLSX",
                   "s3": f"s3://{_bucket()}/{S3_PREFIX}/fund=<T>/date=<D>/holdings.csv",
                   "funds": {}})

    if args.list:
        print(json.dumps(cache.get("funds", {}), indent=1))
        return

    tickers = [t.upper() for t in args.tickers] or list(FUNDS)
    unknown = [t for t in tickers if t not in FUNDS]
    if unknown:
        sys.exit(f"unknown fund(s): {unknown}; known: {list(FUNDS)}")

    s3 = boto3.Session(profile_name=args.profile).client("s3")
    for t in tickers:
        df, as_of = fetch_holdings(t)
        key = f"{S3_PREFIX}/fund={t}/date={as_of}/holdings.csv"
        s3.put_object(Bucket=_bucket(), Key=key,
                      Body=df.to_csv(index=False).encode())
        entry = cache["funds"].setdefault(
            t, {"fund_name": FUNDS[t], "dates": {}})
        entry["dates"][as_of] = {"holdings": len(df), "s3_key": key}
        print(f"{t:5} {as_of}: {len(df):5d} holdings -> s3://{_bucket()}/{key}")

    _cache_file().write_text(json.dumps(cache, indent=1, sort_keys=True))
    print(f"cache index updated: {_cache_file()}")


if __name__ == "__main__":
    main()
