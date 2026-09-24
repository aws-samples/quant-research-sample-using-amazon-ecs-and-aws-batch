#!/usr/bin/env python3
"""SPY benchmark Sharpe for the 10-year basket study window.

Reads the study's daily_returns.parquet directly from S3 to get the exact
trade-day window, fetches SPY daily bars from Alpaca (free SIP, 1Day bars),
and computes Sharpe with the study's convention (mean/std * sqrt(252),
aggregate.py). Reports buy-and-hold (close-to-close) and open->close
(the strategy's intraday holding window), and writes a small parquet next
to the study artifacts:
    s3://<data_bucket>/earnings-basket-study/results-10y/aggregate/spy_benchmark.parquet

Credentials from the Secrets Manager secret named by config.json secrets.alpaca.

Usage:
    scripts/.venv/bin/python scripts/spy_sharpe_10y.py [--profile <profile>]
"""

import argparse
import json
import sys
import time
from io import BytesIO

import boto3
import numpy as np
import pandas as pd
import requests

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
RESULTS_PREFIX = "earnings-basket-study/results-10y/aggregate"
BARS_URL = "https://data.alpaca.markets/v2/stocks/SPY/bars"
TRADING_DAYS = 252
MAX_RETRIES = 2


def sharpe(r: np.ndarray) -> float:
    s = r.std(ddof=1)
    return float(r.mean() / s * np.sqrt(TRADING_DAYS)) if s > 0 else np.nan


def fetch_spy_daily(key_id: str, secret: str, start: str, end: str) -> pd.DataFrame:
    headers = {"APCA-API-KEY-ID": key_id, "APCA-API-SECRET-KEY": secret}
    params = {"timeframe": "1Day", "start": start, "end": end,
              "limit": 10000, "adjustment": "all", "feed": "sip"}
    bars, page_token = [], None
    while True:
        if page_token:
            params["page_token"] = page_token
        for attempt in range(MAX_RETRIES + 1):
            resp = requests.get(BARS_URL, headers=headers, params=params, timeout=60)
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                time.sleep(int(resp.headers.get("Retry-After", 2 * (attempt + 1))))
                continue
            break
        resp.raise_for_status()
        payload = resp.json()
        bars.extend(payload.get("bars") or [])
        page_token = payload.get("next_page_token")
        if not page_token:
            break
    df = pd.DataFrame(bars).rename(columns={"t": "timestamp", "o": "open", "c": "close"})
    ts = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert("America/New_York")
    df["date"] = ts.dt.strftime("%Y-%m-%d")
    return df[["date", "open", "close"]].sort_values("date").reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default=None, help="AWS profile (default: settings aws.profile)")
    args = ap.parse_args()
    args.profile = args.profile or settings.get("aws", "profile")

    sess = boto3.Session(profile_name=args.profile)
    s3 = sess.client("s3")
    sm = sess.client("secretsmanager")
    creds = json.loads(sm.get_secret_value(SecretId=settings.get("secrets", "alpaca"))["SecretString"])

    dr = pd.read_parquet(BytesIO(s3.get_object(
        Bucket=_bucket(), Key=f"{RESULTS_PREFIX}/daily_returns.parquet")["Body"].read()))
    days = pd.to_datetime(dr["trade_day"]).dt.strftime("%Y-%m-%d").sort_values()
    d0, d1 = days.iloc[0], days.iloc[-1]
    print(f"study window: {d0} -> {d1} ({days.nunique()} trade days)")

    spy = fetch_spy_daily(creds["key_id"], creds["secret_key"],
                          start=(pd.Timestamp(d0) - pd.Timedelta(days=7)).strftime("%Y-%m-%d"),
                          end=d1)
    print(f"SPY daily bars: {len(spy)} rows {spy['date'].iloc[0]} -> {spy['date'].iloc[-1]}")

    spy = spy.set_index("date")
    spy["buy_hold"] = spy["close"].pct_change()
    spy["open_close"] = spy["close"] / spy["open"] - 1.0

    # align to the study's trade days (the study trades a subset of calendar days)
    aligned = spy.reindex(days.unique())
    n_missing = int(aligned["buy_hold"].isna().sum())
    if n_missing:
        print(f"warning: {n_missing} study trade days missing SPY bars (filled flat)")
    aligned = aligned.fillna({"buy_hold": 0.0, "open_close": 0.0})

    rows = []
    for col, label in [("buy_hold", "SPY buy&hold (close-to-close)"),
                       ("open_close", "SPY open->close (holding-window matched)")]:
        r = aligned[col].to_numpy()
        total_pct = (np.prod(1 + r) - 1) * 100
        rows.append({"series": label, "sharpe": sharpe(r),
                     "total_return_pct": total_pct,
                     "ann_return_pct": ((1 + total_pct / 100) ** (TRADING_DAYS / len(r)) - 1) * 100,
                     "ann_vol_pct": r.std(ddof=1) * np.sqrt(TRADING_DAYS) * 100,
                     "n_days": len(r), "window": f"{d0} -> {d1}"})
        print(f"{label}: sharpe={rows[-1]['sharpe']:.3f} total={total_pct:+.1f}% "
              f"annret={rows[-1]['ann_return_pct']:+.2f}% vol={rows[-1]['ann_vol_pct']:.1f}%")

    out = pd.DataFrame(rows)
    buf = BytesIO()
    out.to_parquet(buf, index=False)
    key = f"{RESULTS_PREFIX}/spy_benchmark.parquet"
    s3.put_object(Bucket=_bucket(), Key=key, Body=buf.getvalue())
    print(f"wrote s3://{_bucket()}/{key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
