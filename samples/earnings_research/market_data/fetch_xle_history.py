"""Fetch historical 1-minute bars for XLE universe from Alpaca.

Usage:
    # Single ticker test
    python fetch_xle_history.py --ticker COP --start 2007-01-01 --end 2026-12-31

    # All XLE tickers (submit 11 parallel batch jobs)
    python fetch_xle_history.py --all --start 2007-01-01 --end 2026-12-31 --submit-batch
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd

# Add parent dir to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from alpaca_client import AlpacaClient
from logging_config import configure_logging

logger = logging.getLogger(__name__)

XLE_TICKERS = [
    'APA', 'BKR', 'COP', 'CVX', 'DVN', 'EOG', 'EQT', 'EXE', 'FANG', 'HAL', 'KMI',
    'MPC', 'OKE', 'OXY', 'PSX', 'SLB', 'TPL', 'TRGP', 'VLO', 'WMB', 'XOM'
]

import settings


def _bucket() -> str:
    return settings.get("s3", "data_bucket")


OUT_PREFIX = "earnings-market-data/XLE_HISTORY"

# Batch target: settings batch.alpaca_job_queue (capped so Alpaca rate limits hold)
# and batch.job_definitions.market_data.


def fetch_ticker_history(ticker: str, start: str, end: str, client: AlpacaClient, s3_client) -> dict:
    """Fetch full 1-minute history for one ticker and write to S3."""
    logger.info(f"Fetching {ticker} from {start} to {end}")

    try:
        # Get the data
        df = client.get_range(ticker, start, end, schema="ohlcv-1m")

        if df is None or len(df) == 0:
            logger.warning(f"{ticker}: no data returned")
            return {
                "ticker": ticker,
                "status": "no_data",
                "bars": 0,
                "start": start,
                "end": end
            }

        # Add symbol column
        df.insert(0, "symbol", ticker)

        # Convert timestamp to datetime
        if 'ts_event' in df.columns:
            df['timestamp'] = pd.to_datetime(df['ts_event'], unit='ns', utc=True)
            df = df.drop('ts_event', axis=1)

        # Write to S3 as parquet
        output_key = f"{OUT_PREFIX}/ticker={ticker}/data.parquet"

        logger.info(f"{ticker}: writing {len(df):,} bars to s3://{_bucket()}/{output_key}")

        # Write to S3
        parquet_buffer = df.to_parquet(index=False)
        s3_client.put_object(
            Bucket=_bucket(),
            Key=output_key,
            Body=parquet_buffer
        )

        return {
            "ticker": ticker,
            "status": "success",
            "bars": len(df),
            "start": str(df['timestamp'].min()) if len(df) > 0 else start,
            "end": str(df['timestamp'].max()) if len(df) > 0 else end,
            "s3_uri": f"s3://{_bucket()}/{output_key}",
            "size_mb": round(len(parquet_buffer) / (1024 * 1024), 2)
        }

    except Exception as e:
        logger.error(f"{ticker}: fetch failed: {e}", exc_info=True)
        return {
            "ticker": ticker,
            "status": "error",
            "error": str(e),
            "start": start,
            "end": end
        }


def submit_batch_jobs(tickers: list[str], start: str, end: str, profile: str = None) -> list[str]:
    """Submit one batch job per ticker."""
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    batch = session.client("batch")

    job_ids = []

    for ticker in tickers:
        job_name = f"xle-history-{ticker.lower()}-{start}-{end}".replace("_", "-")

        try:
            resp = batch.submit_job(
                jobName=job_name,
                jobQueue=settings.get("batch", "alpaca_job_queue"),
                jobDefinition=settings.get("batch", "job_definitions", "market_data"),
                containerOverrides={
                    "command": [
                        "python", "fetch_xle_history.py",
                        "--ticker", ticker,
                        "--start", start,
                        "--end", end
                    ]
                }
            )
            job_id = resp["jobId"]
            job_ids.append(job_id)
            logger.info(f"✓ Submitted {ticker}: {job_id}")

        except Exception as e:
            logger.error(f"✗ Failed to submit {ticker}: {e}")

    return job_ids


def main():
    parser = argparse.ArgumentParser(description="Fetch XLE historical 1-minute bars from Alpaca")
    parser.add_argument("--ticker", help="Single ticker to fetch (or --all)")
    parser.add_argument("--all", action="store_true", help="Fetch all XLE tickers")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--submit-batch", action="store_true",
                       help="Submit batch jobs (one per ticker) instead of running locally")
    parser.add_argument("--profile", default=None, help="AWS profile (default: credential chain)")
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()

    configure_logging(args.log_level)

    # Determine ticker list
    if args.all:
        tickers = XLE_TICKERS
    elif args.ticker:
        tickers = [args.ticker]
    else:
        parser.error("Must specify --ticker or --all")

    logger.info(f"XLE History Fetch: {len(tickers)} tickers, {args.start} to {args.end}")

    # Submit batch jobs
    if args.submit_batch:
        logger.info("Submitting batch jobs...")
        job_ids = submit_batch_jobs(tickers, args.start, args.end, args.profile)

        result = {
            "submitted": len(job_ids),
            "tickers": tickers,
            "job_ids": job_ids,
            "job_queue": settings.get("batch", "alpaca_job_queue"),
            "start": args.start,
            "end": args.end
        }

        print(json.dumps(result, indent=2))
        return 0

    # Run locally (single ticker or small test)
    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    client = AlpacaClient(boto3_session=session)
    s3_client = session.client("s3")

    results = []
    for ticker in tickers:
        result = fetch_ticker_history(ticker, args.start, args.end, client, s3_client)
        results.append(result)
        print(json.dumps(result, indent=2))

    # Summary
    succeeded = sum(1 for r in results if r["status"] == "success")
    total_bars = sum(r.get("bars", 0) for r in results if r["status"] == "success")

    summary = {
        "tickers_fetched": len(results),
        "succeeded": succeeded,
        "failed": len(results) - succeeded,
        "total_bars": total_bars,
        "output": f"s3://{_bucket()}/{OUT_PREFIX}/"
    }

    print("\n" + "="*80)
    print(json.dumps(summary, indent=2))

    return 0 if succeeded == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
