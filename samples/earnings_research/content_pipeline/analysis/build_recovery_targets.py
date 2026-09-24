#!/usr/bin/env python3
"""Build the large-tier recovery target list from v1 meta exclusions and integrity failures."""
import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import settings  # noqa: E402
import pyarrow.parquet as pq
import requests


def read_s3_parquet(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """Read a parquet file from S3."""
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def read_manifest_shards(s3_client, bucket: str, prefix: str, event_ids: set, max_workers: int = 16) -> pd.DataFrame:
    """Read all manifest shards in parallel, filtering to our event_ids."""
    # List all shards
    paginator = s3_client.get_paginator("list_objects_v2")
    shard_keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            if obj["Key"].endswith(".parquet"):
                shard_keys.append(obj["Key"])

    print(f"Found {len(shard_keys)} manifest shards", file=sys.stderr)

    # Read shards in parallel
    dfs = []
    def read_shard(key):
        df = read_s3_parquet(s3_client, bucket, key)
        # Filter to our event_ids immediately to reduce memory
        return df[df["event_id"].isin(event_ids)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(read_shard, key): key for key in shard_keys}
        for i, future in enumerate(as_completed(futures), 1):
            if i % 50 == 0:
                print(f"  Read {i}/{len(shard_keys)} shards...", file=sys.stderr)
            try:
                df = future.result()
                if not df.empty:
                    dfs.append(df)
            except Exception as e:
                print(f"  Warning: failed to read {futures[future]}: {e}", file=sys.stderr)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def fetch_sec_tickers() -> set:
    """Fetch current SEC ticker list."""
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {"User-Agent": f"quant-research-earnings-pipeline/1.0 (contact: {settings.get('contact_email')})"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # The JSON is {idx: {cik_str, ticker, title}, ...}
    return {entry["ticker"].upper() for entry in data.values()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--event-list",
        default=f"s3://{settings.get('s3', 'diagnostics_bucket')}/diagnostics/earnings_universe/large_tier_events.parquet",
        help="Event list parquet URI",
    )
    parser.add_argument(
        "--meta",
        default=f"s3://{settings.get('s3', 'diagnostics_bucket')}/diagnostics/earnings_universe/large_tier_meta.json",
        help="v1 meta JSON URI",
    )
    parser.add_argument(
        "--integrity",
        default=f"s3://{settings.get('s3', 'diagnostics_bucket')}/diagnostics/earnings_universe/large_tier_integrity_v1.parquet",
        help="v1 integrity audit parquet URI",
    )
    parser.add_argument(
        "--out",
        default=f"s3://{settings.get('s3', 'diagnostics_bucket')}/diagnostics/earnings_universe/large_tier_recovery_targets.parquet",
        help="Output parquet URI",
    )
    parser.add_argument("--profile", default=None, help="AWS profile (default: credential chain)")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name="us-east-1")
    s3 = session.client("s3")

    # 1. Read event list
    print("Reading event list...", file=sys.stderr)
    event_bucket, event_key = args.event_list.replace("s3://", "").split("/", 1)
    events_df = read_s3_parquet(s3, event_bucket, event_key)
    print(f"  {len(events_df)} events", file=sys.stderr)

    # 2. Read v1 meta
    print("Reading v1 meta...", file=sys.stderr)
    meta_bucket, meta_key = args.meta.replace("s3://", "").split("/", 1)
    meta_obj = s3.get_object(Bucket=meta_bucket, Key=meta_key)
    meta = json.loads(meta_obj["Body"].read())
    scorable_event_ids = set(meta["scorable_event_ids"])
    print(f"  {len(scorable_event_ids)} scorable events", file=sys.stderr)

    # v1 excluded events with reasons
    excluded_meta = {item["event_id"]: item["reason"] for item in meta.get("excluded", [])}
    print(f"  {len(excluded_meta)} excluded in meta", file=sys.stderr)

    # 3. Read v1 integrity audit
    print("Reading integrity audit...", file=sys.stderr)
    integ_bucket, integ_key = args.integrity.replace("s3://", "").split("/", 1)
    integrity_df = read_s3_parquet(s3, integ_bucket, integ_key)
    integrity_failures = integrity_df[integrity_df["excluded"] == True].copy()
    print(f"  {len(integrity_failures)} integrity failures", file=sys.stderr)

    # 4. Compute targets: (events ∖ scorable) ∪ integrity_failures
    missing_from_v1 = events_df[~events_df["event_id"].isin(scorable_event_ids)].copy()
    print(f"  {len(missing_from_v1)} events missing from scorable", file=sys.stderr)

    # Assign reasons
    missing_from_v1["prior_reason"] = missing_from_v1["event_id"].map(excluded_meta)
    missing_from_v1["prior_reason"] = missing_from_v1["prior_reason"].fillna("not_in_v1_scorable")

    integrity_failures["prior_reason"] = "integrity_" + integrity_failures["exclusion_reason"]
    # Keep only event_id + prior_reason from integrity, we'll join manifest fields later
    integrity_targets = integrity_failures[["event_id", "prior_reason"]].copy()

    # Merge with events_df to get symbol, event_date, event_datetime_utc
    integrity_targets = integrity_targets.merge(
        events_df[["event_id", "symbol", "event_date", "event_datetime_utc"]],
        on="event_id",
        how="left"
    )

    # Union
    targets_df = pd.concat([missing_from_v1, integrity_targets], ignore_index=True)
    targets_df = targets_df.drop_duplicates(subset=["event_id"])
    print(f"  {len(targets_df)} total targets (after dedup)", file=sys.stderr)

    # 5. Read manifest index and join
    print("Reading manifest index...", file=sys.stderr)
    manifest_bucket = settings.get("s3", "data_bucket")
    manifest_prefix = "earnings-content/manifest/job=er-full/"
    target_event_ids = set(targets_df["event_id"])
    manifest_df = read_manifest_shards(s3, manifest_bucket, manifest_prefix, target_event_ids)
    print(f"  {len(manifest_df)} manifest rows for our events", file=sys.stderr)

    # Join manifest fields
    manifest_cols = ["event_id", "entity_proper_name", "ticker_region", "factset_entity_id", "fiscal_period", "fiscal_year"]
    manifest_subset = manifest_df[manifest_cols].drop_duplicates(subset=["event_id"])

    targets_df = targets_df.merge(manifest_subset, on="event_id", how="left")

    # Fill missing manifest fields with defaults
    targets_df["entity_proper_name"] = targets_df["entity_proper_name"].fillna(targets_df["symbol"])
    targets_df["ticker_region"] = targets_df["ticker_region"].fillna(targets_df["symbol"] + "-US")
    targets_df["factset_entity_id"] = targets_df["factset_entity_id"].fillna("")
    targets_df["fiscal_period"] = targets_df["fiscal_period"].fillna("")
    targets_df["fiscal_year"] = targets_df["fiscal_year"].fillna(0).astype("int64")

    # 6. Final column order
    final_cols = [
        "event_id",
        "symbol",
        "event_date",
        "event_datetime_utc",
        "entity_proper_name",
        "ticker_region",
        "factset_entity_id",
        "fiscal_period",
        "fiscal_year",
        "prior_reason",
    ]
    targets_df = targets_df[final_cols]

    # 7. Write output
    print(f"Writing {len(targets_df)} rows...", file=sys.stderr)
    out_bucket, out_key = args.out.replace("s3://", "").split("/", 1)
    parquet_bytes = targets_df.to_parquet(index=False)
    s3.put_object(Bucket=out_bucket, Key=out_key, Body=parquet_bytes)
    print(f"Wrote {args.out}", file=sys.stderr)

    # 8. Print summary
    print("\n=== SUMMARY ===")
    print(f"Total recovery targets: {len(targets_df)}")

    print("\nBy prior_reason:")
    reason_counts = targets_df["prior_reason"].value_counts()
    for reason, count in reason_counts.head(10).items():
        print(f"  {reason}: {count}")

    print("\nBy year:")
    targets_df["year"] = pd.to_datetime(targets_df["event_date"]).dt.year
    year_counts = targets_df["year"].value_counts().sort_index()
    for year, count in year_counts.items():
        print(f"  {year}: {count}")

    # Names from manifest (not equal to symbol)
    from_manifest = (targets_df["entity_proper_name"] != targets_df["symbol"]).sum()
    print(f"\nEntity names from manifest: {from_manifest}")

    # SEC ticker match
    print("\nFetching SEC tickers...", file=sys.stderr)
    sec_tickers = fetch_sec_tickers()
    sec_matches = targets_df["symbol"].str.upper().isin(sec_tickers).sum()
    print(f"Current SEC ticker matches: {sec_matches}")


if __name__ == "__main__":
    main()
