"""Consolidate the per-event result shards into ONE parquet object in S3.

The 10-year study leaves ~168k tiny shard files under <s3-prefix>/shards/;
a polars scan over that glob takes >30 min, so every consumer that needs the
whole table (finalize_local.py, portfolio_analysis.py) reads the consolidated
object instead — straight from S3, no local copies.

    AWS_PROFILE=<profile> python consolidate_shards.py \
        --s3-prefix earnings-basket-study/results-10y [--threads 160]

Writes <s3-prefix>/shards_consolidated/shards.parquet (all statuses, all
columns; consumers filter status == "ok") and a README.md next to it. Re-run
after any shard backfill. Everything streams through memory.
"""
import argparse
import datetime as dt
import io
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

import aggregate  # noqa: E402
import settings

CONSOLIDATED = "shards_consolidated/shards.parquet"


def consolidated_uri(s3_prefix: str) -> str:
    return f"s3://{settings.get("s3", "data_bucket")}/{s3_prefix}/{CONSOLIDATED}"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--s3-prefix", default="earnings-basket-study/results-10y")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--threads", type=int, default=160)
    args = ap.parse_args()

    sess = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3 = sess.client("s3", config=boto3.session.Config(max_pool_connections=args.threads + 40))
    bucket, prefix = settings.get("s3", "data_bucket"), f"{args.s3_prefix}/shards/"
    t = time.time()
    keys = [o["Key"] for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
            for o in pg.get("Contents", []) if o["Key"].endswith(".parquet")]
    print(f"{len(keys)} shard keys listed in {time.time() - t:.0f}s", flush=True)

    def read(key):
        for attempt in range(3):
            try:
                return pq.read_table(io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read()))
            except Exception:
                if attempt == 2:
                    raise
                time.sleep(1 + attempt)

    tables, done = [], 0
    with ThreadPoolExecutor(args.threads) as ex:
        for tb in ex.map(read, keys, chunksize=64):
            tables.append(tb)
            done += 1
            if done % 20000 == 0:
                print(f"  {done} read, {time.time() - t:.0f}s", flush=True)
    big = pa.concat_tables(tables, promote_options="default")
    print(f"consolidated {big.num_rows} rows x {big.num_columns} cols in {time.time() - t:.0f}s", flush=True)

    buf = io.BytesIO()
    pq.write_table(big, buf)
    s3.put_object(Bucket=bucket, Key=f"{args.s3_prefix}/{CONSOLIDATED}", Body=buf.getvalue())
    readme = (f"# shards_consolidated\n\nAll {len(keys)} shard files under ../shards/ concatenated into one "
              f"parquet ({big.num_rows} rows x {big.num_columns} cols), every status, every column.\n"
              f"Built {dt.datetime.now(dt.timezone.utc):%Y-%m-%d %H:%M UTC} by "
              f"earnings_basket_study/consolidate_shards.py. Consumers (finalize_local.py, "
              f"portfolio_analysis.py) read this object directly from S3; filter status == 'ok'.\n"
              f"Re-run consolidate_shards.py after any shard backfill.\n")
    s3.put_object(Bucket=bucket, Key=f"{args.s3_prefix}/shards_consolidated/README.md", Body=readme.encode())
    print(f"wrote {consolidated_uri(args.s3_prefix)} ({len(buf.getvalue()) / 1e6:.0f} MB)")


if __name__ == "__main__":
    main()
