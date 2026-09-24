"""Build <s3-prefix>/diagnostics/reporter_marks.parquet: one row per evaluated
event with the reporter's marks from the per-event facts.json bundle
(previous_close, signal_928, entry_930_open, exit_close). Used to attach the
reporter's 9:30 entry price to the ok frame (finalize_local.attach_reporter_price)
for the reporter-price scopes (PX<1 .. PX100+, study_config price_bins).

    AWS_PROFILE=<profile> python reporter_marks.py [--s3-prefix earnings-basket-study/results-10y] --write
"""
import argparse
import io
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
import pandas as pd
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parent))
import aggregate  # noqa: E402
import settings

MARKS_KEY = "diagnostics/reporter_marks.parquet"


def marks_uri(s3_prefix: str) -> str:
    return f"s3://{settings.get("s3", "data_bucket")}/{s3_prefix}/{MARKS_KEY}"


def build(s3, s3_prefix: str, threads: int = 96) -> pd.DataFrame:
    t0 = time.time()
    ids = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=settings.get("s3", "data_bucket"),
                                                             Prefix=f"{s3_prefix}/events/", Delimiter="/"):
        ids += [c["Prefix"] for c in page.get("CommonPrefixes", [])]
    print(f"{len(ids)} event bundles listed in {time.time() - t0:.0f}s", flush=True)

    def get(pfx):
        try:
            f = json.loads(s3.get_object(Bucket=settings.get("s3", "data_bucket"), Key=pfx + "facts.json")["Body"].read())
        except Exception as e:
            return {"event_id": int(pfx.rstrip("/").split("/")[-1]), "err": type(e).__name__}
        m = f.get("marks") or {}
        return {"event_id": f.get("event_id"), "event_symbol": f.get("reporter"), "trade_day": f.get("trade_day"),
                "previous_close": m.get("previous_close"), "signal_928": m.get("signal_928"),
                "entry_930_open": m.get("entry_930_open"), "exit_close": m.get("exit_1559_close")}

    with ThreadPoolExecutor(threads) as ex:
        rows = list(ex.map(get, ids))
    df = pd.DataFrame(rows)
    print(f"{len(df)} rows, {df['entry_930_open'].notna().sum()} with entry price, "
          f"{int(df['err'].notna().sum()) if 'err' in df else 0} errors, {time.time() - t0:.0f}s", flush=True)
    return df


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--s3-prefix", default="earnings-basket-study/results-10y")
    ap.add_argument("--profile", default=None)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    sess = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3 = sess.client("s3", config=Config(max_pool_connections=128, retries={"max_attempts": 8, "mode": "adaptive"}))
    df = build(s3, args.s3_prefix)
    if args.write:
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        s3.put_object(Bucket=settings.get("s3", "data_bucket"), Key=f"{args.s3_prefix}/{MARKS_KEY}", Body=buf.getvalue())
        print(f"wrote {marks_uri(args.s3_prefix)}")


if __name__ == "__main__":
    main()
