"""S3 layout and IO — everything is parquet, read/written with Polars.

Layout under s3://<bucket>/<prefix>/ :
  manifest/job=<job>/shard=NNNN.parquet + _manifest_summary.json
  content/job=<job>/shard=<i>/part-<epoch_ms>.parquet   # worker checkpoints,
                                                        # append-only, CONTENT_SCHEMA
  content_by_date/date=YYYY-MM-DD/region=XX/part-*.parquet   # optional compacted view

Worker checkpoints ARE the content store: each part carries manifest columns
+ fetch metadata + raw_content bytes + text_content. compact-content
repartitions them into the date/region hive layout afterwards.
"""

import io
import json
import logging
import time
from typing import List, Optional, Set

import boto3
import polars as pl

logger = logging.getLogger(__name__)


def manifest_shard_key(prefix: str, job_name: str, shard: int) -> str:
    return f"{prefix}/manifest/job={job_name}/shard={shard:04d}.parquet"


def manifest_summary_key(prefix: str, job_name: str) -> str:
    return f"{prefix}/manifest/job={job_name}/_manifest_summary.json"


def content_prefix(prefix: str, job_name: str, shard: int) -> str:
    return f"{prefix}/content/job={job_name}/shard={shard}/"


def backfill_part_key(prefix: str, job_name: str, src_shard: int, fetch_shard: int) -> str:
    """Manifest fragment written by manifest-scan worker `src_shard`, holding
    the failed rows whose symbol-hash assigns them to fetch shard `fetch_shard`.
    Fetch worker S globs .../fshard=S/ to gather its companies from every src."""
    return (f"{prefix}/backfill_manifest/job={job_name}/"
            f"fshard={fetch_shard}/src={src_shard:04d}.parquet")


def backfill_fetch_shard_prefix(prefix: str, job_name: str, fetch_shard: int) -> str:
    return f"{prefix}/backfill_manifest/job={job_name}/fshard={fetch_shard}/"


class S3Io:
    def __init__(self, bucket: str, prefix: str,
                 boto3_session: Optional[boto3.Session] = None):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        sess = boto3_session or boto3.Session()
        self.s3 = sess.client("s3")

    # ---------------------------------------------------------------- objects

    def put_bytes(self, key: str, body: bytes,
                  content_type: str = "application/octet-stream"):
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body,
                           ContentType=content_type)

    def get_bytes(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def put_json(self, key: str, data: dict):
        self.put_bytes(key, json.dumps(data, indent=2).encode("utf-8"),
                       content_type="application/json")

    def get_json(self, key: str) -> dict:
        return json.loads(self.get_bytes(key))

    def list_keys(self, key_prefix: str) -> List[str]:
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=key_prefix):
            keys.extend(o["Key"] for o in page.get("Contents", []))
        return keys

    # --------------------------------------------------------------- parquet

    def put_frame(self, key: str, df: pl.DataFrame):
        buf = io.BytesIO()
        df.write_parquet(buf, compression="zstd")
        self.put_bytes(key, buf.getvalue())

    def get_frame(self, key: str) -> pl.DataFrame:
        return pl.read_parquet(io.BytesIO(self.get_bytes(key)))

    # ------------------------------------------------------------ checkpoints

    def load_completed_event_ids(self, job_name: str, shard: int,
                                 retry_statuses: Optional[List[str]] = None) -> Set[int]:
        """Event ids already processed in prior runs of this shard.

        Any terminal status counts as done unless listed in retry_statuses.
        Reads only the two columns it needs — content parts carry document
        bytes, so a full read would be wasteful.
        """
        retry = set(retry_statuses or [])
        done: Set[int] = set()
        for key in self.list_keys(content_prefix(self.prefix, job_name, shard)):
            if not key.endswith(".parquet"):
                continue
            try:
                # Column-pruned read: parts carry raw document bytes, and a
                # full read of every part OOMs the worker on resume.
                import pyarrow.parquet as pq

                table = pq.read_table(
                    io.BytesIO(self.get_bytes(key)),
                    columns=["event_id", "fetch_status"],
                )
                df = pl.from_arrow(table)
            except Exception as e:
                logger.warning("unreadable checkpoint %s: %s", key, e)
                continue
            if retry:
                df = df.filter(~pl.col("fetch_status").is_in(list(retry)))
            done.update(df.get_column("event_id").to_list())
        return done

    def write_content_part(self, job_name: str, shard: int, df: pl.DataFrame) -> str:
        """Append-only flush of processed rows (manifest cols + content cols)."""
        key = (f"{content_prefix(self.prefix, job_name, shard)}"
               f"part-{int(time.time() * 1000)}.parquet")
        self.put_frame(key, df)
        logger.info("checkpoint: %d rows -> %s", df.height, key)
        return key

    def write_done_marker(self, job_name: str, shard: int, status_counts: dict):
        key = f"{content_prefix(self.prefix, job_name, shard)}_done.json"
        self.put_json(key, {
            "shard": shard,
            "status_counts": status_counts,
            "completed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
