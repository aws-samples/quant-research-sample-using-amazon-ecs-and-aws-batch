"""Repartition worker content parts into the date/region hive layout.

Content parts under content/job=<job>/shard=*/part-*.parquet are grouped by
worker/checkpoint; queries usually want event-date/region slices. This
one-shot step rewrites them as
content_by_date/date=YYYY-MM-DD/region=XX/part-<n>.parquet.

Streams shard-by-shard: each part is read once, split by partition key, and
appended to per-partition accumulators that flush at a size threshold —
raw_content bytes make full materialization of 1.1M rows infeasible.
"""

import logging
from typing import Dict, List

import polars as pl

from config import Config
from s3_io import S3Io

logger = logging.getLogger(__name__)

_FLUSH_ROWS = 2000


def run(cfg: Config, boto3_session=None) -> dict:
    s3io = S3Io(cfg.s3.bucket, cfg.s3.prefix, boto3_session=boto3_session)
    job = cfg.job_name
    part_keys = [k for k in s3io.list_keys(f"{s3io.prefix}/content/job={job}/")
                 if k.endswith(".parquet")]
    logger.info("compacting %d content parts", len(part_keys))

    buffers: Dict[tuple, List[pl.DataFrame]] = {}
    buffered_rows: Dict[tuple, int] = {}
    part_counters: Dict[tuple, int] = {}
    total = 0

    def flush(pkey: tuple):
        frames = buffers.pop(pkey, [])
        buffered_rows.pop(pkey, None)
        if not frames:
            return
        df = pl.concat(frames)
        date, region = pkey
        n = part_counters.get(pkey, 0)
        part_counters[pkey] = n + 1
        key = (f"{s3io.prefix}/content_by_date/date={date}/region={region}/"
               f"part-{n:04d}.parquet")
        s3io.put_frame(key, df)

    for i, part_key in enumerate(part_keys):
        df = s3io.get_frame(part_key)
        total += df.height
        for (date, region), group in df.group_by("event_date", "region"):
            pkey = (date, region)
            buffers.setdefault(pkey, []).append(group)
            buffered_rows[pkey] = buffered_rows.get(pkey, 0) + group.height
            if buffered_rows[pkey] >= _FLUSH_ROWS:
                flush(pkey)
        if (i + 1) % 50 == 0:
            logger.info("compactor: %d/%d parts read", i + 1, len(part_keys))

    for pkey in list(buffers):
        flush(pkey)

    partitions = sum(part_counters.values())
    logger.info("compaction done: %d rows -> %d partition files", total, partitions)
    return {"rows": total, "partition_files": partitions}
