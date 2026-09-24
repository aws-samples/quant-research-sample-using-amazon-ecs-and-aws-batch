"""Build the backfill manifest — parallelized across the source run's S3
partitions.

Two levels of sharding, on different axes:

  manifest scan   one array worker per SOURCE partition (content/job=<src>/
                  shard=<i>/). Each reads only its partition, projects the few
                  metadata columns (never the document bytes), keeps
                  fetch_status != 'ok', classifies each row, drops 'skip', and
                  writes its survivors split BY SYMBOL into per-fetch-shard
                  fragments: backfill_manifest/job=<job>/fshard=<S>/src=<i>.parquet

  fetch           worker S later globs fshard=<S>/ across all src fragments,
                  so every event for one company (symbol-hash -> one fetch
                  shard) is handled by one worker, sequentially — we never hit
                  one host from two workers at once.

There is no separate merge step: the fan-out (by source partition) and the
fan-in (by symbol fetch-shard) are encoded in the fragment key layout.
"""

import logging
import time
from typing import Optional

import polars as pl

from backfill_strategies import STRATEGY_SKIP, classify
from config import Config
from s3_io import (
    S3Io,
    backfill_fetch_shard_prefix,
    backfill_part_key,
    manifest_summary_key,
)
from sharding import shard_for_symbol

logger = logging.getLogger(__name__)

_MANIFEST_COLS = ["event_id", "event_datetime_utc", "event_date", "region",
                  "url_pr", "factset_entity_id", "entity_proper_name",
                  "ticker_region", "fiscal_period", "fiscal_year", "fetch_status"]


def _storage_options(s3io: S3Io) -> dict:
    frozen = s3io.s3._request_signer._credentials.get_frozen_credentials()
    opts = {
        "aws_region": s3io.s3.meta.region_name or "us-east-1",
        "aws_access_key_id": frozen.access_key,
        "aws_secret_access_key": frozen.secret_key,
    }
    if frozen.token:
        opts["aws_session_token"] = frozen.token
    return opts


def load_filter_events(path_or_uri: str, boto3_session=None) -> set:
    """event_ids from a parquet (local path or s3://). Used to restrict a
    backfill to an analysis-chosen subset (e.g. panel-event coverage gaps)."""
    if path_or_uri.startswith("s3://"):
        import io
        import boto3
        bucket, key = path_or_uri[5:].split("/", 1)
        s3 = (boto3_session or boto3.Session()).client("s3")
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        df = pl.read_parquet(io.BytesIO(body), columns=["event_id"])
    else:
        df = pl.read_parquet(path_or_uri, columns=["event_id"])
    return set(df["event_id"].to_list())


def scan_source_shard(cfg: Config, source_job: str, src_shard: int,
                      boto3_session=None,
                      filter_events: Optional[set] = None) -> dict:
    """Manifest-scan worker for one source partition. Writes symbol-keyed
    fragments and returns per-fetch-shard + per-strategy counts."""
    s3io = S3Io(cfg.s3.bucket, cfg.s3.prefix, boto3_session=boto3_session)
    num_fetch_shards = cfg.manifest.num_shards

    # Column-pushdown scan of just this source partition.
    glob = (f"s3://{s3io.bucket}/{s3io.prefix}/content/job={source_job}/"
            f"shard={src_shard}/part-*.parquet")
    failed = (
        pl.scan_parquet(glob, storage_options=_storage_options(s3io))
        .select(_MANIFEST_COLS)
        .filter(pl.col("fetch_status") != "ok")
        .unique(subset=["event_id"], keep="first")
        .collect()
    )
    if filter_events is not None:
        failed = failed.filter(pl.col("event_id").is_in(list(filter_events)))
    if failed.height == 0:
        logger.info("src shard %d: no failures", src_shard)
        return {"src_shard": src_shard, "actionable": 0}

    strategy = [classify(r["region"], r["fetch_status"], r["url_pr"])
                for r in failed.iter_rows(named=True)]
    df = (
        failed.with_columns(pl.Series("strategy", strategy))
        .rename({"fetch_status": "original_fetch_status"})
        .filter(pl.col("strategy") != STRATEGY_SKIP)
        .with_columns(
            pl.col("factset_entity_id")
            .map_elements(lambda e: shard_for_symbol(e, num_fetch_shards),
                          return_dtype=pl.Int32)
            .alias("shard")  # 'shard' = the fetch shard this row belongs to
        )
    )

    fshard_counts, strat_counts = {}, {}
    for (fshard,), g in df.group_by("shard"):
        s3io.put_frame(backfill_part_key(s3io.prefix, cfg.job_name, src_shard, fshard), g)
        fshard_counts[int(fshard)] = g.height
    for (strat,), g in df.group_by("strategy"):
        strat_counts[strat] = g.height

    logger.info("src shard %d: %d actionable -> %d fetch shards",
                src_shard, df.height, len(fshard_counts))
    return {"src_shard": src_shard, "actionable": df.height,
            "fshard_counts": fshard_counts, "strategy_counts": strat_counts}


def run_scan(cfg: Config, source_job: str, src_shard: int, boto3_session=None,
             filter_events: Optional[set] = None) -> dict:
    return scan_source_shard(cfg, source_job, src_shard,
                             boto3_session=boto3_session,
                             filter_events=filter_events)


def load_fetch_shard(s3io: S3Io, job_name: str, fetch_shard: int) -> pl.DataFrame:
    """Fetch worker side: gather this fetch shard's rows from all source
    fragments (fan-in by symbol)."""
    prefix = backfill_fetch_shard_prefix(s3io.prefix, job_name, fetch_shard)
    keys = [k for k in s3io.list_keys(prefix) if k.endswith(".parquet")]
    if not keys:
        return pl.DataFrame()
    return pl.concat([s3io.get_frame(k) for k in keys])
