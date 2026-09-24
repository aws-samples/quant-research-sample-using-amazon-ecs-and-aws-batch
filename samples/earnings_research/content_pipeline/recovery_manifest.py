"""Build a manifest from researched replacement URLs.

`backfill-scan` derives its work by scanning a source run for
`fetch_status != 'ok'` and re-trying the SAME url_pr with a different tactic
(headed browser, path rewrite, gentle retry). That cannot express the XLE
ticker-lineage outcome, where the stored URL is unrecoverable by any tactic and
research produced a DIFFERENT url to fetch. Two reasons it can't:

  - the replacement URL exists nowhere in the source run, and
  - 23 of the 115 XLE gaps are recorded as `fetch_status='ok'` (soft 404s),
    which the scan filters out before classification ever runs.

So recovery takes an explicit `event_id,url` CSV instead of a scan. Event
metadata (dates, fiscal period, entity ids) is carried over from the source
run's rows, keyed by event_id, so recovered rows land in the same schema and
join cleanly to everything downstream; only `url_pr` is replaced.

Output goes to a separate job_name, leaving already-extracted documents
untouched — recovery is additive, and coverage is recomputed by joining on
event_id across both jobs.
"""

import io
import logging
import time

import polars as pl

from config import Config
from models import MANIFEST_SCHEMA
from s3_io import S3Io, manifest_shard_key, manifest_summary_key
from sharding import shard_for_event

logger = logging.getLogger(__name__)

_REQUIRED_COLUMNS = ("event_id", "url")

# Metadata inherited from the source run. url_pr is deliberately absent: the
# replacement URL supersedes it.
_CARRY_COLUMNS = ["event_id", "event_datetime_utc", "event_date", "region",
                  "factset_entity_id", "entity_proper_name", "ticker_region",
                  "fiscal_period", "fiscal_year"]


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


def load_replacements(path_or_uri: str, boto3_session=None) -> pl.DataFrame:
    """Read the researched `event_id,url` pairs (CSV, local or s3://)."""
    if path_or_uri.startswith("s3://"):
        import boto3
        bucket, key = path_or_uri[5:].split("/", 1)
        s3 = (boto3_session or boto3.Session()).client("s3")
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        df = pl.read_csv(io.BytesIO(body))
    else:
        df = pl.read_csv(path_or_uri)

    missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"replacements CSV missing columns: {missing}")

    df = df.select([pl.col("event_id").cast(pl.Int64), pl.col("url")])

    blank = df.filter(pl.col("url").is_null() | (pl.col("url").str.strip_chars() == ""))
    if blank.height:
        raise ValueError(
            f"{blank.height} replacement rows have no URL "
            f"(event_ids: {blank['event_id'].to_list()[:5]})")

    dupes = df.group_by("event_id").len().filter(pl.col("len") > 1)
    if dupes.height:
        raise ValueError(
            f"duplicate event_ids in replacements: {dupes['event_id'].to_list()[:5]}")
    return df


def build_recovery_frame(cfg: Config, source_job: str, replacements: pl.DataFrame,
                         boto3_session=None) -> pl.DataFrame:
    """Join replacement URLs onto source-run event metadata, reshard, and
    return a frame in MANIFEST_SCHEMA."""
    s3io = S3Io(cfg.s3.bucket, cfg.s3.prefix, boto3_session=boto3_session)
    glob = (f"s3://{s3io.bucket}/{s3io.prefix}/content/job={source_job}/"
            f"**/*.parquet")
    meta = (
        pl.scan_parquet(glob, storage_options=_storage_options(s3io))
        .select(_CARRY_COLUMNS)
        .filter(pl.col("event_id").is_in(replacements["event_id"].to_list()))
        .unique(subset=["event_id"], keep="first")
        .collect()
    )

    df = replacements.join(meta, on="event_id", how="left")

    # An event_id absent from the source run would silently carry null dates and
    # region, poisoning the partition layout — refuse instead.
    orphans = df.filter(pl.col("event_date").is_null())
    if orphans.height:
        raise ValueError(
            f"{orphans.height} replacement event_ids not found in job={source_job} "
            f"(first few: {orphans['event_id'].to_list()[:5]})")

    num_shards = cfg.manifest.num_shards
    df = df.rename({"url": "url_pr"}).with_columns(
        pl.col("event_id")
          .map_elements(lambda e: shard_for_event(e, num_shards),
                        return_dtype=pl.Int32)
          .alias("shard"),
    ).select(list(MANIFEST_SCHEMA))

    logger.info("recovery manifest: %d events from job=%s", df.height, source_job)
    return df


def write_manifest(df: pl.DataFrame, s3io: S3Io, cfg: Config,
                   source_job: str) -> dict:
    shard_counts = {}
    for (shard,), shard_df in df.group_by("shard"):
        s3io.put_frame(manifest_shard_key(s3io.prefix, cfg.job_name, shard), shard_df)
        shard_counts[int(shard)] = shard_df.height

    summary = {
        "job_name": cfg.job_name,
        "num_shards": cfg.manifest.num_shards,
        "total_events": df.height,
        "shard_counts": {str(k): v for k, v in sorted(shard_counts.items())},
        "row_limit": None,
        "source_job": source_job,
        "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    s3io.put_json(manifest_summary_key(s3io.prefix, cfg.job_name), summary)
    logger.info("recovery manifest written: %d events across %d shard files",
                df.height, len(shard_counts))
    return summary


def run(cfg: Config, source_job: str, replacements_path: str,
        boto3_session=None) -> dict:
    replacements = load_replacements(replacements_path, boto3_session=boto3_session)
    s3io = S3Io(cfg.s3.bucket, cfg.s3.prefix, boto3_session=boto3_session)
    df = build_recovery_frame(cfg, source_job, replacements,
                              boto3_session=boto3_session)
    return write_manifest(df, s3io, cfg, source_job)
