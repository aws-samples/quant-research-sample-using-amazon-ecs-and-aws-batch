"""Earnings content pipeline CLI.

Subcommands:
  build-manifest   Query Redshift, write per-shard manifest parquets to S3
  fetch            Process one shard (or AWS_BATCH_JOB_ARRAY_INDEX)
  compact-content  Repartition content parts into date/region hive layout
  backfill-scan    Scan ONE source partition (--shard = source shard), classify
                   failures, write symbol-keyed manifest fragments (array job)
  backfill-fetch   Process one fetch shard (--shard); gathers a company's events
                   from all source fragments and fetches them sequentially
  build-recovery-manifest
                   Build a manifest from researched replacement URLs
                   (--replacements event_id,url CSV; --source-job supplies the
                   event metadata). Fetch it afterwards with plain 'fetch'.
"""

import argparse
import json
import os
import sys

import boto3

from config import load_config
from logging_config import configure_logging


def _session(cfg):
    if cfg.aws.profile:
        return boto3.Session(profile_name=cfg.aws.profile, region_name=cfg.aws.region)
    return boto3.Session(region_name=cfg.aws.region)


def main():
    parser = argparse.ArgumentParser(description="Earnings content pipeline")
    parser.add_argument("command",
                        choices=["build-manifest", "fetch", "compact-content",
                                 "backfill-scan", "backfill-fetch",
                                 "build-recovery-manifest"])
    parser.add_argument("--config", required=True,
                        help="config path (local file or s3://)")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--shard", type=int, default=None,
                        help="shard index (fetch); overrides AWS_BATCH_JOB_ARRAY_INDEX")
    parser.add_argument("--num-shards", type=int, default=None,
                        help="override manifest.num_shards from config")
    parser.add_argument("--source-job", default=None,
                        help="source job_name to backfill from (build-backfill-manifest)")
    parser.add_argument("--filter-events", default=None,
                        help="parquet (local or s3://) with an event_id column; "
                             "backfill-scan keeps only these events")
    parser.add_argument("--replacements", default=None,
                        help="CSV (local or s3://) with event_id,url columns; "
                             "researched replacement URLs for "
                             "build-recovery-manifest")
    args = parser.parse_args()

    cfg = load_config(args.config)
    if args.num_shards is not None:
        cfg.manifest.num_shards = args.num_shards

    shard = args.shard
    if shard is None and "AWS_BATCH_JOB_ARRAY_INDEX" in os.environ:
        shard = int(os.environ["AWS_BATCH_JOB_ARRAY_INDEX"])

    shard_cmds = ("fetch", "backfill-fetch", "backfill-scan")
    configure_logging(args.log_level, shard=shard if args.command in shard_cmds else None)
    session = _session(cfg)

    if args.command == "build-manifest":
        import manifest_builder

        summary = manifest_builder.run(cfg, boto3_session=session)
        print(json.dumps({k: v for k, v in summary.items() if k != "shard_counts"},
                         indent=2))
    elif args.command == "fetch":
        if shard is None:
            parser.error("fetch requires --shard or AWS_BATCH_JOB_ARRAY_INDEX")
        import worker

        counts = worker.run(cfg, shard, boto3_session=session)
        print(json.dumps({"shard": shard, "status_counts": counts}, indent=2))
    elif args.command == "compact-content":
        import compactor

        result = compactor.run(cfg, boto3_session=session)
        print(json.dumps(result, indent=2))
    elif args.command == "backfill-scan":
        if not args.source_job:
            parser.error("backfill-scan requires --source-job")
        if shard is None:
            parser.error("backfill-scan requires --shard or AWS_BATCH_JOB_ARRAY_INDEX")
        import backfill_manifest

        filter_events = None
        if args.filter_events:
            filter_events = backfill_manifest.load_filter_events(
                args.filter_events, boto3_session=session)
        result = backfill_manifest.run_scan(cfg, args.source_job, shard,
                                            boto3_session=session,
                                            filter_events=filter_events)
        print(json.dumps(result, indent=2))
    elif args.command == "build-recovery-manifest":
        if not args.source_job:
            parser.error("build-recovery-manifest requires --source-job")
        if not args.replacements:
            parser.error("build-recovery-manifest requires --replacements")
        import recovery_manifest

        summary = recovery_manifest.run(cfg, args.source_job, args.replacements,
                                        boto3_session=session)
        print(json.dumps({k: v for k, v in summary.items()
                          if k != "shard_counts"}, indent=2))
    elif args.command == "backfill-fetch":
        if shard is None:
            parser.error("backfill-fetch requires --shard or AWS_BATCH_JOB_ARRAY_INDEX")
        import backfill_worker

        counts = backfill_worker.run(cfg, shard, boto3_session=session)
        print(json.dumps({"shard": shard, "status_counts": counts}, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
