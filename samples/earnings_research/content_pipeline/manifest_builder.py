"""Phase A: build the work manifest.

Runs the ER-events SQL against Redshift, initializes a Polars DataFrame from
the result, derives partition/shard columns, and writes one parquet per shard
to S3. Workers later extend these rows with content columns.
"""

import logging
import time
from typing import Optional

import polars as pl

from config import Config
from models import MANIFEST_SCHEMA
from redshift_client import RedshiftClient
from s3_io import S3Io, manifest_shard_key, manifest_summary_key
from sharding import event_date_from_datetime, region_from_ticker, shard_for_event

logger = logging.getLogger(__name__)

# One row per ER event with a URL. The symbology hop is mandatory:
# ce_sec_entity.fsym_id is security-level; sym_ticker_region keys on
# regional-level ids — joining them directly returns 0 rows. The hop goes
# through sym_coverage.fsym_regional_id.
#
# The joins fan out ~6x (multi-entity events, multi-listing securities), so
# dedup happens IN SQL via row_number(): one row per event_id, preferring a
# mapped ticker (nulls last). Deduplicating client-side OOMs the container —
# the raw join is ~6.4M rows.
MANIFEST_SQL = """
with candidates as (
    select
        e.event_id,
        e.event_datetime_utc::varchar as event_datetime_utc,
        e.url_pr,
        ec.factset_entity_id,
        ent.entity_proper_name,
        tr.ticker_region,
        e.fiscal_period,
        e.fiscal_year,
        row_number() over (
            partition by e.event_id
            order by tr.ticker_region asc nulls last
        ) as rn
    from factset_ce_events.evt_v1.ce_events e
    join factset_ce_events.evt_v1.ce_events_coverage ec
        on ec.event_id = e.event_id
    left join factset_ce_events.sym_v1.sym_entity ent
        on ent.factset_entity_id = ec.factset_entity_id
    left join factset_ce_events.evt_v1.ce_sec_entity cse
        on cse.factset_entity_id = ec.factset_entity_id
    left join factset_ce_events.sym_v1.sym_coverage sc
        on sc.fsym_id = cse.fsym_id
    left join factset_ce_events.sym_v1.sym_ticker_region tr
        on tr.fsym_id = sc.fsym_regional_id
    where e.event_type = 'ER'
      and e.url_pr is not null
      and e.url_pr <> ''
)
select event_id, event_datetime_utc, url_pr, factset_entity_id,
       entity_proper_name, ticker_region, fiscal_period, fiscal_year
from candidates
where rn = 1
"""


def build_manifest_frame(client: RedshiftClient, cfg: Config) -> pl.DataFrame:
    """Run the SQL and return the deduplicated, shard-annotated manifest frame."""
    sql = MANIFEST_SQL
    if cfg.redshift.row_limit:
        sql += f" limit {int(cfg.redshift.row_limit)}"

    def progress(n):
        logger.info("manifest query: %d rows fetched so far", n)

    rows = list(client.fetch_all(sql, timeout_s=cfg.redshift.query_timeout_s,
                                 progress_cb=progress))
    if not rows:
        raise RuntimeError("manifest query returned no rows")
    logger.info("manifest query complete: %d raw rows", len(rows))

    df = pl.DataFrame(rows, infer_schema_length=None)

    # The joins fan out (multi-entity events, multi-listing securities):
    # keep one row per event_id, preferring rows with a ticker mapping.
    df = (
        df.sort("ticker_region", descending=False, nulls_last=True)
        .unique(subset=["event_id"], keep="first")
    )

    num_shards = cfg.manifest.num_shards
    df = df.with_columns(
        pl.col("event_id").cast(pl.Int64),
        pl.col("fiscal_year").cast(pl.Int64),
        pl.col("event_datetime_utc")
          .map_elements(event_date_from_datetime, return_dtype=pl.Utf8)
          .alias("event_date"),
        pl.col("ticker_region")
          .map_elements(region_from_ticker, return_dtype=pl.Utf8,
                        skip_nulls=False)
          .alias("region"),
        pl.col("event_id")
          .map_elements(lambda e: shard_for_event(e, num_shards),
                        return_dtype=pl.Int32)
          .alias("shard"),
    ).select(list(MANIFEST_SCHEMA))

    logger.info("manifest frame: %d events, %d shards", df.height, num_shards)
    return df


def write_manifest(df: pl.DataFrame, s3io: S3Io, cfg: Config) -> dict:
    """Write per-shard parquets + summary; returns the summary dict."""
    job = cfg.job_name
    shard_counts = {}
    for (shard,), shard_df in df.group_by("shard"):
        key = manifest_shard_key(s3io.prefix, job, shard)
        s3io.put_frame(key, shard_df)
        shard_counts[int(shard)] = shard_df.height

    summary = {
        "job_name": job,
        "num_shards": cfg.manifest.num_shards,
        "total_events": df.height,
        "shard_counts": {str(k): v for k, v in sorted(shard_counts.items())},
        "row_limit": cfg.redshift.row_limit,
        "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    s3io.put_json(manifest_summary_key(s3io.prefix, job), summary)
    logger.info("manifest written: %d events across %d shard files",
                df.height, len(shard_counts))
    return summary


def run(cfg: Config, boto3_session=None) -> dict:
    client = RedshiftClient(
        workgroup=cfg.redshift.workgroup,
        database=cfg.redshift.database,
        secret_arn=cfg.redshift.secret_arn,
        region=cfg.aws.region,
        boto3_session=boto3_session,
    )
    s3io = S3Io(cfg.s3.bucket, cfg.s3.prefix, boto3_session=boto3_session)
    df = build_manifest_frame(client, cfg)
    return write_manifest(df, s3io, cfg)
