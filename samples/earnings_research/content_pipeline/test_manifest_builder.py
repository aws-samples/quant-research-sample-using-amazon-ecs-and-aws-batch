from unittest.mock import MagicMock

import boto3
import polars as pl
import pytest
from moto import mock_aws

from config import Config
from manifest_builder import build_manifest_frame, write_manifest
from models import MANIFEST_SCHEMA
from s3_io import S3Io, manifest_shard_key, manifest_summary_key
from sharding import shard_for_event

BUCKET = "test-bucket"


def _config(num_shards=4, row_limit=None) -> Config:
    return Config.from_dict({
        "job_name": "job1",
        "s3": {"bucket": BUCKET, "prefix": "earnings-content"},
        "redshift": {"secret_arn": "arn:test", "row_limit": row_limit},
        "manifest": {"num_shards": num_shards},
    })


def _sql_rows():
    # event 2 fans out to two tickers (multi-listing); event 3 has no ticker
    return [
        {"event_id": 1, "event_datetime_utc": "2025-02-12 21:31:00",
         "url_pr": "https://a.com/1.pdf", "factset_entity_id": "E1",
         "entity_proper_name": "Alpha", "ticker_region": "ALP-US",
         "fiscal_period": "2", "fiscal_year": 2025},
        {"event_id": 2, "event_datetime_utc": "2025-03-01 10:00:00",
         "url_pr": "https://b.com/2.pdf", "factset_entity_id": "E2",
         "entity_proper_name": "Beta", "ticker_region": "BET-US",
         "fiscal_period": "1", "fiscal_year": 2025},
        {"event_id": 2, "event_datetime_utc": "2025-03-01 10:00:00",
         "url_pr": "https://b.com/2.pdf", "factset_entity_id": "E2",
         "entity_proper_name": "Beta", "ticker_region": "BET34-BR",
         "fiscal_period": "1", "fiscal_year": 2025},
        {"event_id": 3, "event_datetime_utc": "2025-04-01 00:00:00",
         "url_pr": "https://c.com/3.htm", "factset_entity_id": "E3",
         "entity_proper_name": "Gamma", "ticker_region": None,
         "fiscal_period": "3", "fiscal_year": 2025},
    ]


def _client_returning(rows):
    client = MagicMock()
    client.fetch_all.return_value = iter(rows)
    return client


class TestBuildManifestFrame:
    def test_dedup_and_derived_columns(self):
        df = build_manifest_frame(_client_returning(_sql_rows()), _config())

        assert df.height == 3  # event 2 deduped
        assert set(df.columns) == set(MANIFEST_SCHEMA)

        by_id = {r["event_id"]: r for r in df.to_dicts()}
        assert by_id[1]["event_date"] == "2025-02-12"
        assert by_id[1]["region"] == "US"
        # dedup keeps a mapped ticker, and it's deterministic (sorted first)
        assert by_id[2]["ticker_region"] == "BET-US"
        assert by_id[3]["region"] == "UNKNOWN"

        for r in df.to_dicts():
            assert r["shard"] == shard_for_event(r["event_id"], 4)

    def test_row_limit_appended_to_sql(self):
        client = _client_returning(_sql_rows())
        build_manifest_frame(client, _config(row_limit=200))
        sql = client.fetch_all.call_args[0][0]
        assert sql.strip().endswith("limit 200")

    def test_empty_result_raises(self):
        with pytest.raises(RuntimeError, match="no rows"):
            build_manifest_frame(_client_returning([]), _config())


class TestWriteManifest:
    def test_shard_files_and_summary(self):
        with mock_aws():
            session = boto3.Session(region_name="us-east-1")
            session.client("s3").create_bucket(Bucket=BUCKET)
            s3io = S3Io(BUCKET, "earnings-content", boto3_session=session)

            cfg = _config()
            df = build_manifest_frame(_client_returning(_sql_rows()), cfg)
            summary = write_manifest(df, s3io, cfg)

            assert summary["total_events"] == 3
            assert summary["num_shards"] == 4
            assert sum(summary["shard_counts"].values()) == 3

            stored = s3io.get_json(manifest_summary_key(s3io.prefix, "job1"))
            assert stored["total_events"] == 3

            for shard_str, count in summary["shard_counts"].items():
                shard_df = s3io.get_frame(
                    manifest_shard_key(s3io.prefix, "job1", int(shard_str)))
                assert shard_df.height == count
                assert (shard_df.get_column("shard") == int(shard_str)).all()
