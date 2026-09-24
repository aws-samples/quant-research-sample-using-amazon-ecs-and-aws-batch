import boto3
import polars as pl
import pytest
from moto import mock_aws

from models import CONTENT_SCHEMA, MANIFEST_SCHEMA
from s3_io import S3Io, content_prefix, manifest_shard_key, manifest_summary_key

BUCKET = "test-bucket"
PREFIX = "earnings-content"


@pytest.fixture
def s3io():
    with mock_aws():
        session = boto3.Session(region_name="us-east-1")
        session.client("s3").create_bucket(Bucket=BUCKET)
        yield S3Io(BUCKET, PREFIX, boto3_session=session)


def _manifest_frame(event_ids):
    return pl.DataFrame(
        [
            {
                "event_id": e,
                "event_datetime_utc": "2026-01-15 21:00:00",
                "event_date": "2026-01-15",
                "region": "US",
                "url_pr": f"https://example.com/{e}.pdf",
                "factset_entity_id": "000XXX-E",
                "entity_proper_name": "Test Corp",
                "ticker_region": "TST-US",
                "fiscal_period": "4",
                "fiscal_year": 2025,
                "shard": 0,
            }
            for e in event_ids
        ],
        schema=MANIFEST_SCHEMA,
    )


def _content_frame(rows):
    """rows: list of (event_id, fetch_status)"""
    manifest = _manifest_frame([e for e, _ in rows]).to_dicts()
    out = []
    for m, (event_id, status) in zip(manifest, rows):
        m.update(
            final_url=m["url_pr"], fetch_status=status, http_status=200,
            content_type_header="application/pdf", sniffed_type="pdf",
            content_length=10, content_sha256="ab" * 32,
            raw_content=b"%PDF-fake!", text_content="hello",
            text_extract_status="ok", text_length=5, tls_insecure=False,
            fetched_at="2026-07-22T12:00:00Z", attempt=1, error_detail=None,
        )
        out.append(m)
    return pl.DataFrame(out, schema=CONTENT_SCHEMA)


class TestKeys:
    def test_manifest_key_zero_padded(self):
        assert manifest_shard_key(PREFIX, "job1", 7) == \
            f"{PREFIX}/manifest/job=job1/shard=0007.parquet"

    def test_content_prefix(self):
        assert content_prefix(PREFIX, "job1", 7) == \
            f"{PREFIX}/content/job=job1/shard=7/"


class TestRoundTrips:
    def test_frame_roundtrip_with_binary(self, s3io):
        df = _content_frame([(1, "ok"), (2, "http_404")])
        s3io.put_frame("k.parquet", df)
        back = s3io.get_frame("k.parquet")
        assert back.height == 2
        assert back.get_column("raw_content").to_list()[0] == b"%PDF-fake!"
        assert dict(back.schema)["raw_content"] == pl.Binary

    def test_json_roundtrip(self, s3io):
        s3io.put_json("s.json", {"a": 1})
        assert s3io.get_json("s.json") == {"a": 1}


class TestCheckpoints:
    def test_write_and_load_completed(self, s3io):
        s3io.write_content_part("job1", 0, _content_frame([(1, "ok"), (2, "timeout")]))
        s3io.write_content_part("job1", 0, _content_frame([(3, "http_404")]))
        done = s3io.load_completed_event_ids("job1", 0)
        assert done == {1, 2, 3}

    def test_retry_statuses_reincluded(self, s3io):
        s3io.write_content_part("job1", 0, _content_frame([(1, "ok"), (2, "timeout")]))
        done = s3io.load_completed_event_ids("job1", 0, retry_statuses=["timeout"])
        assert done == {1}

    def test_other_shard_not_visible(self, s3io):
        s3io.write_content_part("job1", 0, _content_frame([(1, "ok")]))
        assert s3io.load_completed_event_ids("job1", 1) == set()

    def test_done_marker(self, s3io):
        s3io.write_done_marker("job1", 0, {"ok": 5, "http_404": 2})
        marker = s3io.get_json(f"{content_prefix(PREFIX, 'job1', 0)}_done.json")
        assert marker["status_counts"] == {"ok": 5, "http_404": 2}
