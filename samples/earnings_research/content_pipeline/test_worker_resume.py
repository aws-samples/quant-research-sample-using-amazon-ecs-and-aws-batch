"""Worker resume + end-to-end shard processing against moto S3 and a local
aiohttp test server."""

import boto3
import polars as pl
import pytest
from aiohttp import web
from moto import mock_aws

from config import Config
from models import CONTENT_SCHEMA, MANIFEST_SCHEMA
from s3_io import S3Io, manifest_shard_key, manifest_summary_key
from worker import ShardWorker, _interleave_by_domain

BUCKET = "test-bucket"


def _config(**fetch_overrides) -> Config:
    data = {
        "job_name": "job1",
        "aws": {"region": "us-east-1"},
        "s3": {"bucket": BUCKET, "prefix": "earnings-content"},
        "manifest": {"num_shards": 2},
        "fetch": {
            "worker_concurrency": 4, "per_domain_rps": 1000.0,
            "per_domain_burst": 1000, "connect_timeout_s": 2,
            "total_timeout_s": 2, "max_attempts": 1, "backoff_base_s": 0.01,
            "checkpoint_every_rows": 2, "checkpoint_every_s": 9999,
            "extract_threads": 1, **fetch_overrides,
        },
    }
    return Config.from_dict(data)


def _manifest_rows(server_url, event_ids, shard=0):
    return [
        {
            "event_id": e,
            "event_datetime_utc": "2026-01-15 21:00:00",
            "event_date": "2026-01-15",
            "region": "US",
            "url_pr": f"{server_url}/doc/{e}",
            "factset_entity_id": "000XXX-E",
            "entity_proper_name": "Test Corp",
            "ticker_region": "TST-US",
            "fiscal_period": "4",
            "fiscal_year": 2025,
            "shard": shard,
        }
        for e in event_ids
    ]


@pytest.fixture
async def server(aiohttp_server):
    async def doc(request):
        event = request.match_info["event"]
        if event == "13":
            return web.Response(status=404)
        return web.Response(
            body=f"<html><body><p>Earnings for event {event}: revenue "
                 f"grew strongly this quarter.</p></body></html>".encode(),
            content_type="text/html")

    app = web.Application()
    app.router.add_get("/doc/{event}", doc)
    return await aiohttp_server(app)


@pytest.fixture
def aws(monkeypatch):
    with mock_aws():
        session = boto3.Session(region_name="us-east-1")
        session.client("s3").create_bucket(Bucket=BUCKET)
        yield session


def _seed_manifest(session, cfg, rows):
    s3io = S3Io(cfg.s3.bucket, cfg.s3.prefix, boto3_session=session)
    df = pl.DataFrame(rows, schema=MANIFEST_SCHEMA)
    s3io.put_frame(manifest_shard_key(s3io.prefix, cfg.job_name, 0), df)
    s3io.put_json(manifest_summary_key(s3io.prefix, cfg.job_name), {
        "job_name": cfg.job_name, "num_shards": cfg.manifest.num_shards,
        "total_events": len(rows),
    })
    return s3io


class TestInterleave:
    def test_round_robin(self):
        rows = ([{"url_pr": "https://a.com/1"}] * 3 +
                [{"url_pr": "https://b.com/1"}] * 2)
        order = [r["url_pr"] for r in _interleave_by_domain(list(rows))]
        assert order[:2] == ["https://a.com/1", "https://b.com/1"]
        assert order[2:4] == ["https://a.com/1", "https://b.com/1"]


class TestShardProcessing:
    @pytest.mark.asyncio
    async def test_full_shard_run(self, aws, server):
        cfg = _config()
        rows = _manifest_rows(str(server.make_url("")).rstrip("/"), [11, 12, 13])
        s3io = _seed_manifest(aws, cfg, rows)

        worker = ShardWorker(cfg, shard=0, boto3_session=aws)
        counts = await worker.run()

        assert counts == {"ok": 2, "http_404": 1}
        parts = [k for k in s3io.list_keys(f"{s3io.prefix}/content/job=job1/shard=0/")
                 if k.endswith(".parquet")]
        df = pl.concat([s3io.get_frame(k) for k in parts])
        assert df.height == 3
        ok = df.filter(pl.col("fetch_status") == "ok")
        assert all("revenue" in t for t in ok.get_column("text_content").to_list())
        assert all(b is not None for b in ok.get_column("raw_content").to_list())
        failed = df.filter(pl.col("fetch_status") == "http_404")
        assert failed.get_column("raw_content").to_list() == [None]

    @pytest.mark.asyncio
    async def test_resume_skips_done(self, aws, server):
        cfg = _config()
        rows = _manifest_rows(str(server.make_url("")).rstrip("/"), [11, 12, 13])
        _seed_manifest(aws, cfg, rows)

        first = ShardWorker(cfg, shard=0, boto3_session=aws)
        await first.run()

        second = ShardWorker(cfg, shard=0, boto3_session=aws)
        counts = await second.run()
        assert counts == {}  # nothing pending

    @pytest.mark.asyncio
    async def test_resume_retries_selected_statuses(self, aws, server):
        cfg = _config()
        rows = _manifest_rows(str(server.make_url("")).rstrip("/"), [11, 13])
        _seed_manifest(aws, cfg, rows)

        await ShardWorker(cfg, shard=0, boto3_session=aws).run()

        cfg.fetch.retry_statuses_on_resume = ["http_404"]
        counts = await ShardWorker(cfg, shard=0, boto3_session=aws).run()
        assert counts == {"http_404": 1}  # only the 404 was retried

    @pytest.mark.asyncio
    async def test_num_shards_mismatch_fails_fast(self, aws, server):
        cfg = _config()
        rows = _manifest_rows(str(server.make_url("")).rstrip("/"), [11])
        _seed_manifest(aws, cfg, rows)

        cfg.manifest.num_shards = 99
        with pytest.raises(RuntimeError, match="num_shards mismatch"):
            ShardWorker(cfg, shard=0, boto3_session=aws).load_pending()
