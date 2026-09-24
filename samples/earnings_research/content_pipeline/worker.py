"""Phase B: per-shard fetch worker.

Loads its manifest shard into a Polars DataFrame, filters out already-done
events (resume), fetches every URL with bounded concurrency + per-domain
politeness, extracts text, and periodically flushes completed rows —
manifest columns extended with fetch/content columns (raw_content bytes,
text_content) — as append-only parquet parts.
"""

import asyncio
import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import aiohttp
import polars as pl

from config import Config
from content_type import sniff_type
from domain_rate_limiter import DomainRateLimiterPool, registrable_domain
from extractor import extract_text
from fetcher import Fetcher
from models import (
    CONTENT_SCHEMA,
    EXTRACT_NOT_ATTEMPTED,
    FETCH_OK,
    FETCH_SOFT_404,
)
from s3_io import S3Io, manifest_shard_key, manifest_summary_key
from soft_404 import describe as describe_soft_404
from soft_404 import looks_collapsed

logger = logging.getLogger(__name__)


def _interleave_by_domain(rows: List[dict]) -> List[dict]:
    """Round-robin rows across domains so per-domain limits don't serialize
    the tail (manifests are heavily concentrated on a few IR-hosting CDNs)."""
    by_domain: Dict[str, List[dict]] = {}
    for row in rows:
        by_domain.setdefault(registrable_domain(row["url_pr"]), []).append(row)
    queues = list(by_domain.values())
    out = []
    while queues:
        next_queues = []
        for q in queues:
            out.append(q.pop(0))
            if q:
                next_queues.append(q)
        queues = next_queues
    return out


class ShardWorker:
    def __init__(self, cfg: Config, shard: int, boto3_session=None):
        self.cfg = cfg
        self.shard = shard
        self.s3io = S3Io(cfg.s3.bucket, cfg.s3.prefix, boto3_session=boto3_session)
        self._buffer: List[dict] = []
        self._buffer_bytes = 0
        self._buffer_lock = asyncio.Lock()
        self._last_flush = time.monotonic()
        self._status_counts: Dict[str, int] = {}
        self._extract_pool = ThreadPoolExecutor(
            max_workers=cfg.fetch.extract_threads, thread_name_prefix="extract"
        )

    # ------------------------------------------------------------------ setup

    def load_pending(self) -> List[dict]:
        summary = self.s3io.get_json(
            manifest_summary_key(self.s3io.prefix, self.cfg.job_name))
        if summary["num_shards"] != self.cfg.manifest.num_shards:
            raise RuntimeError(
                f"num_shards mismatch: config={self.cfg.manifest.num_shards}, "
                f"manifest={summary['num_shards']}")

        df = self.s3io.get_frame(
            manifest_shard_key(self.s3io.prefix, self.cfg.job_name, self.shard))
        done = self.s3io.load_completed_event_ids(
            self.cfg.job_name, self.shard,
            retry_statuses=self.cfg.fetch.retry_statuses_on_resume)
        pending = df.filter(~pl.col("event_id").is_in(list(done)))
        logger.info("shard %d: %d total, %d done, %d pending",
                    self.shard, df.height, df.height - pending.height,
                    pending.height)
        return _interleave_by_domain(pending.to_dicts())

    # ------------------------------------------------------------- processing

    async def process_row(self, fetcher: Fetcher, row: dict):
        result = await fetcher.fetch_one(row["url_pr"])

        # A 200 from a collapsed redirect is a dead document wearing a success:
        # recording it as `ok` is what let 23 XLE gaps hide in the success
        # bucket, invisible to the failure-driven backfill scan.
        status = result.fetch_status
        error_detail = result.error_detail
        if status == FETCH_OK and looks_collapsed(row["url_pr"], result.final_url):
            status = FETCH_SOFT_404
            error_detail = describe_soft_404(row["url_pr"], result.final_url)

        out = dict(row)
        out.update(
            final_url=result.final_url,
            fetch_status=status,
            http_status=result.http_status,
            content_type_header=result.content_type_header,
            sniffed_type=None,
            content_length=None,
            content_sha256=None,
            raw_content=None,
            text_content=None,
            text_extract_status=EXTRACT_NOT_ATTEMPTED,
            text_length=None,
            tls_insecure=result.tls_insecure,
            fetched_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            attempt=result.attempt,
            error_detail=error_detail,
        )

        # Extraction is gated on the adjusted status, so a soft 404's landing-page
        # chrome is never stored as document text.
        if status == FETCH_OK and result.body:
            body = result.body
            sniffed = sniff_type(body, result.content_type_header)
            loop = asyncio.get_running_loop()
            text, extract_status = await loop.run_in_executor(
                self._extract_pool, extract_text, body, sniffed)
            out.update(
                sniffed_type=sniffed,
                content_length=len(body),
                content_sha256=hashlib.sha256(body).hexdigest(),
                raw_content=body,
                text_content=text,
                text_extract_status=extract_status,
                text_length=len(text) if text else None,
            )

        self._status_counts[out["fetch_status"]] = \
            self._status_counts.get(out["fetch_status"], 0) + 1

        async with self._buffer_lock:
            self._buffer.append(out)
            self._buffer_bytes += (out["content_length"] or 0)
            if (len(self._buffer) >= self.cfg.fetch.checkpoint_every_rows or
                    self._buffer_bytes >= self.cfg.fetch.checkpoint_every_bytes or
                    time.monotonic() - self._last_flush >= self.cfg.fetch.checkpoint_every_s):
                self._flush_locked()

    def _flush_locked(self):
        if not self._buffer:
            return
        df = pl.DataFrame(self._buffer, schema=CONTENT_SCHEMA)
        self.s3io.write_content_part(self.cfg.job_name, self.shard, df)
        self._buffer = []
        self._buffer_bytes = 0
        self._last_flush = time.monotonic()

    # -------------------------------------------------------------------- run

    async def run(self) -> Dict[str, int]:
        pending = self.load_pending()
        if not pending:
            logger.info("shard %d: nothing to do", self.shard)
            self.s3io.write_done_marker(self.cfg.job_name, self.shard,
                                        self._status_counts)
            return self._status_counts

        limiter = DomainRateLimiterPool(
            rate_per_second=self.cfg.fetch.per_domain_rps,
            burst=self.cfg.fetch.per_domain_burst,
            cooldown_seconds=self.cfg.fetch.domain_cooldown_s,
        )
        semaphore = asyncio.Semaphore(self.cfg.fetch.worker_concurrency)
        connector = aiohttp.TCPConnector(
            limit=self.cfg.fetch.worker_concurrency, force_close=False)

        async with aiohttp.ClientSession(connector=connector) as session:
            fetcher = Fetcher(self.cfg.fetch, limiter, session)

            async def bounded(row):
                async with semaphore:
                    try:
                        await self.process_row(fetcher, row)
                    except Exception as e:
                        # process_row shouldn't raise, but a bug in it must
                        # not kill the shard silently
                        logger.error("event %s: unexpected error %r",
                                     row.get("event_id"), e)

            total = len(pending)
            tasks = [asyncio.ensure_future(bounded(r)) for r in pending]
            done_count = 0
            for fut in asyncio.as_completed(tasks):
                await fut
                done_count += 1
                if done_count % 500 == 0:
                    logger.info("shard %d: %d/%d processed",
                                self.shard, done_count, total)

        async with self._buffer_lock:
            self._flush_locked()
        self._extract_pool.shutdown(wait=False)
        self.s3io.write_done_marker(self.cfg.job_name, self.shard,
                                    self._status_counts)
        logger.info("shard %d complete: %s", self.shard, self._status_counts)
        return self._status_counts


def run(cfg: Config, shard: int, boto3_session=None) -> Dict[str, int]:
    worker = ShardWorker(cfg, shard, boto3_session=boto3_session)
    return asyncio.run(worker.run())
