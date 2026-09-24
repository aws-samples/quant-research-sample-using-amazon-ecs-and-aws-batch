"""Per-shard backfill worker.

Reads a backfill manifest shard, resumes past completed events, and routes
each row by its 'strategy' column:
  browser  headed Chromium fetch (bot-walled hosts)
  rewrite  plain aiohttp fetch of the /AttachHis/ URL
  retry    plain aiohttp fetch of the original URL, gentle settings

Writes CONTENT_SCHEMA rows (+ raw_content/text_content) to
content/job=<backfill_job>/shard=<i>/, reusing the same checkpoint/resume
machinery as the primary worker. Browser fetches are serial per browser and
slow by design — completion over speed.
"""

import asyncio
import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

import aiohttp
import polars as pl

from backfill_strategies import (
    STRATEGY_BROWSER,
    STRATEGY_RETRY,
    STRATEGY_REWRITE,
    rewrite_url,
)
from browser_fetcher import BrowserFetcher
from config import Config

# Recycle the Chromium process after this many browser-strategy fetches:
# relaunch costs ~1-2s; V8's ~4GB per-process heap cap is unreachable from
# container sizing, so bounded page counts are the only real heap bound.
BROWSER_RECYCLE_PAGES = 25
from content_type import sniff_type
from domain_rate_limiter import DomainRateLimiterPool
from extractor import extract_text
from fetcher import Fetcher
from backfill_manifest import load_fetch_shard
from models import CONTENT_SCHEMA, EXTRACT_NOT_ATTEMPTED, FETCH_OK
from s3_io import S3Io

logger = logging.getLogger(__name__)


class BackfillWorker:
    def __init__(self, cfg: Config, shard: int, boto3_session=None):
        self.cfg = cfg
        self.shard = shard
        self.s3io = S3Io(cfg.s3.bucket, cfg.s3.prefix, boto3_session=boto3_session)
        self._buffer: List[dict] = []
        self._buffer_bytes = 0
        self._status_counts: Dict[str, int] = {}
        self._extract_pool = ThreadPoolExecutor(
            max_workers=cfg.fetch.extract_threads, thread_name_prefix="extract")

    # ------------------------------------------------------------------ setup

    def load_pending(self) -> List[dict]:
        # Fan-in: gather this fetch shard's rows from every source fragment.
        df = load_fetch_shard(self.s3io, self.cfg.job_name, self.shard)
        if df.height == 0:
            logger.info("shard %d: empty manifest", self.shard)
            return []
        done = self.s3io.load_completed_event_ids(
            self.cfg.job_name, self.shard,
            retry_statuses=self.cfg.fetch.retry_statuses_on_resume)
        pending = df.filter(~pl.col("event_id").is_in(list(done)))
        logger.info("shard %d: %d total, %d done, %d pending",
                    self.shard, df.height, df.height - pending.height, pending.height)
        return pending.to_dicts()

    # ------------------------------------------------------------ result row

    def _base_row(self, manifest_row: dict) -> dict:
        # Manifest carries an extra 'strategy'/'original_fetch_status'; keep
        # only CONTENT_SCHEMA fields plus the ones we fill in.
        out = {k: manifest_row.get(k) for k in
               ("event_id", "event_datetime_utc", "event_date", "region",
                "url_pr", "factset_entity_id", "entity_proper_name",
                "ticker_region", "fiscal_period", "fiscal_year")}
        out["shard"] = self.shard
        out.update(final_url=None, fetch_status=None, http_status=None,
                   content_type_header=None, sniffed_type=None,
                   content_length=None, content_sha256=None, raw_content=None,
                   text_content=None, text_extract_status=EXTRACT_NOT_ATTEMPTED,
                   text_length=None, tls_insecure=False,
                   fetched_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   attempt=1, error_detail=None)
        return out

    def _store_body(self, out: dict, body: bytes, content_type: str):
        sniffed = sniff_type(body, content_type)
        text, extract_status = extract_text(body, sniffed)
        out.update(
            sniffed_type=sniffed, content_length=len(body),
            content_sha256=hashlib.sha256(body).hexdigest(),
            raw_content=body, text_content=text,
            text_extract_status=extract_status,
            text_length=len(text) if text else None,
        )

    async def _buffer_row(self, out: dict):
        self._status_counts[out["fetch_status"]] = \
            self._status_counts.get(out["fetch_status"], 0) + 1
        self._buffer.append(out)
        self._buffer_bytes += (out["content_length"] or 0)
        if (len(self._buffer) >= self.cfg.fetch.checkpoint_every_rows or
                self._buffer_bytes >= self.cfg.fetch.checkpoint_every_bytes):
            self._flush()

    def _flush(self):
        if not self._buffer:
            return
        df = pl.DataFrame(self._buffer, schema=CONTENT_SCHEMA)
        self.s3io.write_content_part(self.cfg.job_name, self.shard, df)
        self._buffer, self._buffer_bytes = [], 0

    # ------------------------------------------------------------- strategies

    async def _do_http(self, fetcher: Fetcher, row: dict, url: str):
        out = self._base_row(row)
        result = await fetcher.fetch_one(url)
        out.update(final_url=result.final_url, fetch_status=result.fetch_status,
                   http_status=result.http_status,
                   content_type_header=result.content_type_header,
                   tls_insecure=result.tls_insecure, attempt=result.attempt,
                   error_detail=result.error_detail)
        if result.fetch_status == FETCH_OK and result.body:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                self._extract_pool, self._store_body, out,
                result.body, result.content_type_header)
        await self._buffer_row(out)

    async def _launch_stack(self):
        """Fresh Playwright DRIVER + browser. The driver (a Node process) is
        where response bytes get marshaled, so ITS ~4GB V8 heap is the one
        pathological pages exhaust — relaunching only the browser leaves the
        poisoned driver in place (shards 10/14/18, Aug 2026)."""
        from playwright.async_api import async_playwright
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=False)
        bf = BrowserFetcher(browser,
                            nav_timeout_s=self.cfg.fetch.total_timeout_s)
        return pw, browser, bf

    async def _teardown_stack(self, pw, browser):
        for closer in (browser.close, pw.stop):
            try:
                await closer()
            except Exception:
                pass  # a crashed driver can't close cleanly — that's fine

    async def _do_browser(self, browser_fetcher: BrowserFetcher, row: dict):
        out = self._base_row(row)
        res = await browser_fetcher.fetch(row["url_pr"])
        status_map = {"ok": FETCH_OK, "blocked": "http_403",
                      "not_found": "http_404", "error": "other_error"}
        out.update(final_url=res.final_url,
                   fetch_status=status_map.get(res.status, "other_error"),
                   http_status=res.http_status,
                   content_type_header=res.content_type,
                   error_detail=res.error_detail)
        if res.status == "ok" and res.body:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                self._extract_pool, self._store_body, out,
                res.body, res.content_type)
        await self._buffer_row(out)

    # -------------------------------------------------------------------- run

    async def run(self) -> Dict[str, int]:
        pending = self.load_pending()
        if not pending:
            logger.info("shard %d: nothing to do", self.shard)
            self.s3io.write_done_marker(self.cfg.job_name, self.shard,
                                        self._status_counts)
            return self._status_counts

        browser_rows = [r for r in pending if r["strategy"] == STRATEGY_BROWSER]
        http_rows = [r for r in pending if r["strategy"] in
                     (STRATEGY_REWRITE, STRATEGY_RETRY)]

        # HTTP strategies: reuse the async fetcher with gentle concurrency.
        if http_rows:
            limiter = DomainRateLimiterPool(
                rate_per_second=self.cfg.fetch.per_domain_rps,
                burst=self.cfg.fetch.per_domain_burst,
                cooldown_seconds=self.cfg.fetch.domain_cooldown_s)
            sem = asyncio.Semaphore(self.cfg.fetch.worker_concurrency)
            connector = aiohttp.TCPConnector(limit=self.cfg.fetch.worker_concurrency)
            async with aiohttp.ClientSession(connector=connector) as session:
                fetcher = Fetcher(self.cfg.fetch, limiter, session)

                async def bounded(r):
                    url = rewrite_url(r["url_pr"]) if r["strategy"] == STRATEGY_REWRITE \
                        else r["url_pr"]
                    async with sem:
                        try:
                            await self._do_http(fetcher, r, url)
                        except Exception as e:
                            logger.error("event %s http error %r", r["event_id"], e)
                await asyncio.gather(*(bounded(r) for r in http_rows))

        # Browser strategy: serial, deliberately slow. The browser PROCESS is
        # recycled every BROWSER_RECYCLE_PAGES fetches and after any driver
        # crash: V8's per-process heap cap (~4GB) is what pathological IR
        # pages exhaust — container memory can't fix that, only a fresh
        # process can (the shard-10/14/18 OOM cascade of 2026-08).
        if browser_rows:
            pw, browser, bf = await self._launch_stack()
            pages_on_stack = 0
            try:
                for i, r in enumerate(browser_rows):
                    if pages_on_stack >= BROWSER_RECYCLE_PAGES:
                        await self._teardown_stack(pw, browser)
                        pw, browser, bf = await self._launch_stack()
                        logger.info("shard %d: browser stack recycled", self.shard)
                        pages_on_stack = 0
                    try:
                        await self._do_browser(bf, r)
                    except Exception as e:
                        logger.error("event %s browser error %r", r["event_id"], e)
                        # the crashed event still gets a row (poison pages
                        # must self-identify in the output, not vanish) ...
                        out = self._base_row(r)
                        out.update(fetch_status="other_error",
                                   error_detail=f"browser_crash: {e!r}"[:500])
                        await self._buffer_row(out)
                        # ... and a stack that threw is never reused
                        await self._teardown_stack(pw, browser)
                        pw, browser, bf = await self._launch_stack()
                        logger.info("shard %d: browser stack recycled after "
                                    "crash", self.shard)
                        pages_on_stack = 0
                        continue
                    pages_on_stack += 1
                    if (i + 1) % 50 == 0:
                        logger.info("shard %d browser: %d/%d",
                                    self.shard, i + 1, len(browser_rows))
            finally:
                await self._teardown_stack(pw, browser)

        self._flush()
        self._extract_pool.shutdown(wait=False)
        self.s3io.write_done_marker(self.cfg.job_name, self.shard, self._status_counts)
        logger.info("shard %d complete: %s", self.shard, self._status_counts)
        return self._status_counts


def run(cfg: Config, shard: int, boto3_session=None) -> Dict[str, int]:
    return asyncio.run(BackfillWorker(cfg, shard, boto3_session=boto3_session).run())
