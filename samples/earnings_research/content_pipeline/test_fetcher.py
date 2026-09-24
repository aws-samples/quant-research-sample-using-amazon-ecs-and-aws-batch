"""Fetcher tests against a local aiohttp test server (real HTTP, no mocking
of aiohttp internals)."""

import asyncio

import aiohttp
import pytest
from aiohttp import web

from config import FetchConfig
from domain_rate_limiter import DomainRateLimiterPool
from fetcher import Fetcher
from models import (
    FETCH_CONNECT_ERROR,
    FETCH_HTTP_403,
    FETCH_HTTP_404,
    FETCH_HTTP_5XX,
    FETCH_OK,
    FETCH_TIMEOUT,
    FETCH_TOO_LARGE,
)


def _cfg(**overrides) -> FetchConfig:
    base = dict(
        worker_concurrency=4, per_domain_rps=1000.0, per_domain_burst=1000,
        domain_cooldown_s=0.1, connect_timeout_s=2, total_timeout_s=2,
        max_redirects=5, max_body_bytes=1024 * 1024, max_attempts=2,
        backoff_base_s=0.05, user_agent="test-agent/1.0",
        tls_insecure_fallback=True, retry_statuses_on_resume=[],
        checkpoint_every_rows=100, checkpoint_every_s=60, extract_threads=1,
    )
    base.update(overrides)
    return FetchConfig(**base)


@pytest.fixture
async def server(aiohttp_server):
    hits = {"count_5xx": 0}

    async def ok(request):
        return web.Response(body=b"%PDF-1.4 fake pdf content",
                            content_type="application/pdf")

    async def missing(request):
        return web.Response(status=404)

    async def forbidden(request):
        return web.Response(status=403)

    async def flaky(request):
        hits["count_5xx"] += 1
        if hits["count_5xx"] < 2:
            return web.Response(status=503)
        return web.Response(body=b"recovered", content_type="text/plain")

    async def huge(request):
        return web.Response(body=b"x" * (2 * 1024 * 1024),
                            content_type="application/octet-stream")

    async def slow(request):
        await asyncio.sleep(10)
        return web.Response(body=b"too late")

    async def redirected(request):
        raise web.HTTPFound("/ok")

    app = web.Application()
    app.router.add_get("/ok", ok)
    app.router.add_get("/404", missing)
    app.router.add_get("/403", forbidden)
    app.router.add_get("/flaky", flaky)
    app.router.add_get("/huge", huge)
    app.router.add_get("/slow", slow)
    app.router.add_get("/redirect", redirected)
    return await aiohttp_server(app)


async def _fetch(server, path, cfg=None):
    cfg = cfg or _cfg()
    limiter = DomainRateLimiterPool(rate_per_second=1000, burst=1000)
    async with aiohttp.ClientSession() as session:
        fetcher = Fetcher(cfg, limiter, session)
        return await fetcher.fetch_one(str(server.make_url(path)))


@pytest.mark.asyncio
async def test_ok(server):
    r = await _fetch(server, "/ok")
    assert r.fetch_status == FETCH_OK
    assert r.http_status == 200
    assert r.body.startswith(b"%PDF-")
    assert r.content_type_header.startswith("application/pdf")


@pytest.mark.asyncio
async def test_404_not_retried(server):
    r = await _fetch(server, "/404")
    assert r.fetch_status == FETCH_HTTP_404
    assert r.attempt == 1


@pytest.mark.asyncio
async def test_403_not_retried(server):
    r = await _fetch(server, "/403")
    assert r.fetch_status == FETCH_HTTP_403
    assert r.attempt == 1


@pytest.mark.asyncio
async def test_5xx_retried_and_recovers(server):
    r = await _fetch(server, "/flaky")
    assert r.fetch_status == FETCH_OK
    assert r.attempt == 2
    assert r.body == b"recovered"


@pytest.mark.asyncio
async def test_5xx_exhausts_attempts(aiohttp_server):
    async def always_503(request):
        return web.Response(status=503)

    app = web.Application()
    app.router.add_get("/down", always_503)
    server = await aiohttp_server(app)

    r = await _fetch(server, "/down", cfg=_cfg(max_attempts=2))
    assert r.fetch_status == FETCH_HTTP_5XX
    assert r.attempt == 2


@pytest.mark.asyncio
async def test_size_cap_mid_stream(server):
    r = await _fetch(server, "/huge")
    assert r.fetch_status == FETCH_TOO_LARGE


@pytest.mark.asyncio
async def test_timeout(server):
    r = await _fetch(server, "/slow")
    assert r.fetch_status == FETCH_TIMEOUT


@pytest.mark.asyncio
async def test_redirect_followed_final_url_recorded(server):
    r = await _fetch(server, "/redirect")
    assert r.fetch_status == FETCH_OK
    assert r.final_url.endswith("/ok")


@pytest.mark.asyncio
async def test_connection_refused():
    cfg = _cfg()
    limiter = DomainRateLimiterPool(rate_per_second=1000, burst=1000)
    async with aiohttp.ClientSession() as session:
        fetcher = Fetcher(cfg, limiter, session)
        r = await fetcher.fetch_one("http://127.0.0.1:1/nothing-here")
    assert r.fetch_status == FETCH_CONNECT_ERROR
