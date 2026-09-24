"""HTTP document fetcher with retry policy, size cap, and TLS fallback.

Retry policy (mirrors the bedrock_client.py shape, adapted for HTTP):
  - retried (up to max_attempts, jittered exponential backoff): timeouts,
    connection resets, 429 (plus domain cooldown), 5xx
  - fail-fast, recorded: DNS failure, 404/410, 403/401, redirect loops,
    oversized bodies, bad content
  - TLS certificate errors: one extra attempt with verification disabled
    (old IR sites; recorded as tls_insecure=True), if enabled in config

Everything returns a FetchResult — exceptions never escape fetch_one.
"""

import asyncio
import logging
import random
import ssl
from typing import Optional

import aiohttp

from config import FetchConfig
from domain_rate_limiter import DomainRateLimiterPool
from models import (
    FETCH_CONNECT_ERROR,
    FETCH_DNS_ERROR,
    FETCH_HTTP_403,
    FETCH_HTTP_404,
    FETCH_HTTP_429,
    FETCH_HTTP_5XX,
    FETCH_HTTP_OTHER,
    FETCH_OK,
    FETCH_OTHER_ERROR,
    FETCH_REDIRECT_LOOP,
    FETCH_TIMEOUT,
    FETCH_TLS_ERROR,
    FETCH_TOO_LARGE,
    FetchResult,
)

logger = logging.getLogger(__name__)


class Fetcher:
    def __init__(self, cfg: FetchConfig, limiter: DomainRateLimiterPool,
                 session: aiohttp.ClientSession):
        self.cfg = cfg
        self.limiter = limiter
        self.session = session
        self._insecure_ssl = ssl.create_default_context()
        self._insecure_ssl.check_hostname = False
        self._insecure_ssl.verify_mode = ssl.CERT_NONE

    async def fetch_one(self, url: str) -> FetchResult:
        attempt = 0
        insecure = False
        last: Optional[FetchResult] = None
        while attempt < self.cfg.max_attempts:
            attempt += 1
            await self.limiter.acquire(url)
            result = await self._attempt(url, attempt, insecure)
            if result.fetch_status == FETCH_OK:
                return result
            last = result

            if result.fetch_status == FETCH_TLS_ERROR and \
                    self.cfg.tls_insecure_fallback and not insecure:
                # one retry with verification off; doesn't consume backoff
                insecure = True
                continue

            if result.fetch_status == FETCH_HTTP_429:
                self.limiter.cooldown(url)

            if result.fetch_status in (FETCH_TIMEOUT, FETCH_HTTP_429,
                                       FETCH_HTTP_5XX, FETCH_CONNECT_ERROR):
                if attempt < self.cfg.max_attempts:
                    delay = self.cfg.backoff_base_s * (2 ** (attempt - 1))
                    await asyncio.sleep(delay * (0.5 + random.random()))
                continue

            break  # fail-fast statuses
        return last

    async def _attempt(self, url: str, attempt: int, insecure: bool) -> FetchResult:
        timeout = aiohttp.ClientTimeout(
            total=self.cfg.total_timeout_s, connect=self.cfg.connect_timeout_s
        )
        headers = {"User-Agent": self.cfg.user_agent}
        try:
            async with self.session.get(
                url, timeout=timeout, headers=headers,
                max_redirects=self.cfg.max_redirects, allow_redirects=True,
                ssl=self._insecure_ssl if insecure else None,
            ) as resp:
                status = resp.status
                final_url = str(resp.url)
                ct = resp.headers.get("Content-Type")

                if status in (404, 410):
                    return FetchResult(FETCH_HTTP_404, status, final_url, ct,
                                       attempt=attempt, tls_insecure=insecure)
                if status in (401, 403):
                    return FetchResult(FETCH_HTTP_403, status, final_url, ct,
                                       attempt=attempt, tls_insecure=insecure)
                if status == 429:
                    return FetchResult(FETCH_HTTP_429, status, final_url, ct,
                                       attempt=attempt, tls_insecure=insecure)
                if status >= 500:
                    return FetchResult(FETCH_HTTP_5XX, status, final_url, ct,
                                       attempt=attempt, tls_insecure=insecure)
                if status >= 400:
                    return FetchResult(FETCH_HTTP_OTHER, status, final_url, ct,
                                       attempt=attempt, tls_insecure=insecure)

                declared = resp.headers.get("Content-Length")
                if declared and int(declared) > self.cfg.max_body_bytes:
                    return FetchResult(FETCH_TOO_LARGE, status, final_url, ct,
                                       attempt=attempt, tls_insecure=insecure,
                                       error_detail=f"content-length {declared}")

                chunks = []
                size = 0
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    size += len(chunk)
                    if size > self.cfg.max_body_bytes:
                        return FetchResult(FETCH_TOO_LARGE, status, final_url, ct,
                                           attempt=attempt, tls_insecure=insecure,
                                           error_detail=f"streamed > {self.cfg.max_body_bytes}")
                    chunks.append(chunk)

                return FetchResult(FETCH_OK, status, final_url, ct,
                                   body=b"".join(chunks), attempt=attempt,
                                   tls_insecure=insecure)

        except aiohttp.TooManyRedirects:
            return FetchResult(FETCH_REDIRECT_LOOP, attempt=attempt,
                               error_detail="too many redirects")
        except asyncio.TimeoutError:
            return FetchResult(FETCH_TIMEOUT, attempt=attempt,
                               error_detail="timeout")
        except aiohttp.ClientSSLError as e:
            return FetchResult(FETCH_TLS_ERROR, attempt=attempt,
                               error_detail=repr(e))
        except aiohttp.ClientConnectorError as e:
            detail = repr(e)
            os_err = getattr(e, "os_error", None)
            if isinstance(getattr(e, "_conn_key", None), object) and \
                    "Name or service not known" in detail or \
                    "nodename nor servname" in detail or \
                    (os_err is not None and getattr(os_err, "errno", None) in (-2, 8)):
                return FetchResult(FETCH_DNS_ERROR, attempt=attempt, error_detail=detail)
            return FetchResult(FETCH_CONNECT_ERROR, attempt=attempt, error_detail=detail)
        except aiohttp.ClientError as e:
            return FetchResult(FETCH_CONNECT_ERROR, attempt=attempt, error_detail=repr(e))
        except Exception as e:
            return FetchResult(FETCH_OTHER_ERROR, attempt=attempt, error_detail=repr(e))
