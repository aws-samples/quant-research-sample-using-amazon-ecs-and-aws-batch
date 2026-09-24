"""Per-domain politeness limiter.

A lazy pool of token buckets keyed by registrable domain, plus a global
concurrency semaphore owned by the worker. Token buckets are timestamp-based
(no background refill task per domain — with thousands of domains per shard,
one asyncio task each would be wasteful; the bedrock rate_limiter.py refill
pattern doesn't scale here).
"""

import asyncio
import logging
import time
from typing import Dict, Optional
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

# Multi-label public suffixes we care about; a full PSL (tldextract) is
# overkill for the IR-hosting domains this pipeline actually hits.
_TWO_LABEL_SUFFIXES = {
    "co.uk", "org.uk", "ac.uk", "com.au", "net.au", "org.au", "co.jp",
    "or.jp", "ne.jp", "co.in", "co.kr", "com.br", "com.cn", "com.hk",
    "com.sg", "com.tw", "co.za", "com.mx", "co.nz", "com.tr",
}


def registrable_domain(url: str) -> str:
    """Best-effort registrable domain for rate-limit keying."""
    host = (urlsplit(url).hostname or "").lower().strip(".")
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) <= 2:
        return host
    if ".".join(parts[-2:]) in _TWO_LABEL_SUFFIXES:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


class _TokenBucket:
    """Timestamp-based token bucket; refills lazily on acquire."""

    def __init__(self, rate_per_second: float, burst: int):
        self.rate = rate_per_second
        self.capacity = float(max(burst, 1))
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self.cooldown_until = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                wait = self.cooldown_until - now
                if wait <= 0:
                    self.tokens = min(
                        self.capacity, self.tokens + (now - self.updated) * self.rate
                    )
                    self.updated = now
                    if self.tokens >= 1.0:
                        self.tokens -= 1.0
                        return
                    wait = (1.0 - self.tokens) / self.rate
            await asyncio.sleep(wait)

    def start_cooldown(self, seconds: float):
        self.cooldown_until = max(
            self.cooldown_until, time.monotonic() + seconds
        )


class DomainRateLimiterPool:
    """Lazy per-domain token buckets sharing one config."""

    def __init__(self, rate_per_second: float = 2.0, burst: int = 2,
                 cooldown_seconds: float = 60.0):
        self.rate = rate_per_second
        self.burst = burst
        self.cooldown_seconds = cooldown_seconds
        self._buckets: Dict[str, _TokenBucket] = {}

    def _bucket(self, domain: str) -> _TokenBucket:
        bucket = self._buckets.get(domain)
        if bucket is None:
            bucket = _TokenBucket(self.rate, self.burst)
            self._buckets[domain] = bucket
        return bucket

    async def acquire(self, url: str):
        """Wait until a request to this URL's domain is allowed."""
        domain = registrable_domain(url)
        if domain:
            await self._bucket(domain).acquire()

    def cooldown(self, url: str, seconds: Optional[float] = None):
        """Pause a domain after 429/repeated 5xx."""
        domain = registrable_domain(url)
        if domain:
            secs = seconds if seconds is not None else self.cooldown_seconds
            self._bucket(domain).start_cooldown(secs)
            logger.info("domain %s cooling down for %.0fs", domain, secs)
