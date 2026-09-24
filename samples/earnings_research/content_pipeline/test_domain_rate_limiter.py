import asyncio
import time

import pytest

from domain_rate_limiter import DomainRateLimiterPool, registrable_domain


class TestRegistrableDomain:
    @pytest.mark.parametrize("url,domain", [
        ("https://s2.q4cdn.com/299287126/files/x.pdf", "q4cdn.com"),
        ("https://mb.cision.com/Public/1629/x.pdf", "cision.com"),
        ("https://investors.beyondmeat.com/node/14221/pdf", "beyondmeat.com"),
        ("http://www.example.co.uk/a", "example.co.uk"),
        ("https://a.b.company.com.au/x", "company.com.au"),
        ("https://example.com/x", "example.com"),
        ("not a url", ""),
    ])
    def test_cases(self, url, domain):
        assert registrable_domain(url) == domain


class TestDomainRateLimiterPool:
    @pytest.mark.asyncio
    async def test_single_domain_respects_rate(self):
        pool = DomainRateLimiterPool(rate_per_second=10.0, burst=1)
        url = "https://example.com/x"
        start = time.monotonic()
        for _ in range(4):
            await pool.acquire(url)
        elapsed = time.monotonic() - start
        # burst 1 + 3 more at 10/s => >= ~0.3s
        assert elapsed >= 0.25

    @pytest.mark.asyncio
    async def test_domains_independent(self):
        pool = DomainRateLimiterPool(rate_per_second=1.0, burst=1)
        start = time.monotonic()
        await asyncio.gather(*(
            pool.acquire(f"https://domain{i}.com/x") for i in range(10)
        ))
        # 10 different domains, each within its own burst => near-instant
        assert time.monotonic() - start < 0.5

    @pytest.mark.asyncio
    async def test_cooldown_blocks(self):
        pool = DomainRateLimiterPool(rate_per_second=100.0, burst=10,
                                     cooldown_seconds=0.3)
        url = "https://example.com/x"
        await pool.acquire(url)
        pool.cooldown(url)
        start = time.monotonic()
        await pool.acquire(url)
        assert time.monotonic() - start >= 0.25

    @pytest.mark.asyncio
    async def test_unparseable_url_no_crash(self):
        pool = DomainRateLimiterPool()
        await pool.acquire("garbage")  # should not raise
