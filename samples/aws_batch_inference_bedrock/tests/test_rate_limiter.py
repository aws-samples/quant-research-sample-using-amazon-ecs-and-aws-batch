"""Tests for rate limiter functionality."""

import asyncio
import pytest
from rate_limiter import SemaphoreRateLimiter, RateLimiter


class TestSemaphoreRateLimiter:
    """Test cases for SemaphoreRateLimiter implementation."""

    def test_acquire_within_burst_capacity(self):
        """Test that acquire succeeds immediately within burst capacity."""
        async def run():
            limiter = SemaphoreRateLimiter(rate_per_second=10.0, burst_capacity=5)
            for _ in range(5):
                await asyncio.wait_for(limiter.acquire(), timeout=0.1)
        asyncio.run(run())

    def test_acquire_blocks_when_exhausted(self):
        """Test that acquire blocks when burst capacity is exhausted."""
        async def run():
            limiter = SemaphoreRateLimiter(rate_per_second=10.0, burst_capacity=1)
            await limiter.acquire()
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(limiter.acquire(), timeout=0.05)
        asyncio.run(run())

    def test_refill_releases_permits(self):
        """Test that the background refill task releases permits over time."""
        async def run():
            limiter = SemaphoreRateLimiter(rate_per_second=20.0, burst_capacity=1)
            await limiter.acquire()
            await limiter.start()
            try:
                await asyncio.wait_for(limiter.acquire(), timeout=0.2)
            finally:
                await limiter.stop()
        asyncio.run(run())

    def test_start_stop_idempotent(self):
        """Test that start/stop can be called multiple times safely."""
        async def run():
            limiter = SemaphoreRateLimiter(rate_per_second=10.0, burst_capacity=5)
            await limiter.start()
            await limiter.start()
            await limiter.stop()
            await limiter.stop()
        asyncio.run(run())


class TestRateLimiter:
    """Test cases for RateLimiter implementation."""

    def test_acquire_starts_limiter_automatically(self):
        """Test that acquire auto-starts the rate limiter."""
        async def run():
            limiter = RateLimiter(requests_per_minute=600, tokens_per_minute=60000)
            assert limiter._started is False
            await limiter.acquire(100)
            assert limiter._started is True
            await limiter.stop()
        asyncio.run(run())

    def test_acquire_within_burst(self):
        """Test that multiple acquires succeed within burst capacity."""
        async def run():
            limiter = RateLimiter(requests_per_minute=600, tokens_per_minute=60000)
            for _ in range(10):
                await limiter.acquire(100)
            await limiter.stop()
        asyncio.run(run())

    def test_record_actual_tokens_is_noop(self):
        """Test that record_actual_tokens doesn't raise."""
        limiter = RateLimiter(requests_per_minute=60, tokens_per_minute=10000)
        limiter.record_actual_tokens(500)
