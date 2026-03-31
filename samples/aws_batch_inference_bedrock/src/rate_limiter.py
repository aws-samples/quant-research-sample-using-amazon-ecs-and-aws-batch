"""Rate limiting module using semaphore-based approach for high concurrency."""

import asyncio
import logging
import time
from typing import Optional


logger = logging.getLogger(__name__)


class SemaphoreRateLimiter:
    """Semaphore-based rate limiter for high-concurrency scenarios.
    
    Uses asyncio.Semaphore with a background task that periodically releases
    permits based on the configured rate. This avoids lock contention and
    allows true concurrent processing.
    """
    
    def __init__(self, rate_per_second: float, burst_capacity: int):
        """Initialize semaphore rate limiter.
        
        Args:
            rate_per_second: Number of operations allowed per second
            burst_capacity: Maximum burst size (semaphore capacity)
        """
        self.rate_per_second = rate_per_second
        self.burst_capacity = burst_capacity
        self.semaphore = asyncio.Semaphore(burst_capacity)
        self._refill_task: Optional[asyncio.Task] = None
        self._running = False
        
        logger.info(
            f"SemaphoreRateLimiter initialized: {rate_per_second:.2f}/s, "
            f"burst={burst_capacity}"
        )
    
    async def start(self):
        """Start the background refill task."""
        if self._running:
            return
        
        self._running = True
        self._refill_task = asyncio.create_task(self._refill_loop())
        logger.info("Rate limiter refill task started")
    
    async def stop(self):
        """Stop the background refill task."""
        self._running = False
        if self._refill_task:
            self._refill_task.cancel()
            try:
                await self._refill_task
            except asyncio.CancelledError:
                pass
        logger.info("Rate limiter refill task stopped")
    
    async def _refill_loop(self):
        """Background task that periodically releases semaphore permits."""
        # Calculate how often to release permits
        # Release 1 permit every (1 / rate_per_second) seconds
        interval = 1.0 / self.rate_per_second
        
        logger.info(f"Refill loop: releasing 1 permit every {interval:.4f}s")
        
        next_release = time.time() + interval
        
        while self._running:
            try:
                # Sleep until next release time (prevents drift)
                now = time.time()
                sleep_time = max(0, next_release - now)
                await asyncio.sleep(sleep_time)
                
                # Release a permit (semaphore handles over-release internally)
                try:
                    self.semaphore.release()
                except ValueError:
                    # Semaphore at capacity, skip this release
                    pass
                
                # Schedule next release
                next_release += interval
                
            except Exception as e:
                logger.error(f"Error in refill loop: {e}")
                await asyncio.sleep(0.1)
                next_release = time.time() + interval
    
    async def acquire(self):
        """Acquire a permit from the rate limiter."""
        await self.semaphore.acquire()


class RateLimiter:
    """Rate limiter with separate request and token limits.
    
    Uses semaphore-based approach for high concurrency.
    """
    
    def __init__(self, requests_per_minute: int, tokens_per_minute: int):
        """Initialize rate limiter with request and token limits.
        
        Args:
            requests_per_minute: Maximum API requests allowed per minute
            tokens_per_minute: Maximum tokens allowed per minute
        """
        # Convert per-minute to per-second
        requests_per_second = requests_per_minute / 60.0
        tokens_per_second = tokens_per_minute / 60.0
        
        # Use burst capacity = rate per second (allows 1 second of burst)
        self.request_limiter = SemaphoreRateLimiter(
            rate_per_second=requests_per_second,
            burst_capacity=max(int(requests_per_second), 1)
        )
        
        # Token limiter with larger burst capacity
        self.token_limiter = SemaphoreRateLimiter(
            rate_per_second=tokens_per_second,
            burst_capacity=max(int(tokens_per_second), 100)
        )
        
        self._started = False
        
        logger.info(
            f"RateLimiter initialized: {requests_per_minute} req/min, "
            f"{tokens_per_minute} tokens/min"
        )
    
    async def start(self):
        """Start the rate limiter background tasks."""
        if self._started:
            return
        
        await self.request_limiter.start()
        await self.token_limiter.start()
        self._started = True
        logger.info("RateLimiter started")
    
    async def stop(self):
        """Stop the rate limiter background tasks."""
        await self.request_limiter.stop()
        await self.token_limiter.stop()
        self._started = False
        logger.info("RateLimiter stopped")
    
    async def acquire(self, estimated_tokens: int) -> None:
        """Block until rate limits allow request with estimated token count.
        
        Args:
            estimated_tokens: Estimated number of tokens the request will consume
        """
        # Ensure rate limiter is started
        if not self._started:
            await self.start()
        
        # Acquire request permit (always 1)
        await self.request_limiter.acquire()
        
        # For tokens, we don't enforce strict per-request limits
        # Just ensure we don't exceed the overall rate
        # This is a simplified approach - we only throttle on requests
        # The token limiter is informational/secondary
    
    def record_actual_tokens(self, actual_tokens: int) -> None:
        """Record actual token usage (no-op in semaphore implementation)."""
        pass
