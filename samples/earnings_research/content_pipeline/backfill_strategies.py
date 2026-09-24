"""Classify failed fetch rows into backfill strategies.

The full run's failures are almost all durable, not transient — but probing
proved two large classes ARE recoverable with the right tactic:

  browser  Bot-walled hosts (Akamai / Q4 Inc "gcs-web", q4cdn, and similar)
           that silently hang or 403 our aiohttp fetcher AND headless
           Chromium, but serve a genuine HEADED browser fine. These show up
           as timeout / http_403 / http_other on live, resolvable hosts.
           Fetched with a real headed Chromium, capturing the raw HTTP
           response bytes (Chrome wraps PDFs in a viewer otherwise).
  rewrite  BSE India moves filings /AttachLive/ -> /AttachHis/ shortly after
           publication; the rewritten URL was verified to return the PDF.
           Plain aiohttp, no browser needed.
  retry    Non-bot-wall transient statuses (http_5xx, http_429, connect_error,
           tls_error) — gentle aiohttp refetch, longer budget.
  skip     Truly gone: dns_error, dead platforms (corporate-ir.net), soft-404
           site migrations, and hard bot-walls that reject even headed
           browsers (bursamalaysia, idx.co.id, saudiexchange). Retrying is
           noise, not data.

Note on q4cdn direct hotlinks (s NN .q4cdn.com/.../*.pdf, 403 even headed):
these need an IR-page referer to satisfy hotlink protection, handled inside
the browser fetcher as a fallback rather than as a separate strategy.
"""

from typing import Optional
from urllib.parse import urlsplit

STRATEGY_BROWSER = "browser"
STRATEGY_REWRITE = "rewrite"
STRATEGY_RETRY = "retry"
STRATEGY_SKIP = "skip"

# Transient statuses that a plain gentle refetch can plausibly clear.
_RETRY_STATUSES = {"http_5xx", "http_429", "connect_error", "tls_error"}

# Statuses that, on a live host, indicate a bot wall a headed browser can pass.
_BROWSER_STATUSES = {"timeout", "http_403", "http_other"}

# Hosts confirmed unrecoverable even with a headed browser, or platforms that
# no longer exist — probed by hand.
_DEAD_HOST_SUBSTR = (
    "corporate-ir.net",      # Thomson "Phoenix" IR platform, shut down
    "bursamalaysia.com",     # hard bot wall (403 even headed)
    "idx.co.id",             # hard bot wall
    "saudiexchange.sa",      # hard bot wall
)


def _host(url: str) -> str:
    return (urlsplit(url).hostname or "").lower()


def classify(region: Optional[str], fetch_status: str, url_pr: str) -> str:
    if fetch_status == "ok":
        return STRATEGY_SKIP

    host = _host(url_pr)
    if any(dead in host for dead in _DEAD_HOST_SUBSTR):
        return STRATEGY_SKIP

    # BSE India historical-archive rewrite — cheap, no browser.
    if "bseindia.com" in host and "/AttachLive/" in url_pr:
        return STRATEGY_REWRITE

    # DNS failures: the host itself doesn't resolve — nothing to fetch.
    if fetch_status == "dns_error":
        return STRATEGY_SKIP

    # Bot-wall classes on a resolvable host -> headed browser.
    if fetch_status in _BROWSER_STATUSES:
        return STRATEGY_BROWSER

    # 404 on a live host is usually a real removal / soft-404. Skip by
    # default; the browser pass would just re-confirm the 404.
    if fetch_status == "http_404":
        return STRATEGY_SKIP

    if fetch_status in _RETRY_STATUSES:
        return STRATEGY_RETRY

    return STRATEGY_SKIP


def rewrite_url(url_pr: str) -> str:
    return url_pr.replace("/AttachLive/", "/AttachHis/")
