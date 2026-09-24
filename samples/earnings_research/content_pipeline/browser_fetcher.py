"""Headed-Chromium fetcher for bot-walled press releases.

Why headed: Akamai / Q4 Inc ("gcs-web") and similar bot managers fingerprint
the TLS/HTTP2 stack and automation signals. aiohttp and *headless* Chromium
are silently hung or 403'd; a real HEADED Chromium presents a genuine
Chrome fingerprint and is served normally — the same thing a human sees when
clicking the link. On a server this needs a virtual display (xvfb).

Why response-capture: Chrome renders PDFs in its built-in viewer, so reading
the page body yields ~500 bytes of viewer HTML. We listen to network
responses and grab the raw bytes of the document response instead.

Two document shapes are handled:
  - PDF endpoints (…/pdf, *.pdf): capture the pdf response bytes.
  - HTML press-release pages: capture the main document response; if it's
    HTML, also keep the rendered article text for extraction.

q4cdn hotlink fallback: direct s NN .q4cdn.com/*.pdf return 403 even headed
because of referer/hotlink protection. When a q4cdn URL 403s, we retry with
a referer header set to the tenant's IR origin.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Tuple
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)


@dataclass
class BrowserResult:
    status: str            # ok | blocked | error | not_found
    http_status: Optional[int] = None
    final_url: Optional[str] = None
    content_type: Optional[str] = None
    body: Optional[bytes] = None
    error_detail: Optional[str] = None


def _is_pdf_url(url: str) -> bool:
    u = url.lower()
    return u.endswith("/pdf") or ".pdf" in u


# Never capture a response body larger than this through the driver (matches
# the pipeline's max_body_bytes: no document we keep is bigger anyway).
_MAX_CAPTURE_BYTES = 52428800  # 50 MB


class BrowserFetcher:
    """Wraps a single Playwright browser; one page per fetch, serial per browser."""

    def __init__(self, browser, nav_timeout_s: int = 60,
                 settle_ms: int = 2500):
        self.browser = browser
        self.nav_timeout_ms = nav_timeout_s * 1000
        self.settle_ms = settle_ms

    async def fetch(self, url: str) -> BrowserResult:
        res = await self._attempt(url, referer=None)
        # q4cdn hotlink protection: retry with the tenant IR origin as referer
        if (res.status == "blocked" and "q4cdn.com" in (urlsplit(url).hostname or "")):
            res2 = await self._attempt(url, referer=self._guess_referer(url))
            if res2.status == "ok":
                return res2
        return res

    @staticmethod
    async def _fetch_in_page(page, url: str):
        """Re-request a URL from inside the page (established session) and
        return raw bytes, or None. Used when Chrome renders a PDF viewer
        wrapper instead of exposing the document response."""
        try:
            result = await page.evaluate(
                """async (u) => {
                    const r = await fetch(u, {credentials: 'include'});
                    if (!r.ok) return null;
                    const buf = await r.arrayBuffer();
                    return Array.from(new Uint8Array(buf));
                }""", url)
            if result:
                return bytes(result)
        except Exception as e:
            logger.debug("in-page fetch failed: %s", e)
        return None

    @staticmethod
    def _guess_referer(url: str) -> str:
        # q4cdn tenants are fronted by the company IR site; the CDN accepts a
        # generic https referer from the same doc host in practice.
        host = urlsplit(url).hostname or ""
        return f"https://{host}/"

    async def _attempt(self, url: str, referer: Optional[str]) -> BrowserResult:
        want_pdf = _is_pdf_url(url)
        context = await self.browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            accept_downloads=True,
            extra_http_headers={"Referer": referer} if referer else None,
        )
        page = await context.new_page()

        captured = {}

        async def on_response(resp):
            try:
                ct = resp.headers.get("content-type", "")
                is_doc = (resp.url == url or _is_pdf_url(resp.url)
                          or "html" in ct or "pdf" in ct)
                if not is_doc:
                    return
                # resp.body() marshals bytes through the Playwright NODE
                # driver, whose ~4GB V8 heap is shared across browser
                # relaunches — an unbounded body OOMs the driver itself
                # (poison events 1204096056/1203585642/1204163970, Aug 2026).
                clen = resp.headers.get("content-length")
                if clen and int(clen) > _MAX_CAPTURE_BYTES:
                    logger.warning("skipping %d-byte response capture from %s",
                                   int(clen), resp.url)
                    return
                # Prefer a pdf response; otherwise keep the first main-doc html.
                if "pdf" in ct and "pdf" not in captured.get("ct", ""):
                    captured.update(status=resp.status, ct=ct, url=resp.url,
                                    body=await resp.body())
                elif "html" in ct and "body" not in captured:
                    captured.update(status=resp.status, ct=ct, url=resp.url,
                                    body=await resp.body())
            except Exception as e:  # response body may be gone; ignore
                logger.debug("resp capture skipped: %s", e)

        page.on("response", on_response)

        try:
            resp = await page.goto(url, timeout=self.nav_timeout_ms,
                                   wait_until="load")
            nav_status = resp.status if resp else None
            await page.wait_for_timeout(self.settle_ms)
            final_url = page.url

            # If the captured PDF response has real bytes, use it. Chrome's
            # viewer often consumes the stream, so resp.body() comes back empty
            # or non-PDF even though ct=application/pdf — in that case (or for
            # any pdf URL that never surfaced a usable body) re-request the URL
            # inside the page context (session/referer established) via fetch().
            # A 206 capture is the viewer's first 64KB range request — it
            # starts with %PDF- but is truncated; never return it as the doc
            # (VLO phoenix.zhtml events, Aug 2026).
            cap_body = captured.get("body")
            cap_complete = captured.get("status") == 200
            if ("pdf" in captured.get("ct", "") and cap_body
                    and cap_body[:5] == b"%PDF-" and cap_complete):
                return BrowserResult("ok", captured["status"], final_url,
                                     captured["ct"], cap_body)

            if want_pdf or "pdf" in captured.get("ct", ""):
                # fetch() ignores the viewer's range plumbing and returns the
                # whole body. Target the captured document's own URL — on
                # viewer-wrapped pages it differs from the page URL.
                pdf_url = captured.get("url") or url
                pdf = await self._fetch_in_page(page, pdf_url)
                if (pdf is None or pdf[:5] != b"%PDF-") and pdf_url != url:
                    pdf = await self._fetch_in_page(page, url)
                if pdf is not None and pdf[:5] == b"%PDF-":
                    return BrowserResult("ok", 200, final_url,
                                         "application/pdf", pdf)

            # HTML press-release page: capture rendered body as UTF-8 bytes so
            # the existing extractor (trafilatura/bs4) handles it downstream.
            if nav_status and nav_status >= 400:
                status = "blocked" if nav_status in (403, 429) else "not_found"
                return BrowserResult(status, nav_status, final_url,
                                     captured.get("ct"))
            html = await page.content()
            body = html.encode("utf-8", errors="replace")
            return BrowserResult("ok", nav_status or 200, final_url,
                                 "text/html", body)

        except Exception as e:
            detail = repr(e)
            # Akamai kills the stream for detected bots -> protocol error
            if "ERR_HTTP2_PROTOCOL_ERROR" in detail or "ERR_CONNECTION" in detail:
                return BrowserResult("blocked", error_detail=detail)
            if "Timeout" in detail or "timeout" in detail:
                return BrowserResult("blocked", error_detail="nav_timeout")
            return BrowserResult("error", error_detail=detail)
        finally:
            await page.close()
            await context.close()
