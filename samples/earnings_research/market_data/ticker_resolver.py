"""Web-grounded current-ticker resolver.

Replicates, in code, the "search the web then read it" approach that reliably
resolved ticker changes (BK->BNY, ASGN->EFOR, EXPI->AGNT, ...). The base LLM
does NOT know post-cutoff renames and Bedrock has no managed web-search tool, so
we do the retrieval ourselves: HTTP GET the DuckDuckGo HTML endpoint for the
company name, strip to text, and hand that text to the LLM to EXTRACT the current
US ticker. The LLM only reads supplied evidence — it never answers from memory.

Used by peer_deriver as the fallback when a peer ticker does not resolve to
live Databento data (i.e. a dead/renamed ticker).
"""

import html
import json
import logging
import re
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_DDG = "https://html.duckduckgo.com/html/"
_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
_TAG = re.compile(r"<[^>]+>")

_EXTRACT_SYSTEM = (
    "You are an equities reference-data analyst. You are given web search result "
    "text about a company. Extract ONLY what the text supports. Return valid JSON.")
_EXTRACT_PROMPT = """Company: "{name}"
Its former/queried US ticker "{old}" no longer has live market data as of {asof}.

Using ONLY the web search results below, determine the company's CURRENT US
exchange ticker. Do not use prior knowledge; rely on the text. If the text does
not clearly establish a current US-listed ticker, return null.

--- WEB SEARCH RESULTS ---
{evidence}
--- END ---

Respond with ONLY this JSON, no prose:
{{"current_ticker": "<current US exchange ticker, or null>", "status": "active|renamed|acquired|merged|bankrupt_delisted|unknown", "confidence": "high|medium|low", "evidence_quote": "<short phrase from the text supporting it>"}}"""


class SearchBlocked(Exception):
    """The search backend returned a bot/CAPTCHA challenge instead of results."""


_BLOCK_MARKERS = ("bots use duckduckgo", "complete the following challenge",
                  "containing a duck", "captcha")


def _search_text(query: str, timeout: int = 30, max_chars: int = 6000) -> str:
    """Fetch DDG HTML results for a query, return de-tagged text (bounded).

    Raises SearchBlocked if the response is a CAPTCHA/bot-challenge page (DDG
    rate-limits scraping) so the caller does not feed a challenge page to the
    LLM as if it were evidence."""
    r = requests.get(_DDG, params={"q": query}, headers={"User-Agent": _UA},
                     timeout=timeout)
    r.raise_for_status()
    text = html.unescape(_TAG.sub(" ", r.text))
    text = re.sub(r"\s+", " ", text).strip()
    if any(mk in text.lower() for mk in _BLOCK_MARKERS):
        raise SearchBlocked(query)
    return text[:max_chars]


class TickerResolver:
    """Resolve a company's current US ticker via web search + LLM extraction."""

    def __init__(self, bedrock, model_id: str, max_attempts: int = 3):
        self.bedrock = bedrock
        self.model_id = model_id
        self.max_attempts = max_attempts

    def resolve(self, name: str, old_ticker: str, asof: str) -> dict:
        """Return {current_ticker, status, confidence, evidence_quote}.

        current_ticker is None when the web evidence does not establish a live
        US ticker (delisted/bankrupt/ambiguous). Never fabricates: the LLM sees
        only fetched text. On any error returns an empty/unknown result."""
        if not name:
            return {"current_ticker": None, "status": "unknown", "confidence": "low"}
        # Single query per call (multiple rapid queries trip DDG's bot block).
        query = f"{name} current stock ticker symbol {asof}"
        try:
            evidence = _search_text(query)
        except SearchBlocked:
            logger.warning("search blocked (CAPTCHA) for %s — cannot resolve", name)
            return {"current_ticker": None, "status": "search_blocked", "confidence": "low"}
        except Exception as e:
            logger.warning("web search failed for %s: %s", name, e)
            return {"current_ticker": None, "status": "unknown", "confidence": "low"}
        if not evidence:
            return {"current_ticker": None, "status": "unknown", "confidence": "low"}

        prompt = _EXTRACT_PROMPT.format(name=name, old=old_ticker or "?",
                                        asof=asof, evidence=evidence)
        delay = 1.0
        for attempt in range(1, self.max_attempts + 1):
            try:
                resp = self.bedrock.converse(
                    modelId=self.model_id,
                    system=[{"text": _EXTRACT_SYSTEM}],
                    messages=[{"role": "user", "content": [{"text": prompt}]}],
                    inferenceConfig={"maxTokens": 300, "temperature": 0.0})
                txt = " ".join(b.get("text", "") for b in
                               resp["output"]["message"]["content"] if b.get("text"))
                m = re.search(r"\{.*\}", txt, re.DOTALL)
                if not m:
                    return {"current_ticker": None, "status": "unknown", "confidence": "low"}
                obj = json.loads(m.group(0))
                ct = obj.get("current_ticker")
                obj["current_ticker"] = str(ct).strip().upper() if ct else None
                return obj
            except Exception as e:
                if attempt < self.max_attempts:
                    time.sleep(delay); delay = min(delay * 2, 20)
                    continue
                logger.warning("ticker extraction failed for %s: %s", name, e)
                return {"current_ticker": None, "status": "unknown", "confidence": "low"}
