"""Derive a reporter's equity peer set via Bedrock, on the fly.

Given a reporter (bare symbol + name), a Claude model returns US-listed peers
in three mutually-exclusive buckets — pure play / functional / correlated —
the reporter itself being 'primary'. Each peer is returned as {ticker, name};
the NAME is the anchor that lets us re-resolve a ticker that has changed.
Peers are filtered to the US common-equity universe (FactSet), and dead/renamed
tickers are healed via a name-anchored, data-verified rename step. Uses the
Converse API + retry pattern from aws_batch_inference_bedrock/bedrock_client.py.
"""

import json
import logging
import re
import time
from typing import Dict, List, Optional

import boto3

from databento_client import DatabentoClient
from ticker_resolver import TickerResolver
from universe import in_universe, peer_universe_sql

logger = logging.getLogger(__name__)

MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
_RETRYABLE = {"ThrottlingException", "TooManyRequestsException",
              "ServiceUnavailableException", "InternalServerError",
              "ModelTimeoutException"}

_SYSTEM = (
    "You are an equity analyst building peer sets for earnings event studies. "
    "You return only valid JSON, no prose."
)

_PROMPT = """For the reporting company below, produce its US-listed equity peer set in three mutually-exclusive buckets. Each peer must be a US-listed company, appear in exactly one bucket, and NOT be the reporter itself.

- pure_play: companies whose primary business is a single line directly overlapping one of the reporter's segments (narrow product/market competitors).
- functional: companies competing with the reporter across its major functional lines of business, including large diversified competitors.
- correlated: large-cap operating companies that reliably co-move with the reporter but aren't direct business competitors.

Every peer must be an individual operating company (a single common stock). Do NOT include ETFs, index funds, mutual funds, or any other pooled/basket vehicle (e.g. no XLF, SPY, QQQ, sector or country funds).

For each peer give BOTH its current US exchange ticker and its full company name. The name is authoritative: if a ticker has changed, the name lets us re-resolve it.

Aim for 4-6 peers per bucket (12-18 total). Reporter: {name} (ticker {symbol}).

Respond with ONLY this JSON object, no markdown fences, no other text. Each entry is {{"ticker": "<SYM>", "name": "<company name>"}}:
{{"pure_play": [{{"ticker": "...", "name": "..."}}], "functional": [{{"ticker": "...", "name": "..."}}], "correlated": [{{"ticker": "...", "name": "..."}}]}}"""

class PeerDeriver:
    def __init__(self, boto3_session: boto3.Session, db: DatabentoClient,
                 rs=None, model_id: str = MODEL_ID, max_attempts: int = 3):
        self.bedrock = boto3_session.client("bedrock-runtime")
        self.db = db
        self.rs = rs                      # RedshiftClient for security-type filtering
        self.model_id = model_id
        self.max_attempts = max_attempts
        # Web-grounded resolver for dead/renamed tickers. The base model cannot
        # know post-cutoff renames (BK->BNY) and Bedrock has no managed web tool,
        # so the resolver fetches search results itself and the LLM only EXTRACTS
        # from them — never answers a rename from memory.
        self.resolver = TickerResolver(self.bedrock, model_id, max_attempts)

    def _invoke(self, symbol: str, name: str) -> Optional[str]:
        prompt = _PROMPT.format(symbol=symbol, name=name or symbol)
        delay = 1.0
        for attempt in range(1, self.max_attempts + 1):
            try:
                resp = self.bedrock.converse(
                    modelId=self.model_id,
                    system=[{"text": _SYSTEM}],
                    messages=[{"role": "user", "content": [{"text": prompt}]}],
                    inferenceConfig={"maxTokens": 800, "temperature": 0.0},
                )
                return " ".join(b.get("text", "") for b in
                                resp["output"]["message"]["content"] if b.get("text"))
            except Exception as e:
                code = type(e).__name__
                if code in _RETRYABLE and attempt < self.max_attempts:
                    time.sleep(delay); delay = min(delay * 2, 30)
                    continue
                logger.error("bedrock peer derivation failed for %s: %s", symbol, e)
                return None
        return None

    @staticmethod
    def _parse(text: str) -> Dict[str, List[dict]]:
        """Parse the LLM response into {relation: [{ticker, name}, ...]}.

        Tolerates both the new object form ({"ticker","name"}) and the legacy
        bare-string form (name then defaults to "")."""
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if not m:
            return {}
        try:
            obj = json.loads(m.group(0))
        except json.JSONDecodeError:
            return {}
        out = {}
        for bucket in ("pure_play", "functional", "correlated"):
            vals = obj.get(bucket, [])
            if not isinstance(vals, list):
                continue
            items = []
            for v in vals:
                if isinstance(v, dict):
                    t = str(v.get("ticker", "")).strip().upper()
                    nm = str(v.get("name", "")).strip()
                elif isinstance(v, str):
                    t, nm = v.strip().upper(), ""
                else:
                    continue
                if t:
                    items.append({"ticker": t, "name": nm})
            out[bucket.replace("_", " ")] = items
        return out

    def _llm_rename(self, name: str, old_ticker: str, asof: str) -> Optional[str]:
        """Resolve a company's CURRENT US ticker via WEB SEARCH + LLM extraction
        (see TickerResolver). The LLM reads fetched search text, not memory, so it
        can recover post-cutoff renames (BK->BNY). Returns the ticker or None."""
        try:
            return self.resolver.resolve(name, old_ticker, asof).get("current_ticker")
        except Exception as e:
            logger.warning("rename resolve failed for %s (%s): %s", old_ticker, name, e)
            return None

    def _resolve_dead_ticker(self, ticker: str, name: str, start: str, end: str,
                             basket: set) -> Optional[str]:
        """A ticker is inactive in Databento for the window. Try to map it to its
        current ticker via the company NAME, then VERIFY the proposal before
        accepting — the LLM is a lead generator, data is the arbiter.

        A proposed new ticker is accepted only if ALL hold:
          1. it is ACTIVE in Databento for the window (has real bars), and
          2. it is not already in the basket (guards the BK->BLK collision), and
          3. FactSet confirms it is an in-universe US common share.
        Returns the verified new ticker, or None (leave as-is / let it drop)."""
        if not name:
            return None                       # no anchor to resolve on
        cand = self._llm_rename(name, ticker, asof=end)
        if not cand or cand == ticker or cand in basket:
            return None
        if self.db.is_active(cand, start, end) is not True:
            logger.info("rename %s->%s rejected: proposed ticker not active", ticker, cand)
            return None
        if self._excluded_symbols([cand]):     # positively out-of-universe in FactSet
            logger.info("rename %s->%s rejected: not in-universe (FactSet)", ticker, cand)
            return None
        logger.info("rename verified %s (%s) -> %s", ticker, name, cand)
        return cand

    def _excluded_symbols(self, symbols: List[str]) -> set:
        """Bare peer symbols that POSITIVELY resolve OUTSIDE the US common-equity
        universe (see universe.py): ADRs, ETFs, funds, preferreds — anything not
        US-listed single-class common stock. One batched Redshift query via the
        shared universe definition (same rule reporters use). Returns empty if no
        rs handle (filter is a no-op) or on any error (fail-open).

        Fail-open is deliberate: a symbol not returned by the query (e.g. a
        rename Databento resolves but FactSet lags) must NOT be dropped — the
        old date-sensitive validation-drop regression. We only drop symbols we
        POSITIVELY resolve as out-of-universe."""
        if not self.rs or not symbols:
            return set()
        sql = peer_universe_sql(symbols)
        excluded = set()
        try:
            for r in self.rs.fetch_all(sql, timeout_s=120):
                if not in_universe(r.get("listing_exchange"),
                                   r.get("reg_security_type"),
                                   r.get("prim_security_type")):
                    tr = r.get("ticker_region") or ""
                    if tr.endswith("-US"):
                        excluded.add(tr[:-3])
        except Exception as e:
            logger.warning("peer universe filter failed (keeping all): %s", e)
            return set()
        return excluded

    def derive(self, symbol: str, name: str, window=None) -> dict:
        """Return {peers: {rel: [syms]}, names: {sym: name}, dropped, remapped, ...}.

        The LLM returns each peer as {ticker, name}. Filtering, in order:
          1. Universe filter (FactSet): drop peers positively OUT-OF-UNIVERSE
             (ETFs/ADRs/foreign/preferred) via the shared universe.py definition.
             Date-independent, so no market-data pre-validation regression.
          2. Dead-ticker rename (only if `window` given): a kept ticker that is
             INACTIVE in Databento for the window is re-resolved to its current
             ticker via its company NAME (the anchor), verified before remap
             (active + not-in-basket + in-universe). This heals renames like
             BK->BNY without trusting the LLM blindly.

        `window` = (start, end) for the event's [-2,+5] span; when omitted the
        rename step is skipped (pure universe filtering, backward compatible)."""
        raw = self._invoke(symbol, name)
        parsed = self._parse(raw) if raw else {}

        # flatten to candidates (dedup by ticker, excluding the reporter), keep names
        names: Dict[str, str] = {}
        candidates: List[str] = []
        seen = {symbol}
        for items in parsed.values():
            for it in items:
                t = it["ticker"]
                if t and t not in seen:
                    seen.add(t)
                    candidates.append(t)
                    if it.get("name"):
                        names[t] = it["name"]

        excluded = self._excluded_symbols(candidates)
        if excluded:
            logger.info("dropped %d out-of-universe peers for %s: %s",
                        len(excluded), symbol, sorted(excluded))

        # kept in-universe tickers, per relation
        peers: Dict[str, List[str]] = {}
        kept_seen = {symbol}
        for rel, items in parsed.items():
            keep = []
            for it in items:
                t = it["ticker"]
                if t in kept_seen or t in excluded:
                    continue
                kept_seen.add(t)
                keep.append(t)
            if keep:
                peers[rel] = keep

        # dead-ticker rename pass (name-anchored, verified) over the event window
        remapped = {}
        if window:
            start, end = window
            in_basket = {symbol} | {t for v in peers.values() for t in v}
            for rel, syms in peers.items():
                new = []
                for t in syms:
                    if self.db.is_active(t, start, end) is False:   # dead/renamed
                        nt = self._resolve_dead_ticker(t, names.get(t, ""),
                                                       start, end, in_basket)
                        if nt:
                            remapped[t] = nt
                            in_basket.add(nt)
                            names[nt] = names.get(t, "")
                            new.append(nt)
                            continue     # replace dead ticker with resolved one
                    new.append(t)        # active, or unresolved -> keep as-is
                peers[rel] = new

        return {"symbol": symbol, "name": name, "peers": peers, "names": names,
                "dropped": sorted(excluded), "remapped": remapped,
                "model_id": self.model_id,
                "derived_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
