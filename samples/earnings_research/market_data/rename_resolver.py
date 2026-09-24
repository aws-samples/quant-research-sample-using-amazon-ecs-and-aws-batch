"""Ticker-rename resolution for partially available symbols.

The VSXY case (2026-06): FactSet hands the manifest the CURRENT ticker, but
the company renamed (VSCO -> VSXY) inside the event window, so the fetched
data is missing its HEAD sessions — exactly the sessions the signal rules
need (prev close, pre-event fit). The datashare has no ticker-history table
and Databento's corporate-actions dataset is a separate subscription, so:

  1. detect the head gap (first real bar after the window start),
  2. ask Claude (Bedrock Converse — the same model/client pattern as
     peer_deriver) what the previous ticker was,
  3. VERIFY the proposal against Databento's FREE record_count: the old
     ticker must actually have bars in the missing head window. An LLM
     guess never reaches a paid fetch unverified.
  4. fetch the old ticker's head bars and relabel them to the current
     symbol, so downstream sees one continuous series.

Every failure mode (LLM error, garbage output, unverified ticker, genuinely
new listing) fails open to None — the panel keeps its partial head, which
is exactly the pre-existing behavior.
"""

import json
import logging
from typing import Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"

# Deliberately asks for the HISTORICAL ticker, not "do you know about this
# rename": renames near the data boundary postdate the model's knowledge
# cutoff, so a rename-framed question gets a null out of epistemic caution
# even when the model knows the old symbol perfectly well (observed with
# VSXY: reason text said "historically traded under VSCO" while answering
# null). The old ticker IS in training data; the rename itself is proven by
# the Databento verification step, not by the model.
_PROMPT = """What ticker symbol has the US-listed company "{name}" historically traded under on NYSE/Nasdaq?

Context: I have market data for this company under the symbol {symbol} starting {data_start}, and I need to know what symbol its shares traded under BEFORE that date. If the company has used multiple tickers over time, give the most recent one before {symbol}.

Respond with ONLY a JSON object: {{"previous_ticker": "...", "confidence": "high|medium|low", "reason": "..."}}
If you have no knowledge of this company or its ticker history, or its only known ticker is {symbol}, respond {{"previous_ticker": null}}."""


def head_gap(first_day: Optional[str],
             window: Tuple[str, str]) -> Optional[Tuple[str, str]]:
    """(gap_start, gap_end_exclusive) when a symbol's data starts AFTER its
    window start — the rename signature. None when data reaches the window
    start, or when there is no data at all (that's the zero-data class, not
    the partial class; acceptance rules handle it separately)."""
    if first_day is None:
        return None
    start, _ = window
    if first_day <= start:
        return None
    return start, first_day


class RenameResolver:
    """LLM-proposed, Databento-verified previous-ticker lookup."""

    def __init__(self, bedrock, db, model_id: str = MODEL_ID):
        self._bedrock = bedrock
        self._db = db
        self._model_id = model_id

    def resolve(self, symbol: str, name: str,
                gap_start: str, gap_end: str) -> Optional[str]:
        """Previous ticker for `symbol`, or None. A proposal is returned only
        if Databento confirms it has bars inside the missing head window."""
        proposal = self._propose(symbol, name, gap_end)
        if not proposal:
            return None
        try:
            n = self._db.record_count(proposal, gap_start, gap_end)
        except Exception as e:
            logger.warning("rename verification failed for %s->%s: %s",
                           symbol, proposal, e)
            return None
        if not n:
            logger.info("rename proposal %s->%s REJECTED: no bars in "
                        "missing head %s..%s", symbol, proposal,
                        gap_start, gap_end)
            return None
        logger.info("rename verified: %s previously traded as %s "
                    "(%d bars in %s..%s)", symbol, proposal, n,
                    gap_start, gap_end)
        return proposal

    def fetch_head(self, symbol: str, name: str,
                   gap_start: str, gap_end: str) -> Optional[pd.DataFrame]:
        """Old-ticker bars for the missing head, RELABELED to `symbol` so the
        panel is one continuous series. None when no verified rename."""
        old = self.resolve(symbol, name, gap_start, gap_end)
        if old is None:
            return None
        df = self._db.get_range(old, gap_start, gap_end)
        if df is None or df.empty:
            return None
        df = df.copy()
        df["symbol"] = symbol
        logger.info("fetched %d head bars for %s from previous ticker %s",
                    len(df), symbol, old)
        return df

    def _propose(self, symbol: str, name: str,
                 data_start: str) -> Optional[str]:
        """Claude's previous-ticker proposal (UNVERIFIED), or None."""
        prompt = _PROMPT.format(name=name, symbol=symbol,
                                data_start=data_start)
        try:
            resp = self._bedrock.converse(
                modelId=self._model_id,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": 300},
            )
            text = resp["output"]["message"]["content"][0]["text"]
            ans = json.loads(text[text.index("{"):text.rindex("}") + 1])
        except Exception as e:
            logger.warning("rename proposal failed for %s: %s", symbol, e)
            return None
        prev = ans.get("previous_ticker")
        if not prev or not isinstance(prev, str):
            return None
        prev = prev.strip().upper()
        return prev if prev and prev != symbol else None
