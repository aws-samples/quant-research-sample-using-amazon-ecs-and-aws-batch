"""Sentiment-conditioned direction (design spec 2026-08-17, §3).

Wraps fade_the_gap's Signal: at |score| >= band the fade direction is
NEGATED (momentum — both legs flip via the existing -direction hedge
convention downstream). The wrapper never skips an event and never
un-skips one; all skip decisions live upstream. fade_the_gap itself is
untouched — composition, not modification.
"""

from dataclasses import dataclass
from typing import Optional

from rules.direction import Signal


@dataclass
class SentimentSignal:
    gap: float
    direction: int              # possibly negated fade direction
    reason: str                 # fade's reason, unchanged ('long'/'short'/'neutral')
    direction_rule: str         # 'fade' | 'fade_no_score' | 'momentum_flip'
    sentiment_score: Optional[float]


def apply_sentiment(sig: Optional[Signal], score: Optional[float],
                    band: Optional[int]) -> Optional[SentimentSignal]:
    """None passes through (upstream data-quality skip). Neutral passes
    through (upstream band skip). Only a tradable direction can flip."""
    if sig is None:
        return None
    base = dict(gap=sig.gap, direction=sig.direction, reason=sig.reason,
                sentiment_score=score)
    if band is None:
        return SentimentSignal(**base, direction_rule="fade")
    if sig.direction == 0:
        return SentimentSignal(**base, direction_rule="fade")
    if score is None:
        return SentimentSignal(**base, direction_rule="fade_no_score")
    if abs(score) >= band:
        base["direction"] = -sig.direction
        return SentimentSignal(**base, direction_rule="momentum_flip")
    return SentimentSignal(**base, direction_rule="fade")
