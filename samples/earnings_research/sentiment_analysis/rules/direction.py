"""Direction rules (design spec §1). No gap, no fade, no RNG here."""
from dataclasses import dataclass
from typing import Optional


@dataclass
class Decision:
    direction: int                  # +1 long reporter / -1 short / 0 skip
    source: str                     # 'sentiment' | 'random'
    skip_reason: Optional[str]      # None | 'neutral_score' | 'no_score'


def sentiment_direction(score: Optional[float], neutral: float = 1.0) -> Decision:
    """score > +neutral -> LONG; score < -neutral -> SHORT;
    |score| <= neutral -> skip; NULL -> skip (distinct reason)."""
    if score is None:
        return Decision(0, "sentiment", "no_score")
    if score > neutral:
        return Decision(+1, "sentiment", None)
    if score < -neutral:
        return Decision(-1, "sentiment", None)
    return Decision(0, "sentiment", "neutral_score")


def random_direction(sign: int) -> Decision:
    """Pre-generated sign from signs.parquet; never computed here."""
    if sign not in (-1, +1):
        raise ValueError(f"sign must be -1 or +1, got {sign!r}")
    return Decision(sign, "random", None)
