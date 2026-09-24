"""Fade-the-gap direction rule (README "Rules").

gap = signal_price / previous_close - 1
  gap > +NEUTRAL_BAND -> SHORT reporter (-1)
  gap < -NEUTRAL_BAND -> LONG  reporter (+1)
  |gap| <= NEUTRAL_BAND -> neutral -> skip (0)
Peer hedge always takes -direction.
"""

from dataclasses import dataclass
from typing import Optional

NEUTRAL_BAND = 0.005     # +/-0.5%, README decision 1
_EPS = 1e-12             # float guard so exact band-edge prices stay neutral


@dataclass
class Signal:
    gap: float
    direction: int          # +1 long reporter, -1 short reporter, 0 skip
    reason: str             # 'long' | 'short' | 'neutral'


def fade_the_gap(signal_price: Optional[float], previous_close: Optional[float],
                 band: float = NEUTRAL_BAND) -> Optional[Signal]:
    """None if either input is missing (no pre-market print / no prior close)."""
    if signal_price is None or previous_close is None or previous_close <= 0:
        return None
    gap = signal_price / previous_close - 1.0
    if gap > band + _EPS:
        return Signal(gap=gap, direction=-1, reason="short")
    if gap < -band - _EPS:
        return Signal(gap=gap, direction=+1, reason="long")
    return Signal(gap=gap, direction=0, reason="neutral")