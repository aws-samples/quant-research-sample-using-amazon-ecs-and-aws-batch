"""Pins every case row of spec §3: the wrapper changes direction only —
never skips, never un-skips, never touches the gap."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rules.direction import Signal
from rules.sentiment_direction import apply_sentiment

SHORT = Signal(gap=0.02, direction=-1, reason="short")
LONG = Signal(gap=-0.02, direction=+1, reason="long")
NEUTRAL = Signal(gap=0.001, direction=0, reason="neutral")


class TestBaselinePassthrough:
    def test_band_none_keeps_fade_direction(self):
        out = apply_sentiment(SHORT, score=5.0, band=None)
        assert out.direction == -1 and out.direction_rule == "fade"

    def test_band_none_records_score_even_when_unused(self):
        out = apply_sentiment(SHORT, score=3.0, band=None)
        assert out.sentiment_score == 3.0


class TestNoSkipChanges:
    def test_upstream_none_stays_none(self):
        # fade returned None (missing inputs) -> wrapper passes None through
        assert apply_sentiment(None, score=5.0, band=4) is None

    def test_neutral_stays_neutral_even_at_extreme_score(self):
        out = apply_sentiment(NEUTRAL, score=5.0, band=2)
        assert out.direction == 0 and out.reason == "neutral"


class TestNullScore:
    def test_null_score_is_fade_no_score(self):
        out = apply_sentiment(SHORT, score=None, band=4)
        assert out.direction == -1 and out.direction_rule == "fade_no_score"


class TestFlip:
    def test_below_band_keeps_fade(self):
        out = apply_sentiment(SHORT, score=3.0, band=4)
        assert out.direction == -1 and out.direction_rule == "fade"

    def test_at_band_edge_flips(self):          # inclusive: |score| == band
        out = apply_sentiment(SHORT, score=4.0, band=4)
        assert out.direction == +1 and out.direction_rule == "momentum_flip"

    def test_above_band_flips(self):
        out = apply_sentiment(LONG, score=5.0, band=4)
        assert out.direction == -1 and out.direction_rule == "momentum_flip"

    def test_negative_extreme_flips_regardless_of_gap_sign(self):
        # magnitude-only rule (spec §3.4): gap-up + score -4 still flips
        out = apply_sentiment(SHORT, score=-4.0, band=4)
        assert out.direction == +1 and out.direction_rule == "momentum_flip"

    def test_gap_is_never_modified(self):
        out = apply_sentiment(SHORT, score=5.0, band=2)
        assert out.gap == SHORT.gap
