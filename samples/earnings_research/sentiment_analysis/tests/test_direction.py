import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from rules.direction import random_direction, sentiment_direction


class TestSentiment:
    def test_positive_beyond_neutral_is_long(self):
        d = sentiment_direction(3.0, neutral=1.0)
        assert d.direction == +1 and d.source == "sentiment" and d.skip_reason is None

    def test_negative_beyond_neutral_is_short(self):
        d = sentiment_direction(-2.0, neutral=1.0)
        assert d.direction == -1 and d.source == "sentiment" and d.skip_reason is None

    def test_neutral_zone_inclusive_skips(self):
        for s in (-1.0, -0.5, 0.0, 0.5, 1.0):
            d = sentiment_direction(s, neutral=1.0)
            assert d.direction == 0 and d.source == "sentiment" and d.skip_reason == "neutral_score"

    def test_null_score_skips_distinctly(self):
        d = sentiment_direction(None, neutral=1.0)
        assert d.direction == 0 and d.source == "sentiment" and d.skip_reason == "no_score"

    def test_threshold_configurable(self):
        assert sentiment_direction(2.0, neutral=3.0).skip_reason == "neutral_score"
        assert sentiment_direction(4.0, neutral=3.0).direction == +1


class TestRandom:
    def test_signs_pass_through(self):
        assert random_direction(+1).direction == +1
        assert random_direction(-1).direction == -1
        assert random_direction(+1).source == "random"

    @pytest.mark.parametrize("invalid_sign", [0, 2, -2, 5])
    def test_invalid_sign_rejected(self, invalid_sign):
        with pytest.raises(ValueError):
            random_direction(invalid_sign)
