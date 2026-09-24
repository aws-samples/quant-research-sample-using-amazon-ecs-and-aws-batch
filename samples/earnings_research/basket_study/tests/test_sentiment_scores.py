import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sentiment_scores import load_model_list, scores_by_event

CFG = Path(__file__).resolve().parent.parent / "configs" / "sentiment_models_90pct.csv"


def _frame(rows):
    return pd.DataFrame(rows, columns=["event_id", "sentiment_score", "error_class"])


class TestModelList:
    def test_loads_47_models(self):
        models = load_model_list(str(CFG))
        assert len(models) == 47
        assert "claude-haiku-4-5" in models
        assert "baseline" not in models          # baseline is not a model


class TestScoresByEvent:
    def test_scored_row_maps_to_float(self):
        d = scores_by_event(_frame([(101, 4.0, None)]))
        assert d[101] == 4.0

    def test_error_row_maps_to_none(self):
        # error rows carry NULL sentiment_score (catalog invariant)
        d = scores_by_event(_frame([(102, None, "ambiguous_score")]))
        assert d[102] is None

    def test_all_407_events_present_even_when_null(self):
        d = scores_by_event(_frame([(101, 4.0, None), (102, None, "x")]))
        assert set(d) == {101, 102}
