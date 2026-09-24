import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from reconcile_v2_shards import reconcile  # noqa: E402


def _ref():
    return pd.DataFrame({"event_id": [1, 1, 2], "construction": ["a", "b", "a"], "universe": ["u", "u", "u"],
                         "status": ["ok", "skip", "ok"], "skip_reason": [None, "x", None],
                         "direction": [1, 1, -1], "pnl": [0.01, np.nan, -0.02], "pnl_0931": [0.001, np.nan, 0.0]})


def test_identical_plus_new_event_is_ok():
    cand = pd.concat([_ref(), pd.DataFrame({"event_id": [3], "construction": ["a"], "universe": ["u"],
                                            "status": ["ok"], "skip_reason": [None], "direction": [1],
                                            "pnl": [0.5], "pnl_0931": [0.1]})])
    cand["shares_per_dollar"] = 1.0
    s = reconcile(_ref(), cand)
    assert s["ok"] and s["cand_only_events"] == 1 and s["ref_only_rows"] == 0
    assert all(v == 0.0 for v in s["pnl_max_abs_diff"].values())


def test_pnl_drift_fails():
    cand = _ref(); cand.loc[0, "pnl"] += 1e-6
    s = reconcile(_ref(), cand)
    assert not s["ok"] and s["pnl_rows_over_atol"] == 1


def test_missing_ref_row_or_status_change_fails():
    s = reconcile(_ref(), _ref().iloc[1:])
    assert not s["ok"] and s["ref_only_rows"] == 1
    cand = _ref(); cand.loc[2, "status"] = "skip"
    assert reconcile(_ref(), cand)["cat_mismatch"]["status"] == 1


def test_allow_events_whitelists_known_refresh_only():
    cand = _ref(); cand.loc[0, "pnl"] += 1e-3          # event 1 differs
    s = reconcile(_ref(), cand, allow_events=[1])
    assert s["ok"] and s["allowed_diff_rows"] == 1 and s["diff_events"] == [1]
    s2 = reconcile(_ref(), cand, allow_events=[2])     # wrong event whitelisted -> still fails
    assert not s2["ok"]
