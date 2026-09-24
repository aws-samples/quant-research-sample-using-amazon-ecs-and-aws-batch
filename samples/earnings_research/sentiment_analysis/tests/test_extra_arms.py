import json

import numpy as np
import pandas as pd
import pytest

import evaluate
import paired_tests
import study
import validate


def test_defaults_have_no_extra_arms(monkeypatch):
    for key in ("xle", "large"):
        monkeypatch.setenv("ESA_STUDY", key)
        assert study.current().extra_arms == {}


def test_large_v2_carries_the_three_sft_arms(monkeypatch):
    monkeypatch.setenv("ESA_STUDY", "large_v2")
    st = study.current()
    assert st.scores_job == "large_earnings_v2_2026_09"
    assert sorted(st.extra_arms) == ["llama-3-1-8b-sft", "ministral-3-8b-sft", "qwen3-32b-dense-sft"]
    assert st.extra_arms["ministral-3-8b-sft"] == {"scores_job": "sft_large_v1_traded",
                                                   "model": "ministral-3-8b"}


def test_arm_s_source_resolves_prompted_and_fine_tuned(monkeypatch):
    monkeypatch.setenv("ESA_STUDY", "large_v2")
    st = study.current()
    assert study.arm_s_source(st, "ministral-3-8b") == ("ministral-3-8b", "large_earnings_v2_2026_09")
    assert study.arm_s_source(st, "ministral-3-8b-sft") == ("ministral-3-8b", "sft_large_v1_traded")
    with pytest.raises(Exception):
        st.scores_job = "x"            # still frozen


def test_plan_models_extra_only_full_and_narrowed(monkeypatch):
    monkeypatch.setenv("ESA_STUDY", "large_v2")
    st = study.current()
    extras = evaluate.plan_models(st, extra_arms_only=True)
    assert extras == sorted(st.extra_arms)
    full = evaluate.plan_models(st)
    assert full[-3:] == extras and len(full) == 47 + 3
    assert evaluate.plan_models(st, True, ["ministral-3-8b-sft"]) == ["ministral-3-8b-sft"]
    with pytest.raises(SystemExit):
        evaluate.plan_models(st, True, ["nope"])


def test_resolve_scores_source_from_manifest():
    m = {"scores_job": "large_earnings_v2_2026_09",
         "extra_arms": {"ministral-3-8b-sft": {"scores_job": "sft_large_v1_traded",
                                               "model": "ministral-3-8b"}}}
    assert evaluate.resolve_scores_source(m, "ministral-3-8b-sft") == ("ministral-3-8b", "sft_large_v1_traded")
    assert evaluate.resolve_scores_source(m, "claude-opus-5") == ("claude-opus-5", "large_earnings_v2_2026_09")
    assert evaluate.resolve_scores_source({"scores_job": "j"}, "x") == ("x", "j")


def test_evaluate_main_accepts_the_new_flags_and_still_requires_arm(monkeypatch):
    seen = {}
    monkeypatch.setattr(evaluate, "cmd_plan", lambda a: seen.update(vars(a)) or 0)
    assert evaluate.main(["plan", "--arm", "S", "--extra-arms-only",
                          "--only-model", "a", "--only-model", "b"]) == 0
    assert seen["extra_arms_only"] is True and seen["only_model"] == ["a", "b"]
    with pytest.raises(SystemExit):
        evaluate.main(["plan"])


def test_expected_arm_s_models_adds_only_present_extras():
    got = validate.expected_arm_s_models(["m1", "m2"], {"a-sft": {}, "b-sft": {}}, ["m1", "m2", "b-sft"])
    assert got == ["m1", "m2", "b-sft"]
    assert validate.expected_arm_s_models(["m1"], {}, ["m1"]) == ["m1"]


def test_restrict_events_intersects_both_filters(tmp_path):
    pnl = pd.Series([0.1, 0.2, 0.3, 0.4], index=[1, 2, 3, 4])
    td = pd.Series(["2016-05-02", "2020-05-02", "2025-11-02", "2026-02-02"], index=[1, 2, 3, 4])
    j = tmp_path / "ids.json"
    j.write_text(json.dumps({"event_ids": [2, 3, 4]}))
    out = paired_tests.restrict_events(pnl, str(j), "2025-10-01", trade_days=td)
    assert list(out.index) == [3, 4]
    with pytest.raises(ValueError):
        paired_tests.restrict_events(pnl, None, "2025-10-01")


def test_parse_pair_and_placement():
    assert paired_tests.parse_pair("a-sft:a") == ("a-sft", "a")
    with pytest.raises(ValueError):
        paired_tests.parse_pair("nocolon")
    p = paired_tests.placement(0.03, {i: v for i, v in enumerate([-0.01, 0.0, 0.01, 0.05])})
    assert p["n_seeds_at_or_above"] == 1 and p["p"] == pytest.approx(2 / 5)
    assert p["null_max"] == 0.05


def test_per_year_and_null_band_shapes():
    rng = np.random.default_rng(3)
    idx = np.arange(40)
    td = pd.Series(pd.date_range("2024-01-05", periods=40, freq="20D").astype(str), index=idx)
    A = rng.normal(0.01, 0.02, (40, 1))
    B = rng.normal(0.0, 0.02, (40, 1))
    t = paired_tests.per_year(A, B, td, n_boot=100)
    assert set(t.columns) >= {"year", "n_events", "observed", "p_le_0"}
    assert t["n_events"].sum() == 40
    shards = {s: pd.DataFrame({"event_id": idx, "status": "ok",
                               "pnl": rng.normal(0, 0.02, 40), "direction": 1})
              for s in range(3)}
    nb = paired_tests.null_band(shards, idx[:20])
    assert sorted(nb) == [0, 1, 2] and all(np.isfinite(v) for v in nb.values())
