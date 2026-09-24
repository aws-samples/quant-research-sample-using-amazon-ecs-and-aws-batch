import json
import pytest
import settings
import study


def test_default_is_xle_and_matches_frozen_constants(monkeypatch):
    monkeypatch.delenv("ESA_STUDY", raising=False)
    s = study.current()
    assert s.key == "xle"
    assert s.scores_job == "xle_earnings_full_universe_2026_08_16"
    assert s.universe_file == "xle_412.json"
    assert s.universe_uri == (f"s3://{settings.get('s3', 'diagnostics_bucket')}"
                              "/diagnostics/earnings_universe/xle_412.json")
    assert s.results_prefix == "earnings-basket-study/results-sentiment-analysis"
    assert s.signs_key == "earnings-basket-study/results-sentiment-analysis/signs.parquet"
    assert s.signs_sha_file == "signs.sha256"
    assert s.master_seed == 20260821


def test_large_study(monkeypatch):
    monkeypatch.setenv("ESA_STUDY", "large")
    s = study.current()
    assert s.scores_job == "large_earnings_full_universe_2026_09"
    assert s.universe_uri.endswith("/large_tier_meta.json")
    assert s.results_prefix == "earnings-basket-study/results-sentiment-analysis-large"
    assert s.signs_key.endswith("results-sentiment-analysis-large/signs.parquet")
    assert s.signs_sha_file == "signs_large.sha256"
    assert s.master_seed == 20260910


def test_large_v2_study(monkeypatch):
    monkeypatch.setenv("ESA_STUDY", "large_v2")
    s = study.current()
    assert s.scores_job == "large_earnings_v2_2026_09"
    assert s.universe_uri.endswith("/large_tier_v2_meta.json")
    assert s.results_prefix == "earnings-basket-study/results-sentiment-analysis-large-v2"
    assert s.signs_key.endswith("results-sentiment-analysis-large-v2/signs.parquet")
    assert s.signs_sha_file == "signs_large_v2.sha256"
    assert s.master_seed == 20260911


def test_unknown_study_dies(monkeypatch):
    monkeypatch.setenv("ESA_STUDY", "nope")
    with pytest.raises(SystemExit):
        study.current()


class _S3:
    def __init__(self, payload):
        self.payload = payload
    def read_bytes(self, uri):
        return json.dumps(self.payload).encode()


def test_universe_event_ids_json_shape():
    ids = study.universe_event_ids(_S3({"events": [{"event_id": 3}, {"event_id": 1}]}), study.get("xle"))
    assert ids == [1, 3]


def test_universe_event_ids_meta_shape():
    ids = study.universe_event_ids(_S3({"scorable_event_ids": [9, 2, 2]}), study.get("large"))
    assert ids == [2, 9]


def test_scores_prefix_follows_study(monkeypatch):
    import scores
    monkeypatch.setenv("ESA_STUDY", "large")
    assert scores.scores_uri("gemma-3-12b-it") == (
        f"s3://{settings.get('s3', 'data_bucket')}/earnings-sentiment/output/"
        "job=large_earnings_full_universe_2026_09/model=gemma-3-12b-it/data.parquet")
    monkeypatch.delenv("ESA_STUDY")
    assert "job=xle_earnings_full_universe_2026_08_16/" in scores.scores_uri("gemma-3-12b-it")


def test_signs_paths_follow_study(monkeypatch):
    import signs
    monkeypatch.setenv("ESA_STUDY", "large")
    uri, meta, sha = signs.paths()
    assert uri.endswith("results-sentiment-analysis-large/signs.parquet")
    assert meta.endswith("results-sentiment-analysis-large/signs.meta.json")
    assert sha.name == "signs_large.sha256"
    monkeypatch.delenv("ESA_STUDY")
    uri, meta, sha = signs.paths()
    assert uri.endswith("results-sentiment-analysis/signs.parquet")
    assert sha.name == "signs.sha256"


def test_generate_frame_uses_study_master_seed(monkeypatch):
    import signs
    a = signs.generate_frame([1, 2, 3], n_seeds=4)              # xle seed 20260821
    monkeypatch.setenv("ESA_STUDY", "large")
    b = signs.generate_frame([1, 2, 3], n_seeds=4)              # large seed 20260910
    assert not a["sign"].equals(b["sign"])
