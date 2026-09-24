"""Study registry: which scores job, universe, results prefix and signs table an
ESA run uses. Selected by the ESA_STUDY environment variable (default 'xle', the
frozen 2026-08 study — its values are verbatim the constants that used to live in
scores.py / evaluate.py / signs.py / validate.py, so behaviour is unchanged).

Batch children inherit the study from the manifest (evaluate.cmd_eval_child sets
ESA_STUDY from m['study'] before loading scores), so the operator only sets the
variable on the machine that runs `plan` and the analysis CLIs.
"""
import json
import os
from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class Study:
    key: str
    scores_job: str
    universe_file: str          # under <diagnostics_bucket>/diagnostics/earnings_universe/
    results_prefix: str
    signs_sha_file: str
    master_seed: int
    models_csv: str = "sentiment_models_90pct.csv"
    # Arm S models whose scores live in a DIFFERENT scores job. Key = the
    # sentiment_model name used for the shard and in every result table; value
    # says where the scores are. Empty for xle/large, so nothing there moves.
    extra_arms: Mapping[str, Mapping[str, str]] = field(default_factory=dict)

    @property
    def universe_uri(self) -> str:
        import settings
        return (f"s3://{settings.get('s3', 'diagnostics_bucket')}/diagnostics/"
                f"earnings_universe/{self.universe_file}")

    @property
    def signs_key(self) -> str:
        return f"{self.results_prefix}/signs.parquet"


_STUDIES = {
    "xle": Study(
        key="xle",
        scores_job="xle_earnings_full_universe_2026_08_16",
        universe_file="xle_412.json",
        results_prefix="earnings-basket-study/results-sentiment-analysis",
        signs_sha_file="signs.sha256",
        master_seed=20260821,
    ),
    "large": Study(
        key="large",
        scores_job="large_earnings_full_universe_2026_09",
        universe_file="large_tier_meta.json",
        results_prefix="earnings-basket-study/results-sentiment-analysis-large",
        signs_sha_file="signs_large.sha256",
        master_seed=20260910,
    ),
    "large_v2": Study(
        key="large_v2",
        scores_job="large_earnings_v2_2026_09",
        universe_file="large_tier_v2_meta.json",
        results_prefix="earnings-basket-study/results-sentiment-analysis-large-v2",
        signs_sha_file="signs_large_v2.sha256",
        master_seed=20260911,
        extra_arms={
            "ministral-3-8b-sft": {"scores_job": "sft_large_v1_traded",
                                   "model": "ministral-3-8b"},
            "llama-3-1-8b-sft": {"scores_job": "sft_large_v1_traded",
                                 "model": "llama-3-1-8b"},
            "qwen3-32b-dense-sft": {"scores_job": "sft_large_v1_traded",
                                    "model": "qwen3-32b-dense"},
        },
    ),
}


def get(key: str) -> Study:
    if key not in _STUDIES:
        raise SystemExit(f"FATAL: unknown ESA_STUDY {key!r}; known: {sorted(_STUDIES)}")
    return _STUDIES[key]


def current() -> Study:
    return get(os.environ.get("ESA_STUDY", "xle"))


def arm_s_source(st: Study, model_key: str) -> tuple:
    """(scores model name, scores job) for one Arm S model key."""
    ea = st.extra_arms.get(model_key)
    if not ea:
        return model_key, st.scores_job
    return ea.get("model", model_key), ea.get("scores_job") or st.scores_job


def universe_event_ids(s3io, study: Study) -> list:
    """Sorted scorable event ids. xle_412.json carries events[]; the large-tier
    meta JSON carries scorable_event_ids (the 400 MB transcript parquet is never
    read by ESA)."""
    doc = json.loads(s3io.read_bytes(study.universe_uri))
    if "scorable_event_ids" in doc:
        ids = doc["scorable_event_ids"]
    else:
        ids = [e["event_id"] for e in doc["events"]]
    return sorted({int(i) for i in ids})
