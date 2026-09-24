"""Load per-model LLM sentiment scores for the direction study.

Source (catalog: earnings_sentiment_scores): one parquet per model with
exactly 407 rows; sentiment_score is NULL iff error_class is set. NULL
scores stay in the dict as None -> the wrapper tags them fade_no_score.
"""

import csv
import io
from typing import Dict, List, Optional

import pandas as pd

import settings  # noqa: E402


def _bucket() -> str:
    return settings.get("s3", "data_bucket")
SCORES_PREFIX = "earnings-sentiment/output/job=xle_earnings_full_universe_2026_08_16"


def load_model_list(csv_path: str) -> List[str]:
    with open(csv_path, newline="") as f:
        return [r["model_name"] for r in csv.DictReader(f)]


def scores_by_event(df: pd.DataFrame) -> Dict[int, Optional[float]]:
    out: Dict[int, Optional[float]] = {}
    for eid, score in zip(df["event_id"], df["sentiment_score"]):
        out[int(eid)] = None if pd.isna(score) else float(score)
    return out


def load_scores(s3io, model_name: str) -> Dict[int, Optional[float]]:
    raw = s3io.read_bytes(
        f"s3://{_bucket()}/{SCORES_PREFIX}/model={model_name}/data.parquet")
    return scores_by_event(pd.read_parquet(io.BytesIO(raw)))
