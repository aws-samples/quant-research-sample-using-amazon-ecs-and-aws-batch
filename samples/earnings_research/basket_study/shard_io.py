"""Multi-part model shard I/O — Task 11 fine-grained sharding support.

When --shards-per-model N > 1, eval-model writes model=<m>.part=<i>.parquet
per slice. Consumers glob model=<m>* and concat. N=1 keeps the legacy
single-file model=<m>.parquet (byte-identical, so Loop-2 outputs stay valid).
"""

import io
from typing import Optional

import pandas as pd


def read_model_shards(s3_client, bucket: str, prefix: str, model_name: str,
                      profile: Optional[str] = None) -> pd.DataFrame:
    """Glob model=<model_name>* under s3://<bucket>/<prefix>/shards/ and
    concat all parts into one DataFrame. Supports both legacy single-file
    (model=<m>.parquet) and multi-part (model=<m>.part=<i>.parquet) layouts.

    Args:
        s3_client: boto3 S3 client (for listing)
        bucket: S3 bucket name
        prefix: S3 prefix (e.g., "earnings-basket-study/results-xle-sentiment")
        model_name: model name to glob (e.g., "us.anthropic.claude-3-5-sonnet-20241022-v2:0")
        profile: AWS profile for reading (passed through to S3IO if needed)

    Returns:
        Concatenated DataFrame from all matching shards
    """
    shard_prefix = f"{prefix}/shards/model={model_name}"
    parts = []

    # List all objects matching the model prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=shard_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Exact match: model=<m>.parquet OR model=<m>.part=<i>.parquet
            # (prevents sibling-model collision: glm-4-7 vs glm-4-7-flash)
            if key == f"{shard_prefix}.parquet" or \
               (key.startswith(f"{shard_prefix}.part=") and key.endswith(".parquet")):
                body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
                parts.append(pd.read_parquet(io.BytesIO(body)))

    if not parts:
        raise FileNotFoundError(f"No shards found for model={model_name} "
                              f"in s3://{bucket}/{prefix}/shards/")

    return pd.concat(parts, ignore_index=True)


def child_index_to_model_slice(child_index: int, n_models: int, shards_per_model: int):
    """Map array child index (1-based after baseline) to (model_idx, slice_idx).

    Child 0: baseline (always single-file, no slicing)
    Child i >= 1: model_idx, slice_idx = divmod(i - 1, shards_per_model)

    Args:
        child_index: AWS_BATCH_JOB_ARRAY_INDEX (0 = baseline, 1+ = models)
        n_models: total number of models
        shards_per_model: N (sharding factor)

    Returns:
        (model_idx, slice_idx) where model_idx in [0, n_models) and
        slice_idx in [0, shards_per_model)
    """
    if child_index == 0:
        return None, None  # baseline (no slicing)

    model_idx, slice_idx = divmod(child_index - 1, shards_per_model)

    if model_idx >= n_models:
        raise ValueError(f"child_index {child_index} out of range: "
                        f"{n_models} models x {shards_per_model} shards = "
                        f"{n_models * shards_per_model} model children + 1 baseline")

    return model_idx, slice_idx


def panels_for_slice(all_panels: list, slice_idx: int, shards_per_model: int) -> list:
    """Stride slicing: returns panels[slice_idx::shards_per_model].

    With N shards, slices [0::N], [1::N], ..., [(N-1)::N] partition the full
    panel list with no overlap and no gaps.
    """
    return all_panels[slice_idx::shards_per_model]
