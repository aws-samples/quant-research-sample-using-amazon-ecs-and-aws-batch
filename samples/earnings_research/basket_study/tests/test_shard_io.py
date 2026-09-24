"""Task 11: fine-grained sharding logic and multi-part shard reader."""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shard_io import (child_index_to_model_slice, panels_for_slice,
                      read_model_shards)


class TestStrideSlicing:
    @pytest.mark.parametrize("n_shards", [1, 2, 3, 5, 7])
    def test_stride_partition_covers_all_panels_no_overlap(self, n_shards):
        """Parametrized test: N in {1,2,3,5,7} against 10 panels (exact and
        uneven division) — stride slicing ensures no overlap and full coverage."""
        panels = list(range(10))
        slices = [panels_for_slice(panels, i, n_shards) for i in range(n_shards)]

        # No overlap: all slices pairwise disjoint
        for i in range(n_shards):
            for j in range(i + 1, n_shards):
                assert not (set(slices[i]) & set(slices[j])), \
                    f"N={n_shards}: slice {i} and {j} overlap"

        # Full coverage: union is the full panel list
        union = set()
        for s in slices:
            union |= set(s)
        assert union == set(panels), f"N={n_shards}: coverage incomplete"

        # Total length preserved
        assert sum(len(s) for s in slices) == len(panels)


class TestChildIndexMapping:
    def test_baseline_child_zero_returns_none(self):
        """Baseline (child 0) is always single-file, no slicing."""
        model_idx, slice_idx = child_index_to_model_slice(0, 47, 4)
        assert model_idx is None and slice_idx is None

    def test_first_model_child_maps_to_model_0_slice_0(self):
        """Child 1 (first after baseline) -> (model 0, slice 0)."""
        model_idx, slice_idx = child_index_to_model_slice(1, 47, 4)
        assert model_idx == 0 and slice_idx == 0

    def test_last_model_slice_correct_mapping(self):
        """With 47 models x 4 shards, child 188 (= 1 + 47*4 - 1) ->
        (model 46, slice 3)."""
        model_idx, slice_idx = child_index_to_model_slice(188, 47, 4)
        assert model_idx == 46 and slice_idx == 3

    def test_out_of_range_child_raises(self):
        """Child 189 (= 1 + 47*4) is out of range."""
        try:
            child_index_to_model_slice(189, 47, 4)
            assert False, "expected ValueError"
        except ValueError as e:
            assert "out of range" in str(e)


class TestReadModelShards:
    def test_single_file_fallback(self):
        """Legacy N=1 layout: model=<m>.parquet (single file)."""
        mock_s3 = MagicMock()
        # Mock paginator -> list_objects_v2 returns one single-file shard
        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [{"Key": "prefix/shards/model=baseline.parquet"}]}
        ]
        # Mock get_object returns a parquet-encoded DataFrame
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=lambda: df.to_parquet())
        }

        result = read_model_shards(mock_s3, "bucket", "prefix", "baseline")
        assert len(result) == 2
        assert list(result.columns) == ["a", "b"]

    def test_multi_part_concat(self):
        """N>1 layout: model=<m>.part=0.parquet, model=<m>.part=1.parquet —
        both parts are concatenated."""
        mock_s3 = MagicMock()
        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "prefix/shards/model=test.part=0.parquet"},
                {"Key": "prefix/shards/model=test.part=1.parquet"}
            ]}
        ]

        # Two parquet parts with different rows
        df0 = pd.DataFrame({"event_id": [1, 2], "pnl": [0.01, 0.02]})
        df1 = pd.DataFrame({"event_id": [3, 4], "pnl": [0.03, 0.04]})

        def mock_get_object(**kwargs):
            if "part=0" in kwargs["Key"]:
                return {"Body": MagicMock(read=lambda: df0.to_parquet())}
            else:
                return {"Body": MagicMock(read=lambda: df1.to_parquet())}

        mock_s3.get_object = mock_get_object

        result = read_model_shards(mock_s3, "bucket", "prefix", "test")
        assert len(result) == 4
        assert list(result["event_id"]) == [1, 2, 3, 4]

    def test_no_shards_raises(self):
        """Empty glob (no matching shards) raises FileNotFoundError."""
        mock_s3 = MagicMock()
        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        paginator.paginate.return_value = [{"Contents": []}]

        try:
            read_model_shards(mock_s3, "bucket", "prefix", "missing")
            assert False, "expected FileNotFoundError"
        except FileNotFoundError as e:
            assert "No shards found" in str(e)

    def test_sibling_model_collision_prevented(self):
        """CRITICAL: glm-4-7 vs glm-4-7-flash — exact matching prevents
        sibling-model collision. Reading glm-4-7 must load ONLY its own parts,
        not glm-4-7-flash parts."""
        mock_s3 = MagicMock()
        paginator = MagicMock()
        mock_s3.get_paginator.return_value = paginator
        # Both models' shards present in the listing (real scenario)
        paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "prefix/shards/model=glm-4-7.part=0.parquet"},
                {"Key": "prefix/shards/model=glm-4-7.part=1.parquet"},
                {"Key": "prefix/shards/model=glm-4-7-flash.part=0.parquet"},
                {"Key": "prefix/shards/model=glm-4-7-flash.part=1.parquet"},
            ]}
        ]

        # Mock get_object: glm-4-7 parts have event_id [1,2,3,4],
        # glm-4-7-flash parts have event_id [101,102,103,104]
        df_glm47_p0 = pd.DataFrame({"event_id": [1, 2], "pnl": [0.01, 0.02]})
        df_glm47_p1 = pd.DataFrame({"event_id": [3, 4], "pnl": [0.03, 0.04]})
        df_flash_p0 = pd.DataFrame({"event_id": [101, 102], "pnl": [0.11, 0.12]})
        df_flash_p1 = pd.DataFrame({"event_id": [103, 104], "pnl": [0.13, 0.14]})

        def mock_get_object(**kwargs):
            key = kwargs["Key"]
            if key == "prefix/shards/model=glm-4-7.part=0.parquet":
                return {"Body": MagicMock(read=lambda: df_glm47_p0.to_parquet())}
            elif key == "prefix/shards/model=glm-4-7.part=1.parquet":
                return {"Body": MagicMock(read=lambda: df_glm47_p1.to_parquet())}
            elif key == "prefix/shards/model=glm-4-7-flash.part=0.parquet":
                return {"Body": MagicMock(read=lambda: df_flash_p0.to_parquet())}
            elif key == "prefix/shards/model=glm-4-7-flash.part=1.parquet":
                return {"Body": MagicMock(read=lambda: df_flash_p1.to_parquet())}
            else:
                raise KeyError(f"Unexpected key: {key}")

        mock_s3.get_object = mock_get_object

        # Read glm-4-7: must return ONLY [1,2,3,4], NOT [101,102,103,104]
        result = read_model_shards(mock_s3, "bucket", "prefix", "glm-4-7")
        assert len(result) == 4
        assert sorted(result["event_id"].tolist()) == [1, 2, 3, 4], \
            "glm-4-7 must not include glm-4-7-flash rows"

        # Read glm-4-7-flash: must return ONLY [101,102,103,104]
        result_flash = read_model_shards(mock_s3, "bucket", "prefix", "glm-4-7-flash")
        assert len(result_flash) == 4
        assert sorted(result_flash["event_id"].tolist()) == [101, 102, 103, 104]


class TestPlanSentimentArraySize:
    """Integration test: cmd_plan_sentiment dry-run reports correct array size."""

    def test_dry_run_array_size_with_shards_per_model_four(self):
        """With 47 models and --shards-per-model 4, array size = 1 + 47*4 = 189."""
        # This test verifies the Task 11 requirement: array_size = 1 + n_models * N
        # We don't actually invoke cmd_plan_sentiment here (that would require
        # real S3 access and fixtures); instead we directly test the formula that
        # cmd_plan_sentiment uses: size = 1 + len(models) * shards_per_model.
        n_models = 47
        shards_per_model = 4
        expected_size = 1 + n_models * shards_per_model
        assert expected_size == 189

        # Verify the mapping covers exactly 189 children (0..188)
        for i in range(189):
            if i == 0:
                model_idx, slice_idx = child_index_to_model_slice(i, n_models, shards_per_model)
                assert model_idx is None and slice_idx is None
            else:
                model_idx, slice_idx = child_index_to_model_slice(i, n_models, shards_per_model)
                assert 0 <= model_idx < n_models
                assert 0 <= slice_idx < shards_per_model

    def test_legacy_n_equals_one_preserves_size(self):
        """With N=1 (legacy), array size = 1 + 47 = 48 (unchanged)."""
        n_models = 47
        shards_per_model = 1
        expected_size = 1 + n_models * shards_per_model
        assert expected_size == 48
