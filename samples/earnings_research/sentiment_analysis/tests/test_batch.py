"""Tests for the Batch plan/eval-child commands (Task 5)."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def test_plan_rejects_mismatched_seeds(monkeypatch):
    """cmd_plan with --seeds != signs table size should die cleanly."""
    import evaluate
    import signs

    class Args:
        arm = "R"
        seeds = 32  # wrong: signs table is 64
        neutral = 1.0
        dry_run = True
        profile = None

    class FakeS3IO:
        def read_bytes(self, uri):
            if "signs.parquet" in uri:
                # Return minimal valid parquet bytes (won't be used due to guard)
                raise NotImplementedError("Guard should fire before this")
            raise NotImplementedError(f"Unexpected URI: {uri}")

    # Monkeypatch load_signs to succeed (so we reach the guard)
    def fake_load_signs(s3io):
        return {(1, 0): 1, (1, 1): -1}  # dummy table
    monkeypatch.setattr(signs, "load_signs", fake_load_signs)

    # Monkeypatch paths() to return a fake SHA file
    dummy_sha = Path(__file__).parent / "fixtures" / "dummy_sha.txt"
    dummy_sha.parent.mkdir(exist_ok=True)
    dummy_sha.write_text("abc123\n")
    def fake_paths():
        return "s3://fake/signs.parquet", "s3://fake/signs.meta.json", dummy_sha
    monkeypatch.setattr(signs, "paths", fake_paths)

    with pytest.raises(SystemExit, match="FATAL.*signs table size.*64"):
        evaluate.cmd_plan(Args())


def test_plan_accepts_correct_seeds(monkeypatch):
    """cmd_plan with --seeds == 64 should pass the guard."""
    import evaluate
    import signs
    import json

    class Args:
        arm = "R"
        seeds = 64
        neutral = 1.0
        dry_run = True
        profile = None

    class FakeS3IO:
        def __init__(self):
            self.written_manifest = None

        def read_bytes(self, uri):
            if uri.endswith(".json"):
                return json.dumps({"events": []}).encode()
            raise NotImplementedError(f"Unexpected URI: {uri}")

        def write_text(self, text, uri):
            if "manifest" in uri:
                self.written_manifest = json.loads(text)
            # dry-run writes manifest but doesn't submit

    class FakeS3Client:
        def get_paginator(self, op):
            class Paginator:
                def paginate(self, **kwargs):
                    return [{"Contents": []}]  # empty universe
            return Paginator()

    def fake_load_signs(s3io):
        return {(1, s): 1 for s in range(64)}

    def fake_session(profile):
        class Session:
            def client(self, svc, **kwargs):
                return FakeS3Client()
        return Session()

    monkeypatch.setattr(signs, "load_signs", fake_load_signs)
    dummy_sha = Path(__file__).parent / "fixtures" / "dummy_sha.txt"
    dummy_sha.parent.mkdir(exist_ok=True)
    dummy_sha.write_text("abc123\n")
    def fake_paths():
        return "s3://fake/signs.parquet", "s3://fake/signs.meta.json", dummy_sha
    monkeypatch.setattr(signs, "paths", fake_paths)
    monkeypatch.setattr(evaluate, "_session", fake_session)
    monkeypatch.setattr(evaluate, "_git_code_version", lambda: "test-sha")

    # Inject FakeS3IO and capture the instance
    fake_s3io = FakeS3IO()
    monkeypatch.setattr("s3io.S3IO", lambda profile: fake_s3io)

    # Should not raise
    result = evaluate.cmd_plan(Args())
    assert result == 0

    # Verify manifest contains study and scores_job
    assert fake_s3io.written_manifest is not None
    assert fake_s3io.written_manifest["study"] == "xle"
    assert fake_s3io.written_manifest["scores_job"] == "xle_earnings_full_universe_2026_08_16"
