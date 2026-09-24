import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from signs import generate_frame, sha256_of_frame


def test_shape_and_values():
    df = generate_frame([1, 2, 3], n_seeds=4, master_seed=7)
    assert len(df) == 12
    assert set(df["sign"].unique()) <= {-1, 1}
    assert set(df["seed"].unique()) == {0, 1, 2, 3}


def test_deterministic_for_same_master_seed():
    a = generate_frame([10, 20], n_seeds=8, master_seed=42)
    b = generate_frame([10, 20], n_seeds=8, master_seed=42)
    assert a.equals(b) and sha256_of_frame(a) == sha256_of_frame(b)


def test_different_master_seed_differs():
    a = generate_frame(list(range(100)), n_seeds=8, master_seed=1)
    b = generate_frame(list(range(100)), n_seeds=8, master_seed=2)
    assert not a["sign"].equals(b["sign"])


def test_independent_draws_within_and_across_seeds():
    # 407x64 at p=0.5: no seed column and no event row may be constant
    df = generate_frame(list(range(407)), n_seeds=64, master_seed=20260821)
    per_seed = df.groupby("seed")["sign"].nunique()
    assert (per_seed == 2).all()
    frac_pos = df.groupby("seed")["sign"].apply(lambda s: (s > 0).mean())
    assert frac_pos.between(0.35, 0.65).all()   # loose binomial sanity


def test_generate_refuses_overwrite(monkeypatch, tmp_path):
    import signs
    class FakeS3:
        def exists(self, uri): return True
    with pytest.raises(SystemExit):
        signs.cmd_generate(FakeS3(), event_ids=[1], n_seeds=2)


def test_s3io_exists_returns_false_on_404(monkeypatch):
    """exists() should return False on 404 errors."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from s3io import S3IO
    from botocore.exceptions import ClientError

    class FakeBotoClient:
        def head_object(self, **kwargs):
            err = ClientError(
                {"Error": {"Code": "404"}},
                "HeadObject"
            )
            raise err

    s3io = S3IO.__new__(S3IO)
    s3io.pl_opts = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}

    monkeypatch.setattr("boto3.client", lambda *args, **kwargs: FakeBotoClient())
    assert s3io.exists("s3://bucket/key") is False


def test_s3io_exists_raises_on_403(monkeypatch):
    """exists() should raise on 403 (permission denied)."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from s3io import S3IO
    from botocore.exceptions import ClientError

    class FakeBotoClient:
        def head_object(self, **kwargs):
            err = ClientError(
                {"Error": {"Code": "403"}},
                "HeadObject"
            )
            raise err

    s3io = S3IO.__new__(S3IO)
    s3io.pl_opts = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}

    monkeypatch.setattr("boto3.client", lambda *args, **kwargs: FakeBotoClient())
    with pytest.raises(ClientError):
        s3io.exists("s3://bucket/key")
