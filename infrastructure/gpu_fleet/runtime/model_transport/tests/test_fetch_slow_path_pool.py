"""The slow path must ALSO use the local NVMe pool.

WHY THIS FILE EXISTS. Audit 2026-09-16: the NVMe pool was reachable only
through `download.py` (the multi-ENI fast path), and `_fast_path_ok` requires
TWO OR MORE IP-carrying network cards. So on every single-ENI node the fetch
fell back to `fetch.py`'s thread-per-file loop, which writes straight to the
caller's directory on the EBS root — the pool went unused even when the host had
one mounted.

That is not a corner case, it is the majority of the GPU fleet:
  - AWS Batch nodes (global-gpu-ce g6/g5/g4dn, global-p6-ce p6-b200) get ONE
    ENI from Batch's own managed launch template, and they have instance store:
    g5.12xlarge 1x3.8TB, g6.12xlarge 4x940GB, p6-b200.48xlarge 8x3.8TB
    (describe-instance-types, us-west-2, 2026-09-16).
  - Any p5 launched from a template that predates the all-`efa` change.
So mounting /mnt/nvme on those nodes would have changed nothing at all: the
pool has to be wired into the path they actually take.

The fix is in fetch.py, not at the call sites — same reason the fast path lives
there: capability is DETECTED once, centrally, so no job can be left behind by
omission.
"""
import os
import sys
import types
from unittest.mock import patch

import pytest

from model_transport import fetch, nvme


def _fake_pool(tmp_path, model_dir, ndrives=3):
    """A CheckpointPool over real directories, no devices involved."""
    paths = []
    for i in range(ndrives):
        p = tmp_path / f"drive{i}"
        p.mkdir()
        paths.append(str(p))
    return nvme.CheckpointPool(paths, "glm-5", model_dir=str(model_dir))


@pytest.fixture
def slowpath(tmp_path, monkeypatch):
    """fetch_model forced onto the slow path, with S3 stubbed out.

    Files are 8 GiB apiece on paper so the reuse/size logic is exercised, but
    nothing is actually written by S3 — the stub creates the file itself, which
    is what lets us assert WHERE it landed.
    """
    prefix = "base_models/glm-5"
    objs = {f"{prefix}/model-{i:05d}.safetensors": 8 << 30 for i in range(6)}
    objs[f"{prefix}/config.json"] = 4096

    monkeypatch.setattr(
        fetch, "resolve_bucket_ex",
        lambda b, p, region=None, force_source=False: (
            "bkt", "us-west-2", objs, "replica"))

    written = []

    class _S3:
        def download_file(self, bucket, key, dest, Config=None):
            # Mimic S3: create the file at its full nominal size, so the
            # skip_existing size check behaves like production.
            written.append(dest)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.truncate(objs[key])

    monkeypatch.setattr(fetch.boto3, "client", lambda *a, **k: _S3())
    # ONE ENI: this is the whole point. The fast path must decline.
    monkeypatch.setattr(fetch, "_fast_path_ok",
                        lambda include, local_dir, objs_: (
                            False, "only 1 IP-carrying network card(s)"))
    return types.SimpleNamespace(objs=objs, written=written, prefix=prefix)


def test_slow_path_stripes_across_the_pool(slowpath, tmp_path, caplog):
    """THE REGRESSION.

    A single-ENI node with a mounted pool must still put the shards on the
    drives. Before the fix every byte landed under local_dir on the root volume.
    """
    caplog.set_level("INFO")
    local = tmp_path / "base_model"
    pool = _fake_pool(tmp_path, local, ndrives=3)

    with patch.object(nvme, "build_pool", return_value=pool) as bp:
        rep = fetch.fetch_model("s3://src/base_models/glm-5",
                                str(local))

    assert bp.called, "the slow path never built a checkpoint pool"

    # Every shard is reachable under the directory the caller passed...
    for i in range(6):
        link = local / f"model-{i:05d}.safetensors"
        assert link.exists(), f"{link} missing from the caller's directory"

    # ...and the big ones physically live on the drives, not under local_dir.
    on_drives = 0
    for i in range(6):
        real = os.path.realpath(local / f"model-{i:05d}.safetensors")
        if any(real.startswith(p) for p in pool.paths):
            on_drives += 1
    assert on_drives >= 4, (
        f"only {on_drives}/6 shards landed on the NVMe pool; the rest are on "
        f"the root volume — this is the bug this test exists for")

    assert rep.local_dir == str(local), \
        "callers pass local_dir to from_pretrained; it must stay authoritative"


def test_slow_path_without_a_pool_is_unchanged(slowpath, tmp_path):
    """A laptop, CI, or a node with no instance store: same behaviour as before.

    The pool is an optimization. If there is none, every file must still land
    exactly where the caller asked, with no symlinks and no new failure mode.
    """
    local = tmp_path / "base_model"
    # build_pool's own no-drives contract: a CheckpointPool with single_dir.
    nopool = nvme.CheckpointPool([], "glm-5", single_dir=str(local))

    with patch.object(nvme, "build_pool", return_value=nopool):
        rep = fetch.fetch_model("s3://src/base_models/glm-5",
                                str(local))

    for i in range(6):
        f = local / f"model-{i:05d}.safetensors"
        assert f.exists() and not f.is_symlink(), \
            "no pool means plain files in the caller's directory"
    assert rep.files == 7


def test_pool_failure_never_fails_the_download(slowpath, tmp_path, caplog):
    """A broken pool is a performance problem, not a correctness one.

    This runs on the critical path of a multi-hour multi-GPU job. If drive
    discovery throws (odd lsblk, no permission, a container without lsblk at
    all — measured: the amazonlinux:2023 base image has no lsblk), the download
    must proceed to the caller's directory rather than take the run down.
    """
    caplog.set_level("WARNING")
    local = tmp_path / "base_model"

    with patch.object(nvme, "build_pool",
                      side_effect=OSError("lsblk: command not found")):
        rep = fetch.fetch_model("s3://src/base_models/glm-5",
                                str(local))

    assert rep.files == 7
    for i in range(6):
        assert (local / f"model-{i:05d}.safetensors").exists()
    assert "pool" in caplog.text.lower()


def test_skip_existing_finds_files_already_on_the_pool(slowpath, tmp_path):
    """Warm-node reuse must look where the bytes actually are.

    download.py learned this the hard way (see its STEP 0 comment): checking the
    caller's nominal directory while shards live on the drives makes a fully
    warm node re-download the entire model. The slow path must not repeat it.
    """
    local = tmp_path / "base_model"
    pool = _fake_pool(tmp_path, local, ndrives=3)

    with patch.object(nvme, "build_pool", return_value=pool):
        first = fetch.fetch_model(
            "s3://src/base_models/glm-5", str(local))
        assert first.files == 7
        second = fetch.fetch_model(
            "s3://src/base_models/glm-5", str(local))

    assert second.files == 0, (
        f"re-fetched {second.files} files that were already on the pool; a warm "
        f"node would redownload the whole model")


def test_small_fetches_do_not_touch_the_pool(slowpath, tmp_path, monkeypatch):
    """Skeleton fetches (config/tokenizer, ~20 MB) must stay simple.

    On multi-node FSDP every non-loader rank does exactly this, and building a
    pool for 20 MB of small files buys nothing while adding a mount-dependent
    failure mode to N-1 ranks.
    """
    local = tmp_path / "base_model"
    with patch.object(nvme, "build_pool") as bp:
        fetch.fetch_model("s3://src/base_models/glm-5",
                          str(local), include=["config.json"])
    assert not bp.called, "a filtered/skeleton fetch built an NVMe pool"
