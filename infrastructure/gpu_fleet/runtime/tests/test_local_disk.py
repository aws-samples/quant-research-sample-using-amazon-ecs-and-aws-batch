"""local_disk: every local write of a job resolves onto the instance-store pool.

Drives are faked by monkeypatching ``os.stat`` (st_dev) and ``os.statvfs`` (free
space): a pool root on device 1 with subdirectories on devices 2 and 3 is a pool
with two mounted drives; a subdirectory on device 1 is a plain directory Docker
created on the root volume and must be ignored.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import local_disk as ld  # noqa: E402


class _StatVfs:
    def __init__(self, free_bytes, total_bytes=10 * ld.GIB):
        self.f_bavail = free_bytes // 4096
        self.f_frsize = 4096
        self.f_blocks = total_bytes // 4096


@pytest.fixture
def pool(tmp_path, monkeypatch):
    """A fake pool: <tmp>/nvme with drives nvme1n1 (dev 2, 5 GiB free) and nvme2n1
    (dev 3, 20 GiB free) plus a plain dir 'lost+found' on the root device (dev 1)."""
    root = tmp_path / "nvme"
    root.mkdir()
    (root / "nvme1n1").mkdir()
    (root / "nvme2n1").mkdir()
    (root / "lost+found").mkdir()
    devs = {str(root): 1, str(root / "nvme1n1"): 2, str(root / "nvme2n1"): 3,
            str(root / "lost+found"): 1}
    free = {2: 5 * ld.GIB, 3: 20 * ld.GIB, 1: 1 * ld.GIB}
    real_stat, real_statvfs = os.stat, os.statvfs

    def _dev_of(path):
        p = os.path.abspath(str(path))
        while p and p not in devs and p != "/":
            p = os.path.dirname(p)
        return devs.get(p, 1)

    class _St:
        def __init__(self, st, dev):
            self._st, self.st_dev = st, dev

        def __getattr__(self, k):
            return getattr(self._st, k)

    def fake_stat(path, *a, **kw):
        return _St(real_stat(path, *a, **kw), _dev_of(path))

    def fake_statvfs(path):
        return _StatVfs(free[_dev_of(path)])

    monkeypatch.setattr(os, "stat", fake_stat)
    monkeypatch.setattr(os, "statvfs", fake_statvfs)
    monkeypatch.setenv(ld.POOL_ENV, str(root))
    monkeypatch.delenv(ld.SCRATCH_ENV, raising=False)
    monkeypatch.delenv(ld.SCRATCH_RESOLVED_ENV, raising=False)
    monkeypatch.delenv(ld.REQUIRE_ENV, raising=False)
    monkeypatch.delenv("AWS_BATCH_JOB_ID", raising=False)
    return root


def test_pool_devices_are_the_separately_mounted_subdirs_only(pool):
    devs = ld.pool_devices()
    assert devs == [str(pool / "nvme1n1"), str(pool / "nvme2n1")]
    assert str(pool / "lost+found") not in devs      # same filesystem as the root


def test_missing_pool_and_plain_directory_pool_yield_no_devices(tmp_path, monkeypatch):
    monkeypatch.setenv(ld.POOL_ENV, str(tmp_path / "absent"))
    assert ld.pool_devices() == []
    plain = tmp_path / "plain"
    (plain / "sub").mkdir(parents=True)
    assert ld.pool_devices(str(plain)) == []       # sub is on the same st_dev


def test_pick_device_prefers_the_most_free_space(pool):
    assert ld.pick_device() == str(pool / "nvme2n1")


def test_weight_cache_dir_is_stable_across_jobs_and_prefers_an_existing_cache(pool):
    # cold node: lowest-named drive with room
    assert ld.weight_cache_dir(str(pool), model="m", need_bytes=1 * ld.GIB) == \
        str(pool / "nvme1n1" / "weightcache")
    # too big for the first drive: next drive with room
    assert ld.weight_cache_dir(str(pool), model="m", need_bytes=10 * ld.GIB) == \
        str(pool / "nvme2n1" / "weightcache")
    # a drive that already holds this model wins regardless of free space
    (pool / "nvme2n1" / "weightcache" / "m").mkdir(parents=True)
    assert ld.weight_cache_dir(str(pool), model="m", need_bytes=1 * ld.GIB) == \
        str(pool / "nvme2n1" / "weightcache")


def test_weight_cache_dir_passes_legacy_paths_through(tmp_path):
    legacy = tmp_path / "opt_weightcache"
    legacy.mkdir()
    assert ld.weight_cache_dir(str(legacy), model="m") == str(legacy)
    assert ld.weight_cache_dir("", model="m") is None
    assert ld.weight_cache_dir(None) is None


def test_require_space_names_mount_free_and_need(pool):
    target = str(pool / "nvme1n1" / "weightcache" / "m")     # 5 GiB free there
    ld.require_space(target, 4 * ld.GIB, "base weights")    # fits: no raise
    with pytest.raises(RuntimeError) as ei:
        ld.require_space(target, 6 * ld.GIB, "base weights")
    msg = str(ei.value)
    assert "need 6.0 GiB" in msg and "5.0 GiB free" in msg and "base weights" in msg
    assert "on instance-store pool: True" in msg


def test_scratch_defaults_to_tmp_without_configuration(pool):
    assert ld.scratch_root() == "/tmp"
    assert ld.scratch("fold_00") == "/tmp/fold_00"


def test_scratch_lands_on_the_emptiest_drive_per_job_and_is_pinned(pool, monkeypatch):
    monkeypatch.setenv(ld.SCRATCH_ENV, str(pool))
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-1")
    root = ld.scratch_root()
    assert root == str(pool / "nvme2n1" / "scratch" / "job-1")
    assert os.path.isdir(root)
    assert os.environ[ld.SCRATCH_RESOLVED_ENV] == root
    # a later call in the same process tree (or a torchrun child inheriting the env)
    # gets the SAME directory even if free space has shifted meanwhile
    monkeypatch.setenv(ld.POOL_ENV, str(pool / "absent"))
    assert ld.scratch("adapter_01") == os.path.join(root, "adapter_01")


def test_scratch_falls_back_to_tmp_when_the_pool_has_no_drives(tmp_path, monkeypatch):
    monkeypatch.setenv(ld.SCRATCH_ENV, str(tmp_path / "absent"))
    monkeypatch.delenv(ld.SCRATCH_RESOLVED_ENV, raising=False)
    assert ld.scratch_root() == "/tmp"


def test_enforce_nvme_fails_fast_only_when_required(tmp_path, monkeypatch):
    monkeypatch.setenv(ld.POOL_ENV, str(tmp_path / "absent"))
    monkeypatch.delenv(ld.SCRATCH_ENV, raising=False)
    monkeypatch.delenv(ld.SCRATCH_RESOLVED_ENV, raising=False)
    monkeypatch.delenv(ld.REQUIRE_ENV, raising=False)
    rep = ld.enforce_nvme()
    assert rep["devices"] == [] and rep["required"] is False
    monkeypatch.setenv(ld.REQUIRE_ENV, "1")
    with pytest.raises(RuntimeError, match="mounted no instance store"):
        ld.enforce_nvme()


def test_enforce_nvme_reports_every_drive(pool, monkeypatch):
    monkeypatch.setenv(ld.REQUIRE_ENV, "1")
    monkeypatch.setenv(ld.SCRATCH_ENV, str(pool))
    rep = ld.enforce_nvme()
    assert [d["path"] for d in rep["devices"]] == [str(pool / "nvme1n1"), str(pool / "nvme2n1")]
    assert rep["devices"][1]["free_gib"] == 20.0
    assert rep["scratch"].startswith(str(pool / "nvme2n1" / "scratch"))


def test_describe_flags_pool_membership(pool):
    assert ld.describe(str(pool / "nvme2n1" / "weightcache"))["on_nvme_pool"] is True
    assert ld.describe(str(pool / "lost+found"))["on_nvme_pool"] is False
