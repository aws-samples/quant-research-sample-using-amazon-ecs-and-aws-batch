"""Tests for nvme.py — drive discovery, the container format guard, placement.

The container guard is the reason this file exists. Verified on a production
node a p5.48xlarge: 8 instance-store drives, ONE mounted
(/scratch), and the training container sees the devices via the shared /sys but
NOT the host's mounts. So `allow_format=True` inside a container would mkfs a
drive the host is actively using.
"""
import json
from unittest.mock import patch

from model_transport import nvme

# The real `lsblk -b -d -J` output shape from a p5.48xlarge, trimmed to 3 drives.
# nvme0n1 is the EBS root and must never be selected.
LSBLK = json.dumps({"blockdevices": [
    {"name": "nvme0n1", "size": 2199023255552,
     "model": "Amazon Elastic Block Store", "mountpoint": "/", "type": "disk"},
    {"name": "nvme1n1", "size": 3750000000000,
     "model": "Amazon EC2 NVMe Instance Storage", "mountpoint": "/scratch",
     "type": "disk"},
    {"name": "nvme2n1", "size": 3750000000000,
     "model": "Amazon EC2 NVMe Instance Storage", "mountpoint": None,
     "type": "disk"},
]})


def test_discovery_never_selects_the_ebs_root():
    """Both are /dev/nvme*n1; only the MODEL string distinguishes them."""
    with patch.object(nvme, "_sh", return_value=(0, LSBLK, "")):
        drives = nvme.discover_drives()
    names = [d["name"] for d in drives]
    assert names == ["nvme1n1", "nvme2n1"], \
        "the EBS root was selected — a mkfs here destroys the OS"
    # Sorted by name so shard placement is identical run to run.
    assert names == sorted(names)


def test_container_refuses_to_format(caplog):
    """THE DATA-LOSS GUARD.

    In a container the device is visible (shared /sys) but the host's mount is
    not, so nvme1n1 reads back as unmounted. allow_format=True must NOT mkfs it.
    """
    # Same drives, but from the container's namespace nothing is mounted.
    hidden = json.loads(LSBLK)
    for d in hidden["blockdevices"]:
        d["mountpoint"] = None
    calls = []

    def fake_sh(cmd, timeout=300):
        calls.append(cmd)
        if cmd.startswith("lsblk"):
            return 0, json.dumps(hidden), ""
        return 0, "", ""

    with patch.object(nvme, "_sh", side_effect=fake_sh), \
            patch.object(nvme, "in_container", return_value=True):
        paths = nvme.ensure_pool(allow_format=True)

    assert not any("mkfs" in c for c in calls), \
        f"mkfs was issued inside a container: {[c for c in calls if 'mkfs' in c]}"
    assert not any(c.startswith("mount ") for c in calls)
    assert paths == []
    assert "CONTAINER" in caplog.text


def test_host_may_format_unmounted_drives():
    """The same call on the HOST is allowed to format and mount."""
    hidden = json.loads(LSBLK)
    for d in hidden["blockdevices"]:
        d["mountpoint"] = None
    calls = []

    def fake_sh(cmd, timeout=300):
        calls.append(cmd)
        if cmd.startswith("lsblk"):
            return 0, json.dumps(hidden), ""
        if cmd.startswith("blkid"):
            return 1, "", ""          # unformatted -> safe to mkfs
        return 0, "", ""

    with patch.object(nvme, "_sh", side_effect=fake_sh), \
            patch.object(nvme, "in_container", return_value=False), \
            patch("os.statvfs") as sv, patch("os.makedirs"):
        sv.return_value.f_bavail = 10 ** 9
        sv.return_value.f_frsize = 4096
        paths = nvme.ensure_pool(allow_format=True)

    assert sum(1 for c in calls if "mkfs" in c) == 2
    assert len(paths) == 2


def test_formatted_but_unmounted_is_mounted_never_reformatted():
    """An existing cache from an earlier run on this boot must survive."""
    hidden = json.loads(LSBLK)
    for d in hidden["blockdevices"]:
        d["mountpoint"] = None
    calls = []

    def fake_sh(cmd, timeout=300):
        calls.append(cmd)
        if cmd.startswith("lsblk"):
            return 0, json.dumps(hidden), ""
        if cmd.startswith("blkid"):
            return 0, "xfs", ""       # already has a filesystem
        return 0, "", ""

    with patch.object(nvme, "_sh", side_effect=fake_sh), \
            patch.object(nvme, "in_container", return_value=False), \
            patch("os.statvfs") as sv, patch("os.makedirs"):
        sv.return_value.f_bavail = 10 ** 9
        sv.return_value.f_frsize = 4096
        nvme.ensure_pool(allow_format=True)

    assert not any("mkfs" in c for c in calls), \
        "reformatted a drive that already had a filesystem — cache destroyed"
    assert sum(1 for c in calls if c.startswith("mount ")) == 2


def test_placement_is_deterministic_and_survives_hash_randomization():
    """crc32, not hash(): PYTHONHASHSEED randomizes str hashing per process, so
    hash() would place the same shard on a different drive in every worker."""
    with patch("os.makedirs"):
        pool = nvme.CheckpointPool(["/mnt/nvme/a", "/mnt/nvme/b", "/mnt/nvme/c"],
                                   "glm-5")
    rel = "model-00042-of-00288.safetensors"
    first = pool.target(rel)
    for _ in range(5):
        assert pool.target(rel) == first
    # Spread: 288 shards over 3 drives should use all 3.
    used = {pool.target(f"model-{i:05d}-of-00288.safetensors")[0].rsplit(
        "/glm-5/", 1)[0] for i in range(288)}
    assert len(used) == 3


def test_callers_directory_stays_authoritative():
    """A training entry point that does `fetch_model(model_id, local); model_id = local` — it
    IGNORES the returned path. So the caller's dir must hold the files (as
    symlinks), or every caller breaks."""
    with patch("os.makedirs"):
        pool = nvme.CheckpointPool(["/mnt/nvme/a", "/mnt/nvme/b"], "glm-5",
                                   model_dir="/tmp/base_model")
    assert pool.model_dir == "/tmp/base_model"
    # Every shard is reachable under the caller's directory.
    for i in range(20):
        rel = f"model-{i:05d}.safetensors"
        real, link = pool.target(rel)
        assert link == f"/tmp/base_model/{rel}", \
            "shard is not visible in the directory the caller passed"
        assert real.startswith("/mnt/nvme/")


def test_capacity_precheck_fails_before_the_transfer():
    """The ENOSPC that motivated this module surfaced 1.4 TB in."""
    with patch("os.makedirs"):
        pool = nvme.CheckpointPool(["/mnt/nvme/a"], "glm-5")
    with patch.object(pool, "free_bytes", return_value=100 * 10 ** 9):
        try:
            pool.check_capacity(1_500 * 10 ** 9)
            assert False, "no error raised despite a 15x shortfall"
        except OSError as e:
            assert "free" in str(e) and "needs" in str(e)
