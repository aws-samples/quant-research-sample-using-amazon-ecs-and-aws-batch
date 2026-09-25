#!/usr/bin/env python3
"""Where a job's bytes land: the node's instance-store NVMe, never the root volume.

WHY. A batch of scoring jobs once died with ENOSPC: their compute environment had
no launch template, so the nodes booted with the AMI's 30 GiB root, and a 20.9 GB
base model was being staged onto that root through ``/opt/weightcache`` — a host
path that is *created on the root volume* by Docker when nothing else is mounted
there. Meanwhile every P and G family in the fleet carries 1-27 TB of instance
store that the boot-time NVMe setup in the launch template
(``gpu_fleet/catalogue.py``, ``NVME_SETUP``) formats and mounts, one drive per
subdirectory, under ``/mnt/nvme``. Nothing in the job used it.

Rule: **work goes onto the SSDs, not the system root.** This module is the
single resolver for every local path a job writes, so the rule cannot be
half-applied:

  * ``weight_cache_dir()``  — where the base weights are staged (persists across
    jobs on a warm node; the SAME device is chosen across jobs so a cache hit stays
    a hit).
  * ``scratch()``           — per-job working directory (fold output, adapter
    downloads, warm-start adapter, training output). Device with the most free
    space, pinned for the whole process tree through the environment so torchrun
    children agree with the parent.
  * ``require_space()``     — refuse to start a download that cannot fit, with the
    mount, the free and the needed GiB in the message; ENOSPC two minutes in tells
    the operator nothing.
  * ``enforce_nvme()``      — with ``TRAIN_REQUIRE_NVME=1`` (set by the job
    definitions) a node whose launch template did not mount the pool FAILS in the
    first second with a message naming the cause, instead of silently spilling onto
    the root and dying later, or worse, succeeding slowly.

HOW A DEVICE IS RECOGNISED. A subdirectory of the pool root is a mounted drive when
it is on a different filesystem from the pool root itself (``st_dev`` differs).
A plain directory that Docker or a stray ``mkdir`` created on the root volume has
the root's ``st_dev`` and is ignored. Docker bind-mounts are recursive by default,
so the host's sub-mounts are visible to the container through the one ``/mnt/nvme``
volume the job definitions declare.

Torch-free and boto-free on purpose: this is the piece that must be unit-testable
in the laptop environment, exactly like ``weight_cache``.
"""
from __future__ import annotations

import logging
import os
import stat

logger = logging.getLogger(__name__)

#: host path the job definitions mount into the container; the launch template's
#: NVMe setup mounts one instance-store drive per subdirectory beneath it
POOL_ENV = "TRAIN_NVME_POOL"
DEFAULT_POOL = "/mnt/nvme"
#: scratch root override. Pool root => per-job directory on the emptiest device.
SCRATCH_ENV = "TRAIN_SCRATCH"
#: set by the first resolver in a process tree; children inherit it, so every rank
#: and every sibling module agree on ONE scratch directory
SCRATCH_RESOLVED_ENV = "TRAIN_SCRATCH_RESOLVED"
#: "1" => a node with no mounted instance store is a hard error at start
REQUIRE_ENV = "TRAIN_REQUIRE_NVME"
WEIGHTCACHE_SUBDIR = "weightcache"
SCRATCH_SUBDIR = "scratch"
GIB = 2 ** 30


def pool_root() -> str:
    return os.environ.get(POOL_ENV) or DEFAULT_POOL


def pool_devices(pool: str | None = None) -> list[str]:
    """Mounted instance-store drives under the pool root, sorted by name.

    A subdirectory counts only when it is a different filesystem from the pool root
    (see module docstring). Missing pool => []."""
    pool = pool or pool_root()
    try:
        root_dev = os.stat(pool).st_dev
    except OSError:
        return []
    out = []
    for name in sorted(os.listdir(pool)):
        p = os.path.join(pool, name)
        try:
            st = os.stat(p)
        except OSError:
            continue
        if stat.S_ISDIR(st.st_mode) and st.st_dev != root_dev:
            out.append(p)
    return out


def free_bytes(path: str) -> int:
    st = os.statvfs(path)
    return st.f_bavail * st.f_frsize


def total_bytes(path: str) -> int:
    st = os.statvfs(path)
    return st.f_blocks * st.f_frsize


def describe(path: str) -> dict:
    """Free/total GiB of the filesystem holding ``path`` and whether it is a pool drive."""
    probe = path
    while probe and not os.path.exists(probe):
        probe = os.path.dirname(probe)
    probe = probe or "/"
    devices = pool_devices()
    on_pool = any(os.path.abspath(path).startswith(d + os.sep) or os.path.abspath(path) == d
                  for d in devices)
    return {"path": path, "filesystem_probe": probe,
            "free_gib": round(free_bytes(probe) / GIB, 2),
            "total_gib": round(total_bytes(probe) / GIB, 2),
            "on_nvme_pool": on_pool}


def pick_device(pool: str | None = None) -> str | None:
    """The pool drive with the most free space (ties: lowest name). None if no pool."""
    devs = pool_devices(pool)
    if not devs:
        return None
    return max(devs, key=lambda d: (free_bytes(d), -ord(os.path.basename(d)[-1])))


def require_space(path: str, need_bytes: int, what: str = "") -> None:
    """Raise before writing when ``path``'s filesystem cannot hold ``need_bytes``."""
    probe = path
    while probe and not os.path.exists(probe):
        probe = os.path.dirname(probe)
    free = free_bytes(probe or "/")
    if free < need_bytes:
        d = describe(path)
        raise RuntimeError(
            f"not enough local disk for {what or path}: need {need_bytes / GIB:.1f} GiB, "
            f"{d['free_gib']} GiB free of {d['total_gib']} GiB on the filesystem holding "
            f"{path} (on instance-store pool: {d['on_nvme_pool']}). Refusing to start a "
            f"download that would end in ENOSPC. Pool drives: {pool_devices() or 'NONE — '}"
            f"{'' if pool_devices() else 'the launch template mounted no instance store'}")


def weight_cache_dir(configured: str | None, model: str = "", need_bytes: int = 0) -> str | None:
    """Resolve the configured weight-cache location to a directory.

    * ``configured`` is the pool root (has mounted drives beneath it) => a drive's
      ``weightcache`` subdir. Preference order, so a warm node keeps hitting its cache:
      a drive that ALREADY holds ``weightcache/<model>``, else the lowest-named drive
      with room for ``need_bytes``, else the lowest-named drive.
    * anything else (a legacy explicit path such as ``/opt/weightcache``) => verbatim.
    * None/'' => None (caller falls back to its per-container default).
    """
    if not configured:
        return None
    devs = pool_devices(configured)
    if not devs:
        return configured
    if model:
        for d in devs:
            if os.path.isdir(os.path.join(d, WEIGHTCACHE_SUBDIR, model)):
                return os.path.join(d, WEIGHTCACHE_SUBDIR)
    for d in devs:
        if free_bytes(d) >= need_bytes:
            return os.path.join(d, WEIGHTCACHE_SUBDIR)
    return os.path.join(devs[0], WEIGHTCACHE_SUBDIR)


def _job_id() -> str:
    return os.environ.get("AWS_BATCH_JOB_ID") or "local"


def scratch_root() -> str:
    """The per-job scratch directory, created, pinned in the environment.

    ``TRAIN_SCRATCH`` unset  => ``/tmp`` (laptop, unit tests, images without the mount).
    ``TRAIN_SCRATCH`` = pool root with mounted drives => ``<emptiest drive>/scratch/<job>``.
    ``TRAIN_SCRATCH`` = any other directory => ``<that dir>/<job>``.
    ``TRAIN_SCRATCH`` = pool root WITHOUT drives => ``/tmp`` plus a warning (and an error
    when ``TRAIN_REQUIRE_NVME=1``, see ``enforce_nvme``)."""
    pinned = os.environ.get(SCRATCH_RESOLVED_ENV)
    if pinned:
        os.makedirs(pinned, exist_ok=True)
        return pinned
    configured = os.environ.get(SCRATCH_ENV)
    if not configured:
        root = "/tmp"
    else:
        dev = pick_device(configured)
        if dev:
            root = os.path.join(dev, SCRATCH_SUBDIR, _job_id())
        elif os.path.isdir(configured):
            root = os.path.join(configured, _job_id())
        else:
            logger.warning(f"{SCRATCH_ENV}={configured} has no mounted drives and is not a "
                           f"directory; scratch falls back to /tmp (root volume)")
            root = "/tmp"
    os.makedirs(root, exist_ok=True)
    os.environ[SCRATCH_RESOLVED_ENV] = root
    return root


def scratch(*parts: str) -> str:
    """``scratch('fold_00')`` -> ``<scratch_root>/fold_00`` (not created)."""
    return os.path.join(scratch_root(), *parts)


def enforce_nvme(log: logging.Logger | None = None) -> dict:
    """Log the pool and, under ``TRAIN_REQUIRE_NVME=1``, fail fast when it is empty.

    Returns ``{"pool", "devices": [{path, free_gib, total_gib}], "required", "scratch"}``
    for the job's meta record."""
    log = log or logger
    pool = pool_root()
    devs = pool_devices(pool)
    required = os.environ.get(REQUIRE_ENV, "") == "1"
    report = {"pool": pool, "required": required,
              "devices": [{"path": d, "free_gib": round(free_bytes(d) / GIB, 1),
                           "total_gib": round(total_bytes(d) / GIB, 1)} for d in devs]}
    if not devs and required:
        raise RuntimeError(
            f"{REQUIRE_ENV}=1 but no instance-store filesystem is mounted under {pool}. "
            f"This node's launch template mounted no instance store (or the job definition "
            f"does not mount {pool}); every write would land on the root volume. Refusing "
            f"to run. Fix the compute environment's launch template; do not lower the guard.")
    if not devs:
        log.warning(f"no instance-store drives under {pool}; local writes go to the root "
                    f"volume (set {REQUIRE_ENV}=1 to make this fatal)")
    else:
        log.info(f"instance-store pool {pool}: " + ", ".join(
            f"{d['path']} {d['free_gib']}/{d['total_gib']} GiB free" for d in report["devices"]))
    report["scratch"] = scratch_root() if os.environ.get(SCRATCH_ENV) else None
    if report["scratch"]:
        log.info(f"scratch root: {report['scratch']}")
    return report
