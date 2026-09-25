"""Local NVMe instance-store discovery and pooling for model checkpointing.

WHY THIS EXISTS. On 2026-09-15 a 32-card download of GLM-5 failed with 160
`[Errno 28] No space left on device` while the host had 30.4 TB of idle local
NVMe attached. The mirror was writing to the 2 TB EBS root because nothing in
this codebase knew the instance-store drives existed: p5.48xlarge ships 8 x 3.8
TB NVMe, and the AMI mounts exactly one of them. So a downloader that pulls at
8-10 GB/s was persisting to the slowest, smallest, most expensive volume on the
box, and running out of room on a model that fits 20x over in local storage.

EBS IS THE WRONG DESTINATION ON TWO AXES, not just capacity:
  - SPACE. GLM-5 is 1.37 TiB. A 2 TB root holds ONE model plus an OS. Two
    concurrent experiments, or one interrupted run leaving a stale copy, and the
    volume is full — which is exactly what happened.
  - WRITE BANDWIDTH. One instance-store NVMe sustains ~1-2 GB/s of writes, and
    gp3 EBS is capped far below that (1000 MiB/s at the very top of the range,
    typically 125-250). At a 9 GB/s download rate a single-volume mirror is a 5-9x
    mismatch, so the backup copy becomes the bottleneck for the whole transfer.
    Eight drives in parallel is the only local destination that keeps up.

WHY A POOL AND NOT RAID0. mdadm is absent from the ECS/DLAMI GPU images, so a
RAID0 recipe needs a package install at boot — a network dependency in the one
place where failures are hardest to see, and a hard failure on any host where the
install is blocked. Instead each drive gets its own filesystem and shards are
placed round-robin across drives, which gives the same parallel write bandwidth
with zero dependencies. The model directory is real and contains SYMLINKS to
shards living on the other drives; `safetensors` and `transformers` open files by
path and follow symlinks transparently (the HF cache is itself a symlink farm),
and `os.path.getsize` follows them, so the reuse check keeps working unchanged.

EPHEMERAL BY DESIGN. Instance store does not survive a stop or a spot
reclamation. That is the correct medium for this data anyway: the model is
immutable in S3 and re-downloadable in ~3 minutes, so local copies are a cache,
not a record. Nothing durable is ever written here — the caller decides that, and
`is_ephemeral` is exported so it cannot be forgotten.
"""
import json
import logging
import os
import subprocess

logger = logging.getLogger(__name__)

# Instance-store drives report this model string; EBS reports "Amazon Elastic
# Block Store". Matching on the model is what distinguishes "local NVMe I may
# reformat" from "the root volume I must not touch" — a distinction worth being
# explicit about, since the two are both /dev/nvme*n1.
INSTANCE_STORE_MODEL = "Amazon EC2 NVMe Instance Storage"

# Where discovered drives get mounted. Numbered per drive rather than pooled
# under one path so `df` output stays readable and a single bad drive is obvious.
MOUNT_ROOT = "/mnt/nvme"

# Filesystem for fresh drives. xfs over ext4 because it is what the AMI uses for
# the root and /scratch, it handles multi-GiB files and parallel writers better,
# and mkfs is near-instant on a 3.8 TB device (no inode table to write out).
MKFS = "mkfs.xfs"


def _sh(cmd, timeout=300):
    p = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                       timeout=timeout)
    return p.returncode, p.stdout.strip(), p.stderr.strip()


def in_container():
    """True if we are inside a container, i.e. must NEVER format a drive.

    THIS GUARD PREVENTS DATA LOSS. `lsblk` reads /sys, which a container shares
    with the host, so every instance-store device is VISIBLE inside a container
    — but the MOUNTPOINT column reflects the container's own mount namespace, so
    a drive the host has mounted and is actively using reads back as unmounted.
    `ensure_pool(allow_format=True)` would then see "unmounted, no filesystem I
    can see" and mkfs a live drive out from under the host.

    Verified on a p5.48xlarge: the host has 8 instance-store drives with
    nvme1n1 on /scratch, while the training container is started with only
    /mnt/models/<model> bind-mounted.

    So inside a container the pool is whatever is ALREADY MOUNTED and visible
    (a bind mount of /mnt/nvme); formatting is the host's job, done once at boot
    by the launch template's UserData.
    """
    if os.path.exists("/.dockerenv"):
        return True
    try:
        with open("/proc/1/cgroup") as f:
            cg = f.read()
        if "docker" in cg or "ecs" in cg or "containerd" in cg:
            return True
    except OSError:
        pass
    # cgroup v2 hides the path; PID 1 not being init is the fallback signal.
    try:
        with open("/proc/1/comm") as f:
            return f.read().strip() not in ("systemd", "init")
    except OSError:
        return False


def discover_drives():
    """Every local instance-store NVMe drive, with its current mountpoint.

    Reads lsblk rather than guessing device names: NVMe enumeration order is not
    stable across boots, so nvme1n1 is not reliably "the first instance store" —
    on the test host the EBS root was nvme0n1 but that is a convention, not a
    guarantee. Whole disks only (`-d`), because a drive already carrying a
    partition table is one somebody else is managing.
    """
    rc, out, err = _sh("lsblk -b -d -J -o NAME,SIZE,MODEL,MOUNTPOINT,TYPE")
    if rc != 0:
        logger.warning(f"lsblk failed ({err[:120]}); no local NVMe pool")
        return []
    drives = []
    for d in json.loads(out or "{}").get("blockdevices", []):
        if d.get("type") != "disk" or (d.get("model") or "").strip() != \
                INSTANCE_STORE_MODEL:
            continue
        drives.append({"dev": f"/dev/{d['name']}", "name": d["name"],
                       "size": int(d.get("size") or 0),
                       "mountpoint": d.get("mountpoint")})
    # Sort by device name so placement is deterministic run to run: the same
    # shard lands on the same drive, which makes a slow or failing drive show up
    # as a repeatable pattern instead of noise.
    drives.sort(key=lambda d: d["name"])
    return drives


def _has_filesystem(dev):
    rc, out, _ = _sh(f"blkid -p -o value -s TYPE {dev} 2>/dev/null")
    return rc == 0 and bool(out.strip())


def _mount(dev, path):
    os.makedirs(path, exist_ok=True)
    # nodiscard/noatime: these are write-once cache volumes, so neither TRIM
    # bookkeeping nor access-time updates buy anything, and both cost write IOPS
    # on the exact path we are trying to keep out of the way of the download.
    rc, _, err = _sh(f"mount -o noatime,nodiscard {dev} {path}")
    if rc != 0:
        rc, _, err = _sh(f"mount -o noatime {dev} {path}")
    return rc == 0, err


def ensure_pool(allow_format=False, mount_root=MOUNT_ROOT, min_free_gb=100):
    """Return the writable local-NVMe mountpoints, mounting drives as needed.

    `allow_format=False` is the default and it is the safe one: unmounted drives
    are REPORTED, not touched. Formatting destroys data, and a drive can be
    unmounted because another process is about to use it. Pass True only where
    the contract is explicitly "this host's instance store is mine" — node
    bootstrap, or a caller that has just been told so.

    An unformatted drive is always safe to mkfs (nothing to lose); a FORMATTED
    but unmounted drive is only mounted, never reformatted, so an existing cache
    from an earlier run on this boot is picked up rather than erased.
    """
    drives = discover_drives()
    if not drives:
        logger.info("no local instance-store NVMe found; "
                    "checkpointing falls back to the root volume")
        return []

    if allow_format and in_container():
        # See in_container(): a host-mounted drive looks unmounted from in here,
        # so honouring allow_format would mkfs live data. Downgrade, loudly.
        logger.warning(
            "allow_format=True but this process is in a CONTAINER — refusing to "
            "format. A drive the host has mounted reads back as unmounted in "
            "this mount namespace, so formatting could destroy live data. Using "
            "only already-visible mounts; the host formats at boot.")
        allow_format = False

    paths = []
    for i, d in enumerate(drives):
        if d["mountpoint"]:
            paths.append(d["mountpoint"])
            continue
        if not allow_format:
            logger.warning(
                f"{d['dev']} ({d['size']/1e12:.1f} TB) is unmounted and "
                f"allow_format=False, so it stays idle. This is how 30 TB sat "
                f"unused while a mirror filled the 2 TB root.")
            continue
        target = os.path.join(mount_root, d["name"])
        if not _has_filesystem(d["dev"]):
            rc, _, err = _sh(f"{MKFS} -f {d['dev']}", timeout=600)
            if rc != 0:
                logger.error(f"mkfs on {d['dev']} failed: {err[:200]}")
                continue
        ok, err = _mount(d["dev"], target)
        if not ok:
            logger.error(f"mount {d['dev']} -> {target} failed: {err[:200]}")
            continue
        paths.append(target)

    # A mountpoint with no room is worse than no mountpoint: placement would
    # spread shards onto it and the run would die with ENOSPC partway through,
    # which is the failure this module exists to prevent.
    usable = []
    for p in paths:
        try:
            st = os.statvfs(p)
            free_gb = st.f_bavail * st.f_frsize / 1e9
        except OSError as e:
            logger.warning(f"cannot statvfs {p}: {e}")
            continue
        if free_gb < min_free_gb:
            logger.warning(f"{p} has only {free_gb:.0f} GB free "
                           f"(< {min_free_gb}); excluded from the pool")
            continue
        usable.append(p)

    logger.info(f"local NVMe pool: {len(usable)} drives, "
                f"{sum(os.statvfs(p).f_bavail * os.statvfs(p).f_frsize for p in usable)/1e12:.1f} TB free "
                f"{usable}")
    return usable


def is_ephemeral(path):
    """True if `path` lives on instance store, i.e. will not survive a stop.

    Exported so callers cannot accidentally treat a fast cache as durable
    storage. Adapters and metrics belong in S3; only re-downloadable model
    weights belong here.
    """
    real = os.path.realpath(path)
    for d in discover_drives():
        mp = d.get("mountpoint")
        if mp and (real == mp or real.startswith(mp.rstrip("/") + "/")):
            return True
    return False


class CheckpointPool:
    """Places files across several drives, presenting ONE directory to callers.

    The model directory is real and lives on the first drive; every shard placed
    on another drive gets a symlink there. So a caller does
    `from_pretrained(pool.model_dir)` and never learns that the shards are spread
    over eight devices, while the writes themselves fan out and actually reach
    the drives' aggregate bandwidth instead of one drive's.

    Placement is by a stable hash of the relative path, NOT round-robin on
    arrival order: workers finish shards in nondeterministic order, so
    arrival-order placement would put a different shard on a different drive
    every run and make a slow drive impossible to identify. Hashing also means
    the reuse check finds files exactly where a previous run left them.
    """

    def __init__(self, paths, model_name, single_dir=None, model_dir=None):
        """`model_dir` overrides WHERE THE DIRECTORY IS while keeping `paths` as
        the drives the bytes land on. That separation is what lets a caller's
        contract survive: a training entry point that does

            fetch_model(model_id, local); model_id = local

        i.e. it ignores the returned path and uses the directory it passed. So
        relocating the model onto the NVMe pool would break it — and every other
        caller that mounts a host volume to survive `docker rm`. Instead the
        caller's directory stays authoritative and holds symlinks, while the
        multi-GB shards are striped across the drives. Callers see exactly the
        path they asked for; the bytes still get the pool's bandwidth.
        """
        self.model_name = model_name
        if single_dir:
            # Explicit override wins, for callers with their own mount (FSx, a
            # RAID array somebody else built) or for tests.
            self.paths = [os.path.dirname(single_dir.rstrip("/")) or "/"]
            self.model_dir = single_dir
            self.striped = False
        elif paths:
            self.paths = list(paths)
            self.model_dir = model_dir or os.path.join(self.paths[0], model_name)
            # When the directory is elsewhere (the caller's own path), EVERY
            # drive is a stripe target and every shard is a symlink; when it
            # lives on paths[0], that drive holds real files and the rest are
            # linked. Hence the placement offset below.
            self._dir_on_pool = model_dir is None
            self.striped = len(self.paths) > 1 or not self._dir_on_pool
        else:
            raise ValueError("CheckpointPool needs at least one path")
        os.makedirs(self.model_dir, exist_ok=True)
        start = 1 if getattr(self, "_dir_on_pool", True) else 0
        for p in self.paths[start:]:
            os.makedirs(os.path.join(p, model_name), exist_ok=True)

    def free_bytes(self):
        total = 0
        for p in self.paths:
            try:
                st = os.statvfs(p)
                total += st.f_bavail * st.f_frsize
            except OSError:
                pass
        return total

    def check_capacity(self, need_bytes, headroom=1.10):
        """Fail BEFORE the transfer, not 160 shards into it.

        The ENOSPC that motivated this module surfaced after ~1.4 TB had already
        moved. A one-syscall precheck turns that into an immediate, actionable
        error naming the shortfall.
        """
        free = self.free_bytes()
        need = int(need_bytes * headroom)
        if free < need:
            raise OSError(
                f"local checkpoint pool has {free/1e9:.0f} GB free across "
                f"{len(self.paths)} drive(s) but the model needs "
                f"{need/1e9:.0f} GB (incl. {int((headroom-1)*100)}% headroom). "
                f"Pool: {self.paths}. Free space or pass a larger pool.")
        return True

    def target(self, rel):
        """Where `rel` physically goes, plus the symlink to create (or None)."""
        if not self.striped:
            return os.path.join(self.model_dir, rel), None
        # zlib.crc32 rather than hash(): PYTHONHASHSEED randomizes str hashing
        # per process, so hash() would place the same shard differently in every
        # worker and break both determinism and the reuse check.
        import zlib
        i = zlib.crc32(rel.encode()) % len(self.paths)
        if self._dir_on_pool and i == 0:
            # paths[0] IS the model directory: write in place, no link needed.
            return os.path.join(self.model_dir, rel), None
        real = os.path.join(self.paths[i], self.model_name, rel)
        link = os.path.join(self.model_dir, rel)
        return real, link


def build_pool(model_name, local_dir=None, allow_format=True):
    """The one call a caller needs: a CheckpointPool over local NVMe.

    `allow_format` defaults to TRUE here, unlike `ensure_pool`. This function is
    the "GPU node, instance store is scratch" entry point — that is what the
    drives are for on a training host, and defaulting to False here would
    reproduce the original bug (30 TB idle, root volume full) for every caller
    who did not know to opt in. Callers who must not touch unmounted drives call
    `ensure_pool(allow_format=False)` themselves and pass the result.

    When `local_dir` is given, it REMAINS the model directory — callers depend on
    the path they passed — but the shards are still striped across local NVMe if
    any exists. The caller keeps its contract; the transfer keeps the bandwidth.
    """
    paths = ensure_pool(allow_format=allow_format)
    if not paths:
        if local_dir:
            logger.warning(
                f"no local instance-store NVMe; writing to {local_dir} as given. "
                f"On a p5-class node this means the drives are missing or "
                f"unmountable — check `lsblk -d -o NAME,MODEL`.")
            return CheckpointPool([], model_name, single_dir=local_dir)
        fallback = os.path.join("/mnt/models", model_name)
        logger.warning(f"no local NVMe pool; using {fallback} on the root volume "
                       f"— check free space, this is the ENOSPC path")
        return CheckpointPool([], model_name, single_dir=fallback)
    # Pool exists. Honour the caller's directory if it gave one, but stripe the
    # bytes across the drives regardless.
    return CheckpointPool(paths, model_name, model_dir=local_dir)
