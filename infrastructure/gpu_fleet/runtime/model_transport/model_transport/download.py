"""Multi-ENI model downloader: every network card, one shard per process.

WHAT THIS REPLACES. fetch.py runs one stream per file with
TransferConfig(use_threads=False), which measures ~270 MB/s and takes ~1.4 h for
GLM-5's 1.37 TiB. This module measured 8.0 GB/s moving 1,286 GB in 161 s on
p5.48xlarge — roughly 30x, ~3 min for the same
model. Both numbers are on real glm-5 safetensors shards from the region-local
bucket; nothing here is synthetic.

THE FIVE THINGS IT DOES, IN ORDER
  0. REUSE CHECK. If the model is already complete on local disk, return
     immediately without touching S3. GPU nodes get reused across runs and a
     spot node that comes back with a warm root volume should not pay 3 minutes
     again. Completeness is per-file size equality against the S3 listing (model
     prefixes are immutable, so size is a sufficient identity check), and a
     PARTIAL directory is repaired file-by-file rather than restarted.
  1. ENI DISCOVERY. Read every IP-carrying interface off the OS, then keep at
     most one per NETWORK CARD. Bandwidth on p5 is per card (32 x 100 Gbps), so
     two ENIs on one card share that card's 100 and buy nothing; the card index
     comes from the EC2 API, matched to the OS interface by MAC address.
  2. SHARD COUNT. List the model's objects. Shards are the unit of work.
  3. EQUAL DISTRIBUTION. Deterministically assign shards to ENIs, largest-first
     so total BYTES per ENI are balanced rather than merely file counts.
  4. ONE SHARD PER PROCESS, capped at PROCS_PER_ENI_CAP (30). Fewer shards than
     the cap means fewer processes (10 shards per ENI -> 10 processes); more
     means processes take several shards SEQUENTIALLY (60 shards per ENI -> 30
     processes x 2 each). The cap exists because process count is not free: at
     30 per ENI, per-process throughput was already 79.8 MB/s against 277 at 7.5
     per ENI, so the aggregate is a fixed budget being divided, and more
     processes only add context-switching and buffer pressure.

WHY 30 IS A CAP AND NOT A TARGET. Measured on 4 cards: 30 procs total = 8.019
GB/s, 120 procs (30/ENI) = 7.992 GB/s. Quadrupling processes changed nothing, so
the ceiling is elsewhere (host memcpy or a per-instance S3 allocation, still
open). 30/ENI is used because it is the configuration under test, it is
harmless, and it keeps every card busy through shard-boundary stalls — not
because it is known optimal. It is a flag for that reason.

ASYNC DISK MIRROR, WITH OWNERSHIP TRANSFER. Shards land in RAM and are handed to
writer threads through a bounded queue. Disk is off the critical path: isolated
single-shard timings are RAM 59.75 s vs NVMe 61.68 s (2%), but at 30 concurrent
processes disk cost 49% (4568 -> 2337 MB/s), so writers run independently and a
slow disk delays only the mirror.

The queue TRANSFERS OWNERSHIP of the buffer; it does not copy it. The first
version recycled one buffer per worker and therefore had to hand the mirror a
`bytes()` COPY, doubling the footprint of every in-flight shard and adding a 5
GiB memcpy on the exact memory bus the download is contending for. Now each shard
gets a fresh buffer, the worker drops its reference on submit, and the writer
frees it after `os.replace`. Exactly one copy of a shard exists at any time.

MEMORY IS BOUNDED BY IN-FLIGHT SHARDS, NOT BY PROCESS COUNT. This distinction is
a measured bug fix. A static formula (`procs x max_shard x mirror_depth`) decided
that 282 processes could not fit in 2 TiB and throttled the run to ONE process per
ENI: 1.08 GB/s over 32 cards, worse than a 4-card run, with 31/32 of the hardware
idle. Process count is what keeps the cards busy and is the last thing that should
give. So a cross-process SLOT SEMAPHORE caps concurrent in-flight shards instead:
every process starts, and a worker blocks before ALLOCATING when the RAM budget is
committed. Full concurrency, bounded memory, and back-pressure that lands on the
one resource that is actually scarce.

LOCAL NVMe IS THE CHECKPOINT TARGET. See nvme.py. The mirror writes to the
instance-store drives, striped across all of them, never to the EBS root — which
is both too small for a 1.37 TiB model and 5-9x too slow to keep up with the
download.

GATEWAY ENDPOINT REQUIRED. Cards 1..n-1 have no public IP, so they can only
reach S3 through a VPC S3 GATEWAY endpoint (Interface style would meter and
bottleneck it). `preflight()` verifies per-interface reachability and says so
plainly rather than hanging, which is what happened before the endpoint existed.
A gateway endpoint is REGION-SCOPED ("available only in the Region where you
created it"), so the secondary cards can read the REGION-LOCAL REPLICA and
nothing else. That is why resolve.py resolves the replica first and why a
region with no replica needs the public IP of a 1-card shape.

EFA IS IRRELEVANT TO THIS FILE — do not reintroduce it. An EFA attachment
creates TWO devices, an EFA device and an ENA device, and the IP lives on the
ENA half; AWS: "While the ENA device offers traditional IP networking" and
"Normal IP traffic from the ENA device of an EFA interface remains routable."
So a plain `interface` ENI carries a parallel download stream exactly as an
`efa` one does, and PARALLEL MULTI-ENI DOWNLOADS DO NOT DEPEND ON EFA. This
module deliberately keys on IP-carrying interfaces (see `_os_interfaces`) and
reads no EFA attribute anywhere; the single EFA-shaped fact below is the
EXCLUSION of `efa-only` ENIs, which have no ENA device and therefore no IP.
"""
import json
import logging
import os
import queue
import socket
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import boto3
import botocore.config

logger = logging.getLogger(__name__)

# Processes per ENI. A CAP, not a target — see module docstring.
PROCS_PER_ENI_CAP = 30

# Byte-range GETs per shard. One stream per file is ~88 MB/s (the
# single-connection S3 ceiling); 32 ranges on the same file took 8.81 s vs
# 60.92 s = 6.9x. Measured sweep: 16 -> 6.41/6.66, 32 -> 6.49/6.61,
# 64 -> 6.29/5.67, 96 -> 6.33/5.72 GB/s. 32 is the flat top of that curve.
RANGES_PER_SHARD = 32

# Read granularity. A single 5 GiB read materializes a second copy of the shard
# and the host starts swapping at 120 processes.
CHUNK = 8 << 20


# --------------------------------------------------------------- ENI discovery


def _sh(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True,
                          text=True).stdout.strip()


def _os_interfaces():
    """IP-carrying, UP interfaces from the OS, with their MACs.

    The OS, not the EC2 API, is the authority on what is USABLE: the API reports
    what is ATTACHED, and the gap between those two is the whole bug class here
    (an `efa-only` ENI is attached and has no IP; an ENI attached without policy
    routing has an IP the kernel will not source traffic from).
    """
    out = json.loads(_sh("ip -j addr show") or "[]")
    nics = []
    for link in out:
        name = link.get("ifname")
        if name == "lo" or link.get("operstate") not in ("UP", "UNKNOWN"):
            continue
        for a in link.get("addr_info", []):
            if a.get("family") == "inet":
                nics.append({"iface": name, "ip": a["local"],
                             "mac": (link.get("address") or "").lower(),
                             "mtu": link.get("mtu")})
    return nics


def _card_by_mac(region):
    """MAC -> network card index, from IMDS.

    IMDS rather than describe-instance-types because this must work with only an
    instance profile, and it reports the card index for THIS instance's actual
    attachments. Returns {} off-instance (laptop, CI) so discovery degrades to
    "trust the OS list" instead of failing.
    """
    try:
        tok = _sh('curl -s -X PUT -m 2 "http://169.254.169.254/latest/api/token" '
                  '-H "X-aws-ec2-metadata-token-ttl-seconds: 60"')
        if not tok:
            return {}
        h = f'-H "X-aws-ec2-metadata-token: {tok}"'
        base = "http://169.254.169.254/latest/meta-data/network/interfaces/macs"
        macs = _sh(f'curl -s -m 2 {h} {base}/').split()
        out = {}
        for m in macs:
            m = m.strip("/")
            idx = _sh(f'curl -s -m 2 {h} {base}/{m}/network-card')
            # A card index of 0 is valid, so test for digits, not truthiness.
            out[m.lower()] = int(idx) if idx.strip().isdigit() else 0
        return out
    except Exception as e:                                   # pragma: no cover
        logger.debug(f"IMDS card lookup unavailable: {e}")
        return {}


@dataclass
class Eni:
    iface: str
    ip: str
    card: int
    mtu: int = 0


def discover_enis(max_enis=None, region=None):
    """STEP 1: every usable ENI, at most ONE PER NETWORK CARD.

    Deduplicating by card is the correctness point: p5.48xlarge allows 2 ENIs
    per card, but bandwidth is per CARD, so a second ENI on card 0 doubles the
    apparent ENI count and delivers nothing. When IMDS is unavailable, every
    interface is treated as its own card — optimistic, but the alternative is
    refusing to run outside EC2.
    """
    nics = _os_interfaces()
    cards = _card_by_mac(region)
    seen, out = set(), []
    for i, n in enumerate(nics):
        card = cards.get(n["mac"], i)
        if card in seen:
            logger.debug(f"{n['iface']} shares card {card}; skipping "
                         f"(bandwidth is per card)")
            continue
        seen.add(card)
        out.append(Eni(iface=n["iface"], ip=n["ip"], card=card,
                       mtu=n.get("mtu") or 0))
    out.sort(key=lambda e: e.card)
    return out[:max_enis] if max_enis else out


def preflight(enis, bucket, region, timeout=8):
    """Prove every ENI can actually reach S3 before committing to a transfer.

    Without this the failure mode is a hang, not an error: a secondary ENI with
    no public IP and no S3 gateway route produced HTTP 000 after an 8 s timeout
    while card 0 returned 307 in 17 ms, and the run sat there indefinitely. Cheap
    insurance — one HEAD per ENI.
    """
    bad = []
    unverified = []
    for e in enis:
        try:
            c = _bound_client(region, e.ip, pool=4, connect_timeout=timeout)
            c.head_bucket(Bucket=bucket)
            # Verify the KERNEL agrees, not just that the call succeeded: a bind
            # the routing layer ignores still returns 200 from card 0. Probe the
            # SAME endpoint the client just used, rather than a guessed hostname
            # — guessing is what made this check fail inside the container.
            got = _observed_source_ip(region, timeout,
                                      host=c.meta.endpoint_url.split("//")[-1])
            if got is None:
                # INCONCLUSIVE, NOT FAILED. head_bucket already proved this ENI
                # reaches S3; we just cannot independently confirm which card
                # carried it. Post-hoc /proc/net/dev attribution still covers
                # this, so proceed and say so.
                unverified.append(e.iface)
            elif got != e.ip:
                bad.append({"iface": e.iface, "ip": e.ip, "card": e.card,
                            "error": f"bound to {e.ip} but kernel used {got} — "
                                     f"policy routing missing for this ENI"})
        except Exception as ex:
            bad.append({"iface": e.iface, "ip": e.ip, "card": e.card,
                        "error": str(ex)[:200]})
        finally:
            # RESTORE. Leaving the patch installed in the PARENT is what poisoned
            # every forked worker: children inherited a _Bound class pinned to
            # the last ENI checked here, and their own bind then failed EINVAL.
            socket.socket = _REAL_SOCKET
    if bad:
        raise RuntimeError(
            f"{len(bad)} of {len(enis)} ENIs cannot reach s3://{bucket}: {bad}. "
            "Most likely the VPC has no S3 GATEWAY endpoint (secondary ENIs have "
            "no public IP, so the IGW path does not exist for them), or policy "
            "routing is missing so the source bind is a no-op.")
    if unverified:
        logger.warning(
            f"{len(unverified)} of {len(enis)} ENIs reached S3 but their source "
            f"card could not be independently confirmed (endpoint DNS probe "
            f"unavailable): {unverified[:4]}{'...' if len(unverified) > 4 else ''}. "
            f"Proceeding — per-interface byte counters still verify attribution "
            f"after the transfer.")
    return True


# ------------------------------------------------------------------- transport


# The PRISTINE socket class, captured at import before anything can patch it.
# THIS LINE IS LOAD-BEARING. The first version did `real = socket.socket` inside
# _bound_client, which re-reads whatever is installed NOW. Two bugs followed,
# both measured 2026-09-15:
#   1. NESTING. preflight() patches once per ENI in the parent, so by fork time
#      socket.socket was already a _Bound subclass pinned to the last ENI's IP.
#      A child then built _Bound(_Bound(...)); super().__init__() bound to the
#      STALE parent IP first, our own bind() failed EINVAL, and the
#      `except OSError: pass` swallowed it. Every worker silently used one card.
#   2. The swallowed error made it invisible: 70 Gbps on enp71s0 and exactly
#      0.00 on the other three, with host policy routing verifiably correct.
# Subclassing the pristine class makes the patch idempotent and un-nestable.
_REAL_SOCKET = socket.socket


def _bind_socket_class(ip):
    """A socket class that binds to `ip`, built from the PRISTINE class."""

    class _Bound(_REAL_SOCKET):
        def __init__(self, family=-1, type=-1, proto=-1, fileno=None):
            super().__init__(family, type, proto, fileno)
            # Only INET stream sockets are bindable to an IPv4 source address;
            # anything else (AF_UNIX for credentials, AF_NETLINK) must be left
            # alone rather than have its failure swallowed.
            if family in (socket.AF_INET, -1) and fileno is None:
                try:
                    self.bind((ip, 0))
                except OSError as e:
                    # LOUD, not silent. A failed bind is the whole bug class.
                    raise OSError(
                        f"could not bind source socket to {ip}: {e}. This is "
                        f"the multi-ENI failure mode — the transfer would run "
                        f"on one card while reporting N.") from e

    return _Bound


def _bound_client(region, ip, pool, connect_timeout=10):
    """boto3 S3 client whose every socket leaves from `ip`.

    botocore exposes no bind hook, so socket creation is patched process-wide.
    Call this ONCE per process, at the top of a worker: the patch is global to
    the interpreter, so two different IPs cannot coexist in one process. That is
    exactly why _shard_worker is a process and not a thread.

    NECESSARY BUT NOT SUFFICIENT: with several ENIs in one subnet, Linux answers
    from whatever the main route table picks, so without `ip rule from <ip>
    lookup <table>`, a per-table default route, and rp_filter=2, this bind is
    decoration. Hence attribution is measured, never assumed.
    """
    if ip:
        socket.socket = _bind_socket_class(ip)
    return boto3.client("s3", region_name=region,
                        config=botocore.config.Config(
                            max_pool_connections=pool,
                            connect_timeout=connect_timeout,
                            retries={"max_attempts": 3, "mode": "standard"},
                            tcp_keepalive=True))


def _observed_source_ip(region, timeout=8, host=None):
    """The source address the kernel ACTUALLY uses for an S3 connection.

    Proof rather than intent: opens one real connection to the regional S3
    endpoint through the currently-installed socket class and reports
    getsockname(). A worker that thinks it is bound to card 3 but whose packets
    leave card 0 is caught here, in the worker, before it moves 375 GB.

    Returns None — meaning "could not determine" — when the probe itself fails,
    which callers MUST treat differently from a wrong address. Measured
    2026-09-15: DNS for the regional endpoint failed inside the training
    container ("Name or service not known"), this function returned the string
    "unknown (...)", and preflight compared that against the expected IP and
    declared all 32 working ENIs unroutable. A 6.2 GB/s path fell back to 0.29
    GB/s because an inconclusive probe was read as a definitive failure.
    """
    hosts = [host] if host else [f"s3.{region}.amazonaws.com",
                                 f"s3.dualstack.{region}.amazonaws.com",
                                 "s3.amazonaws.com"]
    last = None
    for h in hosts:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((h, 443))
                return s.getsockname()[0]
        except Exception as e:
            last = e
    logger.debug(f"source-IP probe inconclusive for {hosts}: {last}")
    return None


def _get_shard(client, bucket, key, size, ranges, out):
    """One shard into `out` (any writable buffer or memoryview) via parallel
    byte-range GETs. Ranges are disjoint, so the threads never contend."""
    mv = out if isinstance(out, memoryview) else memoryview(out)
    step = (size + ranges - 1) // ranges

    def part(i):
        a = i * step
        b = min(size, a + step) - 1
        if a > b:
            return 0
        body = client.get_object(Bucket=bucket, Key=key,
                                 Range=f"bytes={a}-{b}")["Body"]
        n, off = 0, a
        while True:
            c = body.read(CHUNK)
            if not c:
                break
            mv[off:off + len(c)] = c
            off += len(c)
            n += len(c)
        return n

    with ThreadPoolExecutor(max_workers=ranges) as ex:
        return sum(ex.map(part, range(ranges)))


# ------------------------------------------------------------------- RAM budget


class ByteSlots:
    """A cross-process semaphore over BYTES of in-flight shard buffers.

    THE POINT: memory is bounded by how many shards are resident at once, which
    has nothing to do with how many processes exist. The previous design computed
    `procs x max_shard x mirror_depth` up front and, finding 282 procs x 20 GiB
    over budget on a 2 TiB host, cut the run to ONE process per ENI — 1.08 GB/s
    across 32 cards, worse than four cards, because it throttled the dial that
    drives throughput to satisfy a limit on a different resource entirely.

    Here every process starts and every card stays busy; a worker blocks in
    `acquire()` before allocating when the budget is committed, and gets through
    the moment a writer frees a shard. Back-pressure lands on RAM, which is the
    scarce thing, instead of on parallelism, which is the thing we are buying.

    Counted in UNITS of `unit_bytes` (one max-size shard) rather than raw bytes
    because multiprocessing.Semaphore is a counting semaphore over integers. A
    shard smaller than a unit still takes a whole unit: over-reserving slightly
    is the safe direction, and glm-5's shards are near-uniform (median 4.99 GiB,
    max 5.00) so the waste is negligible.
    """

    def __init__(self, total_bytes, unit_bytes):
        import multiprocessing as mp
        self.unit = max(1, int(unit_bytes))
        self.units = max(1, int(total_bytes // self.unit))
        self._sem = mp.Semaphore(self.units)

    def _n(self, nbytes):
        return max(1, min(self.units, -(-int(nbytes) // self.unit)))

    def acquire(self, nbytes):
        for _ in range(self._n(nbytes)):
            self._sem.acquire()

    def release(self, nbytes):
        for _ in range(self._n(nbytes)):
            self._sem.release()


# The slot budget, published to workers by INHERITANCE via a Pool initializer.
# A multiprocessing.Semaphore cannot travel as a Pool.map argument — it raises
# "Semaphore objects should only be shared between processes through
# inheritance", because the underlying POSIX semaphore is not picklable. So the
# parent sets this global and forked children read it.
_SLOTS = None


def _init_worker(slots):
    global _SLOTS
    _SLOTS = slots


# ------------------------------------------------------------ shard scheduling


def plan_assignment(shards, n_enis, procs_per_eni_cap=PROCS_PER_ENI_CAP):
    """STEPS 2-4: shards -> ENIs -> processes. Pure function, hence testable.

    ENI balance is by BYTES, not file count: largest-first into whichever ENI
    currently holds the least. Round-robin would have been fine for glm-5 (median
    4.99 GiB, min 1.83, max 5.00) but not for checkpoints with one fat shard, and
    the earlier round-robin `items[p::procs]` actually produced a 90/72/60/60
    file split across four ENIs.

    Processes per ENI = min(shards_on_that_eni, cap). Under the cap, one shard
    per process. Over it, shards are dealt round-robin so each process gets a
    near-equal SEQUENTIAL list: 60 shards -> 30 processes x 2.

    Returns [[ (key,size), ... ] ...] — one inner list per process — plus the
    per-ENI grouping, so the caller can report and gate on it.
    """
    if n_enis < 1:
        raise ValueError("no ENIs")
    per_eni = [[] for _ in range(n_enis)]
    load = [0] * n_enis
    for key, size in sorted(shards, key=lambda s: (-s[1], s[0])):
        i = load.index(min(load))
        per_eni[i].append((key, size))
        load[i] += size

    plan = []
    for i, group in enumerate(per_eni):
        nprocs = min(len(group), procs_per_eni_cap)
        if nprocs == 0:
            plan.append([])
            continue
        buckets = [[] for _ in range(nprocs)]
        # Deal round-robin over a size-sorted group: consecutive picks go to
        # different processes, so each process's sequential run is a similar
        # number of similar-sized shards.
        for j, item in enumerate(group):
            buckets[j % nprocs].append(item)
        plan.append(buckets)
    return plan, per_eni


# ----------------------------------------------------------------- disk mirror


class DiskMirror:
    """Writes shards to disk OFF the critical path, taking OWNERSHIP of buffers.

    Threads, not processes: the payload is already a bytes-like object in this
    process's address space, and shipping 5 GiB through a multiprocessing queue
    would copy it twice (pickle + unpickle) to save GIL time that os.write
    releases anyway.

    THE QUEUE OWNS WHAT IT HOLDS. `submit()` takes the buffer and the caller must
    not touch it again; the writer deletes its reference after the rename and
    releases the caller's RAM slot. This is the difference between one copy of a
    shard in flight and two — the earlier design recycled the worker's buffer and
    so had to enqueue a `bytes()` copy, which both doubled peak RAM and put a 5
    GiB memcpy on the memory bus the download is competing for.

    Writes go to `<dest>.partial` then rename, so a killed run never leaves a
    truncated file that the reuse check would mistake for complete.
    """

    def __init__(self, pool, writers=4, queue_depth=2, slots=None):
        # Writers can be generous now that queued items are not copies and the
        # destination is a striped NVMe pool rather than one EBS volume: the
        # earlier 16/8 defaults were dangerous only because each slot held a
        # duplicate 5 GiB shard (writers+depth ~= 24 x 5 GiB = 120 GiB PER
        # WORKER, which drove free RAM to 72 GB and collapsed a 32-card run to
        # 1.92 GB/s). With ownership transfer, queue depth costs nothing beyond
        # the shard that already exists, and the slot semaphore — not this
        # number — is what bounds total memory.
        self.pool = pool
        self.q = queue.Queue(maxsize=queue_depth)
        self.errors = []
        self.written = 0
        self.slots = slots
        self._lock = threading.Lock()
        self.threads = [threading.Thread(target=self._run, daemon=True)
                        for _ in range(writers)]
        for t in self.threads:
            t.start()

    def _run(self):
        while True:
            item = self.q.get()
            if item is None:
                self.q.task_done()
                return
            rel, payload = item
            try:
                dest, link = self.pool.target(rel)
                os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
                tmp = dest + ".partial"
                with open(tmp, "wb") as f:
                    f.write(payload)
                    # DROP THE PAGE CACHE FOR WHAT WE JUST WROTE. Measured
                    # 2026-09-15: a 32-card run showed `buff/cache` at 597 GB
                    # and free RAM at 16 GB, and the network fell to 0 GB/s with
                    # 271 workers stuck in futex_wait_queue — the kernel was
                    # holding 1.5 TB of write-back pages that nothing will ever
                    # read, in direct competition with the shard buffers this
                    # transfer needs. Nobody re-reads a shard we just wrote from
                    # memory, so caching it is pure loss. fdatasync first
                    # because DONTNEED silently skips dirty pages.
                    f.flush()
                    try:
                        # fdatasync AND posix_fadvise are Linux-only; macOS has
                        # neither (this exact AttributeError failed all 16 writes
                        # in a local test). The whole block is best-effort: the
                        # bytes are already handed to the kernel by flush(), so
                        # skipping the cache hint costs performance on a big
                        # transfer and nothing at all on a small one.
                        os.fdatasync(f.fileno())
                        os.posix_fadvise(f.fileno(), 0, 0,
                                         os.POSIX_FADV_DONTNEED)
                    except (AttributeError, OSError):
                        pass
                os.replace(tmp, dest)
                if link:
                    # Striped placement: the model directory presents every
                    # shard, wherever it physically landed. Replace rather than
                    # skip, so a re-run repairs a link pointing at a drive that
                    # was reformatted.
                    os.makedirs(os.path.dirname(link) or ".", exist_ok=True)
                    try:
                        if os.path.islink(link) or os.path.exists(link):
                            os.unlink(link)
                    except OSError:
                        pass
                    os.symlink(dest, link)
                with self._lock:
                    self.written += len(payload)
            except Exception as e:
                with self._lock:
                    self.errors.append(f"{rel}: {e}")
            finally:
                # FREE, THEN RELEASE, IN THAT ORDER. Releasing the slot while
                # still holding the buffer would let another worker allocate
                # against RAM this one has not given back yet, and the budget
                # would drift upward under load — the exact overcommit the
                # semaphore exists to prevent.
                n = len(payload)
                del payload, item
                if self.slots:
                    self.slots.release(n)
                self.q.task_done()

    def submit(self, rel, payload):
        """Hand the buffer over. The caller must drop its reference immediately.

        Blocks when the queue is full — intentional back-pressure. An unbounded
        queue on a 1.37 TiB model is an OOM, not a speedup.
        """
        self.q.put((rel, payload))

    def close(self, wait=True):
        for _ in self.threads:
            self.q.put(None)
        if wait:
            for t in self.threads:
                t.join()
        return self.errors


# ------------------------------------------------------------------ reuse check


def local_status(local_dir, objs, prefix):
    """STEP 0: is this model already here? Returns (missing, present, bytes).

    Size equality per file is enough: objects under a versioned model prefix are
    immutable, so a same-size file is the same file, and paying a checksum per
    file on 1.37 TiB would cost more than the download it is meant to avoid. A
    truncated file from a killed run differs in size and so is refetched.
    """
    missing, present, have = [], 0, 0
    for key, size in objs:
        rel = key[len(prefix):].lstrip("/")
        dest = os.path.join(local_dir, rel)
        try:
            if os.path.getsize(dest) == size:
                present += 1
                have += size
                continue
        except OSError:
            pass
        missing.append((key, size))
    return missing, present, have


# ----------------------------------------------------------------------- report


@dataclass
class DownloadReport:
    bucket: str
    files: int
    bytes: int
    seconds: float
    local_dir: str
    enis: list = field(default_factory=list)
    procs: int = 0
    reused_files: int = 0
    reused_bytes: int = 0
    per_iface_gbps: dict = field(default_factory=dict)
    attributed: bool = False
    balance_worst_dev: float = None
    disk_errors: list = field(default_factory=list)
    #: WHERE the bytes came from, relative to where this process runs. Recorded
    #: rather than logged because a throughput number without these is not
    #: attributable: 270 MB/s is a healthy cross-region pull and a broken
    #: region-local one. `replica_status` carries resolve_bucket_ex's reason.
    node_region: str = ""
    bucket_region: str = ""
    replica_status: str = ""

    @property
    def gb_s(self):
        return self.bytes / max(self.seconds, 1e-6) / 1e9

    @property
    def mbps(self):
        return self.bytes / max(self.seconds, 1e-6) / 1e6

    def summary(self):
        return (f"{self.files} shards / {self.bytes/1e9:.1f} GB in "
                f"{self.seconds:.1f}s = {self.gb_s:.2f} GB/s "
                f"({self.gb_s*8:.1f} Gbps) over {len(self.enis)} ENIs "
                f"x {self.procs//max(1,len(self.enis))} procs"
                + (f"; reused {self.reused_files} files "
                   f"({self.reused_bytes/1e9:.1f} GB)"
                   if self.reused_files else "")
                + ("" if self.attributed else "  [UNATTRIBUTED - see logs]"))


# ------------------------------------------------------------------- attribution


def _dev_counters():
    """iface -> rx_bytes. Kernel ground truth for which card carried the bytes."""
    ctr = {}
    with open("/proc/net/dev") as f:
        for line in f.readlines()[2:]:
            name, _, rest = line.partition(":")
            fields = rest.split()
            if fields:
                ctr[name.strip()] = int(fields[0])
    return ctr


def _attribution(before, after, enis, wall):
    """Did the bytes land on the cards we bound to, in balanced amounts?

    Two independent checks, because they catch different bugs. COVERAGE (>=90% of
    wire bytes on bound interfaces) catches a bind that was ignored entirely.
    BALANCE (every card within 25% of the mean) catches ONE leaked binding, which
    coverage would hide inside a healthy-looking aggregate. Shard assignment is
    deterministic and byte-balanced, so real imbalance means routing, not
    scheduling.
    """
    delta = {k: after.get(k, 0) - before.get(k, 0)
             for k in set(before) | set(after)}
    delta = {k: v for k, v in delta.items() if v > (10 << 20) and k != "lo"}
    wire = sum(delta.values())
    bound = {e.iface for e in enis}
    hit = {k: v for k, v in delta.items() if k in bound}

    ok = bool(hit) and wire > 0 and sum(hit.values()) >= 0.9 * wire \
        and len(hit) >= len(bound)
    worst = None
    if hit:
        mean = sum(hit.values()) / len(hit)
        worst = round(max(abs(v - mean) / mean for v in hit.values()), 3) \
            if mean else 1.0
        if len(hit) > 1 and worst > 0.25:
            ok = False
    per_gbps = {k: round(v * 8 / wall / 1e9, 2) for k, v in sorted(delta.items())}
    return ok, worst, per_gbps


# -------------------------------------------------------------------- the driver


def _shard_worker(job):
    """ONE OS PROCESS = one logical worker: its ENI, its shards, sequentially.

    PROCESSES, NOT THREADS, and this is not a style choice — it is a correctness
    requirement that cost a debugging cycle to relearn on 2026-09-15. First cut
    ran each ENI group as threads in one interpreter. `_bound_client` patches the
    module-global `socket.socket`, so with N groups sharing a process the LAST
    patch wins and every socket binds to one IP: measured enp71s0 at 1.65 Gbps
    with enp72/73/74s0 at exactly 0.00. Precisely the silent-no-op this module's
    docstring warns about, reintroduced by sharing an interpreter. One process per
    worker makes the patch process-local and therefore correct.

    Threads also collapsed throughput independently: 4 x 30 x 32 = 3,840 range
    threads in one interpreter (3,123 observed) spent their time on the GIL, not
    on sockets. Range fan-out inside ONE worker is ~32 threads, which is fine.

    The disk mirror is per-process for the same reason a cross-process queue was
    never worth it: the payload is already in this process's address space, so a
    thread here writes it with no pickling, while sending 5 GiB to a writer
    process would copy it twice.
    """
    (bucket, region, ip, iface, seq, ranges, prefix,
     pool, mirror_writers) = job
    slots = _SLOTS          # inherited, not passed: see _init_worker

    # Reset first: a forked child inherits whatever the parent had installed, and
    # inheriting a foreign _Bound class is precisely how this failed before.
    socket.socket = _REAL_SOCKET
    client = _bound_client(region, ip, pool=max(16, ranges + 4))

    # SELF-CHECK before moving hundreds of GB. Cheap (one connection) and it
    # localizes the failure to the worker instead of leaving it to post-hoc
    # /proc/net/dev arithmetic.
    # `None` means the probe could not run (e.g. endpoint DNS unavailable in a
    # container), NOT that the binding is wrong — treating those the same turned
    # a 6.2 GB/s path into a 0.29 GB/s fallback once already. Only a CONFIRMED
    # mismatch aborts; post-hoc /proc/net/dev attribution catches the rest.
    src = _observed_source_ip(region, host=client.meta.endpoint_url.split("//")[-1])
    if src is not None and src != ip:
        return {"iface": iface, "bytes": 0, "files": 0, "disk_errors": [],
                "bind_error": f"worker for {iface} asked for {ip} but the kernel "
                              f"sourced from {src}"}

    mirror = DiskMirror(pool, writers=mirror_writers,
                        slots=slots) if pool else None
    total = files = 0
    try:
        for key, size in seq:
            # RESERVE BEFORE ALLOCATING. This is where back-pressure lives: if
            # the RAM budget is fully committed, this worker waits here instead
            # of the run having been pre-emptively narrowed to fewer processes.
            if slots:
                slots.acquire(size)
            # A FRESH buffer per shard, not a recycled one. Recycling is what
            # forced the old `bytes()` copy on submit: the mirror needed a
            # snapshot because the buffer was about to be overwritten. A fresh
            # allocation is handed away outright, so only one copy ever exists,
            # and page-faulting new pages is cheaper than a 5 GiB memcpy on a
            # memory bus this transfer is already saturating.
            buf = bytearray(size)
            try:
                n = _get_shard(client, bucket, key, size, ranges,
                               memoryview(buf))
                total += n
                files += 1
            except Exception:
                if slots:
                    slots.release(size)
                raise
            if mirror:
                # OWNERSHIP TRANSFERS HERE. `buf` must not be touched again; the
                # writer frees it and releases the slot. Drop our reference so
                # the only one left belongs to the queue.
                mirror.submit(key[len(prefix):].lstrip("/"), buf)
                buf = None
            else:
                # RAM-only mode (throughput measurement): nothing persists the
                # bytes, so free them and hand the slot back here.
                buf = None
                if slots:
                    slots.release(size)
    finally:
        # The mirror still holds queued shards; draining them releases their
        # slots, so this must complete before the worker exits or the budget
        # leaks and later workers block forever.
        errs = mirror.close(wait=True) if mirror else []
    return {"iface": iface, "bytes": total, "files": files, "disk_errors": errs}


def download_model(s3_uri, local_dir, *, region=None, max_enis=None,
                   procs_per_eni=PROCS_PER_ENI_CAP, ranges=RANGES_PER_SHARD,
                   mirror_to_disk=True, max_resident_gib=None,
                   mirror_writers=2, skip_reuse_check=False,
                   require_attribution=True, limit_bytes=None,
                   force_source=False):
    """Download every object under `s3_uri` using all available network cards.

    Steps in order: reuse check -> ENI discovery -> preflight -> shard listing ->
    byte-balanced ENI assignment -> per-ENI process groups -> RAM, with an
    asynchronous disk mirror. See the module docstring for why each exists.

    `require_attribution=True` fails the run if the kernel says the bytes did not
    arrive on the cards we bound to. That is deliberately strict: an unattributed
    transfer means the multi-ENI machinery silently did nothing, and a downloader
    that quietly delivers 1/32 of the intended bandwidth is worse than one that
    stops and says so.

    `limit_bytes` truncates the object list to the first whole shards under the
    cap, and `force_source` skips replica resolution. Both exist for the
    bench_gpu replica A/B, which times the same capped slice from the
    region-local replica and from the source bucket. Neither is a filter:
    `include=` would take this path out of the comparison entirely — it is the
    one condition that still turns the fast path off (see fetch._fast_path_ok) —
    so the legs would measure the thread-per-file fallback against the fan-out
    path and the ratio would be an artefact of the code, not of the region.
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"expected s3:// URI, got {s3_uri}")
    src_bucket, prefix = s3_uri[len("s3://"):].split("/", 1)
    prefix = prefix.rstrip("/")

    from .resolve import cap_objects, detect_region, resolve_bucket_ex
    bucket, bucket_region, objmap, replica_status = resolve_bucket_ex(
        src_bucket, prefix, region=region, force_source=force_source)
    if not objmap:
        raise FileNotFoundError(f"no objects under {s3_uri}")
    if limit_bytes is not None:
        objmap = cap_objects(objmap, limit_bytes)
    node_region = region if region is not None else detect_region()
    region = bucket_region
    objs = sorted(objmap.items())

    # THE CHECKPOINT POOL IS BUILT FIRST, because STEP 0 must look for a warm
    # cache in the place the mirror actually writes. Building it after the reuse
    # check — as the first version did — meant checking the caller's nominal
    # local_dir while shards lived on the striped NVMe pool, so a fully warm node
    # would re-download the entire model.
    pool = None
    if mirror_to_disk:
        from .nvme import build_pool
        model_name = os.path.basename(prefix.rstrip("/")) or "model"
        pool = build_pool(model_name, local_dir=local_dir)
        local_dir = pool.model_dir

    # STEP 0: reuse. Cheapest possible win on a recycled GPU node.
    reused_files = reused_bytes = 0
    todo = objs
    if not skip_reuse_check and local_dir:
        todo, reused_files, reused_bytes = local_status(local_dir, objs, prefix)
        if not todo:
            logger.info(f"REUSE: all {reused_files} files of {s3_uri} already in "
                        f"{local_dir} ({reused_bytes/1e9:.1f} GB); no download")
            return DownloadReport(bucket=bucket, files=0, bytes=0, seconds=0.0,
                                  local_dir=local_dir, reused_files=reused_files,
                                  reused_bytes=reused_bytes, attributed=True,
                                  node_region=node_region,
                                  bucket_region=bucket_region or "",
                                  replica_status=replica_status)
        if reused_files:
            logger.info(f"PARTIAL: {reused_files} of {len(objs)} files present "
                        f"({reused_bytes/1e9:.1f} GB); fetching {len(todo)}")

    # STEP 1
    enis = discover_enis(max_enis=max_enis, region=region)
    if not enis:
        raise RuntimeError("no IP-carrying interface found")
    logger.info(f"ENIs: {[(e.iface, e.card) for e in enis]}")
    preflight(enis, bucket, region)

    # STEPS 2-4
    plan, per_eni = plan_assignment(todo, len(enis), procs_per_eni)
    total_procs = sum(len(b) for b in plan)
    for e, group, buckets in zip(enis, per_eni, plan):
        logger.info(f"  {e.iface} (card {e.card}): {len(group)} shards, "
                    f"{sum(s for _, s in group)/1e9:.1f} GB, "
                    f"{len(buckets)} procs"
                    + (f", {max((len(b) for b in buckets), default=0)} "
                       f"shards deep" if buckets else ""))

    # RAM BUDGET AS A SEMAPHORE, NOT AS A PROCESS CAP. Peak resident bytes depend
    # on how many shards are in flight, not on how many processes exist, so the
    # budget throttles ALLOCATION and every process still runs. The predecessor
    # to this block computed procs x max_shard x mirror_depth, found 282 x 20 GiB
    # over budget, and cut the run to 1 proc/ENI: 1.08 GB/s over 32 cards, worse
    # than four cards, with the hardware idle. Never trade parallelism for memory
    # when a slot counter will do.
    resident = max_resident_gib or 0
    if not resident:
        try:
            total_gib = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") \
                / (1 << 30)
            resident = max(8.0, total_gib * 0.5)   # half of host RAM
        except (ValueError, OSError):               # pragma: no cover
            resident = 64.0
    max_shard = max((s for _, s in todo), default=1 << 20)
    slots = ByteSlots(int(resident * (1 << 30)), max_shard)
    logger.info(f"RAM budget {resident:.0f} GiB = {slots.units} in-flight shard "
                f"slots of {max_shard/(1<<30):.1f} GiB; {total_procs} procs all "
                f"run and block on allocation when the budget is committed")

    # CAPACITY PRECHECK against what remains to fetch. Fail now, naming the
    # shortfall, rather than 1.4 TB into the transfer as the original ENOSPC did.
    if pool:
        pool.check_capacity(sum(s for _, s in todo))
        logger.info(f"checkpoint pool: {len(pool.paths)} drive(s), "
                    f"{pool.free_bytes()/1e12:.1f} TB free, "
                    f"striped={pool.striped}, model_dir={pool.model_dir}")

    # FLATTEN to one job per logical worker. Each carries its ENI's IP, so all of
    # a group's workers bind to the same card while living in separate processes.
    jobs = []
    for e, buckets in zip(enis, plan):
        for seq in buckets:
            if seq:
                jobs.append((bucket, region, e.ip, e.iface, seq, ranges, prefix,
                             pool, mirror_writers))

    before = _dev_counters()
    t0 = time.time()
    # PROCESSES. See _shard_worker: the socket patch is process-global, so threads
    # would make every worker bind to whichever IP was patched last.
    import multiprocessing as mp
    with mp.Pool(len(jobs), initializer=_init_worker,
                 initargs=(slots,)) as procpool:
        results = procpool.map(_shard_worker, jobs)
    wall = time.time() - t0
    after = _dev_counters()

    bind_errors = [r["bind_error"] for r in results if r.get("bind_error")]
    if bind_errors:
        raise RuntimeError(
            f"{len(bind_errors)} of {len(results)} workers could not source "
            f"traffic from their assigned ENI: {bind_errors[:4]}")

    got_bytes = sum(r["bytes"] for r in results)
    got_files = sum(r["files"] for r in results)
    disk_errors = [e for r in results for e in r["disk_errors"]]
    ok, worst, per_gbps = _attribution(before, after, enis, wall)

    rep = DownloadReport(
        bucket=bucket, files=got_files, bytes=got_bytes, seconds=wall,
        local_dir=local_dir, enis=[e.iface for e in enis], procs=total_procs,
        reused_files=reused_files, reused_bytes=reused_bytes,
        per_iface_gbps=per_gbps, attributed=ok, balance_worst_dev=worst,
        disk_errors=disk_errors, node_region=node_region,
        bucket_region=bucket_region or "", replica_status=replica_status)
    logger.info(rep.summary())
    logger.info(f"per-iface Gbps: {per_gbps}")

    if disk_errors:
        # The RAM copy is authoritative and complete; the mirror is a backup, so
        # this is loud but not fatal.
        logger.error(f"{len(disk_errors)} disk mirror errors: {disk_errors[:5]}")
    if require_attribution and not ok:
        raise RuntimeError(
            f"transfer NOT attributable to the bound cards "
            f"(balance deviation {worst}, per-iface {per_gbps}). The per-ENI "
            f"binding did not take — check `ip rule from <ip> lookup <table>`, "
            f"per-table default routes, and rp_filter=2. Refusing to report "
            f"multi-ENI throughput that was really one card.")
    return rep
