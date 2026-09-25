#!/usr/bin/env python3
"""Per-shape GPU infrastructure attestation: record what this node is and how fast it is.

Run as a route of the fleet runtime image (`gpu-fleet bench_gpu ...`; the bench job
definitions pass `bench_gpu` as the command):

    gpu-fleet bench_gpu --model-uri s3://<weight-bucket>/base_models/qwen3-32b-dense \
              --cohort <cohort-id> --jd-gpus 8 --jd-chip h100 \
              --net-cards 32 --efa-supported yes \
              --report-uri s3://<bucket>/bench_gpu

TWO RULES GOVERN THIS FILE:

  1. NO GATES. Nothing here stops, exits non-zero, or refuses to continue because a measurement
     disagreed with an expectation. Every mismatch is a detailed log entry and a recorded field.
     There is no status token, no exit-code contract, and no grep protocol. Exit 0 unless the
     process dies.
  2. RECORD, NEVER REPAIR. A defect found in a specific compute environment is recorded, never
     fixed in passing, and never blocks a measurement.

WHY NO GATES, stated because the deleted code was not obviously wrong. The gates compared an
EC2-derived chip string and a job-definition GPU count against what torch reported. EC2 guarantees
instance type -> GPU model, so such a comparison cannot catch a hardware fact — only a bug in its
own string derivation. Its two possible outcomes were correct-and-silent, or a false positive that
dropped an entire shape out of the fleet table, where absence reads as "not deployed". Negative
expected value. Detection moves from the job to the reporting agent, which reads the node's own
self-report next to the queue's claim in the same record.

THE CONSEQUENCE, STATED PLAINLY. A degraded node produces a GREEN Batch job. That is accepted: a
non-zero exit makes Batch mark the job FAILED, and then "degraded" and "never ran" are the same
colour — which is the one distinction this tool exists to draw. The Batch console is not an output
of this tool; the JSON record is.

WHAT IT RECORDS, in the order a training step consumes it:

  inventory     both GPU counts (torch and the driver — they disagree usefully), chip, clocks,
                driver, topology, the node's own instance type from IMDS, spot vs on-demand.
  pool          the instance store, via local_disk.enforce_nvme — the one resolver that owns it.
  compute       bf16 dense matmul TFLOP/s per GPU. The roof on anything compute-bound.
  HBM           device-to-device copy GB/s. The roof on anything memory-bound.
  host->device  pinned PCIe / NVLink-C2C. Every weight crosses this once per load.
  disk          sequential write to the pool with O_DIRECT, because the page cache would
                otherwise report RAM speed. Checkpoints land here.
  collectives   intra-node all-gather / reduce-scatter busbw at the message size FSDP uses. The
                number that decides whether adding GPUs helps at all.
  EFA           what fabric hardware is attached and which transport NCCL ACTUALLY selects when
                NVLink and shared memory are taken off the table. See below.
  replica A/B   the same capped slice pulled from the region-local replica and then from the
                source bucket, both times recorded, no verdict.
  transport     S3 -> disk through the sharded multi-card downloader every job uses, so the
                number reported is the number every training job gets.

WHY THE EFA STAGE EXISTS SEPARATELY FROM THE COLLECTIVE STAGE. The intra-node collective cannot
see EFA at all: NVLink and shared memory carry that traffic, so an EFA fabric that has silently
fallen back to TCP produces a PERFECT intra-node score. The run that sat at 16 Gbps for weeks with
every signal green would have passed the unmodified benchmark cleanly. Forcing the net transport is
what lets a single-node job detect a multi-node fault, and it works because the fault is in the
image and the launch template, not in the network between nodes: NCCL that picks TCP on one node
will pick TCP on sixteen.

WHY AGAINST RATED PEAK AND NOT AGAINST LAST WEEK. Every expensive failure in this fleet has been
SILENT DEGRADATION, not an outage. A 32-card node ran the transport at a thirtieth of its measured
rate. Both would have been caught in ninety seconds by a probe that knows what the hardware is
rated for. Nothing that only checks for errors can catch a component working correctly at a tenth
of its speed.

IT DOES NOT TRAIN AND DOES NOT LOAD THE MODEL. `from_pretrained` drags in per-architecture remote
code, dtype negotiation and the attention-implementation dance, every one of which has failed here
for reasons that say nothing about the hardware (eager-attention OOM, MLA under-match, gpt_oss
`_supports_sdpa=False`). A benchmark that dies on `_supports_sdpa=False` cannot answer "how fast is
this shape". Synthetic matmuls at the sizes the real workload uses measure the same silicon without
the per-model lottery; the model match targets half of VRAM so a later load-and-generate stage
needs no re-planning.
"""
from __future__ import annotations

import argparse
import ctypes
import glob
import json
import logging
import os
import pathlib
import platform
import re
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time

logger = logging.getLogger("bench_gpu")

#: Vendor peak per chip. `bf16_tflops` is dense tensor-core bf16 WITHOUT the 2x sparsity
#: marketing figure; `hbm_gbs` is the memory-bandwidth spec; `net_gbps` is the instance's
#: rated AGGREGATE network bandwidth across all cards.
#:
#: These are PEAK, and no real kernel hits peak: a well-tuned large matmul lands at 75-85% and
#: a copy loop at 80-90% of HBM spec. The floor fractions below are set against peak with that
#: headroom already subtracted, so a flag means genuinely degraded and not merely imperfect.
#:
#: ⚠ This table is hand-entered vendor spec and is the only unverified input in the file. A
#:   wrong row makes a healthy shape flag or a degraded one pass. A floor that fires on every
#:   node of one chip is a bad row here, not a fleet of bad nodes.
#:
#: `net_gbps` IS NOT A TRANSPORT DENOMINATOR. It was one, and the ratio built on it demanded
#: 480 Gbps of S3 throughput on a p5 (15% of 3200) when the best figure ever measured in this
#: repo is 6.20 GB/s ~= 50 Gbps. S3 throughput is bound by per-connection and per-prefix limits,
#: not by NIC capacity, so no choice of fraction fixes that: the denominator is the wrong
#: resource. It survives here only as context for the EFA fabric stage, which does move data at
#: NIC speed.
CHIP_SPEC = {
    "a100":  {"bf16_tflops": 312,  "hbm_gbs": 1555, "net_gbps": 400},
    "a100-80": {"bf16_tflops": 312, "hbm_gbs": 2039, "net_gbps": 400},
    "h100":  {"bf16_tflops": 989,  "hbm_gbs": 3350, "net_gbps": 3200},
    "h200":  {"bf16_tflops": 989,  "hbm_gbs": 4800, "net_gbps": 3200},
    "b200":  {"bf16_tflops": 2250, "hbm_gbs": 8000, "net_gbps": 3200},
    "b300":  {"bf16_tflops": 2250, "hbm_gbs": 8000, "net_gbps": 3200},
    #: l4/l40s carry the FP32-ACCUMULATE rate, which is HALF the number the datasheet prints.
    #: Ada's tensor cores run bf16 with FP32 accumulate at half the FP16-accumulate rate, and
    #: `torch.matmul` on bf16 inputs always accumulates in FP32 — so the datasheet figure is
    #: unreachable by this probe, or by training. Measured on the first real L4 cohort
    #: (us-east-2, five g6/gr6 shapes): every card landed at 59.8-60.9 TFLOP/s, against the
    #: dense identity 58 SM x 1024 FLOP/clk x 2.04 GHz = 121.2 for FP16-accumulate. The old
    #: 121 row therefore scored healthy silicon at exactly 50% and SLOW_COMPUTE fired on 5 of
    #: 5 nodes — the "fires on every node of one chip" signature the table's own warning names.
    #: l40s is corrected by the same identity (142 SM x 1024 x 2.49 GHz = 362 FP16-accumulate),
    #: unmeasured: no L40S has run this benchmark yet.
    "l4":    {"bf16_tflops": 60.6, "hbm_gbs": 300,  "net_gbps": 25},
    "l40s":  {"bf16_tflops": 181,  "hbm_gbs": 864,  "net_gbps": 100},
}

#: Fraction of peak below which a stage is flagged. Compute and HBM only: there is deliberately
#: no `download` entry — see the CHIP_SPEC note on net_gbps for why a transport ratio against
#: NIC capacity was deleted rather than retuned.
FLOOR = {"compute": 0.55, "hbm": 0.60}

#: Slowest GPU as a fraction of the fastest in the same node. Tighter than the absolute floors
#: because this is a controlled comparison: the cards are identical parts in one chassis, so the
#: only legitimate spread is clock jitter. It catches the single-degraded-card fault that the
#: absolute floor cannot, since one card at 70% of peak still clears 55%.
#:
#: 92 and not 85: an earlier 85 was tested and MISSED THE EXACT FAULT IT EXISTS FOR — a card
#: 12.5% down cleared both 85% and the absolute floor. That near-miss is why this is a named
#: constant with its reasoning attached.
SPREAD_FLOOR = 0.92

#: all-gather busbw below which an NVLink-class node is flagged. Named, because the bare literal
#: it replaced could be read as arbitrary: NVLink-class shapes report hundreds of GB/s, and a
#: number in the tens means the cards are talking over PCIe or worse while every status signal
#: stays green.
NVLINK_FLOOR_GB_S = 100

#: Rule: any download from S3 taking longer than five minutes is flagged and investigated.
#:
#: ⚠ POST-HOC, NOT A DEADLINE, and intended as such. `seconds` exists only after fetch_model
#:   returns; nothing watches a clock during the transfer, and nothing is terminated. The only
#:   thing that actually kills a slow download is Batch's attemptDurationSeconds, which SIGKILLs
#:   the container and would write no report — which is why the record is emitted BEFORE the
#:   download begins.
#:
#: ⚠ MARGIN IS THIN AT THE TOP OF THE FLEET. The roster matches half of VRAM, so one time budget
#:   implies a 109x throughput spread: 46 MB/s on an L4 (gpt-oss-20b, 12.8 GiB) up to 5026 MB/s
#:   on a B300 (glm-5, 1404 GiB). Against the one measured figure — glm-5 in 243 s at 6.20 GB/s —
#:   B300 has 19% margin. A B300 budget flag is expected, not a fault.
SLOW_AFTER_S = 300.0

#: Matmul shape. Square, large enough that the GPU is compute-bound rather than launch-bound,
#: small enough that three of them fit in the smallest card's VRAM (L4, 22 GiB): three bf16
#: 8192^2 matrices is 0.4 GiB.
MATMUL_N = 8192
MATMUL_ITERS = 50

#: Bytes moved per bandwidth probe. 2 GiB per buffer keeps the working set far outside every
#: L2 in the fleet (B200's is 126 MiB) so the number is HBM and not cache.
BW_BYTES = 2 << 30
BW_ITERS = 20

#: One FSDP wrap unit's worth of all-gather. FSDP materializes a decoder layer at a time, so a
#: benchmark at 64 MiB reports a bandwidth this workload never sees — per-collective latency
#: amortizes completely differently. 2 GiB is a large-model layer unsharded.
COLL_BYTES = 2 << 30
COLL_ITERS = 20

#: Rendezvous ports, DISTINCT PER COLLECTIVE RUN and set unconditionally.
#:
#: The bug this replaces: `os.environ.setdefault("MASTER_PORT", "29577")` inside the worker. A
#: fixed port that setdefault will not overwrite means two collective runs in one job contend
#: for one port, and the second can bind-fail or — worse, because it produces a number — silently
#: join the first run's rendezvous and report its NVLink figure as the EFA figure.
COLL_PORT = 29577
EFA_PORT = 29579

#: Hard wall-clock ceiling per collective run, enforced on the result queue.
#:
#: There are now two of these runs, and a hung NCCL init would otherwise burn the whole Batch
#: timeout and take the download — the number most likely to be wanted — with it. 600 s was the
#: single-run timeout; 420 s each keeps the pair inside the old budget.
COLL_BUDGET_S = 420.0

#: Sequential write probe on the pool, with O_DIRECT.
#:
#: SIZE IS CONDITIONAL ON THE POOL. 4 GiB at ~5 GB/s finishes in under a second, which is inside
#: many NVMe SLC write caches — so on a large array the number would be the cache's and not the
#: device's. The old flat 4 GiB was chosen to finish quickly on the slowest volume here, which is
#: still served by keeping it on small pools.
DISK_BYTES_SMALL = 4 << 30
DISK_BYTES_LARGE = 16 << 30
DISK_LARGE_POOL_BYTES = 1 << 40        # 1 TiB
DISK_BLOCK = 8 << 20

#: The A/B's cap. Both legs identical, so the pair is comparable across shapes and costs minutes
#: on every one of them.
AB_CAP_BYTES = 50 << 30

GIB = 1 << 30

#: Every stage, in the order main() runs them. Named so --skip can address one.
PROBES = ("compute", "hbm", "h2d", "disk", "collectives", "efa")
STAGES = PROBES + ("replica_ab", "download")


def _chip_key(name: str, vram_gib: float) -> str:
    """Map a torch device name onto CHIP_SPEC.

    A100 is the one chip in the fleet that ships in two memory sizes with different bandwidth
    (p4d 40 GiB at 1555 GB/s, p4de 80 GiB at 2039), and the device name does not always say
    which. VRAM does.
    """
    n = name.lower().replace(" ", "")
    #: longest-first, because 'l4' is a substring of 'l40s' and a naive scan would judge every
    #: L40S against a third of its real compute spec
    for key in ("b300", "b200", "h200", "h100", "l40s", "l4", "a100"):
        if key in n:
            if key == "a100" and vram_gib > 60:
                return "a100-80"
            return key
    return "unknown"


def node_inventory() -> dict:
    """What the node has, from torch and from the driver, because they disagree usefully.

    torch reports what the CUDA runtime in THIS image can address — a Hopper-built image sees
    zero devices on a B200 while the driver sees eight — and nvidia-smi reports the hardware.
    Both, because the difference between them is itself the diagnosis:

        torch  smi  meaning
          0     8   wrong image for this chip: a fleet-plan or CDK image-routing bug
          0     0   no GPU attached: a node or launch-template bug
          8     8   fine
          4     8   the container sees fewer cards than the node has: a VISIBILITY finding,
                    i.e. the job definition's GPU request
    """
    info = {"host": socket.gethostname(), "platform": platform.platform()}
    try:
        import torch
        info["torch"] = torch.__version__
        info["cuda"] = torch.version.cuda
        info["cuda_available"] = bool(torch.cuda.is_available())
        n = torch.cuda.device_count() if torch.cuda.is_available() else 0
        info["gpu_count"] = n
        info["gpus"] = []
        for i in range(n):
            p = torch.cuda.get_device_properties(i)
            info["gpus"].append({
                "index": i, "name": p.name,
                "vram_gib": round(p.total_memory / GIB, 1),
                "capability": f"sm_{p.major}{p.minor}",
                "sms": p.multi_processor_count,
            })
        if info["gpus"]:
            g = info["gpus"][0]
            info["chip"] = _chip_key(g["name"], g["vram_gib"])
            info["vram_total_gib"] = round(sum(x["vram_gib"] for x in info["gpus"]), 1)
    except Exception as e:
        #: torch failing to import at all is a finding about the image, not a reason to abort
        #: before nvidia-smi has said what the hardware is.
        info["torch_error"] = f"{type(e).__name__}: {e}"
        info["gpu_count"] = 0
    smi = shutil.which("nvidia-smi")
    if smi:
        for label, q in (("driver", ["--query-gpu=driver_version", "--format=csv,noheader"]),
                         ("smi_gpus", ["--query-gpu=name,memory.total", "--format=csv,noheader"]),
                         ("clocks", ["--query-gpu=clocks.max.sm,clocks.sm,temperature.gpu,"
                                     "power.limit", "--format=csv,noheader"])):
            try:
                out = subprocess.run([smi, *q], capture_output=True, text=True,
                                     timeout=60, check=True).stdout.strip()
                info[label] = [l.strip() for l in out.splitlines() if l.strip()]
            except Exception as e:
                info[f"{label}_error"] = f"{type(e).__name__}: {e}"
        info["smi_gpu_count"] = len(info.get("smi_gpus", []))
        try:
            #: Topology says whether the cards are on NVLink or PCIe, which is the single
            #: biggest determinant of the collective number below. Without it a low busbw is
            #: ambiguous between "degraded NVLink" and "there was never NVLink here".
            info["topology"] = subprocess.run([smi, "topo", "-m"], capture_output=True,
                                              text=True, timeout=60).stdout.strip()
        except Exception as e:
            info["topology_error"] = f"{type(e).__name__}: {e}"
    else:
        info["smi_error"] = "nvidia-smi not on PATH"
    return info


def _imds(path: str, timeout: float = 2.0) -> str | None:
    """One IMDSv2 read, or None. Never raises: IMDS can be unreachable from a container whose
    host set a hop limit of 1, and that is a fact to record rather than a failure."""
    import urllib.request
    try:
        req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token", method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
        token = urllib.request.urlopen(req, timeout=timeout).read().decode()
        req = urllib.request.Request(
            f"http://169.254.169.254/latest/meta-data/{path}",
            headers={"X-aws-ec2-metadata-token": token})
        return urllib.request.urlopen(req, timeout=timeout).read().decode().strip()
    except Exception:
        return None


def node_identity() -> dict:
    """The instance type, ID, AZ and lifecycle the NODE ITSELF reports. Compared to nothing.

    Why it is here at all: `p5.48xlarge` and `p5.4xlarge` are both H100 and differ eightfold in
    GPUs and completely in network capability, so the chip alone cannot attribute a report to a
    shape. Only the node's own instance type can.

    Why it is not a gate: the same negative expected value as every other string comparison here,
    plus a dependency on IMDS being reachable from inside the container. Unreachable IMDS records
    `itype_source: imds-unreachable` and the run continues unaffected.

    Lifecycle is recorded because all P-family capacity is spot by policy, so an on-demand
    placement is a finding about how the CE was built — flagged UNEXPECTED_LIFECYCLE, not fixed.
    """
    itype = _imds("instance-type")
    out = {"itype_source": "imds" if itype else "imds-unreachable",
           "instance_type": itype,
           "instance_id": _imds("instance-id"),
           "availability_zone": _imds("placement/availability-zone"),
           "lifecycle": _imds("instance-life-cycle")}
    macs = _imds("network/interfaces/macs")
    if macs:
        out["imds_macs"] = [m.strip("/") for m in macs.split() if m.strip()]
        out["imds_mac_count"] = len(out["imds_macs"])
    return out


def batch_identity() -> dict:
    """Which queue / job / attempt / node this is, from the environment Batch sets.

    Recorded because the whole point of one-queue-per-instance-type is that placement is
    readable afterwards; a report without the queue name cannot be attributed to a shape.
    JOB_NUM_NODES and JOB_NODE_INDEX are the multi-node fields, reported so a number from a
    2-node job is never mistaken for a single-node one.

    ⚠ AWS_DEFAULT_REGION is recorded here as `sdk_default_region` and IS NOT THE REGION. The job
    definition pins it to the fleet's home region on every node regardless of placement, because
    the shared state lives there. The placement region comes from detect_region(), which resolves ECS task
    metadata -> IMDSv2 -> env vars LAST for exactly this reason. Reading the env var mislabels
    every node outside the home region, and region is now a partition key and the independent variable of
    the replica A/B. The env value is still recorded, because an unexpected value there is itself
    a finding.
    """
    keys = ("AWS_BATCH_JOB_ID", "AWS_BATCH_JOB_ATTEMPT", "AWS_BATCH_JQ_NAME",
            "AWS_BATCH_CE_NAME", "AWS_BATCH_JOB_NODE_INDEX", "AWS_BATCH_JOB_NUM_NODES",
            "BENCH_SHAPE")
    out = {k: os.environ[k] for k in keys if k in os.environ}
    out["sdk_default_region"] = os.environ.get("AWS_DEFAULT_REGION", "")
    return out


def pool_state() -> dict:
    """The pool report from the one resolver that owns it.

    `local_disk.enforce_nvme()` already returns exactly this record ({pool, required,
    devices[{path, free_gib, total_gib}], scratch}). Rebuilding it here would be a second
    implementation of the rule, and the two would drift — `pool_devices()` already recognises a
    mounted drive by comparing `st_dev` against the pool root's, which is why a Docker-created
    directory on the root volume is correctly excluded without a second check here.

    ⚠ THIS CAN RAISE, and it is the one condition in this job that ends a run. Under
    TRAIN_REQUIRE_NVME=1 (standing policy, never lowered) enforce_nvme refuses a node whose launch
    template mounted no instance store, because every write would land on a 30 GiB root — the
    fault where a batch of jobs died ENOSPC. That is not a gate this job implements;
    it is the standing platform guard. The caller records it and emits the report before letting
    it propagate.
    """
    import local_disk
    return local_disk.enforce_nvme(logger)


def _root_free_bytes() -> int | None:
    """Free bytes on the ROOT filesystem. Belt-and-braces for writes that are PARTLY misplaced,
    which no single-path check can see: the pool can be correctly mounted and a stage still put
    some of its bytes on `/`."""
    try:
        s = os.statvfs("/")
        return s.f_bavail * s.f_frsize
    except OSError:
        return None


def _root_total_gib() -> float | None:
    """Total size of the filesystem `statvfs("/")` answers for — the denominator to `_root_free_bytes`.

    Free space alone cannot say WHICH filesystem was measured; total size can. The ap-northeast-1
    ENOSPC fault was a 30 GiB root, so a `/` reporting hundreds of GiB total is by itself proof that
    `statvfs` is not describing that root.
    """
    try:
        s = os.statvfs("/")
        return round(s.f_blocks * s.f_frsize / GIB, 2)
    except OSError:
        return None


def _root_mount() -> dict:
    """What the container's `/` ACTUALLY is, from `/proc/self/mountinfo`. No verdict, no predicate.

    WHY THIS IS A SEPARATE, JUDGEMENT-FREE SLOT.  Two suppressions for ROOT_CONSUMED have been
    written and both were wrong (see `_root_shares_filesystem_with`), each because it encoded a
    GUESS about what `/` is instead of asking.  So this records the kernel's answer — fs type, the
    mount source, and for an overlay the `upperdir`/`lowerdir` options that name the backing
    directory — and predicates are built from it afterwards, if at all.  Recorded on every node
    including clean ones: the reason the two probes disagree is a property of the node, so a node
    with no disagreement is the control that tells us which property matters.

    `mountinfo` rather than `mount` or `df`: it is the only source that carries the per-superblock
    options (field 11 onward), which is where `upperdir=` lives.  Its format is fixed —
    `id parent major:minor root mountpoint opts - fstype source super_opts` — with ` - ` as the
    separator that makes the optional middle fields unambiguous.

    Returns `{}` when the file cannot be read, on the same principle as the rest of the probe: a
    missing measurement must be distinguishable from a measurement that came back empty, and a
    diagnostic slot may never be the thing that fails a run.
    """
    try:
        return _mountinfo_root(pathlib.Path("/proc/self/mountinfo").read_text())
    except OSError:
        return {}


def _mountinfo_root(text: str) -> dict:
    """The `/` entry of a `/proc/self/mountinfo` body, parsed. Pure, so it is testable off-node.

    Split on the file's own ` - ` separator rather than a fixed field index: the fields before it
    are variable in number (`shared:1`, `master:2` appear and vanish) and the `/` line on these
    nodes carries them. Everything after it is fixed at `fstype source super_opts`.
    """
    out: dict = {}
    for ln in text.splitlines():
        try:
            pre, post = ln.split(" - ", 1)
            pre_f, post_f = pre.split(), post.split()
            mountpoint = pre_f[4]
            if mountpoint != "/":
                continue
            out = {
                "fstype": post_f[0],
                "source": post_f[1] if len(post_f) > 1 else None,
                "major_minor": pre_f[2],
                #: The super-block options verbatim.  For overlayfs these name the backing
                #: directories, which is the whole point: `upperdir` is where writes to `/` really
                #: land, and whether that path is on the pool is the question ROOT_CONSUMED is
                #: really asking.  Kept as the raw string — parsing it into a verdict here is
                #: exactly the step that produced two wrong suppressions.
                "super_options": post_f[2] if len(post_f) > 2 else None,
                "mount_options": pre_f[5],
            }
        except (IndexError, ValueError):
            continue
    return out


def _root_shares_filesystem_with(paths: list[str]) -> str | None:
    """The pool path whose FILESYSTEM is also the container's `/`, or None.

    WHY THIS EXISTS. `_root_free_bytes()` watches `/` shrink and calls that misplaced bytes. On a
    container whose overlay upper layer lives ON a pool drive, correct writes shrink `/` by exactly
    their own size, so the delta is real and the conclusion is wrong. Measured 2026-09-21 across
    five g6/gr6 nodes: `statvfs("/").f_bavail` equalled the first instance-store drive's free space
    at four DIFFERENT values (222.70 / 407.58 / 546.23 / 860.52 GiB, tracking the drive size), and
    the root delta equalled the downloaded bytes to two decimals on all five. ROOT_CONSUMED fired
    5 of 5 while every byte was on the pool.

    Identity, not free space: two filesystems can report equal free bytes by coincidence, but
    `st_dev` is the kernel's own answer to "same filesystem". So the flag is suppressed only when
    the kernel says `/` and the pool drive ARE one filesystem — and a genuinely misplaced write on
    a node with a separate root still flags, which is the fault this probe was built for.
    """
    try:
        root_dev = os.stat("/").st_dev
    except OSError:
        return None
    for p in paths:
        try:
            if os.stat(p).st_dev == root_dev:
                return p
        except OSError:
            continue
    return None


def efa_inventory() -> dict:
    """What fabric hardware is attached and what software could use it. No measurement.

    Every field here has been the sole cause of a silent fallback at least once:

      efa_devices        the launch template attached no EFA ENI (Jakarta and Seoul reject every
                         EFA ENI on p6-b300 while describe_instance_types claims support — which
                         is precisely why this is measured on the node and not read from an API)
      fi_info providers  a provider with no device is a launch-template problem; neither is an
                         AMI problem
      aws-ofi-nccl       without the plugin NCCL NEVER uses EFA, whatever the hardware is
      libcudart.so       THE known cause. A multi-node run sat at 16 Gbps with every signal green
                         because the image shipped only the versioned libcudart.so.12 and the EFA
                         plugin needs the unversioned name to dlopen it. Hardware present, plugin
                         present, and still TCP.
      interfaces         feeds the multi-NIC fan-out attestation
      NCCL_*/FI_* env    an override baked into the image or the launch template can disable EFA
                         without anything erroring
    """
    inv = {}
    try:
        ib = sorted(os.path.basename(p) for p in glob.glob("/sys/class/infiniband/*"))
        inv["infiniband_devices"] = ib
        inv["efa_device_count"] = len([d for d in ib if "efa" in d.lower()]) or len(ib)
    except OSError as e:
        inv["infiniband_error"] = f"{type(e).__name__}: {e}"
        inv["efa_device_count"] = 0

    fi = shutil.which("fi_info")
    if fi:
        try:
            out = subprocess.run([fi, "-p", "efa"], capture_output=True, text=True, timeout=60)
            inv["fi_info_efa_rc"] = out.returncode
            inv["fi_info_efa"] = out.stdout.strip()[:4000] or out.stderr.strip()[:2000]
            inv["fi_info_providers"] = sorted(set(
                re.findall(r"provider:\s*(\S+)", out.stdout)))
        except Exception as e:
            inv["fi_info_error"] = f"{type(e).__name__}: {e}"
    else:
        inv["fi_info_error"] = "fi_info not on PATH"

    plugin = [p for pat in ("/opt/amazon/ofi-nccl/lib*/libnccl-net*.so*",
                            "/usr/local/lib/libnccl-net*.so*",
                            "/opt/aws-ofi-nccl/lib/libnccl-net*.so*",
                            "/usr/lib/x86_64-linux-gnu/libnccl-net*.so*")
              for p in glob.glob(pat)]
    inv["ofi_nccl_plugin"] = sorted(plugin)

    #: The unversioned name specifically. libcudart.so.12 existing proves nothing: dlopen of
    #: "libcudart.so" is what the plugin does, and that needs the development symlink.
    cudart = [p for pat in ("/usr/local/cuda/lib64/libcudart.so",
                            "/usr/local/cuda/targets/x86_64-linux/lib/libcudart.so",
                            "/usr/lib/x86_64-linux-gnu/libcudart.so")
              for p in glob.glob(pat)]
    if not cudart:
        try:
            import torch
            tl = os.path.join(os.path.dirname(torch.__file__), "lib")
            cudart = sorted(glob.glob(os.path.join(tl, "libcudart.so")))
        except Exception:
            pass
    inv["libcudart_unversioned"] = sorted(cudart)

    try:
        out = subprocess.run(["ip", "-j", "link"], capture_output=True, text=True, timeout=30)
        links = json.loads(out.stdout or "[]")
        inv["interfaces"] = sorted(l["ifname"] for l in links
                                   if l.get("ifname") != "lo")
        inv["interface_count"] = len(inv["interfaces"])
    except Exception as e:
        inv["ip_link_error"] = f"{type(e).__name__}: {e}"

    inv["fabric_env"] = {k: v for k, v in sorted(os.environ.items())
                         if k.startswith(("NCCL_", "FI_", "OFI_", "EFA_", "RDMAV_"))}
    return inv


def _time_cuda(fn, iters: int) -> float:
    """Median seconds per call, timed on the CUDA stream and not on the host clock.

    Events, warmup, and a median rather than a mean: the host clock measures queue submission
    (which can be microseconds while the kernel runs for milliseconds), the first call pays for
    autotuning and allocation, and one slow iteration from a clock excursion should not move
    the reported number.
    """
    import torch
    for _ in range(3):
        fn()
    torch.cuda.synchronize()
    times = []
    for _ in range(iters):
        s, e = torch.cuda.Event(enable_timing=True), torch.cuda.Event(enable_timing=True)
        s.record()
        fn()
        e.record()
        torch.cuda.synchronize()
        times.append(s.elapsed_time(e) / 1000.0)
    return statistics.median(times)


def bench_compute(dev: int) -> dict:
    """bf16 dense matmul TFLOP/s on one GPU. 2*N^3 flops per multiply."""
    import torch
    torch.cuda.set_device(dev)
    a = torch.randn(MATMUL_N, MATMUL_N, device=f"cuda:{dev}", dtype=torch.bfloat16)
    b = torch.randn(MATMUL_N, MATMUL_N, device=f"cuda:{dev}", dtype=torch.bfloat16)
    c = torch.empty_like(a)
    secs = _time_cuda(lambda: torch.matmul(a, b, out=c), MATMUL_ITERS)
    flops = 2.0 * MATMUL_N ** 3
    del a, b, c
    torch.cuda.empty_cache()
    return {"device": dev, "n": MATMUL_N, "seconds": round(secs, 6),
            "tflops": round(flops / secs / 1e12, 1)}


def bench_hbm(dev: int) -> dict:
    """Device-to-device copy GB/s. Every element is read once and written once."""
    import torch
    torch.cuda.set_device(dev)
    n = BW_BYTES // 2  # bf16
    src = torch.empty(n, device=f"cuda:{dev}", dtype=torch.bfloat16)
    dst = torch.empty_like(src)
    secs = _time_cuda(lambda: dst.copy_(src), BW_ITERS)
    moved = 2 * BW_BYTES
    del src, dst
    torch.cuda.empty_cache()
    return {"device": dev, "bytes_moved": moved, "seconds": round(secs, 6),
            "gb_per_s": round(moved / secs / 1e9, 1)}


def bench_h2d(dev: int) -> dict:
    """Pinned host -> device GB/s. Pinned, because pageable memory measures the copy the
    driver has to stage through its own bounce buffer and understates the link by half."""
    import torch
    torch.cuda.set_device(dev)
    host = torch.empty(BW_BYTES // 2, dtype=torch.bfloat16, pin_memory=True)
    devt = torch.empty_like(host, device=f"cuda:{dev}")
    up = _time_cuda(lambda: devt.copy_(host, non_blocking=True), BW_ITERS)
    down = _time_cuda(lambda: host.copy_(devt, non_blocking=True), BW_ITERS)
    del host, devt
    torch.cuda.empty_cache()
    return {"device": dev,
            "h2d_gb_per_s": round(BW_BYTES / up / 1e9, 1),
            "d2h_gb_per_s": round(BW_BYTES / down / 1e9, 1)}


def disk_bytes_for(path: str) -> int:
    """How much to write: 16 GiB on a pool larger than 1 TiB, 4 GiB otherwise.

    Conditional and not flat, because 4 GiB at ~5 GB/s finishes inside many NVMe SLC write
    caches, so on a large array the flat number measured the cache. Small pools keep 4 GiB
    because that is what finishes quickly on the slowest volume in the fleet.
    """
    try:
        s = os.statvfs(path)
        total = s.f_blocks * s.f_frsize
    except OSError:
        return DISK_BYTES_SMALL
    return DISK_BYTES_LARGE if total > DISK_LARGE_POOL_BYTES else DISK_BYTES_SMALL


def bench_disk(path: str) -> dict:
    """Sequential write to the pool, O_DIRECT where the kernel allows it.

    Without O_DIRECT this measures the page cache: a 4 GiB write on a 2 TiB-RAM p5 returns
    several GB/s the device cannot sustain, and the checkpoint that overruns its window in
    production looks fine here. O_DIRECT needs the buffer aligned to the block size, hence the
    ctypes allocation. If the filesystem refuses it, the fallback writes and fsyncs, which at
    least forces the data out, and the report says which path ran.
    """
    os.makedirs(path, exist_ok=True)
    target = disk_bytes_for(path)
    f = os.path.join(path, "bench_write.bin")
    block = DISK_BLOCK
    direct = hasattr(os, "O_DIRECT")
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | (os.O_DIRECT if direct else 0)
        fd = os.open(f, flags)
    except OSError:
        direct = False
        fd = os.open(f, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    try:
        if direct:
            #: O_DIRECT requires the user buffer aligned to the logical block size.
            raw = ctypes.create_string_buffer(block + 4096)
            addr = (ctypes.addressof(raw) + 4095) & ~4095
            buf = (ctypes.c_char * block).from_address(addr)
            buf[:] = b"\xa5" * block
            view = memoryview(buf).cast("B")
        else:
            view = memoryview(b"\xa5" * block)
        t0 = time.time()
        written = 0
        while written < target:
            written += os.write(fd, view)
        if not direct:
            os.fsync(fd)
        secs = time.time() - t0
    finally:
        os.close(fd)
        try:
            os.unlink(f)
        except OSError:
            pass
    return {"path": path, "bytes": written, "target_bytes": target,
            "seconds": round(secs, 2),
            "mb_per_s": round(written / secs / 1e6, 1),
            "o_direct": direct,
            "note": "" if direct else "O_DIRECT unavailable; fsync fallback may include cache"}


def _coll_worker(rank: int, world: int, q, port: int, extra_env: dict, debug_dir: str | None):
    """One rank of an intra-node collective probe. Spawned, because NCCL wants a process per
    GPU and this job is a single container process.

    `port` is set UNCONDITIONALLY (os.environ[...], not setdefault) and differs per run: two runs
    in one job sharing one port can silently join each other's rendezvous, which would report the
    NVLink figure as the forced-net figure — a wrong number, which is worse than no number.
    """
    import torch
    import torch.distributed as dist
    os.environ["MASTER_ADDR"] = "127.0.0.1"
    os.environ["MASTER_PORT"] = str(port)
    os.environ.update({k: str(v) for k, v in (extra_env or {}).items()})
    if debug_dir:
        #: Per-rank files, because all ranks share stderr and NCCL's own selection line is the
        #: only place the chosen transport is stated.
        os.environ["NCCL_DEBUG_FILE"] = os.path.join(debug_dir, "nccl.%h.%p.log")
    dist.init_process_group("nccl", rank=rank, world_size=world)
    torch.cuda.set_device(rank)
    shard = COLL_BYTES // world // 2
    inp = torch.empty(shard, device=f"cuda:{rank}", dtype=torch.bfloat16)
    out = torch.empty(shard * world, device=f"cuda:{rank}", dtype=torch.bfloat16)
    ag = _time_cuda(lambda: dist.all_gather_into_tensor(out, inp), COLL_ITERS)
    rs = _time_cuda(lambda: dist.reduce_scatter_tensor(inp, out), COLL_ITERS)
    if rank == 0:
        #: Standard NCCL bus-bandwidth correction, (n-1)/n for both collectives, so the numbers
        #: are directly comparable to published nccl-tests figures and to each other.
        corr = (world - 1) / world
        q.put({
            "world": world, "message_bytes": COLL_BYTES, "port": port,
            "all_gather_busbw_gb_per_s": round(COLL_BYTES * corr / ag / 1e9, 1),
            "reduce_scatter_busbw_gb_per_s": round(COLL_BYTES * corr / rs / 1e9, 1),
            "all_gather_seconds": round(ag, 6), "reduce_scatter_seconds": round(rs, 6),
        })
    dist.destroy_process_group()


def _run_collective(world: int, port: int, extra_env: dict | None = None,
                    debug_dir: str | None = None, budget_s: float = COLL_BUDGET_S) -> dict:
    """Spawn one rank per GPU, collect rank 0's result, and never outlive `budget_s`.

    The budget is the protection that makes probes-before-transport safe: a hung NCCL init would
    otherwise consume the whole Batch timeout, and the download — the number most likely to be
    wanted — would never be taken at all.
    """
    import torch.multiprocessing as mp
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    procs = [ctx.Process(target=_coll_worker,
                         args=(r, world, q, port, extra_env or {}, debug_dir))
             for r in range(world)]
    deadline = time.time() + budget_s
    for p in procs:
        p.start()
    try:
        res = q.get(timeout=max(1.0, budget_s))
    except Exception as e:
        res = {"error": f"collective did not report within {budget_s:.0f}s "
                        f"({type(e).__name__})"}
    finally:
        for p in procs:
            p.join(timeout=max(1.0, deadline - time.time()))
            if p.is_alive():
                p.terminate()
                p.join(timeout=30)
                if p.is_alive():
                    p.kill()
    return res


def bench_collectives(world: int) -> dict:
    """all-gather / reduce-scatter busbw across the node's GPUs, as NCCL prefers to run them.

    This is NVLink or shared memory on every shape that has either, which is why it cannot see
    EFA — see bench_efa.
    """
    if world < 2:
        return {"skipped": "single-GPU shape: no intra-node collective to measure"}
    return _run_collective(world, COLL_PORT)


def _net_provider(debug_dir: str) -> dict:
    """Which transport NCCL actually selected, read out of its own debug output.

    This is the whole point of the forced run: hardware inventory says what COULD be used, and
    only NCCL's selection line says what WAS. A node with EFA attached, the plugin installed, and
    `NET/Socket` here is the silent fallback caught in the act.
    """
    lines = []
    for p in sorted(glob.glob(os.path.join(debug_dir, "*"))):
        try:
            with open(p, errors="replace") as fh:
                lines += [l.rstrip() for l in fh if "NET/" in l or "Selected Provider" in l]
        except OSError:
            continue
    text = "\n".join(lines)
    provider = None
    m = re.search(r"Selected Provider is (\S+)", text)
    if m:
        provider = m.group(1).strip(",")
    elif "NET/OFI" in text:
        provider = "ofi"
    elif "NET/IB" in text:
        provider = "ib"
    elif "NET/Socket" in text:
        provider = "socket"
    return {"net_provider": provider,
            "is_efa": bool(provider and "efa" in provider.lower()),
            "nccl_net_lines": lines[:40]}


def bench_efa(world: int, inventory: dict, efa_supported: bool | None) -> dict:
    """Force NCCL onto a net provider and record which one it picks, plus the busbw it gets.

    `NCCL_SHM_DISABLE=1` and `NCCL_P2P_DISABLE=1` take shared memory and NVLink off the table, so
    NCCL must select a net transport. That is what lets a SINGLE-NODE job detect a MULTI-NODE
    fault: the fault is in the image and the launch template, not in the network between nodes,
    and NCCL that picks TCP on one node will pick TCP on sixteen.

    A shape with no EFA is SKIPPED WITH A REASON, never flagged. G-family has no EFA, and
    flagging it would be noise that gets the whole tool ignored.
    """
    if efa_supported is False:
        return {"skipped": "this instance type supports no EFA; a net-transport figure here "
                           "would be a TCP number with nothing to compare it to",
                "inventory": inventory}
    if world < 2:
        return {"skipped": "single-GPU shape: no collective to force onto the fabric",
                "inventory": inventory}
    if efa_supported is None and not inventory.get("efa_device_count"):
        return {"skipped": "no EFA device attached and no shape expectation supplied, so a "
                           "missing device cannot be distinguished from a shape without one",
                "inventory": inventory}
    with tempfile.TemporaryDirectory(prefix="nccl-efa-") as dbg:
        res = _run_collective(
            world, EFA_PORT,
            extra_env={"NCCL_SHM_DISABLE": "1", "NCCL_P2P_DISABLE": "1",
                       "NCCL_DEBUG": "INFO", "NCCL_DEBUG_SUBSYS": "INIT,NET"},
            debug_dir=dbg)
        res.update(_net_provider(dbg))
    res["inventory"] = inventory
    res["forced"] = {"NCCL_SHM_DISABLE": "1", "NCCL_P2P_DISABLE": "1"}
    return res


def _fetch_record(rep, wall: float, dest: str) -> dict:
    """One fetch's numbers, including everything the transport already knew and used to discard.

    `bytes_on_disk` is counted by walking the destination rather than taken from the report:
    "did the transfer claim success" and "did the bytes arrive" are different questions, and the
    second one is the one a corrupted or partial replica answers wrongly.

    `attributed` stays tri-state on purpose. None means the slow path ran and bound no cards, so
    there was nothing to attribute; False means a binding did not take. Collapsing them would put
    every single-ENI node in the defect column.
    """
    on_disk = sum(os.path.getsize(os.path.join(dp, f))
                  for dp, _, fs in os.walk(dest) for f in fs)
    return {
        "served_by_bucket": rep.bucket, "files": rep.files,
        "bytes": rep.bytes, "bytes_on_disk": on_disk,
        "gib": round(rep.bytes / GIB, 2), "seconds": round(wall, 1),
        "fetch_seconds": round(rep.seconds, 1), "local_dir": rep.local_dir,
        "mb_per_s": round(rep.bytes / max(wall, 1e-6) / 1e6, 1),
        "gb_per_s": round(rep.bytes / max(wall, 1e-6) / 1e9, 2),
        "gbps": round(rep.bytes * 8 / max(wall, 1e-6) / 1e9, 1),
        #: The fan-out attestation. Throughput alone cannot answer "did this use every card":
        #: if 32 are attached and 31 show a near-zero delta, the download ran on one NIC.
        "node_region": rep.node_region, "bucket_region": rep.bucket_region,
        "replica_status": rep.replica_status,
        "fast_path_reason": rep.fast_path_reason,
        "per_iface_gbps": rep.per_iface_gbps,
        "attributed": rep.attributed,
        "balance_worst_dev": rep.balance_worst_dev,
    }


def bench_download(model_uri: str, dest: str) -> dict:
    """The matched model through model_transport.fetch_model and nothing else.

    The foundational entry point, so the number reported here is the number every training job
    gets. A hand-rolled boto3 loop would measure something no job uses. Always the CANONICAL
    SOURCE URI: fetch_model resolves the region-local replica internally, and passing a replica
    bucket would pin every region to one bucket — the exact defect that once sent every job to
    the home region.
    """
    from model_transport import fetch_model
    t0 = time.time()
    rep = fetch_model(model_uri, dest)
    wall = time.time() - t0
    out = _fetch_record(rep, wall, dest)
    out["uri"] = model_uri
    out["model"] = model_uri.rstrip("/").rsplit("/", 1)[-1]
    return out


def bench_replica_ab(model_uri: str, dest_root: str, node_region: str,
                     cap_bytes: int = AB_CAP_BYTES) -> dict:
    """The same capped slice from the region-local replica, then from the source. No verdict.

    Every bench run downloads the same slice twice, sequentially: once from the region-local
    replica, once from the source bucket in the home region, and records the absolute and the
    relative difference. The point is a time series of regional slowdown, not a verdict: recorded,
    never judged. No flag, no threshold.

    REPLICA LEG FIRST, and sequential. That orders the confound conservatively — any warm-cache
    or connection-reuse advantage accrues to the CONTROL leg, so a recorded replica margin
    understates rather than flatters. Parallel legs would also compete for the same NICs, which
    destroys the per-interface attribution the full fetch depends on.

    The cap needs `limit_bytes` rather than `include=`: `include is not None` disables the fast
    download path, so a filtered control leg would compare the thread-per-file fallback against
    the fan-out path and the comparison would mean nothing.

    ⚠ `replica-absent` is the fact worth having, not a defect: the weights stack creates a
    replica in every fleet region, so it means a node outside the fleet's regions or a region
    whose replica was never created. `replica-incomplete` means replication has not caught up
    (CRR copies only objects written after the rule; older ones need S3 Batch Replication).
    """
    #: The skip is decided BEFORE the import, so a region with nothing to measure needs no
    #: transport machinery present to say so.
    if node_region == os.environ.get("GPU_FLEET_HOME_REGION", ""):
        return {"skipped": "source bucket is region-local here; both legs would be the same "
                           "bucket and the difference would measure nothing",
                "node_region": node_region}
    from model_transport import fetch_model
    out = {"node_region": node_region, "cap_bytes": cap_bytes,
           "cap_gib": round(cap_bytes / GIB, 1), "legs": {}}
    for leg, kwargs in (("replica", {}), ("source", {"force_source": True})):
        dest = os.path.join(dest_root, f"ab_{leg}")
        shutil.rmtree(dest, ignore_errors=True)
        print(f"=== replica_ab leg={leg} -> {dest}", flush=True)
        try:
            t0 = time.time()
            rep = fetch_model(model_uri, dest, limit_bytes=cap_bytes, **kwargs)
            out["legs"][leg] = _fetch_record(rep, time.time() - t0, dest)
        except Exception as e:
            logger.exception(f"replica A/B {leg} leg failed")
            out["legs"][leg] = {"error": f"{type(e).__name__}: {e}"}
        finally:
            #: Between the legs as well as at the end: fetch_model has skip_existing=True, so a
            #: warm destination would make the second leg a no-op reporting absurd throughput.
            shutil.rmtree(dest, ignore_errors=True)
        print(json.dumps(out["legs"][leg], indent=1), flush=True)

    rl, sl = out["legs"].get("replica", {}), out["legs"].get("source", {})
    if "seconds" in rl and "seconds" in sl:
        out["replica_seconds"] = rl["seconds"]
        out["source_seconds"] = sl["seconds"]
        out["delta_seconds"] = round(sl["seconds"] - rl["seconds"], 1)
        out["ratio"] = round(sl["seconds"] / max(rl["seconds"], 1e-6), 3)
        #: Both legs' byte counts, because the cap keeps WHOLE objects and a replica missing a
        #: shard would legitimately cap to a different total — in which case the ratio is between
        #: two different amounts of work and the reader must be able to see that.
        out["replica_bytes"] = rl.get("bytes")
        out["source_bytes"] = sl.get("bytes")
        out["comparable_sizes"] = rl.get("bytes") == sl.get("bytes")
        out["replica_status"] = rl.get("replica_status")
    return out


def evaluate(report: dict, slow_after: float = SLOW_AFTER_S) -> list[dict]:
    """Turn the numbers into recorded findings. Nothing here can stop a run.

    A "flag" is a recorded field and a log line, consumed by the reporting agent. Every one
    carries its own `investigate` list — the flag is produced on a spot node that will be gone,
    and whoever reads it later has the JSON and nothing else. Cross-references between flags in
    the same report are the merge dividend: a slow transport on an otherwise-healthy node is a
    network or replica problem, and on a degraded node it is one sick instance. Neither half of
    this job could say that alone.
    """
    chip = report.get("node", {}).get("chip", "unknown")
    spec = CHIP_SPEC.get(chip)
    flags = []

    #: A stage that raised is stored as {"error": ...} and a skipped one as {"skipped": ...}, and
    #: a dict is both truthy and iterable (over its KEYS), so `for c in comp` would hand every
    #: comparison below a string. Normalise once here rather than guarding at each use.
    def rows(stage) -> list[dict]:
        v = report.get(stage)
        return v if isinstance(v, list) else []

    def slot(stage) -> dict:
        v = report.get(stage)
        return v if isinstance(v, dict) else {}

    #: Every key this function indexes, required together. `_fetch_record` always produces all of
    #: them, so this is defence against a partial slot — and it matters because evaluate() runs
    #: AFTER the measurement it describes: a KeyError here would throw away a real download
    #: number to report a clerical one.
    dl = slot("download")
    dl = dl if {"seconds", "gbps", "gib", "mb_per_s", "files"} <= dl.keys() else None

    # ---- The absolute five-minute budget. Checked FIRST and independently of CHIP_SPEC,
    # because it is a rule about operations and must hold even for a chip with no spec row.
    if dl and dl["seconds"] > slow_after:
        flags.append({
            "flag": "SLOW_DOWNLOAD_BUDGET",
            "detail": f"{dl['gib']} GiB in {dl['seconds']}s ({dl['mb_per_s']} MB/s) exceeds the "
                      f"{slow_after:.0f}s budget",
            "investigate": [
                f"replica_status={dl.get('replica_status')!r}, served_by_bucket="
                f"{dl['served_by_bucket']} — a source-bucket serve outside us-east-1 means the "
                f"replica was incomplete or absent, which costs ~5x",
                f"fast_path_reason={dl.get('fast_path_reason')!r} — the thread-per-file fallback "
                f"is bounded by the largest single shard",
                f"per_iface_gbps has {len(dl.get('per_iface_gbps') or {})} interface(s); if one "
                f"carries everything, this is a fan-out fault and not a bandwidth one",
                f"{dl['files']} files / {dl['gib']} GiB — few-and-large is the case the "
                f"byte-range fan-out exists for",
                "compare against the compute and HBM flags in this same report: a slow "
                "transport on an otherwise-healthy node is a network or replica problem, on a "
                "degraded node it is one sick instance",
                "on a B300 matched to glm-5 this flag is EXPECTED: the budget leaves 19% margin "
                "against the best throughput ever measured here",
            ]})

    # ---- Bytes arrived, not merely claimed. Standing rule: SUCCEEDED without artifacts is not
    # success. A flag and not a failure, because the mismatch invalidates the throughput number
    # rather than the hardware table beside it.
    if dl and dl.get("bytes_on_disk") != dl.get("bytes"):
        flags.append({
            "flag": "BYTES_ON_DISK_MISMATCH",
            "detail": f"transport reported {dl['bytes']} bytes, {dl['bytes_on_disk']} found on "
                      f"disk under {dl.get('local_dir')}",
            "investigate": [
                "the throughput number in this report is not usable: it divides a byte count "
                "that did not arrive by a wall time that did",
                f"replica_status={dl.get('replica_status')!r} — a partial replica answers 'did "
                f"the transfer succeed' correctly and 'did the bytes arrive' wrongly",
                "size catches truncation and missing shards, not corruption; checksumming was "
                "considered and rejected on cost",
            ]})

    # ---- Multi-NIC fan-out. Direct evidence, which is what replaced the deleted bandwidth
    # ratio: if 32 cards are attached and 31 show a near-zero delta, the download ran on one NIC.
    if dl:
        reason = dl.get("fast_path_reason") or ""
        if reason.startswith("skipped") or reason.startswith("failed"):
            flags.append({
                "flag": "FAST_PATH_SKIPPED",
                "detail": f"the transport did not run the multi-card path: {reason}",
                "investigate": [
                    "the reason is the transport's own words, recorded verbatim",
                    "two gates that once suppressed the fast path — a 20 GiB size floor and a "
                    "2-ENI minimum — were REMOVED and must not return; a reason naming either "
                    "is a regression, not a configuration fact",
                ]})
        if dl.get("attributed") is False:
            flags.append({
                "flag": "NIC_FANOUT_INCOMPLETE",
                "detail": f"the transport's own attribution failed: "
                          f"balance_worst_dev={dl.get('balance_worst_dev')}, "
                          f"{len(dl.get('per_iface_gbps') or {})} interface(s) carried traffic",
                "investigate": [
                    "download.py's thresholds: coverage >= 90% of wire bytes on bound "
                    "interfaces (a bind ignored entirely), balance within 25% of the mean "
                    "(one leaked binding)",
                    "per_iface_gbps in the download slot names the interfaces and their rates",
                ]})
        nics = report.get("shape_expectation", {}).get("net_cards")
        present = (report.get("efa", {}).get("inventory", {})
                   or report.get("efa_inventory", {})).get("interface_count")
        if nics and present is not None and present < nics:
            flags.append({
                "flag": "NIC_COUNT_BELOW_SPEC",
                "detail": f"{present} interface(s) present against a rated {nics} network "
                          f"card(s) for this instance type",
                "investigate": [
                    "this is a launch-template fault, not a transport one: the template "
                    "attached fewer cards than the shape has",
                    "bandwidth is PER CARD — stacking two ENIs on one card buys nothing",
                ]})

    # ---- Root volume. Belt-and-braces for writes that are PARTLY misplaced, which no
    # single-path check can see.
    #
    # SUPPRESSED when the kernel says `/` and a pool drive are ONE filesystem: then correct writes
    # shrink `/` by their own size and the delta proves nothing (see `_root_shares_filesystem_with`
    # for the five-node measurement). The suppression is RECORDED in the record, not silent — an
    # absent flag and an inapplicable probe are different facts, and §10.4 requires the report to
    # be able to tell them apart.
    root = slot("root_free")
    if root.get("delta_gib") is not None and root["delta_gib"] > 1.0 and not root.get("shared_with"):
        flags.append({
            "flag": "ROOT_CONSUMED",
            "detail": f"the root filesystem lost {root['delta_gib']} GiB across this run; "
                      f"every large write was supposed to land on the instance-store pool",
            "investigate": [
                "the pool slot's device list and the download slot's local_dir",
                "TRAIN_REQUIRE_NVME=1 catches a pool that is absent entirely; this catches one "
                "that is present while some writes still go to /",
                "this is the ap-northeast-1 fault's quieter form: there, 13 of 16 jobs died "
                "ENOSPC on a 30 GiB root because the CE had no launch template at all",
            ]})

    # ---- Placement. Policy is that all P-family capacity is spot, so on-demand is a finding
    # about how the CE was built. Recorded, never repaired.
    ident = slot("identity")
    lifecycle = (ident.get("lifecycle") or "").lower()
    if lifecycle and lifecycle != "spot" and chip.startswith(("h", "b", "a100")):
        flags.append({
            "flag": "UNEXPECTED_LIFECYCLE",
            "detail": f"this P-family node reports lifecycle {lifecycle!r}; policy is spot-only",
            "investigate": [
                "the compute environment's allocation strategy, not this job",
                "on-demand has been measured to place WORSE for these shapes (0 of 8 regions "
                "in six hours, against roughly an hour on spot), so this is not a workaround",
            ]})

    if not spec:
        flags.append({"flag": "NO_SPEC",
                      "detail": f"chip {chip!r} has no CHIP_SPEC row; numbers recorded but "
                                f"nothing to compare them against",
                      "investigate": [
                          "no verdict must not read as a pass: every floor below was skipped",
                          "add the row from vendor spec (dense bf16, not the 2x sparsity "
                          "figure) before treating this shape's numbers as attested",
                      ]})
        return flags

    comp = rows("compute")
    if comp:
        worst = min(comp, key=lambda c: c["tflops"])
        frac = worst["tflops"] / spec["bf16_tflops"]
        if frac < FLOOR["compute"]:
            flags.append({
                "flag": "SLOW_COMPUTE",
                "detail": f"GPU {worst['device']} at {worst['tflops']} TFLOP/s is "
                          f"{frac:.0%} of {chip} peak ({spec['bf16_tflops']})",
                "investigate": [
                    "nvidia-smi clocks.sm vs clocks.max.sm in node.clocks — thermal or power "
                    "capping shows up here first",
                    "a single slow GPU among fast ones is a hardware fault; all of them slow "
                    "is a clock, power, or image-build problem",
                    f"image reports CUDA {report.get('node', {}).get('cuda')} — a mismatched "
                    f"sm_ target falls back to slower kernels without erroring",
                    "the one real B200 measurement here was 1642 TFLOP/s = 73% of rated, which "
                    "is what sets this floor's headroom",
                ]})
        #: Spread matters independently of the floor: eight cards that should be identical are
        #: a controlled experiment, and one outlier is a fault even when all pass the floor.
        if len(comp) > 1:
            lo = min(c["tflops"] for c in comp)
            hi = max(c["tflops"] for c in comp)
            if lo < hi * SPREAD_FLOOR:
                flags.append({
                    "flag": "UNEVEN_GPUS",
                    "detail": f"per-GPU TFLOP/s spread {lo}-{hi} ({lo/hi:.0%}); identical cards "
                              f"in one node should agree within a few percent",
                    "investigate": ["the slowest card's index, then nvidia-smi -q -i <index> "
                                    "for retired pages, throttle reasons, and ECC errors",
                                    "collectives wait for the slowest rank, so this sets the "
                                    "step time for every card in the node",
                                    "a card 12.5% down clears the absolute floor entirely; "
                                    "only this spread catches it"]})

    hbm = rows("hbm")
    if hbm:
        worst = min(hbm, key=lambda h: h["gb_per_s"])
        frac = worst["gb_per_s"] / spec["hbm_gbs"]
        if frac < FLOOR["hbm"]:
            flags.append({
                "flag": "SLOW_HBM",
                "detail": f"GPU {worst['device']} at {worst['gb_per_s']} GB/s is {frac:.0%} of "
                          f"{chip} spec ({spec['hbm_gbs']})",
                "investigate": ["ECC retirement degrades bandwidth silently: nvidia-smi -q "
                                "-d ECC,PAGE_RETIREMENT",
                                "node.clocks for a memory clock below max"]})

    coll = slot("collectives")
    if coll.get("error"):
        flags.append({"flag": "COLLECTIVE_FAILED", "detail": coll["error"],
                      "investigate": ["NCCL cannot initialise on this shape; multi-GPU "
                                      "training would fail here, single-GPU would not",
                                      "a SKIPPED collective on a single-GPU shape is not this "
                                      "flag — the skipped key is a different fact"]})
    elif coll.get("all_gather_busbw_gb_per_s") is not None:
        #: The EFA-silent-fallback class of bug, in its intra-node form. NVLink-class shapes
        #: should report hundreds of GB/s; a number in the tens means the cards are talking over
        #: PCIe or worse while every status signal stays green.
        #:
        #: CONDITIONED ON THE TOPOLOGY, because identical numbers get opposite verdicts on
        #: different hardware: tens of GB/s is CORRECT on a G-family node with no NVLink, and
        #: flagging it would be noise that gets the whole tool ignored.
        #:
        #: MATCHED AGAINST THE MATRIX, NOT THE LEGEND. This was `"NV" in topo`, and
        #: `nvidia-smi topo -m` prints a legend under every matrix that reads
        #: "NV#  = Connection traversing a bonded set of # NVLinks" — so the substring was
        #: present on EVERY node, NVLink or not, and the flag fired on a g6.12xlarge whose
        #: matrix is all NODE (measured 2026-09-21, us-east-2). `NV\d+` cannot match the legend:
        #: the legend says NV#, the matrix says NV18/NV12/NV4. A G-family node now correctly
        #: says nothing, which is the whole point of conditioning on topology.
        topo = report.get("node", {}).get("topology", "")
        bw = coll["all_gather_busbw_gb_per_s"]
        if re.search(r"\bNV\d+\b", topo) and bw < NVLINK_FLOOR_GB_S:
            flags.append({
                "flag": "COLLECTIVE_BELOW_NVLINK",
                "detail": f"topology reports NVLink but all-gather busbw is {bw} GB/s, under the "
                          f"{NVLINK_FLOOR_GB_S} GB/s NVLink-class floor — PCIe-class, the exact "
                          f"shape of a silent interconnect fallback",
                "investigate": ["node.topology for the NV link count between the pair",
                                "the efa slot's nccl_net_lines to see which transport NCCL "
                                "selected",
                                "this is how a 16 Gbps multi-node run passed every health "
                                "check for weeks"]})

    flags += _efa_flags(report)
    return flags


def _efa_flags(report: dict) -> list[dict]:
    """The fabric findings. Separated only because there are six of them and they share inputs.

    A shape with no EFA is skipped upstream and reaches here with nothing to say — G-family has
    no fabric, and flagging its absence would be noise that gets the whole tool ignored.
    """
    efa = report.get("efa")
    if not isinstance(efa, dict) or efa.get("skipped"):
        return []
    inv = efa.get("inventory") or {}
    exp = report.get("shape_expectation", {})
    supported = exp.get("efa_supported")
    cards = exp.get("net_cards")
    ndev = inv.get("efa_device_count", 0)
    flags = []

    if supported and not ndev:
        flags.append({
            "flag": "NO_EFA_DEVICE",
            "detail": f"the shape table says this instance type supports EFA and "
                      f"/sys/class/infiniband names none",
            "investigate": [
                "the launch template attached no EFA ENI; that is a template fault, recorded "
                "here and fixed upstream in gpu_fleet/catalogue.py",
                "EC2's own answer is not the last word: describe_instance_types reports EFA "
                "support for p6-b300 while Jakarta and Seoul reject every EFA ENI on that type",
            ]})
    elif cards and ndev and ndev < cards:
        flags.append({
            "flag": "EFA_DEVICE_COUNT_LOW",
            "detail": f"{ndev} EFA device(s) against a rated {cards} network card(s)",
            "investigate": [
                "COUNT CARDS, NOT ENIs: bandwidth is per card, and stacking two ENIs on one "
                "card is the single most expensive misunderstanding available here",
                "the template's per-card interface list",
            ]})

    if ndev and not inv.get("ofi_nccl_plugin"):
        flags.append({
            "flag": "EFA_PLUGIN_MISSING",
            "detail": "EFA hardware is attached and no aws-ofi-nccl plugin is in the image",
            "investigate": ["without the plugin NCCL NEVER uses EFA, whatever the hardware is",
                            "this is an image-build finding, not a node one"]})

    if ndev and not inv.get("libcudart_unversioned"):
        flags.append({
            "flag": "LIBCUDART_UNVERSIONED_MISSING",
            "detail": "no unversioned libcudart.so on the library path; the EFA plugin dlopens "
                      "that exact name",
            "investigate": [
                "THE known cause of silent TCP fallback: a multi-node run sat at 16 Gbps with "
                "every signal green, hardware present and plugin present, because only "
                "libcudart.so.12 shipped",
                "libcudart.so.12 existing proves nothing; the development symlink is what "
                "dlopen needs",
            ]})

    if ndev and efa.get("net_provider") and not efa.get("is_efa"):
        flags.append({
            "flag": "NET_TRANSPORT_NOT_EFA",
            "detail": f"EFA is attached and NCCL selected {efa['net_provider']!r} with shared "
                      f"memory and P2P disabled — the silent fallback caught in the act",
            "investigate": [
                "the efa slot's nccl_net_lines carry NCCL's own selection output",
                "check EFA_PLUGIN_MISSING and LIBCUDART_UNVERSIONED_MISSING in this same "
                "report first: either explains this one",
                "the fabric_env map — an NCCL_ or FI_ override baked into the image or the "
                "launch template can disable EFA without anything erroring",
            ]})
    return flags


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0], allow_abbrev=False)
    ap.add_argument("--model-uri", default=None,
                    help="canonical s3:// source URI; the region-local replica is resolved "
                         "inside fetch_model, so never pass a replica bucket here. Omit to "
                         "skip the transport stages")
    ap.add_argument("--cohort", default=None,
                    help="cohort id, the first S3 partition. One cohort is one sweep of one GPU "
                         "type across every region that offers it")
    #: RECORDED, NEVER COMPARED FOR PASS/FAIL. Deliberately NOT named --expect-gpus/--expect-chip:
    #: those were gates and are deleted, and reusing their names would invite the belief that the
    #: gate is still there.
    ap.add_argument("--jd-gpus", type=int, default=None,
                    help="the GPU count the job definition requested; recorded beside torch's "
                         "count and the driver's, never used to accept or reject a node")
    ap.add_argument("--jd-chip", default=None,
                    help="the chip the shapes table claims for this instance type; recorded "
                         "beside every device name torch reports, compared to nothing")
    ap.add_argument("--net-cards", type=int, default=None,
                    help="rated NETWORK CARD count for this instance type, from the shapes "
                         "table. Cards, not ENIs: bandwidth is per card")
    ap.add_argument("--efa-supported", choices=("yes", "no"), default=None,
                    help="whether the shapes table says this instance type supports EFA. "
                         "Omitted means unknown, and a missing device is then recorded without "
                         "a flag because it cannot be told from a shape that never had one")
    ap.add_argument("--slow-after", type=float, default=SLOW_AFTER_S,
                    help=f"flag a download slower than this many seconds "
                         f"(default {SLOW_AFTER_S:.0f}). Post-hoc: nothing is terminated")
    ap.add_argument("--ab-cap-gib", type=float, default=AB_CAP_BYTES / GIB,
                    help="cap on each replica A/B leg, GiB (default 50)")
    ap.add_argument("--skip", default="",
                    help=f"comma-separated stages to skip: {','.join(STAGES)}. Ad-hoc only — a "
                         f"sweep never passes this, and a skipped stage records a 'skipped' key "
                         f"that is never conflated with an 'error' one")
    ap.add_argument("--report-uri", default=None, help="s3:// prefix for the JSON report")
    ap.add_argument("--keep", action="store_true",
                    help="keep the downloaded weights (default: delete, so a warm node's next "
                         "run measures a cold download and not a skip_existing no-op)")
    args = ap.parse_args(argv)
    skip = {s.strip() for s in args.skip.split(",") if s.strip()}
    unknown = skip - set(STAGES)
    if unknown:
        ap.error(f"--skip names unknown stage(s) {sorted(unknown)}; expected {list(STAGES)}")

    from model_transport import detect_region
    region = detect_region()
    efa_supported = None if args.efa_supported is None else args.efa_supported == "yes"

    report = {
        "argv": sys.argv[1:], "kind": "bench_gpu",
        "cohort": args.cohort or "adhoc",
        #: From detect_region(), NOT from AWS_DEFAULT_REGION — see batch_identity's note. This is
        #: a partition key and the independent variable of the replica A/B.
        "region": region,
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "budget_seconds": args.slow_after,
        "batch": batch_identity(),
        "shape_expectation": {"jd_gpus": args.jd_gpus, "jd_chip": args.jd_chip,
                              "net_cards": args.net_cards, "efa_supported": efa_supported},
        "skipped_stages": sorted(skip),
    }

    # ---- inventory first and unconditionally, so that if everything below dies the log still
    # answers "what hardware did this queue give me".
    node = node_inventory()
    report["node"] = node
    report["identity"] = node_identity()
    print("=== node", flush=True)
    print(json.dumps({k: v for k, v in node.items() if k != "topology"}, indent=1), flush=True)
    if node.get("topology"):
        print(node["topology"], flush=True)
    print("=== identity", flush=True)
    print(json.dumps(report["identity"], indent=1), flush=True)

    n = node.get("gpu_count", 0)
    smi_n = node.get("smi_gpu_count")
    print(f"REGION={region} SDK_DEFAULT_REGION={report['batch']['sdk_default_region']} "
          f"GPU_TORCH={n} GPU_SMI={smi_n} GPU_JD={args.jd_gpus} "
          f"CHIP_TORCH={node.get('chip')} CHIP_TABLE={args.jd_chip} "
          f"QUEUE={report['batch'].get('AWS_BATCH_JQ_NAME')} "
          f"ITYPE_NODE={report['identity'].get('instance_type')} "
          f"LIFECYCLE={report['identity'].get('lifecycle')} "
          f"VRAM_TOTAL_GIB={node.get('vram_total_gib', 0)}", flush=True)
    #: The disagreements, with their interpretation, as log lines and nothing more. This is what
    #: replaced the gates: the reader (or the reporting agent) draws the conclusion.
    for line in _disagreements(node, report["identity"], args):
        print(f"NOTE {line}", flush=True)

    # ---- the pool, before anything writes. enforce_nvme is the standing platform guard, not a
    # gate this job implements: under TRAIN_REQUIRE_NVME=1 it refuses a node whose template
    # mounted no instance store, because every write would land on a 30 GiB root.
    try:
        report["pool"] = pool_state()
    except Exception as e:
        report["pool"] = {"error": f"{type(e).__name__}: {e}"}
        print("=== pool", flush=True)
        print(json.dumps(report["pool"], indent=1), flush=True)
        #: Emit before re-raising so the platform guard firing still leaves a record naming the
        #: node and its template. Then let it propagate: the guard is designed to end the run.
        _emit(report, args.report_uri)
        raise
    print("=== pool", flush=True)
    print(json.dumps(report["pool"], indent=1), flush=True)

    import local_disk

    report["efa_inventory"] = efa_inventory()
    print("=== efa_inventory", flush=True)
    print(json.dumps(report["efa_inventory"], indent=1), flush=True)

    root_before = _root_free_bytes()

    # ---- probes before the transport. They take a couple of minutes against a download that
    # can take forty, so this way a transport failure still yields the hardware table — and the
    # hardware table is what says whether a transport failure is this node's fault. Safe because
    # each collective run has a hard wall-clock budget and the record is emitted before the
    # download starts.
    report["compute"] = _stage("compute", skip, lambda: [bench_compute(i) for i in range(n)],
                               precondition=(n > 0, "torch addresses no GPU on this node"))
    report["hbm"] = _stage("hbm", skip, lambda: [bench_hbm(i) for i in range(n)],
                           precondition=(n > 0, "torch addresses no GPU on this node"))
    #: One device only. The link is per-GPU on every shape here, and probing eight costs eight
    #: times as long to learn the same thing.
    report["h2d"] = _stage("h2d", skip, lambda: [bench_h2d(0)],
                           precondition=(n > 0, "torch addresses no GPU on this node"))
    report["disk"] = _stage("disk", skip, lambda: bench_disk(local_disk.scratch("bench")))
    report["collectives"] = _stage("collectives", skip, lambda: bench_collectives(n))
    report["efa"] = _stage("efa", skip,
                           lambda: bench_efa(n, report["efa_inventory"], efa_supported))

    # ---- THE EARLY EMIT. Everything above is in hand and everything below can take forty
    # minutes, so the record goes out now: Batch's attemptDurationSeconds SIGKILLs the container,
    # which would otherwise discard the whole hardware table to report a slow download.
    report["emitted_partial_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    report["complete"] = False
    _emit(report, args.report_uri, label="partial")

    dest_root = local_disk.scratch()
    if args.model_uri:
        report["replica_ab"] = _stage(
            "replica_ab", skip,
            lambda: bench_replica_ab(args.model_uri, dest_root, region,
                                     int(args.ab_cap_gib * GIB)),
            quiet=True)

        dest = os.path.join(dest_root, "bench_model")
        def _full():
            print(f"=== download {args.model_uri} -> {dest}", flush=True)
            try:
                return bench_download(args.model_uri, dest)
            finally:
                if not args.keep:
                    shutil.rmtree(dest, ignore_errors=True)
        report["download"] = _stage("download", skip, _full, quiet=True)
        dl = report["download"]
        if isinstance(dl, dict) and "gib" in dl:
            print(json.dumps(dl, indent=1), flush=True)
            print(f"DOWNLOAD_GIB={dl['gib']} SECONDS={dl['seconds']} "
                  f"MB_PER_S={dl['mb_per_s']} GBPS={dl['gbps']} "
                  f"REPLICA_STATUS={dl['replica_status']} "
                  f"ATTRIBUTED={dl['attributed']}", flush=True)
    else:
        for s in ("replica_ab", "download"):
            report[s] = {"skipped": "no --model-uri given"}

    root_after = _root_free_bytes()
    report["root_free"] = {
        "before_gib": None if root_before is None else round(root_before / GIB, 2),
        "after_gib": None if root_after is None else round(root_after / GIB, 2),
        "delta_gib": (None if root_before is None or root_after is None
                      else round((root_before - root_after) / GIB, 2)),
        #: The pool drive that IS the root filesystem, if any. Recorded rather than merely used,
        #: so a reader can see WHY a large delta carries no flag — an inapplicable probe and a
        #: clean node must not look the same (§10.4).
        "shared_with": _root_shares_filesystem_with(
            [d["path"] for d in report.get("pool", {}).get("devices", [])]),
        #: What `/` IS, so the next attempt at this suppression starts from a measurement instead
        #: of a third guess.  Two facts to reconcile, both measured
        #: on real nodes: `before_gib` keeps coming back equal to the FIRST pool drive's free space at
        #: four different values, so `statvfs("/")` is reporting a pool filesystem — while
        #: `shared_with` is null, so `st_dev` says `/` is none of them.  Recorded on clean nodes too:
        #: within one cohort two `g6.2xlarge` nodes differed (one flagged, one not), which
        #: already refutes "fires on every node of one chip" and makes the clean one the control.
        "root_mount": _root_mount(),
        #: The root's own size next to its free space.  Absent from every record so far, and it is
        #: the number that separates the two live readings: a `/` of ~30 GiB that reports 407 GiB
        #: free is impossible, so whichever filesystem `statvfs` is answering for, it is not a
        #: 30 GiB root — and the ENOSPC fault this flag exists for was precisely a 30 GiB root.
        "root_total_gib": _root_total_gib(),
    }
    print("=== root_free", flush=True)
    print(json.dumps(report["root_free"], indent=1), flush=True)

    report["flags"] = evaluate(report, args.slow_after)
    report["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    report["complete"] = True
    for f in report["flags"]:
        print(f"FINDING {f['flag']}: {f['detail']}", flush=True)
    _emit(report, args.report_uri)
    #: EXIT 0. Not a verdict, and nothing reads this code. A non-zero exit would make Batch mark
    #: the job FAILED, and then "degraded" and "never ran" are the same colour — which is the one
    #: distinction this tool exists to draw. The findings are in the record.
    return 0


def _disagreements(node: dict, identity: dict, args) -> list[str]:
    """Every mismatch worth a log line, with the interpretation attached. Compared, never gated.

    This function IS the replacement for the deleted gates. It says the same things they said and
    then lets the run continue, because a mismatch is information and information should be
    recorded rather than acted on — and because the two outcomes of acting on it were
    correct-and-silent, or a false positive that dropped a whole shape out of the fleet table.
    """
    out = []
    torch_n = node.get("gpu_count", 0)
    smi_n = node.get("smi_gpu_count")
    if smi_n is not None and torch_n != smi_n:
        if torch_n == 0 and smi_n:
            out.append(f"torch sees 0 GPUs and the driver sees {smi_n}: WRONG IMAGE FOR THIS "
                       f"CHIP (a missing sm_ target), a fleet-plan or CDK image-routing bug — "
                       f"not a hardware fault")
        elif torch_n == 0:
            out.append("neither torch nor the driver sees a GPU: no GPU attached, a node or "
                       "launch-template bug")
        else:
            out.append(f"the container sees {torch_n} GPU(s) and the node has {smi_n}: a "
                       f"VISIBILITY finding, i.e. the job definition's GPU request")
    if args.jd_gpus is not None and torch_n != args.jd_gpus:
        out.append(f"the job definition requested {args.jd_gpus} GPU(s) and torch addresses "
                   f"{torch_n}; probes below iterate over what torch can see")
    if args.jd_chip and node.get("gpus"):
        names = sorted({g["name"] for g in node["gpus"]})
        if not any(args.jd_chip.lower().replace(" ", "") in nm.lower().replace(" ", "")
                   for nm in names):
            out.append(f"the shapes table claims chip {args.jd_chip!r} and torch reports "
                       f"{names}; EC2 guarantees instance type -> GPU model, so this is most "
                       f"likely a naming difference and not a placement one")
    if identity.get("itype_source") == "imds-unreachable":
        out.append("instance metadata is unreachable from this container (hop limit 1?), so the "
                   "node's own instance type, AZ and lifecycle are unknown for this run")
    return out


def _stage(name: str, skip: set, fn, *, precondition=None, quiet: bool = False):
    """Run one stage. Wrapped, because independent measurements must not be lost to one crash —
    otherwise the shape most worth measuring is the shape that returns no data.

    THREE OUTCOMES, THREE DIFFERENT KEYS, never conflated:
      - a result
      - {"skipped": "<why>"} — an operator choice or a shape fact, with no verdict
      - {"error": "<Type>: <msg>"} — a finding
    Under no gates the record is the only output, so an absent measurement must be unambiguous:
    otherwise a run with --skip is indistinguishable from a cohort of broken nodes.
    """
    if name in skip:
        res = {"skipped": "named in --skip"}
        print(f"=== {name}: skipped (--skip)", flush=True)
        return res
    if precondition and not precondition[0]:
        res = {"skipped": precondition[1]}
        print(f"=== {name}: skipped ({precondition[1]})", flush=True)
        return res
    print(f"=== {name}", flush=True)
    t0 = time.time()
    try:
        res = fn()
    except Exception as e:
        logger.exception(f"{name} stage failed")
        return {"error": f"{type(e).__name__}: {e}"}
    if not quiet:
        print(json.dumps(res, indent=1), flush=True)
    print(f"{name} took {time.time() - t0:.1f}s", flush=True)
    return res


def _emit(report: dict, report_uri: str | None, label: str = "final"):
    """Print the whole record, then persist it. Best-effort: never change the exit code.

    THE KEY IS PARTITIONED cohort / shape / region. Container logs land in the region the job
    ran, so a ten-region cohort is ten CloudWatch log groups — and Batch forgets jobs after about
    a week, so a report assembled a fortnight later would find nothing. One bucket makes the
    reporting agent's input a listing instead of a cross-region query, and the partitions let it
    read one cohort without touching the rest.

    Region comes from the record's own `region` field, which came from detect_region(). Using
    AWS_DEFAULT_REGION here would file every node under the home region and quietly
    destroy the one partition the replica A/B is about.
    """
    blob = json.dumps(report, indent=1)
    print(f"=== report ({label})", flush=True)
    print(blob, flush=True)
    if not report_uri:
        return
    try:
        import boto3
        import botocore.config
        bucket, _, prefix = report_uri[len("s3://"):].partition("/")
        b = report.get("batch", {})
        cohort = report.get("cohort") or "adhoc"
        shape = b.get("BENCH_SHAPE", "unknown")
        region = report.get("region") or "unknown"
        job = b.get("AWS_BATCH_JOB_ID", "local")
        key = (f"{prefix.rstrip('/')}/cohort={cohort}/shape={shape}/region={region}/"
               f"{job}.json")
        #: BOUNDED, and the timeout is why a benchmark that measured everything correctly still
        #: read as a total loss. This put is aimed at the report bucket's GLOBAL endpoint, which a
        #: multi-card node (no public IP) cannot route to; on botocore's 60 s x 5 adaptive default
        #: the "best-effort" write below cost ~2409 s of a GPU node before the except fired. Worse,
        #: it left the job with no S3 record at all — and `SUCCEEDED without S3 artifacts is not
        #: success` is precisely how the fleet judges these runs, so the node was billed for three
        #: hours and then scored as unproven. Ten seconds and three attempts: if the endpoint is
        #: unreachable the warning should be immediate, since the log already carries the report.
        boto3.client("s3", config=botocore.config.Config(
            connect_timeout=10, retries={"max_attempts": 3, "mode": "standard"},
        )).put_object(Bucket=bucket, Key=key, Body=blob.encode(),
                      ContentType="application/json")
        print(f"report written to s3://{bucket}/{key}", flush=True)
    except Exception as e:
        #: Never fail the job over its own bookkeeping; the log already carries the whole report,
        #: and failing a completed benchmark over a throttled PutObject would discard a real
        #: measurement to report a clerical one.
        logger.warning(f"could not write report to {report_uri}: {type(e).__name__}: {e}")


if __name__ == "__main__":
    raise SystemExit(main())
