"""Parallel model download: one stream per file, with a multi-ENI fast path.

Sequential S3 GETs run ~20 MB/s cross-region / ~100 MB/s local per stream,
while a p5 node sustains 550-900 MB/s aggregate — so parallelism across
files is the whole game (measured: a 690GB model 9.4h -> ~15min).

THIS IS THE FOUNDATIONAL ENTRY POINT. Every training, eval, and inference job
in the fleet calls `fetch_model`, so the fast path is wired in HERE rather than
at each call site: nothing downstream changes, and no job can be left behind on
the slow path by omission. `download.py` uses every network card and the local
NVMe pool (measured 6.20 GB/s for GLM-5's 1.5 TiB in 243 s across 32 cards vs
~270 MB/s here, ~4 min vs ~1.4 h), and this module falls back to the
thread-per-file implementation whenever the fast path does not apply or fails.

WHY A FALLBACK AND NOT A REPLACEMENT. The fast path needs several IP-carrying
ENIs, policy routing, an S3 gateway endpoint, and root to mount instance store.
That is true on a p5 training node and false on a laptop, in CI, in a Lambda, on
a single-ENI inference host, and on any node whose launch template predates the
all-`efa` change. A downloader that only works in the best case is not
foundational, so capability is DETECTED, never assumed, and the slow path stays
correct and supported.
"""
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from fnmatch import fnmatch

import boto3
import botocore.config
from boto3.s3.transfer import TransferConfig

from .resolve import cap_objects, detect_region, resolve_bucket_ex

logger = logging.getLogger(__name__)

# With one thread per file, per-file multipart threads would multiply into
# tens of thousands of connections; keep each download single-threaded.
_S3_NO_EXTRA_THREADS = TransferConfig(use_threads=False)


@dataclass
class FetchReport:
    bucket: str          # bucket that actually served the download
    files: int
    bytes: int
    seconds: float
    local_dir: str

    #: WHY THESE ARE HERE. Every one of them was already computed — by
    #: resolve_bucket_ex, by download._attribution, by _fast_path_ok — and
    #: discarded at this boundary, which left every caller unable to answer
    #: questions the transport had already answered. The fault this closes is
    #: the one that cost weeks: a 32-card node downloading on one card, with a
    #: throughput number that looked merely disappointing because nothing said
    #: how many cards carried it.
    #:
    #: All default, because the slow path cannot fill most of them and a
    #: required field would make every existing construction site a change.
    #: An empty `fast_path_reason` therefore means "nobody asked", while a
    #: non-empty one starting with "skipped" is a finding.
    node_region: str = ""
    bucket_region: str = ""
    replica_status: str = ""      # local|replica|replica-incomplete|replica-absent|...
    per_iface_gbps: dict = field(default_factory=dict)
    attributed: bool = None       # None = not measured (slow path), not "failed"
    balance_worst_dev: float = None
    fast_path_reason: str = ""    # verbatim "ENGAGED — ..." / "skipped — ..."

    @property
    def mbps(self) -> float:
        return self.bytes / max(self.seconds, 1e-6) / 1e6

    @property
    def gbps(self) -> float:
        return self.bytes * 8 / max(self.seconds, 1e-6) / 1e9


def _fast_path_ok(include, local_dir, objs):
    """Should the multi-ENI/NVMe path handle this fetch? Reasons, not a bool.

    Returns (ok, why). `why` is always logged, because "the fast path silently
    did not engage" is the failure mode that made a 32-card node download at
    1/30th of its capability for weeks without anyone noticing.

    THE FAST PATH IS THE DEFAULT. It engages for any unfiltered fetch on a host
    with a network card. Two earlier gates were removed on 2026-09-16 because
    both inferred "this cannot help" from a proxy rather than from what the fast
    path does — and both excluded cases that needed it most:

      - A 20 GiB SIZE FLOOR. The slow path is one stream PER FILE, so its
        throughput is bounded by the largest single object; a 4 GB shard on one
        stream sets the floor regardless of file count. Measured under the old
        floor: 20.9 GB in 266 s = 79 MB/s on a g6.12xlarge. The fast path's
        RANGES_PER_SHARD=32 concurrent byte-range GETs *within* each file is
        exactly the remedy for few-but-large files, so the floor turned the
        remedy off precisely where it applied.

      - A 2-ENI MINIMUM. `_get_shard` parallelises byte ranges and
        `plan_assignment` accepts n_enis=1 (it only rejects < 1); nothing in the
        range machinery needs a second card. Card count sets how much bandwidth
        is reachable, not whether the path works. Every AWS Batch-managed node has
        exactly one ENI — most of the GPU fleet — and fetch.py passes
        require_attribution=False while _attribution's balance check self-
        disables at one card, so a single card cannot trip the strict gate.

    This is safe rather than reckless only because fetch_model wraps
    download_model in try/except and degrades to the thread-per-file path on ANY
    exception. Universal engagement can cost minutes; it cannot fail a run.
    """
    if os.environ.get("MT_FAST_DOWNLOAD", "").lower() in ("0", "false", "no"):
        return False, "disabled by MT_FAST_DOWNLOAD"
    # KEPT, because this is a statement about the WORK and not a guess about the
    # host: skeleton fetches are ~20 MB of small config files, and on multi-node
    # FSDP every non-loader rank does exactly this. Spawning processes and a
    # 32-range fan-out to pull a tokenizer.json is pure overhead.
    if include is not None:
        return False, "filtered fetch (include=...), too small to matter"
    total = sum(objs.values())
    try:
        from .download import discover_enis
        enis = discover_enis()
    except Exception as e:                                    # pragma: no cover
        return False, f"ENI discovery failed: {str(e)[:120]}"
    # Not a proxy: with no IP-carrying card download_model raises outright.
    if not enis:
        return False, "no IP-carrying network card found"
    return True, (f"{len(enis)} network card(s), {total/1e9:.1f} GB, "
                  f"{len(objs)} file(s)")


def fetch_model(s3_uri: str, local_dir: str, *, max_streams: int = None,
                region: str = None, include=None,
                skip_existing: bool = True, limit_bytes: int = None,
                force_source: bool = False) -> FetchReport:
    """Download every object under `s3_uri` to `local_dir`, preserving
    relative paths.

    - Resolves the region-local replica bucket internally; callers always
      pass the canonical source URI.
    - One stream per file by default; `max_streams` caps concurrency
      (local-bucket benchmark: ~16 streams already saturate; >100 adds
      contention).
    - `include`: optional iterable of relative filenames or glob patterns
      (fnmatch, e.g. "*.py") restricting what is downloaded. Multi-node
      FSDP needs this: only the loader node reads the checkpoint, while the
      others need config/tokenizer/remote-code alone (~20MB vs 1.5TB for
      GLM-5). Individual patterns that match nothing are NOT an error —
      checkpoints vary in which sidecar files they ship — but a filter that
      matches nothing at all raises, since that is a caller bug.
      Patterns match the path relative to the prefix; fnmatch's "*" spans
      "/", so "*.py" matches both "modeling_glm.py" and "sub/mod.py", while
      a bare "config.json" matches only at the top level.
    - `skip_existing`: skip objects already on disk at the right size.
      Retries of a multi-hour fetch are the norm, not the exception (GLM-5 is
      1.5TB / ~87min), and S3 objects under a versioned model prefix are
      immutable, so size equality is a sufficient identity check — no need to
      pay a HEAD or a checksum per file. Files present but truncated (a
      killed download) differ in size and are refetched.
    - `limit_bytes`: download only the first whole objects (by key order)
      totalling at most this many bytes. NOT a filter — unlike `include` it
      leaves the fast path engaged, which is the entire reason it exists:
      bench_gpu's replica A/B  times the same capped slice from
      the replica and from the source, and `include=` would have it compare
      the thread-per-file fallback against the fan-out path. Whole objects
      only, and at least one, so the slice is never a truncated shard.
    - `force_source`: skip replica resolution and read from the source
      bucket. The control leg of that same A/B. Distinct from region="",
      and the returned `replica_status` says which happened.
    - Raises FileNotFoundError when the prefix is empty (an empty model is
      always a caller bug, never something to train on).
    """
    if not s3_uri.startswith("s3://"):
        raise ValueError(f"expected s3:// URI, got {s3_uri}")
    src_bucket, prefix = s3_uri[len("s3://"):].split("/", 1)
    prefix = prefix.rstrip("/")

    bucket, bucket_region, objs, replica_status = resolve_bucket_ex(
        src_bucket, prefix, region=region, force_source=force_source)
    if not objs:
        raise FileNotFoundError(f"no objects under {s3_uri}")
    node_region = region if region is not None else detect_region()

    if limit_bytes is not None:
        objs = cap_objects(objs, limit_bytes)

    if include is not None:
        patterns = list(include)
        objs = {k: v for k, v in objs.items()
                if any(fnmatch(k[len(prefix):].lstrip("/"), p)
                       for p in patterns)}
        if not objs:
            raise FileNotFoundError(
                f"nothing matching {sorted(patterns)} under {s3_uri}")

    # ---- FAST PATH: all network cards + local NVMe. Tried first, always
    # falls back, and the reason is always logged.
    fast_ok, why = _fast_path_ok(include, local_dir, objs)
    fast_reason = f"{'ENGAGED' if fast_ok else 'skipped'} — {why}"
    logger.info(f"fast download path: {fast_reason}")
    if fast_ok:
        try:
            from .download import download_model
            rep = download_model(s3_uri, local_dir, region=region,
                                 skip_reuse_check=not skip_existing,
                                 limit_bytes=limit_bytes,
                                 force_source=force_source,
                                 # Never fail a training job over attribution:
                                 # an unbalanced transfer is a performance bug,
                                 # not a correctness one, and the bytes are
                                 # verified by size either way. The benchmark
                                 # harness is where that gate belongs.
                                 require_attribution=False)
            # The fast path may relocate the model onto the NVMe pool, so tell
            # the caller where the files actually are. Callers pass local_dir to
            # from_pretrained afterwards, so returning the wrong directory would
            # break every one of them.
            return FetchReport(bucket=rep.bucket, files=rep.files,
                               bytes=rep.bytes, seconds=rep.seconds,
                               local_dir=rep.local_dir,
                               node_region=rep.node_region,
                               bucket_region=rep.bucket_region,
                               replica_status=rep.replica_status,
                               per_iface_gbps=rep.per_iface_gbps,
                               attributed=rep.attributed,
                               balance_worst_dev=rep.balance_worst_dev,
                               fast_path_reason=fast_reason)
        except Exception as e:
            # A model download is on the critical path of a multi-hour, multi-GPU
            # job. Degrading to the slow path costs minutes; failing costs the
            # run and the node-hours behind it.
            #
            # OVERWRITE the reason: a fallback that still reported "ENGAGED"
            # would attribute slow-path throughput to the fast path, which is
            # the exact confusion this field was added to end.
            fast_reason = f"failed after ENGAGED — {type(e).__name__}: {str(e)[:200]}"
            logger.warning(f"fast download path failed ({type(e).__name__}: "
                           f"{str(e)[:300]}); falling back to one stream per "
                           f"file. This is a performance regression, not a "
                           f"correctness one — investigate, do not ignore.",
                           exc_info=True)

    def _rel(key):
        return key[len(prefix):].lstrip("/")

    items = sorted(objs.items())

    # ---- LOCAL NVMe, ON THE SLOW PATH TOO.
    #
    # The pool used to be reachable ONLY through download.py, and _fast_path_ok
    # requires two or more IP-carrying cards — so every single-ENI host wrote
    # every byte to the caller's directory on the EBS root while its instance
    # store sat idle. That is not a corner case, it is most of the
    # GPU fleet: AWS Batch hands its managed nodes ONE ENI, and those nodes have
    # local NVMe (g5.12xlarge 1x3.8TB, g6.12xlarge 4x940GB, p6-b200.48xlarge
    # 8x3.8TB — describe-instance-types, 2026-09-16). Mounting the pool on a
    # Batch node without this block would have changed nothing at all.
    #
    # Wired in HERE for the same reason the fast path is: one central place
    # where capability is detected, so no job is left behind by omission.
    #
    # Two properties matter and both are tested:
    #   - the caller's local_dir stays authoritative (it holds symlinks), because
    #     callers pass it straight to from_pretrained and training code may ignore
    #     our return value entirely;
    #   - a pool failure NEVER fails the download. This is the critical path of a
    #     multi-hour multi-GPU job, and "no lsblk in this image" (true of
    #     amazonlinux:2023) must cost performance, not the run.
    pool = None
    if include is None and local_dir:
        try:
            from .nvme import build_pool
            model_name = os.path.basename(prefix.rstrip("/")) or "model"
            pool = build_pool(model_name, local_dir=local_dir)
            if pool.striped:
                logger.info(f"local NVMe pool: {len(pool.paths)} drive(s), "
                            f"{pool.free_bytes()/1e12:.1f} TB free, "
                            f"model_dir={pool.model_dir}")
                pool.check_capacity(sum(s for _, s in items))
            else:
                # No drives found: build_pool already logged why. Keep the
                # plain-file behaviour rather than routing through placement.
                pool = None
        except Exception as e:                                  # noqa: BLE001
            logger.warning(
                f"local NVMe pool unavailable ({type(e).__name__}: "
                f"{str(e)[:200]}); downloading to {local_dir} as given. This is "
                f"a performance and capacity regression — on a node with "
                f"instance store, investigate rather than ignore.")
            pool = None

    def _place(key):
        """(real destination, symlink or None) for one object."""
        rel = _rel(key)
        if pool is None:
            return os.path.join(local_dir, rel), None
        return pool.target(rel)

    skipped_files = skipped_bytes = 0
    if skip_existing:
        keep = []
        for key, size in items:
            # Check the REAL location, not the nominal one. download.py learned
            # this the hard way (its STEP 0 comment): looking under local_dir
            # while the shards live on the drives makes a fully warm node
            # re-download the whole model. os.path.getsize follows symlinks, so
            # the link in local_dir would also work — but only if it survived,
            # and the real path is the fact.
            dest, _ = _place(key)
            if os.path.exists(dest) and os.path.getsize(dest) == size:
                skipped_files += 1
                skipped_bytes += size
            else:
                keep.append((key, size))
        if skipped_files:
            logger.info(f"skip_existing: {skipped_files} of {len(items)} files "
                        f"already present ({skipped_bytes/1e9:.1f}GB), "
                        f"fetching {len(keep)}")
        items = keep

    streams = len(items) if max_streams is None else min(max_streams, len(items))
    #: connect_timeout is bounded for the same reason it is in resolve.py and download.py: a
    #: multi-card node has no public IP, so an endpoint outside its own region never connects, and
    #: botocore's 60 s x 5 adaptive default turns that into ~2409 s per call instead of an error.
    #: That once cost 152 three-hour GPU nodes. The READ timeout is deliberately
    #: left at the default here and NOT bounded: this client transfers multi-GB shards, and a read
    #: bound that is generous enough for a 40 GB object on a slow link is too loose to catch
    #: anything, while a tight one would abort healthy large downloads. Connect is the honest
    #: signal — reaching the endpoint at all is not size-dependent.
    s3 = boto3.client(
        "s3",
        region_name=bucket_region,
        config=botocore.config.Config(max_pool_connections=max(10, streams),
                                      connect_timeout=10,
                                      retries={"max_attempts": 3, "mode": "standard"}),
    )
    os.makedirs(local_dir, exist_ok=True)

    def _fetch(item):
        key, size = item
        dest, link = _place(key)
        os.makedirs(os.path.dirname(dest) or local_dir, exist_ok=True)
        s3.download_file(bucket, key, dest, Config=_S3_NO_EXTRA_THREADS)
        if link:
            # Present the shard in the caller's directory wherever it landed.
            # Replaced rather than skipped so a re-run repairs a link pointing at
            # a drive that was reformatted between boots (instance store is
            # ephemeral by design — see nvme.py).
            os.makedirs(os.path.dirname(link) or local_dir, exist_ok=True)
            try:
                if os.path.islink(link) or os.path.exists(link):
                    os.unlink(link)
            except OSError:
                pass
            os.symlink(dest, link)
        return size

    logger.info(f"Downloading s3://{bucket}/{prefix} -> {local_dir} "
                f"({len(items)} files, {streams} streams)")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=max(1, streams)) as ex:
        total = sum(ex.map(_fetch, items))
    report = FetchReport(bucket=bucket, files=len(items), bytes=total,
                         seconds=time.time() - t0, local_dir=local_dir,
                         node_region=node_region,
                         bucket_region=bucket_region or "",
                         replica_status=replica_status,
                         #: attributed stays None, not False: this path binds no
                         #: cards, so there is nothing to attribute. False would
                         #: read as "the binding did not take".
                         fast_path_reason=fast_reason)
    logger.info(f"Downloaded {report.files} files, {report.bytes/1e9:.1f}GB "
                f"in {report.seconds:.0f}s ({report.mbps:.0f} MB/s) "
                f"from {report.bucket} [{report.replica_status}]")
    return report
