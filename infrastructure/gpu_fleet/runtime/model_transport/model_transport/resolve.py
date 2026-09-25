"""Region detection and replica-bucket resolution.

Base models live in a source bucket in the fleet's home region and are mirrored
by S3 CRR to per-region replicas named `<source-bucket>-<region>` (see
gpu_fleet/weights.py). A job should pull from its own region:
cross-region single-stream S3 measured 20 MB/s vs 100 MB/s local, and
cross-region transfer costs ~$0.02/GB.
"""
import logging
import os

import boto3
import botocore.config

from .verify import list_model_objects, replica_is_complete

logger = logging.getLogger(__name__)


def detect_region() -> str:
    """Region this process PHYSICALLY runs in.

    Placement sources in order: ECS container metadata (always reachable
    inside Batch containers), then EC2 IMDSv2 (can be blocked by hop
    limit 1 in containers), then env vars LAST — Batch job definitions
    pin AWS_DEFAULT_REGION to the home region for API calls regardless of
    where the job lands, so trusting env first would send every job to the
    home-region bucket."""
    try:
        import json
        import urllib.request

        meta_uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
        if meta_uri:
            task = json.loads(
                urllib.request.urlopen(meta_uri + "/task", timeout=2).read())
            az = task.get("AvailabilityZone", "")
            if az:
                return az.rstrip("abcdef")
    except Exception:
        pass
    try:
        import urllib.request

        req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
        )
        token = urllib.request.urlopen(req, timeout=2).read().decode()
        req = urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/placement/region",
            headers={"X-aws-ec2-metadata-token": token},
        )
        region = urllib.request.urlopen(req, timeout=2).read().decode().strip()
        if region:
            return region
    except Exception:
        pass
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or ""


def cap_objects(objs: dict, limit_bytes: int) -> dict:
    """The first whole objects under `prefix` totalling at most `limit_bytes`.

    DETERMINISTIC BY KEY ORDER, because the caller that needs this is
    measuring the SAME slice twice from two buckets (bench_gpu's replica A/B) and a
    cap that picked different objects per leg would make the comparison
    meaningless. Key order is the one ordering both buckets agree on.

    Whole objects, never a partial one: a truncated shard is indistinguishable
    from a failed download to every size check in this package, and the
    slice is never loaded — it exists only to be timed.

    At least one object is always kept, so a limit below the first shard's
    size still measures something rather than reporting a zero-byte success.
    Callers that cap therefore get *at least* limit_bytes only in that case,
    and the returned total is what should be reported, never the limit.
    """
    if limit_bytes is None:
        return objs
    if limit_bytes <= 0:
        raise ValueError(f"limit_bytes must be positive, got {limit_bytes}")
    kept, total = {}, 0
    for key in sorted(objs):
        size = objs[key]
        if kept and total + size > limit_bytes:
            break
        kept[key] = size
        total += size
    logger.info(f"limit_bytes={limit_bytes/1e9:.1f} GB: keeping {len(kept)} of "
                f"{len(objs)} objects ({total/1e9:.1f} GB)")
    return kept


def resolve_bucket(bucket: str, prefix: str, *, region: str = None) -> tuple:
    """Pick the bucket to download `prefix` from.

    Returns (bucket, bucket_region, objects) where objects is the
    {key: size} listing of the chosen bucket — callers reuse it to avoid a
    second LIST. bucket_region is None when boto3 should use its default
    endpoint resolution (source bucket).

    The replica is used only when its listing for the prefix is
    byte-identical to the source's (a mid-backfill replica must never win).
    Any failure falls back to the source bucket: correctness over speed.
    """
    return resolve_bucket_ex(bucket, prefix, region=region)[:3]


#: Seconds to wait for a TCP connect on a LISTING call, and how many times to retry it.
#:
#: THE DEFAULTS BURNED 152 THREE-HOUR GPU NODES (measured on a real bench cohort).
#: botocore's defaults are a 60 s connect timeout with `max_attempts: 5` in adaptive mode, so a
#: single unreachable endpoint costs ~2409 s before the call gives up. A multi-card GPU node has
#: NO PUBLIC IP — it reaches S3 only through its own region's gateway endpoint — and the listing
#: below is aimed at the SOURCE bucket, which outside the home region resolves to an endpoint that node
#: cannot route to. Four such calls per job = the full 10800 s Batch cap. The correlation was
#: exact: 152 of 152 timed-out jobs were multi-card, 0 were single-card.
#:
#: 10 s matches `download.py::_bound_client`, which has always bounded its clients; this module
#: was simply never given the same treatment. A LISTING is a few KB, so a connect that has not
#: completed in 10 s is not slow, it is unroutable — and the correct answer then is
#: `replica-absent` / a raised error in seconds, which callers already handle, not an hour of
#: silence. DO NOT RAISE THESE to "be safe on a flaky link": the failure mode they prevent is a
#: three-hour burn that writes no record, which is strictly worse than a fast, legible failure.
_LIST_CONNECT_TIMEOUT_S = 10
_LIST_MAX_ATTEMPTS = 3


def _listing_client(region: str | None = None):
    """An S3 client for LISTING that fails fast when the endpoint is unroutable.

    Kept as a helper rather than inlined twice because the source-bucket listing and the replica
    listing must be bounded the SAME way: the source leg is the one a no-public-IP node cannot
    reach, and the replica leg is the one that must be allowed to fail so `replica-absent` can be
    reported. Either one left on botocore's defaults reintroduces the burn.
    """
    return boto3.client("s3", region_name=region,
                        config=botocore.config.Config(
                            connect_timeout=_LIST_CONNECT_TIMEOUT_S,
                            retries={"max_attempts": _LIST_MAX_ATTEMPTS,
                                     "mode": "standard"}))


def resolve_bucket_ex(bucket: str, prefix: str, *, region: str = None,
                      force_source: bool = False) -> tuple:
    """`resolve_bucket` plus the REASON it chose what it chose.

    Returns (bucket, bucket_region, objects, replica_status). The fourth
    element is one of:

      local              the source bucket already lives in this region
      no-region          placement could not be determined, so no replica
                         name could be formed
      replica            the region-local replica was complete and was used
      replica-incomplete a replica exists but its listing differs from the
                         source's, so the source was used
      replica-absent     no replica bucket in this region at all
      forced-source      the caller asked for the source bucket explicitly

    WHY THIS EXISTS SEPARATELY. The status was already computed here and
    thrown away into a log line, which is how a benchmark measuring regional
    locality (bench_gpu's replica A/B) ended up unable to say which bucket served it.
    `replica-absent` in particular is a fact about the region: the weights
    stack gives every fleet region a replica, so absence means a region
    outside the fleet or a replica never created. A caller
    that can only see the chosen bucket name cannot distinguish that from a
    replica it declined to use.

    A THREE-TUPLE IS KEPT for `resolve_bucket`, which download.py and four
    tests unpack positionally. Widening the arity there would buy nothing.

    `force_source` bypasses replica resolution and says so in the status,
    which is different from region="" — that one means "nowhere in
    particular", and reading a forced choice as an undetected placement
    would silently turn the A/B's control leg into a missing measurement.
    """
    source_objs = list_model_objects(_listing_client(), bucket, prefix)
    if force_source:
        logger.info(f"Forced source bucket {bucket} (replica resolution skipped)")
        return bucket, None, source_objs, "forced-source"
    if region is None:
        region = detect_region()
    if not region:
        return bucket, None, source_objs, "no-region"
    if bucket.endswith(region):
        return bucket, None, source_objs, "local"

    replica = f"{bucket}-{region}"
    try:
        rs3 = _listing_client(region)
        replica_objs = list_model_objects(rs3, replica, prefix)
        if replica_is_complete(source_objs, replica_objs):
            logger.info(f"Using region-local replica {replica}")
            return replica, region, replica_objs, "replica"
        logger.info(
            f"Replica {replica} incomplete "
            f"({len(replica_objs)}/{len(source_objs)} files); using {bucket}"
        )
        return bucket, None, source_objs, "replica-incomplete"
    except Exception as e:
        logger.info(f"No replica {replica} ({e.__class__.__name__}); using {bucket}")
    return bucket, None, source_objs, "replica-absent"
