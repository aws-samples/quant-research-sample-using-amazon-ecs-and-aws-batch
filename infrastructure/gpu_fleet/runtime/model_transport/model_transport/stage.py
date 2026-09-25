"""Stage a HuggingFace repo's files straight into the source S3 bucket
(no local disk). CRR then fans the objects out to all regional replicas
automatically (gpu_fleet/weights.py).

Resumable: skips files already fully staged; retries each file twice.
Needs extras: pip install model_transport[stage]

CLI: python -m model_transport.stage <hf_repo> <model_name>

The bucket defaults to $GPU_FLEET_WEIGHT_BUCKET, which the fleet job definitions set.
"""
import logging
import os
import sys
import time

import boto3

logger = logging.getLogger(__name__)

CHUNK = 256 * 1024 * 1024
DEFAULT_PREFIX = "base_models"
KEEP_EXT = (".safetensors", ".json", ".txt", ".py", ".jinja", ".model")
ATTEMPTS = 8


def _with_backoff(fn, what):
    """Retry fn() honoring HF 429 Retry-After; exponential backoff otherwise.
    A single anonymous 429 once killed a whole staging run;
    rate limits are routine on multi-GB anonymous pulls and must be waited
    out, not treated as fatal."""
    import requests

    for attempt in range(ATTEMPTS):
        try:
            return fn()
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status == 429 and attempt < ATTEMPTS - 1:
                wait = int(e.response.headers.get("Retry-After", 0)) or 2 ** attempt * 15
                logger.warning(f"429 on {what}; sleeping {wait}s "
                               f"(attempt {attempt+1}/{ATTEMPTS})")
                time.sleep(min(wait, 600))
                continue
            raise
        except Exception as e:  # noqa: BLE001 — connection resets etc.
            if attempt < ATTEMPTS - 1:
                wait = 2 ** attempt * 10
                logger.warning(f"{e.__class__.__name__} on {what}; retrying "
                               f"in {wait}s (attempt {attempt+1}/{ATTEMPTS})")
                time.sleep(wait)
                continue
            raise


def stage_model(hf_repo: str, model_name: str, *,
                bucket: str = None,
                base_prefix: str = DEFAULT_PREFIX,
                workers: int = None) -> int:
    """Stream every relevant file of `hf_repo` into
    s3://{bucket}/{base_prefix}/{model_name}/. Returns files staged.

    Shards download CONCURRENTLY (default 6 workers, STAGE_WORKERS env
    overrides): HF throttles per connection (~40MB/s), so serial staging
    of a 1.5TB checkpoint is ~10h while 6 streams cut it to ~1.5-2h.
    Kept modest — one aggressive client invites repo-wide 429s, which
    _with_backoff absorbs but still costs time.
    """
    from concurrent.futures import ThreadPoolExecutor

    import requests
    from huggingface_hub import HfApi

    bucket = bucket or os.environ.get("GPU_FLEET_WEIGHT_BUCKET")
    if not bucket:
        raise ValueError("no weight bucket: pass bucket= or set GPU_FLEET_WEIGHT_BUCKET")
    if workers is None:
        workers = int(os.environ.get("STAGE_WORKERS", "6"))

    api = HfApi()
    files = [f for f in api.list_repo_files(hf_repo)
             if f.endswith(KEEP_EXT) or f.startswith("tokenizer")]
    # sharded checkpoints make consolidated.* redundant (and huge)
    if any("model-0" in f and f.endswith(".safetensors") for f in files):
        files = [f for f in files if not f.startswith("consolidated")]
    files = sorted(files)

    s3 = boto3.client("s3")
    tok = os.environ.get("HF_TOKEN")
    hdr = {"Authorization": f"Bearer {tok}"} if tok else {}

    def _one(item):
        i, f = item
        key = f"{base_prefix}/{model_name}/{f}"
        url = f"https://huggingface.co/{hf_repo}/resolve/main/{f}"

        def _head():
            h = requests.head(url, headers=hdr, allow_redirects=True, timeout=60)
            h.raise_for_status()
            return int(h.headers.get("content-length", 0))

        expect = _with_backoff(_head, f"HEAD {f}")
        try:
            if s3.head_object(Bucket=bucket, Key=key)["ContentLength"] == expect:
                logger.info(f"[{i+1}/{len(files)}] {f} already staged, skip")
                return 0
        except s3.exceptions.ClientError:
            pass
        _with_backoff(lambda: _stage_one(s3, url, hdr, bucket, key), f"GET {f}")
        logger.info(f"[{i+1}/{len(files)}] {f} ({expect/1e9:.1f}GB)")
        return 1

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        staged = sum(ex.map(_one, enumerate(files)))
    logger.info(f"DONE {hf_repo} -> s3://{bucket}/{base_prefix}/{model_name}/ "
                f"({staged} new, {len(files)-staged} already present)")
    return staged


def _stage_one(s3, url, hdr, bucket, key):
    import requests

    r = requests.get(url, headers=hdr, stream=True, timeout=120)
    r.raise_for_status()
    size = int(r.headers.get("content-length", 0))
    if size < CHUNK:
        s3.put_object(Bucket=bucket, Key=key, Body=r.content)
        return
    mp = s3.create_multipart_upload(Bucket=bucket, Key=key)
    parts, pn = [], 1
    try:
        for chunk in r.iter_content(chunk_size=CHUNK):
            if not chunk:
                continue
            p = s3.upload_part(Bucket=bucket, Key=key, UploadId=mp["UploadId"],
                               PartNumber=pn, Body=chunk)
            parts.append({"ETag": p["ETag"], "PartNumber": pn})
            pn += 1
        s3.complete_multipart_upload(Bucket=bucket, Key=key,
                                     UploadId=mp["UploadId"],
                                     MultipartUpload={"Parts": parts})
    except Exception:
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=mp["UploadId"])
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if len(sys.argv) != 3:
        sys.exit("usage: python -m model_transport.stage <hf_repo> <model_name>")
    stage_model(sys.argv[1], sys.argv[2])
