#!/usr/bin/env python3
"""Entry point of the fleet runtime image: route argv[1] to a fleet tool.

Batch overrides the command, never the entrypoint, so the first word of the command picks
the tool:

    gpu-fleet bench_gpu [bench_gpu options]           per-shape attestation (bench job definitions)
    gpu-fleet fetch_model <s3-uri> <dir> [--limit-gib N] [--force-source]
                                                      download weights; prints the FetchReport as JSON
    gpu-fleet stage_model <hf_repo> <name> [--bucket B] [--prefix P]
                                                      Hugging Face -> source weight bucket, no local disk

Anything else is executed as given, so images built FROM this one keep the router and can
still run their own programs (`gpu-fleet python train.py ...`).
"""
import argparse
import dataclasses
import json
import logging
import os
import sys

GIB = 1024 ** 3


def _fetch(argv) -> int:
    from model_transport import fetch_model
    ap = argparse.ArgumentParser(prog="gpu-fleet fetch_model")
    ap.add_argument("s3_uri", help="canonical source URI; the region-local replica is resolved")
    ap.add_argument("local_dir")
    ap.add_argument("--limit-gib", type=float, default=None,
                    help="download only the first whole objects totalling at most this many GiB")
    ap.add_argument("--force-source", action="store_true",
                    help="skip replica resolution and read the source bucket")
    a = ap.parse_args(argv)
    rep = fetch_model(a.s3_uri, a.local_dir, force_source=a.force_source,
                      limit_bytes=None if a.limit_gib is None else int(a.limit_gib * GIB))
    out = {**dataclasses.asdict(rep), "gbps": rep.gbps}
    print(json.dumps(out, indent=1, default=str), flush=True)
    return 0


def _stage(argv) -> int:
    from model_transport import stage_model
    from model_transport.stage import DEFAULT_PREFIX
    ap = argparse.ArgumentParser(prog="gpu-fleet stage_model")
    ap.add_argument("hf_repo")
    ap.add_argument("model_name")
    ap.add_argument("--bucket", default=os.environ.get("GPU_FLEET_WEIGHT_BUCKET"),
                    help="source weight bucket (default: $GPU_FLEET_WEIGHT_BUCKET)")
    ap.add_argument("--prefix", default=DEFAULT_PREFIX)
    a = ap.parse_args(argv)
    if not a.bucket:
        ap.error("no weight bucket: pass --bucket or set GPU_FLEET_WEIGHT_BUCKET")
    n = stage_model(a.hf_repo, a.model_name, bucket=a.bucket, base_prefix=a.prefix)
    print(json.dumps({"staged_files": n, "uri": f"s3://{a.bucket}/{a.prefix}/{a.model_name}/"}),
          flush=True)
    return 0


def _bench(argv) -> int:
    import bench_gpu
    return bench_gpu.main(argv)


ROUTES = {"bench_gpu": _bench, "fetch_model": _fetch, "stage_model": _stage}


def main(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 0 if argv else 2
    route = ROUTES.get(argv[0])
    if route is None:
        os.execvp(argv[0], argv)   # does not return
        return 127
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return route(argv[1:])


if __name__ == "__main__":
    sys.exit(main())