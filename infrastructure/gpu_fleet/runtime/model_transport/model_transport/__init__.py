"""Shared model-weight transport.

One implementation of weight movement for every training/eval/staging
consumer in the fleet — model-specific code must import from here, never
copy the logic (no "copy-paste reuse").

    from model_transport import fetch_model, resolve_bucket, stage_model

`fetch_model` IS THE FOUNDATIONAL ENTRY POINT and the only one callers need. It
automatically uses every network card and the local NVMe pool when the host
supports it (6.20 GB/s measured on p5.48xlarge: GLM-5's 1.5 TiB in 243 s, vs
~1.4 h one-stream-per-file), and falls back to the portable path everywhere
else. Callers do not choose, and no job can be accidentally left on the slow
path — that is the point of putting the decision here.

`download_model` and the NVMe helpers are exported for benchmarks and for
callers with unusual needs (RAM-only transfers, explicit ENI counts). Ordinary
training, eval, and inference code should call `fetch_model`.
"""
from .resolve import cap_objects, resolve_bucket, resolve_bucket_ex, detect_region
from .fetch import fetch_model, FetchReport
from .verify import list_model_objects, replica_is_complete

__all__ = [
    "fetch_model",
    "FetchReport",
    "resolve_bucket",
    "resolve_bucket_ex",
    "cap_objects",
    "detect_region",
    "stage_model",
    "list_model_objects",
    "replica_is_complete",
    "download_model",
    "DownloadReport",
    "discover_enis",
    "discover_drives",
    "ensure_pool",
    "build_pool",
    "is_ephemeral",
]


def __getattr__(name):
    """Lazy re-export of the fast path and NVMe helpers.

    Deferred rather than imported at module load so that importing
    model_transport on a laptop, in CI, or in a Lambda never pays for (or
    trips over) EC2-only machinery. `fetch_model` already imports them on
    demand, so nothing on the hot path depends on this.
    """
    if name in ("download_model", "DownloadReport", "discover_enis"):
        from . import download
        return getattr(download, name)
    if name in ("discover_drives", "ensure_pool", "build_pool", "is_ephemeral",
                "CheckpointPool"):
        from . import nvme
        return getattr(nvme, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def stage_model(hf_repo: str, model_name: str, **kwargs):
    """Lazy import: staging needs huggingface_hub/requests, which the
    training image does not ship (install extra: model_transport[stage])."""
    from .stage import stage_model as _stage_model
    return _stage_model(hf_repo, model_name, **kwargs)
