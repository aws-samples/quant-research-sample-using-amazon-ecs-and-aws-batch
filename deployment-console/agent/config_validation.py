# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Self-contained config validation + merge for the agent zip.

Mirrors deployment-console/schema/validate.py (cross-field rules) but bundles the
canonical defaults and the deep-merge so the agent has no external file deps. Structural
JSON-Schema validation is intentionally omitted here (jsonschema is heavy for the runtime
and the cross-field rules + defaults catch the meaningful errors); the authoritative
schema still lives in schema/config.schema.json for the build/CI path.

Keep in sync with schema/validate.py and infrastructure/config/parameters.json.
"""

from __future__ import annotations

import math
from typing import Any

# Canonical defaults — mirrors infrastructure/config/parameters.json. The agent's override
# is deep-merged ONTO this, guaranteeing a complete object (and preserving the locked
# container_command). Verified necessary: app.py reads single_node.container_command etc.
DEFAULTS: dict[str, Any] = {
    "availability_zone": {"name": "us-east-1a", "id": "use1-az4"},
    "app_with_codepipeline": False,
    "app_with_fsx": False,
    "app_with_s3express": False,
    "batch": {
        "deployment_type": "ALL",
        "single_node": {
            "maxv_cpus": 50, "minv_cpus": 0, "num_queues": 1,
            "container_cpu": 8, "container_memory": 16384,
            "container_command": ["python3", "main.py"],
            "instance_classes": ["C6I", "C7A", "C7I", "R5", "R5A", "R5B", "R5D",
                                 "R5N", "R6A", "R6I", "R7I"],
            "allocation_strategy": "BEST_FIT_PROGRESSIVE", "spot": False,
        },
        "multi_node": {
            "maxv_cpus": 100, "minv_cpus": 0,
            "main": {"start_node_index": 0, "end_node_index": 0,
                     "container_cpu": 32, "container_gpu": 4, "container_memory": 65536,
                     "container_command": ["python3", "main.py"]},
            "worker": {"start_node_index": 1, "end_node_index": 2,
                       "container_cpu": 32, "container_gpu": 4, "container_memory": 65536,
                       "container_command": ["python3", "main.py"]},
            "instance_classes": ["G5", "C5"],
            "allocation_strategy": "BEST_FIT_PROGRESSIVE", "spot": True,
        },
    },
    "fsx": {"per_unit_storage_throughput": 200, "storage_capacity_gib": 1200,
            "deployment_type": "SCRATCH_2"},
    "s3": {"object_expiration_in_days": 1, "custom_arns": []},
}

_GPU_PREFIXES = ("G", "P")


def merge_with_defaults(override: dict) -> dict:
    """Deep-merge override onto a copy of DEFAULTS (override scalars/lists win)."""
    import json
    base = json.loads(json.dumps(DEFAULTS))

    def _m(a: dict, b: dict) -> None:
        for k, v in b.items():
            if isinstance(v, dict) and isinstance(a.get(k), dict):
                _m(a[k], v)
            else:
                a[k] = v
    if isinstance(override, dict):
        _m(base, override)
    return base


def validate(params: dict) -> dict:
    """Cross-field validation of a COMPLETE params object. Returns
    {"ok", "errors", "warnings"}."""
    errors: list[str] = []
    warnings: list[str] = []

    az = params.get("availability_zone", {})
    if params.get("app_with_s3express") and not az.get("id"):
        errors.append("app_with_s3express=true requires availability_zone.id.")

    fsx = params.get("fsx", {})
    if params.get("app_with_fsx"):
        if fsx.get("storage_capacity_gib", 0) < 1200:
            errors.append("app_with_fsx requires fsx.storage_capacity_gib >= 1200.")
        if fsx.get("per_unit_storage_throughput", 0) < 50:
            errors.append("app_with_fsx requires fsx.per_unit_storage_throughput >= 50.")
    if fsx and fsx.get("deployment_type") != "PERSISTENT_2" and fsx.get("per_unit_storage_throughput"):
        warnings.append("fsx.per_unit_storage_throughput only applies for PERSISTENT_2; ignored otherwise.")

    batch = params.get("batch", {})
    dtype = batch.get("deployment_type")
    if dtype not in ("SINGLE_NODE", "MULTI_NODE", "ALL"):
        errors.append(f"batch.deployment_type must be SINGLE_NODE|MULTI_NODE|ALL (got {dtype}).")
    needs_single = dtype in ("SINGLE_NODE", "ALL")
    needs_multi = dtype in ("MULTI_NODE", "ALL")

    sn = batch.get("single_node")
    if needs_single and isinstance(sn, dict):
        maxv, minv, nq, ccpu = (sn.get("maxv_cpus", 0), sn.get("minv_cpus", 0),
                                sn.get("num_queues", 1), sn.get("container_cpu", 0))
        if minv > maxv:
            errors.append(f"single_node.minv_cpus ({minv}) > maxv_cpus ({maxv}).")
        if nq >= 1 and math.floor(maxv / nq) < ccpu:
            errors.append(f"single_node container_cpu ({ccpu}) exceeds per-queue capacity "
                          f"floor({maxv}/{nq})={math.floor(maxv/nq)}; jobs can't schedule.")

    mn = batch.get("multi_node")
    if needs_multi and isinstance(mn, dict):
        main, worker = mn.get("main", {}), mn.get("worker", {})
        classes = mn.get("instance_classes", [])
        any_gpu = main.get("container_gpu", 0) > 0 or worker.get("container_gpu", 0) > 0
        if any_gpu and not any(str(c).upper().startswith(_GPU_PREFIXES) for c in classes):
            errors.append(f"multi_node requests GPUs but instance_classes {classes} "
                          "include no GPU family (G/P).")
        w_start, w_end = worker.get("start_node_index", 1), worker.get("end_node_index", 2)
        worker_count = max(0, w_end - w_start + 1)
        needed = main.get("container_cpu", 0) + worker.get("container_cpu", 0) * worker_count
        if mn.get("maxv_cpus", 0) < needed:
            errors.append(f"multi_node.maxv_cpus ({mn.get('maxv_cpus')}) < required "
                          f"main+{worker_count} workers = {needed}.")

    return {"ok": not errors, "errors": errors, "warnings": warnings}
