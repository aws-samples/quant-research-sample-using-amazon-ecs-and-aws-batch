# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Validate a Deployment Console `parameters` object.

Two layers:
  1. Structural validation against config.schema.json (JSON Schema Draft 2020-12).
  2. Cross-field rules that JSON Schema cannot express (conditionals, arithmetic,
     locked fields). These mirror the CDK app's own runtime expectations and the
     rules in design/CONFIG_SCHEMA_PLAN.md section 4.

Returns a ValidationReport with hard errors (block deploy) and warnings (advisory).
This module is the contract the `validate_config` agent tool calls. It must NEVER
trust the agent's claims about validity — it re-derives everything from the object.

Usage:
    from validate import validate
    report = validate(parameters, deployment_identity={"NAMESPACE": "...", ...})
    if not report.ok:
        ...  # surface report.errors back to the agent

Dependencies: jsonschema (pip install jsonschema). If unavailable, structural
validation is skipped with a warning and only cross-field rules run.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

SCHEMA_PATH = Path(__file__).with_name("config.schema.json")

# EC2 instance-class families considered GPU-capable (prefix match, uppercased).
# Source: AWS accelerated-computing families. Kept as a maintained list, not exhaustive.
_GPU_FAMILY_PREFIXES = ("G", "P")
# Families that are GPU-capable but whose names start with a letter we also want to
# allow as CPU (none currently) — kept explicit for future-proofing.
_GPU_FAMILIES_EXACT: tuple[str, ...] = ()

_NAMESPACE_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")


@dataclass
class ValidationReport:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def err(self, msg: str) -> None:
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def to_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, "errors": self.errors, "warnings": self.warnings}


def _load_schema() -> dict[str, Any]:
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _structural(params: dict[str, Any], report: ValidationReport) -> None:
    """Layer 1: JSON Schema validation."""
    try:
        import jsonschema
    except ImportError:
        report.warn(
            "jsonschema not installed; skipped structural validation "
            "(cross-field rules still ran)."
        )
        return

    schema = _load_schema()
    validator_cls = jsonschema.validators.validator_for(schema)
    validator_cls.check_schema(schema)
    validator = validator_cls(schema)
    for e in sorted(validator.iter_errors(params), key=lambda x: list(x.path)):
        loc = "/".join(str(p) for p in e.path) or "(root)"
        report.err(f"[schema] {loc}: {e.message}")


def _is_gpu_family(instance_class: str) -> bool:
    ic = instance_class.upper()
    return ic.startswith(_GPU_FAMILY_PREFIXES) or ic in _GPU_FAMILIES_EXACT


def _cross_field(
    params: dict[str, Any],
    deployment_identity: Optional[dict[str, Any]],
    report: ValidationReport,
) -> None:
    """Layer 2: rules from CONFIG_SCHEMA_PLAN.md section 4."""

    # Rule 1: S3 Express requires availability_zone.id
    if params.get("app_with_s3express"):
        az = params.get("availability_zone") or {}
        if not az.get("id"):
            report.err(
                "Rule 1: app_with_s3express=true requires availability_zone.id "
                "(the AZ ID, e.g. use1-az4)."
            )

    fsx = params.get("fsx") or {}
    # Rule 2: FSx minimums (also enforced structurally, repeated here for clarity
    # when fsx block is present but app_with_fsx drives meaning)
    if params.get("app_with_fsx"):
        if fsx.get("storage_capacity_gib", 0) < 1200:
            report.err("Rule 2: app_with_fsx=true requires fsx.storage_capacity_gib >= 1200.")
        if fsx.get("per_unit_storage_throughput", 0) < 50:
            report.err("Rule 2: app_with_fsx=true requires fsx.per_unit_storage_throughput >= 50.")

    # Rule 3: per_unit_storage_throughput only applies for PERSISTENT_2
    if fsx and fsx.get("deployment_type") != "PERSISTENT_2":
        if fsx.get("per_unit_storage_throughput") not in (None, 0):
            report.warn(
                "Rule 3: fsx.per_unit_storage_throughput only takes effect for "
                f"deployment_type=PERSISTENT_2 (current: {fsx.get('deployment_type')}). "
                "Value will be ignored by the CDK app."
            )

    batch = params.get("batch") or {}
    dtype = batch.get("deployment_type")

    # Rule 4: deployment_type gates required sub-blocks
    needs_single = dtype in ("SINGLE_NODE", "ALL")
    needs_multi = dtype in ("MULTI_NODE", "ALL")
    if needs_single and "single_node" not in batch:
        report.err(f"Rule 4: batch.deployment_type={dtype} requires batch.single_node.")
    if needs_multi and "multi_node" not in batch:
        report.err(f"Rule 4: batch.deployment_type={dtype} requires batch.multi_node.")
    if dtype == "SINGLE_NODE" and "multi_node" in batch:
        report.warn("Rule 4: batch.multi_node is ignored when deployment_type=SINGLE_NODE.")
    if dtype == "MULTI_NODE" and "single_node" in batch:
        report.warn("Rule 4: batch.single_node is ignored when deployment_type=MULTI_NODE.")

    # Rule 5: single_node arithmetic
    sn = batch.get("single_node")
    if needs_single and isinstance(sn, dict):
        maxv = sn.get("maxv_cpus", 0)
        minv = sn.get("minv_cpus", 0)
        nq = sn.get("num_queues", 1)
        ccpu = sn.get("container_cpu", 0)
        if minv > maxv:
            report.err(f"Rule 5: single_node.minv_cpus ({minv}) must be <= maxv_cpus ({maxv}).")
        if nq >= 1:
            per_queue = math.floor(maxv / nq)
            if per_queue < ccpu:
                report.err(
                    f"Rule 5: single_node container_cpu ({ccpu}) exceeds per-queue capacity "
                    f"floor(maxv_cpus/num_queues) = floor({maxv}/{nq}) = {per_queue}; "
                    "jobs could never be scheduled."
                )

    # Rule 6 + 7: multi_node GPU + node topology + capacity
    mn = batch.get("multi_node")
    if needs_multi and isinstance(mn, dict):
        main = mn.get("main") or {}
        worker = mn.get("worker") or {}
        classes = mn.get("instance_classes") or []
        any_gpu = (main.get("container_gpu", 0) > 0) or (worker.get("container_gpu", 0) > 0)
        if any_gpu and not any(_is_gpu_family(c) for c in classes):
            report.err(
                "Rule 6: multi_node requests GPUs (container_gpu>0) but instance_classes "
                f"{classes} include no GPU-capable family (G/P series)."
            )

        # Rule 7: node index sanity
        m_start, m_end = main.get("start_node_index", 0), main.get("end_node_index", 0)
        w_start, w_end = worker.get("start_node_index", 0), worker.get("end_node_index", 0)
        if m_start != 0:
            report.err(f"Rule 7: multi_node.main.start_node_index must be 0 (got {m_start}).")
        if m_end < m_start:
            report.err("Rule 7: multi_node.main end_node_index < start_node_index.")
        if w_end < w_start:
            report.err("Rule 7: multi_node.worker end_node_index < start_node_index.")
        # overlap check between main and worker ranges
        if not (w_start > m_end or w_end < m_start):
            report.err(
                f"Rule 7: multi_node main node range [{m_start},{m_end}] overlaps "
                f"worker range [{w_start},{w_end}]."
            )

        # Rule 6 capacity: maxv >= main_cpu + worker_cpu * worker_count
        worker_count = max(0, w_end - w_start + 1)
        needed = main.get("container_cpu", 0) + worker.get("container_cpu", 0) * worker_count
        if mn.get("maxv_cpus", 0) < needed:
            report.err(
                f"Rule 6: multi_node.maxv_cpus ({mn.get('maxv_cpus')}) is less than the vCPUs "
                f"required by main + {worker_count} worker(s) = {needed}."
            )

    # Rule 8: CodePipeline requires GitHub identity fields
    if params.get("app_with_codepipeline"):
        ident = deployment_identity or {}
        missing = [
            k for k in ("GITHUB_OWNER", "GITHUB_REPO", "GITHUB_TOKEN_SECRET_NAME")
            if not ident.get(k)
        ]
        if missing:
            report.err(
                "Rule 8: app_with_codepipeline=true requires deployment identity fields "
                f"{missing}. (Secrets Manager secret existence is checked at deploy time.)"
            )

    # Rule 10: NAMESPACE must be S3/DNS safe (prefixes bucket names, 63-char limit)
    if deployment_identity and "NAMESPACE" in deployment_identity:
        ns = (deployment_identity.get("NAMESPACE") or "").lower()
        if not _NAMESPACE_RE.match(ns):
            report.err(
                f"Rule 10: NAMESPACE '{ns}' must be lowercase alphanumeric/hyphen, "
                "3-63 chars, start/end alphanumeric (it prefixes S3 bucket names)."
            )

    # Locked-field guard: container_command must never deviate from the canonical value.
    # The agent is not permitted to set it (arbitrary code execution). We flag any
    # non-default value so the tool layer can reject agent-originated changes.
    _check_locked_command(sn, "single_node", report)
    _check_locked_command(mn.get("main") if isinstance(mn, dict) else None, "multi_node.main", report)
    _check_locked_command(mn.get("worker") if isinstance(mn, dict) else None, "multi_node.worker", report)


_CANONICAL_COMMAND = ["python3", "main.py"]


def _check_locked_command(node: Optional[dict[str, Any]], where: str, report: ValidationReport) -> None:
    if not isinstance(node, dict):
        return
    cmd = node.get("container_command")
    if cmd is not None and cmd != _CANONICAL_COMMAND:
        report.warn(
            f"Locked field: {where}.container_command is set to {cmd}. This field is "
            "agent-locked (arbitrary code execution risk); the tool layer must reject "
            "agent-originated values. Allowed default: "
            f"{_CANONICAL_COMMAND}."
        )


def validate(
    params: dict[str, Any],
    deployment_identity: Optional[dict[str, Any]] = None,
) -> ValidationReport:
    """Validate a parameters object. `deployment_identity` carries the .env-style
    fields (NAMESPACE, GITHUB_*) needed for rules 8 and 10; pass None to skip those."""
    report = ValidationReport()
    if not isinstance(params, dict):
        report.err("parameters must be a JSON object.")
        return report
    _structural(params, report)
    _cross_field(params, deployment_identity, report)
    return report


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("usage: python validate.py <parameters.json> [identity.json]", file=sys.stderr)
        raise SystemExit(2)
    with open(sys.argv[1], "r", encoding="utf-8") as f:
        _params = json.load(f)
    _ident = None
    if len(sys.argv) > 2:
        with open(sys.argv[2], "r", encoding="utf-8") as f:
            _ident = json.load(f)
    _report = validate(_params, _ident)
    print(json.dumps(_report.to_dict(), indent=2))
    raise SystemExit(0 if _report.ok else 1)
