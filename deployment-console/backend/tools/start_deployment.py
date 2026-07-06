# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""start_deployment — the agent tool that triggers a deployment.

Validates the agent's override, persists it to S3, and starts the CodeBuild project that
runs `cdk deploy`. Returns a run id the UI/agent can poll.

CONTRACT (must hold before this is callable):
  - The override has already been validated by validate_config (schema/validate.py).
    We re-validate here defensively — never trust the caller.
  - Human approval has occurred upstream (in this POC, approval is implicit / out of band;
    a Step Functions approval gate is possible future work).

Inputs (event):
  {
    "run_id": "string",                 # caller-supplied or generated upstream
    "namespace": "string",              # S3-safe resource prefix
    "override": { ... },                # agent-produced PARTIAL parameter override
    "deployment_identity": { ... }      # optional: NAMESPACE, GITHUB_* for validation rules
  }

Env:
  ARTIFACT_BUCKET     - S3 bucket for overrides/outputs
  CODEBUILD_PROJECT   - CodeBuild project name to start
  TARGET_ACCOUNT_ID, TARGET_REGION

Returns: { "ok": bool, "run_id", "build_id"?, "override_s3_uri"?, "errors": [...] }
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import boto3

# Make schema/validate.py importable (bundled alongside in deployment, sibling in repo)
_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schema"
if str(_SCHEMA_DIR) not in sys.path:
    sys.path.insert(0, str(_SCHEMA_DIR))

try:
    from validate import validate  # type: ignore
except Exception:  # pragma: no cover - validate is optional at import time in some envs
    validate = None  # type: ignore

s3 = boto3.client("s3")
codebuild = boto3.client("codebuild")


def handler(event: dict, _context=None) -> dict:
    run_id = event.get("run_id")
    namespace = event.get("namespace")
    override = event.get("override")
    identity = event.get("deployment_identity")

    errors: list[str] = []
    if not run_id:
        errors.append("run_id is required.")
    if not namespace:
        errors.append("namespace is required.")
    if not isinstance(override, dict):
        errors.append("override must be a JSON object (partial parameter override).")
    if errors:
        return {"ok": False, "run_id": run_id, "errors": errors}

    # Defensive re-validation. NOTE: validate() expects a COMPLETE parameters object;
    # the override is partial, so we validate only if a full object was passed, else
    # defer validation to the build's merge step (which produces the complete object).
    if validate is not None and _looks_complete(override):
        report = validate(override, identity)
        if not report.ok:
            return {"ok": False, "run_id": run_id, "errors": report.errors,
                    "warnings": report.warnings}

    bucket = os.environ["ARTIFACT_BUCKET"]
    project = os.environ["CODEBUILD_PROJECT"]
    key = f"runs/{run_id}/override.json"
    s3.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(override).encode("utf-8"),
        ContentType="application/json",
    )
    override_uri = f"s3://{bucket}/{key}"

    build = codebuild.start_build(
        projectName=project,
        environmentVariablesOverride=[
            {"name": "RUN_ID", "value": run_id, "type": "PLAINTEXT"},
            {"name": "NAMESPACE", "value": namespace, "type": "PLAINTEXT"},
            {"name": "OVERRIDE_S3_URI", "value": override_uri, "type": "PLAINTEXT"},
            {"name": "TARGET_ACCOUNT_ID",
             "value": os.environ.get("TARGET_ACCOUNT_ID", ""), "type": "PLAINTEXT"},
            {"name": "TARGET_REGION",
             "value": os.environ.get("TARGET_REGION", "us-east-1"), "type": "PLAINTEXT"},
        ],
    )
    build_id = build["build"]["id"]
    return {
        "ok": True,
        "run_id": run_id,
        "build_id": build_id,
        "override_s3_uri": override_uri,
    }


def _looks_complete(obj: dict) -> bool:
    """Heuristic: a complete parameters object has the top-level required keys."""
    required = {"availability_zone", "batch", "fsx", "s3",
                "app_with_codepipeline", "app_with_fsx", "app_with_s3express"}
    return required.issubset(obj.keys())
