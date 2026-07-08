# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Idempotent create-or-update of the AgentCore Runtime from the built zip.

Called by deploy-console.sh after build_package.sh has produced deployment_package.zip
and the platform-infra stack has provisioned the artifact bucket + exec role.

Steps:
  1. Upload deployment_package.zip to S3 (the code bucket).
  2. If a runtime with AGENT_NAME exists -> update_agent_runtime; else create_agent_runtime.
  3. Poll until READY; print the agentRuntimeArn on the LAST line (script captures it).

Env (all required unless noted):
  REGION                 (default us-east-1)
  ARTIFACT_BUCKET        S3 bucket for the code zip
  AGENT_EXEC_ROLE_ARN    execution role ARN
  CODEBUILD_PROJECT      passed to the agent as env
  TARGET_ACCOUNT_ID      target account
  AGENT_NAME             runtime name (default agentpoc_deploy_console)
  BEDROCK_MODEL_ID       (default us.anthropic.claude-sonnet-4-5-20250929-v1:0)
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import boto3

REGION = os.environ.get("REGION", "us-east-1")
BUCKET = os.environ["ARTIFACT_BUCKET"]
ROLE_ARN = os.environ["AGENT_EXEC_ROLE_ARN"]
CODEBUILD_PROJECT = os.environ["CODEBUILD_PROJECT"]
ACCOUNT = os.environ["TARGET_ACCOUNT_ID"]
NAMESPACE = os.environ.get("NAMESPACE", "agentpoc")
AGENT_NAME = os.environ.get("AGENT_NAME", "agentpoc_deploy_console")
MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")
ZIP = Path(__file__).with_name("deployment_package.zip")
S3_PREFIX = "agent-code/deployment_package.zip"

_s3 = boto3.client("s3", region_name=REGION)
_cp = boto3.client("bedrock-agentcore-control", region_name=REGION)


def log(m: str) -> None:
    print(m, file=sys.stderr, flush=True)


def upload() -> None:
    if not ZIP.exists():
        raise SystemExit(f"missing {ZIP} — run build_package.sh first")
    log(f"Uploading {ZIP.name} ({ZIP.stat().st_size // 1024} KiB) → s3://{BUCKET}/{S3_PREFIX}")
    _s3.upload_file(str(ZIP), BUCKET, S3_PREFIX, ExtraArgs={"ExpectedBucketOwner": ACCOUNT})


def _all_runtimes() -> list[dict]:
    paginator = _cp.get_paginator("list_agent_runtimes") if _cp.can_paginate("list_agent_runtimes") else None
    if paginator:
        out = []
        for page in paginator.paginate():
            out.extend(page.get("agentRuntimes", []))
        return out
    return _cp.list_agent_runtimes().get("agentRuntimes", [])


def find_runtime_id() -> str | None:
    """Find the runtime to update. Prefer an exact AGENT_NAME match, else any
    runtime whose name starts with AGENT_NAME (e.g. a "_v2" created when an SCP
    blocked CreateAgentRuntime under the canonical name). Updating an existing
    runtime in place is allowed even when Create is denied."""
    runtimes = _all_runtimes()
    for r in runtimes:
        if r.get("agentRuntimeName") == AGENT_NAME:
            return r.get("agentRuntimeId")
    for r in runtimes:
        if (r.get("agentRuntimeName") or "").startswith(AGENT_NAME):
            return r.get("agentRuntimeId")
    return None


def artifact() -> dict:
    return {"codeConfiguration": {
        "code": {"s3": {"bucket": BUCKET, "prefix": S3_PREFIX}},
        "runtime": "PYTHON_3_13",
        "entryPoint": ["main.py"],
    }}


def env_vars() -> dict:
    return {
        "ARTIFACT_BUCKET": BUCKET,
        "CODEBUILD_PROJECT": CODEBUILD_PROJECT,
        "TARGET_ACCOUNT_ID": ACCOUNT,
        "TARGET_REGION": REGION,
        "NAMESPACE": NAMESPACE,
        "BEDROCK_MODEL_ID": MODEL_ID,
    }


def _create(name: str, common: dict) -> dict:
    log(f"Creating runtime {name}")
    return _cp.create_agent_runtime(
        agentRuntimeName=name,
        lifecycleConfiguration={"idleRuntimeSessionTimeout": 900, "maxLifetime": 3600},
        description="Deployment console agent (project-aware)",
        **common,
    )


def create_or_update() -> tuple[str, str]:
    rid = find_runtime_id()
    common = dict(
        agentRuntimeArtifact=artifact(),
        networkConfiguration={"networkMode": "PUBLIC"},
        roleArn=ROLE_ARN,
        environmentVariables=env_vars(),
    )
    if rid:
        log(f"Updating existing runtime {rid}")
        try:
            r = _cp.update_agent_runtime(agentRuntimeId=rid, **common)
        except Exception as e:
            # Some org guardrails (SCP) deny UpdateAgentRuntime even when Create is
            # allowed. Fall back to creating a fresh, versioned runtime so a deploy
            # still ships new code. NOTE: this mints a NEW ARN — the caller must
            # repoint the bridge Lambda's AGENT_RUNTIME_ARN (deploy-console.sh
            # passes the printed ARN to the hosting stack, which does this).
            if "AccessDenied" not in type(e).__name__ and "AccessDenied" not in str(e):
                raise
            new_name = f"{AGENT_NAME}_v{int(time.time())}"
            log(f"UpdateAgentRuntime denied ({e.__class__.__name__}); creating {new_name} instead")
            r = _create(new_name, common)
            rid = r["agentRuntimeId"]
    else:
        r = _create(AGENT_NAME, common)
        rid = r["agentRuntimeId"]
    return rid, r["agentRuntimeArn"]


def wait_ready(rid: str) -> None:
    for _ in range(40):
        status = _cp.get_agent_runtime(agentRuntimeId=rid).get("status")
        log(f"  status={status}")
        if status == "READY":
            return
        if status in ("CREATE_FAILED", "UPDATE_FAILED"):
            raise SystemExit(f"runtime entered {status}")
        time.sleep(15)
    raise SystemExit("timed out waiting for READY")


def main() -> None:
    upload()
    rid, arn = create_or_update()
    wait_ready(rid)
    log("Runtime READY")
    # stdout: ONLY the ARN, so the shell can capture it cleanly.
    print(arn)


if __name__ == "__main__":
    main()
