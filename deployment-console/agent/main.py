# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Deployment Console agent — runs on Amazon Bedrock AgentCore Runtime.

Contract: BedrockAgentCoreApp provides the /invocations + /ping endpoints on :8080.
The @app.entrypoint receives the invocation payload ({"prompt": "..."}) and returns the
agent's reply. The agent is a Strands Agent driving Claude (Bedrock) with 3 tools:

  - validate_config(override)            -> structural + cross-field validation result
  - start_deployment(override)           -> S3 put + CodeBuild startBuild -> build_id
  - get_deployment_status(build_id)      -> PULL: BatchGetBuilds + (optional) log tail

POC posture: no auth, public-GitHub infra source, pull observability. Tools call AWS
directly via boto3 — no AgentCore Gateway, no tool Lambdas.

Env vars (set on the agent runtime):
  ARTIFACT_BUCKET    - S3 bucket for parameter overrides
  CODEBUILD_PROJECT  - CodeBuild project to start
  TARGET_ACCOUNT_ID, TARGET_REGION
  BEDROCK_MODEL_ID   - e.g. us.anthropic.claude-sonnet-4-5-20250929-v1:0 (geo profile)
"""

from __future__ import annotations

import json
import os
import time

import boto3
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands import Agent, tool
from strands.models import BedrockModel

import config_validation as cv  # bundled copy of schema/validate.py logic

REGION = os.environ.get("TARGET_REGION", "us-east-1")
ARTIFACT_BUCKET = os.environ.get("ARTIFACT_BUCKET", "")
CODEBUILD_PROJECT = os.environ.get("CODEBUILD_PROJECT", "")
TARGET_ACCOUNT_ID = os.environ.get("TARGET_ACCOUNT_ID", "")
# Namespace prefix of the quant-research stacks this console deploys (e.g.
# "agentpoc" → network-stack-agentpoc, ...). Used to scope the read tools.
NAMESPACE = os.environ.get("NAMESPACE", "agentpoc")
MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")

_s3 = boto3.client("s3", region_name=REGION)
_codebuild = boto3.client("codebuild", region_name=REGION)
_logs = boto3.client("logs", region_name=REGION)
_cfn = boto3.client("cloudformation", region_name=REGION)


def _in_namespace(name: str) -> bool:
    """Quant-research stacks for this console's namespace, excluding the console's
    own infra (hosting/phase0 stacks contain 'console')."""
    return NAMESPACE in name and "console" not in name

app = BedrockAgentCoreApp()


# --------------------------------------------------------------------------- tools
@tool
def validate_config(override: dict) -> dict:
    """Validate a partial deployment parameter override against the schema and the
    cross-field rules. Call this before start_deployment. `override` is a partial
    parameters object (e.g. {"batch": {"deployment_type": "SINGLE_NODE"}, "app_with_fsx": false}).
    Returns {"ok": bool, "errors": [...], "warnings": [...]}. The override is deep-merged
    onto the canonical defaults at deploy time, so it need not be complete."""
    merged = cv.merge_with_defaults(override)
    report = cv.validate(merged)
    return report


@tool
def start_deployment(override: dict, run_id: str = "") -> dict:
    """Start a real deployment: upload the parameter override to S3 and trigger the
    CodeBuild project that runs `cdk deploy` of the quant-research infrastructure. Only
    call this AFTER validate_config returns ok and the user has confirmed. Returns
    {"ok": bool, "build_id": str, "run_id": str}. Use the build_id with
    get_deployment_status to track progress."""
    if not run_id:
        run_id = f"run-{int(time.time())}"
    # defensive re-validation
    report = cv.validate(cv.merge_with_defaults(override))
    if not report["ok"]:
        return {"ok": False, "errors": report["errors"], "run_id": run_id}

    key = f"runs/{run_id}/override.json"
    _s3.put_object(Bucket=ARTIFACT_BUCKET, Key=key,
                   Body=json.dumps(override).encode("utf-8"),
                   ContentType="application/json")
    override_uri = f"s3://{ARTIFACT_BUCKET}/{key}"
    build = _codebuild.start_build(
        projectName=CODEBUILD_PROJECT,
        environmentVariablesOverride=[
            {"name": "RUN_ID", "value": run_id, "type": "PLAINTEXT"},
            {"name": "NAMESPACE", "value": "agentpoc", "type": "PLAINTEXT"},
            {"name": "OVERRIDE_S3_URI", "value": override_uri, "type": "PLAINTEXT"},
            {"name": "TARGET_ACCOUNT_ID", "value": TARGET_ACCOUNT_ID, "type": "PLAINTEXT"},
            {"name": "TARGET_REGION", "value": REGION, "type": "PLAINTEXT"},
        ],
    )
    return {"ok": True, "build_id": build["build"]["id"], "run_id": run_id,
            "override_s3_uri": override_uri}


@tool
def get_deployment_status(build_id: str, include_logs: bool = False) -> dict:
    """Pull the current status of a deployment by its CodeBuild build_id. Returns the
    build status (IN_PROGRESS/SUCCEEDED/FAILED/...), current phase, per-phase summary,
    and — if include_logs=True — the tail of the build log. Use this to answer
    'how's the deployment going?' questions."""
    resp = _codebuild.batch_get_builds(ids=[build_id])
    builds = resp.get("builds", [])
    if not builds:
        return {"ok": False, "errors": [f"No build {build_id}"]}
    b = builds[0]
    out = {
        "ok": True,
        "build_status": b.get("buildStatus"),
        "current_phase": b.get("currentPhase"),
        "complete": bool(b.get("buildComplete")),
        "phases": [{"phase": p.get("phaseType"), "status": p.get("phaseStatus")}
                   for p in b.get("phases", [])],
    }
    if include_logs:
        loc = b.get("logs", {}) or {}
        g, s = loc.get("groupName"), loc.get("streamName")
        if g and s:
            try:
                ev = _logs.get_log_events(logGroupName=g, logStreamName=s,
                                          limit=40, startFromHead=False)
                out["log_tail"] = [e["message"].rstrip() for e in ev.get("events", [])]
            except Exception as e:  # log stream may not exist until PROVISIONING done
                out["log_note"] = f"logs not available yet: {e}"
    return out


@tool
def list_deployed_stacks() -> dict:
    """List the quant-research CloudFormation stacks already deployed in this account
    (this console's namespace only). Use this to answer 'what is already deployed?' /
    'what's live in my account?'. Returns {"ok": bool, "stacks": [{"name", "status",
    "updated"}], "count": int}. Stack names look like network-stack-agentpoc,
    s3-storage-stack-agentpoc, batch-job-single-node-with-cpu-stack-agentpoc, etc.
    An empty list means nothing is deployed yet."""
    active = ["CREATE_COMPLETE", "UPDATE_COMPLETE", "CREATE_IN_PROGRESS",
              "UPDATE_IN_PROGRESS", "ROLLBACK_COMPLETE", "UPDATE_ROLLBACK_COMPLETE",
              "CREATE_FAILED", "DELETE_IN_PROGRESS"]
    stacks = []
    try:
        paginator = _cfn.get_paginator("list_stacks")
        for page in paginator.paginate(StackStatusFilter=active):
            for s in page.get("StackSummaries", []):
                name = s.get("StackName", "")
                if not _in_namespace(name):
                    continue
                when = s.get("LastUpdatedTime") or s.get("CreationTime")
                stacks.append({
                    "name": name,
                    "status": s.get("StackStatus"),
                    "updated": when.isoformat() if when else None,
                })
    except Exception as e:
        return {"ok": False, "errors": [str(e)]}
    stacks.sort(key=lambda x: x["name"])
    return {"ok": True, "stacks": stacks, "count": len(stacks)}


@tool
def describe_stack_resources(stack_name: str) -> dict:
    """List the AWS resources inside a deployed stack (scoped to this console's
    namespace). Use after list_deployed_stacks to drill into a specific stack.
    Returns {"ok": bool, "stack": str, "resources": [{"type", "logical_id",
    "physical_id", "status"}]}."""
    if not _in_namespace(stack_name):
        return {"ok": False, "errors": ["stack not in this console's namespace"]}
    try:
        resp = _cfn.describe_stack_resources(StackName=stack_name)
    except Exception as e:
        return {"ok": False, "errors": [str(e)]}
    resources = [{
        "type": r.get("ResourceType"),
        "logical_id": r.get("LogicalResourceId"),
        "physical_id": r.get("PhysicalResourceId"),
        "status": r.get("ResourceStatus"),
    } for r in resp.get("StackResources", [])]
    return {"ok": True, "stack": stack_name, "resources": resources}


# --------------------------------------------------------------------------- agent
SYSTEM_PROMPT = """You are the Deployment Console agent. You both CONVERSE about this project
and help an operator configure and deploy it. Be brief and accurate; give crisp answers, not
walls of text. When the user seems ready, offer to validate and deploy.

# Tools
- validate_config: check a partial parameter override is valid.
- start_deployment: trigger the real CDK deployment (ONLY after validate_config is ok AND
  the user explicitly confirms they want to deploy).
- get_deployment_status: report live status of a running/finished deployment (needs a build_id).
- list_deployed_stacks: list the quant-research stacks ALREADY deployed in this account. Call
  this whenever the user asks "what's deployed?", "what's live?", "what's in my account?", or
  similar — do NOT tell the user to check the console themselves; you can see it directly.
- describe_stack_resources: list the resources inside one deployed stack (drill-in).

# Answering "what's deployed?" / "check status"
- "What's already deployed?" → call list_deployed_stacks and summarize the result (group by
  what they map to: network/VPC, S3, pipeline/ECR, single-node CPU Batch, multi-node GPU Batch,
  FSx). If the list is empty, say nothing is deployed yet and offer to deploy. NEVER claim you
  lack a tool to see deployments, and NEVER invent stack names — report exactly what the tool
  returns.
- "How's the deployment going?" → if you have a build_id from this session, call
  get_deployment_status. If you don't have one, DON'T just ask for it — also call
  list_deployed_stacks so you can report the current deployed state, then mention you can track
  a specific build live if they start or name one.

# About this project
A quantitative-research sample on AWS Batch: it runs order-flow / orderbook-imbalance ML on
financial tick data, scaling from CPU feature-engineering to multi-node GPU model training.
The infrastructure is defined and deployed with the AWS CDK.

# What you can configure
The config is an OVERRIDE deep-merged onto canonical defaults, so it need not be complete.
- batch.deployment_type (enum): SINGLE_NODE = CPU-only Batch; MULTI_NODE = multi-node GPU
  Batch; ALL = both. This is the most important field.
- app_with_fsx (bool): adds FSx for Lustre as fast shared scratch (S3-linked), at extra cost.
- app_with_s3express (bool): adds an S3 Express One Zone bucket (low-latency single-AZ).
- app_with_codepipeline (bool): adds a CI/CD CodePipeline that builds the container image.
- Sizing knobs: single_node.maxv_cpus, single_node.instance_classes, container_cpu/memory;
  multi_node main/worker container_gpu/cpu/memory, multi_node.maxv_cpus, instance_classes.
Key validation rules to keep in mind:
- app_with_s3express=true requires availability_zone.id to be set.
- app_with_fsx=true requires fsx.storage_capacity_gib >= 1200 (and per_unit_storage_throughput >= 50).
- If a multi_node container requests GPUs, instance_classes must include a GPU family (G/P series).
- single_node: minv_cpus <= maxv_cpus, and floor(maxv_cpus / num_queues) >= container_cpu.

# What stacks will deploy (real names use the "-agentpoc" namespace suffix)
ALWAYS: network-stack-agentpoc (VPC + endpoints), s3-storage-stack-agentpoc (buckets),
deployment-pipeline-stack-agentpoc (ECR repo, plus the CodePipeline itself only when
app_with_codepipeline=true).
CONDITIONALLY: batch-job-single-node-with-cpu-stack-agentpoc (when SINGLE_NODE or ALL);
batch-job-multi-node-with-gpu-stack-agentpoc (when MULTI_NODE or ALL); fsx-storage-stack-agentpoc
(when app_with_fsx=true).
So SINGLE_NODE with no FSx is ~4 stacks; ALL with FSx is ~6 stacks. Never refer to stacks by
made-up names like "quant-research-network" — use the real "-stack-agentpoc" names above, or
better, call list_deployed_stacks to see exactly what exists.

# Picking rational config
- Cheapest data prep / feature engineering: SINGLE_NODE, no FSx.
- GPU model training: MULTI_NODE (or ALL for end-to-end).
- FSx adds fast shared scratch (good for large shared datasets) but costs more.

# Workflow
Understand the user's goal -> propose a small override -> validate_config -> summarize what
will deploy -> (the UI shows the user an interactive options form with a Deploy button) -> when
the user confirms (they click Deploy, which sends "confirm and deploy" plus the exact config),
call start_deployment -> report the build_id -> on request, get_deployment_status.

# IMPORTANT — how confirmation works in THIS UI (do not ask yes/no questions)
The user is in a graphical console, NOT a plain chat. After you validate a config, the UI
automatically renders an editable options form and a **Deploy** button right below your message.
So when you finish summarizing a validated proposal, DO NOT end with a yes/no question like
"Do you want to proceed?", "Shall I deploy?", or "Confirm?". Instead close with a short pointer
to the UI, e.g. "Review and adjust the options below, then click Deploy to start the build." —
one sentence, no question mark.
Only call start_deployment when the user's message clearly confirms (e.g. contains "confirm and
deploy" or an explicit config override to deploy). Never start_deployment on a mere proposal.
When you call a tool, explain what you found in plain language."""


def _build_agent() -> Agent:
    model = BedrockModel(model_id=MODEL_ID, region_name=REGION)
    return Agent(model=model, system_prompt=SYSTEM_PROMPT,
                 tools=[validate_config, start_deployment, get_deployment_status,
                        list_deployed_stacks, describe_stack_resources])


@app.entrypoint
def invoke(payload: dict) -> dict:
    """AgentCore entrypoint. payload = {"prompt": "..."}; returns {"result": "..."}."""
    prompt = (payload or {}).get("prompt", "")
    if not prompt:
        return {"result": "Send a JSON payload like {\"prompt\": \"deploy a CPU batch stack\"}."}
    agent = _build_agent()
    result = agent(prompt)
    # Strands AgentResult -> stringify the final message
    return {"result": str(result)}


if __name__ == "__main__":
    app.run()
