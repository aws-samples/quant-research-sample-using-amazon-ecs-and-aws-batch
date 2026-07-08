# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Bridge Lambda (Function URL) — the browser's only backend.

Keeps AWS creds server-side (the Lambda role); the SPA calls these over HTTPS.
Routes (path from the Function URL / CloudFront /api/* behavior):

  POST /api/message  body {"prompt": "...", "sessionId": "..."}
       -> bedrock-agentcore invoke_agent_runtime -> {"result": "..."}
  GET  /api/status?buildId=...
       -> codebuild batch_get_builds -> {buildStatus, currentPhase, phases[], logTail?}
  GET  /api/logs?buildId=...&nextToken=...
       -> codebuild batch_get_builds (for the log location) + logs get_log_events
       -> {lines[], nextToken}   (nextToken pages/tails the build's CloudWatch stream)
  GET  /api/health -> {"ok": true}

POC posture: no auth (AuthType NONE). Single trusted operator.
Env: AGENT_RUNTIME_ARN, REGION.
"""

from __future__ import annotations

import json
import os
import uuid

import boto3

REGION = os.environ.get("REGION", "us-east-1")
AGENT_ARN = os.environ.get("AGENT_RUNTIME_ARN", "")
CODEBUILD_PROJECT = os.environ.get("CODEBUILD_PROJECT", "agentpoc-deploy-console-cdk-deploy")

_agent = boto3.client("bedrock-agentcore", region_name=REGION)
_codebuild = boto3.client("codebuild", region_name=REGION)
_cfn = boto3.client("cloudformation", region_name=REGION)
_logs = boto3.client("logs", region_name=REGION)

# Only expose the quant-research stacks deployed by this console — never the console's
# own infra or unrelated account stacks. These prefixes match the CDK app's stack names
# for NAMESPACE=agentpoc; "console" stacks are explicitly excluded.
STACK_NAMESPACE = os.environ.get("STACK_NAMESPACE", "agentpoc")

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "content-type",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
}


def _resp(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json", **CORS},
        "body": json.dumps(body),
    }


def _route(event: dict) -> tuple[str, str]:
    """Return (method, path) across Function URL / API GW payload shapes."""
    ctx = event.get("requestContext", {})
    http = ctx.get("http", {})
    method = http.get("method") or event.get("httpMethod", "GET")
    path = http.get("path") or event.get("rawPath") or event.get("path", "/")
    return method.upper(), path


def handler(event, _context=None):
    method, path = _route(event)

    if method == "OPTIONS":
        return _resp(200, {"ok": True})

    if path.endswith("/health"):
        return _resp(200, {"ok": True, "agent": bool(AGENT_ARN)})

    try:
        if path.endswith("/message") and method == "POST":
            return _message(event)
        if path.endswith("/status") and method == "GET":
            return _status(event)
        if path.endswith("/logs") and method == "GET":
            return _logs_route(event)
        if path.endswith("/builds") and method == "GET":
            return _builds(event)
        if path.endswith("/stacks") and method == "GET":
            return _stacks(event)
        if path.endswith("/resources") and method == "GET":
            return _resources(event)
    except Exception as e:  # surface a clean error to the UI
        return _resp(502, {"ok": False, "error": f"{type(e).__name__}: {e}"})

    return _resp(404, {"ok": False, "error": f"no route for {method} {path}"})


def _message(event: dict) -> dict:
    body = json.loads(event.get("body") or "{}")
    prompt = body.get("prompt", "")
    if not prompt:
        return _resp(400, {"ok": False, "error": "prompt required"})
    session = body.get("sessionId") or f"web-{uuid.uuid4().hex}"
    # AgentCore requires runtimeSessionId length >= 33.
    if len(session) < 33:
        session = (session + "-" + uuid.uuid4().hex)[:48]

    r = _agent.invoke_agent_runtime(
        agentRuntimeArn=AGENT_ARN,
        runtimeSessionId=session,
        payload=json.dumps({"prompt": prompt}).encode(),
        qualifier="DEFAULT",
    )
    raw = r.get("response")
    text = raw.read().decode() if hasattr(raw, "read") else "".join(
        x.decode() if isinstance(x, bytes) else str(x) for x in raw
    )
    # Agent returns {"result": "..."}; pass it through (and the raw, defensively).
    result = text
    try:
        result = json.loads(text).get("result", text)
    except Exception:
        pass
    return _resp(200, {"ok": True, "result": result, "sessionId": session})


def _status(event: dict) -> dict:
    qs = event.get("queryStringParameters") or {}
    build_id = qs.get("buildId")
    if not build_id:
        return _resp(400, {"ok": False, "error": "buildId required"})
    resp = _codebuild.batch_get_builds(ids=[build_id])
    builds = resp.get("builds", [])
    if not builds:
        return _resp(404, {"ok": False, "error": f"no build {build_id}"})
    b = builds[0]
    return _resp(200, {
        "ok": True,
        "buildId": build_id,
        "buildStatus": b.get("buildStatus"),
        "currentPhase": b.get("currentPhase"),
        "complete": bool(b.get("buildComplete")),
        "phases": [{"phase": p.get("phaseType"), "status": p.get("phaseStatus")}
                   for p in b.get("phases", [])],
    })


def _logs_route(event: dict) -> dict:
    """Tail a build's CloudWatch log stream — the real CodeBuild execution output.

    batch_get_builds carries the build's log location (logs.groupName/streamName);
    logs:GetLogEvents pages through the stream. The frontend passes the previous
    response's `nextToken` to fetch only new lines (GetLogEvents returns the SAME
    forward token at the end of the stream, so polling with it yields just the tail).

    The log location is only populated once the build reaches PROVISIONING; before
    that we return no lines (not an error) so the poller keeps trying."""
    qs = event.get("queryStringParameters") or {}
    build_id = qs.get("buildId")
    if not build_id:
        return _resp(400, {"ok": False, "error": "buildId required"})

    resp = _codebuild.batch_get_builds(ids=[build_id])
    builds = resp.get("builds", [])
    if not builds:
        return _resp(404, {"ok": False, "error": f"no build {build_id}"})

    loc = builds[0].get("logs") or {}
    group, stream = loc.get("groupName"), loc.get("streamName")
    if not group or not stream:
        # Logs not available yet (build still PROVISIONING) — empty, not an error.
        return _resp(200, {"ok": True, "lines": [], "nextToken": qs.get("nextToken")})

    kwargs = {
        "logGroupName": group,
        "logStreamName": stream,
        "startFromHead": True,
        "limit": 1000,
    }
    if qs.get("nextToken"):
        kwargs["nextToken"] = qs["nextToken"]
    try:
        ev = _logs.get_log_events(**kwargs)
    except _logs.exceptions.ResourceNotFoundException:
        return _resp(200, {"ok": True, "lines": [], "nextToken": qs.get("nextToken")})
    lines = [e.get("message", "").rstrip("\n") for e in ev.get("events", [])]
    return _resp(200, {"ok": True, "lines": lines, "nextToken": ev.get("nextForwardToken")})


def _builds(event: dict) -> dict:
    """List the most recent CodeBuild builds for the deploy project, newest first.
    Drives the chat 'recent builds' block so it reflects REAL builds (incl. ones
    from prior sessions), not just this browser session. ?limit= caps the count."""
    qs = event.get("queryStringParameters") or {}
    try:
        limit = max(1, min(20, int(qs.get("limit", "8"))))
    except (TypeError, ValueError):
        limit = 8
    ids_resp = _codebuild.list_builds_for_project(
        projectName=CODEBUILD_PROJECT, sortOrder="DESCENDING")
    ids = ids_resp.get("ids", [])[:limit]
    if not ids:
        return _resp(200, {"ok": True, "builds": []})
    detail = _codebuild.batch_get_builds(ids=ids)
    builds = []
    for b in detail.get("builds", []):
        start = b.get("startTime")
        end = b.get("endTime")
        builds.append({
            "buildId": b.get("id"),
            "buildNumber": b.get("buildNumber"),
            "status": b.get("buildStatus"),          # SUCCEEDED / FAILED / IN_PROGRESS / ...
            "currentPhase": b.get("currentPhase"),
            "complete": bool(b.get("buildComplete")),
            "startedAt": start.isoformat() if start else None,
            "finishedAt": end.isoformat() if end else None,
            "durationSec": int((end - start).total_seconds()) if (start and end) else None,
            "phases": [{"phase": p.get("phaseType"), "status": p.get("phaseStatus")}
                       for p in b.get("phases", [])],
        })
    # batch_get_builds doesn't preserve the requested order — restore newest-first.
    order = {bid: i for i, bid in enumerate(ids)}
    builds.sort(key=lambda x: order.get(x["buildId"], 1e9))
    return _resp(200, {"ok": True, "builds": builds})


def _in_namespace(name: str) -> bool:
    """Quant-research stacks for this console's namespace, excluding the console's
    own infra (hosting/phase0 stacks contain 'console')."""
    return STACK_NAMESPACE in name and "console" not in name


def _stacks(_event: dict) -> dict:
    """List the deployed quant-research CloudFormation stacks (drives the CFN node)."""
    stacks = []
    paginator = _cfn.get_paginator("list_stacks")
    active = ["CREATE_COMPLETE", "UPDATE_COMPLETE", "CREATE_IN_PROGRESS",
              "UPDATE_IN_PROGRESS", "ROLLBACK_COMPLETE", "UPDATE_ROLLBACK_COMPLETE",
              "CREATE_FAILED", "DELETE_IN_PROGRESS"]
    for page in paginator.paginate(StackStatusFilter=active):
        for s in page.get("StackSummaries", []):
            name = s.get("StackName", "")
            if not _in_namespace(name):
                continue
            stacks.append({
                "name": name,
                "status": s.get("StackStatus"),
                "updated": (s.get("LastUpdatedTime") or s.get("CreationTime")).isoformat()
                if (s.get("LastUpdatedTime") or s.get("CreationTime")) else None,
            })
    stacks.sort(key=lambda x: x["name"])
    return _resp(200, {"ok": True, "stacks": stacks})


def _resources(event: dict) -> dict:
    """List a stack's resources (drives a component's info panel). Scoped to the
    console's namespace so arbitrary stacks can't be read."""
    qs = event.get("queryStringParameters") or {}
    stack = qs.get("stack", "")
    if not stack:
        return _resp(400, {"ok": False, "error": "stack required"})
    if not _in_namespace(stack):
        return _resp(403, {"ok": False, "error": "stack not in this console's namespace"})
    resp = _cfn.describe_stack_resources(StackName=stack)
    resources = [{
        "type": r.get("ResourceType"),
        "logicalId": r.get("LogicalResourceId"),
        "physicalId": r.get("PhysicalResourceId"),
        "status": r.get("ResourceStatus"),
    } for r in resp.get("StackResources", [])]
    return _resp(200, {"ok": True, "stack": stack, "resources": resources})
