# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""get_status / get_logs — the agent's PULL-based observability tools.

POC observability model (Decision 7, verified in design/AWS_RESEARCH_FINDINGS.md §F):
the agent reads deployment status DIRECTLY from AWS read APIs when the user asks — no
push infrastructure (no AppSync, EventBridge, or Live Tail relay).

  - codebuild:BatchGetBuilds  -> build status, current phase, phases[], log location
  - logs:GetLogEvents          -> the actual log lines (paginated)

IAM (read-only) required by this Lambda:
  codebuild:BatchGetBuilds, logs:GetLogEvents (+ FilterLogEvents optional),
  cloudformation:DescribeStacks, cloudformation:DescribeStackResources

Two entry points (one Lambda, routed by event["action"]):
  - "get_status": summarize a build (status + phases + log pointer)
  - "get_logs":   page through the build's CloudWatch log stream
"""

from __future__ import annotations

import boto3

codebuild = boto3.client("codebuild")
logs = boto3.client("logs")


def handler(event: dict, _context=None) -> dict:
    action = event.get("action", "get_status")
    if action == "get_logs":
        return get_logs(event)
    return get_status(event)


def get_status(event: dict) -> dict:
    """Summarize a CodeBuild build for the agent to report in chat."""
    build_id = event.get("build_id")
    if not build_id:
        return {"ok": False, "errors": ["build_id is required."]}

    resp = codebuild.batch_get_builds(ids=[build_id])
    builds = resp.get("builds", [])
    if not builds:
        return {"ok": False, "errors": [f"No build found for id {build_id}."]}

    b = builds[0]
    phases = [
        {
            "phase": p.get("phaseType"),
            "status": p.get("phaseStatus"),
            "duration_s": p.get("durationInSeconds"),
        }
        for p in b.get("phases", [])
    ]
    logs_loc = b.get("logs", {}) or {}
    return {
        "ok": True,
        "build_id": build_id,
        "build_status": b.get("buildStatus"),     # IN_PROGRESS|SUCCEEDED|FAILED|...
        "current_phase": b.get("currentPhase"),
        "complete": bool(b.get("buildComplete")),
        "phases": phases,
        # Log pointer (valid once PROVISIONING completes — see §F caveat)
        "log_group": logs_loc.get("groupName"),
        "log_stream": logs_loc.get("streamName"),
        "log_deep_link": logs_loc.get("deepLink"),
    }


def get_logs(event: dict) -> dict:
    """Page through the build's CloudWatch log stream. Pass log_group/log_stream from
    a prior get_status call. `next_token` continues a previous page."""
    group = event.get("log_group")
    stream = event.get("log_stream")
    if not group or not stream:
        return {"ok": False, "errors": ["log_group and log_stream are required "
                                        "(get them from get_status)."]}

    kwargs = {
        "logGroupName": group,
        "logStreamName": stream,
        "limit": min(int(event.get("limit", 200)), 10000),
        "startFromHead": bool(event.get("from_head", True)),
    }
    if event.get("next_token"):
        kwargs["nextToken"] = event["next_token"]

    resp = logs.get_log_events(**kwargs)
    lines = [e.get("message", "") for e in resp.get("events", [])]
    return {
        "ok": True,
        "lines": lines,
        "count": len(lines),
        # GetLogEvents returns the SAME token at the end of the stream; the caller stops
        # paging when the token stops changing (documented behavior).
        "next_token": resp.get("nextForwardToken"),
    }
