# Implementation Action Plan — AgentCore + Deployment Orchestration (POC)

> **STATUS: ✅ COMPLETE (2026-06-08).** All phases validated live in burner 786721988357.
> See `VALIDATION_REPORT.md` for results. Resources torn down.
>
> **Goal:** stand up and validate the agent-driven deployment infrastructure end-to-end,
> driven from the burner account, by **invoking the AgentCore endpoint directly** (no UI).
> Scope: AgentCore agent + the deploy-orchestration tools. **UI is explicitly out of scope.**
>
> **Decisions already locked:** no auth (POC), public-GitHub infra source, pull
> observability (`BatchGetBuilds`/`GetLogEvents`), admin CodeBuild role (§11a), Claude on
> Bedrock (verified working in burner: `us.anthropic.claude-sonnet-4-5`).

## Target validated flow

```
[direct invoke: boto3/curl → AgentCore InvokeAgentRuntime]
        │  "deploy single-node CPU batch, no FSx"
        ▼
  Agent (Claude on AgentCore Runtime)
        │  calls tools
        ├── validate_config(override)      → schema/validate.py result
        ├── start_deployment(override)      → S3 put + CodeBuild startBuild → build_id
        └── get_status(build_id)/get_logs   → BatchGetBuilds + GetLogEvents
        ▼
  CodeBuild clones public GitHub repo → merge_params → cdk deploy → CloudFormation
        ▼
  Real stacks in burner 786721988357   (verified live in the Phase 0 spike)
```

**Definition of done:** a single `boto3 invoke_agent_runtime` call with a natural-language
deploy request causes a real CDK deployment in the burner, and a follow-up invoke returns
the live build status/logs the agent pulled — all without touching a UI. Then tear down.

## Decisions made autonomously (rationale)

- **Agent authoring = Strands Agents SDK on AgentCore Runtime** (pending research
  confirmation). Rationale: AWS's documented first-party path for AgentCore; `@tool`
  decorator lets the agent call our logic directly. If research shows the starter toolkit
  (`agentcore configure/launch`) is materially simpler, use that. Either way the agent is
  a container on Runtime.
- **Tools = in-process functions in the agent container, NOT AgentCore Gateway.** Rationale:
  for a POC with 3 tools, Gateway (Lambda→MCP) adds a whole service to stand up. The agent
  can call `validate.py` in-process and use boto3 for `start_deployment`/`get_status`
  directly. Fewer moving parts; the tool *logic* is already written
  (`validate.py`, `start_deployment.py`, `get_status.py`) and is reused as importable
  functions. (Gateway/Lambda split is a later hardening step.)
- **No Step Functions for the POC.** `start_deployment` calls `codebuild.start_build`
  directly. Approval gate is deferred (it pairs with auth, also deferred).
- **Single-node CPU, no-FSx config** as the test deployment — fastest (≈4 min in the
  spike), lowest cost, exercises the full path.
- **Region us-east-1**, burner `786721988357`.

## Phases & tasks

### P1 — Foundations (platform infra in burner)
- T1. Deploy the Phase 0 platform stack (S3 bucket + CodeBuild project, public-GitHub
  source, admin role) into the burner. Confirm `startBuild` works headless (reuse the
  proven spike, now via the deployed CodeBuild project rather than local cdk).
- T2. Create the **AgentCore Runtime execution role** (trust `bedrock-agentcore.amazonaws.com`;
  perms: ECR pull, CloudWatch logs, `bedrock:InvokeModel` for Claude, and the tool perms:
  s3 put on the artifact bucket, `codebuild:StartBuild`+`BatchGetBuilds`, `logs:GetLogEvents`,
  `cloudformation:Describe*`). Exact perms per research.

### P2 — Agent + tools (the core)
- T3. Author the agent (`agent/`): a Strands/AgentCore app exposing 3 tools that wrap the
  ALREADY-WRITTEN logic — `validate_config` (import `schema/validate.py`),
  `start_deployment` (import tool), `get_status`/`get_logs` (import tool). System prompt =
  the config-builder + deploy-driver persona working from presets + overrides.
- T4. Containerize (ARM64 per Runtime contract), push to ECR in burner.
- T5. Create the agent runtime (`CreateAgentRuntime` / starter toolkit `launch`), get the
  `agentRuntimeArn`.

### P3 — Direct-invoke validation (NO UI)
- T6. Invoke the endpoint directly (boto3 `bedrock-agentcore` `invoke_agent_runtime`) with
  "validate this config" → confirm the agent calls `validate_config` and returns the result.
- T7. Invoke with "deploy single-node CPU batch, no FSx" → confirm the agent calls
  `start_deployment`, a real CodeBuild build starts, returns build_id.
- T8. Poll: invoke "what's the deployment status?" → agent pulls `BatchGetBuilds`/logs and
  reports. Wait for build SUCCEEDED + stacks live.
- T9. Verify the real stacks exist in the burner (CFN list, Batch queue).

### P4 — Cleanup + report
- T10. Tear down deployed stacks + the platform slice (keep CDKToolkit). Confirm no cost.
- T11. Write validation report (what worked, what the agent actually did, any doc-vs-reality
  gaps found) into `design/`. Then hand off to UI phase (separate, via subagents).

## Risk register (live)
- AgentCore packaging contract unknown until research returns → T3/T4 may adjust.
- Container build for ARM64 from this Mac (arm64 host — should be native; verify).
- Agent-in-loop tool calling reliability → keep tools dead-simple, structured I/O.
- Cost: deploys create VPC+endpoints+Batch CE; tear down promptly (T10).

## Out of scope (this goal)
UI/frontend, auth, Step Functions approval gate, cost estimation, teardown tool, IAM
scoping (§11a admin deferral stands). These come after direct-invoke validation passes.
