# Deployment Console

An agent-driven, chat-based UI for configuring and deploying the quant-research AWS Batch
solution in this repo. A user describes their goal in natural language; an agent builds a
**validated configuration**, the user approves, and the platform deploys the existing
`infrastructure/` CDK app and streams progress, logs, components, and config back to a
live canvas.

> **Status:** Design phase. See [`design/`](./design/) for the full design, the
> AWS-doc-verified findings, and the config-schema plan. No runtime code yet.

## Why it lives here

The console operates *on* the `infrastructure/` CDK app in this repo. We chose a top-level
directory (rather than a separate repo) for convenience. It is intentionally
**self-contained** — it references the infra by **versioned artifact**, not by path — so it
can be extracted to its own repo later with minimal churn.

> ⚠️ **Open-source note.** This repo is public, MIT-0, `aws-samples`. Anything committed
> here is subject to the same open-source / PCSR review as the rest. **Keep internal-only
> values out of committed code** — account IDs, Cognito pool IDs, internal endpoints,
> burner-account specifics belong in env/config/secrets, never in source.

## Core design decisions (locked)

1. **Internal tool, single account** — one CDK deploy role; no cross-account vending.
2. **Bedrock AgentCore** (GA) hosts the agent — Lambda tools via Gateway/MCP, SSE streaming.
   (Verified — `design/AWS_RESEARCH_FINDINGS.md`.)
3. **Teardown out of scope** for the first pass.
4. **Cost estimation out of scope** for the first pass.
5. **Config is the contract** — the agent only ever produces a `parameters` override that
   passes `schema/config.schema.json`; it never authors or runs code.
6. **POC: no auth** (Cognito removed) — single trusted operator, burner/sandbox only.
7. **POC: infra source = public GitHub** (`aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch`),
   cloned by CodeBuild; repo URL lives in config. No S3 artifact.
8. **POC: pull observability** — agent reads `codebuild:BatchGetBuilds` + `logs:GetLogEvents`
   directly. No AppSync / EventBridge / Live Tail relay.

> POC posture: no auth means anyone reaching the agent can deploy. Acceptable for a
> single-operator burner POC only. Auth + approval gate come before any shared use.

## Where things live

| Path | What |
|---|---|
| `frontend/` | React + Vite SPA — chat + status panel (Phase 2) |
| `agent/` | AgentCore config, system prompt, tool schemas (Phase 1) |
| `backend/tools/` | Lambda tools: `start_deployment` ✅, `get_status`/`get_logs` ✅; (later) `update_config`, `validate_config`, `preview`, `get_resources` |
| `backend/orchestration/` | `merge_params.py` ✅ + `buildspec.yml` ✅; (optional) Step Functions approval gate |
| `platform-infra/` | CDK for the console: S3 bucket + CodeBuild (public-GitHub source) + admin role (§11a) |
| `schema/` | `config.schema.json` ✅ + `validate.py` ✅ |
| `design/` | Design docs, AWS-verified findings, diagrams |

(`backend/relay/` removed — no push observability in the POC.)

## Runtime artifacts are NOT code

Per-deployment outputs — the parameter override, build IDs, cdk outputs — are **data**
(S3 + CodeBuild build records), never committed to this repo. For the POC the CodeBuild
build ID is the source of truth for a run; a DynamoDB run table is optional/deferred.

## Next steps

See the checklist at the end of `design/AGENT_DRIVEN_DEPLOYMENT_UX.md`. Phase 0 spike is
**done (live-verified)**; next is the implementation plan for Phase 1 (AgentCore + tools).
