# Hosting + Live Integration Plan

> **Goal:** a functional UI deployed on AWS, wired to the live AgentCore agent, tested
> end-to-end by deploying the GitHub repo's infrastructure into the burner — **left
> running** so it's accessible. Plus: the agent can **explain the project** (what it is,
> which config points exist, what stacks deploy) to help users make rational choices.

## Target architecture (POC — no auth, single account 786721988357 / us-east-1)
```
Browser ──HTTPS──> CloudFront ──OAC──> S3 (static SPA: the Vite build)
   │
   └─ /api/* ──> Lambda Function URL (the "bridge", AuthType NONE for POC)
                   ├─ POST /message  → bedrock-agentcore invoke_agent_runtime → {result}
                   └─ GET  /status   → codebuild batch_get_builds
                              │
                  AgentCore Runtime (Strands + Claude) ── tools ──> S3 + CodeBuild
                              │                                          │
                              └ start_deployment ──────────────────────┘
                                         CodeBuild clones PUBLIC GitHub repo → cdk deploy
                                         → CloudFormation → network/s3/pipeline/batch stacks
```
Bridge holds creds via its Lambda role — **never in the browser**. Buffered request/response
(our agent returns a single `{"result": ...}` JSON, so no SSE needed end-to-end; the UI can
simulate token cadence client-side).

## Phases
- **A. Recreate platform slice** — redeploy `platform-infra/phase0_stack.py` (S3 artifact
  bucket + CodeBuild project, public-GitHub source, admin role). Confirm a headless build.
- **B. Enhance + redeploy the agent** — enrich the system prompt so the agent can converse
  about the project (overview, config points, stacks-per-config), keeping the 3 action
  tools. Rebuild arm64 zip → upload → `create_agent_runtime` → capture ARN.
- **C. Hosting infra (CDK)** — `hosting/` stack: S3 (SPA) + CloudFront (OAC) + Lambda bridge
  (Function URL, CORS) + bridge IAM role (invoke agent runtime, codebuild read). Outputs the
  CloudFront URL + Function URL.
- **D. Wire + ship the UI** — point `LiveClient` at `/api/*` (same-origin via CloudFront
  behavior, or the Function URL), build the SPA, sync to S3, invalidate CloudFront. Default
  the deployed app to **Live** mode.
- **E. End-to-end test (left running)** — open the CloudFront URL in a headless browser,
  chat → confirm → real deploy of the GitHub repo → verify CFN stacks live. Capture the URL
  + screenshots. **Do not tear down.**

## Agent "explain the project" capability (B)
Embed concise project knowledge in the system prompt (sourced from PROJECT_OVERVIEW.md +
CDK_INFRASTRUCTURE_DEEP_DIVE.md + CONFIG_SCHEMA_PLAN.md):
- **What it is:** quant-research AWS Batch sample (order-flow ML), deployed via CDK.
- **Config points:** `batch.deployment_type` (SINGLE_NODE/MULTI_NODE/ALL), `app_with_fsx`,
  `app_with_s3express`, `app_with_codepipeline`, plus sizing (maxv_cpus, instance_classes…).
- **Stacks per config:** always network + s3 + pipeline(ECR); + single-node-CPU batch
  (SINGLE_NODE/ALL); + multi-node-GPU batch (MULTI_NODE/ALL); + FSx (if app_with_fsx).
- Agent should answer these conversationally and guide the user, without deploying.

## Decisions (autonomous, POC-appropriate)
- **Lambda Function URL** over API Gateway for the bridge (one fewer resource; AuthType
  NONE matches the no-auth POC). CORS locked to the CloudFront origin where possible.
- **CloudFront + S3 OAC** (current best practice; not legacy OAI).
- **Leave everything running** per the goal; record all URLs/ARNs in a STATUS doc.
- Region us-east-1 (AgentCore + the deploy target).
