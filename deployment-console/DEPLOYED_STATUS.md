# Deployed Status — Live Access

> **Everything below is LIVE in burner account `786721988357` / `us-east-1` and left
> running** (per the goal). Verified end-to-end on 2026-06-09.

> **Real builds + prose fix (2026-06-09):** (1) the agent no longer ends a validated proposal
> with a yes/no question — it points to the inline options form / Deploy button (system prompt
> updated; agent code updated in place on the `_v2` runtime — UpdateAgentRuntime on an existing
> runtime IS allowed even though Create/Update-by-id were intermittently SCP-blocked).
> (2) "check status" / "recent builds" now reflect **real CodeBuild builds** (incl. prior
> sessions), not just the browser session: new bridge route `GET /api/builds`
> (`ListBuildsForProject` + `BatchGetBuilds`, IAM added via CDK), a `listBuilds()` client method,
> and `RecentBuilds` merges real builds with session history. Loading a real build sets up the
> canvas and (if still running) polls live. The hosting stack was `cdk deploy`-ed with the v2
> ARN, which also folded the manual `invoke-agent-v2` inline policy back into CDK (since removed).

> **Live-mode UI parity (2026-06-09):** a deploy proposal in **live** mode now renders the
> same inline interactivity as mock — an architecture diagram + an editable **Deployment
> options** form (dropdowns/toggles) + **Deploy/Cancel** buttons — instead of prose-only.
> The live client derives the proposal's config client-side (shared `lib/intent.ts`), and the
> inline **Deploy** sends the exact edited config to the agent with an explicit
> `start_deployment` instruction, so the form is authoritative over what deploys.

> **Agent fix (2026-06-09):** the agent gained two read tools —
> `list_deployed_stacks` and `describe_stack_resources` (CloudFormation, namespace-scoped to
> `agentpoc`) — so "what's deployed?" / "check status" now report the **real** stacks instead
> of claiming no tool and inventing `quant-research-*` names. System prompt corrected to the
> real `*-stack-agentpoc` names. Shipped as a new runtime (`agentpoc_deploy_console_v2-…`)
> because this account's SCP denies `bedrock-agentcore:UpdateAgentRuntime`; the bridge Lambda
> was repointed to the new ARN (supplemental inline policy `invoke-agent-v2` grants invoke on
> it). `deploy_agent.py` now auto-falls-back to create-with-versioned-name when Update is
> denied. The old runtime was deleted.

> **UI update (2026-06-09):** the SPA was rebuilt (live mode) and re-shipped to
> `agentpoc-deploy-console-spa-786721988357` + CloudFront invalidated (dist `E114VJJFBOVF2G`).
> New frontend features now live: **side-by-side Deploy view** (Timeline + Architecture
> shown together), **rich inline chat blocks** (architecture diagrams, an editable
> deployment-options form with dropdowns/toggles, confirm/cancel buttons), a
> **"what's deployed" view** that pulls CloudFormation and renders a click-to-inspect
> diagram with per-service resource tables, and **AWS-console deep-links** throughout
> (build id → CodeBuild, each service glyph → its console, stack rows → CloudFormation).
> Backend (agent runtime, bridge, CodeBuild, CDK stacks) unchanged.

## 🔗 Access the app

**Open the deployment console:**
```
https://d33az5v1lw2i0n.cloudfront.net
```
It loads in **Live** mode — the chat talks to the real AgentCore agent, which can both
**explain the project** (what it is, configurable points, which stacks deploy) and
**actually deploy** the GitHub repo's infrastructure into this account.

## What's deployed (and how to reach it)

### The hosting + agent platform (the console itself)
| Resource | Identifier |
|---|---|
| CloudFront (the app URL) | `https://d33az5v1lw2i0n.cloudfront.net` · dist `E114VJJFBOVF2G` |
| SPA bucket | `agentpoc-deploy-console-spa-786721988357` |
| Bridge Lambda (CF `/api/*` → AgentCore/CodeBuild) | `agentpoc-deploy-console-bridge` (Function URL, IAM auth + CloudFront OAC) |
| Agent runtime (Bedrock AgentCore) | `arn:aws:bedrock-agentcore:us-east-1:786721988357:runtime/agentpoc_deploy_console_v2-bkY0OyFZSC` |
| Agent exec role | `agentpoc-agentcore-exec-role` |
| Deploy runner (CodeBuild) | `agentpoc-deploy-console-cdk-deploy` |
| Artifact bucket | `agentpoc-deploy-console-artifacts-786721988357` |
| CFN stacks | `deploy-console-hosting-agentpoc`, `deploy-console-phase0-agentpoc` |

### The quant-research infrastructure (deployed BY the agent through the UI, E2E test)
| Resource | Identifier |
|---|---|
| Network stack | `network-stack-agentpoc` (VPC + interface endpoints) |
| S3 storage stack | `s3-storage-stack-agentpoc` |
| Deployment pipeline stack | `deployment-pipeline-stack-agentpoc` (ECR repo) |
| Single-node CPU Batch stack | `batch-job-single-node-with-cpu-stack-agentpoc` |
| **Batch job queue (live)** | `agentpoc-single-node-with-cpu-00` — **ENABLED / VALID** |

> This 4-stack set is the SINGLE_NODE (CPU, no FSx) configuration. Deploy more from the UI:
> "Add GPU training" (MULTI_NODE) or "Full platform + FSx" adds the GPU Batch and FSx stacks.

## How it works (data path)
```
Browser → CloudFront ┬ default      → S3 (SPA)
                     └ /api/*        → Lambda bridge (OAC SigV4)
                                        ├ POST /api/message → AgentCore InvokeAgentRuntime
                                        │     → Strands + Claude (us.anthropic.claude-sonnet-4-5)
                                        │       → tools: validate_config / start_deployment / get_deployment_status
                                        │         → start_deployment → CodeBuild StartBuild
                                        │             → clone PUBLIC GitHub repo → cdk deploy → CloudFormation
                                        ├ GET  /api/status  → CodeBuild BatchGetBuilds (UI polls → animates Timeline)
                                        ├ GET  /api/stacks   → CFN ListStacks (Components tab: CloudFormation node → all stacks)
                                        └ GET  /api/resources?stack= → CFN DescribeStackResources (Components: node info icon → real resources)
```
The `/api/stacks` and `/api/resources` reads are **namespace-scoped** in the bridge (only
`agentpoc` quant stacks; the console's own infra is excluded). Verified live: 4 stacks,
25 real resources on the network stack, and a 403 on out-of-namespace stacks.

## E2E test result (2026-06-09)
Driven entirely through the deployed CloudFront URL (no local tooling):
1. Asked "what is this project / what stacks for CPU-only?" → agent gave accurate project
   overview + the 4-stack breakdown + cost guidance. ✅ (`design/screenshots/live-02-explained.png`)
2. "Deploy single-node CPU, no FSx — confirm" → agent started real CodeBuild build,
   UI live-polled the Timeline through the phases. ✅ (`live-03-deploying.png`)
3. Build **SUCCEEDED**; 4 CFN stacks `CREATE_COMPLETE`; Batch queue ENABLED/VALID. ✅

### Live canvas updates — separately verified in a single browser session
A second live deploy was watched start→finish in one session to confirm the canvas
animates against **real** CodeBuild status (not the mock script):
- **Timeline** updates live: phases green-check as CodeBuild advances, elapsed timer runs,
  status badge flips in-progress → succeeded. ✅ (`live-04-timeline-live.png`)
- **Components** topology lights up live: GitHub→CodeBuild→CloudFormation→VPC/S3/ECR/Batch
  go grey→green with drawn edges, ending "All stacks live". ✅ (`live-05-components-live.png`)

> **Correction (supersedes an earlier claim):** the previously-saved `live-04-status.png`
> was a *fresh page reload* that only asked about a prior build in chat, so its canvas
> read "No deployment yet" — it did NOT demonstrate live canvas updates. It has been
> removed and replaced by the two verified screenshots above.

## Verify it yourself
```bash
# the app
open https://d33az5v1lw2i0n.cloudfront.net
# bridge health (through CloudFront)
curl -s https://d33az5v1lw2i0n.cloudfront.net/api/health    # {"ok":true,"agent":true}
# the deployed quant-research stacks
AWS_PROFILE=dialseny-burner-1 aws cloudformation list-stacks --region us-east-1 \
  --stack-status-filter CREATE_COMPLETE \
  --query "StackSummaries[?contains(StackName,'agentpoc')].StackName" --output text
# the live Batch queue
AWS_PROFILE=dialseny-burner-1 aws batch describe-job-queues --region us-east-1 \
  --query "jobQueues[?contains(jobQueueName,'agentpoc')].jobQueueName" --output text
```

## Notes
- **POC posture (unchanged):** no auth on the app/bridge; CodeBuild deploy role is admin
  (§11a). Fine for this single-operator burner; harden before any shared/prod use.
- **Cost:** the platform (CloudFront/Lambda/AgentCore/CodeBuild) is near-idle cost; the
  deployed Batch stack's compute env scales from 0 (no idle EC2) but the VPC interface
  endpoints have an hourly charge. Tear down the `*-agentpoc` quant stacks if cost matters;
  the console platform can stay.
- To redeploy/teardown the quant stacks: use the UI, or
  `infrastructure/` + `cdk.context.json` with `NAMESPACE=agentpoc` (see backend/README.md).
