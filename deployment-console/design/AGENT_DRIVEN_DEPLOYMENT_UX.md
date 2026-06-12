# Agent-Driven Deployment UX — Design & Feasibility

> **Status:** Concept / design note. Living document — build on it and refer back.
>
> **Idea:** A beautiful chat-driven UI where a user configures and deploys this
> quant-research AWS Batch solution through conversation with an agent, and watches
> deployment progress, logs, deployed components, and the resolved configuration in
> real time.

---

## 1. Verdict up front

**Feasible — yes.** The chat→config and observe-deployment parts are genuinely
straightforward. There's exactly **one hard part**, and it's not technical complexity —
it's the **security boundary** around "a chat UI that can spin up VPCs, GPU fleets, and
FSx in your account." Get that boundary right and the rest is well-trodden AWS plumbing.

The reframe that makes the whole thing tractable:

> **The config is the contract. The chat doesn't "deploy" anything — it produces a
> validated configuration artifact, then *triggers and observes* a managed deployment
> workflow.**

This matters because the agent never touches credentials or runs arbitrary code. The
user's natural language only ever influences a **constrained parameter set** validated
against a JSON Schema. The attack surface is the parameters, not arbitrary CDK/shell.
That's the core guardrail.

---

## 2. Why this maps unusually well to *this* repo

The project is already 100% parameter-driven (`infrastructure/config/parameters.json` +
`.env` + feature toggles + `cdk deploy`). The existing `parameters.json` **is** the
agent's target schema:

| User says (chat) | Agent sets (validated config) | Result |
|---|---|---|
| "I just need CPU batch + S3, no GPU" | `batch.deployment_type: SINGLE_NODE`, toggles off | 4 stacks instead of 6 |
| "Add a fast scratch layer" | `app_with_fsx: true` | FSxStack joins the graph |
| "Use bigger memory instances, cap at 200 vCPUs" | `single_node.instance_classes`, `maxv_cpus` | compute env reshaped |
| "Wire up CI/CD from my GitHub" | `app_with_codepipeline: true` + `.env` GitHub fields | pipeline stack added |

Critically, the CDK app already supports **context override**
(`app.node.try_get_context("parameters")` in `infrastructure/utils.py`), so the agent's
output can be injected at deploy time with `cdk deploy -c parameters='{...}'` —
**zero code changes to the CDK app needed.**

---

## 3. Reference architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  BROWSER — React + Tailwind/shadcn, split pane                    │
│  ┌──────────────┐  ┌──────────────────────────────────────────┐  │
│  │   CHAT       │  │  DEPLOYMENT CANVAS                        │  │
│  │  (agent)     │  │  ▸ Progress timeline (stack events)       │  │
│  │              │  │  ▸ Live logs tab (CDK/CodeBuild stdout)   │  │
│  │              │  │  ▸ Components tab (topology + resources)  │  │
│  │              │  │  ▸ Config tab (the parameters.json diff)  │  │
│  └──────┬───────┘  └───────────────────▲──────────────────────┘  │
└─────────┼──────────────────────────────┼─────────────────────────┘
          │ HTTPS                          │ WebSocket / AppSync Events
   ┌──────▼──────┐                  ┌──────┴───────────────┐
   │  Cognito    │                  │  Real-time fan-out   │
   │  (auth)     │                  │  (push progress+logs)│
   └─────────────┘                  └──────▲───────────────┘
          │                                 │
   ┌──────▼─────────────────────────────────┴───────────────┐
   │  AGENT BACKEND  (Bedrock Claude + tool use)              │
   │  Tools (Lambda): update_config, validate, estimate_cost, │
   │  preview_diff, start_deployment, get_status, get_logs    │
   └──────┬───────────────────────────────────────┬──────────┘
          │ writes config/session                  │ start_deployment
   ┌──────▼──────┐                          ┌───────▼────────────────┐
   │  DynamoDB   │                          │  Step Functions         │
   │ sessions /  │                          │  Validate→Synth→        │
   │ config /    │                          │  DIFF+APPROVE(chat)→     │
   │ runs        │                          │  Deploy→Poll→Notify      │
   └─────────────┘                          └───────┬────────────────┘
                                                     │ runs cdk
                                            ┌────────▼─────────┐
                                            │  CodeBuild       │
                                            │  cdk deploy      │  ← assumes a
                                            │  (scoped role)   │    SCOPED deploy role
                                            └───┬─────────┬────┘
                                       logs─────┘         └──── CloudFormation
                                  (CloudWatch Live Tail)        (stack events → EventBridge/SNS)
```

**Flow:** chat builds config → agent shows a **preview/diff + cost estimate** → user
approves in chat → Step Functions runs CodeBuild (`cdk deploy`) under a scoped role →
stack events + logs stream back to the canvas in real time → on completion, the Components
tab is populated from CloudFormation's resource list.

**Why Step Functions + CodeBuild and not "agent runs CDK directly":** the agent must
*never* hold deploy credentials or execute shell. SFN gives the human-approval gate,
retries, and milestone visibility for free; CodeBuild is the only thing with the scoped
role that can mutate the account. Clean blast-radius separation.

---

## 4. Observability — PULL, not push (POC simplification)

> **REVISED (colleague feedback + AWS doc verification).** Dropped the push stack
> (AppSync Events + Live Tail relay + EventBridge). For a POC the agent **pulls** status
> on demand — when the user asks "how's it going?", the agent calls AWS read APIs directly
> and reports back in chat. Verified the APIs return everything needed — see
> `AWS_RESEARCH_FINDINGS.md §F`.

The agent's `get_status` / `get_logs` tools (Lambdas) call:

1. **Build status & phase** — `codebuild:BatchGetBuilds` returns `buildStatus`
   (`IN_PROGRESS|SUCCEEDED|FAILED|…`), `currentPhase`, the full `phases[]` array
   (per-phase status + timing), and a `logs` **LogsLocation** (CloudWatch group/stream +
   deep link). One call = the whole progress picture.
2. **Log lines** — `logs:GetLogEvents` on the `groupName`/`streamName` from step 1
   (≤1 MB / 10k events per page, pagination tokens for "show me more"). The CloudWatch
   stream exists once the build's PROVISIONING phase completes.
3. **Deployed components** — after the build succeeds, `cloudformation:DescribeStacks` +
   `DescribeStackResources` (or the cdk `--outputs-file` the build wrote) map back to the
   logical components. The shape is also predictable from the config toggles.

**Trade-off (accepted for POC):** pull = the agent reports a *snapshot* when asked, not a
live-updating stream. No sub-second tailing, no push fan-out — but also no AppSync, no
relay, no WebSocket, no Live Tail 15-session cap, and the browser needs no AWS creds. If a
live canvas is wanted later, re-introduce the push plane (see git history of this section).

**AgentCore invoke path (verified):** `InvokeAgentRuntime` streams the agent's *responses*
to the UI as **Server-Sent Events**. (Auth removed for POC — see Decision 5 — so the simple
SDK invoke path applies; the OAuth-direct-HTTPS caveat only matters once auth returns.)

---

## 5. AWS tech stack (POC — trimmed per colleague feedback)

| Layer | Choice | Why |
|---|---|---|
| **UI** | React + TypeScript + Vite, Tailwind + shadcn/ui; streaming chat via Vercel AI SDK or `assistant-ui` | Clean chat + canvas; first-class tool-call rendering |
| **Hosting** | CloudFront + S3, or Amplify Hosting | Static SPA, cheap |
| ~~**Auth**~~ | ~~Cognito~~ → **REMOVED (POC)** | No auth for the POC (Decision 5). Single trusted operator. |
| **Agent model** | Amazon Bedrock — Claude (Sonnet 4.x default) via tool use | Tools map 1:1 to config/deploy/status actions |
| **Agent runtime** | **Bedrock AgentCore** | Managed session memory + observability |
| **Tool functions** | Lambda | One Lambda per tool: `update_config`, `validate_config`, `preview`, `start_deployment`, `get_status`, `get_logs`, `get_resources` |
| **Observability** | **Agent-pull** via `codebuild:BatchGetBuilds` + `logs:GetLogEvents` + `cloudformation:Describe*` | No push infra (Decision 7). Snapshot-on-ask. |
| **Deployment orchestration** | **AWS Step Functions** *(optional for POC)* | Approval gate + retries. Can be skipped initially: `start_deployment` Lambda → `startBuild` directly. |
| **Deployment executor** | **AWS CodeBuild**, source = **public GitHub repo** | Toolchain + logs. Source URL from config (Decision 6). |
| **Infra source** | **Public GitHub** `aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch` | No S3 artifact / versioned-zip CI. |
| **State** | DynamoDB *(optional for POC)* | Run records / transcripts. Can defer; CodeBuild build IDs are the source of truth. |
| ~~Real-time / Events / Audit~~ | ~~AppSync / EventBridge / Cognito-attributed audit~~ → **dropped for POC** | Re-add with auth before shared use. |

---

## 6. Agent design (tools + guardrails)

The agent is a **config-builder + deployment-observer**, with these tools:

- `update_config(patch)` — merges into the session config, **validates against JSON
  Schema**, returns errors for the agent to resolve conversationally
- `validate_config()` — full check (e.g., AZ id required if S3 Express on; FSx ≥1200 GiB)
- `estimate_cost()` — rough monthly/run estimate from instance classes + toggles
- `preview_deployment()` — `cdk diff` → "here's what will be created/changed"
- `start_deployment()` — **only callable after explicit user approval**; kicks Step Functions
- `get_status()` / `get_logs()` / `get_resources()` — read-only observation

**Guardrails:**
- Constrained schema = no arbitrary code; user can't inject CDK.
- Human approval gate before any mutation (SFN callback resumed from chat).
- Scoped deploy role: only the services this app provisions
  (Batch/ECS/EC2/VPC/S3/FSx/IAM-pass/CFN), nothing else.
- Cost ceiling check (agent refuses / warns above a threshold).
- One deployment per session at a time; idempotent re-deploys via CFN.

---

## 7. Feasibility breakdown — easy vs. hard

**Genuinely easy (days, not weeks):**
- NL → validated config (LLM tool use over a known schema — the LLM's sweet spot)
- Triggering CodeBuild/SFN from a Lambda tool
- Reading back the deployed component list from CloudFormation
- Cost estimation (instance types are known)

**Moderate:**
- Real-time streaming wiring (3 planes, but all standard AWS patterns)
- The split-pane "beautiful" UI with live timeline + log tail
- Human-approval handshake (SFN task token ↔ chat)

**Hard / the real risks:**
- 🔴 **Security boundary** — the deploy role is powerful. The make-or-break design
  decision, not an afterthought.
- 🔴 **Multi-tenancy** — *if* this is a product (deploy into customers' accounts) vs. an
  internal tool (one account). Multi-account vending (Control Tower / account-vending
  style) is a different, much larger project.
- 🟡 **Long-running + rollback UX** — deploys take 10–30 min; CFN rollbacks on failure
  need clear in-chat surfacing.
- 🟡 **Destroy/teardown** — the stacks use `RemovalPolicy.DESTROY` + `auto_delete_objects`;
  a chat "tear it down" button is easy to build and dangerous — gate it hard.

---

## 8. Suggested phasing (crawl → walk → run)

- **Phase 0 — Headless slice:** Lambda tool that takes a config JSON, runs CodeBuild
  `cdk deploy -c parameters=…`, returns a run id. Prove the mutation path + scoped role
  end-to-end. *No UI, no agent yet.*
- **Phase 1 — Chat → config → approve → deploy:** Add Bedrock agent + the config tools +
  the SFN workflow with the approval gate. Output: working deployment driven by chat,
  status polled.
- **Phase 2 — The beautiful canvas:** Real-time progress timeline + live log tail +
  components/topology + config diff tabs.
- **Phase 3 — Polish:** cost estimates, teardown (gated), deployment history, multi-config
  presets, diagram auto-render.

---

## 9. Decisions (LOCKED for first pass)

| # | Decision | Choice | Implication |
|---|---|---|---|
| 1 | Internal tool or product? | **Internal tool, single account** | One deploy role; no cross-account vending. Big scope reduction. |
| 2 | Agent framework | **Bedrock AgentCore** | Managed session memory + observability/traces. Tools = Lambda targets via Gateway/MCP (verified). |
| 3 | Teardown | **Out of scope (first pass)** | No `cdk destroy` path. Deploy + observe only. |
| 4 | Cost estimation/approval | **Out of scope (first pass)** | No `estimate_cost` tool. Human approval gate stays (safety, not cost). |
| 5 | **Auth (Cognito)** | **❌ REMOVED — POC, no auth** | No Cognito user pool / identity pool. No per-user channels. Single trusted operator. *(Colleague feedback)* |
| 6 | **Infra source** | **Public GitHub repo by URL** | CodeBuild source = the public repo; URL lives in the config. No S3 artifact, no versioned-zip CI. *(Colleague feedback)* |
| 7 | **Observability** | **Agent PULLS status directly** | Agent polls `codebuild:BatchGetBuilds` (status/phase/log location) + `logs:GetLogEvents` (log lines). **No AppSync Events, no Live Tail relay, no EventBridge.** *(Colleague feedback)* |

Net effect: first pass is **chat → validated config → human approve → deploy → poll
status**, single account, AgentCore-hosted agent, **no auth**, **public-GitHub source**,
**pull-based observability**, no teardown, no cost quoting.

> **POC posture note:** removing auth means anyone who can reach the agent can deploy into
> the account. Acceptable for a single-operator POC in a burner/sandbox account ONLY. Auth
> (Cognito) + the approval gate are the first things to add before any shared use.

---

## 9a. How does the code reach the build? (source strategy)

> **REVISED (colleague feedback):** infra source = the **public GitHub repo**, cloned by
> CodeBuild. The repo URL lives in the config. No S3 artifact, no versioned-zip CI.

**Code and config are decoupled.** The CDK infra code (`infrastructure/`) is *identical*
across every deployment — only the parameters change. The agent produces a parameter
**override**; it **never authors or pushes code**.

**POC source = public GitHub, by URL in the config:**

```
https://github.com/aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch
```

- CodeBuild's source is this **public** repo (`codebuild.Source.git_hub(...)`, a public
  repo needs **no GitHub token / CodeConnections** for read-only clone — verify at build
  time). The build clones it, then runs the verified inject-and-deploy flow
  (`merge_params.py` → `cdk.context.json` → `cdk deploy`).
- The **repo URL + branch live in the config**, so the operator can point at a fork or a
  branch without code changes:
  ```jsonc
  // deployment-identity (not the CDK `parameters` object)
  "source": {
    "repo_url": "https://github.com/aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch",
    "branch": "main"
  }
  ```
- Two distinct source needs, unchanged: **infra source** (above) vs. **app-image source**
  (root `Dockerfile` + `samples/order_flow/src`, built by the *existing*
  `DeploymentPipelineStack` — orthogonal to this console).

> ⚠️ **AWS CodeCommit is closed to new customers** (~July 2024) — not used here.
> Trade-off of public-GitHub-at-build-time: no version pinning beyond a branch/commit, and
> the build depends on GitHub availability. Fine for a POC; revisit (pin a commit, or S3
> artifact) if reproducibility matters later.

Each deployment run = `(repo_url@branch) + (agent-produced parameter override)`.

---

## 9b. Diagrams

- **Reference architecture (POC):** `assets/agent-deployment-architecture.drawio[.png]`
  — Frontend (no auth) · AgentCore + Tools · Execution + **pull** observability · Deployed
  Solution. GitHub→CodeBuild→CFN→Batch; dashed arrow = agent pulls status. (Updated for
  Decisions 5-7; the old auth+push version is in git history.)
- **User flow:** `assets/agent-deployment-userflow.drawio[.png]`
  — describe goal → agent builds config → validate loop → preview → approve → deploy →
  observe → outcome. *(Note: "sign in" step is moot now that auth is removed for the POC.)*

Edit the `.drawio` source; regenerate PNGs with the draw.io desktop binary:
```
/Applications/draw.io.app/Contents/MacOS/draw.io -x -f png -s 2 -t \
  -o <name>.drawio.png <name>.drawio
```

---

## 10. Companion docs & next concrete artifacts

**Companion docs (in this folder):**
- `AWS_RESEARCH_FINDINGS.md` — **verified-against-docs** answers (AgentCore, AppSync Events, Live Tail, CDK bootstrap), with citations. ✅ Read this before building — it corrects §4.
- `CONFIG_SCHEMA_PLAN.md` — the agent's config contract (field-by-field, validation rules, presets). ✅ drafted
- `PROJECT_OVERVIEW.md`, `CDK_INFRASTRUCTURE_DEEP_DIVE.md` — context on what gets deployed.

---

## 11. Security / blast-radius — the verified approach (was the #1 blocker)

Verified against CDK + IAM docs (see `AWS_RESEARCH_FINDINGS.md` §D). The deploy path
*must* create IAM roles (the CDK app builds job/exec/instance roles), which is
privilege-escalation-capable. **AWS's documented stance: do NOT hand-craft least-privilege
on the CFN execution role** (complex, hits policy-size limits, breaks rollbacks). Use the
**guardrails approach** instead:

1. **Custom bootstrap** the account: replace the default `AdministratorAccess` CFN
   execution role via `cdk bootstrap --cloudformation-execution-policies "<scoped-arns>"`.
2. **Permissions boundary on bootstrap roles**: `cdk bootstrap --custom-permissions-boundary <name>`.
3. **Permissions boundary on app roles**: add the `core.PermissionsBoundary` aspect at
   Stage/Stack scope so every role `base_construct.py` creates is bounded.
4. **Deploy-role trust + `iam:PassRole` scoping** controls *who/what* can deploy; CodeBuild
   only needs `sts:AssumeRole` on the bootstrap roles (gated by the
   `aws-cdk:bootstrap-role` tag — the only documented pattern).
5. **Detective controls** the execution role can't disable: AWS Config, CloudTrail, Control
   Tower. Optionally a `ConfirmPermissionsBroadening`-style `cdk diff` gate before deploy.

This is what makes "a chat UI that can provision VPCs/GPU fleets" safe: the agent produces
params, the human approves, CodeBuild assumes a **bounded** role, and nothing the deploy
creates can exceed the permissions boundary.

### 11a. DEFERRED for Phase 0 spike — admin role is OK *for now*

> **Decision (deliberate deferral):** For the **Phase 0 headless spike only**, the CodeBuild
> deploy role uses **`AdministratorAccess`** and the account is bootstrapped with the
> **default** (admin) CFN execution role. We are **not** scoping IAM or applying permissions
> boundaries yet.
>
> **Why:** the goal of Phase 0 is to prove the deploy path works end-to-end. Premature IAM
> scoping turns the spike into a permission-error debugging exercise. Running in a
> **single-account burner** keeps blast radius contained.
>
> **Hardening gate (do BEFORE any non-burner / shared / production use):** apply §11 items
> 1–5 — scoped `--cloudformation-execution-policies`, `--custom-permissions-boundary`, the
> `core.PermissionsBoundary` aspect on app roles, deploy-role trust + `iam:PassRole`
> scoping, and detective controls. Tracked as a checklist item below; **must not ship to a
> real account with admin.**

**Next concrete artifacts (not yet built):**

- [x] **Config Schema plan** — see `CONFIG_SCHEMA_PLAN.md`. Next: generate the actual
  `config.schema.json` (Draft 2020-12) + `validate.py` for the cross-field rules.
- [x] **Architecture diagram** — `assets/agent-deployment-architecture.drawio`.
- [x] **User flow diagram** — `assets/agent-deployment-userflow.drawio`.
- [x] **`config.schema.json` + `validate.py`** — built in `schema/`. JSON Schema +
  10 cross-field rules; canonical `parameters.json` passes (regression guard); bad configs
  rejected (verified). TODO: wrap `validate.py` as the `validate_config` Lambda.
- [x] **Phase 0 headless slice — SPIKE PASSED (live).** Artifacts in `platform-infra/` +
  `backend/`. On 2026-06-08, ran the full chain LIVE into burner `786721988357`:
  `cdk bootstrap` → override `{deployment_type: SINGLE_NODE}` → `merge_params.py` →
  `cdk.context.json` → `cdk deploy --all`. **All 4 stacks CREATE_COMPLETE**; Batch queue
  `phase0-spike-single-node-with-cpu-00` ENABLED/VALID; GPU stack correctly omitted;
  outputs captured (VPC/SG/S3/ECR). Resources then **destroyed** (cost cleanup); CDKToolkit
  bootstrap kept. The core thesis (agent override → merge → real deploy) is **proven**.
  TODO before reuse: wrap as CodeBuild-driven (not local) + the SFN approval gate (Phase 1).
- [ ] **Tool contracts** — input/output schemas for each AgentCore tool.
- [ ] **🔒 IAM hardening (gated)** — replace admin with scoped policies + permissions
  boundaries per §11. **Required before any non-burner / shared / production use.**

> Recommended order: `config.schema.json` → Phase 0 slice → tool contracts → UI. The
> schema is the contract everything else hangs off.
> **Note:** IAM scoping is intentionally deferred until after the spike works (§11a).
