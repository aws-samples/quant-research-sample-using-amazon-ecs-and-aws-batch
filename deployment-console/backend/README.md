# Phase 0 — Headless Deployment Slice (the de-risking spike)

**Goal:** prove that an agent-produced parameter override can drive a real `cdk deploy` of
the `infrastructure/` app into a target account, end-to-end, with no UI and no agent yet.

> Status: artifacts built; mechanism **verified LIVE** in burner `786721988357` (all 4
> stacks CREATE_COMPLETE, then destroyed). See design checklist.

## POC simplifications (colleague feedback — design Decisions 5-7)

- **No auth** (Cognito removed) — single trusted operator, burner/sandbox only.
- **Source = public GitHub** `aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch`,
  cloned by CodeBuild (no token for public read). URL/branch overridable.
- **Pull observability** — agent reads `codebuild:BatchGetBuilds` + `logs:GetLogEvents`
  via `tools/get_status.py`. No AppSync/EventBridge/Live Tail.

## What's here

| File | Role |
|---|---|
| `orchestration/merge_params.py` | Deep-merges the agent's **partial override** onto canonical `parameters.json` → writes `infrastructure/cdk.context.json`. **The verified injection path.** |
| `orchestration/buildspec.yml` | What CodeBuild runs: clone repo → install CDK → fetch override from S3 → merge → `cdk deploy --all`. |
| `tools/start_deployment.py` | Lambda: validate (defensive) → put override to S3 → `startBuild`. Returns a build/run id. |
| `tools/get_status.py` | Lambda: `get_status` (BatchGetBuilds → status/phases/log pointer) + `get_logs` (GetLogEvents paging). **Pull observability.** |
| `../platform-infra/phase0_stack.py` | CDK for the slice: S3 bucket + CodeBuild project (public-GitHub source) + **admin** build role (§11a). |

## The two empirically-verified findings that shaped this

Both tested with CDK CLI 2.1024.0 against the real `infrastructure/app.py` — see
`design/AWS_RESEARCH_FINDINGS.md §E`. **Neither was an assumption.**

1. **Injection must be a context FILE, not `-c key=value`.**
   `cdk -c parameters='{json}'` delivers a **string** → `utils.dict_to_obj` breaks. Writing
   `cdk.context.json` = `{"parameters": {...}}` delivers a **dict** → works, zero app changes.

2. **The injected object must be COMPLETE.** `app.py:129` reads
   `params.batch.single_node.container_command` unconditionally. So the agent supplies a
   **partial override** that we **deep-merge onto the canonical defaults** — never a
   from-scratch object. Bonus: the locked `container_command` default is always preserved.

   Verified: `SINGLE_NODE` override → synth produced network + s3 + pipeline + single-node
   CPU stacks, and **correctly omitted** the multi-node GPU stack.

## Run the spike (against the burner account)

```bash
# 0. one-time: bootstrap the burner (default admin exec role — fine for Phase 0, §11a)
cd infrastructure
AWS_PROFILE=dialseny-burner-1 npx cdk bootstrap aws://786721988357/us-east-1

# 1. deploy the Phase 0 platform slice (CodeBuild project + bucket + admin role)
cd ../deployment-console/platform-infra
python3 -m venv .venv && .venv/bin/pip install aws-cdk-lib constructs
CDK_DEFAULT_ACCOUNT=786721988357 CDK_DEFAULT_REGION=us-east-1 \
  AWS_PROFILE=dialseny-burner-1 \
  SOURCE_OWNER=<gh-owner> SOURCE_REPO=<gh-repo> \
  npx cdk deploy --app ".venv/bin/python phase0_stack.py" --require-approval never

# 2. trigger a deployment (locally simulate the Lambda, or invoke it)
#    override = the agent's partial config
echo '{"batch": {"deployment_type": "SINGLE_NODE"}, "app_with_fsx": false}' > /tmp/override.json
#    upload + startBuild (or call start_deployment.py handler with the event)

# 3. watch the build logs (CloudWatch Logs group /<namespace>/deploy-console/codebuild)
```

## Acceptance criteria (what "spike passed" means)

- [ ] `cdk bootstrap` succeeds on the burner.
- [ ] Phase 0 platform stack deploys (bucket + project + role exist).
- [ ] `start_deployment` → CodeBuild runs → `cdk deploy --all` **succeeds**.
- [ ] The deployed stacks match the injected `deployment_type` (e.g. SINGLE_NODE → no GPU stack).
- [ ] cdk outputs land back in S3 (`runs/<run_id>/outputs-*.json`).

Once green, we have a trustworthy base to write the real `IMPLEMENTATION_PLAN.md` and wire
Phase 1 (SFN approval gate + AgentCore + validate_config Lambda).

## Reminder: IAM is intentionally wide open here

The build role is `AdministratorAccess` and the bootstrap uses the default admin exec role.
**Deliberate (§11a).** Harden — scoped `--cloudformation-execution-policies`,
`--custom-permissions-boundary`, `core.PermissionsBoundary` on app roles — **before any
non-burner / shared / production use.**
