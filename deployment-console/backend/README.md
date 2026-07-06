# Backend — Deployment Orchestration

The headless deployment slice: how an agent-produced parameter override drives a real
`cdk deploy` of the `infrastructure/` app into the target account, with the agent tools
that start and observe a deployment.

## Design choices

- **No auth** — single trusted operator, sandbox only (see the POC posture in the top-level
  `README.md`).
- **Source = public GitHub** `aws-samples/quant-research-sample-using-amazon-ecs-and-aws-batch`,
  cloned by CodeBuild (no token needed for a public read). URL/branch overridable via
  `SOURCE_OWNER`/`SOURCE_REPO`/`SOURCE_BRANCH`.
- **Pull observability** — the agent reads `codebuild:BatchGetBuilds` + `logs:GetLogEvents`
  via `tools/get_status.py`. No AppSync/EventBridge/Live Tail relay.

## What's here

| File | Role |
|---|---|
| `orchestration/merge_params.py` | Deep-merges the agent's **partial override** onto canonical `parameters.json` → writes `infrastructure/cdk.context.json`. The injection path. |
| `orchestration/buildspec.yml` | What CodeBuild runs: clone repo → install CDK → fetch override from S3 → merge → `cdk deploy --all`. |
| `tools/start_deployment.py` | Lambda: validate (defensive) → put override to S3 → `startBuild`. Returns a build/run id. |
| `tools/get_status.py` | Lambda: `get_status` (BatchGetBuilds → status/phases/log pointer) + `get_logs` (GetLogEvents paging). Pull observability. |
| `../platform-infra/phase0_stack.py` | CDK for the platform: S3 bucket + CodeBuild project (public-GitHub source) + **admin** build role. |

## Two findings that shaped the injection path

Both were verified against the real `infrastructure/app.py` — neither is an assumption.

1. **Injection must be a context FILE, not `-c key=value`.**
   `cdk -c parameters='{json}'` delivers a **string** → `utils.dict_to_obj` breaks. Writing
   `cdk.context.json` = `{"parameters": {...}}` delivers a **dict** → works, zero app changes.

2. **The injected object must be COMPLETE.** `app.py` reads
   `params.batch.single_node.container_command` unconditionally. So the agent supplies a
   **partial override** that is **deep-merged onto the canonical defaults** — never a
   from-scratch object. Bonus: the locked `container_command` default is always preserved.

   Example: a `SINGLE_NODE` override synthesizes the network + s3 + pipeline + single-node
   CPU stacks, and **correctly omits** the multi-node GPU stack.

## Run it directly (without the UI)

```bash
# 0. one-time: bootstrap the account
cd infrastructure
AWS_PROFILE=<your-profile> npx cdk bootstrap aws://<account-id>/<region>

# 1. deploy the platform slice (CodeBuild project + bucket + build role)
cd ../deployment-console/platform-infra
python3 -m venv .venv && .venv/bin/pip install aws-cdk-lib constructs
CDK_DEFAULT_ACCOUNT=<account-id> CDK_DEFAULT_REGION=<region> \
  AWS_PROFILE=<your-profile> \
  SOURCE_OWNER=<gh-owner> SOURCE_REPO=<gh-repo> \
  npx cdk deploy --app ".venv/bin/python phase0_stack.py" --require-approval never

# 2. trigger a deployment (locally simulate the Lambda, or invoke it)
#    override = the agent's partial config
echo '{"batch": {"deployment_type": "SINGLE_NODE"}, "app_with_fsx": false}' > /tmp/override.json
#    upload + startBuild (or call start_deployment.py handler with the event)

# 3. watch the build logs (CloudWatch Logs group /<namespace>/deploy-console/codebuild)
```

Expected result: `start_deployment` → CodeBuild runs → `cdk deploy --all` succeeds; the
deployed stacks match the injected `deployment_type` (e.g. `SINGLE_NODE` → no GPU stack);
and cdk outputs land back in S3 (`runs/<run_id>/outputs-*.json`).

## IAM is intentionally wide open here

The build role is `AdministratorAccess` and the bootstrap uses the default admin execution
role. **Deliberate for a single-operator sandbox.** Harden — scoped
`--cloudformation-execution-policies`, `--custom-permissions-boundary`,
`core.PermissionsBoundary` on app roles — **before any shared / production use.**
