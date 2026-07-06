# Deploying the Deployment Console

> **One command stands up (or updates) the entire console.** It is **idempotent** —
> safe to re-run; it create-or-updates every piece.

```bash
cd deployment-console
AWS_PROFILE=<your-profile> ./deploy-console.sh
```

That's it. On success it prints the **Console URL**, agent ARN, and every resource id.
A clean run takes **~7 minutes** end to end (CloudFront distribution creation is the long pole).

## What it provisions (in order)

| Step | Piece | Mechanism |
|---|---|---|
| 1 | CDK bootstrap (CDKToolkit) — skipped if present | `cdk bootstrap` |
| 2 | Platform slice: CodeBuild project + artifact S3 bucket | CDK (`platform-infra/phase0_stack.py`) |
| 3 | AgentCore execution role (+ inline policy) | AWS CLI, create-if-absent |
| 4 | Agent runtime: build arm64 zip → upload → create/update → wait READY | `agent/build_package.sh` + `agent/deploy_agent.py` |
| 5 | Hosting: S3 (SPA) + CloudFront (OAC) + Lambda bridge | CDK (`hosting/hosting_stack.py`) |
| 6 | SPA: `vite build` (live mode) → `s3 sync` → CloudFront invalidate | AWS CLI |

Each step's outputs are chained automatically (bucket → role → agent ARN → hosting →
CloudFront URL → SPA sync). No manual copy-paste between steps.

## Prerequisites

| Tool | Why | Verified with |
|---|---|---|
| `aws` CLI, authenticated | every step | `aws sts get-caller-identity` |
| `node` / `npm` | CDK via `npx`, and the SPA build | `node --version` (tested on v24) |
| `python3` | CDK app entrypoints + the agent deploy script | `python3 --version` (tested on 3.14) |
| `uv` | builds the agent's arm64 wheels | `uv --version` |

**No Docker** — the agent uses AgentCore code-zip deployment, and the CodeBuild-based
quant-research deploy runs in the cloud.

The target account must allow **Bedrock AgentCore** and have model access for the
configured `BEDROCK_MODEL_ID` in the region.

## Configuration (env overrides — all optional except `AWS_PROFILE`)

| Var | Default | Purpose |
|---|---|---|
| `AWS_PROFILE` | — (required) | AWS credentials profile |
| `AWS_REGION` | `us-east-1` | target region (AgentCore + deploy target) |
| `ACCOUNT_ID` | from STS | target account |
| `NAMESPACE` | `agentpoc` | prefix for all console resources |
| `BEDROCK_MODEL_ID` | `us.anthropic.claude-sonnet-4-5-20250929-v1:0` | agent model (geo profile) |
| `SOURCE_OWNER`/`SOURCE_REPO`/`SOURCE_BRANCH` | aws-samples / this repo / main | the **quant infra** repo CodeBuild clones |

## Idempotency / re-runs
- CDK stacks update in place.
- The exec role is create-if-absent, policy always re-applied.
- The agent runtime is **create-or-update** (`deploy_agent.py` lists by name; updates if it
  exists). So re-running ships new agent code without duplicating runtimes.
- The SPA is always rebuilt + synced + invalidated.

Because it's idempotent, if a run fails partway you can fix the cause and simply
**re-run the same command** — completed steps are detected and skipped or updated in place.

## Verifying a deploy

After the script prints the summary, confirm it's actually live (not just that the
script exited 0):

```bash
URL=https://<your-distribution>.cloudfront.net

curl -s "$URL/"                 # SPA — expect HTTP 200
curl -s "$URL/api/health"       # bridge — expect {"ok": true, "agent": true}
curl -s "$URL/api/builds?limit=3"   # bridge → CodeBuild — expect {"ok": true, "builds": [...]}
curl -s "$URL/api/stacks"           # bridge → CloudFormation — expect {"ok": true, "stacks": [...]}

# agent runtime should be READY
aws bedrock-agentcore-control get-agent-runtime \
  --agent-runtime-id <printed-runtime-id> --region us-east-1 \
  --query status --output text
```

CloudFront invalidation takes ~1–2 min to fully propagate, so the very first page load
may lag briefly after the script finishes.

## Design notes (why a few pieces aren't pure CDK)
- **Agent runtime** isn't CDK because AgentCore Runtime had no stable L2 construct at build
  time; `deploy_agent.py` uses the `bedrock-agentcore-control` boto3 API (create/update).
- **Exec role** is CLI, not CDK. Some sandbox/guardrailed accounts **deny `iam:DeleteRole`**
  (a same-named role may already exist out-of-band), so CDK could neither recreate nor
  cleanly import it. The role's trust + permissions are still version-controlled (inline in
  `deploy-console.sh`).
- **`cdk bootstrap`** is called with only the explicit `aws://<account>/<region>` target and
  **no `--app`** — bootstrap just provisions the `CDKToolkit` stack for the environment; it
  does not need to synthesize the infra app. (See "Troubleshooting" for why this matters.)
- Everything else (CodeBuild, S3, CloudFront, Lambda bridge) **is** proper CDK.

## Troubleshooting (issues seen during a real first run)

These were hit and fixed while deploying into a fresh account; the script now handles
both, but they're documented here in case a variant resurfaces.

### 1. `FileNotFoundError: config/parameters.json` during bootstrap
**Symptom:** step 1/6 aborts with a traceback from `infrastructure/app.py` →
`load_parameters` trying to `open("config/parameters.json")`.
**Cause:** the infra CDK app reads its parameters file **relative to the current working
directory**. An earlier version of this script passed the infra app as `cdk bootstrap
--app "…/app.py"`, which ran the app from `deployment-console/` where that relative path
doesn't resolve.
**Fix (already applied):** bootstrap no longer passes `--app` at all — it only needs to
create the `CDKToolkit` stack for the target env.

### 2. `ENOENT: cdk.out/manifest.json` during bootstrap
**Symptom:** step 1/6 aborts right after the parameters error is resolved.
**Cause:** an intermediate fix `cd`-ed into the infra dir for the `--app` command, so synth
output landed in `infrastructure/cdk.out` while `cdk` (running from `deployment-console/`)
looked for `./cdk.out`.
**Fix (already applied):** same as above — dropping `--app` from bootstrap removes the need
to synth the infra app during bootstrap entirely.

### 3. CodeBuild deploy fails on `s3-storage-stack` — `NAME_CONFLICT_VALIDATION`
**Symptom:** a deployment triggered *from the console* runs in CodeBuild, deploys the
network + pipeline stacks, then fails on `s3-storage-stack-<namespace>`:
```
The following hook(s)/validation failed: [AWS::EarlyValidation::ResourceExistenceCheck]
```
The `s3-storage-stack` sits in `REVIEW_IN_PROGRESS` and the build is marked FAILED. The
build role can't show the detail (`AccessDenied` on `cloudformation:DescribeEvents`), and
the raw error misleadingly mentions "bootstrap version 30". The **real** reason is visible
with admin creds:
```bash
CS=$(aws cloudformation list-change-sets --stack-name s3-storage-stack-<namespace> \
  --region <region> --query 'Summaries[0].ChangeSetId' --output text)
aws cloudformation describe-events --change-set-name "$CS" --region <region>
# → NAME_CONFLICT_VALIDATION: Resource of type 'AWS::S3::Bucket' with identifier
#   '<namespace>-standard-bucket-<region>' already exists.
```
**Cause:** the quant infra's standard S3 bucket name was derived from only
`namespace + region`, but **S3 bucket names are globally unique across all AWS accounts**.
If that name was ever used in another account (e.g. a previous deploy), the new deploy
collides — CloudFormation's `NAME_CONFLICT` early-validation now rejects the changeset
before anything is created.
**Fix (applied in `infrastructure/common/s3.py`):** the standard bucket name is now scoped
by **account and region** (`<namespace>-standard-bucket-<account>-<region>`), which is
globally unique per account. **Note:** the console's CodeBuild deploy clones the infra from
the public `aws-samples` repo (`main`), so this fix only takes effect once it's on the
branch CodeBuild clones — set `SOURCE_OWNER`/`SOURCE_REPO`/`SOURCE_BRANCH` to point at a
fork/branch that has it.
**Workaround without the code fix:** deploy with a fresh, unused `NAMESPACE`
(`NAMESPACE=<unique> ./deploy-console.sh`) so every resource name — including the bucket —
is unique. Clean up any `REVIEW_IN_PROGRESS` `s3-storage-stack` first:
`aws cloudformation delete-stack --stack-name s3-storage-stack-<namespace> --region <region>`
(a `REVIEW_IN_PROGRESS` stack has no provisioned resources — safe to delete).

### 4. CDK telemetry NOTICE in the logs
Harmless. Recent CDK CLI prints a telemetry notice; it does not affect the deploy. Silence
it with `npx cdk acknowledge 34892` if desired.

### 5. Vite "chunks larger than 500 kB" warning
Harmless build-time warning from the SPA bundle. The build still succeeds and ships.

## Teardown (manual; not part of this script)
```bash
# CDK stacks
cd deployment-console/hosting && npx cdk destroy --app ".venv/bin/python hosting_stack.py" --force
cd ../platform-infra && npx cdk destroy --app ".venv/bin/python phase0_stack.py" --force
# agent runtime (CLI) — get the id from the deploy summary or list-agent-runtimes
aws bedrock-agentcore-control delete-agent-runtime --agent-runtime-id <id> --region us-east-1
# (exec-role deletion may be blocked by a sandbox guardrail — leave it; it's harmless)
```
> Tearing down does NOT remove the **quant-research stacks** the console deployed
> (`*-stack-agentpoc` network/s3/pipeline/batch) — destroy those separately if desired
> (see `backend/README.md`).

## POC posture
No auth on the app/bridge (`AuthType NONE`); the CodeBuild deploy role is
`AdministratorAccess`. Fine for a single-operator sandbox/burner account —
**harden (add auth + an approval gate, scope the deploy role) before any shared or
production use.**
