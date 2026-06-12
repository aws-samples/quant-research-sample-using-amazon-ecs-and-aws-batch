# Deploying the Deployment Console

> **One command stands up (or updates) the entire console.** It is **idempotent** —
> safe to re-run; it create-or-updates every piece.

```bash
cd deployment-console
AWS_PROFILE=dialseny-burner-1 ./deploy-console.sh
```

That's it. On success it prints the **Console URL**, agent ARN, and every resource id.

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

## Configuration (env overrides — all optional except `AWS_PROFILE`)

| Var | Default | Purpose |
|---|---|---|
| `AWS_PROFILE` | — (required) | AWS credentials profile |
| `AWS_REGION` | `us-east-1` | target region (AgentCore + deploy target) |
| `ACCOUNT_ID` | from STS | target account |
| `NAMESPACE` | `agentpoc` | prefix for all console resources |
| `BEDROCK_MODEL_ID` | `us.anthropic.claude-sonnet-4-5-20250929-v1:0` | agent model (geo profile) |
| `SOURCE_OWNER`/`SOURCE_REPO`/`SOURCE_BRANCH` | aws-samples / this repo / main | the **quant infra** repo CodeBuild clones |

## Prerequisites
- `aws` CLI authenticated (`AWS_PROFILE`), `node`/`npm` (CDK via `npx`, + the SPA build),
  `python3`, and `uv` (for the agent's arm64 wheel build). **No Docker** — the agent uses
  AgentCore code-zip deployment.

## Idempotency / re-runs
- CDK stacks update in place.
- The exec role is create-if-absent, policy always re-applied.
- The agent runtime is **create-or-update** (`deploy_agent.py` lists by name; updates if it
  exists). So re-running ships new agent code without duplicating runtimes.
- The SPA is always rebuilt + synced + invalidated.

## Design notes (why a few pieces aren't pure CDK)
- **Agent runtime** isn't CDK because AgentCore Runtime had no stable L2 construct at build
  time; `deploy_agent.py` uses the `bedrock-agentcore-control` boto3 API (create/update).
- **Exec role** is CLI, not CDK, because the burner account guardrail **denies
  `iam:DeleteRole`** — a same-named role already exists out-of-band, so CDK could neither
  recreate nor cleanly import it. The role's trust + permissions are still
  version-controlled (inline in `deploy-console.sh`).
- Everything else (CodeBuild, S3, CloudFront, Lambda bridge) **is** proper CDK.

## After deploying
- Open the printed **Console URL** (CloudFront). It defaults to **Live** mode.
- CloudFront invalidation takes ~1-2 min to fully propagate.
- Live resource/URL inventory is in `DEPLOYED_STATUS.md`.

## Teardown (manual; not part of this script)
```bash
# CDK stacks
cd deployment-console/hosting && npx cdk destroy --app ".venv/bin/python hosting_stack.py" --force
cd ../platform-infra && npx cdk destroy --app ".venv/bin/python phase0_stack.py" --force
# agent runtime + exec role (CLI)
aws bedrock-agentcore-control delete-agent-runtime --agent-runtime-id <id> --region us-east-1
# (role deletion may be blocked by the burner guardrail — leave it; it's harmless)
```
> Tearing down does NOT remove the **quant-research stacks** the console deployed
> (`*-agentpoc` network/s3/pipeline/batch) — destroy those separately if desired
> (see `backend/README.md`).

## POC posture (unchanged)
No auth on the app/bridge; the CodeBuild deploy role is `AdministratorAccess` (design §11a).
Fine for a single-operator burner; harden before any shared/production use.
