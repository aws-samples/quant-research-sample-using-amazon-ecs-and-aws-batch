#!/usr/bin/env bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# ============================================================================
# deploy-console.sh — SINGLE ENTRY POINT to deploy the Deployment Console.
# ============================================================================
# Idempotent: safe to re-run. Stands up (or updates) all pieces in order:
#   1. CDK bootstrap (once)
#   2. Platform slice (CodeBuild + artifact bucket)                         [CDK]
#   3. AgentCore exec role (create-if-absent + put inline policy)           [CLI]
#   4. Agent runtime (build arm64 zip → upload → create/update → wait READY)
#   5. Hosting (S3 SPA + CloudFront + Lambda bridge)                        [CDK]
#   6. SPA build → S3 sync → CloudFront invalidate
#
# Usage:
#   AWS_PROFILE=<your-profile> ./deploy-console.sh
#
# Env (override as needed):
#   AWS_PROFILE     (required)         AWS_REGION   (default us-east-1)
#   ACCOUNT_ID      (default: from STS) NAMESPACE   (default agentpoc)
#   SOURCE_OWNER / SOURCE_REPO / SOURCE_BRANCH  (infra repo; defaults to aws-samples)
#
# Prereqs: aws cli, node/npm (for cdk via npx + the SPA build), python3, uv,
#          docker NOT required (agent uses code-zip deploy).
# ============================================================================
set -euo pipefail

# ---- resolve config --------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGION="${AWS_REGION:-us-east-1}"
NS="${NAMESPACE:-agentpoc}"
: "${AWS_PROFILE:?set AWS_PROFILE (e.g. my-sandbox-profile)}"
export AWS_PROFILE AWS_REGION="$REGION"
ACCOUNT_ID="${ACCOUNT_ID:-$(aws sts get-caller-identity --query Account --output text)}"
MODEL_ID="${BEDROCK_MODEL_ID:-us.anthropic.claude-sonnet-4-5-20250929-v1:0}"
INFRA="$(cd "$HERE/../infrastructure" && pwd)"

say(){ printf '\n\033[1;33m▶ %s\033[0m\n' "$*"; }
ok(){ printf '\033[1;32m✓ %s\033[0m\n' "$*"; }

say "Deploy Console → account $ACCOUNT_ID / $REGION / namespace=$NS"

# ---- helper: ensure a python venv with pinned cdk lib ----------------------
venv(){ # $1=dir
  local d="$1"
  [ -x "$d/.venv/bin/python" ] || python3 -m venv "$d/.venv"
  "$d/.venv/bin/pip" install -q "aws-cdk-lib==2.199.0" "constructs>=10,<11" 2>/dev/null || true
}

# ---- 1. CDK bootstrap (idempotent) -----------------------------------------
say "1/6 CDK bootstrap"
if aws cloudformation describe-stacks --stack-name CDKToolkit >/dev/null 2>&1; then
  ok "CDKToolkit already bootstrapped"
else
  # Bootstrap only provisions the CDKToolkit stack for the explicit target env;
  # no --app needed (synthesizing the infra app would require its config/ CWD).
  CDK_DEFAULT_ACCOUNT="$ACCOUNT_ID" CDK_DEFAULT_REGION="$REGION" \
    npx --yes cdk bootstrap "aws://$ACCOUNT_ID/$REGION" >/dev/null
  ok "bootstrapped"
fi

# ---- 2. Platform slice (CodeBuild + bucket + agent exec role) --------------
say "2/6 Platform slice (CodeBuild, artifact bucket)"
venv "$HERE/platform-infra"
CDK_DEFAULT_ACCOUNT="$ACCOUNT_ID" CDK_DEFAULT_REGION="$REGION" PLATFORM_NAMESPACE="$NS" \
  ${SOURCE_OWNER:+SOURCE_OWNER="$SOURCE_OWNER"} ${SOURCE_REPO:+SOURCE_REPO="$SOURCE_REPO"} \
  ${SOURCE_BRANCH:+SOURCE_BRANCH="$SOURCE_BRANCH"} \
  npx --yes cdk deploy --app "$HERE/platform-infra/.venv/bin/python $HERE/platform-infra/phase0_stack.py" \
    --require-approval never --outputs-file /tmp/platform-out.json >/dev/null
PSTACK="deploy-console-phase0-$NS"
ARTIFACT_BUCKET=$(python3 -c "import json;print(json.load(open('/tmp/platform-out.json'))['$PSTACK']['ArtifactBucket'])")
CODEBUILD_PROJECT=$(python3 -c "import json;print(json.load(open('/tmp/platform-out.json'))['$PSTACK']['CodeBuildProject'])")
ok "bucket=$ARTIFACT_BUCKET  project=$CODEBUILD_PROJECT"

# ---- 3. AgentCore exec role (idempotent via CLI) ---------------------------
# Created via CLI (not CDK) because the burner guardrail denies iam:DeleteRole, so
# CDK can't own a same-named role. create-if-absent, then always (re)put the policy.
say "3/6 AgentCore execution role"
ROLE_NAME="$NS-agentcore-exec-role"
AGENT_EXEC_ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/$ROLE_NAME"
TRUST=$(cat <<JSON
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"bedrock-agentcore.amazonaws.com"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"aws:SourceAccount":"$ACCOUNT_ID"},"ArnLike":{"aws:SourceArn":"arn:aws:bedrock-agentcore:$REGION:$ACCOUNT_ID:*"}}}]}
JSON
)
if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  ok "role exists ($ROLE_NAME)"
else
  aws iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document "$TRUST" \
    --description "AgentCore Runtime execution role for the deployment console" >/dev/null
  ok "role created ($ROLE_NAME)"
fi
PERMS=$(cat <<JSON
{"Version":"2012-10-17","Statement":[
{"Sid":"ECRPull","Effect":"Allow","Action":["ecr:BatchGetImage","ecr:GetDownloadUrlForLayer"],"Resource":"arn:aws:ecr:$REGION:$ACCOUNT_ID:repository/*"},
{"Sid":"ECRToken","Effect":"Allow","Action":["ecr:GetAuthorizationToken"],"Resource":"*"},
{"Sid":"Logs","Effect":"Allow","Action":["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents","logs:DescribeLogStreams","logs:DescribeLogGroups"],"Resource":["arn:aws:logs:$REGION:$ACCOUNT_ID:log-group:/aws/bedrock-agentcore/runtimes/*","arn:aws:logs:$REGION:$ACCOUNT_ID:log-group:/aws/bedrock-agentcore/runtimes/*:*"]},
{"Sid":"XRay","Effect":"Allow","Action":["xray:PutTraceSegments","xray:PutTelemetryRecords","xray:GetSamplingRules","xray:GetSamplingTargets"],"Resource":"*"},
{"Sid":"Metrics","Effect":"Allow","Action":"cloudwatch:PutMetricData","Resource":"*","Condition":{"StringEquals":{"cloudwatch:namespace":"bedrock-agentcore"}}},
{"Sid":"WorkloadIdentity","Effect":"Allow","Action":["bedrock-agentcore:GetWorkloadAccessToken","bedrock-agentcore:GetWorkloadAccessTokenForJWT","bedrock-agentcore:GetWorkloadAccessTokenForUserId"],"Resource":["arn:aws:bedrock-agentcore:$REGION:$ACCOUNT_ID:workload-identity-directory/default","arn:aws:bedrock-agentcore:$REGION:$ACCOUNT_ID:workload-identity-directory/default/workload-identity/*"]},
{"Sid":"BedrockModel","Effect":"Allow","Action":["bedrock:InvokeModel","bedrock:InvokeModelWithResponseStream"],"Resource":["arn:aws:bedrock:*::foundation-model/*","arn:aws:bedrock:$REGION:$ACCOUNT_ID:inference-profile/*"]},
{"Sid":"ToolS3","Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:ListBucket"],"Resource":["arn:aws:s3:::$ARTIFACT_BUCKET","arn:aws:s3:::$ARTIFACT_BUCKET/*"]},
{"Sid":"ToolCodeBuild","Effect":"Allow","Action":["codebuild:StartBuild","codebuild:BatchGetBuilds"],"Resource":"arn:aws:codebuild:$REGION:$ACCOUNT_ID:project/$CODEBUILD_PROJECT"},
{"Sid":"ToolLogsRead","Effect":"Allow","Action":["logs:GetLogEvents","logs:FilterLogEvents"],"Resource":"arn:aws:logs:$REGION:$ACCOUNT_ID:log-group:/$NS/*"},
{"Sid":"ToolCfnRead","Effect":"Allow","Action":["cloudformation:DescribeStacks","cloudformation:DescribeStackResources","cloudformation:ListStacks"],"Resource":"*"},
{"Sid":"ToolBatchRead","Effect":"Allow","Action":["batch:DescribeJobQueues","batch:DescribeComputeEnvironments"],"Resource":"*"}
]}
JSON
)
aws iam put-role-policy --role-name "$ROLE_NAME" --policy-name "$NS-agentcore-perms" \
  --policy-document "$PERMS" >/dev/null
ok "execRole=$AGENT_EXEC_ROLE_ARN (policy applied)"

# ---- 4. Agent: build arm64 zip → upload → create/update → wait READY -------
say "4/6 Agent runtime (build + deploy, idempotent)"
bash "$HERE/agent/build_package.sh" >/dev/null
AGENT_ARN=$(
  REGION="$REGION" ARTIFACT_BUCKET="$ARTIFACT_BUCKET" AGENT_EXEC_ROLE_ARN="$AGENT_EXEC_ROLE_ARN" \
  CODEBUILD_PROJECT="$CODEBUILD_PROJECT" TARGET_ACCOUNT_ID="$ACCOUNT_ID" NAMESPACE="$NS" \
  BEDROCK_MODEL_ID="$MODEL_ID" \
  python3 "$HERE/agent/deploy_agent.py"
)
ok "agentRuntimeArn=$AGENT_ARN"

# ---- 4. Hosting (S3 SPA + CloudFront + Lambda bridge) ----------------------
say "5/6 Hosting (CloudFront + S3 + Lambda bridge)"
venv "$HERE/hosting"
CDK_DEFAULT_ACCOUNT="$ACCOUNT_ID" CDK_DEFAULT_REGION="$REGION" PLATFORM_NAMESPACE="$NS" \
  AGENT_RUNTIME_ARN="$AGENT_ARN" CODEBUILD_PROJECT="$CODEBUILD_PROJECT" \
  npx --yes cdk deploy --app "$HERE/hosting/.venv/bin/python $HERE/hosting/hosting_stack.py" \
    --require-approval never --outputs-file /tmp/hosting-out.json >/dev/null
HSTACK="deploy-console-hosting-$NS"
SPA_BUCKET=$(python3 -c "import json;print(json.load(open('/tmp/hosting-out.json'))['$HSTACK']['SpaBucketName'])")
DIST_ID=$(python3 -c "import json;print(json.load(open('/tmp/hosting-out.json'))['$HSTACK']['DistributionId'])")
CONSOLE_URL=$(python3 -c "import json;print(json.load(open('/tmp/hosting-out.json'))['$HSTACK']['ConsoleUrl'])")
ok "spaBucket=$SPA_BUCKET  dist=$DIST_ID"

# ---- 5. SPA: build (live) → sync → invalidate ------------------------------
say "6/6 SPA build + ship"
( cd "$HERE/frontend" && [ -d node_modules ] || npm install --silent )
( cd "$HERE/frontend" && VITE_DEFAULT_MODE=live npm run build >/dev/null )
aws s3 sync "$HERE/frontend/dist/" "s3://$SPA_BUCKET/" --delete >/dev/null
aws cloudfront create-invalidation --distribution-id "$DIST_ID" --paths "/*" >/dev/null
ok "SPA shipped + CloudFront invalidated"

# ---- summary ---------------------------------------------------------------
cat <<EOF

============================================================================
✓ Deployment Console is live.

  Console URL : $CONSOLE_URL
  Agent ARN   : $AGENT_ARN
  Bucket(SPA) : $SPA_BUCKET     Dist: $DIST_ID
  Artifacts   : $ARTIFACT_BUCKET
  CodeBuild   : $CODEBUILD_PROJECT
  Exec role   : $AGENT_EXEC_ROLE_ARN

  (CloudFront invalidation may take 1-2 min to fully propagate.)
============================================================================
EOF
