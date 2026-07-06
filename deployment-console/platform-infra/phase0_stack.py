# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Platform infrastructure — the minimal slice that runs the deploy path.

Provisions:
  - An S3 bucket for agent-produced parameter overrides + cdk outputs (per run).
  - A CodeBuild project whose source is the PUBLIC GitHub repo (cloned at build time),
    running `cdk deploy` on infrastructure/ via the injection path
    (cdk.context.json via merge_params.py). See buildspec.yml.
  - The CodeBuild service role.

POC SIMPLIFICATIONS:
  - NO Cognito / auth — single trusted operator.
  - Source = PUBLIC GitHub repo by URL (no S3 artifact, no GitHub token needed for a
    public read-only clone).
  - Observability is PULL: the agent reads status via codebuild:BatchGetBuilds +
    logs:GetLogEvents (see backend/tools/get_status.py). No AppSync/EventBridge/Live Tail.

SECURITY (POC only): the CodeBuild role is granted AdministratorAccess. DELIBERATE,
TRACKED deferral for a single-operator sandbox. Scope down BEFORE any shared / prod use.

Not included in this POC (possible later work): Step Functions approval gate.
"""

from aws_cdk import (
    App,
    Stack,
    Environment,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_codebuild as codebuild,
    aws_iam as iam,
    aws_s3 as s3,
    aws_logs as logs,
)
from constructs import Construct
import os


class Phase0DeploySlice(Stack):
    # Default infra source: the public aws-samples repo.
    DEFAULT_SOURCE_OWNER = "aws-samples"
    DEFAULT_SOURCE_REPO = "quant-research-sample-using-amazon-ecs-and-aws-batch"

    def __init__(self, scope: Construct, cid: str, *, env: Environment, namespace: str,
                 source_branch: str = "main", source_owner: str | None = None,
                 source_repo: str | None = None, **kwargs) -> None:
        super().__init__(scope, cid, env=env, **kwargs)

        # --- S3 bucket: agent overrides in, cdk outputs out -------------------------
        self.artifact_bucket = s3.Bucket(
            self, "DeployArtifacts",
            bucket_name=f"{namespace}-deploy-console-artifacts-{self.account}",
            removal_policy=RemovalPolicy.DESTROY,  # spike convenience; revisit for prod
            auto_delete_objects=True,
            enforce_ssl=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
        )

        log_group = logs.LogGroup(
            self, "BuildLogs",
            log_group_name=f"/{namespace}/deploy-console/codebuild",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # --- CodeBuild service role -------------------------------------------------
        # POC: AdministratorAccess. Deferred scoping — see the module docstring.
        self.build_role = iam.Role(
            self, "DeployBuildRole",
            role_name=f"{namespace}-deploy-console-build-role",
            assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
            description="POC build role - ADMIN, scope down before shared/production use",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"),
            ],
        )

        # --- Source: PUBLIC GitHub repo, cloned at build time -----------------------
        # A public repo needs no GitHub token / CodeConnections for a read-only clone.
        # Owner/repo/branch default to the aws-samples repo and can be overridden so the
        # operator can point at a fork or branch without code changes.
        # report_build_status=False: we clone anonymously (no source credential) and don't
        # own the repo, so don't attempt to post a commit/PR status back to GitHub —
        # otherwise CodeBuild logs a benign but confusing "Failed to report build status:
        # There's no access token found for this account" in the FINALIZING phase.
        source = codebuild.Source.git_hub(
            owner=source_owner or self.DEFAULT_SOURCE_OWNER,
            repo=source_repo or self.DEFAULT_SOURCE_REPO,
            branch_or_ref=source_branch,
            clone_depth=1,
            report_build_status=False,
        )

        self.project = codebuild.Project(
            self, "DeployProject",
            project_name=f"{namespace}-deploy-console-cdk-deploy",
            role=self.build_role,
            source=source,
            # INLINE buildspec (not from_source_filename): the cloned PUBLIC repo only
            # contains infrastructure/ — it does NOT contain deployment-console/. So the
            # build instructions + the param-merge logic are embedded here and depend
            # ONLY on the repo's infrastructure/ dir + the OVERRIDE_S3_URI env var.
            build_spec=codebuild.BuildSpec.from_object(self._inline_buildspec()),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                compute_type=codebuild.ComputeType.MEDIUM,
                privileged=False,
            ),
            timeout=Duration.minutes(60),  # CDK deploys (FSx etc.) are slow
            logging=codebuild.LoggingOptions(
                cloud_watch=codebuild.CloudWatchLoggingOptions(log_group=log_group)
            ),
            environment_variables={
                "TARGET_ACCOUNT_ID": codebuild.BuildEnvironmentVariable(value=self.account),
                "TARGET_REGION": codebuild.BuildEnvironmentVariable(value=self.region),
                "NAMESPACE": codebuild.BuildEnvironmentVariable(value=namespace),
            },
        )

        self.artifact_bucket.grant_read_write(self.build_role)

        # NOTE on the AgentCore execution role: it is NOT created here. In the burner the
        # role already exists out-of-band and the account guardrail DENIES iam:DeleteRole,
        # so CDK can neither recreate (name clash) nor cleanly import it. The orchestrator
        # `deploy-console.sh` therefore ensures that role idempotently via the AWS CLI
        # (ensure_agent_role: create-if-absent, always put the inline policy). This keeps
        # a single entry point while respecting the guardrail. Its exact trust+perms live
        # in deploy-console.sh so they remain version-controlled.

        # Outputs consumed by deploy-console.sh to chain the next steps.
        CfnOutput(self, "ArtifactBucket", value=self.artifact_bucket.bucket_name)
        CfnOutput(self, "CodeBuildProject", value=self.project.project_name)

    @staticmethod
    def _inline_buildspec() -> dict:
        """Inline buildspec. Clones the public repo (CodeBuild source), then:
        fetch override from S3 -> deep-merge onto infrastructure/config/parameters.json
        -> infrastructure/cdk.context.json (VERIFIED injection path) -> cdk deploy --all.
        The merge is a small inline python -c so the build needs nothing from
        deployment-console/ (which is not in the public repo)."""
        # Merge script written to a file at build time, then executed.
        merge_script = (
            "import json\n"
            "base=json.load(open('config/parameters.json'))\n"
            "ovr=json.load(open('/tmp/override.json'))\n"
            "def m(a,b):\n"
            "    for k,v in b.items():\n"
            "        if isinstance(v,dict) and isinstance(a.get(k),dict): m(a[k],v)\n"
            "        else: a[k]=v\n"
            "    return a\n"
            "merged=m(base,ovr)\n"
            "json.dump({'parameters':merged},open('cdk.context.json','w'),indent=2)\n"
            "print('deployment_type=',merged['batch']['deployment_type'])\n"
        )
        # NOTE: CodeBuild runs each list item in a FRESH shell — `cd` does NOT persist
        # between commands. So each phase is a SINGLE chained command (&&) operating from
        # an absolute $CODEBUILD_SRC_DIR/infrastructure path.
        pre_build_cmd = (
            'echo "RUN_ID=$RUN_ID NAMESPACE=$NAMESPACE TARGET=$TARGET_ACCOUNT_ID/$TARGET_REGION" && '
            'cd "$CODEBUILD_SRC_DIR/infrastructure" && '
            "python -m pip install --upgrade pip && "
            "pip install -r requirements.txt && "
            'aws s3 cp "$OVERRIDE_S3_URI" /tmp/override.json && '
            "cat /tmp/override.json && "
            f"printf '%s' {_shquote(merge_script)} > /tmp/merge.py && "
            "python /tmp/merge.py && "
            'echo "Effective cdk.context.json:" && cat cdk.context.json'
        )
        build_cmd = (
            'cd "$CODEBUILD_SRC_DIR/infrastructure" && '
            "export AWS_ACCOUNT_ID=$TARGET_ACCOUNT_ID AWS_REGION=$TARGET_REGION "
            "CDK_DEFAULT_ACCOUNT=$TARGET_ACCOUNT_ID CDK_DEFAULT_REGION=$TARGET_REGION && "
            "cdk list && "
            "cdk deploy --all --require-approval never --outputs-file /tmp/cdk-outputs.json"
        )
        post_cmd = (
            'echo "Deploy succeeding=$CODEBUILD_BUILD_SUCCEEDING"; '
            "cat /tmp/cdk-outputs.json || echo '(no outputs)'; "
            '[ -f /tmp/cdk-outputs.json ] && aws s3 cp /tmp/cdk-outputs.json '
            '"$(dirname $OVERRIDE_S3_URI)/outputs-$RUN_ID.json" || true'
        )
        return {
            "version": "0.2",
            "phases": {
                "install": {
                    "runtime-versions": {"python": "3.12", "nodejs": "22"},
                    "commands": ["npm install -g aws-cdk@2", "cdk --version"],
                },
                "pre_build": {"commands": [pre_build_cmd]},
                "build": {"commands": [build_cmd]},
                "post_build": {"commands": [post_cmd]},
            },
        }


def _shquote(s: str) -> str:
    """Single-quote a string for safe embedding in a shell printf '%s'."""
    return "'" + s.replace("'", "'\\''") + "'"


def main() -> None:
    app = App()
    account = os.environ["CDK_DEFAULT_ACCOUNT"]
    region = os.environ.get("CDK_DEFAULT_REGION", "us-east-1")
    namespace = os.environ.get("PLATFORM_NAMESPACE", "phase0-spike")
    Phase0DeploySlice(
        app, f"deploy-console-phase0-{namespace}",
        env=Environment(account=account, region=region),
        namespace=namespace,
        source_owner=os.environ.get("SOURCE_OWNER"),
        source_repo=os.environ.get("SOURCE_REPO"),
        source_branch=os.environ.get("SOURCE_BRANCH", "main"),
    )
    app.synth()


if __name__ == "__main__":
    main()
