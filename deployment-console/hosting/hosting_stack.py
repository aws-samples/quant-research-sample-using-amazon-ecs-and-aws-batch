# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Hosting stack for the Deployment Console SPA + bridge.

  Browser → CloudFront ┬─ default        → S3 (private, OAC) : the Vite SPA
                       └─ /api/*          → Lambda Function URL (the bridge)

Single origin host for the browser (no CORS): CloudFront serves the SPA and proxies
/api/* to the bridge Lambda's Function URL. The bridge holds creds via its role and
calls AgentCore + CodeBuild.

POC posture: no auth (Function URL AuthType NONE). Inputs via env:
  CDK_DEFAULT_ACCOUNT, CDK_DEFAULT_REGION, AGENT_RUNTIME_ARN,
  CODEBUILD_PROJECT (for status reads), PLATFORM_NAMESPACE.
"""

import os

from aws_cdk import (
    App, Stack, Environment, CfnOutput, Duration, RemovalPolicy,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_cloudfront as cf,
    aws_cloudfront_origins as origins,
)
from constructs import Construct


class HostingStack(Stack):
    def __init__(self, scope: Construct, cid: str, *, env: Environment,
                 namespace: str, agent_arn: str, codebuild_project: str, **kwargs):
        super().__init__(scope, cid, env=env, **kwargs)

        # --- SPA bucket (private; served only via CloudFront OAC) ----------------
        site = s3.Bucket(
            self, "SpaBucket",
            bucket_name=f"{namespace}-deploy-console-spa-{self.account}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            enforce_ssl=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        # --- Bridge Lambda + Function URL ---------------------------------------
        bridge = _lambda.Function(
            self, "BridgeFn",
            function_name=f"{namespace}-deploy-console-bridge",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=_lambda.Code.from_asset(os.path.join(os.path.dirname(__file__), "bridge")),
            timeout=Duration.seconds(60),  # agent turns can take a while
            memory_size=256,
            environment={
                "AGENT_RUNTIME_ARN": agent_arn,
                "REGION": self.region,
                "STACK_NAMESPACE": namespace,
                "CODEBUILD_PROJECT": codebuild_project,
            },
        )
        bridge.add_to_role_policy(iam.PolicyStatement(
            sid="InvokeAgentRuntime",
            actions=["bedrock-agentcore:InvokeAgentRuntime"],
            resources=[agent_arn, agent_arn + "/*"],
        ))
        bridge.add_to_role_policy(iam.PolicyStatement(
            sid="CodeBuildRead",
            actions=["codebuild:BatchGetBuilds", "codebuild:ListBuildsForProject"],
            resources=[f"arn:aws:codebuild:{self.region}:{self.account}:project/{codebuild_project}"],
        ))
        # Read the CodeBuild execution output for the Logs tab (GET /api/logs).
        # The build writes to /{namespace}/deploy-console/codebuild (see phase0_stack);
        # GetLogEvents needs the log-stream ARN, so scope to that group's streams.
        bridge.add_to_role_policy(iam.PolicyStatement(
            sid="BuildLogsRead",
            actions=["logs:GetLogEvents"],
            resources=[
                f"arn:aws:logs:{self.region}:{self.account}:"
                f"log-group:/{namespace}/deploy-console/codebuild:*",
            ],
        ))
        # Read the deployed quant-research stacks for the Components tab. ListStacks /
        # DescribeStackResources don't support resource-level scoping, so they're "*";
        # the bridge code restricts results to this console's namespace (see handler).
        bridge.add_to_role_policy(iam.PolicyStatement(
            sid="CfnRead",
            actions=[
                "cloudformation:ListStacks",
                "cloudformation:DescribeStacks",
                "cloudformation:DescribeStackResources",
            ],
            resources=["*"],
        ))
        # IAM auth on the Function URL: this burner's account guardrail (SCP) blocks
        # public (NONE) Function URLs. CloudFront signs requests via OAC (SigV4), so the
        # browser still calls anonymously through CloudFront while CF→Lambda is IAM-authed.
        fn_url = bridge.add_function_url(auth_type=_lambda.FunctionUrlAuthType.AWS_IAM)

        # --- CloudFront: SPA default + /api/* → bridge (OAC-signed) -------------
        api_origin = origins.FunctionUrlOrigin.with_origin_access_control(fn_url)

        dist = cf.Distribution(
            self, "Distribution",
            comment=f"{namespace} deployment console",
            default_root_object="index.html",
            default_behavior=cf.BehaviorOptions(
                origin=origins.S3BucketOrigin.with_origin_access_control(site),
                viewer_protocol_policy=cf.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                cache_policy=cf.CachePolicy.CACHING_OPTIMIZED,
            ),
            additional_behaviors={
                "/api/*": cf.BehaviorOptions(
                    origin=api_origin,
                    viewer_protocol_policy=cf.ViewerProtocolPolicy.HTTPS_ONLY,
                    allowed_methods=cf.AllowedMethods.ALLOW_ALL,
                    cache_policy=cf.CachePolicy.CACHING_DISABLED,
                    # Forward everything EXCEPT Host — Lambda Function URLs reject a
                    # mismatched Host header, so CloudFront must not pass the viewer's.
                    origin_request_policy=cf.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                ),
            },
            # SPA fallback so client routes resolve.
            error_responses=[
                cf.ErrorResponse(http_status=403, response_http_status=200,
                                 response_page_path="/index.html"),
                cf.ErrorResponse(http_status=404, response_http_status=200,
                                 response_page_path="/index.html"),
            ],
        )

        # OAC-signed CloudFront→Lambda invocations need BOTH lambda:InvokeFunctionUrl
        # AND lambda:InvokeFunction. The FunctionUrlOrigin.with_origin_access_control
        # construct only adds InvokeFunctionUrl, but as of October 2025 AWS requires
        # both permissions on (new) Function URLs, so OAC requests are otherwise rejected
        # with 403 AccessDeniedException before the function is invoked. We add the
        # second grant explicitly, scoped to this distribution.
        # Docs: https://docs.aws.amazon.com/lambda/latest/dg/urls-auth.html
        #       https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-restricting-access-to-lambda.html
        _lambda.CfnPermission(
            self, "AllowCloudFrontInvokeFunction",
            action="lambda:InvokeFunction",
            function_name=bridge.function_name,
            principal="cloudfront.amazonaws.com",
            source_arn=f"arn:aws:cloudfront::{self.account}:distribution/{dist.distribution_id}",
        )

        CfnOutput(self, "SpaBucketName", value=site.bucket_name)
        CfnOutput(self, "DistributionId", value=dist.distribution_id)
        CfnOutput(self, "ConsoleUrl", value=f"https://{dist.distribution_domain_name}")
        CfnOutput(self, "BridgeFunctionUrl", value=fn_url.url)


def main():
    app = App()
    HostingStack(
        app, f"deploy-console-hosting-{os.environ.get('PLATFORM_NAMESPACE','agentpoc')}",
        env=Environment(account=os.environ["CDK_DEFAULT_ACCOUNT"],
                        region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1")),
        namespace=os.environ.get("PLATFORM_NAMESPACE", "agentpoc"),
        agent_arn=os.environ["AGENT_RUNTIME_ARN"],
        codebuild_project=os.environ.get("CODEBUILD_PROJECT", "agentpoc-deploy-console-cdk-deploy"),
    )
    app.synth()


if __name__ == "__main__":
    main()
