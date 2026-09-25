# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json

import aws_cdk as core
from aws_cdk import aws_ecr as ecr
from aws_cdk.assertions import Match, Template

from infrastructure.batch.base_construct import S3BucketArnConfig
from infrastructure.batch.container_jobs import ContainerJobConfig, ContainerJobs


def _template(**overrides) -> Template:
    app = core.App()
    env = core.Environment(account="012345678901", region="us-east-1")
    stack = core.Stack(app, "infrastructure", env=env)
    repo = ecr.Repository(stack, "Repo", repository_name="test-ns-earnings-research")
    values = dict(
        namespace="test-ns",
        name="earnings-research",
        s3_bucket_config=S3BucketArnConfig(
            s3_standard_bucket_arn="arn:aws:s3:::example-standard",
            s3_express_bucket_arn=None,
            custom_s3_arns=["arn:aws:s3:::example-data", "arn:aws:s3:::example-data/*"],
        ),
        image_repository=repo,
        secret_names=["example/redshift", "example/vendor"],
    )
    values.update(overrides)
    ContainerJobs(stack, "Jobs", config=ContainerJobConfig(**values))
    return Template.from_stack(stack)


def _job_role_statements(template: Template):
    policies = template.find_resources("AWS::IAM::Policy")
    statements = []
    for policy in policies.values():
        statements.extend(policy["Properties"]["PolicyDocument"]["Statement"])
    return {s.get("Sid"): s for s in statements}


def test_one_job_definition_on_latest_image():
    template = _template()
    template.resource_count_is("AWS::Batch::JobDefinition", 1)
    template.has_resource_properties(
        "AWS::Batch::JobDefinition",
        {
            "JobDefinitionName": "test-ns-earnings-research",
            "RetryStrategy": {"Attempts": 2},
            "ContainerProperties": Match.object_like(
                {
                    "Image": Match.object_like(
                        {"Fn::Join": Match.array_with([Match.array_with([":latest"])])}
                    )
                }
            ),
        },
    )
    template.has_resource_properties(
        "AWS::Logs::LogGroup",
        {"LogGroupName": "/test-ns/batch-job/earnings-research"},
    )


def test_secrets_granted_by_name_only():
    statements = _job_role_statements(_template())
    secrets = statements["ReadSecrets"]
    assert secrets["Action"] == "secretsmanager:GetSecretValue"
    resources = json.dumps(secrets["Resource"])
    assert "secret:example/redshift-*" in resources
    assert "secret:example/vendor-*" in resources


def test_redshift_and_bedrock_grants():
    statements = _job_role_statements(_template())
    assert "redshift-data:ExecuteStatement" in statements["RedshiftDataStatements"]["Action"]
    assert "redshift-data:GetStatementResult" in statements["RedshiftDataResults"]["Action"]
    assert statements["RedshiftServerlessCredentials"]["Action"] == "redshift-serverless:GetCredentials"
    bedrock = statements["BedrockInvoke"]
    assert "bedrock:InvokeModel" in bedrock["Action"]
    assert "foundation-model/anthropic.*" in json.dumps(bedrock["Resource"])


def test_no_secrets_no_secret_statement():
    statements = _job_role_statements(_template(secret_names=[]))
    assert "ReadSecrets" not in statements


def test_base_role_policies_kept():
    template = _template()
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "Policies": Match.array_with(
                [Match.object_like({"PolicyName": "ReadWriteS3CustomLocations"})]
            )
        },
    )
