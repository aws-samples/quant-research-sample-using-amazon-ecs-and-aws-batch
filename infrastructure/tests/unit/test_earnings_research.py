# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
from pathlib import Path

import aws_cdk as core
from aws_cdk.assertions import Template

from infrastructure.batch.base_construct import S3BucketArnConfig
from infrastructure.batch.cpu_queues import CpuQueueSpec
from infrastructure.batch.earnings_research import (
    EarningsResearchConfig,
    EarningsResearchStack,
)
from infrastructure.common.network import NetworkStack
from infrastructure.common.pipeline import (
    DeploymentPipelineStack,
    ImageBuildSpec,
    PipelineConfig,
)
from infrastructure.utils import dict_to_obj

PARAMETERS = Path(__file__).resolve().parents[2] / "config" / "parameters.json"


def _stacks():
    params = dict_to_obj(json.loads(PARAMETERS.read_text()))
    app = core.App()
    env = core.Environment(account="012345678901", region="us-east-1")
    network = NetworkStack(
        app,
        "network",
        env=env,
        namespace="test-ns",
        availability_zone="us-east-1a",
        with_s3express=False,
        nat_gateways=params.earnings_research.nat_gateways,
    )
    pipeline = DeploymentPipelineStack(
        app,
        "pipeline",
        env=env,
        config=PipelineConfig(
            namespace="test-ns",
            github_owner="owner",
            github_repo="repo",
            github_branch="main",
            github_token_secret_name="github-token",
            enable_code_pipeline=False,
            image_builds=[ImageBuildSpec.from_params(params.earnings_research.image_build)],
        ),
    )
    stack = EarningsResearchStack(
        app,
        "earnings",
        env=env,
        config=EarningsResearchConfig(
            namespace="test-ns",
            vpc=network.vpc,
            security_group=network.security_group,
            s3_bucket_config=S3BucketArnConfig(
                s3_standard_bucket_arn="arn:aws:s3:::example-standard",
                s3_express_bucket_arn=None,
                custom_s3_arns=params.s3.custom_arns,
            ),
            image_repository=pipeline.image_repos[params.earnings_research.image_build.id],
            queues=[CpuQueueSpec.from_params(q) for q in params.batch.queues],
            secret_names=params.earnings_research.secret_names,
        ),
    )
    return params, Template.from_stack(stack)


def test_default_parameters_deploy_every_listed_queue():
    params, template = _stacks()
    template.resource_count_is("AWS::Batch::JobQueue", len(params.batch.queues))
    template.resource_count_is(
        "AWS::Batch::ComputeEnvironment", len(params.batch.queues)
    )
    template.resource_count_is("AWS::Batch::JobDefinition", 1)


def test_default_queue_ids_are_the_documented_ones():
    params, _ = _stacks()
    assert [q.id for q in params.batch.queues] == ["cpu", "capped"]
    capped = next(q for q in params.batch.queues if q.id == "capped")
    assert capped.maxv_cpus <= 32
