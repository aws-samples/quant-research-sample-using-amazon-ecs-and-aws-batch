# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from dataclasses import dataclass, field
from typing import List

from aws_cdk import Stack, aws_ec2 as ec2, aws_ecr as ecr, Environment
from constructs import Construct

from .base_construct import S3BucketArnConfig
from .container_jobs import ContainerJobConfig, ContainerJobs
from .cpu_queues import CpuQueueSpec, CpuQueues


@dataclass
class EarningsResearchConfig:
    """Configuration for the samples/earnings_research compute stack"""

    namespace: str
    vpc: ec2.IVpc
    security_group: ec2.ISecurityGroup
    s3_bucket_config: S3BucketArnConfig
    image_repository: ecr.IRepository
    queues: List[CpuQueueSpec]
    job_definition_name: str = "earnings-research"
    secret_names: List[str] = field(default_factory=list)
    bedrock_model_patterns: List[str] = field(default_factory=lambda: ["anthropic.*"])
    container_cpu: int = 1
    container_memory: int = 4096


class EarningsResearchStack(Stack):
    """
    Job queues listed in batch.queues plus one job definition running the
    samples/earnings_research image (all packages, dispatched by the container command).
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        env: Environment,
        config: EarningsResearchConfig,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, env=env, **kwargs)

        self.jobs = ContainerJobs(
            self,
            "Jobs",
            config=ContainerJobConfig(
                namespace=config.namespace,
                name=config.job_definition_name,
                s3_bucket_config=config.s3_bucket_config,
                image_repository=config.image_repository,
                secret_names=config.secret_names,
                bedrock_model_patterns=config.bedrock_model_patterns,
                container_cpu=config.container_cpu,
                container_memory=config.container_memory,
            ),
        )

        self.queues = CpuQueues(
            self,
            "Queues",
            namespace=config.namespace,
            vpc=config.vpc,
            security_group=config.security_group,
            instance_role=self.jobs.instance_role,
            queues=config.queues,
        )

        self.jobs.job_definition.node.add_dependency(
            *self.queues.job_queues.values(),
            *self.queues.compute_environments.values(),
        )
