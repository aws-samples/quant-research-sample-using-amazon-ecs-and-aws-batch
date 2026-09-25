# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from dataclasses import dataclass, field
from typing import List, Optional

from aws_cdk import (
    CfnOutput,
    RemovalPolicy,
    Size,
    Stack,
    aws_batch as batch,
    aws_ecr as ecr,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_logs as logs,
)
from constructs import Construct

from .base_construct import (
    S3BucketArnConfig,
    BatchJobConstruct,
    BatchJobConstructConfig,
)


@dataclass
class ContainerJobConfig:
    """Configuration for one image + one job definition shared by several packages"""

    namespace: str
    name: str
    s3_bucket_config: S3BucketArnConfig
    image_repository: ecr.IRepository
    image_tag: str = "latest"
    # Secrets Manager secret names; the stack only grants read, the secrets are created by the user
    secret_names: List[str] = field(default_factory=list)
    bedrock_model_patterns: List[str] = field(default_factory=lambda: ["anthropic.*"])
    # Defaults only: package, command and vCPU/memory are overridden at submit time
    container_cpu: int = 1
    container_memory: int = 4096
    container_command: Optional[List[str]] = None
    retry_attempts: int = 2


class ContainerJobs(Construct):
    """
    IAM roles, log group and ONE EC2 job definition on <repository>:<tag>. Callers pick the
    package and command through containerOverrides.command and size the job through
    containerOverrides.resourceRequirements.
    """

    def __init__(
        self, scope: Construct, construct_id: str, config: ContainerJobConfig
    ) -> None:
        super().__init__(scope, construct_id)

        self.config = config

        # Base roles (S3 standard/express/custom, Glue, Batch, CloudWatch, Lake Formation)
        self.batch_job_construct = BatchJobConstruct(
            self,
            "BatchJobIAMPermissions",
            config=BatchJobConstructConfig(
                namespace=config.namespace,
                s3_bucket_config=config.s3_bucket_config,
            ),
        )
        self.job_role = self.batch_job_construct.job_role
        self.instance_role = self.batch_job_construct.instance_role
        self.task_execution_role = self.batch_job_construct.task_execution_role

        self.job_role.attach_inline_policy(
            iam.Policy(
                self,
                "ApplicationAccess",
                statements=self._application_statements(),
            )
        )

        self.job_definition = self.build_job_definition()

        CfnOutput(
            self,
            "JobDefinitionName",
            value=self.job_definition.job_definition_name,
            description="Job definition shared by all packages of the image",
        )

    @property
    def region(self) -> str:
        return Stack.of(self).region

    @property
    def account(self) -> str:
        return Stack.of(self).account

    def _application_statements(self) -> List[iam.PolicyStatement]:
        """Grants beyond the base job role: secrets, Redshift Data API, Bedrock"""
        statements = []

        if self.config.secret_names:
            statements.append(
                iam.PolicyStatement(
                    sid="ReadSecrets",
                    actions=["secretsmanager:GetSecretValue"],
                    resources=[
                        f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:{name}-*"
                        for name in self.config.secret_names
                    ],
                )
            )

        workgroups = f"arn:aws:redshift-serverless:{self.region}:{self.account}:workgroup/*"
        statements.extend(
            [
                iam.PolicyStatement(
                    sid="RedshiftDataStatements",
                    actions=[
                        "redshift-data:ExecuteStatement",
                        "redshift-data:BatchExecuteStatement",
                    ],
                    resources=[workgroups],
                ),
                # Statement-level calls have no resource type; they are scoped to the caller
                iam.PolicyStatement(
                    sid="RedshiftDataResults",
                    actions=[
                        "redshift-data:DescribeStatement",
                        "redshift-data:GetStatementResult",
                        "redshift-data:CancelStatement",
                        "redshift-data:ListStatements",
                    ],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    sid="RedshiftServerlessCredentials",
                    actions=["redshift-serverless:GetCredentials"],
                    resources=[workgroups],
                ),
            ]
        )

        if self.config.bedrock_model_patterns:
            statements.append(
                iam.PolicyStatement(
                    sid="BedrockInvoke",
                    actions=[
                        "bedrock:InvokeModel",
                        "bedrock:InvokeModelWithResponseStream",
                    ],
                    resources=[
                        *[
                            f"arn:aws:bedrock:*::foundation-model/{pattern}"
                            for pattern in self.config.bedrock_model_patterns
                        ],
                        f"arn:aws:bedrock:*:{self.account}:inference-profile/*",
                    ],
                )
            )

        return statements

    def build_job_definition(self) -> batch.EcsJobDefinition:
        """
        Creates the AWS Batch job definition
        """
        config = self.config
        return batch.EcsJobDefinition(
            self,
            "BatchJobDefinition",
            propagate_tags=True,
            job_definition_name=f"{config.namespace}-{config.name}",
            retry_attempts=config.retry_attempts,
            container=batch.EcsEc2ContainerDefinition(
                self,
                "BatchJobContainerDefinition",
                image=ecs.ContainerImage.from_ecr_repository(
                    config.image_repository, config.image_tag
                ),
                command=config.container_command or ["help"],
                memory=Size.mebibytes(config.container_memory),
                cpu=config.container_cpu,
                job_role=self.job_role,
                execution_role=self.task_execution_role,
                logging=ecs.LogDriver.aws_logs(
                    stream_prefix=config.namespace,
                    log_group=logs.LogGroup(
                        self,
                        "BatchJobLogGroup",
                        log_group_name=f"/{config.namespace}/batch-job/{config.name}",
                        retention=logs.RetentionDays.ONE_WEEK,
                        removal_policy=RemovalPolicy.DESTROY,
                    ),
                    mode=ecs.AwsLogDriverMode.NON_BLOCKING,
                    max_buffer_size=Size.mebibytes(128),
                ),
            ),
        )
