# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional

from aws_cdk import (
    Stack,
    aws_iam as iam,
)
from constructs import Construct


@dataclass
class S3BucketArnConfig:
    """Configuration for S3 Bucket ARNs"""

    s3_standard_bucket_arn: str
    s3_express_bucket_arn: str


@dataclass
class BatchJobDeploymentType(str, Enum):
    """
    Batch deployment types supported by the infrastructure
    """

    SINGLE_NODE = "SINGLE_NODE"
    MULTI_NODE = "MULTI_NODE"
    ALL = "ALL"


@dataclass
class BatchJobConstructConfig:
    """Configuration for Batch Job Construct"""

    namespace: str
    s3_bucket_config: S3BucketArnConfig


class BatchJobConstruct(Construct):
    MOUNT_PATH: str = "/fsx"

    def __init__(
        self, scope: Construct, construct_id: str, config: BatchJobConstructConfig
    ):
        super().__init__(scope, construct_id)

        self.s3_bucket_config = config.s3_bucket_config
        self.namespace = config.namespace

        self.job_role = self.create_job_role()
        self.instance_role = self.create_instance_role()
        self.task_execution_role = self.create_task_execution_role()

    @property
    def region(self) -> str:
        """Get the current AWS region."""
        return Stack.of(self).region

    @property
    def account(self) -> str:
        """Get the current AWS account ID."""
        return Stack.of(self).account

    def create_job_role(self) -> iam.Role:
        """
        Creates an IAM role for AWS Batch jobs with conditional policies.

        Returns:
            iam.Role: Configured IAM role with appropriate permissions
        """
        return iam.Role(
            self,
            "BatchJobRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            inline_policies=self._get_job_role_policies(),
        )

    def _get_job_role_policies(self) -> Dict[str, iam.PolicyDocument]:
        """
        Assembles all required policies for the job role.

        Returns:
            Dict[str, iam.PolicyDocument]: Combined policies
        """
        policies = {}

        # Add S3 Standard policy if configured
        if s3_standard_policy := self._create_s3_standard_policy():
            policies.update(s3_standard_policy)

        # Add S3 Express policy if configured
        if s3_express_policy := self._create_s3_express_policy():
            policies.update(s3_express_policy)

        # Add required Glue policy
        policies.update(self._create_glue_policy())

        # Add required Batch policy
        policies.update(self._create_batch_policy())

        # Add required CloudWatch policy
        policies.update(self._create_cloudwatch_policy())

        return policies

    def _create_s3_standard_policy(self) -> Optional[Dict[str, iam.PolicyDocument]]:
        """Creates S3 standard bucket policy if configured."""
        if (
            not hasattr(self, "s3_bucket_config")
            or not self.s3_bucket_config.s3_standard_bucket_arn
        ):
            return None

        return {
            "ReadWriteS3Standard": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        sid="StandardS3Access",
                        actions=[
                            "s3:ListBucket",
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                        ],
                        resources=[
                            self.s3_bucket_config.s3_standard_bucket_arn,
                            f"{self.s3_bucket_config.s3_standard_bucket_arn}/*",
                        ],
                    )
                ]
            )
        }

    def _create_s3_express_policy(self) -> Optional[Dict[str, iam.PolicyDocument]]:
        """Creates S3 Express bucket policy if configured."""
        if (
            not hasattr(self, "s3_bucket_config")
            or not self.s3_bucket_config.s3_express_bucket_arn
        ):
            return None

        return {
            "ReadWriteS3Express": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        sid="S3ExpressAccess",
                        actions=[
                            "s3express:ListBucket",
                            "s3express:GetObject",
                            "s3express:PutObject",
                            "s3express:DeleteObject",
                        ],
                        resources=[
                            self.s3_bucket_config.s3_express_bucket_arn,
                            f"{self.s3_bucket_config.s3_express_bucket_arn}/*",
                        ],
                    )
                ]
            )
        }

    def _create_glue_policy(self) -> Dict[str, iam.PolicyDocument]:
        """Creates Glue access policy."""
        return {
            "ReadGlue": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        sid="GlueReadAccess",
                        actions=[
                            "glue:GetTable",
                            "glue:GetDatabase",
                            "glue:GetPartitions",
                            "glue:GetTableVersions",
                        ],
                        resources=[
                            f"arn:aws:glue:{self.region}:{self.account}:catalog",
                            f"arn:aws:glue:{self.region}:{self.account}:database/*",
                            f"arn:aws:glue:{self.region}:{self.account}:table/*",
                        ],
                    )
                ]
            )
        }

    def _create_batch_policy(self) -> Dict[str, iam.PolicyDocument]:
        """Creates Batch access policy."""
        return {
            "BatchJobAccess": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        sid="BatchJobAccess",
                        actions=[
                            "batch:SubmitJob",
                            "batch:DescribeJobs",
                            "batch:TerminateJob",
                            "batch:ListJobs",
                        ],
                        resources=[
                            f"arn:aws:batch:{self.region}:{self.account}:job-queue/*",
                            f"arn:aws:batch:{self.region}:{self.account}:job-definition/*",
                        ],
                    )
                ]
            )
        }

    def _create_cloudwatch_policy(self) -> Dict[str, iam.PolicyDocument]:
        """Creates CloudWatch Logs access policy."""
        return {
            "CloudWatchLogsAccess": iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        sid="CloudWatchLogsAccess",
                        actions=[
                            "logs:CreateLogStream",
                            "logs:PutLogEvents",
                            "logs:DescribeLogStreams",
                        ],
                        resources=[
                            f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/ecs/*"
                        ],
                    )
                ]
            )
        }

    def create_task_execution_role(self) -> iam.Role:
        """
        Creates an ECS task execution role for AWS Batch.
        """

        return iam.Role(
            self,
            "BatchJobTaskExecutionRole",
            description=f"ECS task execution role for AWS Batch with {self.namespace}",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ],
            inline_policies={
                "LogAccess": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            sid="CloudWatchLogsAccess",
                            actions=[
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                                "logs:DescribeLogStreams",
                            ],
                            resources=[
                                f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/ecs/*"
                            ],
                        )
                    ]
                ),
            },
        )

    def create_instance_role(self) -> iam.Role:
        """
        Creates an EC2 instance role for AWS Batch.
        """

        return iam.Role(
            self,
            "BatchJobInstanceRole",
            description=f"EC2 instance role for AWS Batch with {self.namespace}",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("ecs.amazonaws.com"),
                iam.ServicePrincipal("ec2.amazonaws.com"),
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonEC2ContainerServiceforEC2Role"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchAgentAdminPolicy"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                ),
            ],
        )
