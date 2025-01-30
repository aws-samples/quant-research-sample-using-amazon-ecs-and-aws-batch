# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from dataclasses import dataclass

from aws_cdk import (
    Stack,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as pipeline_actions,
    aws_codebuild as codebuild,
    aws_ecr as ecr,
    SecretValue,
    Duration,
    RemovalPolicy,
    Environment,
)
from constructs import Construct


@dataclass
class PipelineConfig:
    """Configuration for Deployment Pipeline Stack"""

    namespace: str
    github_owner: str
    github_repo: str
    github_branch: str
    github_token_secret_name: str


class DeploymentPipelineStack(Stack):
    """Stack that creates a CodePipeline for container image builds"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        env: Environment,
        config: PipelineConfig,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, env=env, **kwargs)

        self.config = config

        # Create ECR Repository
        self.ecr_repo = self._create_ecr_repository()

        # Create CodeBuild Project
        self.build_project = self._create_build_project()

        # Create Pipeline
        self.pipeline = self._create_pipeline()

    def _create_ecr_repository(self) -> ecr.Repository:
        """Create ECR repository for container images"""
        return ecr.Repository(
            self,
            "ContainerImageRepository",
            repository_name=f"{self.config.namespace}-repo",
            removal_policy=RemovalPolicy.DESTROY,  # Be careful with this in production
            empty_on_delete=True,
            image_scan_on_push=True,
            lifecycle_rules=[
                ecr.LifecycleRule(
                    max_image_count=5,
                    rule_priority=1,
                    description="Keep only 5 latest images",
                )
            ],
        )

    def _create_build_project(self) -> codebuild.PipelineProject:
        """Create CodeBuild project for container image build"""
        return codebuild.PipelineProject(
            self,
            "ContainerImageBuildProject",
            project_name=f"{self.config.namespace}-image-build",
            environment=codebuild.BuildEnvironment(
                privileged=True,  # Required for container image builds
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                compute_type=codebuild.ComputeType.SMALL,
            ),
            cache=codebuild.Cache.local(codebuild.LocalCacheMode.DOCKER_LAYER),
            timeout=Duration.minutes(30),
            environment_variables={
                "ECR_REPOSITORY_URI": codebuild.BuildEnvironmentVariable(
                    value=self.ecr_repo.repository_uri
                ),
                "IMAGE_TAG": codebuild.BuildEnvironmentVariable(value="latest"),
            },
            build_spec=codebuild.BuildSpec.from_object(
                {
                    "version": "0.2",
                    "phases": {
                        "pre_build": {
                            "commands": [
                                "echo Logging in to Amazon ECR...",
                                "aws ecr get-login-password --region $AWS_DEFAULT_REGION | docker login --username AWS --password-stdin $ECR_REPOSITORY_URI",
                                "COMMIT_HASH=$(echo $CODEBUILD_RESOLVED_SOURCE_VERSION | cut -c 1-7)",
                                "IMAGE_TAG=${COMMIT_HASH:=latest}",
                            ]
                        },
                        "build": {
                            "commands": [
                                "echo Build started on `date`",
                                "echo Building the Docker image...",
                                "docker build -t $ECR_REPOSITORY_URI:$IMAGE_TAG .",
                                "docker tag $ECR_REPOSITORY_URI:$IMAGE_TAG $ECR_REPOSITORY_URI:latest",
                            ]
                        },
                        "post_build": {
                            "commands": [
                                "echo Build completed on `date`",
                                "echo Pushing the Docker image...",
                                "docker push $ECR_REPOSITORY_URI:$IMAGE_TAG",
                                "docker push $ECR_REPOSITORY_URI:latest",
                                "echo Writing image definitions file...",
                                'printf \'{"ImageURI":"%s"}\' $ECR_REPOSITORY_URI:$IMAGE_TAG > imageDefinitions.json',
                            ]
                        },
                    },
                    "artifacts": {"files": ["imageDefinitions.json"]},
                }
            ),
        )

    def _create_pipeline(self) -> codepipeline.Pipeline:
        """Create CodePipeline"""
        pipeline = codepipeline.Pipeline(
            self,
            "ContainerImagePipeline",
            pipeline_name=f"{self.config.namespace}-image-pipeline",
            cross_account_keys=False,
            restart_execution_on_update=True,
        )

        # Source Stage
        source_output = codepipeline.Artifact("SourceOutput")
        source_action = pipeline_actions.GitHubSourceAction(
            action_name="GitHubSource",
            owner=self.config.github_owner,
            repo=self.config.github_repo,
            branch=self.config.github_branch,
            oauth_token=SecretValue.secrets_manager(
                self.config.github_token_secret_name
            ),
            output=source_output,
            trigger=pipeline_actions.GitHubTrigger.WEBHOOK,
        )
        pipeline.add_stage(
            stage_name="Source",
            actions=[source_action],
        )

        # Build Stage
        build_output = codepipeline.Artifact("BuildOutput")
        build_action = pipeline_actions.CodeBuildAction(
            action_name="ContainerImageBuild",
            project=self.build_project,
            input=source_output,
            outputs=[build_output],
        )
        pipeline.add_stage(
            stage_name="Build",
            actions=[build_action],
        )

        # Grant permissions
        self.ecr_repo.grant_pull_push(self.build_project)

        return pipeline
