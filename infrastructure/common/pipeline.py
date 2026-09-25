# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import re
import shlex
from dataclasses import dataclass, field
from typing import Dict, List

from aws_cdk import (
    Stack,
    aws_codepipeline as codepipeline,
    aws_codepipeline_actions as pipeline_actions,
    aws_codebuild as codebuild,
    aws_ecr as ecr,
    aws_iam as aws_iam,
    SecretValue,
    Duration,
    RemovalPolicy,
    Environment,
)
from constructs import Construct


@dataclass
class ImageBuildSpec:
    """
    One entry of image_builds[] in parameters.json: a CodeBuild project triggered by a
    GitHub push webhook that builds one image into its own ECR repository.
    include_paths / exclude_paths are repository path prefixes.
    """

    id: str
    repository: str
    dockerfile: str
    context: str
    branch: str
    include_paths: List[str] = field(default_factory=list)
    exclude_paths: List[str] = field(default_factory=list)
    #: extra `docker build --build-arg` values, "KEY=VALUE"
    build_args: List[str] = field(default_factory=list)
    compute_type: str = "MEDIUM"
    timeout_minutes: int = 30

    def docker_build_command(self) -> str:
        """The build line; extra build args are shell-quoted (a value like "9.0;10.0"
        would otherwise end the command at the semicolon)"""
        extra = "".join(f" --build-arg {shlex.quote(a)}" for a in self.build_args)
        return (
            f"docker build -f $DOCKERFILE --build-arg CODE_VERSION=$CODE_VERSION{extra}"
            " -t $ECR_REPOSITORY_URI:$CODE_VERSION $BUILD_CONTEXT"
        )

    @classmethod
    def from_params(cls, entry) -> "ImageBuildSpec":
        """Build a spec from a parameters.json entry (SimpleNamespace or dict)"""
        values = entry if isinstance(entry, dict) else vars(entry)
        known = {k: v for k, v in values.items() if k in cls.__dataclass_fields__}
        return cls(**known)


@dataclass
class PipelineConfig:
    """Configuration for Deployment Pipeline Stack"""

    namespace: str
    github_owner: str
    github_repo: str
    github_branch: str
    github_token_secret_name: str
    enable_code_pipeline: str
    image_builds: List[ImageBuildSpec] = field(default_factory=list)
    # CodeBuild accepts one GitHub source credential per account and region;
    # set to False when the account already has one registered
    register_github_credentials: bool = True


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

        # Enable CodeBuild and CodePipeline only if needed
        if config.enable_code_pipeline:

            # Create CodeBuild Project
            self.build_project = self._create_build_project()

            # Create Pipeline
            self.pipeline = self._create_pipeline()

        # Push-triggered image builds, one CodeBuild project + ECR repository per entry
        self.image_repos: Dict[str, ecr.Repository] = {}
        self.image_build_projects: Dict[str, codebuild.Project] = {}
        if config.image_builds:
            if config.register_github_credentials:
                codebuild.GitHubSourceCredentials(
                    self,
                    "GitHubSourceCredentials",
                    access_token=SecretValue.secrets_manager(
                        config.github_token_secret_name
                    ),
                )
            for spec in config.image_builds:
                self._create_image_build(spec)

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

    @staticmethod
    def _prefix_pattern(prefixes: List[str]) -> str:
        """Webhook FILE_PATH regex matching any of the path prefixes"""
        return "^(" + "|".join(re.escape(p) for p in prefixes) + ")"

    def _webhook_filters(self, spec: ImageBuildSpec) -> List[codebuild.FilterGroup]:
        """PUSH on the configured branch, touching the included paths"""
        # anchored: and_branch_is() would also match branches sharing the prefix
        group = codebuild.FilterGroup.in_event_of(
            codebuild.EventAction.PUSH
        ).and_head_ref_is(f"^refs/heads/{re.escape(spec.branch)}$")
        if spec.include_paths:
            group = group.and_file_path_is(self._prefix_pattern(spec.include_paths))
        if spec.exclude_paths:
            group = group.and_file_path_is_not(self._prefix_pattern(spec.exclude_paths))
        return [group]

    def _create_image_build(self, spec: ImageBuildSpec) -> None:
        """Create the ECR repository and webhook-triggered CodeBuild project of one entry"""
        repo = ecr.Repository(
            self,
            f"ImageRepository-{spec.id}",
            repository_name=f"{self.config.namespace}-{spec.repository}",
            removal_policy=RemovalPolicy.DESTROY,
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

        project = codebuild.Project(
            self,
            f"ImageBuildProject-{spec.id}",
            project_name=f"{self.config.namespace}-{spec.id}-image-build",
            source=codebuild.Source.git_hub(
                owner=self.config.github_owner,
                repo=self.config.github_repo,
                webhook=True,
                webhook_filters=self._webhook_filters(spec),
            ),
            environment=codebuild.BuildEnvironment(
                privileged=True,  # Required for container image builds
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                compute_type=codebuild.ComputeType[spec.compute_type],
            ),
            cache=codebuild.Cache.local(codebuild.LocalCacheMode.DOCKER_LAYER),
            timeout=Duration.minutes(spec.timeout_minutes),
            environment_variables={
                "ECR_REPOSITORY_URI": codebuild.BuildEnvironmentVariable(
                    value=repo.repository_uri
                ),
                "DOCKERFILE": codebuild.BuildEnvironmentVariable(value=spec.dockerfile),
                "BUILD_CONTEXT": codebuild.BuildEnvironmentVariable(value=spec.context),
            },
            build_spec=codebuild.BuildSpec.from_object(
                {
                    "version": "0.2",
                    "phases": {
                        "pre_build": {
                            "commands": [
                                # CODE_VERSION = first 12 characters of the commit, the same
                                # value the job planners compute from their checkout
                                "CODE_VERSION=$(echo $CODEBUILD_RESOLVED_SOURCE_VERSION | cut -c 1-12)",
                                "aws ecr get-login-password --region $AWS_DEFAULT_REGION | docker login --username AWS --password-stdin $ECR_REPOSITORY_URI",
                            ]
                        },
                        "build": {
                            "commands": [
                                spec.docker_build_command(),
                                "docker tag $ECR_REPOSITORY_URI:$CODE_VERSION $ECR_REPOSITORY_URI:latest",
                            ]
                        },
                        "post_build": {
                            "commands": [
                                "docker push $ECR_REPOSITORY_URI:$CODE_VERSION",
                                "docker push $ECR_REPOSITORY_URI:latest",
                            ]
                        },
                    },
                }
            ),
        )
        repo.grant_pull_push(project)

        self.image_repos[spec.id] = repo
        self.image_build_projects[spec.id] = project

    def _create_build_project(self) -> codebuild.PipelineProject:
        """Create CodeBuild project for container image build"""
        build_project = codebuild.PipelineProject(
            self,
            "ContainerImageBuildProject",
            project_name=f"{self.config.namespace}-image-build",
            environment=codebuild.BuildEnvironment(
                privileged=True,  # Required for container image builds
                build_image=codebuild.LinuxBuildImage.STANDARD_7_0,
                compute_type=codebuild.ComputeType.MEDIUM,  # Faster builds with more CPU/memory
            ),
            cache=codebuild.Cache.local(
                codebuild.LocalCacheMode.DOCKER_LAYER, codebuild.LocalCacheMode.CUSTOM
            ),
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
                                "COMMIT_HASH=$(echo $CODEBUILD_RESOLVED_SOURCE_VERSION | cut -c 1-7)",
                                "IMAGE_TAG=${COMMIT_HASH:=latest}",
                            ]
                        },
                        "build": {
                            "commands": [
                                "echo Build started on `date`",
                                "echo Building the Docker image...",
                                "aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 763104351884.dkr.ecr.us-east-1.amazonaws.com",
                                "docker build -t $ECR_REPOSITORY_URI:$IMAGE_TAG .",
                                "docker tag $ECR_REPOSITORY_URI:$IMAGE_TAG $ECR_REPOSITORY_URI:latest",
                            ]
                        },
                        "post_build": {
                            "commands": [
                                "echo Build completed on `date`",
                                "echo Pushing the Docker image...",
                                "aws ecr get-login-password --region $AWS_DEFAULT_REGION | docker login --username AWS --password-stdin $ECR_REPOSITORY_URI",
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

        # Add permissions to access AWS Deep Learning Containers
        build_project.add_to_role_policy(
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchGetImage",
                ],
                resources=[
                    "arn:aws:ecr:us-east-1:763104351884:repository/pytorch-training"
                ],
            )
        )

        return build_project

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
