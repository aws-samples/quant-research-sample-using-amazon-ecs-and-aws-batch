# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json

import aws_cdk as core
from aws_cdk.assertions import Match, Template

from infrastructure.common.pipeline import (
    DeploymentPipelineStack,
    ImageBuildSpec,
    PipelineConfig,
)


def _template(image_builds, register_github_credentials=True, enable_code_pipeline=False):
    app = core.App()
    env = core.Environment(account="012345678901", region="us-east-1")
    config = PipelineConfig(
        namespace="test-ns",
        github_owner="github_owner",
        github_repo="github_repo",
        github_branch="main",
        github_token_secret_name="github-token",
        enable_code_pipeline=enable_code_pipeline,
        image_builds=image_builds,
        register_github_credentials=register_github_credentials,
    )
    stack = DeploymentPipelineStack(app, "infrastructure", env=env, config=config)
    return stack, Template.from_stack(stack)


BUILD = ImageBuildSpec(
    id="earnings-research",
    repository="earnings-research",
    dockerfile="samples/earnings_research/Dockerfile",
    context="samples/earnings_research",
    branch="main",
    include_paths=["samples/earnings_research/"],
    exclude_paths=["samples/earnings_research/viewer/"],
)


def test_default_mode_unchanged():
    _, template = _template([])
    template.resource_count_is("AWS::ECR::Repository", 1)
    template.resource_count_is("AWS::CodeBuild::Project", 0)
    template.resource_count_is("AWS::CodeBuild::SourceCredential", 0)
    template.resource_count_is("AWS::CodePipeline::Pipeline", 0)


def test_webhook_build_per_entry_with_its_own_repository():
    stack, template = _template([BUILD])
    template.resource_count_is("AWS::ECR::Repository", 2)
    template.has_resource_properties(
        "AWS::ECR::Repository", {"RepositoryName": "test-ns-earnings-research"}
    )
    template.resource_count_is("AWS::CodeBuild::Project", 1)
    template.resource_count_is("AWS::CodePipeline::Pipeline", 0)
    assert set(stack.image_repos) == {"earnings-research"}


def test_webhook_filter_group_matches_configuration():
    _, template = _template([BUILD])
    template.has_resource_properties(
        "AWS::CodeBuild::Project",
        {
            "Source": Match.object_like(
                {"Type": "GITHUB", "Location": "https://github.com/github_owner/github_repo.git"}
            ),
            "Triggers": {
                "Webhook": True,
                "FilterGroups": [
                    [
                        {"Type": "EVENT", "Pattern": "PUSH"},
                        {"Type": "HEAD_REF", "Pattern": "^refs/heads/main$"},
                        {"Type": "FILE_PATH", "Pattern": "^(samples/earnings_research/)"},
                        {
                            "Type": "FILE_PATH",
                            "Pattern": "^(samples/earnings_research/viewer/)",
                            "ExcludeMatchedPattern": True,
                        },
                    ]
                ],
            },
        },
    )


def test_build_passes_dockerfile_context_and_code_version():
    _, template = _template([BUILD])
    project = next(iter(template.find_resources("AWS::CodeBuild::Project").values()))
    spec = json.loads(project["Properties"]["Source"]["BuildSpec"])
    commands = " ".join(
        c for phase in spec["phases"].values() for c in phase["commands"]
    )
    assert "cut -c 1-12" in commands
    assert "--build-arg CODE_VERSION=$CODE_VERSION" in commands
    assert "-f $DOCKERFILE" in commands and "$BUILD_CONTEXT" in commands
    assert "$ECR_REPOSITORY_URI:latest" in commands
    env = {
        v["Name"]: v["Value"]
        for v in project["Properties"]["Environment"]["EnvironmentVariables"]
    }
    assert env["DOCKERFILE"] == "samples/earnings_research/Dockerfile"
    assert env["BUILD_CONTEXT"] == "samples/earnings_research"


def test_github_credentials_registered_once_and_optional():
    second = ImageBuildSpec(
        id="custom", repository="custom", dockerfile="Dockerfile", context=".",
        branch="main", include_paths=["src/"],
    )
    _, template = _template([BUILD, second])
    template.resource_count_is("AWS::CodeBuild::SourceCredential", 1)
    template.resource_count_is("AWS::CodeBuild::Project", 2)
    _, template = _template([BUILD], register_github_credentials=False)
    template.resource_count_is("AWS::CodeBuild::SourceCredential", 0)


def test_both_modes_coexist():
    _, template = _template([BUILD], enable_code_pipeline=True)
    template.resource_count_is("AWS::CodeBuild::Project", 2)
    template.resource_count_is("AWS::CodePipeline::Pipeline", 1)
