# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import aws_cdk as core
from aws_cdk import aws_ec2 as ec2, aws_iam as iam
from aws_cdk.assertions import Match, Template

from infrastructure.batch.cpu_queues import CpuQueueSpec, CpuQueues


def _template(queues) -> Template:
    app = core.App()
    env = core.Environment(account="012345678901", region="us-east-1")
    stack = core.Stack(app, "infrastructure", env=env)
    vpc = ec2.Vpc(
        stack,
        "VPC",
        availability_zones=["us-east-1a"],
        nat_gateways=1,
        subnet_configuration=[
            ec2.SubnetConfiguration(
                name="Private", subnet_type=ec2.SubnetType.PRIVATE_ISOLATED, cidr_mask=17
            ),
            ec2.SubnetConfiguration(
                name="PrivateEgress",
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                cidr_mask=18,
            ),
            ec2.SubnetConfiguration(
                name="Public", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=24
            ),
        ],
    )
    security_group = ec2.SecurityGroup(stack, "SG", vpc=vpc)
    instance_role = iam.Role(
        stack, "InstanceRole", assumed_by=iam.ServicePrincipal("ec2.amazonaws.com")
    )
    CpuQueues(
        stack,
        "CpuQueues",
        namespace="test-ns",
        vpc=vpc,
        security_group=security_group,
        instance_role=instance_role,
        queues=queues,
    )
    return Template.from_stack(stack)


QUEUES = [
    CpuQueueSpec(
        id="cpu",
        name="cpu",
        maxv_cpus=4000,
        instance_classes=["C7I", "M7I"],
        spot=True,
        egress=True,
    ),
    CpuQueueSpec(
        id="capped",
        name="capped",
        maxv_cpus=24,
        instance_classes=["C7I"],
        spot=False,
        egress=False,
    ),
]


def test_one_compute_environment_and_queue_per_entry():
    template = _template(QUEUES)
    template.resource_count_is("AWS::Batch::ComputeEnvironment", 2)
    template.resource_count_is("AWS::Batch::JobQueue", 2)
    template.has_resource_properties(
        "AWS::Batch::JobQueue", {"JobQueueName": "test-ns-cpu"}
    )
    template.has_resource_properties(
        "AWS::Batch::JobQueue", {"JobQueueName": "test-ns-capped"}
    )


def test_caps_and_pricing_follow_the_entry():
    template = _template(QUEUES)
    template.has_resource_properties(
        "AWS::Batch::ComputeEnvironment",
        {
            "ComputeEnvironmentName": "test-ns-cpu",
            "ComputeResources": Match.object_like(
                {"MaxvCpus": 4000, "Type": "SPOT"}
            ),
        },
    )
    template.has_resource_properties(
        "AWS::Batch::ComputeEnvironment",
        {
            "ComputeEnvironmentName": "test-ns-capped",
            "ComputeResources": Match.object_like({"MaxvCpus": 24, "Type": "EC2"}),
        },
    )


def test_egress_entry_uses_private_with_egress_subnets():
    template = _template(QUEUES)
    resources = template.find_resources("AWS::Batch::ComputeEnvironment")
    subnets = {
        props["Properties"]["ComputeEnvironmentName"]: props["Properties"][
            "ComputeResources"
        ]["Subnets"]
        for props in resources.values()
    }
    assert subnets["test-ns-cpu"] != subnets["test-ns-capped"]
    assert "PrivateEgress" in str(subnets["test-ns-cpu"])
    assert "Private" in str(subnets["test-ns-capped"])
    assert "PrivateEgress" not in str(subnets["test-ns-capped"])


def test_empty_list_deploys_nothing():
    template = _template([])
    template.resource_count_is("AWS::Batch::ComputeEnvironment", 0)
    template.resource_count_is("AWS::Batch::JobQueue", 0)
    template.resource_count_is("AWS::EC2::LaunchTemplate", 0)


def test_duplicate_ids_are_rejected():
    try:
        _template([QUEUES[0], QUEUES[0]])
    except ValueError as err:
        assert "cpu" in str(err)
    else:
        raise AssertionError("duplicate queue ids must raise")
