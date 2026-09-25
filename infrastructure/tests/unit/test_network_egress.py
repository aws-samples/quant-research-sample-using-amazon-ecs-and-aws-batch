# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import aws_cdk as core
from aws_cdk.assertions import Template

from infrastructure.common.network import NetworkStack


def _template(**kwargs) -> Template:
    app = core.App()
    env = core.Environment(account="012345678901", region="us-east-1")
    stack = NetworkStack(
        app,
        "infrastructure",
        env=env,
        namespace="test-quant-research",
        availability_zone="us-east-1a",
        with_s3express=False,
        **kwargs,
    )
    return Template.from_stack(stack)


def test_default_network_has_no_egress():
    template = _template()
    template.resource_count_is("AWS::EC2::NatGateway", 0)
    template.resource_count_is("AWS::EC2::InternetGateway", 0)
    template.resource_count_is("AWS::EC2::Subnet", 1)


def test_nat_gateway_adds_public_and_egress_subnets():
    template = _template(nat_gateways=1)
    template.resource_count_is("AWS::EC2::NatGateway", 1)
    template.resource_count_is("AWS::EC2::InternetGateway", 1)
    # isolated subnet kept for existing consumers + public + private-with-egress
    template.resource_count_is("AWS::EC2::Subnet", 3)
