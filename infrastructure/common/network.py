# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from typing import List, Dict

from aws_cdk import Stack, aws_ec2 as ec2, Environment
from constructs import Construct


class NetworkStack(Stack):
    """
    Stack for creating a VPC with various VPC endpoints and security groups.
    """

    # Define constants for VPC configuration
    DEFAULT_CIDR = "172.0.0.0/16"
    DEFAULT_SUBNET_MASK = 16
    # Subnet masks used when NAT egress is enabled (isolated subnet kept for existing consumers)
    EGRESS_ISOLATED_MASK = 17
    EGRESS_PRIVATE_MASK = 18
    EGRESS_PUBLIC_MASK = 24

    # Define required interface endpoints
    REQUIRED_ENDPOINTS = {
        "ECR": ec2.InterfaceVpcEndpointAwsService.ECR,
        "ECRDocker": ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER,
        "ECS": ec2.InterfaceVpcEndpointAwsService.ECS,
        "ECSAgent": ec2.InterfaceVpcEndpointAwsService.ECS_AGENT,
        "ECSTelemetry": ec2.InterfaceVpcEndpointAwsService.ECS_TELEMETRY,
        "FSxLustre": ec2.InterfaceVpcEndpointAwsService.FSX,
        "Logs": ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
        "SSM": ec2.InterfaceVpcEndpointAwsService.SSM,
        "SSMMessages": ec2.InterfaceVpcEndpointAwsService.SSM_MESSAGES,
        "EC2Messages": ec2.InterfaceVpcEndpointAwsService.EC2_MESSAGES,
        "EC2": ec2.InterfaceVpcEndpointAwsService.EC2,
        "KMS": ec2.InterfaceVpcEndpointAwsService.KMS,
        "Batch": ec2.InterfaceVpcEndpointAwsService.BATCH,
        "Glue": ec2.InterfaceVpcEndpointAwsService.GLUE,
    }

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        env: Environment,
        namespace: str,
        availability_zone: str,
        with_s3express: bool = True,
        nat_gateways: int = 0,
        **kwargs,
    ) -> None:
        """
        Initialize NetworkStack.

        Args:
            scope: CDK Construct scope
            construct_id: Unique identifier for the stack
            env: Deployment environment for the stack
            namespace: Namespace for resource naming
            availability_zone: AZ where resources will be deployed
            with_s3express: Whether to include S3 Express endpoint
            nat_gateways: NAT gateways to create; > 0 adds a public subnet and a
                PRIVATE_WITH_EGRESS subnet next to the isolated one (default 0: no egress)
        """
        super().__init__(scope, construct_id, env=env, **kwargs)

        # Create VPC and related resources
        self.vpc = self._create_vpc(
            namespace, availability_zone, with_s3express, nat_gateways
        )
        self.security_group = self._create_security_group(namespace)
        self.interface_endpoints = self._create_interface_endpoints()

    def _create_gateway_endpoints(self, with_s3express: bool) -> Dict:
        """Create gateway endpoint configurations."""
        endpoints = {
            "S3": ec2.GatewayVpcEndpointOptions(
                service=ec2.GatewayVpcEndpointAwsService.S3
            ),
        }

        if with_s3express:
            endpoints["S3Express"] = ec2.GatewayVpcEndpointOptions(
                service=ec2.GatewayVpcEndpointAwsService.S3_EXPRESS
            )

        return endpoints

    def _create_subnet_configuration(
        self, nat_gateways: int
    ) -> List[ec2.SubnetConfiguration]:
        """Isolated subnet only, or isolated + private-with-egress + public when NAT is on."""
        if nat_gateways <= 0:
            return [
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=self.DEFAULT_SUBNET_MASK,
                )
            ]
        return [
            ec2.SubnetConfiguration(
                name="Private",
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                cidr_mask=self.EGRESS_ISOLATED_MASK,
            ),
            ec2.SubnetConfiguration(
                name="PrivateEgress",
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                cidr_mask=self.EGRESS_PRIVATE_MASK,
            ),
            ec2.SubnetConfiguration(
                name="Public",
                subnet_type=ec2.SubnetType.PUBLIC,
                cidr_mask=self.EGRESS_PUBLIC_MASK,
            ),
        ]

    def _create_vpc(
        self,
        namespace: str,
        availability_zone: str,
        with_s3express: bool,
        nat_gateways: int = 0,
    ) -> ec2.Vpc:
        """Create VPC with specified configuration."""
        with_egress = nat_gateways > 0
        return ec2.Vpc(
            self,
            "VPC",
            vpc_name=f"{namespace}-vpc",
            ip_addresses=ec2.IpAddresses.cidr(self.DEFAULT_CIDR),
            availability_zones=[availability_zone],
            nat_gateways=max(nat_gateways, 0),
            create_internet_gateway=with_egress,
            enable_dns_hostnames=True,
            enable_dns_support=True,
            subnet_configuration=self._create_subnet_configuration(nat_gateways),
            gateway_endpoints=self._create_gateway_endpoints(with_s3express),
        )

    def _create_security_group(self, namespace: str) -> ec2.SecurityGroup:
        """Create security group with internal traffic rules."""
        security_group = ec2.SecurityGroup(
            self,
            "SecurityGroup",
            security_group_name=f"{namespace}-sg",
            vpc=self.vpc,
            allow_all_outbound=True,
            description="Security group for internal traffic",
        )

        security_group.add_ingress_rule(
            peer=security_group,
            connection=ec2.Port.all_tcp(),
            description="Allow internal TCP traffic",
        )

        return security_group

    def _create_interface_endpoints(self) -> List[ec2.InterfaceVpcEndpoint]:
        """Create all required interface endpoints."""
        endpoints = []

        for name, service in self.REQUIRED_ENDPOINTS.items():
            endpoint = ec2.InterfaceVpcEndpoint(
                self,
                f"{name}InterfaceEndpoint",
                service=service,
                vpc=self.vpc,
                security_groups=[self.security_group],
            )
            endpoints.append(endpoint)

        return endpoints

    def add_interface_endpoint(
        self, name: str, service: ec2.InterfaceVpcEndpointAwsService
    ) -> ec2.InterfaceVpcEndpoint:
        """
        Add a new interface endpoint to the VPC.

        Args:
            name: Name of the endpoint
            service: AWS service for the endpoint
        """
        endpoint = ec2.InterfaceVpcEndpoint(
            self,
            f"{name}InterfaceEndpoint",
            service=service,
            vpc=self.vpc,
            security_groups=[self.security_group],
        )
        self.interface_endpoints.append(endpoint)
        return endpoint
