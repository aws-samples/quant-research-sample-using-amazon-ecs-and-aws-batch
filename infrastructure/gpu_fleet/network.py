# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""The fleet VPC of one region: what a multi-card GPU node needs to run at all.

A launch template with several network cards cannot take a public IP (RunInstances rejects
AssociatePublicIpAddress with multiple interfaces), so such a node reaches nothing outside
the VPC unless the VPC routes it privately:

  S3 gateway endpoint       weights, checkpoints, image layers (no per-GB charge, no ENI cap)
  ECR api + dkr             docker pull
  ECS, ECS agent/telemetry  the node registers with the Batch-managed ECS cluster
  CloudWatch Logs           job logs
  SSM, SSM/EC2 messages     the raw-EC2 path drives docker over SSM
  NAT gateway               everything else, including another region's S3

Without the endpoints a multi-card Batch node boots, never registers a container instance,
and its job sits RUNNABLE, which reads exactly like "no spot capacity". Without the NAT an
off-region request is silently DROPPED (the IGW only translates for instances that have a
public IP), so it presents as a connect that never completes, not a refusal.

Layout: one public and one private /20 per AZ, addressed by AZ ID. One-card nodes launch
into the public subnets and reach the world through the IGW for free; multi-card nodes
launch into the private ones. ONE NAT gateway per region, not one per AZ: the NAT path is
for reach, the S3 gateway endpoint is for bytes, and a cross-AZ hop costs less than idle
gateways.

Built from L1 resources because the AZ IDs come from the offerings snapshot: synth never
looks anything up, and a subnet's AZ ID names the same physical zone in every account.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from aws_cdk import CfnOutput, aws_ec2 as ec2
from constructs import Construct

from . import catalogue


@dataclass
class FleetNetworkConfig:
    region: str
    name: str
    vpc_cidr: str
    az_ids: List[str]
    endpoint_az_ids: List[str]
    #: AZ ID of the one NAT gateway. The NAT gateway limit (5) is PER AZ, so an AZ already full
    #: of other projects' gateways fails the deploy; name a different one. Declared, never
    #: picked from a live count: moving a NAT gateway is a replacement that interrupts egress.
    nat_az_id: Optional[str] = None
    #: False where the region's Elastic IP quota is exhausted. Without the opt-out the stack is
    #: undeployable there, not merely NAT-less. Multi-card nodes then use the public subnets
    #: and have no off-region path.
    egress: bool = True
    interface_endpoints: List[str] = field(default_factory=lambda: list(catalogue.INTERFACE_ENDPOINTS))


class FleetNetwork(Construct):
    def __init__(self, scope: Construct, construct_id: str, *, config: FleetNetworkConfig) -> None:
        super().__init__(scope, construct_id)
        c = config
        if not c.az_ids:
            raise ValueError(f"{c.region}: no AZ IDs to build subnets in")
        az_ids = sorted(c.az_ids)
        nat_az = c.nat_az_id or az_ids[0]
        if c.egress and nat_az not in az_ids:
            raise ValueError(f"{c.region}: nat_az_id {nat_az!r} is not one of {az_ids}")

        def tags(suffix: str):
            return [{"key": "Name", "value": f"{c.name}-{suffix}"}]

        self.vpc = ec2.CfnVPC(self, "Vpc", cidr_block=c.vpc_cidr, enable_dns_support=True,
                              enable_dns_hostnames=True, tags=tags("vpc"))
        igw = ec2.CfnInternetGateway(self, "Igw", tags=tags("igw"))
        attach = ec2.CfnVPCGatewayAttachment(self, "IgwAttachment", vpc_id=self.vpc.ref,
                                             internet_gateway_id=igw.ref)

        public_rt = ec2.CfnRouteTable(self, "PublicRouteTable", vpc_id=self.vpc.ref,
                                      tags=tags("public"))
        public_default = ec2.CfnRoute(self, "PublicDefaultRoute", route_table_id=public_rt.ref,
                                      destination_cidr_block="0.0.0.0/0", gateway_id=igw.ref)
        public_default.add_dependency(attach)
        # ONE route table for every private subnet: they all want the same two routes
        private_rt = ec2.CfnRouteTable(self, "PrivateRouteTable", vpc_id=self.vpc.ref,
                                       tags=tags("private"))

        #: az id -> subnet. The CIDR index is the position in the SORTED id list, so adding
        #: an AZ never moves an existing subnet (a changed CIDR is a replacement).
        self.public_subnets: Dict[str, ec2.CfnSubnet] = {}
        self.private_subnets: Dict[str, ec2.CfnSubnet] = {}
        for i, az in enumerate(az_ids):
            pub_cidr, priv_cidr = catalogue.subnet_cidrs(c.vpc_cidr, i)
            key = az.replace("-", "").title()
            pub = ec2.CfnSubnet(self, f"Public{key}", vpc_id=self.vpc.ref,
                                availability_zone_id=az, cidr_block=pub_cidr,
                                map_public_ip_on_launch=True, tags=tags(f"public-{az}"))
            ec2.CfnSubnetRouteTableAssociation(self, f"PublicRta{key}", subnet_id=pub.ref,
                                               route_table_id=public_rt.ref)
            priv = ec2.CfnSubnet(self, f"Private{key}", vpc_id=self.vpc.ref,
                                 availability_zone_id=az, cidr_block=priv_cidr,
                                 map_public_ip_on_launch=False, tags=tags(f"private-{az}"))
            ec2.CfnSubnetRouteTableAssociation(self, f"PrivateRta{key}", subnet_id=priv.ref,
                                               route_table_id=private_rt.ref)
            self.public_subnets[az] = pub
            self.private_subnets[az] = priv

        self.nat = None
        if c.egress:
            eip = ec2.CfnEIP(self, "NatEip", domain="vpc", tags=tags("nat"))
            eip.add_dependency(attach)
            self.nat = ec2.CfnNatGateway(self, "NatGateway",
                                         subnet_id=self.public_subnets[nat_az].ref,
                                         allocation_id=eip.attr_allocation_id, tags=tags("nat"))
            ec2.CfnRoute(self, "PrivateDefaultRoute", route_table_id=private_rt.ref,
                         destination_cidr_block="0.0.0.0/0", nat_gateway_id=self.nat.ref)

        # EFA needs all traffic between fabric members in both directions
        self.node_sg = ec2.CfnSecurityGroup(
            self, "NodeSecurityGroup", vpc_id=self.vpc.ref, group_name=f"{c.name}-efa",
            group_description="GPU nodes: EFA fabric (all traffic between members), egress to the world",
            security_group_egress=[ec2.CfnSecurityGroup.EgressProperty(
                ip_protocol="-1", cidr_ip="0.0.0.0/0", description="all egress")],
            tags=tags("efa"))
        ec2.CfnSecurityGroupIngress(self, "NodeSelfIngress", group_id=self.node_sg.attr_group_id,
                                    ip_protocol="-1",
                                    source_security_group_id=self.node_sg.attr_group_id,
                                    description="EFA: all traffic between fabric members")
        ec2.CfnSecurityGroupEgress(self, "NodeSelfEgress", group_id=self.node_sg.attr_group_id,
                                   ip_protocol="-1",
                                   destination_security_group_id=self.node_sg.attr_group_id,
                                   description="EFA: all traffic between fabric members")
        endpoint_sg = ec2.CfnSecurityGroup(
            self, "EndpointSecurityGroup", vpc_id=self.vpc.ref, group_name=f"{c.name}-endpoints",
            group_description="GPU fleet interface endpoints: 443 from the VPC",
            security_group_ingress=[ec2.CfnSecurityGroup.IngressProperty(
                ip_protocol="tcp", from_port=443, to_port=443, cidr_ip=c.vpc_cidr,
                description="HTTPS from every subnet of the VPC")],
            security_group_egress=[ec2.CfnSecurityGroup.EgressProperty(
                ip_protocol="icmp", from_port=252, to_port=86, cidr_ip="255.255.255.255/32",
                description="no egress")],
            tags=tags("endpoints"))

        # GATEWAY for S3, associated with BOTH route tables: without the private one, a private
        # node's in-region S3 traffic takes the NAT and is billed per GB for a weight pull that
        # should be free.
        ec2.CfnVPCEndpoint(self, "S3Gateway", vpc_id=self.vpc.ref,
                           service_name=f"com.amazonaws.{c.region}.s3",
                           vpc_endpoint_type="Gateway",
                           route_table_ids=[public_rt.ref, private_rt.ref])

        # some AZs host no endpoint for some services; the snapshot records the ones that host all
        ep_subnets = [self.private_subnets[a].ref for a in az_ids if a in set(c.endpoint_az_ids)]
        if not ep_subnets:
            raise ValueError(f"{c.region}: no AZ hosts every interface endpoint service")
        for svc in c.interface_endpoints:
            ec2.CfnVPCEndpoint(self, "Ep" + svc.replace(".", "-").title().replace("-", ""),
                               vpc_id=self.vpc.ref,
                               service_name=f"com.amazonaws.{c.region}.{svc}",
                               vpc_endpoint_type="Interface", private_dns_enabled=True,
                               subnet_ids=ep_subnets,
                               security_group_ids=[endpoint_sg.attr_group_id])

        self.egress = c.egress

        CfnOutput(self, "VpcId", value=self.vpc.ref)
        CfnOutput(self, "NodeSecurityGroupId", value=self.node_sg.attr_group_id)
        if self.nat is not None:
            CfnOutput(self, "NatGatewayId", value=self.nat.ref)
        else:
            CfnOutput(self, "NoEgress", value="egress opted out: multi-card nodes use the "
                                              "public subnets and have no off-region path")

    def subnets_in(self, cards: int, az_ids) -> List[str]:
        """Subnet ids a shape launches into, in the AZ IDs that offer its type: private for
        multi-card (no public IP possible) when the region has egress, public otherwise."""
        pool = self.private_subnets if (cards > 1 and self.egress) else self.public_subnets
        return [pool[a].ref for a in sorted(az_ids) if a in pool]
