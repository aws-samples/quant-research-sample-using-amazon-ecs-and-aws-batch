# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Everything the GPU fleet builds in ONE region: VPC, templates, queues, job definitions,
the weight replica and the raw-EC2 terminator. One stack per region, so a deploy that fails in one region
(a quota, an AZ that stopped selling a type) rolls back that region only."""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from aws_cdk import Stack
from constructs import Construct

from . import catalogue as cat
from .fleet import GpuFleet, GpuFleetConfig
from .jobs import FleetJobDefinitions, image_uri
from .network import FleetNetwork, FleetNetworkConfig
from .self_managed import SelfManagedNodes
from .weights import WeightReplica


@dataclass
class GpuRegionConfig:
    namespace: str
    home_region: str
    prefix: str
    instance_types: List[str]
    offerings: dict
    vpc_cidr: str
    #: arch -> ECR repository of the training images (sft job definitions)
    repositories: Dict[str, str]
    #: arch -> ECR repository of the fleet runtime image (bench job definitions); both kinds
    #: are replicated from the home region
    runtime_repositories: Dict[str, str]
    instance_profile_name: str
    job_role_name: str
    execution_role_name: str
    #: source bucket of the base-model weights; every region but the home one gets
    #: `<weight_bucket>-<region>`. None: no weight buckets
    weight_bucket: Optional[str] = None
    nat_az_id: Optional[str] = None
    egress: bool = True
    exclude_az_ids: Tuple[str, ...] = ()
    raw_ec2: bool = True
    bench_job_definitions: bool = True
    training_job_definitions: bool = True
    priority: int = 100
    max_node_hours: int = 24
    tags: Dict[str, str] = field(default_factory=dict)


class GpuFleetRegionStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, config: GpuRegionConfig,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        c = config
        region = self.region
        snap = c.offerings.get(region)
        if snap is None:
            raise ValueError(f"{region} is not in the offerings snapshot; run "
                             f"gpu_fleet/refresh_offerings.py --regions {region}")
        az_ids = cat.region_az_ids(region, c.offerings, c.exclude_az_ids)
        self.network = FleetNetwork(self, "Network", config=FleetNetworkConfig(
            region=region, name=f"{c.namespace}-gpu", vpc_cidr=c.vpc_cidr, az_ids=az_ids,
            endpoint_az_ids=[a for a in snap.get("endpoint_az_ids", []) if a in az_ids],
            nat_az_id=c.nat_az_id, egress=c.egress))

        acct, part = self.account, self.partition
        self.fleet = GpuFleet(self, "Fleet", network=self.network, config=GpuFleetConfig(
            region=region, prefix=c.prefix, instance_types=c.instance_types,
            offerings=c.offerings, instance_profile_name=c.instance_profile_name,
            instance_profile_arn=f"arn:{part}:iam::{acct}:instance-profile/{c.instance_profile_name}",
            raw_ec2=c.raw_ec2, priority=c.priority, exclude_az_ids=c.exclude_az_ids,
            tags=c.tags))

        if c.bench_job_definitions or c.training_job_definitions:
            FleetJobDefinitions(
                self, "JobDefinitions", fleet=self.fleet, prefix=c.prefix,
                images={arch: image_uri(self, repo) for arch, repo in c.repositories.items()},
                bench_images={arch: image_uri(self, repo)
                              for arch, repo in c.runtime_repositories.items()},
                weight_bucket=c.weight_bucket,
                job_role_arn=f"arn:{part}:iam::{acct}:role/{c.job_role_name}",
                execution_role_arn=f"arn:{part}:iam::{acct}:role/{c.execution_role_name}",
                home_region=c.home_region, bench=c.bench_job_definitions,
                training=c.training_job_definitions, tags=c.tags)

        if c.weight_bucket and region != c.home_region:
            self.weight_replica = WeightReplica(self, "WeightReplica", source_bucket=c.weight_bucket)

        if c.raw_ec2:
            SelfManagedNodes(self, "SelfManaged",
                             log_group_name=f"/{c.namespace}/gpu-fleet/nodes",
                             max_node_hours=c.max_node_hours)
