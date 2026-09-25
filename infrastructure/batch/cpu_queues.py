# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from aws_cdk import (
    CfnOutput,
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_iam as iam,
)
from constructs import Construct


@dataclass
class CpuQueueSpec:
    """One entry of batch.queues[] in parameters.json: one EC2 compute environment + one job queue"""

    id: str
    name: str
    maxv_cpus: int
    instance_classes: List[str] = field(default_factory=lambda: ["C7I", "M7I", "R7I"])
    spot: bool = True
    egress: bool = False
    minv_cpus: int = 0
    allocation_strategy: Optional[str] = None

    @classmethod
    def from_params(cls, entry) -> "CpuQueueSpec":
        """Build a spec from a parameters.json entry (SimpleNamespace or dict)"""
        values = entry if isinstance(entry, dict) else vars(entry)
        known = {k: v for k, v in values.items() if k in cls.__dataclass_fields__}
        return cls(**known)


class CpuQueues(Construct):
    """
    Deploys exactly the queues listed in the configuration: each entry gets its own
    EC2 compute environment (so maxv_cpus is a hard cap for that queue) and job queue.
    Entries with egress=True are placed on PRIVATE_WITH_EGRESS subnets (NAT), the
    others on PRIVATE_ISOLATED subnets.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        namespace: str,
        vpc: ec2.IVpc,
        security_group: ec2.ISecurityGroup,
        instance_role: iam.IRole,
        queues: List[CpuQueueSpec],
    ) -> None:
        super().__init__(scope, construct_id)

        self.namespace = namespace
        self.job_queues: Dict[str, batch.JobQueue] = {}
        self.compute_environments: Dict[str, batch.ManagedEc2EcsComputeEnvironment] = {}

        ids = [q.id for q in queues]
        duplicates = sorted({i for i in ids if ids.count(i) > 1})
        if duplicates:
            raise ValueError(f"duplicate queue ids in batch.queues: {duplicates}")

        if not queues:
            return

        self.launch_template = self.build_launch_template()

        for spec in queues:
            self.build_queue(spec, vpc, security_group, instance_role)

    def build_queue(
        self,
        spec: CpuQueueSpec,
        vpc: ec2.IVpc,
        security_group: ec2.ISecurityGroup,
        instance_role: iam.IRole,
    ) -> None:
        """Creates the compute environment and job queue of one entry"""
        name = f"{self.namespace}-{spec.name}"
        allocation_strategy = spec.allocation_strategy or (
            "SPOT_PRICE_CAPACITY_OPTIMIZED" if spec.spot else "BEST_FIT_PROGRESSIVE"
        )

        compute_environment = batch.ManagedEc2EcsComputeEnvironment(
            self,
            f"ComputeEnvironment-{spec.id}",
            compute_environment_name=name,
            instance_role=instance_role,
            launch_template=self.launch_template,
            instance_classes=[ec2.InstanceClass(c) for c in spec.instance_classes],
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=(
                    ec2.SubnetType.PRIVATE_WITH_EGRESS
                    if spec.egress
                    else ec2.SubnetType.PRIVATE_ISOLATED
                )
            ),
            vpc=vpc,
            allocation_strategy=batch.AllocationStrategy(allocation_strategy),
            spot=spec.spot,
            update_to_latest_image_version=True,
            use_optimal_instance_classes=False,
            security_groups=[security_group],
            minv_cpus=spec.minv_cpus,
            maxv_cpus=spec.maxv_cpus,
        )

        job_queue = batch.JobQueue(
            self,
            f"JobQueue-{spec.id}",
            job_queue_name=name,
            compute_environments=[
                batch.OrderedComputeEnvironment(
                    compute_environment=compute_environment, order=1
                )
            ],
        )

        self.compute_environments[spec.id] = compute_environment
        self.job_queues[spec.id] = job_queue

        CfnOutput(
            self,
            f"JobQueueName-{spec.id}",
            value=job_queue.job_queue_name,
            description=f"Job queue for batch.queues entry '{spec.id}'",
        )

    def build_launch_template(self) -> ec2.LaunchTemplate:
        """
        Creates the EC2 launch template shared by all queue compute environments.
        """
        return ec2.LaunchTemplate(
            self,
            "LaunchTemplate",
            launch_template_name=f"{self.namespace}-cpu-queues",
            machine_image=ec2.MachineImage.from_ssm_parameter(
                "/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id"
            ),
            detailed_monitoring=True,
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",  # Root device
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size=100,
                        volume_type=ec2.EbsDeviceVolumeType.GP3,
                        encrypted=True,
                        delete_on_termination=True,
                    ),
                )
            ],
        )
