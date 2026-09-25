# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Launch template + spot compute environment + job queue per selected instance type.

One queue per TYPE, not per AZ and not per family. GPU spot capacity is per AZ, and the
compute environment lists a subnet in every AZ that offers the type, so Batch itself places
into whichever AZ has capacity at submit time. A caller cannot know the AZ in advance; it
can know the hardware it wants.

Four states per type and region (catalogue.region_state, from the offerings snapshot):

  enabled        LT + CE + ENABLED queue
  disabled       LT + CE + DISABLED queue: Batch knows the type, no AZ here sells it
  template-only  LT only, when raw_ec2 is on: EC2 sells it, Batch does not accept it here
  absent         nothing

Subnets are the AZs that OFFER the type. Batch does not validate this: a compute
environment naming a type no listed AZ offers settles VALID and its jobs stay RUNNABLE.

maxvCpus is the vCPU count of one node: a ceiling, not a reservation. The account's spot
quota is the real limit, and idle costs nothing.

The CE takes `lt.attr_latest_version_number` from the template in the same stack, so a
template edit rolls straight into the CE with nothing exported across stacks. No security
groups on the CE: Batch rejects them when the template carries NetworkInterfaces.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

from aws_cdk import CfnOutput, aws_batch as batch, aws_ec2 as ec2
from constructs import Construct

from . import catalogue as cat
from .network import FleetNetwork


def lt_data(data: dict) -> ec2.CfnLaunchTemplate.LaunchTemplateDataProperty:
    """catalogue.launch_template_data (EC2 API casing) -> L1 property objects. One body
    serves CloudFormation and the raw-EC2 path, so they cannot drift."""
    P = ec2.CfnLaunchTemplate
    meta = data.get("MetadataOptions")
    return P.LaunchTemplateDataProperty(
        iam_instance_profile=P.IamInstanceProfileProperty(name=data["IamInstanceProfile"]["Name"]),
        block_device_mappings=[
            P.BlockDeviceMappingProperty(
                device_name=b["DeviceName"],
                ebs=P.EbsProperty(delete_on_termination=b["Ebs"]["DeleteOnTermination"],
                                  volume_size=b["Ebs"]["VolumeSize"],
                                  volume_type=b["Ebs"]["VolumeType"],
                                  iops=b["Ebs"]["Iops"], throughput=b["Ebs"]["Throughput"],
                                  encrypted=b["Ebs"].get("Encrypted")))
            for b in data["BlockDeviceMappings"]],
        network_interfaces=[
            P.NetworkInterfaceProperty(
                device_index=n["DeviceIndex"], network_card_index=n["NetworkCardIndex"],
                groups=n["Groups"], delete_on_termination=n["DeleteOnTermination"],
                interface_type=n.get("InterfaceType"),
                associate_public_ip_address=n.get("AssociatePublicIpAddress"))
            for n in data["NetworkInterfaces"]],
        metadata_options=P.MetadataOptionsProperty(
            http_tokens=meta["HttpTokens"],
            http_put_response_hop_limit=meta["HttpPutResponseHopLimit"]) if meta else None,
        user_data=data["UserData"],
        tag_specifications=[P.TagSpecificationProperty(
            resource_type=s["ResourceType"],
            tags=[{"key": t["Key"], "value": t["Value"]} for t in s["Tags"]])
            for s in data.get("TagSpecifications", [])] or None,
    )


@dataclass
class GpuFleetConfig:
    region: str
    prefix: str
    instance_types: List[str]
    offerings: dict
    instance_profile_name: str
    instance_profile_arn: str
    raw_ec2: bool = True
    priority: int = 100
    exclude_az_ids: Tuple[str, ...] = ()
    tags: Dict[str, str] = field(default_factory=dict)


class GpuFleet(Construct):
    def __init__(self, scope: Construct, construct_id: str, *, config: GpuFleetConfig,
                 network: FleetNetwork) -> None:
        super().__init__(scope, construct_id)
        c = config
        #: keyed by TEMPLATE NAME: a row with an extra shape produces two of everything
        self.templates: Dict[str, ec2.CfnLaunchTemplate] = {}
        self.queues: Dict[str, batch.CfnJobQueue] = {}
        self.shapes: Dict[str, cat.Shape] = {}
        self.states: Dict[str, str] = {}

        for itype in c.instance_types:
            t = cat.INSTANCE_TYPES[itype]
            state, azs = cat.region_state(t, c.region, c.offerings, c.exclude_az_ids)
            if state == cat.ABSENT or (state == cat.TEMPLATE_ONLY and not c.raw_ec2):
                continue
            for shape in cat.shapes_in(t, c.region):
                self._shape(c, network, shape, state, azs)

        CfnOutput(self, "TemplateCount", value=str(len(self.templates)))
        CfnOutput(self, "QueueCount", value=str(len(self.queues)))
        CfnOutput(self, "DisabledQueues", value=",".join(
            sorted(n for n, s in self.states.items() if s == cat.DISABLED)) or "none")
        CfnOutput(self, "TemplateOnly", value=",".join(
            sorted(n for n, s in self.states.items() if s == cat.TEMPLATE_ONLY)) or "none")

    def _shape(self, c: GpuFleetConfig, network: FleetNetwork, shape: cat.Shape,
               state: str, azs) -> None:
        t = shape.itype
        name = shape.name(c.prefix)
        cid = name.replace("-", "")
        data = cat.launch_template_data(
            shape, network.node_sg.attr_group_id, c.instance_profile_name,
            tags={"rt-gpu:shape": name, "rt-gpu:family": t.chip, "rt:shape": cat.shape_tag(t)})
        efa_note = " efa" if shape.efa else (" no efa" if shape.extra else "")
        lt = ec2.CfnLaunchTemplate(
            self, "Lt" + cid, launch_template_name=name,
            version_description=f"{t.itype}: {t.gpus}x{t.chip}, {shape.cards} card(s){efa_note}",
            launch_template_data=lt_data(data))
        self.templates[name], self.shapes[name], self.states[name] = lt, shape, state
        if state == cat.TEMPLATE_ONLY:
            return

        # a disabled queue still needs a valid CE: any subnets of the right kind will do
        subnets = (network.subnets_in(shape.cards, azs) if azs
                   else network.subnets_in(shape.cards, list(network.public_subnets)))
        tags = dict(c.tags)
        ce = batch.CfnComputeEnvironment(
            self, "Ce" + cid, type="MANAGED", compute_environment_name=shape.ce_name(c.prefix),
            state="ENABLED", replace_compute_environment=False,
            update_policy=batch.CfnComputeEnvironment.UpdatePolicyProperty(
                terminate_jobs_on_update=False, job_execution_timeout_minutes=30),
            compute_resources=batch.CfnComputeEnvironment.ComputeResourcesProperty(
                type="SPOT", allocation_strategy="SPOT_PRICE_CAPACITY_OPTIMIZED",
                minv_cpus=0, maxv_cpus=t.vcpu, instance_types=[t.itype],
                subnets=subnets, instance_role=c.instance_profile_arn,
                ec2_configuration=[batch.CfnComputeEnvironment.Ec2ConfigurationObjectProperty(
                    image_type=cat.IMAGE_TYPE)],
                launch_template=batch.CfnComputeEnvironment.LaunchTemplateSpecificationProperty(
                    launch_template_name=name, version=lt.attr_latest_version_number),
                tags=tags),
            tags=tags)
        ce.add_dependency(lt)
        self.queues[name] = batch.CfnJobQueue(
            self, "Q" + cid, job_queue_name=shape.queue_name(c.prefix), priority=c.priority,
            state="ENABLED" if state == cat.ENABLED else "DISABLED",
            compute_environment_order=[batch.CfnJobQueue.ComputeEnvironmentOrderProperty(
                order=1, compute_environment=ce.attr_compute_environment_arn)],
            tags=tags)
