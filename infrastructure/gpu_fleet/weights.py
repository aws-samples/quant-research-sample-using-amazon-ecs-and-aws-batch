# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""Base-model weights: one source bucket in the home region, one replica in every other region.

  <weight bucket>             source, home region: models are staged here once
                              (`stage_model` in the fleet image, straight from Hugging Face)
  <weight bucket>-<region>    replica, one per fleet region, filled by S3 replication

A job reads the replica of the region it PLACED in (`model_transport.resolve_bucket`), and only
when the replica's listing of the model is byte-identical to the source's; otherwise it reads
the source. Cross-region single-stream reads are several times slower than local ones and are
billed per GB, and a raced job can land in any of the fleet's regions, so every region holds
its own copy.

The name `<source>-<region>` is a contract with the image, not a convention: the resolver forms
it from the source name and the placement region, so these buckets are named, never generated.

ORDER. S3 validates every replication destination when the rule is written, so the replicas
exist first: they live in the region stacks, and this stack depends on all of them.

RETAINED. Deleting a stack keeps its bucket: weights are slow to stage and CloudFormation
cannot empty a bucket anyway. Versioning is on because replication requires it; old versions
expire after NONCURRENT_DAYS.
"""
from dataclasses import dataclass
from typing import List

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack, aws_iam as iam, aws_s3 as s3
from constructs import Construct

NONCURRENT_DAYS = 7
#: S3 bucket names are at most 63 characters; the longest fleet region adds 15 (-ap-southeast-3)
MAX_BUCKET_NAME = 63


def replica_bucket_name(source: str, region: str) -> str:
    name = f"{source}-{region}"
    if len(name) > MAX_BUCKET_NAME:
        raise ValueError(f"weight replica bucket {name} is {len(name)} characters (S3 allows "
                         f"{MAX_BUCKET_NAME}); shorten gpu_fleet.weight_replica_bucket_prefix")
    return name


def _weights_bucket(scope: Construct, construct_id: str, name: str) -> s3.Bucket:
    return s3.Bucket(
        scope, construct_id, bucket_name=name, versioned=True,
        encryption=s3.BucketEncryption.S3_MANAGED,
        block_public_access=s3.BlockPublicAccess.BLOCK_ALL, enforce_ssl=True,
        removal_policy=RemovalPolicy.RETAIN,
        lifecycle_rules=[s3.LifecycleRule(
            abort_incomplete_multipart_upload_after=Duration.days(NONCURRENT_DAYS),
            noncurrent_version_expiration=Duration.days(NONCURRENT_DAYS))])


class WeightReplica(Construct):
    """The region-local replica, created by the region stack of every region but the home one."""

    def __init__(self, scope: Construct, construct_id: str, *, source_bucket: str) -> None:
        super().__init__(scope, construct_id)
        region = Stack.of(self).region
        self.bucket = _weights_bucket(self, "Bucket", replica_bucket_name(source_bucket, region))
        CfnOutput(self, "WeightReplicaBucket", value=self.bucket.bucket_name)


@dataclass
class GpuFleetWeightsConfig:
    source_bucket: str
    home_region: str
    regions: List[str]


class GpuFleetWeightsStack(Stack):
    """The source bucket and its replication, in the home region."""

    def __init__(self, scope: Construct, construct_id: str, *, config: GpuFleetWeightsConfig,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        c = config
        part = self.partition
        destinations = [r for r in c.regions if r != c.home_region]
        self.source = _weights_bucket(self, "Source", c.source_bucket)
        if not destinations:
            CfnOutput(self, "WeightSourceBucket", value=self.source.bucket_name)
            return

        replica_arns = [f"arn:{part}:s3:::{replica_bucket_name(c.source_bucket, r)}"
                        for r in destinations]
        role = iam.Role(self, "ReplicationRole",
                        assumed_by=iam.ServicePrincipal("s3.amazonaws.com"))
        role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetReplicationConfiguration", "s3:ListBucket"],
            resources=[self.source.bucket_arn]))
        role.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObjectVersionForReplication", "s3:GetObjectVersionAcl",
                     "s3:GetObjectVersionTagging"],
            resources=[self.source.arn_for_objects("*")]))
        role.add_to_policy(iam.PolicyStatement(
            actions=["s3:ReplicateObject", "s3:ReplicateDelete", "s3:ReplicateTags"],
            resources=[f"{a}/*" for a in replica_arns]))

        P = s3.CfnBucket
        cfn: s3.CfnBucket = self.source.node.default_child
        cfn.replication_configuration = P.ReplicationConfigurationProperty(
            role=role.role_arn,
            # one rule per destination; S3 requires a filter, a priority and an explicit
            # delete-marker choice on every rule once there is more than one
            rules=[P.ReplicationRuleProperty(
                id=f"to-{r}", status="Enabled", priority=i + 1,
                filter=P.ReplicationRuleFilterProperty(prefix=""),
                # a delete in the source must not pull a model out from under a running job
                delete_marker_replication=P.DeleteMarkerReplicationProperty(status="Disabled"),
                destination=P.ReplicationDestinationProperty(bucket=arn))
                for i, (r, arn) in enumerate(zip(destinations, replica_arns))])
        CfnOutput(self, "WeightSourceBucket", value=self.source.bucket_name)
        CfnOutput(self, "WeightReplicaRegions", value=",".join(destinations))
