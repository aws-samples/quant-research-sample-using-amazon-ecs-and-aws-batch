# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""The account-wide half of the GPU fleet, deployed once in the home region.

  {namespace}-gpu-node          instance role + profile of every GPU node, Batch or raw EC2
  {namespace}-gpu-job           job role of every fleet job definition, in every region
  {namespace}-gpu-execution     execution role: pull the image, write the log stream
  {namespace}-gpu-customization the role Bedrock assumes for model-customization jobs
  {namespace}-gpu-hunter        managed policy for operators and CI running the raw-EC2 hunt
  ECR replication               home-region image repositories -> every fleet region

IAM is global, so the region stacks reference these by NAME and nothing is exported
across stacks or regions.

The job role is deliberately broad inside the account: fine-tuning drivers submit their
own follow-on jobs (train -> score), cancel losing queues after a cross-region race, read
the spot quota, and call Bedrock for baselines. What it does NOT get is launching raw
instances or creating fleets: those stay with the hunter policy, attached to people and CI.

ECR REPLICATION IS A REGISTRY-LEVEL SINGLETON. An account has one replication
configuration; deploying this one replaces any other. Turn `ecr_replication` off in
parameters.json if the account already manages its own.
"""
from dataclasses import dataclass, field
from typing import List

from aws_cdk import CfnOutput, Stack, aws_ecr as ecr, aws_iam as iam
from constructs import Construct

from . import catalogue as cat


def _bucket_resources(arns: List[str]) -> List[str]:
    """bucket ARN(s) -> bucket + object ARNs, whatever suffix the config used"""
    out = []
    for a in arns:
        b = a.rstrip("*").rstrip("/")
        out += [b, f"{b}/*"]
    return out


@dataclass
class GpuFleetGlobalConfig:
    namespace: str
    home_region: str
    regions: List[str]
    bucket_arns: List[str]
    weight_replica_bucket_prefix: str
    #: repositories to replicate: everything named `<namespace>-...`
    replication_prefix: str
    ecr_replication: bool = True
    raw_ec2: bool = True
    fleet_log_group_prefix: str = "/aws/batch/job"
    extra_log_group_prefixes: List[str] = field(default_factory=list)


class GpuFleetGlobalStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, *, config: GpuFleetGlobalConfig,
                 **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        c = config
        ns, part, acct = c.namespace, self.partition, self.account
        self.node_role_name = f"{ns}-gpu-node"
        self.job_role_name = f"{ns}-gpu-job"
        self.execution_role_name = f"{ns}-gpu-execution"
        tag_cond_ec2 = {"StringEquals": {f"ec2:ResourceTag/{cat.HUNT_TAG_KEY}": cat.HUNT_TAG_VALUE}}
        tag_cond_ssm = {"StringEquals": {f"ssm:resourceTag/{cat.HUNT_TAG_KEY}": cat.HUNT_TAG_VALUE}}
        buckets = _bucket_resources(c.bucket_arns)
        weight_source = _bucket_resources([f"arn:{part}:s3:::{c.weight_replica_bucket_prefix}"])
        replicas = weight_source + _bucket_resources(
            [f"arn:{part}:s3:::{c.weight_replica_bucket_prefix}-*"])

        # ---- node: the ECS agent, SSM for the raw-EC2 runner, image pull
        node = iam.Role(
            self, "NodeRole", role_name=self.node_role_name,
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonEC2ContainerServiceforEC2Role"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"),
            ])
        # raw-EC2 nodes run docker themselves, so the node reads the data the job reads
        node.add_to_policy(iam.PolicyStatement(
            sid="NodeData", actions=["s3:GetObject", "s3:ListBucket"], resources=buckets + replicas))
        node.add_to_policy(iam.PolicyStatement(
            sid="NodeLogs", actions=["logs:CreateLogStream", "logs:PutLogEvents",
                                     "logs:DescribeLogStreams"],
            resources=[f"arn:{part}:logs:*:{acct}:log-group:/{ns}/gpu-fleet/*"]))
        profile = iam.CfnInstanceProfile(self, "NodeInstanceProfile",
                                         instance_profile_name=self.node_role_name,
                                         roles=[node.role_name])
        self.instance_profile_name = profile.instance_profile_name

        # ---- execution: ECR pull from any fleet region's replica + the job's log stream
        iam.Role(
            self, "ExecutionRole", role_name=self.execution_role_name,
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AmazonECSTaskExecutionRolePolicy")])

        # ---- customization: what Bedrock itself assumes for a model-customization job
        customization = iam.Role(
            self, "CustomizationRole", role_name=f"{ns}-gpu-customization",
            assumed_by=iam.ServicePrincipal(
                "bedrock.amazonaws.com",
                conditions={"StringEquals": {"aws:SourceAccount": acct}}))
        customization.add_to_policy(iam.PolicyStatement(
            actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"], resources=buckets))

        # ---- job
        job = iam.Role(self, "JobRole", role_name=self.job_role_name,
                       assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"))
        for s in [
            iam.PolicyStatement(sid="Data", actions=[
                "s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
                "s3:GetBucketLocation", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
                resources=buckets),
            # region-local copies of base-model weights, read where the node runs
            iam.PolicyStatement(sid="WeightReplicas", actions=[
                "s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"], resources=replicas),
            # `stage_model` writes a Hugging Face repo into the source; replication fans it out
            iam.PolicyStatement(sid="WeightStage", actions=[
                "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
                resources=weight_source[1:]),
            # train -> score chains and cross-region races submit and cancel their own jobs
            iam.PolicyStatement(sid="BatchChain", actions=[
                "batch:SubmitJob", "batch:TerminateJob", "batch:CancelJob", "batch:TagResource"],
                resources=[f"arn:{part}:batch:*:{acct}:job-queue/*",
                           f"arn:{part}:batch:*:{acct}:job-definition/*",
                           f"arn:{part}:batch:*:{acct}:job/*"]),
            iam.PolicyStatement(sid="BatchRead", actions=[
                "batch:Describe*", "batch:List*"], resources=["*"]),
            iam.PolicyStatement(sid="Ec2Read", actions=["ec2:Describe*"], resources=["*"]),
            iam.PolicyStatement(sid="Ec2TerminateHuntNodes", actions=["ec2:TerminateInstances"],
                                resources=[f"arn:{part}:ec2:*:{acct}:instance/*"],
                                conditions=tag_cond_ec2),
            iam.PolicyStatement(sid="SsmRead", actions=["ssm:GetParameter", "ssm:GetParameters"],
                                resources=[f"arn:{part}:ssm:*:{acct}:parameter/*",
                                           f"arn:{part}:ssm:*::parameter/aws/service/*"]),
            iam.PolicyStatement(sid="SsmSendCommandDocument", actions=["ssm:SendCommand"],
                                resources=[f"arn:{part}:ssm:*::document/AWS-RunShellScript"]),
            iam.PolicyStatement(sid="SsmSendCommandHuntNodes", actions=["ssm:SendCommand"],
                                resources=[f"arn:{part}:ec2:*:{acct}:instance/*"],
                                conditions=tag_cond_ssm),
            iam.PolicyStatement(sid="SsmCommandRead", actions=[
                "ssm:GetCommandInvocation", "ssm:ListCommandInvocations"], resources=["*"]),
            iam.PolicyStatement(sid="BedrockInvoke", actions=[
                "bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream",
                "bedrock:Converse", "bedrock:ConverseStream"],
                resources=[f"arn:{part}:bedrock:*::foundation-model/*",
                           f"arn:{part}:bedrock:*:{acct}:inference-profile/*",
                           f"arn:{part}:bedrock:*:{acct}:custom-model/*",
                           f"arn:{part}:bedrock:*:{acct}:custom-model-deployment/*",
                           f"arn:{part}:bedrock:*:{acct}:provisioned-model/*",
                           f"arn:{part}:bedrock:*:{acct}:imported-model/*"]),
            iam.PolicyStatement(sid="BedrockCustomize", actions=[
                "bedrock:CreateModelCustomizationJob", "bedrock:GetModelCustomizationJob",
                "bedrock:ListModelCustomizationJobs", "bedrock:StopModelCustomizationJob",
                "bedrock:CreateModelImportJob", "bedrock:GetModelImportJob",
                "bedrock:GetCustomModel", "bedrock:ListCustomModels",
                "bedrock:GetImportedModel", "bedrock:ListImportedModels",
                "bedrock:ListFoundationModels", "bedrock:GetFoundationModel",
                "bedrock:TagResource"], resources=["*"]),
            iam.PolicyStatement(sid="BedrockPassCustomizationRole", actions=["iam:PassRole"],
                                resources=[customization.role_arn],
                                conditions={"StringEquals": {
                                    "iam:PassedToService": "bedrock.amazonaws.com"}}),
            iam.PolicyStatement(sid="SpotQuota", actions=[
                "servicequotas:ListServiceQuotas", "servicequotas:GetServiceQuota"],
                resources=["*"]),
            iam.PolicyStatement(sid="Logs", actions=[
                "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams",
                "logs:GetLogEvents", "logs:FilterLogEvents"],
                resources=[f"arn:{part}:logs:*:{acct}:log-group:{p}*"
                           for p in [c.fleet_log_group_prefix, f"/{ns}/gpu-fleet/",
                                     *c.extra_log_group_prefixes]]),
        ]:
            job.add_to_policy(s)

        # ---- hunter: people and CI racing regions for raw EC2 nodes
        if c.raw_ec2:
            hunter = iam.ManagedPolicy(
                self, "HunterPolicy", managed_policy_name=f"{ns}-gpu-hunter",
                description="raw-EC2 GPU hunt + SSM docker runner (attach to operators / CI)",
                statements=[
                    iam.PolicyStatement(sid="HuntRead", actions=[
                        "ec2:Describe*", "servicequotas:GetServiceQuota",
                        "servicequotas:ListServiceQuotas", "ssm:GetParameter", "ssm:GetParameters",
                        "ssm:GetCommandInvocation", "ssm:ListCommandInvocations",
                        "ssm:ListCommands", "ssm:DescribeInstanceInformation",
                        "logs:GetLogEvents", "logs:FilterLogEvents", "logs:DescribeLogStreams",
                    ], resources=["*"]),
                    iam.PolicyStatement(sid="HuntLaunch", actions=[
                        "ec2:CreateFleet", "ec2:RunInstances", "ec2:CreateTags",
                        "ec2:CreatePlacementGroup", "ec2:CreateLaunchTemplateVersion",
                    ], resources=["*"]),
                    iam.PolicyStatement(sid="HuntPassNodeRole", actions=["iam:PassRole"],
                                        resources=[node.role_arn]),
                    iam.PolicyStatement(sid="HuntTerminateOwnNodes",
                                        actions=["ec2:TerminateInstances"],
                                        resources=[f"arn:{part}:ec2:*:{acct}:instance/*"],
                                        conditions=tag_cond_ec2),
                    iam.PolicyStatement(sid="RunnerSendCommandDocument", actions=["ssm:SendCommand"],
                                        resources=[f"arn:{part}:ssm:*::document/AWS-RunShellScript"]),
                    iam.PolicyStatement(sid="RunnerSendCommandOwnNodes", actions=["ssm:SendCommand"],
                                        resources=[f"arn:{part}:ec2:*:{acct}:instance/*"],
                                        conditions=tag_cond_ssm),
                ])
            CfnOutput(self, "HunterPolicyArn", value=hunter.managed_policy_arn)

        # ---- images: build once in the home region, pull region-locally everywhere else
        destinations = [r for r in c.regions if r != c.home_region]
        if c.ecr_replication and destinations:
            P = ecr.CfnReplicationConfiguration
            ecr.CfnReplicationConfiguration(
                self, "ImageReplication",
                replication_configuration=P.ReplicationConfigurationProperty(rules=[
                    P.ReplicationRuleProperty(
                        destinations=[P.ReplicationDestinationProperty(region=r, registry_id=acct)
                                      for r in destinations],
                        repository_filters=[P.RepositoryFilterProperty(
                            filter=c.replication_prefix, filter_type="PREFIX_MATCH")])]))

        self.instance_profile_arn = f"arn:{part}:iam::{acct}:instance-profile/{self.node_role_name}"
        CfnOutput(self, "JobRoleArn", value=job.role_arn)
        CfnOutput(self, "InstanceProfile", value=self.node_role_name)
